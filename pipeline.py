#!/usr/bin/env python3
"""
BKK-NOW — Bangkok events data pipeline v3
Tavily Search (discovery) → Gemini Flash (normalisation) → data/events.json
# TODO: add cron
"""

import os
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from tavily import TavilyClient
from google import genai

load_dotenv(Path(__file__).parent / ".env")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
TAVILY_API_KEY = os.environ.get("TAVILY_API_KEY")

missing = [k for k, v in [("GEMINI_API_KEY", GEMINI_API_KEY), ("TAVILY_API_KEY", TAVILY_API_KEY)] if not v]
if missing:
    print(f"Error: missing env vars: {', '.join(missing)}")
    sys.exit(1)

gemini_client = genai.Client(api_key=GEMINI_API_KEY)
tavily = TavilyClient(api_key=TAVILY_API_KEY)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "events.json"

now = datetime.now()
TODAY = now.strftime("%Y-%m-%d")
TODAY_DISPLAY = now.strftime("%d %b %Y").upper()
NOW_ISO = datetime.now(timezone.utc).isoformat()
MONTH_YEAR = now.strftime("%B %Y")
MONTH_TH = {
    1: "มกราคม", 2: "กุมภาพันธ์", 3: "มีนาคม", 4: "เมษายน",
    5: "พฤษภาคม", 6: "มิถุนายน", 7: "กรกฎาคม", 8: "สิงหาคม",
    9: "กันยายน", 10: "ตุลาคม", 11: "พฤศจิกายน", 12: "ธันวาคม",
}[now.month]
YEAR_TH = str(now.year + 543)

QUERIES = [
    # --- Venues with their own event calendars ---
    f"Siam Paragon events activities {MONTH_YEAR}",
    f"CentralWorld ICONSIAM EmQuartier events {MONTH_YEAR}",
    f"BACC Bangkok Art Culture Centre exhibition {MONTH_YEAR}",
    f"Lumpini Park Chatuchak Park events activities {MONTH_YEAR}",
    f"River City Bangkok Asiatique events {MONTH_YEAR}",

    # --- Category-specific, non-concert ---
    f"Bangkok art exhibition gallery opening {MONTH_YEAR}",
    f"Bangkok food festival market fair {MONTH_YEAR}",
    f"Bangkok Muay Thai boxing fight night {MONTH_YEAR}",
    f"Bangkok marathon run triathlon sports {MONTH_YEAR}",
    f"Bangkok culture festival community fair {MONTH_YEAR}",
    f"Bangkok night market popup weekend {MONTH_YEAR}",
    f"Bangkok rooftop party club night DJ {MONTH_YEAR}",

    # --- Known reliable sources ---
    f"site:bk.asia-city.com events {MONTH_YEAR}",
    f"site:coconuts.co Bangkok events {MONTH_YEAR}",
    f"site:eventpop.me Bangkok {MONTH_YEAR}",
    f"site:ra.co Bangkok {MONTH_YEAR}",

    # --- Thai language queries ---
    f"งานกรุงเทพ {MONTH_TH} {YEAR_TH}",
    f"เทศกาลกรุงเทพ {MONTH_TH} {YEAR_TH}",
    f"กิจกรรมสยามพารากอน เซ็นทรัลเวิลด์ {MONTH_TH}",
    f"คอนเสิร์ต งานวิ่ง ตลาดนัด กรุงเทพ {MONTH_TH}",
]

VALID_CATS = {"art", "culture", "food", "music", "nightlife", "sports"}


# ── Deduplication ─────────────────────────────────────────────────────────────

def is_similar_title(a, b):
    words_a = a.lower().split()
    words_b_str = " ".join(b.lower().split())
    for i in range(len(words_a) - 3):
        chunk = " ".join(words_a[i:i+4])
        if chunk in words_b_str:
            return True
    return False


def dedup_results(raw_results):
    # Pass 1 — URL dedup, keep highest score per URL
    by_url = {}
    for r in raw_results:
        url = r.get("url", "")
        if url and (url not in by_url or r.get("score", 0) > by_url[url].get("score", 0)):
            by_url[url] = r
    url_deduped = list(by_url.values())

    # Pass 2 — Title similarity dedup
    deduped = []
    for result in url_deduped:
        title = result.get("title") or ""
        matched = False
        for i, seen in enumerate(deduped):
            seen_title = seen.get("title") or ""
            if is_similar_title(title, seen_title):
                if result.get("score", 0) > seen.get("score", 0):
                    deduped[i] = result
                matched = True
                break
        if not matched:
            deduped.append(result)

    return url_deduped, deduped


# ── Tavily ────────────────────────────────────────────────────────────────────

def run_tavily():
    all_results = []
    total_raw = 0

    for query in QUERIES:
        try:
            resp = tavily.search(
                query,
                search_depth="advanced",
                max_results=8,
                include_answer=True,
                days=30,
            )
            results = resp.get("results", [])
            total_raw += len(results)
            all_results.extend(results)
        except Exception as e:
            print(f"  WARNING: query failed ({query!r}): {e}")

    return all_results, total_raw


# ── Gemini normalisation ──────────────────────────────────────────────────────

def run_gemini(results):
    results_json = json.dumps(
        [
            {
                "title": r.get("title"),
                "url": r.get("url"),
                "content": r.get("content"),
                "score": r.get("score"),
            }
            for r in results
        ],
        ensure_ascii=False,
        indent=2,
    )

    prompt = f"""You are an event data extractor. Below is raw search result data about Bangkok events.

Extract ONLY real, non-recurring, temporary events happening in the next 30 days.
Skip: permanent venues, weekly recurring events, bars/restaurants that are always open.
Today's date is {TODAY}.

Return ONLY a JSON array. No markdown, no preamble, no explanation.

Each event object must match this schema exactly:
{{
  "id": "slugified-title-YYYYMMDD",
  "title": "Event Title",
  "category": "art|culture|food|music|nightlife|sports",
  "description": "2-3 sentences. Include price if known (Free / ฿XXX). Include times if known.",
  "date_start": "YYYY-MM-DD",
  "date_end": "YYYY-MM-DD",
  "date_display": "SAT 07 MAR 2026",
  "venue": "Venue Name, District/Area, Bangkok",
  "url": "source url or null",
  "updated_at": "{NOW_ISO}"
}}

Rules:
- venue must be specific — never just "Bangkok" or "Thailand"
- if you cannot determine a specific venue, skip the event entirely
- date_end equals date_start for single-day events
- category must be exactly one of: art, culture, food, music, nightlife, sports

Additional rules:
- BALANCE is critical: aim for roughly equal events per category
- If you have 10+ music events, be selective — only include the most notable ones
- NEVER include the same artist/event twice even at different venues
- Malls, parks, markets, community fairs belong in "culture" or "food" categories
- Rooftop parties, pool parties, club nights = "nightlife" not "music"
- Running events, Muay Thai, sports tournaments = "sports"
- Max 5 events per category — if you have more, pick the most interesting/specific ones

Raw search data:
{results_json}"""

    response = gemini_client.models.generate_content(model=GEMINI_MODEL, contents=prompt)

    # TODO: Phase 2 — enrich event URLs with self-hosted Firecrawl for full page content

    raw = response.text
    clean = strip_fences(raw)

    try:
        events = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"Gemini JSON parse error: {e}")
        print("Raw response (first 500 chars):")
        print(raw[:500])
        sys.exit(1)

    if not isinstance(events, list):
        print(f"Expected JSON array, got: {type(events)}")
        sys.exit(1)

    return events


# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_fences(text):
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    return text.strip()


def slugify(title, date_start):
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"{slug}-{date_start.replace('-', '')}"


def normalise(events):
    for event in events:
        if not event.get("id") or not event["id"].strip():
            event["id"] = slugify(event.get("title", "unknown"), event.get("date_start", TODAY))
        event["updated_at"] = NOW_ISO
        cat = event.get("category", "").lower()
        if cat not in VALID_CATS:
            event["category"] = "culture"
    events.sort(key=lambda e: e.get("date_start", ""))
    return events


# ── Main ──────────────────────────────────────────────────────────────────────

def run_pipeline():
    print(f"BKK-NOW Pipeline v3 — {TODAY_DISPLAY}")
    print(f"Gemini model: {GEMINI_MODEL}")

    # Step 1: Tavily discovery
    print(f"\nRunning {len(QUERIES)} Tavily queries...")
    raw_results, total_raw = run_tavily()
    url_deduped, title_deduped = dedup_results(raw_results)

    print(f"Tavily: {total_raw} raw results across {len(QUERIES)} queries")
    print(f"After URL dedup: {len(url_deduped)} unique URLs")
    print(f"After title dedup: {len(title_deduped)} results")

    if not title_deduped:
        print("Error: no Tavily results — check API key and quota")
        sys.exit(1)

    # Step 2: Gemini normalisation
    print("\nNormalising with Gemini Flash...")
    events = run_gemini(title_deduped)
    events = normalise(events)

    # Step 3: Write output
    output = {"generated_at": NOW_ISO, "source": "tavily+gemini", "events": events}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    categories = {cat: 0 for cat in VALID_CATS}
    for event in events:
        cat = event.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    cats_str = ", ".join(f"{k}={v}" for k, v in sorted(categories.items()))
    empty = [k for k, v in categories.items() if v == 0]

    print(f"Gemini extracted: {len(events)} valid events")
    print(f"Breakdown: {cats_str}")
    if empty:
        print(f"⚠️  Categories with 0 events: {', '.join(sorted(empty))}")
    print(f"\n✓ Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
