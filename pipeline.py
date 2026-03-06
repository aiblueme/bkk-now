#!/usr/bin/env python3
"""
BKK-NOW — Bangkok events data pipeline v4
Tavily (discovery) + Firecrawl (curated scrapes) → Gemini Flash (normalisation) → data/events.json
# TODO: add cron
"""

import os
import json
import re
import sys
import time
import requests
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
    f"Bangkok Muay Thai schedule fights {MONTH_YEAR}",
    f"Bangkok food pop-up dining experience chef {MONTH_YEAR}",
    f"Bangkok sports tournament competition {MONTH_YEAR}",

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

FIRECRAWL_HOST = os.environ.get("FIRECRAWL_HOST", "http://localhost:3002")

# Single-page scrapes
FIRECRAWL_SOURCES = [
    "https://bk.asia-city.com/events",
    "https://www.coconuts.co/bangkok/events/",
    "https://www.bacc.or.th/en/event/",
    "https://www.siamparagon.co.th/en/events/",
    "https://www.centralworld.co.th/en/event",
    "https://www.iconsiam.com/en/events",
    "https://emquartier.co.th/event",
    "https://www.eventpop.me/events?location=bangkok",
    "https://www.thaiticketmajor.com/event/",
    "https://rajadamnern.com/tickets/",
    "https://ra.co/clubs/th/bangkok",
    "https://www.tatnews.org/category/festivals-events/",
    "https://www.expatden.com/thailand/events-in-bangkok/",
    "https://www.muaythaiworld.com/schedule",
]

# Crawl sources — follows links 1 level deep to discover sub-articles
FIRECRAWL_CRAWL_SOURCES = [
    "https://www.timeout.com/bangkok/things-to-do",
]


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


# ── Firecrawl ─────────────────────────────────────────────────────────────────

def firecrawl_crawl(url, max_pages=10):
    """Crawl a URL and follow links one level deep. Returns list of markdown strings."""
    try:
        resp = requests.post(f"{FIRECRAWL_HOST}/v1/crawl", json={
            "url": url,
            "maxDepth": 2,
            "limit": max_pages,
            "scrapeOptions": {
                "formats": ["markdown"],
                "onlyMainContent": True,
            },
        }, timeout=10)
        if resp.status_code != 200:
            print(f"  ✗ crawl start failed {url} (status {resp.status_code})")
            return []

        crawl_id = resp.json().get("id")
        if not crawl_id:
            return []

        for _ in range(12):
            time.sleep(5)
            status_resp = requests.get(f"{FIRECRAWL_HOST}/v1/crawl/{crawl_id}", timeout=10)
            data = status_resp.json()
            if data.get("status") == "completed":
                pages = data.get("data", [])
                results = [p.get("markdown", "") for p in pages if p.get("markdown")]
                print(f"  ✓ crawl {url} → {len(results)} pages")
                return results
            elif data.get("status") == "failed":
                print(f"  ✗ crawl failed {url}")
                return []

        print(f"  ✗ crawl timeout {url}")
        return []
    except Exception as e:
        print(f"  ✗ crawl error {url}: {e}")
        return []


def firecrawl_scrape(url):
    try:
        resp = requests.post(f"{FIRECRAWL_HOST}/v1/scrape", json={
            "url": url,
            "formats": ["markdown"],
            "onlyMainContent": True,
        }, timeout=30)
        if resp.status_code == 200:
            md = resp.json().get("data", {}).get("markdown", "")
            if md:
                print(f"  ✓ {url} ({len(md)} chars)")
                return md
        print(f"  ✗ {url} (status {resp.status_code})")
    except Exception as e:
        print(f"  ✗ {url} ({e})")
    return None


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

def run_gemini(combined_input):
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
  "description": "2-3 sentences. If price is known include it (Free / ฿XXX) — if unknown omit it entirely, never write 'Price not specified'. Include times if known.",
  "date_start": "YYYY-MM-DD",
  "date_end": "YYYY-MM-DD",
  "date_display": "SAT 07 MAR 2026",
  "venue": "Venue Name, District/Area, Bangkok",
  "url": "source url or null",
  "updated_at": "{NOW_ISO}"
}}

Rules:
- venue must include a Bangkok district/neighbourhood — "Bangkok" or "Bangkok, Thailand" alone are NOT acceptable
- Good format: "Rajadamnern Stadium, Pom Prap Sattru Phai, Bangkok" or "River City Bangkok, Charoenkrung, Bangkok"
- if you cannot determine a specific district, skip the event entirely
- date_end equals date_start for single-day events
- category must be exactly one of: art, culture, food, music, nightlife, sports

Additional rules:
- Never start a description with "Experience the" or "Enjoy the" — describe what actually happens at the event
- BALANCE is critical: aim for roughly equal events per category
- If you have 10+ music events, be selective — only include the most notable ones
- NEVER include the same artist/event twice even at different venues
- Malls, parks, markets, community fairs belong in "culture" or "food" categories
- Rooftop parties, pool parties, club nights = "nightlife" not "music"
- Running events, Muay Thai, sports tournaments = "sports"
- Max 5 events per category — if you have more, pick the most interesting/specific ones

Raw search data:
{combined_input}"""

    response = gemini_client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
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
    # Strip control characters that break JSON parsing (keep \t \n \r)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
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
    print(f"BKK-NOW Pipeline v4 — {TODAY_DISPLAY}")
    print(f"Gemini model: {GEMINI_MODEL}")

    # Step 1: Tavily discovery
    print(f"\nRunning {len(QUERIES)} Tavily queries...")
    raw_results, total_raw = run_tavily()
    url_deduped, title_deduped = dedup_results(raw_results)

    print(f"Tavily: {total_raw} raw → {len(title_deduped)} after dedup")

    if not title_deduped:
        print("Error: no Tavily results — check API key and quota")
        sys.exit(1)

    # Step 2: Firecrawl curated scrapes
    print(f"\nFirecrawl: scraping {len(FIRECRAWL_SOURCES)} sources...")
    firecrawl_results = []
    for url in FIRECRAWL_SOURCES:
        md = firecrawl_scrape(url)
        if md:
            firecrawl_results.append({"url": url, "markdown": md[:8000]})

    print(f"Firecrawl: {len(firecrawl_results)}/{len(FIRECRAWL_SOURCES)} sources returned content")

    print(f"\nFirecrawl: crawling {len(FIRECRAWL_CRAWL_SOURCES)} sources (depth=1)...")
    for url in FIRECRAWL_CRAWL_SOURCES:
        pages = firecrawl_crawl(url, max_pages=15)
        for i, md in enumerate(pages):
            if md:
                firecrawl_results.append({"url": f"{url}#page{i}", "markdown": md[:6000]})

    # Step 3: Build combined input for Gemini
    combined_input = "=== TAVILY SEARCH RESULTS ===\n"
    combined_input += json.dumps(
        [{"title": r.get("title"), "url": r.get("url"), "content": r.get("content"), "score": r.get("score")}
         for r in title_deduped],
        ensure_ascii=False,
    )
    combined_input += "\n\n=== FIRECRAWL SCRAPED PAGES ===\n"
    for r in firecrawl_results:
        combined_input += f"\n--- SOURCE: {r['url']} ---\n"
        combined_input += r["markdown"]
        combined_input += "\n"

    # Step 4: Gemini normalisation
    print("\nNormalising with Gemini Flash...")
    events = run_gemini(combined_input)
    events = normalise(events)

    # Step 5: Write output
    output = {"generated_at": NOW_ISO, "source": "tavily+firecrawl+gemini", "events": events}
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
        print(f"⚠️  Empty categories: {', '.join(sorted(empty))}")
    print(f"\n✓ Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
