#!/usr/bin/env python3
"""
BKK-NOW — Bangkok events data pipeline v2
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

gemini = genai.Client(api_key=GEMINI_API_KEY)
tavily = TavilyClient(api_key=TAVILY_API_KEY)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "events.json"

TODAY = datetime.now().strftime("%Y-%m-%d")
TODAY_DISPLAY = datetime.now().strftime("%d %b %Y").upper()
NOW_ISO = datetime.now(timezone.utc).isoformat()

QUERIES = [
    "Bangkok art exhibitions events March 2026",
    "Bangkok concerts live music shows March 2026",
    "Bangkok nightlife club events DJ March 2026",
    "Bangkok food festivals markets events March 2026",
    "Bangkok sports events Muay Thai fights March 2026",
    "Bangkok culture festivals events March 2026",
    "site:ra.co Bangkok events 2026",
    "site:eventpop.me Bangkok events 2026",
]

VALID_CATS = {"art", "culture", "food", "music", "nightlife", "sports"}


def strip_fences(text):
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    return text.strip()


def slugify(title, date_start):
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return f"{slug}-{date_start.replace('-', '')}"


def run_tavily():
    all_results = {}  # url → result (dedup, keep highest score)
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
            for r in results:
                url = r.get("url", "")
                if url and (url not in all_results or r.get("score", 0) > all_results[url].get("score", 0)):
                    all_results[url] = r
        except Exception as e:
            print(f"  WARNING: query failed ({query!r}): {e}")

    return list(all_results.values()), total_raw


def run_gemini(results):
    results_json = json.dumps(
        [{"title": r.get("title"), "url": r.get("url"), "content": r.get("content"), "score": r.get("score")} for r in results],
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

Raw search data:
{results_json}"""

    response = gemini.models.generate_content(model=GEMINI_MODEL, contents=prompt)

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


def run_pipeline():
    print(f"BKK-NOW Pipeline v2 — {TODAY_DISPLAY}")
    print(f"Gemini model: {GEMINI_MODEL}")

    # Step 1: Tavily discovery
    print(f"\nRunning {len(QUERIES)} Tavily queries...")
    results, total_raw = run_tavily()
    print(f"Tavily: {total_raw} raw results across {len(QUERIES)} queries")
    print(f"After dedup: {len(results)} unique URLs")

    if not results:
        print("Error: no Tavily results — check API key and quota")
        sys.exit(1)

    # Step 2: Gemini normalisation
    print("\nNormalising with Gemini Flash...")
    events = run_gemini(results)
    events = normalise(events)

    # Step 3: Write output
    output = {"generated_at": NOW_ISO, "source": "tavily+gemini", "events": events}
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    categories = {}
    for event in events:
        cat = event.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    cats_str = ", ".join(f"{k}={v}" for k, v in sorted(categories.items()))
    print(f"Gemini extracted: {len(events)} valid events")
    print(f"Breakdown: {cats_str}")
    print(f"\n✓ Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    run_pipeline()
