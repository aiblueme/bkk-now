#!/usr/bin/env python3
"""
BKK-NOW — Bangkok events data pipeline
Uses Gemini Flash with Google Search grounding to find current Bangkok events.
# TODO: add cron
"""

import os
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv(Path(__file__).parent / ".env")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

if not GEMINI_API_KEY:
    print("Error: GEMINI_API_KEY not set (add to .env or environment)")
    sys.exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "events.json"

TODAY = datetime.now().strftime("%Y-%m-%d")
TODAY_DISPLAY = datetime.now().strftime("%d %b %Y").upper()
NOW_ISO = datetime.now(timezone.utc).isoformat()

PROMPT = f"""Today is {TODAY}. You are a Bangkok events researcher.

Find non-recurring, temporary Bangkok events happening in the next 30 days (from {TODAY} onwards).

Focus on: concerts, live music, art exhibitions, pop-up markets, club nights, festivals, sports events, food events, cultural events.

DO NOT include: permanent venues, weekly recurring events (e.g. "Chatuchak every Saturday"), regular club nights that happen every week, permanent exhibitions, generic ongoing activities.

Return ONLY a JSON array. No markdown, no preamble, no explanation. Just the raw JSON array.

Each event object must follow this exact schema:
{{
  "id": "slugified-title-YYYYMMDD",
  "title": "Event Title",
  "category": "art|culture|food|music|nightlife|sports",
  "description": "2-3 sentence description. Include price if known (Free / ฿XXX). Include times.",
  "date_start": "YYYY-MM-DD",
  "date_end": "YYYY-MM-DD",
  "date_display": "FRI 27 FEB 2026",
  "venue": "Venue Name, District/Area",
  "url": "https://... or null",
  "updated_at": "{NOW_ISO}"
}}

category must be exactly one of: art, culture, food, music, nightlife, sports
Return as many real, verified events as you can find. Aim for 15-25 events."""


def strip_fences(text):
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"```\s*$", "", text, flags=re.MULTILINE)
    return text.strip()


def slugify(title, date_start):
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    date_compact = date_start.replace("-", "")
    return f"{slug}-{date_compact}"


def run_pipeline():
    print(f"BKK-NOW Pipeline — {TODAY_DISPLAY}")
    print(f"Model: {GEMINI_MODEL}")
    print("Querying Gemini with Google Search grounding...")

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=PROMPT,
        config=types.GenerateContentConfig(
            tools=[types.Tool(google_search=types.GoogleSearch())],
        ),
    )

    raw = response.text
    print(f"Response received ({len(raw)} chars)")

    clean = strip_fences(raw)

    try:
        events = json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print("Raw response (first 500 chars):")
        print(raw[:500])
        sys.exit(1)

    if not isinstance(events, list):
        print(f"Expected JSON array, got: {type(events)}")
        sys.exit(1)

    valid_cats = {"art", "culture", "food", "music", "nightlife", "sports"}
    for event in events:
        if not event.get("id") or not event["id"].strip():
            event["id"] = slugify(
                event.get("title", "unknown"), event.get("date_start", TODAY)
            )
        event["updated_at"] = NOW_ISO
        # Normalize category aliases
        cat = event.get("category", "").lower()
        if cat == "cultural":
            event["category"] = "culture"
        elif cat not in valid_cats:
            event["category"] = "culture"
        # Strip Google grounding redirect URLs — not useful to end users
        url = event.get("url") or ""
        if "vertexaisearch.cloud.google.com" in url or "grounding-api-redirect" in url:
            event["url"] = None

    events.sort(key=lambda e: e.get("date_start", ""))

    output = {"generated_at": NOW_ISO, "events": events}

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    total = len(events)
    categories = {}
    for event in events:
        cat = event.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

    print(f"\n✓ {total} events written to {OUTPUT_FILE}")
    print("Breakdown by category:")
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")


if __name__ == "__main__":
    run_pipeline()
