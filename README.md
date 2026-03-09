# BKK_NOW

Bangkok events calendar — upcoming art, culture, food, music, nightlife and sports events.

## Live

https://bkk-now.shellnode.lol

## Stack

- Static HTML/CSS/JS (vanilla, no frameworks)
- Python pipeline: Tavily (discovery) + Firecrawl (scraping) + Gemini Flash (normalisation) → `data/events.json`
- nginx:alpine container
- Ghost VPS / Docker
- SSL via SWAG + Cloudflare DNS

## Run Locally

Open `index.html` in a browser (needs `data/events.json`), or:

    docker build -t bkk-now .
    docker run -p 8080:80 -v ./data:/usr/share/nginx/html/data bkk-now

## Pipeline

Copy `.env.example` to `.env` and fill in API keys, then:

    pip install -r requirements.txt
    python pipeline.py

Output: `data/events.json`

## Data Sources

- Tavily search (22 queries — venue calendars, category searches, Thai-language queries)
- Firecrawl scrapes of 14 curated event listing pages
- Gemini Flash normalisation and deduplication
