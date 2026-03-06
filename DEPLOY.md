# bkk-now — Deployment Report

**Live:** https://bkk-now.shellnode.lol | **Repo:** https://github.com/aiblueme/bkk-now

## What Was Built
1. `pipeline.py` — queries Gemini 2.5 Flash with Google Search grounding, returns structured JSON of Bangkok events, writes to `data/events.json`
2. `index.html` — single-file brutalist frontend: category filter nav, 3-col grid, past/upcoming split, red/mono/black design
3. `Dockerfile` — nginx:alpine serving static files; `data/` bind-mounted so pipeline can refresh without rebuilding
4. `docker-compose.yml` — attaches container to `swag-network` for SWAG reverse proxy

## Problems Solved During Build
- `google-generativeai` deprecated → switched to `google-genai` SDK
- `gemini-3-flash-preview` requires billing → switched to `gemini-2.5-flash` (free tier, supports grounding)
- Gemini returning `"cultural"` category → normalised to `"culture"` in pipeline
- Gemini returning Google grounding redirect URLs → stripped, set to `null`
- No pip/venv on server (Ubuntu 24.04, no sudo) → ran pipeline via `python:3.12-slim` Docker container
- Docker context named `vps2` not `shell` on dev machine; network `swag-network` not `swag_network`

## Deployment Steps
1. Built and tested pipeline locally on dev machine
2. Pushed code to GitHub (`aiblueme/bkk-now`)
3. SSH'd into vps2 via `dockerssh@192.168.0.1`, cloned repo
4. Ran pipeline inside `python:3.12-slim` container → wrote `data/events.json` (28 events)
5. Built `bkk-now` Docker image on vps2
6. Started container via `docker compose up -d` on `swag-network`
7. Wrote `bkk-now.subdomain.conf` to SWAG proxy-confs, validated and reloaded nginx
8. Confirmed: `HTTP/2 200`, 28 events served at `bkk-now.shellnode.lol`
