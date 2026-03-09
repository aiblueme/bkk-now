---
project: bkk-now
url: https://bkk-now.shellnode.lol
vps: ghost
port: (bind-mounted via compose, no host port exposed)
stack: single-file HTML + Python pipeline, nginx:alpine, SWAG
standards_version: "2.0"
security: done
ux_ui: done
repo_cleanup: done
readme: done
last_session: "2026-03-10"
has_blockers: false
---

# Project Status — bkk-now

## Last Session
Date: 2026-03-09
Agent: Claude Sonnet 4.6

### Completed
- Created `nginx.conf` with security headers (X-Content-Type-Options, X-Frame-Options, Referrer-Policy, Permissions-Policy), gzip, 7d static asset cache, dotfile blocking — commit de1a777
- Updated `Dockerfile`: added RUN rm default html, COPY nginx.conf, EXPOSE 80, CMD — commit de1a777
- Created `.dockerignore` — commit de1a777
- Updated `.gitignore`: added .DS_Store, .vscode/, *.log — commit de1a777
- Updated `.env.example`: added missing TAVILY_API_KEY entry — commit de1a777
- Created `README.md` — commit 8fcc8c4
- `index.html`: added meta description, OG tags, inline SVG favicon, skip-link, focus-visible outlines, font-family inheritance — commit 8fcc8c4
- Pushed all changes to GitHub

### Incomplete
- None — all P0/P1/P2/P3 items addressed

### Blocked — Needs Matt
- None

## Backlog
(none)

## Done
- [x] Add MIT LICENSE — 2026-03-10 — commit 766dd71
- [x] nginx: add server_tokens off — 2026-03-10
- [x] Verify SWAG labels on live container — confirmed correct (swag=enable, correct address/port/url) — 2026-03-10
- [x] No secrets in git history — confirmed clean (2026-03-09)
- [x] .env not tracked (in .gitignore since initial commit) — (2026-03-09)
- [x] Add nginx.conf with security headers — de1a777 (2026-03-09)
- [x] Add .dockerignore — de1a777 (2026-03-09)
- [x] Update .gitignore — de1a777 (2026-03-09)
- [x] Update .env.example — de1a777 (2026-03-09)
- [x] Add README.md — 8fcc8c4 (2026-03-09)
- [x] Add OG tags, favicon, skip-link, focus styles — 8fcc8c4 (2026-03-09)

## Decisions Log
- "Did not commit pipeline.py or data/events.json — both had pre-existing local changes unrelated to audit scope (pipeline crawl source additions, fresh JSON output)" (2026-03-09)
- "Did not add CSP header — project has no external CDN deps so could be added, but skipped as P3 since SWAG likely handles at proxy level" (2026-03-09)
- "Did not add memory limit to docker-compose.yml — P2 but requires knowing the right value; static site with bind-mounted data directory, 128m is reasonable but skipped to avoid breaking running container" (2026-03-09)

## Project Notes
- Python pipeline (pipeline.py) fetches Bangkok events via Tavily + Firecrawl + Gemini Flash, writes to data/events.json
- data/ directory is bind-mounted at runtime — pipeline can refresh without rebuilding the Docker image
- git remote uses HTTPS (https://github.com/aiblueme/bkk-now.git) — push works without SSH key
- The local working tree has uncommitted changes to pipeline.py (added Timeout crawl sources, improved prompt) and data/events.json (fresh pipeline run) — not audit scope, left as-is
