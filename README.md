# WarRoom Flask Backend (Structured for Multiple Scrapers)

This backend is now organized for multiple scrapers and scheduled jobs.

## Layout

- `warroom_backend/`
  - `app.py` app factory and HTTP routes
  - `config.py` env settings
  - `utils.py` shared helpers
  - `jobs/` job orchestration and in-memory job state
  - `scheduler/` APScheduler integration for cron/interval jobs
  - `services/` Appwrite + file storage services
  - `scrapers/` independent scraper modules
    - `generic.py`
    - `overthecap.py`

## Setup

1. Create virtual environment and install dependencies:
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -r requirements.txt`
   - or `make install`
2. Configure:
   - `cp .env.example .env`
3. Run:
   - `python app.py`
   - or `make run`

## Run with Make

```
make run
# optional:
make run PORT=5001
make run-debug
make install
```

## Endpoints

- `GET /health`
  - Returns app health and scheduler/job counts.

- `POST /api/scrape`
  - Generic scrape endpoint.
  - Body:
    - `type` optional (`generic` default)
    - `url` required when `type=generic`
    - `selector`, `limit`, `source`, `user_agent`, `async`

- `POST /api/scrape/overthecap/teams`
  - Starts the OverTheCap team salary-cap scraper.
  - Body:
    - `seed_url` (optional, default `https://overthecap.com/`)
    - `max_pages` (optional)
    - `enable_team_fallback` (optional, default `true`) when discovery misses teams
    - `include_player_details` (optional, default `true`)
    - `player_detail_limit` (optional, max unique player profile pages to fetch)
    - `user_agent` (optional)
    - `async` (optional, default true)
  - Output artifact is CSV in `live_data`.

- `GET /api/scrape/jobs`
- `GET /api/scrape/jobs/<job_id>`
  - Check async job status.

- `GET /api/scrape/overthecap/teams/<job_id>/download`
  - Returns gzip compressed CSV artifact for OTC jobs.
  - Saved file name for OTC artifacts: `live_NFL_cap_tables.csv`
  - Leave `max_pages` unset to attempt all 32 team pages in one run.

OTC one-liner (from another terminal):

```bash
curl -X POST http://127.0.0.1:5001/api/scrape/overthecap/teams \
  -H "Content-Type: application/json" \
  -d '{"seed_url":"https://overthecap.com/","max_pages":5,"include_player_details":true}'
```

Disable player detail enrichment for a faster, salary-only run:

```bash
curl -X POST http://127.0.0.1:5001/api/scrape/overthecap/teams \
  -H "Content-Type: application/json" \
  -d '{"seed_url":"https://overthecap.com/","max_pages":5,"include_player_details":false}'
```

If discovery misses pages, pass exact `team_urls`:

```bash
curl -X POST http://127.0.0.1:5001/api/scrape/overthecap/teams \
  -H "Content-Type: application/json" \
  -d '{"team_urls":["https://overthecap.com/teams/buf/team-caps","https://overthecap.com/teams/nyj/team-caps"]}'
```

- `GET /files`
- `POST /files`
- `GET /files/<filename>`
  - Generic file upload/list/download for `UPLOAD_DIR`.
- `GET /live-data`
  - List OTC CSV artifacts in `LIVE_DATA_DIR`.

- Scheduler (cron jobs)
  - `POST /api/schedules`
    - `trigger`: `cron` or `interval`
    - `name` optional
    - `scraper_payload`: object passed to scraper at run time (same fields as `POST /api/scrape`)
    - `cron`: cron args (`minute`, `hour`, `day`, `day_of_week`, ...)
    - `interval`: interval args (`seconds`, `minutes`, `hours`, `days`, `weeks`)
  - `GET /api/schedules`
  - `DELETE /api/schedules/<id>`

## Example schedule body

```json
{
  "name": "daily-overthecap",
  "trigger": "cron",
  "scraper_payload": {
    "type": "overthecap_team_csv",
    "include_player_details": true,
    "enable_team_fallback": true
  },
  "cron": {
    "hour": "4",
    "minute": "0"
  }
}
```

Create/replace this schedule now:

```bash
curl -X POST http://127.0.0.1:5001/api/schedules \
  -H "Content-Type: application/json" \
  -d '{"name":"daily-overthecap","trigger":"cron","scraper_payload":{"type":"overthecap_team_csv","include_player_details":true,"enable_team_fallback":true},"cron":{"hour":"4","minute":"0"}}'
```

If running 3rd party tooling uses UTC internally, keep timezone set in `.env` as:
`SCHEDULER_TIMEZONE=America/New_York`

## Notes

- OTC CSV files are written to `live_data/`.
- OTC CSV file name is fixed to `live_NFL_cap_tables.csv` for each run (overwritten on each new OTC job).
- Download endpoint returns gzip-compressed CSV (`application/gzip`) to reduce transfer size.
