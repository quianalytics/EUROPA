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
2. Configure:
   - `cp .env.example .env`
3. Run:
   - `python app.py`

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
    - `user_agent` (optional)
    - `async` (optional, default true)
  - Output artifact is CSV in `live_data`.

- `GET /api/scrape/jobs`
- `GET /api/scrape/jobs/<job_id>`
  - Check async job status.

- `GET /api/scrape/overthecap/teams/<job_id>/download`
  - Returns gzip compressed CSV artifact for OTC jobs.

- `GET /files`
- `POST /files`
- `GET /files/<filename>`
  - Generic file upload/list/download for `UPLOAD_DIR`.

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
    "max_pages": 10
  },
  "cron": {
    "hour": "3",
    "minute": "0"
  }
}
```

## Notes

- OTC CSV files are written to `live_data/`.
- Download endpoint returns gzip-compressed CSV (`application/gzip`) to reduce transfer size.
