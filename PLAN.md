# WarRoom Backend Python - Project Plan

## Current architecture (implemented)

- Flask app factory in `warroom_backend/app.py`.
- Explicit package structure for easy scraper expansion:
  - `warroom_backend/scrapers/` (one class per scraper).
  - `warroom_backend/jobs/` (job execution and state).
  - `warroom_backend/scheduler/` (cron/interval schedule orchestration).
  - `warroom_backend/services/` (Appwrite + storage abstractions).
- Entry point remains `app.py` in repo root for local execution.
- OTC scraper now supports player-profile enrichment (`include_player_details`) with optional profile fetch cap (`player_detail_limit`) and shared profile caching per run.

## Runtime flow

1. API call creates a scrape request payload.
2. `JobManager` enqueues it, marks job `running`, and dispatches worker thread.
3. Selected scraper runs and returns normalized records + artifact hints.
4. Job manager persists records to Appwrite (if configured).
5. Artifacts are persisted:
   - Generic scraper -> JSON in `UPLOAD_DIR`
   - OverTheCap scraper -> CSV in `LIVE_DATA_DIR` (`live_data`)
6. OTC completion can be fetched via `/api/scrape/overthecap/teams/<job_id>/download` as gzip.
7. Scheduler jobs (`/api/schedules`) trigger repeat calls to the same job manager pipeline.
- OTC artifact filename is fixed to `live_NFL_cap_tables.csv` in `live_data`.
- OTC rows can now include flattened player fields (`player_position`, `player_age`, `player_contract`, etc., depending on site data availability) from linked player pages.
- OTC team discovery now includes a 32-team fallback URL seed and optional `enable_team_fallback` toggle.
- Scheduler default timezone moved to `America/New_York`; OTC schedule can be set as a daily 4:00 AM ET cron job via `/api/schedules`.

## Current files

- `warroom_backend/app.py`
- `warroom_backend/config.py`
- `warroom_backend/utils.py`
- `warroom_backend/scrapers/base.py`
- `warroom_backend/scrapers/generic.py`
- `warroom_backend/scrapers/overthecap.py`
- `warroom_backend/scrapers/registry.py`
- `warroom_backend/jobs/manager.py`
- `warroom_backend/services/appwrite_store.py`
- `warroom_backend/services/storage.py`
- `warroom_backend/scheduler/manager.py`
- `app.py`
- `requirements.txt`, `.env.example`, `.gitignore`, `README.md`, `PLAN.md`

## Planned improvements

- Add per-scraper route-level defaults and config files.
- Add retry policy and exponential backoff for flaky scrapers.
- Persist job history beyond in-memory runtime.
- Add scraper auth/secrets management and request auth middleware.
- Add OpenAPI/Swagger docs.
- Add a scraper health/validation endpoint returning row-count + page discovery telemetry to speed up debugging if a scraper returns zero rows.
