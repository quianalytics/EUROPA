# WarRoom Flask Backend

A small Flask backend that:
- accepts scrape jobs
- writes scraped records to Appwrite
- stores job artifacts as JSON files
- serves uploaded/scraped files

## Quick start

1. Create a virtual environment and install dependencies:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -r requirements.txt`
2. Copy environment file:
   - `cp .env.example .env`
   - fill Appwrite settings if needed
3. Run:
   - `python app.py`

## Endpoints

- `GET /health`
  - health info and Appwrite readiness

- `POST /api/scrape`
  - JSON body:
    - `url` (required)
    - `selector` (optional, default: `a`)
    - `source` (optional)
    - `limit` (optional)
    - `user_agent` (optional)
    - `async` (optional, default `true`)
  - starts a scrape job, returns `job_id`

- `GET /api/scrape/jobs`
  - returns all known job statuses

- `GET /api/scrape/jobs/<job_id>`
  - returns one job status including output artifact path

- `GET /files`
  - lists files in `UPLOAD_DIR`

- `POST /files`
  - multipart form upload with field `file`

- `GET /files/<filename>`
  - serves/downloads the file

## Notes

- The scraper stores one Appwrite document per extracted node.
- Scraped content and Appwrite writes are best-effort in this starter version; network hiccups fail only that job, not the server.
- Replace the default selector logic with your scraper rules as your WarRoom needs grow.
