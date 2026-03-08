from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import threading
import uuid
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request, send_from_directory, abort
from werkzeug.utils import secure_filename

try:
    from appwrite.client import Client
    from appwrite.services.databases import Databases
    from appwrite.id import ID
except Exception:  # pragma: no cover - optional dependency
    Client = None
    Databases = None
    ID = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ScrapeResult:
    source: str
    url: str
    selector: str
    count: int
    items: List[Dict[str, Any]]


class AppwriteStore:
    """Minimal Appwrite sync wrapper with graceful fallback when Appwrite is unavailable."""

    def __init__(self) -> None:
        endpoint = os.getenv("APPWRITE_ENDPOINT", "").strip()
        project = os.getenv("APPWRITE_PROJECT_ID", "").strip()
        api_key = os.getenv("APPWRITE_API_KEY", "").strip()
        self.database_id = os.getenv("APPWRITE_DATABASE_ID", "").strip()
        self.collection_id = os.getenv("APPWRITE_COLLECTION_ID", "").strip()

        self.enabled = bool(endpoint and project and api_key and self.database_id and self.collection_id)
        self.client = None
        self.databases = None
        self._error = None

        if not self.enabled:
            return

        if Client is None or Databases is None:
            self.enabled = False
            self._error = "appwrite package not installed"
            return

        try:
            self.client = Client()
            self.client.set_endpoint(endpoint)
            self.client.set_project(project)
            self.client.set_key(api_key)
            self.databases = Databases(self.client)
        except Exception as exc:  # pragma: no cover - runtime environment dependent
            self.enabled = False
            self._error = str(exc)

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        if not self.enabled:
            return 0

        count = 0
        for record in records:
            try:
                payload = {
                    "source": record.get("source"),
                    "url": record.get("url"),
                    "selector": record.get("selector"),
                    "title": record.get("title"),
                    "index": record.get("index"),
                    "value": record.get("value"),
                    "value_hash": record.get("value_hash"),
                    "scraped_at": record.get("scraped_at"),
                    "job_id": record.get("job_id"),
                }
                doc_id = ID.unique() if ID else str(uuid.uuid4())
                self.databases.create_document(
                    self.database_id,
                    self.collection_id,
                    doc_id,
                    payload,
                )
                count += 1
            except Exception:  # pragma: no cover - external service errors are transient
                continue
        return count

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "database_id": self.database_id,
            "collection_id": self.collection_id,
            "error": self._error,
        }


class ScraperManager:
    def __init__(self, appwrite_store: AppwriteStore, upload_dir: Path) -> None:
        self.appwrite = appwrite_store
        self.upload_dir = upload_dir
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.jobs_lock = threading.Lock()

    def sanitize_selector(self, selector: str) -> str:
        return selector if selector else "a"

    def scrape(self, url: str, source: str, selector: str, limit: Optional[int], user_agent: Optional[str]) -> ScrapeResult:
        hdrs = {"User-Agent": user_agent or "WarRoomScraper/1.0"}
        response = requests.get(url, timeout=int(os.getenv("SCRAPER_TIMEOUT", "15")), headers=hdrs)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        selector = self.sanitize_selector(selector)
        nodes = soup.select(selector)
        if limit is not None:
            nodes = nodes[:limit]

        values: List[str] = []
        for node in nodes:
            if hasattr(node, "get_text"):
                text = node.get_text(separator=" ", strip=True)
                if text:
                    values.append(text)

        page_title = soup.title.get_text(strip=True) if soup.title else ""
        items = [
            {
                "source": source,
                "url": url,
                "selector": selector,
                "index": idx,
                "title": page_title,
                "value": value,
                "value_hash": hashlib.sha1(value.encode("utf-8")).hexdigest(),
                "scraped_at": utc_now(),
            }
            for idx, value in enumerate(values)
        ]

        return ScrapeResult(source=source, url=url, selector=selector, count=len(items), items=items)

    def run_job(self, job_id: str, params: Dict[str, Any]) -> None:
        try:
            result = self.scrape(
                url=params["url"],
                source=params.get("source", "default"),
                selector=params.get("selector", "a"),
                limit=params.get("limit"),
                user_agent=params.get("user_agent"),
            )

            payloads = []
            for item in result.items:
                row = dict(item)
                row["job_id"] = job_id
                payloads.append(row)

            appwrite_written = self.appwrite.write_records(payloads)

            artifact_name = f"scrape-{job_id}.json"
            artifact_path = self.upload_dir / artifact_name
            with open(artifact_path, "w", encoding="utf-8") as output:
                json.dump(
                    {
                        "source": result.source,
                        "url": result.url,
                        "selector": result.selector,
                        "count": result.count,
                        "items": result.items,
                        "created_at": utc_now(),
                    },
                    output,
                    indent=2,
                )

            with self.jobs_lock:
                self.jobs[job_id].update(
                    {
                        "status": "completed",
                        "finished_at": utc_now(),
                        "count": result.count,
                        "appwrite_written": appwrite_written,
                        "artifact": artifact_name,
                    }
                )
        except Exception as exc:  # pragma: no cover - external network failures are expected
            with self.jobs_lock:
                self.jobs[job_id].update({
                    "status": "failed",
                    "finished_at": utc_now(),
                    "error": str(exc),
                })

    def enqueue(self, params: Dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        with self.jobs_lock:
            self.jobs[job_id] = {
                "id": job_id,
                "status": "running",
                "created_at": utc_now(),
                "input": params,
            }

        if params.get("async", True):
            self.executor.submit(self.run_job, job_id, params)
            return job_id

        self.run_job(job_id, params)
        return job_id

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self.jobs_lock:
            return list(self.jobs.values())

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.jobs_lock:
            return self.jobs.get(job_id)


def create_app() -> Flask:
    app = Flask(__name__)

    upload_dir = Path(os.getenv("UPLOAD_DIR", "./files")).resolve()
    upload_dir.mkdir(parents=True, exist_ok=True)

    appwrite_store = AppwriteStore()
    scraper_manager = ScraperManager(appwrite_store=appwrite_store, upload_dir=upload_dir)

    @app.get("/health")
    def health() -> Any:
        return {
            "ok": True,
            "appwrite": appwrite_store.status(),
            "jobs": len(scraper_manager.list_jobs()),
        }

    @app.post("/api/scrape")
    def start_scrape():
        body = request.get_json(silent=True) or {}
        if not body.get("url"):
            return jsonify({"error": "Missing required field 'url'"}), 400

        try:
            body["limit"] = int(body["limit"]) if body.get("limit") is not None else None
            if body["limit"] is not None and body["limit"] < 1:
                return jsonify({"error": "limit must be a positive integer"}), 400
        except (TypeError, ValueError):
            return jsonify({"error": "limit must be a positive integer"}), 400

        body.setdefault("source", "generic")
        body.setdefault("selector", "a")
        body.setdefault("async", True)
        body.setdefault("user_agent", None)

        job_id = scraper_manager.enqueue(body)
        return jsonify({"job_id": job_id, "status": scraper_manager.get_job(job_id)["status"]}), 202

    @app.get("/api/scrape/jobs")
    def list_scrape_jobs():
        return jsonify({"jobs": scraper_manager.list_jobs()})

    @app.get("/api/scrape/jobs/<job_id>")
    def get_scrape_job(job_id: str):
        job = scraper_manager.get_job(job_id)
        if not job:
            return jsonify({"error": "job not found"}), 404
        return jsonify(job)

    @app.get("/files")
    def list_files():
        files = []
        for file_path in sorted(upload_dir.iterdir()):
            if not file_path.is_file():
                continue
            stat = file_path.stat()
            files.append(
                {
                    "name": file_path.name,
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
        return jsonify({"files": files})

    @app.post("/files")
    def upload_file():
        uploaded = request.files.get("file")
        if not uploaded or not uploaded.filename:
            return jsonify({"error": "No file uploaded"}), 400

        filename = secure_filename(uploaded.filename)
        if not filename:
            return jsonify({"error": "Invalid file name"}), 400

        destination = upload_dir / filename
        uploaded.save(destination)
        return jsonify({"name": filename, "size": destination.stat().st_size}), 201

    @app.get("/files/<path:filename>")
    def get_file(filename: str):
        safe_name = secure_filename(filename)
        target = upload_dir / safe_name
        if not target.exists() or not target.is_file():
            abort(404)
        return send_from_directory(str(upload_dir), safe_name, as_attachment=True)

    return app


app = create_app()

if __name__ == "__main__":  # pragma: no cover
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")
    app.run(host=host, port=port, debug=debug)
