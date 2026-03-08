from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional
import uuid

from warroom_backend.services.appwrite_store import AppwriteStore
from warroom_backend.services.storage import StorageService
from warroom_backend.scrapers.base import BaseScraper, ScrapeResult
from warroom_backend.scrapers.registry import ScraperRegistry
from warroom_backend.utils import utc_now


class JobManager:
    def __init__(self, appwrite_store: AppwriteStore, storage: StorageService, timeout: int, max_workers: int = 2) -> None:
        self.appwrite = appwrite_store
        self.storage = storage
        self.timeout = timeout
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.scraper_registry = ScraperRegistry()
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.jobs_lock = threading.Lock()

    def _get_scraper(self, scraper_type: str) -> BaseScraper:
        scraper = self.scraper_registry.get(scraper_type)
        if scraper is None:
            raise ValueError(f"Unknown scraper type: {scraper_type}")
        return scraper

    def enqueue(self, params: Dict[str, Any], async_mode: bool = True) -> str:
        job_id = str(uuid.uuid4())
        with self.jobs_lock:
            self.jobs[job_id] = {
                "id": job_id,
                "status": "running",
                "created_at": utc_now(),
                "input": params,
            }

        if async_mode:
            self.executor.submit(self.run_job, job_id, params)
            return job_id
        self.run_job(job_id, params)
        return job_id

    def run_job(self, job_id: str, params: Dict[str, Any]) -> None:
        try:
            scraper_type = params.get("type", "generic")
            scraper: BaseScraper = self._get_scraper(scraper_type)
            result: ScrapeResult = scraper.run(params, timeout=self.timeout)

            payloads = []
            for item in result.items:
                row = dict(item)
                row["job_id"] = job_id
                payloads.append(row)
            appwrite_written = self.appwrite.write_records(payloads)

            if result.artifact_kind == "csv":
                artifact_name = None
                if result.source == "overthecap":
                    artifact_name = "live_NFL_cap_tables.csv"
                artifact = self.storage.write_csv(
                    job_id,
                    result.items,
                    base_name=result.artifact_name_hint,
                    filename=artifact_name,
                )
                artifact_dir = "live_data"
            else:
                artifact_payload = {
                    "source": result.source,
                    "url": result.url,
                    "count": result.count,
                    "items": result.items,
                    "created_at": __import__("warroom_backend.utils", fromlist=["utc_now"]).utc_now(),
                }
                artifact = self.storage.write_json(job_id, result.artifact_name_hint, artifact_payload)
                artifact_dir = "files"

            with self.jobs_lock:
                self.jobs[job_id].update(
                    {
                        "status": "completed",
                        "finished_at": __import__("warroom_backend.utils", fromlist=["utc_now"]).utc_now(),
                        "count": result.count,
                        "appwrite_written": appwrite_written,
                        "artifact": artifact,
                        "artifact_dir": artifact_dir,
                        "source": result.source,
                        "url": result.url,
                        "scraper_type": scraper_type,
                    }
                )
        except Exception as exc:  # pragma: no cover
            with self.jobs_lock:
                self.jobs[job_id].update(
                    {
                        "status": "failed",
                        "finished_at": utc_now(),
                        "error": str(exc),
                    }
                )

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.jobs_lock:
            return self.jobs.get(job_id)

    def list_jobs(self) -> List[Dict[str, Any]]:
        with self.jobs_lock:
            return list(self.jobs.values())
