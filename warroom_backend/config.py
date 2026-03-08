from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict


def _to_bool(value: str | None) -> bool:
    return str(value or "").lower() in {"1", "true", "yes", "y", "on"}


class Settings:
    def __init__(self) -> None:
        self.host: str = os.getenv("HOST", "0.0.0.0")
        self.port: int = int(os.getenv("PORT", "5000"))
        self.debug: bool = _to_bool(os.getenv("DEBUG", "false"))
        self.scraper_timeout: int = int(os.getenv("SCRAPER_TIMEOUT", "15"))
        self.upload_dir: Path = Path(os.getenv("UPLOAD_DIR", "./files")).resolve()
        self.live_data_dir: Path = Path(os.getenv("LIVE_DATA_DIR", "./live_data")).resolve()
        self.scheduler_timezone: str = os.getenv("SCHEDULER_TIMEZONE", "UTC")

        self.appwrite_endpoint: str = os.getenv("APPWRITE_ENDPOINT", "").strip()
        self.appwrite_project_id: str = os.getenv("APPWRITE_PROJECT_ID", "").strip()
        self.appwrite_api_key: str = os.getenv("APPWRITE_API_KEY", "").strip()
        self.appwrite_database_id: str = os.getenv("APPWRITE_DATABASE_ID", "").strip()
        self.appwrite_collection_id: str = os.getenv("APPWRITE_COLLECTION_ID", "").strip()

        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.live_data_dir.mkdir(parents=True, exist_ok=True)

    def appwrite_config(self) -> Dict[str, Any]:
        return {
            "endpoint": self.appwrite_endpoint,
            "project_id": self.appwrite_project_id,
            "api_key": self.appwrite_api_key,
            "database_id": self.appwrite_database_id,
            "collection_id": self.appwrite_collection_id,
        }
