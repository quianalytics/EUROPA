from __future__ import annotations

from typing import Any, Dict, List
import uuid

from warroom_backend.config import Settings

try:
    from appwrite.client import Client
    from appwrite.services.databases import Databases
    from appwrite.id import ID
except Exception:  # pragma: no cover - optional dependency
    Client = None
    Databases = None
    ID = None


class AppwriteStore:
    def __init__(self, settings: Settings) -> None:
        self.database_id = settings.appwrite_database_id
        self.collection_id = settings.appwrite_collection_id
        self.enabled = bool(
            settings.appwrite_endpoint
            and settings.appwrite_project_id
            and settings.appwrite_api_key
            and self.database_id
            and self.collection_id
        )
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
            self.client.set_endpoint(settings.appwrite_endpoint)
            self.client.set_project(settings.appwrite_project_id)
            self.client.set_key(settings.appwrite_api_key)
            self.databases = Databases(self.client)
        except Exception as exc:  # pragma: no cover
            self.enabled = False
            self._error = str(exc)

    def write_records(self, records: List[Dict[str, Any]]) -> int:
        if not self.enabled:
            return 0

        count = 0
        for record in records:
            try:
                payload = {}
                for key, value in record.items():
                    if value is None:
                        continue
                    payload[key] = value
                self.databases.create_document(
                    self.database_id,
                    self.collection_id,
                    ID.unique() if ID else str(uuid.uuid4()),
                    payload,
                )
                count += 1
            except Exception:  # pragma: no cover
                continue

        return count

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "database_id": self.database_id,
            "collection_id": self.collection_id,
            "error": self._error,
        }
