from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List


class StorageService:
    def __init__(self, upload_dir: Path, live_data_dir: Path) -> None:
        self.upload_dir = upload_dir
        self.live_data_dir = live_data_dir
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.live_data_dir.mkdir(parents=True, exist_ok=True)

    def write_json(self, job_id: str, source: str, payload: Any) -> str:
        filename = f"{source}-{job_id}.json"
        path = self.upload_dir / filename
        with open(path, "w", encoding="utf-8") as output:
            json.dump(payload, output, indent=2)
        return filename

    def write_csv(
        self,
        job_id: str,
        rows: List[Dict[str, Any]],
        base_name: str = "overthecap-teams",
        filename: str | None = None,
    ) -> str:
        artifact_name = filename if filename else f"{base_name}-{job_id}.csv"
        path = self.live_data_dir / artifact_name
        path = self.live_data_dir / filename

        fieldnames = self._collect_fieldnames(rows)
        with open(path, "w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                normalized = {}
                for field in fieldnames:
                    value = row.get(field, "")
                    normalized[field] = value
                writer.writerow(normalized)
        return artifact_name

    def read_binary(self, filename: str, artifact_dir: str) -> bytes:
        base_dir = self.live_data_dir if artifact_dir == "live_data" else self.upload_dir
        target = base_dir / filename
        return target.read_bytes()

    def exists(self, filename: str, artifact_dir: str) -> bool:
        base_dir = self.live_data_dir if artifact_dir == "live_data" else self.upload_dir
        target = base_dir / filename
        return target.exists() and target.is_file()

    @staticmethod
    def _collect_fieldnames(rows: List[Dict[str, Any]]) -> List[str]:
        ordered = [
            "team_name",
            "table_index",
            "row_index",
            "scraped_at",
        ]
        dynamic: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in ordered and key not in dynamic:
                    dynamic.append(key)
        return ordered + sorted(dynamic)
