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

        fieldnames = self._collect_fieldnames(rows)
        with open(path, "w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                normalized = {}
                for field in fieldnames:
                    value = row.get(field, "")
                    normalized[field] = self._serialize_csv_value(value)
                writer.writerow(normalized)
        return artifact_name

    def read_csv(
        self,
        filename: str,
        artifact_dir: str,
        deserialize_json: bool = False,
    ) -> List[Dict[str, Any]]:
        base_dir = self.live_data_dir if artifact_dir == "live_data" else self.upload_dir
        target = base_dir / filename
        rows: List[Dict[str, Any]] = []
        with open(target, "r", encoding="utf-8", newline="") as input_file:
            reader = csv.DictReader(input_file)
            for row in reader:
                if deserialize_json:
                    for key, value in row.items():
                        if isinstance(value, str) and len(value) > 0:
                            if (value.startswith("{") and value.endswith("}")) or (value.startswith("[") and value.endswith("]")):
                                try:
                                    row[key] = json.loads(value)
                                except json.JSONDecodeError:
                                    pass
                rows.append(row)
        return rows

    def get_path(self, filename: str, artifact_dir: str) -> Path:
        base_dir = self.live_data_dir if artifact_dir == "live_data" else self.upload_dir
        return base_dir / filename

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
            "schema_version",
            "cap_year",
            "team_name",
            "team_slug",
            "team_abbr",
            "team_page_url",
            "player_name",
            "player_position",
            "player_age",
            "player_contract",
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

    @staticmethod
    def _serialize_csv_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, separators=(",", ":"))
        return value
