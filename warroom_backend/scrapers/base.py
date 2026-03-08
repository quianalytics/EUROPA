from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ScrapeResult:
    source: str
    url: str
    artifact_name_hint: str
    artifact_kind: str  # json or csv
    count: int
    items: List[Dict[str, Any]]


class BaseScraper:
    name = "base"

    def run(self, params: Dict[str, Any], timeout: int) -> ScrapeResult:
        raise NotImplementedError

    @staticmethod
    def _to_int(value: Optional[Any]) -> Optional[int]:
        if value is None:
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise ValueError("invalid integer")
        if parsed < 1:
            raise ValueError("integer must be greater than zero")
        return parsed
