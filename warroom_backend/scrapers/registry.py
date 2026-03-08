from __future__ import annotations

from typing import Dict, Optional

from warroom_backend.scrapers.base import BaseScraper
from warroom_backend.scrapers.generic import GenericScraper
from warroom_backend.scrapers.overthecap import OverTheCapTeamScraper


class ScraperRegistry:
    def __init__(self) -> None:
        self._scrapers: Dict[str, BaseScraper] = {
            GenericScraper.name: GenericScraper(),
            OverTheCapTeamScraper.name: OverTheCapTeamScraper(),
        }

    def get(self, name: str) -> Optional[BaseScraper]:
        return self._scrapers.get(name)

    def available(self):
        return sorted(self._scrapers.keys())
