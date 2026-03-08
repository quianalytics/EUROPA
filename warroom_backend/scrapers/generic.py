from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from warroom_backend.scrapers.base import BaseScraper, ScrapeResult
from warroom_backend.utils import utc_now


class GenericScraper(BaseScraper):
    name = "generic"

    def run(self, params: Dict[str, Any], timeout: int) -> ScrapeResult:
        url = params.get("url", "").strip()
        if not url:
            raise ValueError("Missing required field 'url'")

        selector = (params.get("selector") or "a").strip() or "a"
        limit = self._to_int(params.get("limit"))
        source = params.get("source", "generic")
        user_agent = params.get("user_agent")

        headers = {"User-Agent": user_agent or "WarRoomScraper/1.0"}
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
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

        return ScrapeResult(
            source=source,
            url=url,
            artifact_name_hint="scrape",
            artifact_kind="json",
            count=len(items),
            items=items,
        )
