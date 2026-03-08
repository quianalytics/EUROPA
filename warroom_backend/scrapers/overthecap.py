from __future__ import annotations

import urllib.parse
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from warroom_backend.scrapers.base import BaseScraper, ScrapeResult
from warroom_backend.utils import utc_now


class OverTheCapTeamScraper(BaseScraper):
    name = "overthecap_team_csv"
    BASE_URL = "https://overthecap.com"

    def run(self, params: Dict[str, Any], timeout: int) -> ScrapeResult:
        user_agent = params.get("user_agent")
        seed_url = params.get("seed_url", f"{self.BASE_URL}/").strip() or f"{self.BASE_URL}/"
        max_pages = self._to_int(params.get("max_pages"))

        headers = {"User-Agent": user_agent or "WarRoomScraper/1.0"}
        page_urls = self._collect_team_urls(seed_url, headers, timeout)
        if not page_urls:
            page_urls = [seed_url]
        if max_pages and max_pages > 0:
            page_urls = page_urls[:max_pages]

        all_rows: List[Dict[str, Any]] = []
        for page_url in page_urls:
            response = requests.get(page_url, timeout=timeout, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            all_rows.extend(self._extract_team_table_rows(page_url, soup))

        for row in all_rows:
            row.setdefault("source", "overthecap_team_salary_caps")
        return ScrapeResult(
            source="overthecap",
            url=seed_url,
            artifact_name_hint="overthecap-teams",
            artifact_kind="csv",
            count=len(all_rows),
            items=all_rows,
        )

    def _collect_team_urls(self, home_url: str, headers: Dict[str, str], timeout: int) -> List[str]:
        response = requests.get(home_url, timeout=timeout, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        discovered: List[str] = []
        seen = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor.get("href", "").strip()
            candidate = self._to_absolute(home_url, href)
            if not candidate:
                continue

            path = urllib.parse.urlparse(candidate).path.lower()
            label = (anchor.get_text(" ", strip=True) or "").lower()
            if "/team" not in path and "/teams" not in path and "/salary" not in path:
                continue
            if not any(word in path for word in ("salary", "cap")):
                continue
            if (
                any(word in label for word in ("team", "salary", "cap", "salary cap", "cap room"))
                or "/team" in path
                or "/teams" in path
            ):
                if candidate not in seen:
                    seen.add(candidate)
                    discovered.append(candidate)

        return discovered

    def _to_absolute(self, base_url: str, href: str) -> Optional[str]:
        try:
            absolute = urllib.parse.urljoin(base_url, href)
        except Exception:
            return None

        if absolute.startswith("//"):
            absolute = f"https:{absolute}"
        if not absolute.startswith(self.BASE_URL):
            return None
        parsed = urllib.parse.urlparse(absolute)
        if not parsed.scheme or not parsed.netloc:
            return None
        return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    def _is_salary_related_table(self, header_values: List[str]) -> bool:
        if not header_values:
            return False
        header = " ".join([h.lower() for h in header_values if h]).strip()
        if not header:
            return False
        terms = ["team", "salary", "cap", "space", "hit", "prorated", "spending", "base", "total"]
        return any(term in header for term in terms)

    def _extract_team_table_rows(self, team_url: str, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        team_name_tag = soup.find("h1")
        team_name_text = team_name_tag.get_text(" ", strip=True) if team_name_tag else urllib.parse.urlparse(team_url).path.strip("/").split("/")[-1]

        rows: List[Dict[str, Any]] = []
        for table_index, table in enumerate(soup.find_all("table")):
            table_rows = table.find_all("tr")
            if not table_rows:
                continue

            headers = [cell.get_text(" ", strip=True) for cell in table_rows[0].find_all(["th", "td"])]
            if not self._is_salary_related_table(headers):
                continue

            header_keys = [h if h else f"column_{idx+1}" for idx, h in enumerate(headers)]
            for row_index, row in enumerate(table_rows[1:], start=1):
                cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["th", "td"])]
                if not any(cells) or len(cells) < 2:
                    continue

                mapped: Dict[str, Any] = {}
                for idx, cell_value in enumerate(cells):
                    key = header_keys[idx] if idx < len(header_keys) else f"column_{idx+1}"
                    mapped[key] = cell_value

                mapped.update(
                    {
                        "team_name": team_name_text,
                        "source_page": team_url,
                        "table_index": table_index,
                        "row_index": row_index,
                        "scraped_at": utc_now(),
                    }
                )
                rows.append(mapped)

        return rows
