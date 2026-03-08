from __future__ import annotations

import re
import urllib.parse
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from warroom_backend.scrapers.base import BaseScraper, ScrapeResult
from warroom_backend.utils import utc_now


class OverTheCapTeamScraper(BaseScraper):
    name = "overthecap_team_csv"
    BASE_URL = "https://overthecap.com"
    TEAM_FALLBACK_TEAMS = [
        ("arizona-cardinals", "ari"),
        ("atlanta-falcons", "atl"),
        ("baltimore-ravens", "bal"),
        ("buffalo-bills", "buf"),
        ("carolina-panthers", "car"),
        ("chicago-bears", "chi"),
        ("cincinnati-bengals", "cin"),
        ("cleveland-browns", "cle"),
        ("dallas-cowboys", "dal"),
        ("denver-broncos", "den"),
        ("detroit-lions", "det"),
        ("green-bay-packers", "gb"),
        ("houston-texans", "hou"),
        ("indianapolis-colts", "ind"),
        ("jacksonville-jaguars", "jax"),
        ("kansas-city-chiefs", "kc"),
        ("las-vegas-raiders", "lv"),
        ("los-angeles-chargers", "lac"),
        ("los-angeles-rams", "lar"),
        ("miami-dolphins", "mia"),
        ("minnesota-vikings", "min"),
        ("new-england-patriots", "ne"),
        ("new-orleans-saints", "no"),
        ("new-york-giants", "nyg"),
        ("new-york-jets", "nyj"),
        ("philadelphia-eagles", "phi"),
        ("pittsburgh-steelers", "pit"),
        ("san-francisco-49ers", "sf"),
        ("seattle-seahawks", "sea"),
        ("tampa-bay-buccaneers", "tb"),
        ("tennessee-titans", "ten"),
        ("washington-commanders", "wsh"),
    ]
    TEAM_ABBR_TO_SLUG = {abbr: slug for slug, abbr in TEAM_FALLBACK_TEAMS}
    TEAM_SLUG_TO_ABBR = {slug: abbr for slug, abbr in TEAM_FALLBACK_TEAMS}

    MONEY_FIELDS = {
        "base_salary": {
            "base salary",
            "base",
            "base_salary",
            "base pay",
            "salary",
        },
        "prorated_bonus": {
            "prorated bonus",
            "prorated bonuses",
            "prorated",
            "proratedbonus",
            "prorated_bonus",
        },
        "roster_bonus": {
            "roster bonus",
            "roster",
            "roster_bonus",
        },
        "signing_bonus": {
            "signing bonus",
            "signing",
            "signing_bonus",
        },
        "dead_money": {
            "dead money",
            "dead_money",
            "dead",
        },
        "cap_hit": {
            "cap hit",
            "cap hit cap hit",
            "cap_hit",
            "hit",
            "cap",
            "annual cap hit",
        },
        "cap_number": {
            "cap number",
            "cap #",
            "cap number cap#",
            "capno",
            "cap_number",
        },
        "guaranteed_cash": {
            "guaranteed",
            "guaranteed cash",
            "guaranteed money",
            "guaranteed cash total",
            "guaranteed_cash",
        },
        "prorated_base": {
            "prorated base",
            "proratedbase",
        },
    }
    PROFILE_FLOOR_FIELDS = {
        "player_position": {"position", "pos"},
        "player_age": {"age"},
        "player_height": {"height"},
        "player_weight": {"weight"},
        "player_cap_number": {"cap number", "capnumber"},
        "player_contract": {"contract"},
        "player_contract_length": {"years", "contract length", "years left", "contract years"},
        "player_salary": {"salary"},
        "player_guaranteed_cash": {"guaranteed", "guaranteed cash", "guaranteed money"},
    }
    PLAYER_NAME_FIELD_KEYS = {"player", "player_name", "name", "player name"}
    EXCLUDED_RAW_FIELDS = {
        "cap_dollars_by_college",
        "cap-dollars-by-college",
        "cap dollars by college",
    }
    EXCLUDED_ROW_KEYWORDS = {
        "cap dollars by college",
    }

    SCHEMA_VERSION = "salary_cap_player_v1"

    def run(self, params: Dict[str, Any], timeout: int) -> ScrapeResult:
        user_agent = params.get("user_agent")
        seed_url = params.get("seed_url", f"{self.BASE_URL}/").strip() or f"{self.BASE_URL}/"
        max_pages = self._to_int(params.get("max_pages"))
        explicit_pages = params.get("team_urls")
        enable_fallback_teams = self._to_bool(params.get("enable_team_fallback"), default=True)
        include_player_details = self._to_bool(params.get("include_player_details"), default=True)
        player_detail_limit = self._to_int(params.get("player_detail_limit"))

        headers = {"User-Agent": user_agent or "WarRoomScraper/1.0"}
        if isinstance(explicit_pages, list) and explicit_pages:
            page_urls = [str(url).strip() for url in explicit_pages if str(url).strip()]
        else:
            page_urls = self._collect_team_urls(seed_url, headers, timeout)
            if enable_fallback_teams:
                fallback = self._collect_team_fallback_urls(seed_url)
                page_urls = self._merge_urls(page_urls, fallback)
            if not page_urls:
                page_urls = [seed_url]

        if not page_urls:
            page_urls = [seed_url]

        page_urls = self._normalize_urls(page_urls)
        if max_pages and max_pages > 0:
            page_urls = page_urls[:max_pages]

        player_cache: Dict[str, Dict[str, Any]] = {}
        all_rows: List[Dict[str, Any]] = []
        for page_url in page_urls:
            try:
                response = requests.get(page_url, timeout=timeout, headers=headers)
                response.raise_for_status()
            except Exception:
                continue

            try:
                soup = BeautifulSoup(response.text, "html.parser")
                all_rows.extend(
                    self._extract_team_table_rows(
                        page_url,
                        soup,
                        headers=headers,
                        timeout=timeout,
                        include_player_details=include_player_details,
                        player_detail_limit=player_detail_limit,
                        player_cache=player_cache,
                    )
                )
            except Exception:
                continue

        return ScrapeResult(
            source="overthecap",
            url=seed_url,
            artifact_name_hint="overthecap-teams",
            artifact_kind="csv",
            count=len(all_rows),
            items=all_rows,
        )

    def _normalize_urls(self, urls: List[str]) -> List[str]:
        normalized: List[str] = []
        seen = set()
        for raw in urls:
            if not isinstance(raw, str):
                continue
            value = raw.strip()
            if not value:
                continue

            absolute = urllib.parse.urljoin(self.BASE_URL, value)
            if absolute not in seen:
                seen.add(absolute)
                normalized.append(absolute)

        return normalized

    def _merge_urls(self, first: List[str], second: List[str]) -> List[str]:
        merged: List[str] = []
        seen = set()
        for value in first + second:
            if not value:
                continue
            if value not in seen:
                seen.add(value)
                merged.append(value)
        return merged

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
            if not self._is_team_url(candidate, path, label):
                continue
            if candidate not in seen:
                seen.add(candidate)
                discovered.append(candidate)

        return discovered

    def _collect_team_fallback_urls(self, seed_url: str) -> List[str]:
        fallback: List[str] = []
        seen = set()
        for slug, abbr in self.TEAM_FALLBACK_TEAMS:
            candidate = self._to_absolute(seed_url, f"/salary-cap/{slug}")
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            fallback.append(candidate)
            alt = self._to_absolute(seed_url, f"/teams/{abbr}/team-caps")
            if alt and alt not in seen:
                seen.add(alt)
                fallback.append(alt)
        for _, abbr in self.TEAM_FALLBACK_TEAMS:
            candidate = self._to_absolute(seed_url, f"/teams/{abbr}/team-caps")
            if candidate and candidate not in seen:
                seen.add(candidate)
                fallback.append(candidate)
        return fallback

    def _is_team_url(self, candidate: str, path: str, anchor_label: str) -> bool:
        if not path:
            return False
        if not candidate.startswith(self.BASE_URL):
            return False

        normalized = path.rstrip("/")
        if normalized in ("/", "/salary-cap", "/salary"):
            return False
        if normalized.count("/") == 2 and "/salary-cap/" in normalized:
            return True
        if "/team/" in normalized or "/teams/" in normalized:
            return True
        if normalized.startswith("/salary-cap/"):
            # OTC links usually look like /salary-cap/<team-slug>
            parts = [part for part in normalized.split("/") if part]
            if len(parts) == 2 and parts[0] == "salary-cap":
                return True
            return any(term in normalized for term in ("/team-caps", "/team/"))
        if any(word in anchor_label for word in ("team", "cap", "salary")):
            return True
        return False

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
        terms = [
            "team",
            "salary",
            "cap",
            "space",
            "hit",
            "prorated",
            "spending",
            "base",
            "total",
            "player",
            "guaranteed",
            "bonus",
            "contract",
        ]
        return any(term in header for term in terms)

    def _extract_team_table_rows(
        self,
        team_url: str,
        soup: BeautifulSoup,
        headers: Dict[str, str],
        timeout: int,
        include_player_details: bool,
        player_detail_limit: Optional[int],
        player_cache: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
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
                if not self._looks_like_salary_row(cells):
                    continue

                mapped: Dict[str, Any] = {}
                for idx, cell_value in enumerate(cells):
                    key = header_keys[idx] if idx < len(header_keys) else f"column_{idx+1}"
                    mapped[key] = cell_value

                player_url, player_name_from_link = self._extract_player_link(row, team_url)
                if player_name_from_link:
                    mapped["player"] = player_name_from_link
                if include_player_details and player_url:
                    if player_detail_limit is None or len(player_cache) < player_detail_limit:
                        player_profile = self._extract_player_profile(player_url, headers, timeout, player_cache)
                        if player_profile:
                            mapped["player_profile_url"] = player_url
                            for key, value in player_profile.items():
                                mapped[f"player_{key}"] = value
                        else:
                            mapped["player_profile_url"] = player_url
                    else:
                        mapped["player_profile_url"] = player_url
                        mapped["player_profile_skipped"] = "player_detail_limit_reached"

                elif player_url:
                    mapped["player_profile_url"] = player_url

                rows.append(
                    self._normalize_player_row(
                        team_name_text=team_name_text,
                        team_url=team_url,
                        row=row,
                        table_index=table_index,
                        row_index=row_index,
                        mapped_row=mapped,
                        scraped_at=utc_now(),
                    )
                )

        return rows

    def _looks_like_salary_row(self, cells: List[str]) -> bool:
        if not cells:
            return False

        text = " ".join(cells).lower()
        if any(keyword in text for keyword in self.EXCLUDED_ROW_KEYWORDS):
            return False
        if any(keyword in text for keyword in ("totals", "total", "average", "totalling", "summary")):
            return False
        if not any(re.search(r"\d", value or "") for value in cells):
            return False

        return True

    def _extract_player_link(self, row: Tag, team_url: str) -> Tuple[Optional[str], Optional[str]]:
        for anchor in row.find_all("a", href=True):
            href = anchor.get("href", "").strip()
            if not href:
                continue
            candidate = self._to_absolute(team_url, href)
            if not candidate:
                continue

            path = urllib.parse.urlparse(candidate).path.lower()
            label = (anchor.get_text(" ", strip=True) or "").lower()
            if "/player/" in path or "/players/" in path:
                return candidate, (anchor.get_text(" ", strip=True) or None)
            if not any(term in path for term in ("salary-cap", "team", "teams")) and " " in label:
                # Player names are often plain text links without explicit player path.
                return candidate, (anchor.get_text(" ", strip=True) or None)

        return None, None

    @staticmethod
    def _extract_text(row_value: Dict[str, Any], keys: set[str]) -> Optional[str]:
        for key, value in row_value.items():
            if not value:
                continue
            normalized = OverTheCapTeamScraper._normalize_label(str(key))
            if any(target in normalized for target in keys):
                text = str(value).strip()
                if text:
                    return text
        return None

    @staticmethod
    def _normalize_label(raw: str) -> str:
        normalized = re.sub(r"[^a-z0-9 ]", " ", str(raw).lower())
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def _contains_any(haystack: str, terms: set[str]) -> bool:
        normalized = OverTheCapTeamScraper._normalize_label(haystack)
        return any(term in normalized for term in terms)

    def _normalize_player_row(
        self,
        team_name_text: str,
        team_url: str,
        row: Tag,
        table_index: int,
        row_index: int,
        mapped_row: Dict[str, Any],
        scraped_at: str,
    ) -> Dict[str, Any]:
        player_position = self._extract_text(mapped_row, {"position", "pos"})
        player_age = self._extract_text(mapped_row, {"age"})
        player_contract = self._extract_text(mapped_row, {"contract", "contract length", "years", "years left"})
        player_name = self._clean_text(str(self._extract_text(mapped_row, self.PLAYER_NAME_FIELD_KEYS) or "").strip())
        if not player_name:
            player_name = self._clean_text(row.get_text(" ", strip=True).split("\n", 1)[0])

        team_slug, team_abbr = self._team_identity_from_url(team_url)
        cap_year = self._extract_year(team_name_text) or datetime.utcnow().year

        normalized: Dict[str, Any] = {
            "schema_version": self.SCHEMA_VERSION,
            "team_name": team_name_text,
            "team_slug": team_slug,
            "team_abbr": team_abbr,
            "team_page_url": team_url,
            "cap_year": cap_year,
            "player_name": player_name,
            "player_position": player_position or "",
            "player_age": player_age or "",
            "player_contract": player_contract or "",
            "table_index": table_index,
            "row_index": row_index,
            "scraped_at": scraped_at,
        }

        # Pull known player profile values.
        for src_key, aliases in self.PROFILE_FLOOR_FIELDS.items():
            raw_key = self._extract_text(mapped_row, aliases)
            if raw_key is not None:
                normalized[src_key] = raw_key

        raw_fields: Dict[str, Any] = {}
        for key, value in mapped_row.items():
            if value is None or value == "":
                continue
            normalized_key = self._normalize_label(key)
            if key == "team_name" or key == "player_profile_url" or key == "player_profile_skipped":
                continue
            if normalized_key.startswith("player_"):
                continue

            salary_field = self._map_salary_metric_key(key)
            if salary_field:
                numeric_value = self._parse_money(value)
                if numeric_value is not None:
                    normalized.setdefault(salary_field, numeric_value)
                else:
                    raw_fields[f"{salary_field}_text"] = self._clean_text(str(value))
                continue

            if self._contains_any(normalized_key, {"table", "index", "scraped"}):
                continue
            if normalized_key in self.EXCLUDED_RAW_FIELDS:
                continue
            if normalized_key.replace("_", " ") in self.EXCLUDED_RAW_FIELDS:
                continue

            raw_fields[key] = self._clean_text(str(value))

        if raw_fields:
            normalized["raw_fields"] = raw_fields

        for salary_field in self.MONEY_FIELDS:
            normalized.setdefault(salary_field, None)

        return normalized

    def _extract_year(self, text: str) -> Optional[int]:
        match = re.search(r"\b(20\d{2})\b", text or "")
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    def _team_identity_from_url(self, team_url: str) -> Tuple[str, Optional[str]]:
        path = urllib.parse.urlparse(team_url).path.strip("/")
        parts = [part for part in path.split("/") if part]
        slug: Optional[str] = None
        abbr: Optional[str] = None
        if len(parts) >= 2 and parts[0] == "salary-cap":
            slug = parts[1].lower()
            abbr = self.TEAM_SLUG_TO_ABBR.get(slug)
        elif len(parts) >= 3 and parts[0] == "teams":
            abbr = parts[1].lower()
            slug = self.TEAM_ABBR_TO_SLUG.get(abbr)
        if not slug:
            slug = path.split("/")[-1].lower().replace("team-caps", "").strip("-")
        if not abbr:
            abbr = self.TEAM_SLUG_TO_ABBR.get(slug)
        return slug, abbr

    def _map_salary_metric_key(self, raw_header: str) -> Optional[str]:
        normalized = self._normalize_label(raw_header)
        if not normalized:
            return None
        for canonical, aliases in self.MONEY_FIELDS.items():
            if any(alias in normalized for alias in aliases):
                return canonical
        return None

    @staticmethod
    def _parse_money(value: Any) -> Optional[float]:
        text = OverTheCapTeamScraper._clean_text(str(value))
        if not text:
            return None
        if "$" not in text and "," not in text and not any(ch.isdigit() for ch in text):
            return None

        cleaned = text.replace("$", "").replace(",", "").strip()
        cleaned = cleaned.replace("—", "").replace("-", "").strip()
        if not cleaned:
            return None

        # Handle parenthesized negatives and trailing symbols like 'M' (million) if present.
        multiplier = 1.0
        if cleaned.endswith("m"):
            cleaned = cleaned[:-1]
            multiplier = 1_000_000.0
        elif cleaned.endswith("mm"):
            cleaned = cleaned[:-2]
            multiplier = 1_000_000.0

        cleaned = cleaned.strip()
        if not cleaned:
            return None
        try:
            return float(cleaned) * multiplier
        except ValueError:
            return None

    def _extract_player_profile(
        self,
        player_url: str,
        headers: Dict[str, str],
        timeout: int,
        player_cache: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        if player_url in player_cache:
            return player_cache[player_url]

        try:
            response = requests.get(player_url, timeout=timeout, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            player_data: Dict[str, Any] = {}

            name_tag = soup.find("h1")
            if name_tag:
                player_data["name"] = name_tag.get_text(" ", strip=True)
            player_data.update(self._collect_player_text_pairs(soup))

            if not player_data:
                player_data = {}
            player_cache[player_url] = player_data
            return player_data
        except Exception:
            return {}

    def _collect_player_text_pairs(self, soup: BeautifulSoup) -> Dict[str, Any]:
        pairs: Dict[str, Any] = {}

        # Parse definition lists.
        for dt in soup.find_all("dt"):
            key = self._clean_text(dt.get_text(" ", strip=True).rstrip(":"))
            if not key:
                continue
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            value = self._clean_text(dd.get_text(" ", strip=True))
            if key and value:
                pairs[key] = value

        # Parse two-column rows in generic tables.
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = self._clean_text(cells[0].get_text(" ", strip=True).rstrip(":"))
            value = self._clean_text(" ".join(cells[1].stripped_strings))
            if not key or not value:
                continue
            if key.lower() in {"player", "name", "team", "position", "age", "height", "weight", "cap number", "contract", "years", "years left"}:
                pairs[key.lower().replace(" ", "_")] = value
            else:
                if self._looks_like_label_value_pair(key, value):
                    pairs[key.lower().replace(" ", "_")] = value

        # Parse <li><strong>Label:</strong> value</li> patterns.
        for item in soup.find_all("li"):
            strong = item.find("strong")
            if not strong:
                continue
            label = self._clean_text(strong.get_text(" ", strip=True).rstrip(":"))
            if not label:
                continue
            full_text = self._clean_text(item.get_text(" ", strip=True))
            value = full_text.replace(strong.get_text(" ", strip=True), "", 1).strip(" :|-")
            if not value:
                continue
            pairs[label.lower().replace(" ", "_")] = value

        return pairs

    @staticmethod
    def _looks_like_label_value_pair(label: str, value: str) -> bool:
        if not label or not value:
            return False
        if len(label) > 70 or len(value) > 300:
            return False
        if label.lower() in {"player", "name", "team", "cap", "base salary", "prorated bonus"}:
            return False
        return True

    @staticmethod
    def _clean_text(value: str) -> str:
        cleaned = re.sub(r"\s+", " ", (value or "").strip())
        if cleaned.lower() in {"$", "", "none", "n/a", "na"}:
            return ""
        return cleaned

    @staticmethod
    def _to_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
        return bool(value)
