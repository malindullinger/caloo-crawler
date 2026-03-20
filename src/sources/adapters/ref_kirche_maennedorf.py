"""
Ref. Kirche Männedorf adapter — TYPO3 + lpc_kool_events (Lauper Computing).

Strategy:
- Fetch /agenda/ listing page (single page, no pagination, ~136 events)
- Discover per-event ICS download links (a.goIcs)
- Download each ICS file, parse VEVENT fields
- Classify relevance via ICS SUMMARY category prefix
- Return only INCLUDE + REVIEW events

Classification: Tier A (ICS structured data — RFC 5545 VEVENT)
Platform: TYPO3 + lpc_kool_events (Lauper Computing)

Relevance filtering:
- ICS SUMMARY format: "Category: Event Title"
- INCLUDE categories: family/child/youth/all-ages events
- EXCLUDE categories: seniors, adult-only, generic worship, volunteers
- REVIEW categories: borderline (cultural, some worship styles)
"""
from __future__ import annotations

import re
import time
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..base import BaseAdapter
from ..http import http_get
from ..types import SourceConfig, ExtractedItem

# Base URL for resolving relative ICS hrefs (NOT the agenda page —
# ICS hrefs are relative like "agenda/?tx_lpckoolevents_..." and
# urljoin with /agenda/ would double the path to /agenda/agenda/?...).
_SITE_ROOT = "https://www.ref-maennedorf.ch/"

# Relevance categories extracted from full ICS audit (136 events, 2026-03-15).
# Categories appear as prefix in ICS SUMMARY: "Category: Event Title"
_INCLUDE_CATEGORIES = frozenset({
    "Für alle",
    "chile für chind",
    "jugend. kirche.",
    "Familien",
    "Gottesdienste - Mitenand",
})

_REVIEW_CATEGORIES = frozenset({
    "Gottesdienste - Grenzenlos",
    "Gottesdienste - Highfive",
    "Kulturelles",
})

# Everything else is EXCLUDE (Gottesdienste - Classic, Senioren, Erwachsene,
# Männer, Frauen, Freiwillige, Gottesdienste - Ökumene, etc.)


def _extract_vevent(ics_text: str) -> Optional[str]:
    """Extract the VEVENT block from ICS text.

    ICS files contain VTIMEZONE blocks with their own DTSTART fields
    (DST transition rules like DTSTART:19700329T020000). We must parse
    only from the VEVENT block to avoid picking up timezone fields.
    """
    m = re.search(r"BEGIN:VEVENT\r?\n(.*?)END:VEVENT", ics_text, re.DOTALL)
    return m.group(1) if m else None


def _parse_ics_field(vevent_text: str, field: str) -> Optional[str]:
    """Extract a field value from VEVENT block text.

    Handles parameterized fields like DTSTART;TZID=...:value.
    Handles line unfolding (RFC 5545 §3.1: continuation lines start with space/tab).
    """
    pattern = re.compile(
        rf"^{re.escape(field)}(?:;[^:]+)?:(.+)$",
        re.MULTILINE,
    )
    m = pattern.search(vevent_text)
    if not m:
        return None

    value = m.group(1).rstrip("\r")

    # Handle line unfolding: continuation lines start with a single space or tab
    end_pos = m.end()
    remaining = vevent_text[end_pos:]
    for line in remaining.split("\n"):
        stripped = line.rstrip("\r")
        if stripped.startswith(" ") or stripped.startswith("\t"):
            value += stripped[1:]
        else:
            break

    return value.strip() if value else None


def _ics_datetime_to_iso(dtval: str, vevent_text: str, field: str) -> Optional[str]:
    """Convert ICS datetime value to ISO 8601.

    ICS formats:
    - 20260315T140000 (local, TZID from field parameter)
    - 20260315T140000Z (UTC)
    - 20260315 (date-only)
    """
    if not dtval:
        return None

    dtval = dtval.strip()

    # Date-only: 20260315
    if re.match(r"^\d{8}$", dtval):
        return f"{dtval[:4]}-{dtval[4:6]}-{dtval[6:8]}"

    # DateTime: 20260315T140000 or 20260315T140000Z
    m = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(Z)?$", dtval)
    if not m:
        return None

    y, mo, d, h, mi, s, utc_flag = m.groups()
    iso = f"{y}-{mo}-{d}T{h}:{mi}:{s}"

    if utc_flag:
        iso += "+00:00"
    # Else: naive ISO — normalizer defaults to Europe/Zurich

    return iso


def _extract_category_and_title(summary: str) -> tuple[str, str]:
    """Split ICS SUMMARY into category prefix and event title.

    Format: "Category: Event Title"
    Returns (category, title). If no colon, category="" and title=summary.
    """
    if not summary:
        return "", ""

    if ": " in summary:
        category, title = summary.split(": ", 1)
        return category.strip(), title.strip()

    return "", summary.strip()


def _classify_relevance(category: str) -> str:
    """Classify event relevance based on ICS SUMMARY category prefix.

    Returns: "include", "exclude", or "review"
    """
    if not category:
        return "review"

    if category in _INCLUDE_CATEGORIES:
        return "include"
    if category in _REVIEW_CATEGORIES:
        return "review"
    return "exclude"


def _parse_ics_organizer(vevent_text: str) -> dict:
    """Extract organiser name and email from ICS ORGANIZER field.

    ICS format: ORGANIZER;CN=Display Name:mailto:email@example.com
    Returns {"organiser": {"name": ..., "email": ...}} or empty dict.
    """
    m = re.search(r"^ORGANIZER(;[^:]+)?:(.+)$", vevent_text, re.MULTILINE)
    if not m:
        return {}

    params = m.group(1) or ""
    value = m.group(2).strip().rstrip("\r")

    inner: dict = {}

    # Extract CN (common name) parameter
    cn_match = re.search(r"CN=([^;:]+)", params, re.IGNORECASE)
    if cn_match:
        name = cn_match.group(1).strip().strip('"')
        if name:
            inner["name"] = name

    # Extract email from mailto: URI
    if value.lower().startswith("mailto:"):
        email = value[7:].strip()
        if email:
            inner["email"] = email

    return {"organiser": inner} if inner else {}


def _unescape_ics(text: str) -> str:
    """Unescape ICS text values (RFC 5545 §3.3.11)."""
    return (
        text
        .replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
    )


class RefKircheMaennedorfAdapter(BaseAdapter):
    """
    TIER A SOURCE — TYPO3 + lpc_kool_events (ICS extraction)
    =========================================================
    Classification: Tier A (ICS structured data on every event)
    Platform: TYPO3 + lpc_kool_events (Lauper Computing)

    Produces: church events, children's programs, youth events, concerts
    Relevance filter: ICS SUMMARY category prefix → include/exclude/review
    Expected: ~61 INCLUDE (45%), ~10 REVIEW (7%), ~65 EXCLUDE (48%)
    """

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Surface tracking: listing page + ICS feed (conceptual surface)
        self._surfaces_attempted = 2  # listing page + ICS download

        # Phase 1: fetch listing page and discover ICS download URLs
        ics_urls = self._discover_ics_urls(cfg)
        print(f"RefKircheMaennedorfAdapter: {len(ics_urls)} ICS URLs discovered")

        # Listing succeeded if we got any ICS URLs
        self._surfaces_succeeded = 1 if ics_urls else 0
        self._detail_urls_found = len(ics_urls)

        # Respect max_items (before filtering — we want the full picture)
        ics_urls = ics_urls[: cfg.max_items]

        # Phase 2: download each ICS, parse, classify
        # Custom fetch loop (not _fetch_detail_pages) because excluded events
        # returning None must NOT trigger the circuit breaker.
        items: List[ExtractedItem] = []
        stats = {"include": 0, "exclude": 0, "review": 0, "parse_fail": 0}
        examples: dict[str, list[str]] = {"include": [], "exclude": [], "review": []}
        consecutive_http_failures = 0

        for i, url in enumerate(ics_urls):
            try:
                result = self._parse_single_ics(url, stats, examples)
                if result is not None:
                    items.append(result)
                consecutive_http_failures = 0
            except Exception as e:
                consecutive_http_failures += 1
                print(f"RefKircheMaennedorfAdapter: ICS fetch failed: {url} err: {repr(e)}")
                if consecutive_http_failures >= 5:
                    remaining = len(ics_urls) - i - 1
                    print(
                        f"RefKircheMaennedorfAdapter: CIRCUIT BREAKER — "
                        f"{consecutive_http_failures} consecutive HTTP failures, "
                        f"aborting {remaining} remaining ICS fetches"
                    )
                    self._circuit_breaker_triggered = True
                    break

            # Polite delay every 20 requests
            if (i + 1) % 20 == 0 and i + 1 < len(ics_urls):
                time.sleep(0.5)

        # ICS surface succeeded if we parsed at least one event
        total_parsed = stats["include"] + stats["exclude"] + stats["review"]
        if total_parsed > 0:
            self._surfaces_succeeded = 2  # both listing + ICS succeeded
        self._detail_urls_fetched = stats["include"] + stats["exclude"] + stats["review"] + stats["parse_fail"]

        # Phase 3: print relevance report
        total = stats["include"] + stats["exclude"] + stats["review"]
        print(f"\nRefKircheMaennedorfAdapter: RELEVANCE REPORT")
        print(f"  Total parsed: {total} (parse failures: {stats['parse_fail']})")
        print(f"  INCLUDE: {stats['include']} ({self._pct(stats['include'], total)})")
        print(f"  EXCLUDE: {stats['exclude']} ({self._pct(stats['exclude'], total)})")
        print(f"  REVIEW:  {stats['review']} ({self._pct(stats['review'], total)})")
        if examples["include"]:
            print(f"  INCLUDE examples: {examples['include'][:5]}")
        if examples["exclude"]:
            print(f"  EXCLUDE examples: {examples['exclude'][:5]}")
        if examples["review"]:
            print(f"  REVIEW examples:  {examples['review'][:5]}")

        print(f"\nRefKircheMaennedorfAdapter: {len(items)} items returned (include + review)")
        return items

    @staticmethod
    def _pct(n: int, total: int) -> str:
        return f"{n / total * 100:.0f}%" if total > 0 else "0%"

    def _discover_ics_urls(self, cfg: SourceConfig) -> List[str]:
        """Fetch listing page and extract ICS download link URLs."""
        res = http_get(cfg.seed_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        ics_urls: List[str] = []
        seen: set[str] = set()

        for a in soup.select("a.goIcs[href]"):
            href = a.get("href", "").strip()
            if not href:
                continue
            abs_url = urljoin(_SITE_ROOT, href)
            if abs_url not in seen:
                seen.add(abs_url)
                ics_urls.append(abs_url)

        return ics_urls

    def _parse_single_ics(
        self,
        ics_url: str,
        stats: dict,
        examples: dict[str, list[str]],
    ) -> Optional[ExtractedItem]:
        """Download ICS file, parse VEVENT, classify relevance.

        Returns ExtractedItem for INCLUDE/REVIEW events, None for EXCLUDE.
        Raises on HTTP errors (for circuit breaker).
        """
        res = http_get(ics_url)
        ics_text = res.text or ""

        # Extract VEVENT block (avoids VTIMEZONE DTSTART confusion)
        vevent = _extract_vevent(ics_text)
        if not vevent:
            stats["parse_fail"] += 1
            return None

        # Parse VEVENT fields
        summary = _parse_ics_field(vevent, "SUMMARY") or ""
        dtstart_raw = _parse_ics_field(vevent, "DTSTART") or ""
        dtend_raw = _parse_ics_field(vevent, "DTEND")
        location = _parse_ics_field(vevent, "LOCATION")
        description = _parse_ics_field(vevent, "DESCRIPTION")
        uid = _parse_ics_field(vevent, "UID")
        organiser_info = _parse_ics_organizer(vevent)

        # Convert to ISO 8601
        start_iso = _ics_datetime_to_iso(dtstart_raw, vevent, "DTSTART")
        end_iso = _ics_datetime_to_iso(dtend_raw, vevent, "DTEND") if dtend_raw else None

        if not start_iso or not summary:
            stats["parse_fail"] += 1
            return None

        # Split category and title
        category, title = _extract_category_and_title(summary)

        # Classify relevance
        relevance = _classify_relevance(category)
        stats[relevance] += 1

        # Collect examples (first 5 per bucket)
        example_label = f"{category}: {title}" if category else title
        if len(examples.get(relevance, [])) < 5:
            examples.setdefault(relevance, []).append(example_label)

        # Only return INCLUDE and REVIEW events
        if relevance == "exclude":
            return None

        # Build datetime_raw for normalizer (ISO pipe-separated range)
        datetime_raw = f"{start_iso} | {end_iso}" if end_iso else start_iso

        # Unescape ICS text
        if description:
            description = _unescape_ics(description)[:2000]
        if location:
            location = _unescape_ics(location)

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location,
            description_raw=description,
            item_url=ics_url,
            extra={
                "adapter": "ref_kirche_maennedorf",
                "extraction_method": "ics",
                "ics_category": category,
                "ics_summary": summary,
                "relevance": relevance,
                "ics_uid": uid,
                **organiser_info,
            },
            fetched_at=self.now_utc(),
        )
