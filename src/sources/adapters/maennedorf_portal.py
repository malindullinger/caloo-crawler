from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import List
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

from bs4 import BeautifulSoup, NavigableString

from ..base import BaseAdapter
from ..content_surfaces import scan_content_surfaces
from ..detail_fields import extract_age, extract_category
from ..extraction import extract_title, extract_image, extract_description
from ..http import http_get
from ..link_classifier import classify_page_links
from ..structured_time import extract_datetime_structured
from ..types import SourceConfig, ExtractedItem


# Detail pages we want:
# - /_rte/anlass/7060739
# - /anlaesseaktuelles/7081397
_DETAIL_PATH_RE = re.compile(r"^/(?:_rte/anlass|anlaesseaktuelles)/(\d+)$")

# Default family-relevant ICMS category IDs.
# Resolved from the Kategorie <select> on ICMS portals:
#   18 = Familie, 14 = Für Kinder, 17 = Jugend
# Override per source via SourceConfig.extra["category_ids"].
_DEFAULT_CATEGORY_IDS = {"18", "14", "17"}

# Patterns indicating free events in the Preis field
_FREE_PATTERNS = re.compile(
    r"\b(kostenlos|gratis|frei|unentgeltlich|kein[e]?\s+(kosten|gebühr))\b",
    re.IGNORECASE,
)

# Extract a CHF numeric amount from a Preis string
_CHF_AMOUNT_RE = re.compile(
    r"(?:CHF|Fr\.?)\s*(\d+(?:[.,]\d{1,2})?)",
    re.IGNORECASE,
)


class MaennedorfPortalAdapter(BaseAdapter):
    """
    TIER B SOURCE — MUNICIPAL EXCEPTION
    ===================================
    Classification: Tier B (Explicit text-based exception)
    Decision: 2026-02-09
    Approval: Explicitly approved for text-based datetime parsing

    This source does NOT provide structured datetime (no JSON-LD, no <time>).
    Text heuristics are QUARANTINED in this adapter only.
    See docs/tier-b-sources.md for constraints.

    Strategy:
    - Fetch list page with JS rendering (Playwright)
    - Parse data-entities JSON from the DataTables element
    - Filter events by _hauptkategorieId (Familie / Für Kinder / Jugend)
    - Extract detail URLs from filtered events
    - Sort by numeric id DESC (newest first)
    - Parse each detail page:
        1) Try JSON-LD (never found)
        2) Try <time> element (never found)
        3) Fall back to text heuristic (QUARANTINED HERE)
    - Extract structured fields from <dl> (Preis, Anmeldung, Voraussetzungen)
    - Extract organizer from <address>
    """

    @staticmethod
    def _apply_date_window(seed_url: str) -> str:
        """Inject datumVon=today and datumBis=today+365d into the ICMS URL.

        Strips any existing datumVon/datumBis params from the seed_url,
        then appends fresh ones computed at crawl time. All other query
        params are preserved.
        """
        today = date.today()
        datum_von = today.strftime("%d.%m.%Y")
        datum_bis = (today + timedelta(days=365)).strftime("%d.%m.%Y")

        parsed = urlparse(seed_url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        # Remove any hardcoded date params
        params.pop("datumVon", None)
        params.pop("datumBis", None)
        # Add fresh date window
        params["datumVon"] = [datum_von]
        params["datumBis"] = [datum_bis]

        new_query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def fetch(self, cfg: SourceConfig) -> List[ExtractedItem]:
        # Surface tracking: single listing page
        self._surfaces_attempted = 1

        fetch_url = self._apply_date_window(cfg.seed_url)
        self._fetch_url = fetch_url
        print(f"MaennedorfPortalAdapter [{cfg.source_id}]: fetch URL: {fetch_url}")
        res = http_get(fetch_url, render_js=True)
        html = res.text or ""

        self._surfaces_succeeded = 1
        soup = BeautifulSoup(html, "html.parser")

        # ----------------------------
        # 1) Try structured category filtering via data-entities JSON
        # ----------------------------
        category_ids = _DEFAULT_CATEGORY_IDS
        if cfg.extra and "category_ids" in cfg.extra:
            category_ids = set(str(c) for c in cfg.extra["category_ids"])

        filtered_urls = self._extract_filtered_urls(soup, cfg.seed_url, category_ids)

        if filtered_urls is not None:
            # Structured filtering succeeded
            detail_urls = filtered_urls
            print(
                f"MaennedorfPortalAdapter [{cfg.source_id}]: "
                f"category filter applied ({len(detail_urls)} family-relevant events)"
            )
        else:
            # Fallback: extract ALL URLs (original unfiltered behavior)
            print(
                f"MaennedorfPortalAdapter [{cfg.source_id}]: "
                f"WARN: data-entities not found, falling back to unfiltered crawl"
            )
            detail_urls = self._extract_all_urls(soup, html, cfg.seed_url)

        # Respect max_items
        pre_truncation_count = len(detail_urls)
        self._dom_items_visible = pre_truncation_count
        self._detail_urls_found = pre_truncation_count
        detail_urls = detail_urls[: cfg.max_items]

        print("MaennedorfPortalAdapter: detail_urls:", len(detail_urls))
        print(
            f"MaennedorfPortalAdapter [{cfg.source_id}]: "
            f"DOM-visible={pre_truncation_count}, "
            f"after max_items({cfg.max_items})={len(detail_urls)}, "
            f"truncated={pre_truncation_count > len(detail_urls)}"
        )
        if detail_urls:
            print("MaennedorfPortalAdapter: first detail url:", detail_urls[0])

        # ----------------------------
        # 2) Fetch detail pages
        # ----------------------------
        items = self._fetch_detail_pages(
            detail_urls,
            lambda url: self._extract_from_detail(cfg, url),
            adapter_name="MaennedorfPortalAdapter",
            circuit_breaker_threshold=15,
        )

        print("MaennedorfPortalAdapter: items built:", len(items))
        return items

    def _extract_filtered_urls(
        self, soup: BeautifulSoup, seed_url: str, category_ids: set[str]
    ) -> List[str] | None:
        """Extract detail URLs filtered by category from the data-entities JSON.

        ICMS portals embed all event data in a data-entities attribute on the
        DataTables element. Each event has a _hauptkategorieId field with the
        numeric category ID. We parse this JSON and return only URLs for events
        matching the target categories.

        Returns None if data-entities is not found (triggers fallback).
        """
        el = soup.find(attrs={"data-entities": True})
        if not el:
            return None

        raw = el.get("data-entities", "")
        if not raw:
            return None

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None

        events = data.get("data")
        if not isinstance(events, list):
            return None

        total = len(events)
        matched: List[tuple[int, str]] = []

        for evt in events:
            cat_id = str(evt.get("_hauptkategorieId", "")).strip()
            # Some events have comma-separated IDs (e.g. "18,14")
            evt_cats = {c.strip() for c in cat_id.split(",")} if cat_id else set()

            if not evt_cats & category_ids:
                continue

            # Extract detail path from the name HTML:
            # <a href="/_rte/anlass/6615883">Title</a>
            name_html = evt.get("name", "")
            m = re.search(r'href="([^"]+)"', name_html)
            if not m:
                continue

            path = m.group(1).split("?")[0].split("#")[0]
            path_m = _DETAIL_PATH_RE.match(path)
            if not path_m:
                continue

            numeric_id = int(path_m.group(1))
            abs_url = urljoin(seed_url, path)
            matched.append((numeric_id, abs_url))

        # Sort by numeric ID DESC (newest first), deduplicate
        matched.sort(key=lambda t: t[0], reverse=True)
        seen: set[str] = set()
        detail_urls: List[str] = []
        for _, url in matched:
            if url not in seen:
                seen.add(url)
                detail_urls.append(url)

        filtered = total - len(detail_urls)
        print(
            f"MaennedorfPortalAdapter: data-entities: {total} total, "
            f"{len(detail_urls)} matched categories {category_ids}, "
            f"{filtered} filtered out"
        )

        return detail_urls

    def _extract_all_urls(
        self, soup: BeautifulSoup, html: str, seed_url: str
    ) -> List[str]:
        """Fallback: extract ALL detail URLs (unfiltered, original behavior)."""
        # 1) Collect normal href paths
        href_paths: List[str] = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if not h:
                continue
            path = h.split("?")[0].split("#")[0]
            if _DETAIL_PATH_RE.match(path):
                href_paths.append(path)

        # 2) Collect escaped paths from raw HTML
        escaped_hits = re.findall(r"\\/(?:_rte\\/anlass|anlaesseaktuelles)\\/\d+", html)
        escaped_paths = [h.replace("\\/", "/") for h in escaped_hits]
        escaped_paths = [p for p in escaped_paths if _DETAIL_PATH_RE.match(p)]

        # 3) Combine, sort by ID DESC, dedupe
        candidates = href_paths + escaped_paths
        parsed: List[tuple[int, str]] = []
        for p in candidates:
            m = _DETAIL_PATH_RE.match(p)
            if not m:
                continue
            parsed.append((int(m.group(1)), p))

        parsed.sort(key=lambda t: t[0], reverse=True)

        seen: set[str] = set()
        detail_urls: List[str] = []
        for _, path in parsed:
            abs_url = urljoin(seed_url, path)
            if abs_url in seen:
                continue
            seen.add(abs_url)
            detail_urls.append(abs_url)

        return detail_urls

    def enrich(self, cfg: SourceConfig, item: ExtractedItem) -> ExtractedItem:
        # fetch() already parses detail pages
        return item

    # ------------------------------------------------------------------
    # Structured field extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_dl_fields(soup: BeautifulSoup) -> dict:
        """Extract key/value pairs from <dl> elements on ICMS detail pages.

        ICMS pages have an "Informationen" section rendered as <dl> with
        <dt> keys (Voraussetzungen, Preis, Anmeldung) and <dd> values.
        """
        fields: dict = {}
        for dl in soup.find_all("dl"):
            dts = dl.find_all("dt")
            dds = dl.find_all("dd")
            for dt, dd in zip(dts, dds):
                key = dt.get_text(strip=True)
                val_text = dd.get_text(" ", strip=True)
                val_links = [
                    a.get("href", "")
                    for a in dd.find_all("a", href=True)
                    if not a.get("href", "").startswith("mailto:")
                ]
                if key and val_text:
                    fields[key] = {"text": val_text, "links": val_links}
        return fields

    @staticmethod
    def _extract_address(soup: BeautifulSoup) -> dict | None:
        """Extract organizer from <address class='icms-contact-container'>.

        Structure: first text node = org name, second text node = contact person,
        mailto: link = email.
        """
        addr = soup.find("address")
        if not addr:
            return None

        # Collect bare text nodes separated by <br/>
        text_parts: List[str] = []
        for child in addr.children:
            if isinstance(child, NavigableString):
                txt = child.strip()
                if txt:
                    text_parts.append(txt)

        org_name = text_parts[0] if len(text_parts) >= 1 else ""
        contact_person = text_parts[1] if len(text_parts) >= 2 else ""

        # Email from mailto: link
        email = ""
        email_link = addr.find("a", href=lambda h: h and h.startswith("mailto:"))
        if email_link:
            email = email_link.get("href", "").replace("mailto:", "").strip()

        # Website link
        website = ""
        for a in addr.find_all("a", href=True):
            href = a.get("href", "")
            if href.startswith("http") and "mailto:" not in href:
                website = href
                break

        if not org_name and not contact_person and not email:
            return None

        result: dict = {}
        if org_name:
            result["name"] = org_name
        if contact_person:
            result["contact_person"] = contact_person
        if email:
            result["email"] = email
        if website:
            result["url"] = website
        return result

    # Labels that indicate an organizer in ICMS <dl> blocks, checked
    # case-insensitively. Order = priority (first match wins).
    _ORGANISER_DL_LABELS = (
        "Veranstalter",
        "Organisation",
        "Durchführung",
        "Anbieter",
        "Kontakt",
    )

    @staticmethod
    def _extract_organiser_from_dl(dl_fields: dict) -> dict | None:
        """Fallback: extract organizer name from <dl> labeled fields.

        Only used when _extract_address() returned None.
        Returns {"name": ...} dict matching the standard organiser shape,
        or None if no suitable label/value found.
        """
        for label in MaennedorfPortalAdapter._ORGANISER_DL_LABELS:
            for key, val in dl_fields.items():
                if key.lower().startswith(label.lower()):
                    text = val["text"].strip()
                    # Reject values that are clearly not organizer entities:
                    # phone numbers, email addresses, URLs, very short fragments
                    if not text or len(text) < 4 or len(text) > 200:
                        continue
                    if "@" in text:
                        continue
                    if text.startswith("http://") or text.startswith("https://"):
                        continue
                    if re.match(r"^[\d\s+\-/()\\.]+$", text):
                        continue
                    return {"name": text}
        return None

    @staticmethod
    def _parse_price(preis_text: str) -> dict:
        """Parse the Preis field into price_type and price_from_chf.

        Returns {"price_type": "free"|"paid", "price_from_chf": float|None,
                 "price_raw": str}.
        """
        result: dict = {"price_raw": preis_text}

        if _FREE_PATTERNS.search(preis_text):
            result["price_type"] = "free"
            return result

        m = _CHF_AMOUNT_RE.search(preis_text)
        if m:
            try:
                amount = float(m.group(1).replace(",", "."))
                result["price_type"] = "paid"
                result["price_from_chf"] = amount
            except ValueError:
                result["price_type"] = "paid"
        else:
            # Has text but no recognizable amount (e.g., "Siehe Website")
            result["price_type"] = "paid"

        return result

    # ------------------------------------------------------------------
    # Detail page extraction
    # ------------------------------------------------------------------

    def _extract_from_detail(self, cfg: SourceConfig, detail_url: str) -> ExtractedItem | None:
        res = http_get(detail_url)
        soup = BeautifulSoup(res.text or "", "html.parser")

        # ICMS puts event title in og:title, not h1 (h1 = site nav element)
        title = None
        og = soup.find("meta", property="og:title")
        if og and (og.get("content") or "").strip():
            title = og["content"].strip()
        if not title:
            title = extract_title(soup, strip_title_suffix=" - ")
        if not title:
            print(f"MaennedorfPortalAdapter [{cfg.source_id}]: no title extracted — {detail_url}")
            return None

        # Lead container for location + fallback datetime extraction
        lead = soup.select_one(".icms-lead-container")

        # Tiers 1-3: structured extraction (JSON-LD → <time> → ISO text)
        datetime_raw, extraction_method = extract_datetime_structured(soup, container=lead)

        location_raw = None

        # Tier 4: text heuristic (TIER B QUARANTINE)
        # This text parsing is ONLY allowed for this source.
        # Pattern: "D. Mon. YYYY, HH.MM Uhr - HH.MM Uhr"
        # No inference, no defaults — if ambiguous, preserve unknown-time semantics.
        if not datetime_raw:
            extraction_method = "text_heuristic"
            lead_lines: List[str] = []
            if lead:
                lead_text = lead.get_text("\n", strip=True)
                lead_lines = [ln.strip() for ln in lead_text.split("\n") if ln.strip()]

            if lead_lines:
                # Pick the best datetime line:
                # 1) Prefer a line containing "Uhr" (has time window)
                # 2) Else last line that contains a year/date-like pattern
                dt_idx = None

                for i in range(len(lead_lines) - 1, -1, -1):
                    if "Uhr" in lead_lines[i]:
                        dt_idx = i
                        break

                if dt_idx is None:
                    for i in range(len(lead_lines) - 1, -1, -1):
                        if re.search(r"\d{4}", lead_lines[i]) or re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", lead_lines[i]):
                            dt_idx = i
                            break

                if dt_idx is not None:
                    datetime_raw = lead_lines[dt_idx]
                    if dt_idx > 0:
                        location_raw = ", ".join(lead_lines[:dt_idx])

        # Extract location from lead if not already set (for structured paths)
        if not location_raw and lead:
            lead_lines = [ln.strip() for ln in lead.get_text("\n", strip=True).split("\n") if ln.strip()]
            if lead_lines and extraction_method != "text_heuristic":
                loc_lines = [ln for ln in lead_lines if not re.search(r"\d{4}", ln)]
                if loc_lines:
                    location_raw = ", ".join(loc_lines)

        # RawEvent requires datetime_raw to be a string
        if not datetime_raw:
            print(f"MaennedorfPortalAdapter [{cfg.source_id}]: no datetime extracted — {detail_url} (title: {title!r})")
            return None

        # Description: ICMS pages use .icms-text-container for event description.
        description_raw = extract_description(soup, primary_selector=".icms-text-container", max_length=4000)

        # Image (og:image already captures i-web.ch CDN images)
        image_url = extract_image(soup, page_url=detail_url)

        # ----------------------------
        # Structured fields from <dl> and <address>
        # ----------------------------
        dl_fields = self._extract_dl_fields(soup)
        organiser = self._extract_address(soup)

        # Fallback: organizer from <dl> fields when <address> is absent.
        # ICMS "Informationen" sections sometimes contain organizer-related
        # labels (Veranstalter, Organisation, Kontakt, Durchführung, Anbieter).
        if not organiser:
            organiser = self._extract_organiser_from_dl(dl_fields)

        # Price
        price_info: dict = {}
        preis = dl_fields.get("Preis")
        if preis:
            price_info = self._parse_price(preis["text"])

        # Registration
        registration_info: dict = {}
        anmeldung = dl_fields.get("Anmeldung")
        if anmeldung:
            registration_info["registration_raw"] = anmeldung["text"]
            if anmeldung["links"]:
                registration_info["registration_url"] = anmeldung["links"][0]

        # Voraussetzungen (prerequisites — lightweight context)
        voraussetzungen = dl_fields.get("Voraussetzungen")
        prerequisites_raw = ""
        if voraussetzungen:
            text = voraussetzungen["text"]
            # Skip generic "keine" values
            if text.lower() not in ("keine", "keine voraussetzungen", "keine erforderlich.", "-"):
                prerequisites_raw = text

        # Content surface scan (Phase 7C.1 — measurement)
        surfaces = scan_content_surfaces(soup, detail_url)

        # Age extraction (Phase 7C.2) — from dl fields, title, description
        age_info = extract_age(soup, dl_fields=dl_fields, title=title, description=description_raw)

        # Category extraction (Phase 7C.2) — from JSON-LD
        cat_info = extract_category(soup)

        # Link classification (Phase 7C.2)
        link_cls = classify_page_links(surfaces.get("external_links", []))

        return ExtractedItem(
            title_raw=title,
            datetime_raw=datetime_raw,
            location_raw=location_raw,
            description_raw=description_raw,
            item_url=detail_url,
            extra={
                "adapter": "maennedorf_portal",
                "detail_parsed": True,
                "extraction_method": extraction_method,
                "fetch_url": getattr(self, "_fetch_url", None),
                **({"image_url": image_url} if image_url else {}),
                **({"organiser": organiser} if organiser else {}),
                **({"price_type": price_info["price_type"]} if "price_type" in price_info else {}),
                **({"price_from_chf": price_info["price_from_chf"]} if "price_from_chf" in price_info else {}),
                **({"price_raw": price_info["price_raw"]} if "price_raw" in price_info else {}),
                **(registration_info),
                **({"prerequisites": prerequisites_raw} if prerequisites_raw else {}),
                **{k: v for k, v in surfaces.items() if v},
                **{k: v for k, v in age_info.items() if v},
                **{k: v for k, v in cat_info.items() if v},
                **{k: v for k, v in link_cls.items() if v},
            },
            fetched_at=getattr(cfg, "now_utc", None),
        )
