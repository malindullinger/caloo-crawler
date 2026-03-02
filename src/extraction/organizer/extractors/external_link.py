# src/extraction/organizer/extractors/external_link.py
"""
ExternalLinkExtractor — conservative organizer hints from outbound URLs.

Hard constraints:
  - Ignore same-domain links
  - Domain inference ONLY if domain contains strong org keywords
  - Anchor text ONLY if org keyword present in domain AND text is non-generic, >= 3 chars
  - Explicit blocklists for social media, generic hosting, generic anchor text
  - If constraints fail → no candidate. No guessing.
"""
from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..normalize import is_junk_organizer_name, normalize_organizer_name
from ..types import EvidenceType, OrganizerCandidate

# ---------------------------------------------------------------------------
# Blocklists and allowlists
# ---------------------------------------------------------------------------

_BLOCKED_DOMAINS: frozenset[str] = frozenset({
    "facebook.com", "fb.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "youtu.be", "tiktok.com", "linkedin.com", "pinterest.com",
    "google.com", "google.ch", "google.de", "google.at",
    "maps.google.com", "maps.google.ch",
    "goo.gl", "bit.ly", "t.co", "tinyurl.com",
    "apple.com", "microsoft.com", "amazon.com",
    "wikipedia.org", "wikimedia.org",
    "admin.ch", "ch.ch",
    "whatsapp.com", "telegram.org", "signal.org",
    "dropbox.com", "wetransfer.com",
    "outlook.com", "gmail.com", "yahoo.com",
})

_GENERIC_ANCHOR_TEXT: frozenset[str] = frozenset({
    "website", "link", "hier", "klicken", "mehr", "details",
    "infos", "informationen", "anmeldung", "www", "http",
    "https", "weiter", "zur website", "homepage", "webseite",
    "hier klicken", "mehr erfahren", "weitere infos",
    "link öffnen", "extern", "externer link",
})

# Strong org-type keywords that must appear in the domain for inference.
# NOTE: "gemeinde" intentionally excluded — municipality names should come
# from labeled fields, not domain keyword inference.
_ORG_DOMAIN_KEYWORDS: frozenset[str] = frozenset({
    "verein", "club", "schule", "kita", "museum", "kirche",
    "jugend", "eltern", "sport", "kultur", "bibliothek",
    "theater", "chor", "orchester", "pfadi", "stiftung",
    "turnverein", "musikverein", "frauenverein",
    "maennerverein", "quartierverein", "seniorenverein",
})

_STRIP_TLDS: frozenset[str] = frozenset({
    "ch", "com", "org", "net", "de", "at", "li", "eu", "info",
})

_URL_PATTERN = re.compile(r"^https?://", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------

class ExternalLinkExtractor:
    """
    Extract organizer hints from external outbound links.

    Conservative: only produces candidates when strong structural signals exist.
    Both anchor-text and domain-name strategies require org keyword in domain.
    """

    def extract(
        self,
        soup: BeautifulSoup,
        base_url: str,
        *,
        outbound_urls: Optional[List[str]] = None,
    ) -> List[OrganizerCandidate]:
        """
        Extract organizer candidates from external links.

        Parameters:
            soup: Parsed HTML tree (for anchor text extraction)
            base_url: The page URL (to determine which links are external)
            outbound_urls: Pre-extracted outbound URLs (currently unused;
                          reserved for future optimization)

        Returns:
            List of OrganizerCandidate with conservative confidence scores.
        """
        if not soup or not base_url:
            return []

        try:
            base_host = (urlparse(base_url).hostname or "").lower()
        except Exception:
            return []

        if not base_host:
            return []

        candidates: List[OrganizerCandidate] = []
        seen_names: set[str] = set()

        container = soup.select_one("main") or soup.select_one("article") or soup

        for a in container.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
                continue

            try:
                parsed = urlparse(href)
                link_host = (parsed.hostname or "").lower()
            except Exception:
                continue

            if not link_host or link_host == base_host:
                continue

            # Safe www stripping (compatible with older Python)
            clean_host = link_host[4:] if link_host.startswith("www.") else link_host

            if self._is_blocked_domain(clean_host):
                continue

            # Both strategies require org keyword in domain — no exceptions.
            if not self._has_org_keyword(clean_host):
                continue

            # Strategy 1: anchor text (requires org keyword in domain)
            anchor_text = (a.get_text(strip=True) or "").strip()
            if anchor_text and not self._is_generic_anchor_text(anchor_text):
                name = normalize_organizer_name(anchor_text)
                if name and not is_junk_organizer_name(name) and name not in seen_names:
                    seen_names.add(name)
                    candidates.append(OrganizerCandidate(
                        name=name,
                        confidence=45,
                        evidence_type=EvidenceType.EXTERNAL_LINK,
                        evidence_ref=f"anchor:{href[:200]}",
                        evidence_excerpt=f"{anchor_text} [{href}]"[:200],
                    ))

            # Strategy 2: domain name inference (requires org keyword in domain)
            domain_name = self._domain_to_name(clean_host)
            if domain_name:
                name = normalize_organizer_name(domain_name)
                if name and not is_junk_organizer_name(name) and name not in seen_names:
                    seen_names.add(name)
                    candidates.append(OrganizerCandidate(
                        name=name,
                        confidence=35,
                        evidence_type=EvidenceType.EXTERNAL_LINK,
                        evidence_ref=f"domain:{clean_host}",
                        evidence_excerpt=clean_host,
                    ))

        return candidates

    def _is_blocked_domain(self, host: str) -> bool:
        """Check if domain is in the blocklist (matches base or parent domain)."""
        if host in _BLOCKED_DOMAINS:
            return True
        parts = host.split(".")
        if len(parts) >= 2:
            parent = ".".join(parts[-2:])
            if parent in _BLOCKED_DOMAINS:
                return True
        return False

    def _has_org_keyword(self, host: str) -> bool:
        """
        Check if domain contains a strong org keyword.

        Uses substring matching within segments to handle compound words
        like 'elternverein' (contains 'verein') or 'sportclub' (contains 'club').
        """
        segments = re.split(r"[.\-]", host)
        for seg in segments:
            seg_lower = seg.lower()
            for kw in _ORG_DOMAIN_KEYWORDS:
                if kw in seg_lower:
                    return True
        return False

    def _domain_to_name(self, host: str) -> Optional[str]:
        """
        Convert domain to potential organizer name.

        'elternverein-maennedorf.ch' → 'Elternverein Maennedorf'

        Returns None if domain is too short or doesn't look like an org name.
        """
        parts = host.split(".")
        if len(parts) >= 2 and parts[-1].lower() in _STRIP_TLDS:
            parts = parts[:-1]
        if not parts:
            return None

        domain_part = parts[-1] if parts else ""
        if not domain_part:
            return None

        segments = domain_part.split("-")
        segments = [s for s in segments if s]
        if not segments:
            return None

        name = " ".join(s.capitalize() for s in segments)
        if len(name) < 3:
            return None

        return name

    def _is_generic_anchor_text(self, text: str) -> bool:
        """Return True if anchor text is too generic to be an organizer hint."""
        if not text:
            return True
        if len(text.strip()) < 3:
            return True
        if _URL_PATTERN.match(text.strip()):
            return True
        if text.strip().lower() in _GENERIC_ANCHOR_TEXT:
            return True
        return False
