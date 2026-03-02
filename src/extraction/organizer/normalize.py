# src/extraction/organizer/normalize.py
"""
Deterministic organizer-name normalization and junk filtering.

All functions are pure, deterministic, and have no side effects.
"""
from __future__ import annotations

import re
import unicodedata


# ---------------------------------------------------------------------------
# Junk organizer names (systematic false positives)
# ---------------------------------------------------------------------------

_JUNK_NAMES: frozenset[str] = frozenset({
    "kontakt",
    "impressum",
    "weitere informationen",
    "anmeldung",
    "veranstaltungen",
    "gemeinde",
    "navigation",
    "startseite",
    "inhalt",
    "footer",
    "header",
    "menü",
    "suche",
    "info",
    "home",
    "zurück",
    "mehr",
    "download",
    "datenschutz",
    "agenda",
    "kalender",
    "events",
    "aktuell",
    "aktuelles",
    "archiv",
    "newsletter",
    "sitemap",
    "login",
    "logout",
})

# Known abbreviations: lowercase key → canonical form.
# When converting ALL CAPS to title case, these tokens get their canonical form.
_CANONICAL_ABBREVIATIONS: dict[str, str] = {
    "gmbh": "GmbH", "ag": "AG", "okja": "OKJA", "ev": "EV",
    "e.v.": "e.V.", "sa": "SA", "kg": "KG", "ohg": "OHG",
    "se": "SE", "sarl": "SARL", "co": "Co", "ltd": "Ltd",
    "inc": "Inc",
}

_TRAILING_PUNCT_RE = re.compile(r"[.:;,\-–—]+$")
_WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_organizer_name(name: str) -> str:
    """
    Deterministic normalization of an organizer name.

    Steps:
      1. Unicode NFKC normalization
      2. Strip leading/trailing whitespace
      3. Collapse internal whitespace to single space
      4. Remove trailing punctuation (.:;,-)
      5. ALL CAPS → Title Case (preserving known abbreviations)

    Returns empty string for empty/whitespace-only input.
    """
    if not name:
        return ""

    # 1. Unicode NFKC
    s = unicodedata.normalize("NFKC", name)

    # 2. Strip
    s = s.strip()
    if not s:
        return ""

    # 3. Collapse internal whitespace
    s = _WHITESPACE_RE.sub(" ", s)

    # 4. Remove trailing punctuation
    s = _TRAILING_PUNCT_RE.sub("", s).strip()
    if not s:
        return ""

    # 5. ALL CAPS → Title Case (only if clearly all-caps, not abbreviations)
    if s == s.upper() and len(s) > 5:
        s = _smart_title_case(s)

    return s


def is_junk_organizer_name(name: str) -> bool:
    """
    Return True if the name is a systematic false positive.

    Checks:
      - Empty or whitespace-only
      - Fewer than 3 meaningful characters (after stripping)
      - Exact match against known junk names (case-insensitive, NFKC-normalized)
    """
    if not name:
        return True

    normalized = unicodedata.normalize("NFKC", name).strip()
    if not normalized:
        return True

    # Fewer than 3 meaningful characters
    meaningful = re.sub(r"[\s\-_.:;,]", "", normalized)
    if len(meaningful) < 3:
        return True

    # Exact match against junk names
    folded = normalized.casefold()
    if folded in _JUNK_NAMES:
        return True

    return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _smart_title_case(s: str) -> str:
    """
    Convert ALL-CAPS string to Title Case, preserving known abbreviations.

    'ELTERNVEREIN MÄNNEDORF' → 'Elternverein Männedorf'
    'SPORTVEREIN GMBH'       → 'Sportverein GmbH'  (GmbH canonical form)
    'OKJA ZÜRICH'            → 'OKJA Zürich'        (OKJA canonical form)
    'AG'                     → 'AG'                  (short token preserved)
    """
    tokens = s.split()
    result: list[str] = []
    for token in tokens:
        lower = token.lower()
        if lower in _CANONICAL_ABBREVIATIONS:
            result.append(_CANONICAL_ABBREVIATIONS[lower])
        elif len(token) <= 3 and token == token.upper():
            result.append(token)  # short all-caps tokens preserved (AG, EV, SA)
        else:
            result.append(token.capitalize())
    return " ".join(result)
