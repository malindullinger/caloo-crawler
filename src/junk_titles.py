# src/junk_titles.py
"""
Single source of truth for junk / header / non-event title detection.

Imported by:
  - storage.py           (ingestion gate)
  - maennedorf_portal.py (adapter-level skip)
  - bridge jobs           (bridge-level skip)
  - eligibility.py       (feed eligibility gate)

Rules are deterministic (no NLP, no heuristics):
  1. Empty or whitespace-only → junk
  2. Exact match (case-insensitive) against known noise words → junk
  3. Starts with a known noise prefix (case-insensitive) → junk
  4. Contains only whitespace / digits / punctuation (no letters) → junk
"""
from __future__ import annotations

import re

# Known structural noise titles (case-insensitive exact match after strip)
JUNK_TITLES_EXACT: frozenset[str] = frozenset({
    "kopfzeile",
    "fusszeile",
})

# Prefixes that indicate a structural header/footer artifact
JUNK_TITLE_PREFIXES: tuple[str, ...] = (
    "kopfzeile",
    "fusszeile",
)

# Regex: title is purely whitespace, digits, punctuation — no real words
_STRUCTURAL_ONLY_RE = re.compile(r"^[\s\d\W]*$", re.UNICODE)


def is_junk_title(title: str | None) -> bool:
    """Return True if *title* is a known header/noise artifact.

    Rules (deterministic, no heuristics):
      1. Empty or whitespace-only → junk
      2. Exact match (case-insensitive) against known noise words → junk
      3. Starts with a known noise prefix (case-insensitive) → junk
      4. Contains only whitespace / digits / punctuation (no letters) → junk
    """
    t = (title or "").strip()
    if not t:
        return True
    low = t.lower()
    if low in JUNK_TITLES_EXACT:
        return True
    for prefix in JUNK_TITLE_PREFIXES:
        if low.startswith(prefix):
            return True
    if _STRUCTURAL_ONLY_RE.fullmatch(t):
        return True
    return False
