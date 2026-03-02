# src/extraction/organizer/selection.py
"""
Deterministic candidate selection for organizer extraction.

Selection algorithm:
  1. Sort by confidence DESC
  2. Tie-break by evidence_type priority (html > pdf > link)
  3. Stable NFKC-casefolded name as final tie-breaker

Pure functions, no side effects, identical results across runs and environments.
"""
from __future__ import annotations

import unicodedata
from typing import List, Optional

from .types import EVIDENCE_PRIORITY, OrganizerCandidate


def _sort_key(c: OrganizerCandidate) -> tuple:
    """
    Deterministic sort key for total ordering.

    Returns:
        (-confidence, evidence_priority_index, stable_name)

    - Negated confidence: higher confidence sorts first.
    - Evidence priority: lower index sorts first (html=0, pdf=1, link=2).
    - Stable name: NFKC + casefold for cross-platform determinism.
    """
    try:
        priority_idx = EVIDENCE_PRIORITY.index(c.evidence_type)
    except ValueError:
        priority_idx = len(EVIDENCE_PRIORITY)  # unknown types sort last
    stable_name = unicodedata.normalize("NFKC", c.name).casefold()
    return (-c.confidence, priority_idx, stable_name)


def sort_candidates(
    candidates: List[OrganizerCandidate],
) -> List[OrganizerCandidate]:
    """
    Sort candidates by selection priority (best first).

    Returns a new list; does not mutate input.
    """
    return sorted(candidates, key=_sort_key)


def select_winner(
    candidates: List[OrganizerCandidate],
) -> Optional[OrganizerCandidate]:
    """
    Select the best candidate deterministically.

    Returns None if candidates is empty.
    """
    if not candidates:
        return None
    return sort_candidates(candidates)[0]
