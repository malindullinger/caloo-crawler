# src/extraction/organizer/types.py
"""
Core types for the organizer extraction pipeline.

All types are frozen dataclasses — immutable, hashable, JSON-friendly.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class EvidenceType(str, Enum):
    """
    Evidence source that produced an organizer candidate.

    The str mixin makes values JSON-serializable and printable.
    """
    HTML_LABELED_FIELD = "html_labeled_field"
    PDF_TEXT = "pdf_text"
    EXTERNAL_LINK = "external_link"


# Deterministic priority ordering for tie-breaking.
# Lower index = higher priority.
EVIDENCE_PRIORITY: list[EvidenceType] = [
    EvidenceType.HTML_LABELED_FIELD,
    EvidenceType.PDF_TEXT,
    EvidenceType.EXTERNAL_LINK,
]


@dataclass(frozen=True)
class OrganizerCandidate:
    """
    A single organizer candidate produced by an extractor.

    Immutable to prevent accidental mutation during selection.
    """
    name: str                              # Normalized organizer name (non-empty)
    confidence: int                        # 0-100, extraction-level confidence
    evidence_type: EvidenceType            # Which extractor produced this
    evidence_ref: str                      # Machine-readable ref, e.g. "dt/dd:Veranstalter"
    evidence_excerpt: Optional[str] = None  # Human-readable evidence snippet


@dataclass(frozen=True)
class OrganizerResult:
    """
    Pipeline output: winning candidate plus all candidates for observability.
    """
    winner: Optional[OrganizerCandidate]       # None if no candidates found
    all_candidates: List[OrganizerCandidate]   # Sorted by selection order (best first)
