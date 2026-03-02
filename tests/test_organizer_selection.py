# tests/test_organizer_selection.py
"""
Unit tests for deterministic candidate selection.
"""
from __future__ import annotations

from src.extraction.organizer.selection import select_winner, sort_candidates
from src.extraction.organizer.types import EvidenceType, OrganizerCandidate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _c(
    name: str = "Test Org",
    confidence: int = 85,
    evidence_type: EvidenceType = EvidenceType.HTML_LABELED_FIELD,
    evidence_ref: str = "test",
) -> OrganizerCandidate:
    return OrganizerCandidate(
        name=name,
        confidence=confidence,
        evidence_type=evidence_type,
        evidence_ref=evidence_ref,
    )


# ---------------------------------------------------------------------------
# select_winner
# ---------------------------------------------------------------------------

class TestSelectWinner:
    def test_empty_candidates_returns_none(self):
        assert select_winner([]) is None

    def test_single_candidate(self):
        c = _c("Elternverein")
        assert select_winner([c]) == c

    def test_highest_confidence_wins(self):
        high = _c("High", confidence=90)
        low = _c("Low", confidence=45)
        assert select_winner([low, high]) == high

    def test_evidence_type_tiebreak(self):
        html = _c("Org", confidence=85, evidence_type=EvidenceType.HTML_LABELED_FIELD)
        link = _c("Org", confidence=85, evidence_type=EvidenceType.EXTERNAL_LINK)
        assert select_winner([link, html]) == html

    def test_name_lexicographic_tiebreak(self):
        a = _c("Alpha Verein", confidence=90, evidence_type=EvidenceType.HTML_LABELED_FIELD)
        b = _c("Beta Verein", confidence=90, evidence_type=EvidenceType.HTML_LABELED_FIELD)
        assert select_winner([b, a]) == a  # "Alpha" < "Beta"

    def test_deterministic_across_runs(self):
        candidates = [
            _c("Zebra Club", confidence=70, evidence_type=EvidenceType.EXTERNAL_LINK),
            _c("Alpha Verein", confidence=90, evidence_type=EvidenceType.HTML_LABELED_FIELD),
            _c("Beta Schule", confidence=90, evidence_type=EvidenceType.PDF_TEXT),
        ]
        # Run 100 times, must always produce same result
        results = [select_winner(candidates) for _ in range(100)]
        assert all(r == results[0] for r in results)
        assert results[0].name == "Alpha Verein"


# ---------------------------------------------------------------------------
# sort_candidates
# ---------------------------------------------------------------------------

class TestSortCandidates:
    def test_empty_list(self):
        assert sort_candidates([]) == []

    def test_sorted_by_confidence_desc(self):
        candidates = [
            _c("Low", confidence=35),
            _c("High", confidence=90),
            _c("Mid", confidence=70),
        ]
        sorted_c = sort_candidates(candidates)
        assert sorted_c[0].confidence == 90
        assert sorted_c[1].confidence == 70
        assert sorted_c[2].confidence == 35

    def test_evidence_type_priority(self):
        candidates = [
            _c("C", confidence=85, evidence_type=EvidenceType.EXTERNAL_LINK),
            _c("A", confidence=85, evidence_type=EvidenceType.HTML_LABELED_FIELD),
            _c("B", confidence=85, evidence_type=EvidenceType.PDF_TEXT),
        ]
        sorted_c = sort_candidates(candidates)
        assert sorted_c[0].evidence_type == EvidenceType.HTML_LABELED_FIELD
        assert sorted_c[1].evidence_type == EvidenceType.PDF_TEXT
        assert sorted_c[2].evidence_type == EvidenceType.EXTERNAL_LINK

    def test_name_tiebreak_uses_casefold(self):
        a = _c("alpha", confidence=85)
        b = _c("Beta", confidence=85)
        sorted_c = sort_candidates([b, a])
        # "alpha".casefold() < "beta".casefold()
        assert sorted_c[0].name == "alpha"
        assert sorted_c[1].name == "Beta"

    def test_full_ordering(self):
        candidates = [
            _c("Zebra", confidence=35, evidence_type=EvidenceType.EXTERNAL_LINK),
            _c("Alpha", confidence=90, evidence_type=EvidenceType.HTML_LABELED_FIELD),
            _c("Beta", confidence=90, evidence_type=EvidenceType.HTML_LABELED_FIELD),
            _c("Gamma", confidence=75, evidence_type=EvidenceType.PDF_TEXT),
        ]
        sorted_c = sort_candidates(candidates)
        assert [c.name for c in sorted_c] == ["Alpha", "Beta", "Gamma", "Zebra"]
