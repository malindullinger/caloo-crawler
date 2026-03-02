# tests/test_organizer_pipeline.py
"""
End-to-end tests for OrganizerExtractionPipeline.

Verifies orchestration, backward compatibility, and idempotency.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from src.extraction.organizer.pipeline import OrganizerExtractionPipeline
from src.extraction.organizer.types import EvidenceType


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

NORMAL_EVENT_HTML = """
<html>
<head><title>Kinderflohmarkt</title></head>
<body>
<main>
  <h1>Kinderflohmarkt</h1>
  <div class="icms-lead-container">
    <p>Gemeindesaal Männedorf</p>
    <p>Samstag, 15. März 2026, 10.00 – 14.00 Uhr</p>
  </div>
  <div class="content">
    <p>Grosser Kinderflohmarkt mit Spielzeug, Büchern und Kleidern.</p>
    <p>Veranstalter: Elternverein Männedorf</p>
    <p>Weitere Infos: <a href="https://elternverein-maennedorf.ch/flohmarkt">Website</a></p>
  </div>
</main>
</body>
</html>
"""

NO_ORGANIZER_HTML = """
<html>
<head><title>Sommerfest</title></head>
<body>
<main>
  <h1>Sommerfest</h1>
  <p>Ein Fest für die ganze Familie.</p>
</main>
</body>
</html>
"""

LINK_ONLY_HTML = """
<html><body><main>
  <h1>Event</h1>
  <a href="https://sportclub-maennedorf.ch/events">Sportclub Männedorf</a>
</main></body></html>
"""

HTML_AND_LINK_HTML = """
<html><body><main>
  <h1>Event</h1>
  <p>Veranstalter: Labeled Org Name</p>
  <a href="https://sportclub-maennedorf.ch/events">Sportclub Männedorf</a>
</main></body></html>
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

pipeline = OrganizerExtractionPipeline()


class TestBackwardCompatibility:
    """Critical: pipeline must produce same name as old _extract_organizer_name."""

    def test_normal_event_returns_elternverein(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/100")
        assert result.winner is not None
        assert result.winner.name == "Elternverein Männedorf"


class TestPipelineNoOrganizer:
    def test_no_organizer_returns_none(self):
        soup = BeautifulSoup(NO_ORGANIZER_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/200")
        assert result.winner is None
        assert result.all_candidates == []


class TestHtmlBeatsLink:
    def test_html_candidate_wins_over_link(self):
        soup = BeautifulSoup(HTML_AND_LINK_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/300")
        assert result.winner is not None
        assert result.winner.name == "Labeled Org Name"
        assert result.winner.evidence_type == EvidenceType.HTML_LABELED_FIELD
        assert result.winner.confidence == 85
        # Link candidates should also be present
        assert len(result.all_candidates) >= 2


class TestLinkOnly:
    def test_link_candidate_wins_when_no_html(self):
        soup = BeautifulSoup(LINK_ONLY_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/400")
        assert result.winner is not None
        assert result.winner.evidence_type == EvidenceType.EXTERNAL_LINK


class TestPdfText:
    def test_pdf_text_extraction(self):
        result = pipeline.run(pdf_text="Veranstalter: PDF Org Name")
        assert result.winner is not None
        assert result.winner.name == "PDF Org Name"
        assert result.winner.evidence_type == EvidenceType.PDF_TEXT
        assert result.winner.confidence == 75

    def test_pdf_text_none(self):
        result = pipeline.run(pdf_text=None)
        assert result.winner is None

    def test_pdf_text_empty(self):
        result = pipeline.run(pdf_text="")
        assert result.winner is None


class TestIdempotency:
    def test_same_input_same_output(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        base = "https://www.maennedorf.ch/_rte/anlass/100"
        r1 = pipeline.run(soup=soup, base_url=base)
        r2 = pipeline.run(soup=soup, base_url=base)
        assert r1.winner == r2.winner
        assert r1.all_candidates == r2.all_candidates


class TestOrganizerResult:
    def test_all_candidates_sorted(self):
        soup = BeautifulSoup(HTML_AND_LINK_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/300")
        if len(result.all_candidates) >= 2:
            # First should have higher confidence or priority
            assert result.all_candidates[0].confidence >= result.all_candidates[-1].confidence

    def test_winner_is_first_candidate(self):
        soup = BeautifulSoup(HTML_AND_LINK_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/300")
        if result.winner and result.all_candidates:
            assert result.winner == result.all_candidates[0]

    def test_evidence_excerpt_populated(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/100")
        assert result.winner is not None
        assert result.winner.evidence_excerpt is not None

    def test_evidence_ref_populated(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        result = pipeline.run(soup=soup, base_url="https://www.maennedorf.ch/_rte/anlass/100")
        assert result.winner is not None
        assert result.winner.evidence_ref != ""


class TestNoInputs:
    def test_no_soup_no_pdf(self):
        result = pipeline.run()
        assert result.winner is None
        assert result.all_candidates == []
