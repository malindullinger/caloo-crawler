# tests/test_organizer_html_extractor.py
"""
Unit tests for HtmlLabeledFieldExtractor.

Tests all three strategies: inline text, dt/dd, th/td.
Uses the same HTML fixtures as test_maennedorf_adapter.py for regression proof.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from src.extraction.organizer.extractors.html_labeled_field import (
    HtmlLabeledFieldExtractor,
)
from src.extraction.organizer.types import EvidenceType


# ---------------------------------------------------------------------------
# HTML fixtures (shared with test_maennedorf_adapter.py)
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

ORGANIZER_DT_DD_HTML = """
<html>
<head><title>Laternenumzug</title></head>
<body>
<main>
  <h1>Laternenumzug</h1>
  <dl>
    <dt>Veranstalter:</dt>
    <dd>Quartierverein Dorfkern</dd>
    <dt>Kosten:</dt>
    <dd>Gratis</dd>
  </dl>
</main>
</body>
</html>
"""

ORGANIZER_TABLE_HTML = """
<html>
<head><title>Märchenstunde</title></head>
<body>
<main>
  <h1>Märchenstunde</h1>
  <table>
    <tr><th>Organisator</th><td>Bibliothek Männedorf</td></tr>
    <tr><th>Alter</th><td>4-8 Jahre</td></tr>
  </table>
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

ORGANIZER_ORGANISATION_LABEL_HTML = """
<html><body><main>
  <p>Organisation: Musikschule Zürichsee</p>
</main></body></html>
"""

ORGANIZER_EMPTY_VALUE_HTML = """
<html><body><main>
  <p>Veranstalter: </p>
</main></body></html>
"""

ORGANIZER_JUNK_VALUE_HTML = """
<html><body><main>
  <p>Veranstalter: Kontakt</p>
</main></body></html>
"""

ORGANIZER_DT_DD_NON_IMMEDIATE_SIBLING_HTML = """
<html><body><main>
  <dl>
    <dt>Veranstalter:</dt>
    <dt>Extra label</dt>
    <dd>Should Not Match</dd>
  </dl>
</main></body></html>
"""

ORGANIZER_TH_TD_NON_IMMEDIATE_SIBLING_HTML = """
<html><body><main>
  <table>
    <tr><th>Organisator</th><th>Extra</th><td>Should Not Match</td></tr>
  </table>
</main></body></html>
"""

ORGANIZER_OUTSIDE_MAIN_HTML = """
<html><body>
  <main><h1>Event</h1></main>
  <footer><p>Veranstalter: Footer Org</p></footer>
</body></html>
"""

MULTIPLE_STRATEGIES_HTML = """
<html><body><main>
  <p>Veranstalter: Inline Org</p>
  <dl>
    <dt>Veranstalter:</dt>
    <dd>DL Org</dd>
  </dl>
</main></body></html>
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

extractor = HtmlLabeledFieldExtractor()


class TestInlineTextStrategy:
    def test_veranstalter_label(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        candidates = extractor.extract(soup)
        names = [c.name for c in candidates]
        assert "Elternverein Männedorf" in names
        # Check the inline text candidate
        inline = [c for c in candidates if c.evidence_ref.startswith("text_line:")]
        assert len(inline) >= 1
        assert inline[0].confidence == 85
        assert inline[0].evidence_type == EvidenceType.HTML_LABELED_FIELD

    def test_organisation_label(self):
        soup = BeautifulSoup(ORGANIZER_ORGANISATION_LABEL_HTML, "html.parser")
        candidates = extractor.extract(soup)
        assert len(candidates) >= 1
        assert candidates[0].name == "Musikschule Zürichsee"

    def test_empty_value_produces_no_candidate(self):
        soup = BeautifulSoup(ORGANIZER_EMPTY_VALUE_HTML, "html.parser")
        candidates = extractor.extract(soup)
        assert len(candidates) == 0

    def test_junk_value_filtered(self):
        soup = BeautifulSoup(ORGANIZER_JUNK_VALUE_HTML, "html.parser")
        candidates = extractor.extract(soup)
        assert len(candidates) == 0


class TestDtDdStrategy:
    def test_veranstalter_dt_dd(self):
        soup = BeautifulSoup(ORGANIZER_DT_DD_HTML, "html.parser")
        candidates = extractor.extract(soup)
        dt_dd = [c for c in candidates if c.evidence_ref.startswith("dt/dd:")]
        assert len(dt_dd) >= 1
        assert dt_dd[0].name == "Quartierverein Dorfkern"
        assert dt_dd[0].confidence == 90

    def test_non_immediate_sibling_skipped(self):
        soup = BeautifulSoup(ORGANIZER_DT_DD_NON_IMMEDIATE_SIBLING_HTML, "html.parser")
        candidates = extractor.extract(soup)
        dt_dd = [c for c in candidates if c.evidence_ref.startswith("dt/dd:")]
        assert len(dt_dd) == 0


class TestThTdStrategy:
    def test_organisator_th_td(self):
        soup = BeautifulSoup(ORGANIZER_TABLE_HTML, "html.parser")
        candidates = extractor.extract(soup)
        th_td = [c for c in candidates if c.evidence_ref.startswith("th/td:")]
        assert len(th_td) >= 1
        assert th_td[0].name == "Bibliothek Männedorf"
        assert th_td[0].confidence == 90

    def test_non_immediate_sibling_skipped(self):
        soup = BeautifulSoup(ORGANIZER_TH_TD_NON_IMMEDIATE_SIBLING_HTML, "html.parser")
        candidates = extractor.extract(soup)
        th_td = [c for c in candidates if c.evidence_ref.startswith("th/td:")]
        assert len(th_td) == 0


class TestContainerScoping:
    def test_organizer_outside_main_not_found(self):
        soup = BeautifulSoup(ORGANIZER_OUTSIDE_MAIN_HTML, "html.parser")
        candidates = extractor.extract(soup)
        names = [c.name for c in candidates]
        assert "Footer Org" not in names


class TestMultipleStrategies:
    def test_both_inline_and_dt_dd_returned(self):
        soup = BeautifulSoup(MULTIPLE_STRATEGIES_HTML, "html.parser")
        candidates = extractor.extract(soup)
        assert len(candidates) >= 2
        refs = [c.evidence_ref for c in candidates]
        assert any(r.startswith("dt/dd:") for r in refs)
        assert any(r.startswith("text_line:") for r in refs)


class TestNoOrganizer:
    def test_no_organizer_returns_empty(self):
        soup = BeautifulSoup(NO_ORGANIZER_HTML, "html.parser")
        candidates = extractor.extract(soup)
        assert candidates == []

    def test_empty_html_returns_empty(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        candidates = extractor.extract(soup)
        assert candidates == []


class TestEvidenceExcerpt:
    def test_inline_has_excerpt(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        candidates = extractor.extract(soup)
        inline = [c for c in candidates if c.evidence_ref.startswith("text_line:")]
        assert len(inline) >= 1
        assert "Veranstalter" in inline[0].evidence_excerpt
        assert "Elternverein" in inline[0].evidence_excerpt

    def test_dt_dd_has_excerpt(self):
        soup = BeautifulSoup(ORGANIZER_DT_DD_HTML, "html.parser")
        candidates = extractor.extract(soup)
        dt_dd = [c for c in candidates if c.evidence_ref.startswith("dt/dd:")]
        assert len(dt_dd) >= 1
        assert "<dt>" in dt_dd[0].evidence_excerpt
        assert "<dd>" in dt_dd[0].evidence_excerpt
