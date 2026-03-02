# tests/test_organizer_external_link.py
"""
Unit tests for ExternalLinkExtractor.

Tests conservative domain inference and anchor text extraction.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from src.extraction.organizer.extractors.external_link import (
    ExternalLinkExtractor,
)
from src.extraction.organizer.types import EvidenceType


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

ORG_LINK_HTML = """
<html><body><main>
  <a href="https://elternverein-maennedorf.ch/programm">Elternverein Männedorf</a>
</main></body></html>
"""

GENERIC_ANCHOR_HTML = """
<html><body><main>
  <a href="https://elternverein-maennedorf.ch/programm">Website</a>
</main></body></html>
"""

SOCIAL_MEDIA_HTML = """
<html><body><main>
  <a href="https://facebook.com/elternverein">Elternverein auf Facebook</a>
  <a href="https://www.instagram.com/verein123">Follow us</a>
  <a href="https://youtube.com/watch?v=abc">Video</a>
</main></body></html>
"""

SAME_HOST_HTML = """
<html><body><main>
  <a href="https://www.maennedorf.ch/other-page">Other page</a>
  <a href="/relative/path">Relative</a>
</main></body></html>
"""

MAILTO_TEL_HTML = """
<html><body><main>
  <a href="mailto:info@elternverein.ch">E-Mail</a>
  <a href="tel:+41441234567">Telefon</a>
  <a href="#">Anchor</a>
</main></body></html>
"""

NO_ORG_KEYWORD_HTML = """
<html><body><main>
  <a href="https://random-website.ch/page">Random Website Name</a>
</main></body></html>
"""

MULTIPLE_ORG_LINKS_HTML = """
<html><body><main>
  <a href="https://sportclub-zurich.ch">Sportclub Zürich</a>
  <a href="https://musikverein-horgen.ch/events">Musikverein Horgen</a>
</main></body></html>
"""

DOMAIN_ONLY_ORG_HTML = """
<html><body><main>
  <a href="https://turnverein-stefa.ch/termine">Anmeldung</a>
</main></body></html>
"""

BLOCKED_PARENT_DOMAIN_HTML = """
<html><body><main>
  <a href="https://events.facebook.com/elternverein">Event page</a>
</main></body></html>
"""

GEMEINDE_DOMAIN_HTML = """
<html><body><main>
  <a href="https://gemeinde-stefa.ch/events">Gemeinde Stäfa Events</a>
</main></body></html>
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

extractor = ExternalLinkExtractor()
BASE_URL = "https://www.maennedorf.ch/_rte/anlass/123"


class TestOrgKeywordDomain:
    def test_anchor_text_with_org_domain(self):
        soup = BeautifulSoup(ORG_LINK_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        anchor_candidates = [c for c in candidates if c.evidence_ref.startswith("anchor:")]
        assert len(anchor_candidates) >= 1
        assert anchor_candidates[0].name == "Elternverein Männedorf"
        assert anchor_candidates[0].confidence == 45
        assert anchor_candidates[0].evidence_type == EvidenceType.EXTERNAL_LINK

    def test_domain_inference_with_org_keyword(self):
        soup = BeautifulSoup(ORG_LINK_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        domain_candidates = [c for c in candidates if c.evidence_ref.startswith("domain:")]
        assert len(domain_candidates) >= 1
        assert domain_candidates[0].confidence == 35

    def test_generic_anchor_filtered(self):
        soup = BeautifulSoup(GENERIC_ANCHOR_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        anchor_candidates = [c for c in candidates if c.evidence_ref.startswith("anchor:")]
        assert len(anchor_candidates) == 0
        # But domain inference should still work
        domain_candidates = [c for c in candidates if c.evidence_ref.startswith("domain:")]
        assert len(domain_candidates) >= 1


class TestBlockedDomains:
    def test_social_media_blocked(self):
        soup = BeautifulSoup(SOCIAL_MEDIA_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert len(candidates) == 0

    def test_blocked_parent_domain(self):
        soup = BeautifulSoup(BLOCKED_PARENT_DOMAIN_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert len(candidates) == 0


class TestSameHost:
    def test_same_host_excluded(self):
        soup = BeautifulSoup(SAME_HOST_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert len(candidates) == 0


class TestMailtoTelAnchor:
    def test_mailto_tel_skipped(self):
        soup = BeautifulSoup(MAILTO_TEL_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert len(candidates) == 0


class TestNoOrgKeyword:
    def test_no_org_keyword_no_candidates(self):
        soup = BeautifulSoup(NO_ORG_KEYWORD_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert len(candidates) == 0


class TestGemeindeDomainExcluded:
    def test_gemeinde_not_in_org_keywords(self):
        soup = BeautifulSoup(GEMEINDE_DOMAIN_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        # "gemeinde" is not an org keyword → no candidates
        assert len(candidates) == 0


class TestMultipleLinks:
    def test_multiple_org_links(self):
        soup = BeautifulSoup(MULTIPLE_ORG_LINKS_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        names = [c.name for c in candidates]
        # Should find both organizations
        assert any("Sportclub" in n for n in names)
        assert any("Musikverein" in n for n in names)


class TestDomainOnlyInference:
    def test_domain_inference_when_anchor_is_generic(self):
        soup = BeautifulSoup(DOMAIN_ONLY_ORG_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        # "Anmeldung" is generic anchor text → filtered
        anchor_candidates = [c for c in candidates if c.evidence_ref.startswith("anchor:")]
        assert len(anchor_candidates) == 0
        # But domain "turnverein-stefa.ch" has org keyword → domain inference works
        domain_candidates = [c for c in candidates if c.evidence_ref.startswith("domain:")]
        assert len(domain_candidates) >= 1


class TestEdgeCases:
    def test_empty_soup(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        assert candidates == []

    def test_no_base_url(self):
        soup = BeautifulSoup(ORG_LINK_HTML, "html.parser")
        candidates = extractor.extract(soup, "")
        assert candidates == []

    def test_evidence_excerpt_present(self):
        soup = BeautifulSoup(ORG_LINK_HTML, "html.parser")
        candidates = extractor.extract(soup, BASE_URL)
        for c in candidates:
            assert c.evidence_excerpt is not None
            assert len(c.evidence_excerpt) > 0
