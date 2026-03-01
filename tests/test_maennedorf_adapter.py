# tests/test_maennedorf_adapter.py
"""
Unit tests for maennedorf_portal adapter enrichment functions.

Tests login overlay detection, attachment extraction, outbound URL extraction,
and organizer name extraction — all pure functions operating on HTML strings.
"""
from __future__ import annotations

from bs4 import BeautifulSoup

from src.sources.adapters.maennedorf_portal import (
    _extract_attachment_urls,
    _extract_outbound_urls,
    _extract_organizer_name,
    _is_login_overlay,
)


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

LOGIN_OVERLAY_HTML = """
<html>
<head><title>Männedorf Portal</title></head>
<body>
<div class="overlay-login">
  <h2>Zugang eingeschränkt</h2>
  <p>LOGIN mit Benutzerkonto</p>
  <form>
    <input type="text" name="username" />
    <input type="password" name="password" />
    <button type="submit">Anmelden</button>
  </form>
</div>
</body>
</html>
"""

PDF_ATTACHMENT_HTML = """
<html>
<head><title>Ferienpass 2026</title></head>
<body>
<main>
  <h1>Ferienpass 2026</h1>
  <div class="icms-lead-container">
    <p>Gemeindehaus</p>
    <p>Montag, 6. Juli 2026</p>
  </div>
  <div class="content">
    <p>Das Programm als Download:</p>
    <a href="/dokumente/ferienpass_2026.pdf">Programm herunterladen (PDF)</a>
    <a href="/bilder/flyer.jpg">Flyer ansehen</a>
    <a href="/bilder/plakat.png">Plakat ansehen</a>
    <a href="/dokumente/ferienpass_2026.pdf">Programm nochmals</a>
  </div>
</main>
</body>
</html>
"""

OUTBOUND_LINKS_HTML = """
<html>
<head><title>Yoga im Park</title></head>
<body>
<main>
  <h1>Yoga im Park</h1>
  <div class="icms-lead-container">
    <p>Seeanlage</p>
    <p>Mittwoch, 20. Mai 2026, 18.00 Uhr</p>
  </div>
  <div class="content">
    <p>Anmeldung über die Website des Anbieters:</p>
    <a href="https://yoga-zurichsee.ch/anmeldung">Anmeldung</a>
    <a href="https://yoga-zurichsee.ch/programm">Programm</a>
    <a href="/anlaesseaktuelles/123">Zurück zur Übersicht</a>
    <a href="mailto:info@yoga-zurichsee.ch">E-Mail</a>
    <a href="tel:+41441234567">Telefon</a>
    <a href="#">Anchor</a>
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
  <div class="icms-lead-container">
    <p>Dorfplatz</p>
    <p>Freitag, 11. November 2026, 18.00 Uhr</p>
  </div>
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
  <div class="icms-lead-container">
    <p>Bibliothek</p>
    <p>Samstag, 21. März 2026, 14.00 Uhr</p>
  </div>
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
  <div class="icms-lead-container">
    <p>Seeanlage</p>
    <p>Samstag, 27. Juni 2026, 11.00 Uhr</p>
  </div>
  <p>Ein Fest für die ganze Familie.</p>
</main>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Login overlay tests
# ---------------------------------------------------------------------------

class TestLoginOverlay:
    def test_detects_login_overlay(self):
        soup = BeautifulSoup(LOGIN_OVERLAY_HTML, "html.parser")
        assert _is_login_overlay(soup) is True

    def test_normal_page_not_login_overlay(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        assert _is_login_overlay(soup) is False

    def test_empty_page_not_login_overlay(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        assert _is_login_overlay(soup) is False


# ---------------------------------------------------------------------------
# Attachment extraction tests
# ---------------------------------------------------------------------------

class TestAttachmentExtraction:
    def test_extracts_pdf_jpg_png(self):
        soup = BeautifulSoup(PDF_ATTACHMENT_HTML, "html.parser")
        urls = _extract_attachment_urls(soup, "https://www.maennedorf.ch/_rte/anlass/500")
        assert len(urls) == 3  # pdf + jpg + png (deduped)
        extensions = [u.rsplit(".", 1)[-1].lower() for u in urls]
        assert "pdf" in extensions
        assert "jpg" in extensions
        assert "png" in extensions

    def test_deduplicates_same_href(self):
        soup = BeautifulSoup(PDF_ATTACHMENT_HTML, "html.parser")
        urls = _extract_attachment_urls(soup, "https://www.maennedorf.ch/_rte/anlass/500")
        pdf_urls = [u for u in urls if u.endswith(".pdf")]
        assert len(pdf_urls) == 1  # only one despite two <a> tags

    def test_no_attachments_normal_page(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        urls = _extract_attachment_urls(soup, "https://www.maennedorf.ch/_rte/anlass/100")
        assert urls == []

    def test_resolves_relative_urls(self):
        soup = BeautifulSoup(PDF_ATTACHMENT_HTML, "html.parser")
        urls = _extract_attachment_urls(soup, "https://www.maennedorf.ch/_rte/anlass/500")
        for u in urls:
            assert u.startswith("https://www.maennedorf.ch/")


# ---------------------------------------------------------------------------
# Outbound URL extraction tests
# ---------------------------------------------------------------------------

class TestOutboundUrlExtraction:
    def test_extracts_external_links(self):
        soup = BeautifulSoup(OUTBOUND_LINKS_HTML, "html.parser")
        urls = _extract_outbound_urls(soup, "https://www.maennedorf.ch/_rte/anlass/200")
        assert len(urls) == 2  # two yoga-zurichsee.ch links
        for u in urls:
            assert "yoga-zurichsee.ch" in u

    def test_skips_same_host(self):
        soup = BeautifulSoup(OUTBOUND_LINKS_HTML, "html.parser")
        urls = _extract_outbound_urls(soup, "https://www.maennedorf.ch/_rte/anlass/200")
        for u in urls:
            assert "maennedorf.ch" not in u

    def test_skips_mailto_tel_anchor(self):
        soup = BeautifulSoup(OUTBOUND_LINKS_HTML, "html.parser")
        urls = _extract_outbound_urls(soup, "https://www.maennedorf.ch/_rte/anlass/200")
        for u in urls:
            assert not u.startswith("mailto:")
            assert not u.startswith("tel:")
            assert u != "#"

    def test_no_outbound_on_normal_page(self):
        soup = BeautifulSoup(NO_ORGANIZER_HTML, "html.parser")
        urls = _extract_outbound_urls(soup, "https://www.maennedorf.ch/_rte/anlass/300")
        assert urls == []


# ---------------------------------------------------------------------------
# Organizer name extraction tests
# ---------------------------------------------------------------------------

class TestOrganizerNameExtraction:
    def test_inline_veranstalter_label(self):
        soup = BeautifulSoup(NORMAL_EVENT_HTML, "html.parser")
        name = _extract_organizer_name(soup)
        assert name == "Elternverein Männedorf"

    def test_dt_dd_veranstalter(self):
        soup = BeautifulSoup(ORGANIZER_DT_DD_HTML, "html.parser")
        name = _extract_organizer_name(soup)
        assert name == "Quartierverein Dorfkern"

    def test_table_organisator(self):
        soup = BeautifulSoup(ORGANIZER_TABLE_HTML, "html.parser")
        name = _extract_organizer_name(soup)
        assert name == "Bibliothek Männedorf"

    def test_no_organizer_returns_none(self):
        soup = BeautifulSoup(NO_ORGANIZER_HTML, "html.parser")
        name = _extract_organizer_name(soup)
        assert name is None

    def test_empty_html_returns_none(self):
        soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        name = _extract_organizer_name(soup)
        assert name is None
