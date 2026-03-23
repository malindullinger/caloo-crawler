"""
Phase 7C.1 — Content surface measurement.

Scans HTML detail pages for non-HTML content surfaces (PDFs, external links).
This is MEASUREMENT ONLY: records what exists, does not extract content from them.
"""

from __future__ import annotations

from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

# Schemes to skip when scanning links
_SKIP_SCHEMES = {"mailto", "tel", "javascript"}

# Domain prefixes that indicate CDN/asset hosts, not real external links
_CDN_PREFIXES = ("cdn.", "fonts.", "static.")


def scan_content_surfaces(soup: BeautifulSoup, page_url: str) -> dict:
    """
    Scan a detail page for non-HTML content surfaces.
    This is MEASUREMENT — records what exists, not what we extract.

    Returns dict to merge into extra{}:
      pdf_urls: list[str]       - all PDF link hrefs found
      pdf_count: int            - len(pdf_urls)
      external_links: list[dict] - [{url, text}]
      external_link_count: int  - len(external_links)
    """
    pdf_urls = _find_pdfs(soup, page_url)
    external_links = _find_external_links(soup, page_url)

    return {
        "pdf_urls": pdf_urls,
        "pdf_count": len(pdf_urls),
        "external_links": external_links,
        "external_link_count": len(external_links),
    }


def _find_pdfs(soup: BeautifulSoup, page_url: str) -> list[str]:
    """Find all links pointing to PDF files, deduplicated."""
    seen: set[str] = set()
    results: list[str] = []

    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        link_text: str = a.get_text(strip=True)

        is_pdf_href = href.lower().endswith(".pdf")
        is_pdf_text = "pdf" in link_text.lower() if link_text else False

        if is_pdf_href or is_pdf_text:
            absolute = urljoin(page_url, href)
            if absolute not in seen:
                seen.add(absolute)
                results.append(absolute)

    return results


def _find_external_links(soup: BeautifulSoup, page_url: str) -> list[dict]:
    """Find all links pointing to external domains, deduplicated by URL."""
    page_domain = urlparse(page_url).netloc.lower()

    seen: set[str] = set()
    results: list[dict] = []

    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()

        # Skip empty hrefs and anchors
        if not href or href.startswith("#"):
            continue

        parsed = urlparse(href)

        # Skip non-http schemes
        if parsed.scheme and parsed.scheme.lower() in _SKIP_SCHEMES:
            continue

        # Make absolute
        absolute = urljoin(page_url, href)
        abs_parsed = urlparse(absolute)
        link_domain = abs_parsed.netloc.lower()

        # Must have a domain and it must differ from the page domain
        if not link_domain or link_domain == page_domain:
            continue

        # Skip CDN/asset domains
        if any(link_domain.startswith(prefix) for prefix in _CDN_PREFIXES):
            continue

        if absolute not in seen:
            seen.add(absolute)
            results.append({
                "url": absolute,
                "text": a.get_text(strip=True) or "",
            })

    return results
