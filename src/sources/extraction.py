"""Shared extraction helpers for adapter detail pages.

Reusable building blocks for extracting title, description, image, and other
fields from HTML detail pages. Used by multiple adapters.
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag


def extract_title(
    soup: BeautifulSoup,
    *,
    primary_selector: str | None = None,
    strip_title_suffix: str | None = None,
) -> str | None:
    """Extract event title from an HTML page.

    Strategy (first match wins):
    1) primary_selector CSS match (if provided)
    2) <h1> element
    3) og:title meta tag
    4) <title> tag (optionally stripping suffix like " | Site Name")

    Returns stripped title string, or None if nothing found.
    """
    # 1) Custom primary selector
    if primary_selector:
        el = soup.select_one(primary_selector)
        if el and el.get_text(strip=True):
            return el.get_text(" ", strip=True)

    # 2) <h1>
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(" ", strip=True)

    # 3) og:title
    og = soup.find("meta", property="og:title")
    if og and (og.get("content") or "").strip():
        return og["content"].strip()

    # 4) <title> with optional suffix stripping
    if soup.title:
        raw = soup.title.get_text(" ", strip=True)
        if raw:
            if strip_title_suffix and strip_title_suffix in raw:
                return raw.split(strip_title_suffix)[0].strip()
            return raw

    return None


# Minimum image dimensions (pixels) to consider an image "meaningful"
_MIN_IMAGE_DIM = 100
# Patterns for images to skip (icons, logos, tracking pixels, avatars)
_SKIP_IMAGE_RE = re.compile(
    r"(logo|icon|avatar|badge|sprite|pixel|tracking|spacer|button"
    r"|favicon|spinner|loader|arrow|chevron|breadcrumb|pfeil|nav[_-]"
    r"|bullet|separator|divider|bg[_-]|background)",
    re.IGNORECASE,
)


def extract_image(
    soup: BeautifulSoup,
    *,
    page_url: str | None = None,
) -> str | None:
    """Extract the best event image from an HTML page.

    Strategy (first match wins):
    1) og:image meta tag
    2) JSON-LD Event.image
    3) First meaningful <img> in main content (skip tiny/logo/icon images)

    Returns absolute URL or None.
    """

    def _abs(url: str) -> str:
        """Make URL absolute if page_url is available."""
        if not url:
            return ""
        if url.startswith(("http://", "https://", "//")):
            if url.startswith("//"):
                return "https:" + url
            return url
        if page_url:
            return urljoin(page_url, url)
        return url

    # 1) og:image
    og = soup.find("meta", property="og:image")
    if og and (og.get("content") or "").strip():
        url = _abs(og["content"].strip())
        if url:
            return url

    # 2) JSON-LD Event.image
    import json
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text() or "")
            items = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "Event":
                    continue
                img = item.get("image")
                if isinstance(img, str) and img.strip():
                    return _abs(img.strip())
                if isinstance(img, list) and img:
                    first = img[0]
                    if isinstance(first, str) and first.strip():
                        return _abs(first.strip())
                    if isinstance(first, dict) and first.get("url"):
                        return _abs(first["url"].strip())
                if isinstance(img, dict) and img.get("url"):
                    return _abs(img["url"].strip())
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    # 3) First meaningful <img> in content
    for img_tag in soup.find_all("img", src=True):
        if not isinstance(img_tag, Tag):
            continue
        src = (img_tag.get("src") or "").strip()
        if not src:
            continue

        # Skip data URIs and tiny inline images
        if src.startswith("data:"):
            continue

        # Skip images matching skip patterns
        full_str = f"{src} {img_tag.get('alt', '')} {img_tag.get('class', '')}"
        if _SKIP_IMAGE_RE.search(full_str):
            continue

        # Skip explicitly small images
        w = img_tag.get("width")
        h = img_tag.get("height")
        try:
            if w and int(str(w).replace("px", "")) < _MIN_IMAGE_DIM:
                continue
            if h and int(str(h).replace("px", "")) < _MIN_IMAGE_DIM:
                continue
        except (ValueError, TypeError):
            pass

        return _abs(src)

    return None


def extract_description(
    soup: BeautifulSoup,
    *,
    primary_selector: str | None = None,
    max_length: int = 4000,
) -> str | None:
    """Extract the main event description from an HTML page.

    Strategy (first match wins):
    1) primary_selector CSS match (if provided) — full text content
    2) Common content selectors (article, .event-description, .entry-content, etc.)
    3) og:description meta tag (shortest fallback)

    Returns cleaned text or None. Preserves paragraph breaks as double newlines.
    """

    def _clean(el: Tag) -> str | None:
        """Extract text from an element, preserving paragraph breaks."""
        if not el:
            return None

        # Get text with paragraph separation
        parts: list[str] = []
        for child in el.children:
            if isinstance(child, Tag):
                if child.name in ("p", "div", "br", "h2", "h3", "h4", "li"):
                    txt = child.get_text(" ", strip=True)
                    if txt:
                        parts.append(txt)
                elif child.name in ("ul", "ol"):
                    for li in child.find_all("li"):
                        txt = li.get_text(" ", strip=True)
                        if txt:
                            parts.append(f"• {txt}")
                else:
                    txt = child.get_text(" ", strip=True)
                    if txt:
                        parts.append(txt)
            elif hasattr(child, "strip"):
                txt = child.strip()
                if txt:
                    parts.append(txt)

        if not parts:
            # Fallback: plain get_text
            txt = el.get_text(" ", strip=True)
            if txt and len(txt) > 20:
                return txt[:max_length]
            return None

        text = "\n\n".join(parts)
        return text[:max_length] if text else None

    # 1) Primary selector
    if primary_selector:
        el = soup.select_one(primary_selector)
        if el:
            result = _clean(el)
            if result and len(result) > 20:
                return result

    # 2) Common content selectors
    for selector in [
        "[data-testid='event-description']",
        ".event-description",
        ".event-details__description",
        ".rte",
        ".detail-content",
        "article .entry-content",
        ".structured-content",
        "article",
    ]:
        el = soup.select_one(selector)
        if el:
            result = _clean(el)
            if result and len(result) > 50:
                return result

    # 3) og:description fallback
    og = soup.find("meta", property="og:description")
    if og and (og.get("content") or "").strip():
        desc = og["content"].strip()
        if len(desc) > 20:
            return desc[:max_length]

    # 4) meta description fallback
    meta = soup.find("meta", attrs={"name": "description"})
    if meta and (meta.get("content") or "").strip():
        desc = meta["content"].strip()
        if len(desc) > 20:
            return desc[:max_length]

    return None
