from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple

import requests


@dataclass
class HttpResult:
    url: str
    status_code: int
    text: str


class SuspiciousResponseError(Exception):
    """Raised when an HTTP response appears to be a block, challenge, or error page."""


# HTTP status codes that indicate blocking or unavailability.
# 404 is intentionally excluded — a removed detail page is normal, not suspicious.
_SUSPICIOUS_STATUS_CODES = {403, 429, 503}

# Content patterns checked against the first 5KB of response body.
# Each is (compiled regex, human-readable label).
_SUSPICIOUS_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    # Cloudflare
    (re.compile(r"challenges\.cloudflare\.com", re.IGNORECASE), "Cloudflare challenge"),
    (re.compile(r"<title>\s*Just a moment\.\.\.\s*</title>", re.IGNORECASE), "Cloudflare waiting page"),
    # WAF / access denied
    (re.compile(r"<title>\s*Access Denied\s*</title>", re.IGNORECASE), "Access Denied page"),
    (re.compile(r"<title>\s*403\s+Forbidden\s*</title>", re.IGNORECASE), "403 Forbidden page"),
    # CDN error pages
    (re.compile(r"<title>\s*502\s+Bad Gateway\s*</title>", re.IGNORECASE), "502 Bad Gateway"),
    (re.compile(r"<title>\s*503\s+Service\b.*?Unavailable\s*</title>", re.IGNORECASE), "503 Service Unavailable"),
]


def _check_suspicious(result: HttpResult) -> None:
    """Check if an HTTP response looks like a block/error/challenge page.

    Raises SuspiciousResponseError if suspicious patterns are detected.
    Called automatically by http_get() before returning results.
    """
    # 1) Check HTTP status code
    if result.status_code in _SUSPICIOUS_STATUS_CODES:
        msg = f"[http] SUSPICIOUS: HTTP {result.status_code} from {result.url}"
        print(msg)
        raise SuspiciousResponseError(msg)

    # 2) Check response content for known block/challenge/error patterns
    text = result.text or ""
    if not text:
        return

    # Only scan first 5KB — block/error pages are short
    head = text[:5000]
    for pattern, label in _SUSPICIOUS_PATTERNS:
        if pattern.search(head):
            msg = f"[http] SUSPICIOUS: {label} detected in response from {result.url}"
            print(msg)
            raise SuspiciousResponseError(msg)


def http_get(url: str, *, render_js: bool = False, timeout_s: int = 30) -> HttpResult:
    if not render_js:
        import time as _time
        max_retries = 3
        for attempt in range(max_retries):
            r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 429 and attempt < max_retries - 1:
                wait = 5 * (attempt + 1)  # 5s, 10s backoff
                print(f"[http] 429 rate limited, retrying in {wait}s: {url}")
                _time.sleep(wait)
                continue
            result = HttpResult(url=r.url, status_code=r.status_code, text=r.text)
            _check_suspicious(result)
            return result
        # Should not reach here, but just in case
        result = HttpResult(url=r.url, status_code=r.status_code, text=r.text)
        _check_suspicious(result)
        return result

    # --- Playwright branch ---
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, timeout=30000)
        page = browser.new_page()

        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)

        # Give client JS time
        page.wait_for_timeout(2000)

        # Some portals lazy-load after scroll
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(1500)

        # Let network settle
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_s * 1000)
        except Exception:
            # Some pages never go fully idle — not fatal
            pass

        html = page.content()
        final_url = page.url

        browser.close()
        result = HttpResult(url=final_url, status_code=200, text=html)
        _check_suspicious(result)
        return result
