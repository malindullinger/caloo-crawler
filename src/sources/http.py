from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class HttpResult:
    url: str
    status_code: int
    text: str


def http_get(url: str, *, render_js: bool = False, timeout_s: int = 30) -> HttpResult:
    print(f"http_get(): render_js={render_js} url={url}")

    if not render_js:
        r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"})
        return HttpResult(url=r.url, status_code=r.status_code, text=r.text)

    # --- Playwright branch ---
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
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

        print("http_get(): Playwright used. final_url=", final_url, " html_len=", len(html))
        print("http_get(): contains /_rte/anlass/ =", "/_rte/anlass/" in html)
        print("http_get(): contains /anlaesseaktuelles/ =", "/anlaesseaktuelles/" in html)

        browser.close()
        return HttpResult(url=final_url, status_code=200, text=html)
