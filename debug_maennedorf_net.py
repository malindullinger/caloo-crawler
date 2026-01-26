from playwright.sync_api import sync_playwright

URL = "https://www.maennedorf.ch/anlaesseaktuelles?datumVon=22.01.2026&datumBis=30.12.2026"

BLOCK_RESOURCE_TYPES = {"image", "font", "stylesheet", "media"}

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)

        context = browser.new_context(
            locale="de-CH",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
            },
            record_har_path="maennedorf.har",
            record_har_content="embed",
        )

        page = context.new_page()

        xhr_count = 0
        rate_limited = []

        # Block heavy assets to lower request volume
        def route_handler(route, request):
            if request.resource_type in BLOCK_RESOURCE_TYPES:
                return route.abort()
            return route.continue_()

        page.route("**/*", route_handler)

        def on_request(req):
            nonlocal xhr_count
            if req.resource_type in ("xhr", "fetch"):
                xhr_count += 1
                print(f"XHR -> {req.method} {req.url}")

        def on_response(res):
            if res.status == 429:
                rate_limited.append(res.url)
                print(f"🚫 429 <- {res.url}")

            req = res.request
            if req.resource_type in ("xhr", "fetch"):
                ct = (res.headers.get("content-type") or "")
                print(f"RESP <- {res.status} {res.url}  ct={ct}")

        def on_console(msg):
            print(f"CONSOLE [{msg.type}]: {msg.text}")

        page.on("request", on_request)
        page.on("response", on_response)
        page.on("console", on_console)

        resp = page.goto(URL, wait_until="domcontentloaded")
        if resp:
            print("\nMAIN DOC status:", resp.status, "url:", resp.url)
        else:
            print("\nMAIN DOC status: (no response object)")

        page.wait_for_timeout(2000)
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(2500)

        html = page.content()
        with open("maennedorf_rendered.html", "w", encoding="utf-8") as f:
            f.write(html)

        print("\nSaved: maennedorf_rendered.html")
        print("Saved: maennedorf.har")
        print("Total XHR/fetch seen:", xhr_count)

        if rate_limited:
            print("\n429 URLs (unique):")
            for u in sorted(set(rate_limited))[:50]:
                print(" -", u)
            if len(set(rate_limited)) > 50:
                print(" ... (more omitted)")
        else:
            print("\nNo 429 responses detected in this run ✅")

        print("\nDONE. Keep the browser open a moment; scroll a bit if needed.")
        page.wait_for_timeout(8000)

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
