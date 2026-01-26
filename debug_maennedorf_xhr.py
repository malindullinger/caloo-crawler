from playwright.sync_api import sync_playwright

URL = "https://www.maennedorf.ch/anlaesseaktuelles?datumVon=22.01.2026&datumBis=30.12.2026"

KEYWORDS = [
    "json", "api", "ajax", "anlass", "events", "event", "search", "query",
    "_rte", "anlaesse", "i-web", "guest", "getImageString"
]

def interesting(u: str) -> bool:
    u_l = (u or "").lower()
    return any(k in u_l for k in KEYWORDS)

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # set True later
        page = browser.new_page()

        def on_request(req):
            if req.resource_type in ("xhr", "fetch") and interesting(req.url):
                print("XHR ->", req.method, req.url)

        def on_response(res):
            if interesting(res.url) and res.request.resource_type in ("xhr", "fetch"):
                try:
                    ct = (res.headers.get("content-type") or "").lower()
                    status = res.status
                    print("RESP <-", status, res.url, "content-type:", ct)
                    # If JSON, print first ~500 chars to identify structure
                    if "json" in ct:
                        txt = res.text()
                        print("JSON snippet:", txt[:500].replace("\n", " "))
                except Exception as e:
                    print("RESP read failed:", res.url, repr(e))

        page.on("request", on_request)
        page.on("response", on_response)

        page.goto(URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(3000)

        print("\nDONE. Keep the browser open a moment; scroll a bit if needed.\n")
        page.wait_for_timeout(10000)

        browser.close()

if __name__ == "__main__":
    main()
