WEB SCRAPING GUIDE — WHAT WE DID WRONG & HOW WE FIXED IT

Context:
This guide describes lessons learned while building a large-scale scraping system for financial documents. The system collects annual reports, earnings transcripts, and governance documents from ~260 companies across 15+ countries.

Many failures were discovered only after months of silent data corruption.

The purpose of this document is to describe the failures and the architectural fixes.

---

CORE PROBLEM

Financial document collection sounds simple:

find PDF → download PDF → store PDF

In practice the real problems included:

• IR websites returning HTML error pages but setting content-type: application/pdf
• CDNs (Akamai / Cloudflare) blocking Python requests but allowing wget
• The same ticker symbol returning different companies depending on the provider
• Missing transcript coverage for some companies
• Source URLs going stale within months
• JS-rendered pages returning empty HTML to simple HTTP clients

Many of these issues caused silent data corruption.

---

ARCHITECTURE APPROACH

Instead of relying on a single scraping method, a layered fallback strategy was used:

SEC EDGAR → Financial APIs → regulatory sources → Wayback Machine → web scraping

For downloading files, a multi-strategy downloader was implemented that tries:

1. Direct HTTP request
2. Proxy request
3. wget with Safari headers
4. wget with proxy
5. curl fallback

The key insight: different tools present different TLS fingerprints.

Some CDNs block Python requests but allow wget.

Example:

FAILED:
requests.get(url, headers={"User-Agent": SAFARI_UA}) → 403

WORKED:
wget with Safari user-agent → 200 OK

---

MISTAKE 1 — TRUSTING CONTENT-TYPE HEADERS

Problem:
Some servers returned HTML error pages but labeled them as PDFs.

Result:
Corrupted files were stored.

Fix:
Always validate file contents using magic bytes.

Example validation:

def validate_pdf(content: bytes) -> bool:
    if content[:5] != b'%PDF-':
        return False

    if len(content) < 10000:
        return False

    if b'<!DOCTYPE' in content[:100] or b'<html' in content[:100]:
        return False

    return True

Lesson:
Never trust HTTP headers. Validate the actual content.

---

MISTAKE 2 — PLAYWRIGHT INSTALLATION ERROR

Problem:
Playwright browsers were installed using the Node.js command:

npx playwright install chromium

But the scraping system used Python Playwright.

Result:
Browser scraping silently returned zero results.

Fix:

python -m playwright install chromium --with-deps

Lesson:
Silent failures are dangerous. When a scraping step returns zero results, it should trigger warnings.

---

MISTAKE 3 — TICKER COLLISIONS

Problem:
Some providers returned the wrong company when using ticker symbols.

Example:
"SENS" matched different companies.

Fix:
Validate the returned company name against the expected company.

Example:

from difflib import SequenceMatcher

def validate_company_match(returned_name, expected_name):
    ratio = SequenceMatcher(None, returned_name.lower(), expected_name.lower()).ratio()
    return ratio >= 0.5

Lesson:
Tickers are not unique identifiers. Always verify entity identity.

---

MISTAKE 4 — NO CIRCUIT BREAKER FOR BLOCKED DOMAINS

Problem:
If a site blocked requests (403 or 429), the system kept retrying.

Result:
The site escalated blocking and banned the crawler.

Fix:
Introduce a domain-level circuit breaker.

Example behavior:

• If 3 consecutive failures occur
• Stop requesting the domain
• Wait 60 minutes before retrying

Lesson:
Retrying immediately after being blocked makes the problem worse.

---

MISTAKE 5 — SOURCE URL LOST IN PIPELINE

Problem:
Documents uploaded from Google Drive had no source URL attached.

Later, the reprocessing pipeline required source_url IS NOT NULL.

Result:
Thousands of documents were invisible to reprocessing.

Fix:

• Always preserve source_url in the ingestion pipeline
• Use stored PDFs as the durable source of truth
• Original URLs often go stale

Lesson:
The stored archive (S3, GDrive, etc.) must be the real source of truth.

---

BROWSER SCRAPING STRATEGY

JS-rendered pages are scraped using Playwright.

A tiered loading strategy is used:

1. Try networkidle (ideal but may timeout)
2. Fallback to domcontentloaded + wait
3. Final fallback: commit state

Example:

await page.goto(url, wait_until="networkidle", timeout=30000)

fallback:

await page.goto(url, wait_until="domcontentloaded", timeout=15000)
await page.wait_for_timeout(3000)

final fallback:

await page.goto(url, wait_until="commit", timeout=10000)

Lesson:
Different sites require different wait strategies.

---

ANTI-BOT STRATEGY

Stealth plugins were unreliable.

What actually worked:

1. Tool diversity
   Different HTTP clients have different TLS fingerprints.

2. Rate limiting
   Add cooldown between retries.

3. Proxy rotation
   Use country-specific proxies (Swiss proxy for Swiss sites).

4. Authentic browser headers
   Include realistic headers and language settings.

5. Session uniqueness
   Avoid request correlation.

Lesson:
Multiple legitimate strategies work better than one advanced bypass.

---

WEB SEARCH AS LAST RESORT

If all automated sources fail, a web search fallback is used.

Example query:

"{company_name} investor relations {year} annual report filetype:pdf"

Download candidates and validate them.

---

KEY TAKEAWAYS

1. Never trust HTTP content-type headers
2. wget may succeed when requests fails
3. Silent zero results should trigger warnings
4. Verify entity identity when using external APIs
5. Use circuit breakers for blocked domains
6. Store downloaded files as the source of truth
7. Use multiple scraping tools instead of stealth techniques
8. Python Playwright installation differs from Node.js Playwright

Main lesson:
Build scraping systems assuming that every step will eventually fail.