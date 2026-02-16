# Crawler Time Extraction Audit

## 0. Implemented Improvements

### P0 — JSON-LD and `<time>` Extraction (2026-02-09)

- Added `src/sources/structured_time.py` with reusable helpers for all adapters
- JSON-LD Event `startDate`/`endDate` extraction (supports `@type` as string or array)
- HTML `<time datetime="...">` extraction with smart candidate selection:
  - Prefers candidates with time component over date-only
  - Searches within event container first, then full page
  - Picks closest future datetime when multiple candidates exist
- ISO 8601 parsing using `datetime.fromisoformat` (timezone-safe, no dateparser)
- Updated `_has_time_hint()` to recognize ISO datetime format
- Uses ` | ` separator for unambiguous start/end ISO strings
- Integrated into `MaennedorfPortalAdapter` as first consumer
- Preserves existing text heuristics as fallback

**Note:** This commit adds support; it will only affect sources that expose structured time. Maennedorf portal continues using text heuristics since it doesn't provide JSON-LD or `<time>` markup.

---

## Status

This document was originally an audit (2026-02-08). Section 0 tracks
implemented improvements. Sections 1–7 are updated to reflect current
state (last reviewed: 2026-02).

---

## 1. Where Extraction Happens

### File Paths & Key Functions

| File | Function | Purpose |
|------|----------|---------|
| `src/normalize.py` | `parse_datetime_or_range()` | Main datetime parser |
| `src/normalize.py` | `_has_time_hint()` | Heuristic: does raw string contain time info? |
| `src/normalize.py` | `_parse_with_dateparser()` | Fallback parser (dateparser library) |
| `src/normalize.py` | `raw_to_normalized()` | Orchestrates parsing + sets `date_precision` |
| `src/sources/adapters/*.py` | `fetch()` / `_extract_from_detail()` | Source-specific extraction |
| `src/sources/types.py` | `ExtractedItem` | Raw extraction result (contains `datetime_raw`) |

### Data Flow

```
Source HTML/Page
       ↓
Adapter.fetch() → ExtractedItem(datetime_raw="22. Jan. 2026, 18.00 Uhr")
       ↓
raw_to_normalized() → parse_datetime_or_range(datetime_raw)
       ↓
NormalizedEvent(start_at, end_at, date_precision)
```

---

## 2. Current Heuristics

### Time Detection (`_has_time_hint`)

The function checks if the raw datetime string contains time information:

```python
# Matches:
- \b\d{1,2}:\d{2}\b     # "15:00"
- \b\d{1,2}\.\d{2}\s*uhr\b  # "18.00 Uhr"
- "uhr" in string       # any mention of "Uhr"
```

**If time hint found:** `date_precision = 'datetime'`
**If no time hint:** `date_precision = 'date'` (all-day)

### Datetime Parsing (`parse_datetime_or_range`)

Supports these patterns (in order of precedence):

| Pattern | Regex | Example |
|---------|-------|---------|
| Numeric date range | `DD.MM.YYYY - DD.MM.YYYY` | `06.01.2026 - 10.02.2026` |
| German range + time | `D. Mon. YYYY - D. Mon. YYYY, HH.MM Uhr` | `6. Jan. 2026 - 10. Feb. 2026, 14.00 Uhr - 14.45 Uhr` |
| German single + time | `D. Mon. YYYY, HH.MM Uhr - HH.MM Uhr` | `22. Jan. 2026, 18.00 Uhr - 23.00 Uhr` |
| Fallback | dateparser library | `Sa, 24.01.2026, 15:00` |

### Adapter-Specific Extraction

| Adapter | Status | Extraction Strategy |
|---------|--------|---------------------|
| `MaennedorfPortalAdapter` | ✅ Implemented | HTML lead container; prefers lines with "Uhr"; text heuristic fallback (Tier B) |
| `EventbriteAdapter` | ✅ Implemented | JSON-LD primary (`extract_jsonld_event`); text heuristic fallback; non-JS first, JS fallback (Tier A) |
| `ChurchHubAdapter` | ⚠️ TODO stub | Not implemented |
| `KinoWildenmannAdapter` | ⚠️ TODO stub | Not implemented |
| `VereinsDirectoryAdapter` | ⚠️ Discovery only | Returns no events |

---

## 3. Common Failures

### 3.1 Missing Time → `date_precision='date'`

**Cause:** Raw string has no recognizable time pattern.

| Failure | Example | Result |
|---------|---------|--------|
| No "Uhr" keyword | `"22. Jan. 2026"` | `date_precision='date'` |
| Time in 24h format without "Uhr" | `"22.01.2026 15:00"` | May detect `:` but misses end time |
| Time embedded in prose | `"Beginn um 15 Uhr"` | May work, but inconsistent |

### 3.2 Missing End Time → `end_at=NULL`

**Cause:** Source only provides start time, or end time is in different location.

| Failure | Example | Result |
|---------|---------|--------|
| Single time only | `"22. Jan. 2026, 18.00 Uhr"` | `end_at=NULL` |
| End time on separate line | `"Start: 14:00\nEnd: 16:00"` | Only first line captured |
| Duration instead of end | `"2 Stunden"` | Not parsed to `end_at` |

### 3.3 Format Mismatch

| Failure | Example | Result |
|---------|---------|--------|
| English month names | `"Jan 22, 2026"` | Falls back to dateparser (may work) |
| Non-standard separators | `"22/01/2026"` | May fail regex, dateparser fallback |
| ISO format | `"2026-01-22T15:00:00"` | ✅ Now handled by `structured_time.py` and `_has_time_hint()` |

### 3.4 Structural Extraction Issues

| Source Pattern | Status |
|----------------|--------|
| **HTML tables** | Not parsed; datetime may be in `<td>` cells |
| **JSON-LD / Schema.org** | ✅ **Implemented** — `extract_jsonld_event()` in `structured_time.py`. Used by Eventbrite adapter. |
| **HTML `<time>` elements** | ✅ **Implemented** — `extract_time_element()` in `structured_time.py`. |
| **ICS/Calendar embeds** | Not parsed; `.ics` links not followed |
| **PDFs** | Not fetched; PDF content inaccessible |
| **JavaScript-rendered** | Partial support (Playwright); JS fallback used by Eventbrite + Maennedorf |

### 3.5 Multi-Line / Split Datetime

**Cause:** Datetime spans multiple lines in source HTML.

```html
<p>Datum: 22. Januar 2026</p>
<p>Zeit: 18:00 - 20:00 Uhr</p>
```

**Current behavior:** Only one line is captured (usually date without time).

---

## 4. Source Pattern Coverage

| Pattern Type | Current Support | Notes |
|--------------|-----------------|-------|
| **1. HTML visible text** | ✅ Partial | Works for lead containers, misses tables |
| **2. JSON-LD Event markup** | ✅ Implemented | `extract_jsonld_event()` — supports Event + subtypes (e.g. `SocialEvent`) |
| **3. HTML `<time>` elements** | ✅ Implemented | `extract_time_element()` — smart candidate selection |
| **4. HTML tables** | ❌ Not implemented | Common in event listings |
| **5. PDFs** | ❌ Not implemented | Would need PDF parsing library |
| **6. Calendar embeds / ICS** | ❌ Not implemented | Would need ICS parser |

---

## 5. Prioritized Improvement Ideas

### P0 — High Impact, Low Effort (DONE)

| Improvement | Status |
|-------------|--------|
| **Extract JSON-LD `startDate`/`endDate`** | ✅ Implemented in `structured_time.py` (2026-02-09) |
| **Parse `<time datetime="...">` tags** | ✅ Implemented in `structured_time.py` (2026-02-09) |
| **Improve `_has_time_hint()` to catch ISO format** | ✅ Updated to recognize ISO datetime strings |

### P1 — Medium Impact, Medium Effort

| Improvement | Rationale |
|-------------|-----------|
| **Table extraction** | Many event listings use HTML tables with date/time columns |
| **Multi-line datetime aggregation** | Combine date + time from adjacent elements |
| **Duration → end_at inference** | Parse "2 Stunden" to compute `end_at` from `start_at` |
| **Implement ChurchHubAdapter** | Unlocks church event source |
| **Implement KinoWildenmannAdapter** | Unlocks cinema showtimes |

### P2 — Lower Priority / Higher Effort

| Improvement | Rationale |
|-------------|-----------|
| **ICS calendar parsing** | Follow `.ics` links, parse iCalendar format |
| **PDF text extraction** | Requires pdfplumber or similar; complex layout handling |
| **More robust fallback chain** | Try multiple parsing strategies per source |

---

## 6. Metrics to Track

Once improvements are implemented, track:

| Metric | Query |
|--------|-------|
| % with `date_precision='date'` | `SELECT count(*) FILTER (WHERE date_precision='date') * 100.0 / count(*) FROM events;` |
| % with `end_at IS NULL` | `SELECT count(*) FILTER (WHERE end_at IS NULL) * 100.0 / count(*) FROM events;` |
| % ongoing-eligible | Events with both `start_at` and `end_at` and duration ≤ 12h |

---

## 7. Summary

**Current state (Feb 2026):**
- JSON-LD and `<time>` extraction implemented (`structured_time.py`)
- Two adapters fully operational: `MaennedorfPortalAdapter` (Tier B), `EventbriteAdapter` (Tier A)
- German-specific regex + dateparser as fallback for non-structured sources
- Tables, PDFs, and ICS calendars are not parsed

**Remaining impact:**
- Non-structured sources still get `date_precision='date'` when time exists but isn't in JSON-LD or `<time>`
- Some events have `end_at=NULL` when end time exists but is not captured
- Ongoing detection fails for events missing `end_at`

**Next steps (deferred):**
- Complete stub adapters (ChurchHub, KinoWildenmann)
- Table extraction for HTML-table-based event listings
- Track metrics to measure ongoing improvement
