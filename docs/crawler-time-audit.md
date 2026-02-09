# Crawler Time Extraction Audit

## Out of Scope

**This document is an audit only.** No crawler logic, SQL, or views are changed as part of this task. Implementation of improvements is deferred.

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
| `MaennedorfPortalAdapter` | ✅ Implemented | HTML lead container; prefers lines with "Uhr", falls back to lines with year/date |
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
| ISO format | `"2026-01-22T15:00:00"` | Not explicitly handled, dateparser fallback |

### 3.4 Structural Extraction Issues

| Source Pattern | Failure Mode |
|----------------|--------------|
| **HTML tables** | Not parsed; datetime may be in `<td>` cells |
| **JSON-LD / Schema.org** | Not extracted; `startDate`/`endDate` ignored |
| **ICS/Calendar embeds** | Not parsed; `.ics` links not followed |
| **PDFs** | Not fetched; PDF content inaccessible |
| **JavaScript-rendered** | Partial support (Playwright); some dynamic content missed |

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
| **2. `<time>` tags / structured data** | ❌ Not implemented | JSON-LD, microdata, Schema.org ignored |
| **3. HTML tables** | ❌ Not implemented | Common in event listings |
| **4. PDFs** | ❌ Not implemented | Would need PDF parsing library |
| **5. Calendar embeds / ICS** | ❌ Not implemented | Would need ICS parser |

---

## 5. Prioritized Improvement Ideas

### P0 — High Impact, Low Effort

| Improvement | Rationale |
|-------------|-----------|
| **Extract JSON-LD `startDate`/`endDate`** | Many sites include Schema.org Event markup; provides exact ISO datetimes |
| **Parse `<time datetime="...">` tags** | HTML5 standard; contains machine-readable datetime |
| **Improve `_has_time_hint()` to catch `HH:MM` without "Uhr"** | Reduces false `date_precision='date'` |

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

**Current state:**
- Time extraction relies on German-specific regex + dateparser fallback
- Only `MaennedorfPortalAdapter` is fully implemented
- Structured data (JSON-LD, `<time>` tags) is not used
- Tables, PDFs, and ICS calendars are not parsed

**Impact:**
- Some events get `date_precision='date'` when time is available but not recognized
- Many events have `end_at=NULL` when end time exists but is not captured
- Ongoing detection fails for events missing `end_at`

**Next steps (deferred):**
- Implement P0 improvements (JSON-LD, `<time>` tags, better time hint detection)
- Complete stub adapters
- Track metrics to measure improvement
