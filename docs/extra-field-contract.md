# Extra-Field Contract: Crawler to Transform

This document defines the contract between crawler adapters (producer) and
the canonical transform (consumer) for the `extra` field on `RawEvent` /
`event_raw.raw_payload.extra`.

**Data flow:**
```
Adapter → ExtractedItem.extra → RawEvent.extra → event_raw.raw_payload.extra → transformCanonical.ts
```

**Rules:**
- Absent keys default to `null` in the transform. No error is raised.
- The transform never infers values for missing keys.
- Type mismatches (e.g., string where number expected) cause the value to
  be silently dropped to `null`.

---

## Section 1 — Consumer Side (Transform Reads)

These are the `extra` keys that `transformCanonical.ts` actively reads and
maps to canonical model fields. If an adapter omits any of these, the
corresponding canonical field will be `null` or empty.

### Consumed Fields

| Key | Type | Maps to | Absent behavior | Notes |
|---|---|---|---|---|
| `image_url` | string | `source_happenings.image_url` | `null` | Normalized via `normalizeText()` (trim) |
| `organiser` | object `{name, url?, email?, contact_person?}` | `organizer` table + `source_happenings.organizer_name` | Empty string, no organizer created | See organizer resolution below |
| `organizer` | object `{name, url?}` or string | Same as `organiser` | Same | American spelling fallback |
| `price_type` | `"free"` \| `"paid"` \| `"donation"` | `offering.price_type` | `null` | |
| `price_from_chf` | number (float) | `offering.price_from_chf` | `null` | Rejected if not numeric type |
| `age_min` | number (int) | `occurrence.age_min` | `null` | Rejected if not numeric type |
| `age_max` | number (int) | `occurrence.age_max` | `null` | Rejected if not numeric type |
| `registration_url` | string (URL) | `offering.registration_url` | Falls back to `registration_url_from_links`, then `null` | |
| `registration_url_from_links` | string (URL) | `offering.registration_url` (fallback) | `null` | Only used when `registration_url` is absent |
| `category_raw` | string | `happening.topic_tags` (derived via keyword matching) | Skipped; title-based classification is fallback | Lowercased for matching |
| `ics_category` | string | `happening.topic_tags` (derived via keyword matching) | Skipped; title-based classification is fallback | Specific to ICS adapters |
| `categories` | string[] | `happening.topic_tags` (derived via keyword matching) | Skipped; title-based classification is fallback | Specific to ai1ec adapter |

### Organizer Resolution

`extractOrganizerName()` in `transformCanonical.ts` tries these shapes in
order (first non-empty wins):

1. `extra.organiser.name` (nested dict, British spelling)
2. `extra.organizer.name` (nested dict, American spelling)
3. `extra.organiser_name` (flat key, British)
4. `extra.organizer_name` (flat key, American)
5. `extra.organiser` as plain string (British)
6. `extra.organizer` as plain string (American)

**Preferred shape for new adapters:** `organiser: { name: "...", url: "..." }`

### Fields NOT Consumed by Transform

These keys are extracted by crawlers and stored in `event_raw.raw_payload.extra`
but are **not read** by `transformCanonical.ts`. They exist for diagnostics,
auditing, or future use.

| Key | Type | Purpose | Produced by |
|---|---|---|---|
| `adapter` | string | Identifies which adapter produced the item | All adapters |
| `extraction_method` | string | Documents how datetime was extracted | All adapters |
| `detail_parsed` | boolean | Whether detail page was fetched | Most adapters |
| `detail_page_fetched` | boolean | Whether detail enrichment completed | fluugepilz |
| `price_raw` | string | Unparsed price text | detail_fields.py |
| `age_raw` | string | Matched age expression text | detail_fields.py |
| `registration_raw` | string | Registration label/link text | detail_fields.py |
| `category_source` | string | Source of category (`"jsonld"`) | detail_fields.py |
| `pdf_urls` | string[] | PDF links found on page | content_surfaces.py |
| `pdf_count` | int | Count of PDF links | content_surfaces.py |
| `external_links` | object[] | External links `[{url, text}]` | content_surfaces.py |
| `external_link_count` | int | Count of external links | content_surfaces.py |
| `link_classifications` | object | Link intent counts `{registration: 1, ...}` | link_classifier.py |
| `organizer_url_from_links` | string | First organizer/mailto link | link_classifier.py |
| `end_time` | string (ISO) | Event end time | fluugepilz, lanterne_magique |
| `venue` | string | Venue name from RSS | fluugepilz |
| `street` | string | Street address from RSS | fluugepilz |
| `city` | string | City from RSS | fluugepilz |
| `event_id` | string | Numeric event ID from URL | forum_magazin |
| `event_type` | string | Event type label | clubdesk |
| `prerequisites` | string | Event prerequisites text | maennedorf_portal |
| `location_source` | string | How location was extracted | frauenverein_maennedorf |
| `location_raw_matched` | string | Raw matched location text | frauenverein_maennedorf |
| `ics_summary` | string | Full ICS SUMMARY field | ref_kirche_maennedorf |
| `ics_uid` | string | ICS unique identifier | ref_kirche_maennedorf |
| `relevance` | string | ICS category relevance classification | ref_kirche_maennedorf |
| `film_title` | string | Current film title | lanterne_magique |
| `duration` | string | Film duration HH:MM | lanterne_magique |
| `showtimes` | string[] | Showtime list `["13:30", "15:30"]` | lanterne_magique |

---

## Section 2 — Producer Side (Adapter Output)

### Shared Modules

These modules contribute fields to `extra` across most adapters that fetch
detail pages:

**content_surfaces.py** — called via `scan_content_surfaces(soup, page_url)`:
- `pdf_urls`, `pdf_count`, `external_links`, `external_link_count`

**detail_fields.py** — called via `extract_price()`, `extract_age()`, etc.:
- `price_type`, `price_from_chf`, `price_raw`
- `age_min`, `age_max`, `age_raw`
- `registration_raw`, `registration_url`
- `category_raw`, `category_source`

**link_classifier.py** — called via `classify_page_links()`:
- `link_classifications`, `registration_url_from_links`, `organizer_url_from_links`

**extraction.py** — called via `extract_image()`:
- `image_url`

### Per-Adapter/Platform Field Coverage

Legend: **A** = always present, **S** = sometimes present, **—** = never present

| Field | kirchenweb | govis | ICMS | clubdesk | ai1ec | contao | typo3_kool | fluugepilz | forum_magazin | gemeinde_herrliberg | lanterne_magique | eventbrite |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `adapter` | A | A | A | A | A | A | A | A | A | A | A | A |
| `extraction_method` | A | A | A | A | A | A | A | A | A | A | A | A |
| `detail_parsed` | A | A | A | A | A | A | — | S | S | A | — | A |
| `image_url` | S | S | S | S | S | S | — | S | S | S | S | S |
| `organiser` (dict) | S | S | S | — | S | S | S | — | S | — | — | S |
| `organizer` (string) | — | — | — | — | — | — | — | — | — | S | — | — |
| `price_type` | S | S | S | S | S | S | — | S | S | — | — | S |
| `price_from_chf` | S | S | S | S | S | S | — | S | S | — | — | S |
| `age_min` | S | S | S | S | S | S | — | S | S | — | — | S |
| `age_max` | S | S | S | S | S | S | — | S | S | — | — | S |
| `registration_url` | S | S | S | S | S | S | — | S | S | — | — | S |
| `category_raw` | S | S | S | S | S | S | — | S | S | — | — | S |
| `categories` (array) | — | — | — | — | A | — | — | — | — | — | — | — |
| `ics_category` | — | — | — | — | — | — | A | — | — | — | — | — |

**Notes:**
- kirchenweb (11 sources): Tier A JSON-LD. Richest field coverage.
- govis (6 sources): Tier A semantic CSS. Good coverage; organizer from
  dedicated CSS selector.
- ICMS (5 sources): Tier B text heuristic. Organizer from `<address>` tag;
  also emits `prerequisites`.
- clubdesk (2 sources): Tier A labeled fields. Also emits `event_type`.
- ai1ec (1 source): Tier A ISO timestamps. Also emits `categories` array.
- contao (1 source): Tier A JSON-LD. Also emits `location_source`,
  `location_raw_matched` for location extraction debugging.
- typo3_kool/ICS (1 source): Tier A ICS RFC 5545. Emits `ics_category`,
  `ics_summary`, `ics_uid`, `relevance`. Does NOT use shared detail_fields
  or content_surfaces modules (ICS parsing, no HTML detail page).
- fluugepilz (1 source): Tier A RSS CDATA. Also emits `end_time`, `venue`,
  `street`, `city` from structured RSS fields.
- forum_magazin (1 source): Tier B Django+HTMX. Also emits `event_id`.
- gemeinde_herrliberg (1 source): Tier B GOViS variant. Emits `organizer`
  as plain string (not dict).
- lanterne_magique (1 source): Tier A fixed venue. Also emits `film_title`,
  `duration`, `showtimes`.
- eventbrite (1 source): Tier A JSON-LD. Standard field set. Currently
  dormant (to be disabled).

---

## Section 3 — Gap Analysis

### Transform reads a key that some adapters never emit

| Key | Transform expects | Adapters that never emit it |
|---|---|---|
| `image_url` | `source_happenings.image_url` | typo3_kool (ICS — no HTML page) |
| `organiser`/`organizer` | `organizer` table | clubdesk, fluugepilz, lanterne_magique |
| `price_type` | `offering.price_type` | gemeinde_herrliberg, lanterne_magique, typo3_kool |
| `age_min`/`age_max` | `occurrence.age_min`/`age_max` | gemeinde_herrliberg, lanterne_magique, typo3_kool |
| `registration_url` | `offering.registration_url` | gemeinde_herrliberg, lanterne_magique, typo3_kool |

**Assessment:** These gaps are expected — not all source pages contain this
information. The transform handles absence correctly (silent `null`). No
action required unless a source is known to have the data but the adapter
fails to extract it.

### Adapter emits a key that transform does not read

| Key | Adapter(s) | Potential value |
|---|---|---|
| `end_time` | fluugepilz, lanterne_magique | Could populate `occurrence.end_at` |
| `venue`, `street`, `city` | fluugepilz | Could improve venue resolution |
| `organizer_url_from_links` | Most (via link_classifier) | Could enrich organizer records |
| `price_raw` | Most (via detail_fields) | Could provide price display text |
| `prerequisites` | maennedorf_portal | Could inform offering metadata |

**Assessment:** These are potential future enhancements. The transform does
not currently consume them. They are preserved in `event_raw` for future
use. No contract violation — these are optional enrichment opportunities.

### Inconsistent field shapes

| Field | Shape inconsistency | Affected adapters |
|---|---|---|
| `organiser`/`organizer` | Most adapters emit `organiser: {name, url?}` (dict). gemeinde_herrliberg emits `organizer: "string"` (plain string). | gemeinde_herrliberg |

**Assessment:** The transform handles both shapes via the fallback chain in
`extractOrganizerName()`. Not a bug, but new adapters should use the
preferred dict shape: `organiser: { name: "...", url: "..." }`.

---

## Section 4 — Deprecated Fields

### `audience_type`

- **Status:** Deprecated (Phase 7D strategy review, 2026-03-26)
- **Reason:** No downstream consumer. The family relevance classifier
  (v1.5) does not read `audience_type` — it was removed in Phase 6D after
  systematic over-assignment by crawler metadata was proven to inflate
  false positives. The classifier now uses age ranges, tags, and title
  keywords exclusively.
- **Existing adapters:** May continue to emit it. No removal required.
- **New adapters:** Must not emit it.
- **Transform behavior:** `transformCanonical.ts` may still map it to
  `source_happenings.audience_type` if present. This is harmless — the
  field carries no weight in any downstream decision (classifier,
  publication, feed ranking).

---

## Section 5 — Conventions for New Adapters

When building a new adapter, follow these conventions:

1. **Always emit:** `adapter` (string), `extraction_method` (string)

2. **Emit when available:** `image_url`, `organiser` (dict shape),
   `price_type`, `price_from_chf`, `age_min`, `age_max`,
   `registration_url`, `category_raw`

3. **Use shared modules** for detail page extraction:
   - `extract_image(soup, page_url)` → `image_url`
   - `extract_price(soup)` → `price_type`, `price_from_chf`, `price_raw`
   - `extract_age(soup)` → `age_min`, `age_max`, `age_raw`
   - `scan_content_surfaces(soup, page_url)` → PDF and link metrics
   - `classify_page_links(external_links)` → link classifications

4. **Preferred organizer shape:** `organiser: { name: "...", url: "..." }`
   The transform supports both `organiser` (British) and `organizer`
   (American) spellings for backward compatibility, including nested dict,
   flat `_name` key, and plain string forms. New adapters should use the
   British dict shape above. Do not use plain string format.

5. **Do not emit:** `audience_type` (deprecated)

6. **Document any adapter-specific keys** in this contract (Section 2)
   before merging the adapter.
