# Source Backlog

Candidate sources that have been identified but do not yet have an implemented
adapter. These were previously placeholder stubs in `src/sources/adapters/` and
have been moved here to keep the adapter directory reserved for production code.

When picking up a source from this list, create the adapter, register it in
`registry.py`, and add its `SourceConfig` to `multi_source.py`.

Strategic direction (2026-03-15): deep Männedorf coverage first. Family-ecosystem
sources (Tier A anchors) before platform aggregators. See
`caloo/docs/crawler/gold-coast-source-map.md` for full candidate inventory.

---

## Priority 1 — Männedorf Family Ecosystem

| Source | source_id | Seed URL | Platform | Status | Notes |
|--------|-----------|----------|----------|--------|-------|
| ~~Frauenverein Männedorf~~ | `frauenverein-maennedorf` | — | Contao CMS | **active** | Moved to production 2026-03-15. 25 events. See `source-status.md`. |
| ~~Ref. Kirche Männedorf~~ | `ref-kirche-maennedorf` | — | TYPO3 + lpc_kool_events | **active** | Moved to production 2026-03-15. 70 events (136 discovered, relevance-filtered). First ICS source. See `source-status.md`. |
| ~~Kulturkreis Männedorf~~ | `kulturkreis-maennedorf` | https://www.kulturkreis-maennedorf.ch/ | Joomla + SP Page Builder | **deferred** | 3 events only. Tier B (no structured data). Assessed 2026-03-15: low ROI, moved to Priority 3. |

## Priority 2 — Kirchenweb Expansion & Regional

| Source | source_id | Seed URL | Platform | Status | Notes |
|--------|-----------|----------|----------|--------|-------|
| ~~Ref. Kirche Stäfa-Hombrechtikon~~ | `ref-staefa-hombrechtikon` | — | Kirchenweb | **active** | Moved to production 2026-03-15. Config-only onboarding (kirchenweb adapter). `a.agendaTitel` confirmed. ~1000 events discovered. See `source-status.md`. |
| Fluugepilz (Erlenbach) | `fluugepilz-erlenbach` | https://www.xn--flgepilz-75aa.ch/events/feed/ | WordPress + Events Manager | candidate | Family centre. Domain is flüügepilz.ch (punycode). RSS feed with all 19 events in single request. Tier A (structured datetime in `<pubDate>`). WordPress Events Manager (not Tribe). |

## Priority 3 — Deferred

| Source | source_id | Seed URL | Platform | Status | Notes |
|--------|-----------|----------|----------|--------|-------|
| Kulturkreis Männedorf | `kulturkreis-maennedorf` | https://www.kulturkreis-maennedorf.ch/ | Joomla + SP Page Builder | deferred | 3 events. Tier B. Deferred 2026-03-15 — low ROI vs Ref. Kirche (130+ events). Revisit after deep Männedorf coverage. |
| Eventfrog | `eventfrog` | https://eventfrog.ch/ | Eventfrog | deferred | Major Swiss aggregator. JS-rendered, anti-bot. Deprioritized vs direct family sources. |
| Kino Wildenmann | `kino-wildenmann` | https://www.kino-wildenmann.ch/ | Custom | deferred | Small cinema. Low family relevance. Revisit after Männedorf deep coverage. |
| Uetikon Vereinsliste | `uetikon_vereinsliste` | https://www.uetikonamsee.ch/vereinsliste | i-web | deferred | Association directory, not an event calendar. Better handled through source discovery workflow. |

## Not Crawlable (assessed 2026-03-15)

| Source | URL | Platform | Reason |
|--------|-----|----------|--------|
| Familienzentrum Männedorf | familienzentrum-maennedorf.ch | Squarespace | No structured event listing. Single-page content. Events visible only via portal. |
| CGM Männedorf | cgm-maennedorf.ch | WordPress | JS-rendered, no structured events. Poor crawlability. |
| DTV Männedorf | dtv-maennedorf.ch | Joomla | 1 event + PDF schedule. Insufficient structured data. |
| Theater Tamtam | theater-tamtam.ch | Custom | 11 courses but custom layout, no structured dates. Fair difficulty, low ROI. |
| Schule Männedorf | schule-maennedorf.ch | i-web | No public event calendar. |

Last updated: 2026-03-15
