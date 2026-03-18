# Source Backlog

Candidate sources that have been identified but do not yet have an implemented
adapter. These were previously placeholder stubs in `src/sources/adapters/` and
have been moved here to keep the adapter directory reserved for production code.

When picking up a source from this list, create the adapter, register it in
`registry.py`, and add its `SourceConfig` to `multi_source.py`.

Strategic direction (2026-03-17): Männedorf depth-first audit complete. Source
expansion Wave 1 next (ICMS portals: Zollikon, Uetikon, Rapperswil-Jona).
Family-ecosystem sources (Tier A anchors) before platform aggregators. See
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
| ~~Fluugepilz (Erlenbach)~~ | `fluugepilz-erlenbach` | — | WordPress + Events Manager | **active** | Moved to production 2026-03-15. RSS adapter. 19 events. WordPress Events Manager family 1/3. Opens Erlenbach municipality. See `source-status.md`. |

## Priority 2b — Männedorf Depth-First Candidates (discovered 2026-03-17 audit)

| Source | source_id | Seed URL | Platform | Status | Notes |
|--------|-----------|----------|----------|--------|-------|
| FC Männedorf | `fc-maennedorf` | fcmaennedorf.ch | WordPress | candidate | Youth football (Junioren). D1 candidate. Highest ROI new source type for Männedorf. |
| CHINDaktiv Männedorf | `chindaktiv-maennedorf` | chindaktiv.ch | National platform | candidate | Children's activity platform with Männedorf listings. D1 candidate. |
| Zauberlaterne Männedorf | `zauberlaterne-maennedorf` | lanterne-magique.org | National platform | candidate | Children's cinema club. Monthly screenings Oct–Jun. D1 candidate. |
| Kath. St. Stephan Männedorf-Uetikon | `kath-maennedorf-uetikon` | kath-maennedorf-uetikon.ch | WordPress | candidate | Covers Männedorf + Uetikon catholic. JS-rendered agenda. Crawlability TBD. |

## Priority 3 — Deferred

| Source | source_id | Seed URL | Platform | Status | Notes |
|--------|-----------|----------|----------|--------|-------|
| Kulturkreis Männedorf | `kulturkreis-maennedorf` | https://www.kulturkreis-maennedorf.ch/ | Joomla + SP Page Builder | deferred | 3 events. Tier B. Deferred 2026-03-15 — low ROI. Confirmed in 2026-03-17 audit. |
| Eventfrog | `eventfrog` | https://eventfrog.ch/ | Eventfrog | deferred | Major Swiss aggregator. JS-rendered, anti-bot. Deprioritized vs direct family sources. |
| Kino Wildenmann | `kino-wildenmann` | https://www.kino-wildenmann.ch/ | Custom | deferred | Small cinema. Low family relevance. Confirmed deferred in 2026-03-17 audit. |
| Uetikon Vereinsliste | `uetikon_vereinsliste` | https://www.uetikonamsee.ch/vereinsliste | i-web | deferred | Association directory, not an event calendar. Better handled through source discovery workflow. |

## Not Crawlable (assessed 2026-03-15, updated 2026-03-17)

| Source | URL | Platform | Reason |
|--------|-----|----------|--------|
| Familienzentrum Männedorf | familienzentrum-maennedorf.ch | Squarespace | No structured event listing. D2: 15 events visible via portal organizer extraction. |
| CGM Männedorf | cgm-maennedorf.ch | WordPress | JS-rendered, no structured events. Poor crawlability. |
| DTV Männedorf | dtv-maennedorf.ch | Joomla | 1 event + PDF schedule. Insufficient structured data. |
| Theater Tamtam | theater-tamtam.ch | Custom | 11 courses but custom layout, no structured dates. Fair difficulty, low ROI. |
| Familienkreis Männedorf | — | Unknown | Active org confirmed (audit T2) but no crawlable digital presence. Bot-blocking. D3. |
| Schule Männedorf | schule-maennedorf.ch | i-web | No public event calendar. |

Last updated: 2026-03-17 (Männedorf depth-first audit complete, 4 new candidates added)
