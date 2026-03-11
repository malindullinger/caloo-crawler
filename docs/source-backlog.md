# Source Backlog

Candidate sources that have been identified but do not yet have an implemented
adapter. These were previously placeholder stubs in `src/sources/adapters/` and
have been moved here to keep the adapter directory reserved for production code.

When picking up a source from this list, create the adapter, register it in
`registry.py`, and add its `SourceConfig` to `multi_source.py`.

---

| Source | source_id | Seed URL | Status | Notes |
|--------|-----------|----------|--------|-------|
| Ref. Kirche Stäfa-Hombrechtikon | `ref-staefa-hombrechtikon` | https://www.ref-staefa-hombrechtikon.ch/ | candidate | Church hub with calendar page. Strategy: find calendar link on hub homepage, then parse listing. May share a pattern with other Swiss church sites (potential adapter family). |
| Kino Wildenmann | `kino-wildenmann` | https://www.kino-wildenmann.ch/ | candidate | Small cinema. Strategy: parse showtimes from homepage/program page. Likely simple HTML table or list. |
| Uetikon Vereinsliste | `uetikon_vereinsliste` | https://www.uetikonamsee.ch/vereinsliste | deferred | Association directory, not an event calendar. Better handled through a future source discovery / directory ingestion workflow (write to `sources_discovered` table) rather than a dedicated event adapter. |

Last updated: 2026-03-11
