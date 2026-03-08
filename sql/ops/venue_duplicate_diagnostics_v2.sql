-- =============================================================================
-- VENUE DUPLICATE DIAGNOSTICS V2
-- Phase: V1 — governed convergence, diagnostics only, NO mutations
--
-- Pair-first model with three independent signals:
--   1. Coordinate proximity  (with town-center fallback detection)
--   2. Name similarity       (place name + street address, independently scored)
--   3. Postal code agreement
--
-- Candidate pair generation:
--   Gate A: both non-town-center, coords within 1km
--   Gate B: one/both town-center, strong textual evidence (name + street/address)
--
-- Tiers assigned per pair from signal combination:
--   A = conservative auto-merge candidate (exact coords + name+address + postal ok)
--   B = plausible duplicate, needs review
--   C = ambiguous, do not auto-merge
--
-- Usage:
--   psql $DATABASE_URL -f sql/ops/venue_duplicate_diagnostics_v2.sql
-- =============================================================================


-- ─── CTE 0: Pick exactly one geocode row per venue ──────────────────────────
-- Priority: non-town-center > town-center, nominatim > geo_admin_ch, newest.

WITH best_geocode AS (
  SELECT DISTINCT ON (g.venue_id)
    g.venue_id,
    g.lat,
    g.lng,
    g.provider,
    g.raw_response,
    g.created_at AS geo_created_at,
    CASE
      WHEN g.provider = 'nominatim'
       AND g.raw_response->>'addresstype' = 'town' THEN true
      WHEN g.provider = 'geo_admin_ch'
       AND g.raw_response->'attrs'->>'origin' = 'gg25' THEN true
      ELSE false
    END AS is_town_center
  FROM venue_geocode_result g
  ORDER BY
    g.venue_id,
    -- 1. Prefer non-town-center
    CASE
      WHEN g.provider = 'nominatim'   AND g.raw_response->>'addresstype' = 'town' THEN 1
      WHEN g.provider = 'geo_admin_ch' AND g.raw_response->'attrs'->>'origin' = 'gg25' THEN 1
      ELSE 0
    END ASC,
    -- 2. Prefer nominatim over geo_admin_ch
    CASE WHEN g.provider = 'nominatim' THEN 0 ELSE 1 END ASC,
    -- 3. Newest created_at
    g.created_at DESC
),


-- ─── CTE 1: Enrich each distinct venue name ────────────────────────────────
-- One row per distinct name (representative = oldest created_at).

venue_enriched AS (
  SELECT DISTINCT ON (v.name)
    v.id,
    v.name,
    v.created_at,

    -- Place name: first comma-part, lowercased, whitespace-normalized
    lower(trim(regexp_replace(split_part(v.name, ',', 1), '\s+', ' ', 'g')))
      AS place_name,

    -- Street: second comma-part, normalized.
    -- NULL if no comma, or if second part is a postal code (e.g. "8708 Männedorf")
    CASE
      WHEN position(',' IN v.name) > 0
       AND trim(split_part(v.name, ',', 2)) ~ '^\d{4}\s'
        THEN NULL
      WHEN position(',' IN v.name) > 0
       AND trim(split_part(v.name, ',', 2)) != ''
        THEN lower(trim(regexp_replace(split_part(v.name, ',', 2), '\s+', ' ', 'g')))
      ELSE NULL
    END AS street_norm,

    -- Postal code: 4-digit Swiss postal extracted from full name
    (regexp_match(v.name, '\m(\d{4})\M'))[1] AS postal,

    -- Geocoded coordinates (from deterministic best_geocode pick)
    bg.lat,
    bg.lng,
    bg.provider AS geo_provider,
    bg.is_town_center,

    -- Live refs: published happenings (publication_status)
    (SELECT count(*) FROM happening h
     WHERE h.primary_venue_id = v.id
       AND h.publication_status = 'published') AS live_happening_refs,
    -- Total happening refs
    (SELECT count(*) FROM happening h
     WHERE h.primary_venue_id = v.id) AS total_happening_refs,
    -- Occurrence refs
    (SELECT count(*) FROM occurrence o
     WHERE o.venue_id = v.id) AS occurrence_refs

  FROM venue v
  LEFT JOIN best_geocode bg ON bg.venue_id = v.id
  ORDER BY v.name, v.created_at ASC
),


-- ─── CTE 2: Candidate pairs with gate reason ───────────────────────────────
-- Gate A: coordinate proximity (non-town-center only)
-- Gate B: one/both town-center + strong textual evidence

candidate_pairs AS (

  -- Gate A: coord_gate — both non-town-center, within 1km
  SELECT
    'coord_gate'::text AS candidate_generation_reason,
    1 AS gate_priority,
    a.id AS id_a, a.name AS name_a,
    b.id AS id_b, b.name AS name_b,
    a.place_name AS place_name_a, b.place_name AS place_name_b,
    a.street_norm AS street_a, b.street_norm AS street_b,
    a.postal AS postal_a, b.postal AS postal_b,
    a.lat AS lat_a, a.lng AS lng_a,
    b.lat AS lat_b, b.lng AS lng_b,
    a.geo_provider AS provider_a, b.geo_provider AS provider_b,
    a.is_town_center AS is_town_center_a, b.is_town_center AS is_town_center_b,
    a.live_happening_refs AS live_refs_a, a.total_happening_refs AS total_refs_a, a.occurrence_refs AS occ_refs_a, a.created_at AS created_a,
    b.live_happening_refs AS live_refs_b, b.total_happening_refs AS total_refs_b, b.occurrence_refs AS occ_refs_b, b.created_at AS created_b
  FROM venue_enriched a
  JOIN venue_enriched b
    ON a.name < b.name
   AND a.lat IS NOT NULL AND b.lat IS NOT NULL
   AND NOT a.is_town_center AND NOT b.is_town_center
   AND 111045.0 * sqrt(
         power(a.lat - b.lat, 2)
         + power((a.lng - b.lng) * cos(radians((a.lat + b.lat) / 2.0)), 2)
       ) < 1000

  UNION ALL

  -- Gate B: town_center_address_gate or town_center_name_gate
  SELECT
    CASE
      WHEN a.street_norm IS NOT NULL AND b.street_norm IS NOT NULL AND a.street_norm = b.street_norm
        THEN 'town_center_address_gate'::text
      ELSE 'town_center_name_gate'::text
    END AS candidate_generation_reason,
    CASE
      WHEN a.street_norm IS NOT NULL AND b.street_norm IS NOT NULL AND a.street_norm = b.street_norm
        THEN 2
      ELSE 3
    END AS gate_priority,
    a.id, a.name,
    b.id, b.name,
    a.place_name, b.place_name,
    a.street_norm, b.street_norm,
    a.postal, b.postal,
    a.lat, a.lng,
    b.lat, b.lng,
    a.geo_provider, b.geo_provider,
    a.is_town_center, b.is_town_center,
    a.live_happening_refs, a.total_happening_refs, a.occurrence_refs, a.created_at,
    b.live_happening_refs, b.total_happening_refs, b.occurrence_refs, b.created_at
  FROM venue_enriched a
  JOIN venue_enriched b
    ON a.name < b.name
   AND (a.is_town_center OR b.is_town_center)
   -- Strong textual evidence required:
   AND (
     a.place_name = b.place_name
     OR a.place_name LIKE '%' || b.place_name || '%'
     OR b.place_name LIKE '%' || a.place_name || '%'
   )
   AND (
     (a.street_norm IS NOT NULL AND b.street_norm IS NOT NULL AND a.street_norm = b.street_norm)
     OR (a.postal IS NOT NULL AND b.postal IS NOT NULL AND a.postal = b.postal)
   )
),


-- ─── CTE 3: Dedupe pairs (a pair may enter via both gates) ─────────────────
-- Explicit priority: coord_gate (1) > town_center_address_gate (2) > town_center_name_gate (3)

deduped_pairs AS (
  SELECT DISTINCT ON (id_a, id_b)
    *
  FROM candidate_pairs
  ORDER BY id_a, id_b, gate_priority ASC
),


-- ─── CTE 4: Score signals per pair ──────────────────────────────────────────

scored AS (
  SELECT
    dp.*,

    -- ═══ SIGNAL 1: Coordinate proximity ═══

    CASE WHEN lat_a IS NOT NULL AND lat_b IS NOT NULL
      THEN round((111045.0 * sqrt(
        power(lat_a - lat_b, 2)
        + power((lng_a - lng_b) * cos(radians((lat_a + lat_b) / 2.0)), 2)
      ))::numeric, 1)
      ELSE NULL
    END AS distance_m,

    CASE
      WHEN is_town_center_a AND is_town_center_b
        THEN 'town_center_shared'
      WHEN lat_a IS NULL OR lat_b IS NULL
        THEN 'no_coords'
      WHEN lat_a = lat_b AND lng_a = lng_b
        THEN 'exact_match'
      WHEN 111045.0 * sqrt(
             power(lat_a - lat_b, 2)
             + power((lng_a - lng_b) * cos(radians((lat_a + lat_b) / 2.0)), 2)
           ) < 5
        THEN 'exact_match'
      WHEN 111045.0 * sqrt(
             power(lat_a - lat_b, 2)
             + power((lng_a - lng_b) * cos(radians((lat_a + lat_b) / 2.0)), 2)
           ) < 50
        THEN 'very_close'
      WHEN 111045.0 * sqrt(
             power(lat_a - lat_b, 2)
             + power((lng_a - lng_b) * cos(radians((lat_a + lat_b) / 2.0)), 2)
           ) < 200
        THEN 'nearby'
      WHEN 111045.0 * sqrt(
             power(lat_a - lat_b, 2)
             + power((lng_a - lng_b) * cos(radians((lat_a + lat_b) / 2.0)), 2)
           ) < 500
        THEN 'distant'
      ELSE 'far'
    END AS coord_evidence,

    -- ═══ SIGNAL 2: Name similarity ═══

    CASE
      WHEN place_name_a = place_name_b
       AND street_a IS NOT NULL AND street_b IS NOT NULL
       AND street_a = street_b
        THEN 'name_and_address_match'

      WHEN place_name_a = place_name_b
        THEN 'name_match_only'

      WHEN (place_name_a LIKE '%' || place_name_b || '%'
            OR place_name_b LIKE '%' || place_name_a || '%')
       AND street_a IS NOT NULL AND street_b IS NOT NULL
       AND street_a = street_b
        THEN 'name_contained_and_address_match'

      WHEN place_name_a LIKE '%' || place_name_b || '%'
        OR place_name_b LIKE '%' || place_name_a || '%'
        THEN 'name_contained_only'

      WHEN street_a IS NOT NULL AND street_b IS NOT NULL
       AND street_a = street_b
        THEN 'different_name_same_address'

      ELSE 'no_match'
    END AS name_evidence,

    -- ═══ SIGNAL 3: Postal code agreement ═══

    CASE
      WHEN postal_a IS NOT NULL AND postal_b IS NOT NULL AND postal_a = postal_b
        THEN 'postal_match'
      WHEN postal_a IS NULL OR postal_b IS NULL
        THEN 'postal_unknown'
      ELSE 'postal_conflict'
    END AS postal_evidence

  FROM deduped_pairs dp
),


-- ─── CTE 5: Assign tier + keeper ────────────────────────────────────────────

tiered AS (
  SELECT
    s.*,

    -- ── Tier assignment ──
    CASE
      -- Tier A: exact non-town-center coords + name+address match + no postal conflict
      WHEN coord_evidence = 'exact_match'
       AND name_evidence = 'name_and_address_match'
       AND postal_evidence != 'postal_conflict'
        THEN 'A'

      -- Tier A: exact non-town-center coords + contained name + same address + no postal conflict
      WHEN coord_evidence = 'exact_match'
       AND name_evidence = 'name_contained_and_address_match'
       AND postal_evidence != 'postal_conflict'
        THEN 'A'

      -- Tier B: exact/very close coords + name match (no address) — needs review
      WHEN coord_evidence IN ('exact_match', 'very_close')
       AND name_evidence IN ('name_match_only', 'name_contained_only')
       AND postal_evidence != 'postal_conflict'
        THEN 'B'

      -- Tier B: exact/very close coords + different name but same address
      WHEN coord_evidence IN ('exact_match', 'very_close')
       AND name_evidence = 'different_name_same_address'
       AND postal_evidence != 'postal_conflict'
        THEN 'B'

      -- Tier B: town-center shared + name+address textual match
      WHEN coord_evidence = 'town_center_shared'
       AND name_evidence IN ('name_and_address_match', 'name_contained_and_address_match')
       AND postal_evidence != 'postal_conflict'
        THEN 'B'

      -- Tier B: nearby coords + strong name+address match + postal match
      WHEN coord_evidence = 'nearby'
       AND name_evidence IN ('name_and_address_match', 'name_contained_and_address_match')
       AND postal_evidence = 'postal_match'
        THEN 'B'

      -- Tier C: everything else
      ELSE 'C'
    END AS tier,

    -- ── Keeper suggestion (diagnostic only — not a merge action) ──
    CASE
      WHEN live_refs_a > live_refs_b THEN id_a
      WHEN live_refs_b > live_refs_a THEN id_b
      WHEN total_refs_a > total_refs_b THEN id_a
      WHEN total_refs_b > total_refs_a THEN id_b
      WHEN created_a < created_b THEN id_a
      WHEN created_b < created_a THEN id_b
      WHEN id_a::text < id_b::text THEN id_a
      ELSE id_b
    END AS keeper_id,

    CASE
      WHEN live_refs_a > live_refs_b THEN 'more_live_refs'
      WHEN live_refs_b > live_refs_a THEN 'more_live_refs'
      WHEN total_refs_a > total_refs_b THEN 'more_total_refs'
      WHEN total_refs_b > total_refs_a THEN 'more_total_refs'
      WHEN created_a < created_b THEN 'older'
      WHEN created_b < created_a THEN 'older'
      ELSE 'uuid_tiebreak'
    END AS keeper_reason

  FROM scored s
)


-- ═══════════════════════════════════════════════════════════════════════════
-- FINAL OUTPUT — diagnostics only, no mutation
-- ═══════════════════════════════════════════════════════════════════════════

SELECT
  row_number() OVER (ORDER BY tier, distance_m NULLS LAST, name_a) AS pair_id,
  least(id_a::text, id_b::text) || '|' || greatest(id_a::text, id_b::text) AS pair_key,
  tier,
  candidate_generation_reason,

  -- Venue A
  id_a    AS venue_id_a,
  name_a,

  -- Venue B
  id_b    AS venue_id_b,
  name_b,

  -- Signal 1: coordinate proximity
  coord_evidence,
  distance_m,
  provider_a,
  provider_b,
  is_town_center_a  AS tc_a,
  is_town_center_b  AS tc_b,

  -- Signal 2: name similarity
  name_evidence,
  place_name_a,
  place_name_b,
  street_a,
  street_b,

  -- Signal 3: postal agreement
  postal_evidence,
  postal_a,
  postal_b,

  -- Keeper suggestion (diagnostic only)
  keeper_id,
  keeper_reason,

  -- Reference counts
  live_refs_a,
  total_refs_a,
  occ_refs_a,
  live_refs_b,
  total_refs_b,
  occ_refs_b

FROM tiered
ORDER BY
  tier ASC,
  distance_m ASC NULLS LAST,
  name_a ASC;
