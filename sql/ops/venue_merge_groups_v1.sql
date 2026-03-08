-- =============================================================================
-- VENUE MERGE GROUPS V1 — Part 1: Group Members
-- Diagnostics only — NO mutations, NO FK reassignment, NO deletes.
--
-- Converts Tier A duplicate pairs into connected merge groups via recursive
-- graph walk. One row per group member with keeper suggestion.
--
-- Usage:
--   psql $DATABASE_URL -f sql/ops/venue_merge_groups_v1.sql
-- =============================================================================


-- ─── Best geocode per venue (deterministic pick) ────────────────────────────

WITH RECURSIVE best_geocode AS (
  SELECT DISTINCT ON (g.venue_id)
    g.venue_id,
    g.lat,
    g.lng,
    g.provider,
    g.raw_response,
    CASE
      WHEN g.provider = 'nominatim'    AND g.raw_response->>'addresstype' = 'town' THEN true
      WHEN g.provider = 'geo_admin_ch' AND g.raw_response->'attrs'->>'origin' = 'gg25' THEN true
      ELSE false
    END AS is_town_center,
    CASE
      WHEN g.provider = 'nominatim' AND g.raw_response->>'addresstype' IN ('building','amenity','place') THEN 'address_level'
      WHEN g.provider = 'nominatim' AND g.raw_response->>'addresstype' IN ('road','postcode') THEN 'street_level'
      WHEN g.provider = 'nominatim' AND g.raw_response->>'addresstype' = 'town' THEN 'town_center'
      WHEN g.provider = 'geo_admin_ch' AND g.raw_response->'attrs'->>'origin' = 'gg25' THEN 'town_center'
      WHEN g.provider = 'geo_admin_ch' AND g.raw_response->'attrs'->>'origin' = 'address' THEN 'address_level'
      ELSE 'other'
    END AS geocode_quality
  FROM venue_geocode_result g
  ORDER BY
    g.venue_id,
    CASE
      WHEN g.provider = 'nominatim'    AND g.raw_response->>'addresstype' = 'town' THEN 1
      WHEN g.provider = 'geo_admin_ch' AND g.raw_response->'attrs'->>'origin' = 'gg25' THEN 1
      ELSE 0
    END ASC,
    CASE WHEN g.provider = 'nominatim' THEN 0 ELSE 1 END ASC,
    g.created_at DESC
),

-- ─── Enriched distinct venue names ──────────────────────────────────────────

venue_enriched AS (
  SELECT DISTINCT ON (v.name)
    v.id,
    v.name,
    v.created_at,
    lower(trim(regexp_replace(split_part(v.name, ',', 1), '\s+', ' ', 'g'))) AS place_name,
    CASE
      WHEN position(',' IN v.name) > 0
       AND trim(split_part(v.name, ',', 2)) ~ '^\d{4}\s' THEN NULL
      WHEN position(',' IN v.name) > 0
       AND trim(split_part(v.name, ',', 2)) != ''
        THEN lower(trim(regexp_replace(split_part(v.name, ',', 2), '\s+', ' ', 'g')))
      ELSE NULL
    END AS street_norm,
    (regexp_match(v.name, '\m(\d{4})\M'))[1] AS postal,
    bg.lat, bg.lng,
    bg.provider AS geo_provider,
    bg.is_town_center,
    bg.geocode_quality,
    (SELECT count(*) FROM happening h WHERE h.primary_venue_id = v.id AND h.publication_status = 'published') AS live_happening_refs,
    (SELECT count(*) FROM happening h WHERE h.primary_venue_id = v.id) AS total_happening_refs,
    (SELECT count(*) FROM occurrence o WHERE o.venue_id = v.id) AS occurrence_refs
  FROM venue v
  LEFT JOIN best_geocode bg ON bg.venue_id = v.id
  ORDER BY v.name, v.created_at ASC
),

-- ─── Tier A edges ───────────────────────────────────────────────────────────

tier_a_edges AS (
  SELECT a.id AS id_a, b.id AS id_b
  FROM venue_enriched a
  JOIN venue_enriched b
    ON a.name < b.name
   AND a.lat IS NOT NULL AND b.lat IS NOT NULL
   AND NOT a.is_town_center AND NOT b.is_town_center
   AND 111045.0 * sqrt(
         power(a.lat - b.lat, 2)
         + power((a.lng - b.lng) * cos(radians((a.lat + b.lat) / 2.0)), 2)
       ) < 5
  WHERE
    (
      (a.place_name = b.place_name
       AND a.street_norm IS NOT NULL AND b.street_norm IS NOT NULL
       AND a.street_norm = b.street_norm)
      OR
      ((a.place_name LIKE '%' || b.place_name || '%'
        OR b.place_name LIKE '%' || a.place_name || '%')
       AND a.street_norm IS NOT NULL AND b.street_norm IS NOT NULL
       AND a.street_norm = b.street_norm)
    )
    AND NOT (a.postal IS NOT NULL AND b.postal IS NOT NULL AND a.postal != b.postal)
),

-- ─── Connected-component walk ───────────────────────────────────────────────

vertices AS (
  SELECT id_a AS id FROM tier_a_edges
  UNION
  SELECT id_b FROM tier_a_edges
),

edges AS (
  SELECT id_a, id_b FROM tier_a_edges
  UNION ALL
  SELECT id_b, id_a FROM tier_a_edges
),

component_walk AS (
  SELECT id AS vertex, id AS root
  FROM vertices
  UNION
  SELECT e.id_b AS vertex, cw.root
  FROM component_walk cw
  JOIN edges e ON e.id_a = cw.vertex
  WHERE cw.root < e.id_b
),

components AS (
  SELECT vertex AS venue_id, min(root::text)::uuid AS component_root
  FROM component_walk
  GROUP BY vertex
),

group_ids AS (
  SELECT
    venue_id,
    component_root,
    dense_rank() OVER (ORDER BY component_root) AS group_id
  FROM components
),

-- ─── Group members with keeper selection ────────────────────────────────────

group_members AS (
  SELECT
    gi.group_id,
    gi.venue_id,
    ve.name,
    ve.place_name,
    ve.street_norm,
    ve.postal,
    ve.lat,
    ve.lng,
    ve.geo_provider,
    ve.is_town_center,
    ve.geocode_quality,
    ve.live_happening_refs,
    ve.total_happening_refs,
    ve.occurrence_refs,
    ve.created_at,
    count(*) OVER (PARTITION BY gi.group_id) AS group_size,

    -- Keeper priority:
    -- 1. address-level geocode
    -- 2. fuller name (more comma-parts)
    -- 3. has postal code
    -- 4. more live refs
    -- 5. more total refs
    -- 6. older created_at
    -- 7. UUID tie-break
    row_number() OVER (
      PARTITION BY gi.group_id
      ORDER BY
        CASE ve.geocode_quality
          WHEN 'address_level' THEN 0
          WHEN 'street_level'  THEN 1
          WHEN 'other'         THEN 2
          WHEN 'town_center'   THEN 3
          ELSE 4
        END ASC,
        array_length(string_to_array(ve.name, ','), 1) DESC,
        (CASE WHEN ve.postal IS NOT NULL THEN 1 ELSE 0 END) DESC,
        ve.live_happening_refs DESC,
        ve.total_happening_refs DESC,
        ve.created_at ASC,
        gi.venue_id ASC
    ) AS keeper_rank

  FROM group_ids gi
  JOIN venue_enriched ve ON ve.id = gi.venue_id
)

-- ═══════════════════════════════════════════════════════════════════════════
-- OUTPUT: one row per group member
-- ═══════════════════════════════════════════════════════════════════════════

SELECT
  gm.group_id,
  gm.group_size,
  gm.venue_id,
  CASE WHEN gm.keeper_rank = 1 THEN 'KEEPER' ELSE 'merge_into_keeper' END AS role,
  gm.name,

  -- Keeper suggestion
  (SELECT k.venue_id FROM group_members k WHERE k.group_id = gm.group_id AND k.keeper_rank = 1) AS suggested_keeper_id,
  CASE
    WHEN gm.keeper_rank = 1 THEN 'self'
    ELSE (
      SELECT CASE
        WHEN k.geocode_quality != gm.geocode_quality THEN 'better_geocode'
        WHEN array_length(string_to_array(k.name, ','), 1)
           > array_length(string_to_array(gm.name, ','), 1) THEN 'fuller_name'
        WHEN (k.postal IS NOT NULL) AND (gm.postal IS NULL) THEN 'has_postal'
        WHEN k.live_happening_refs > gm.live_happening_refs THEN 'more_live_refs'
        WHEN k.total_happening_refs > gm.total_happening_refs THEN 'more_total_refs'
        WHEN k.created_at < gm.created_at THEN 'older'
        ELSE 'uuid_tiebreak'
      END
      FROM group_members k WHERE k.group_id = gm.group_id AND k.keeper_rank = 1
    )
  END AS keeper_reason,

  -- Geocode context
  gm.geocode_quality,
  gm.geo_provider,
  gm.is_town_center,

  -- Reference counts
  gm.live_happening_refs AS live_refs,
  gm.total_happening_refs AS total_refs,
  gm.occurrence_refs AS occ_refs,

  -- Address signals
  gm.street_norm AS street,
  gm.postal,
  gm.lat,
  gm.lng

FROM group_members gm
ORDER BY gm.group_id, gm.keeper_rank;
