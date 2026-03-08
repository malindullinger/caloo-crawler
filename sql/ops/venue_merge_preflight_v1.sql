-- =============================================================================
-- VENUE MERGE PREFLIGHT V1 — Read-only diagnostic
-- Shows exactly what venue_merge_execute_v1.sql will change.
-- NO mutations to real tables. Safe to run repeatedly.
--
-- Scope: 5 approved Tier A merge groups (15 venue names, 10 discards).
-- Hardcoded UUIDs — does NOT re-derive groups from live data.
--
-- Usage:
--   psql $DATABASE_URL -f sql/ops/venue_merge_preflight_v1.sql
-- =============================================================================


-- ─── Setup: temp tables for shared data across sections ─────────────────────

CREATE TEMP TABLE _pf_merge_plan (
  group_id int NOT NULL,
  venue_id uuid NOT NULL,
  role     text NOT NULL,
  label    text NOT NULL
);

INSERT INTO _pf_merge_plan (group_id, venue_id, role, label) VALUES
  -- Group 1: Untervogthaus
  (1, '27edf7a5-8ed1-42a5-986b-7c493cd38310', 'KEEPER',  'Untervogthaus'),
  (1, '0f62aae8-d4e2-4d44-a070-ddf32f42c4de', 'DISCARD', 'Untervogthaus (double-space)'),
  (1, '045964d8-9081-4bee-b6d3-28423ea19f0f', 'DISCARD', 'Café Untervogthaus'),
  (1, '9bf427e7-48a9-4278-9fd2-31d46dcbcfb2', 'DISCARD', 'Untervogthaus (no postal)'),
  -- Group 2: Gemeindesaal
  (2, '929c2471-9d86-49c6-b86c-90ec62e2506f', 'KEEPER',  'Gemeindesaal'),
  (2, '049a5fe0-b3e5-47f3-acda-62c6fadd0690', 'DISCARD', 'Gemeindesaal (variant)'),
  (2, '430f3315-48a3-4889-b9e0-6e1c895e0f3f', 'DISCARD', 'Gemeindesaal Männedorf'),
  (2, '8be85954-ac16-415c-874e-20690921c43a', 'DISCARD', 'Gemeindesaal Männedorf (double-space)'),
  -- Group 3: Turnhalle Blatten
  (3, '4d924e77-ce9b-4d45-a1e8-87b7913327e4', 'KEEPER',  'Turnhalle Blatten'),
  (3, '28b9db0e-4908-4038-a007-4572e5783e1d', 'DISCARD', 'Turnhalle Blatten (variant)'),
  -- Group 4: Familienzentrum Männedorf
  (4, '4d749ceb-278e-4625-8bec-dc4c9af3ca6b', 'KEEPER',  'Familienzentrum Männedorf'),
  (4, '84de632c-cb32-4058-9311-610422728fdc', 'DISCARD', 'Familienzentrum (short)'),
  (4, '672c22a2-0c4c-4a0f-8722-8988260925cd', 'DISCARD', 'Familienzentrum Männedorf (double-space)'),
  -- Group 5: Anna Zemp-Stiftung
  (5, 'f25faacc-2eb6-4ebb-bb86-2c38bb4872f5', 'KEEPER',  'Anna Zemp-Stiftung'),
  (5, '7a9d858b-4315-4e38-9050-636ae9def606', 'DISCARD', 'Naturgarten der Anna Zemp-Stiftung');

CREATE TEMP TABLE _pf_discards AS
  SELECT mp.group_id, mp.venue_id AS discard_id, mp.label, k.venue_id AS keeper_id
  FROM _pf_merge_plan mp
  JOIN _pf_merge_plan k ON k.group_id = mp.group_id AND k.role = 'KEEPER'
  WHERE mp.role = 'DISCARD';

CREATE TEMP TABLE _pf_keeper_canonical (
  keeper_id     uuid NOT NULL,
  canonical_name text NOT NULL,
  address_line1  text NOT NULL,
  postal_code    text NOT NULL,
  locality       text NOT NULL,
  lat            double precision NOT NULL,
  lng            double precision NOT NULL
);

INSERT INTO _pf_keeper_canonical VALUES
  ('27edf7a5-8ed1-42a5-986b-7c493cd38310', 'Untervogthaus',            'Dorfgasse 37',          '8708', 'Männedorf', 47.2532197, 8.690348),
  ('929c2471-9d86-49c6-b86c-90ec62e2506f', 'Gemeindesaal',             'Alte Landstrasse 250',  '8708', 'Männedorf', 47.2533338, 8.6951263),
  ('4d924e77-ce9b-4d45-a1e8-87b7913327e4', 'Turnhalle Blatten',        'Schulstrasse 25',       '8708', 'Männedorf', 47.2540303, 8.6965024),
  ('4d749ceb-278e-4625-8bec-dc4c9af3ca6b', 'Familienzentrum Männedorf', 'Alte Landstrasse 250',  '8708', 'Männedorf', 47.2533338, 8.6951263),
  ('f25faacc-2eb6-4ebb-bb86-2c38bb4872f5', 'Anna Zemp-Stiftung',       'Lönerenweg 10',         '8708', 'Männedorf', 47.2627359, 8.698407);


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1: Merge group summary
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 1: Merge Group Summary ──' AS section;

SELECT
  mp.group_id,
  mp.role,
  mp.venue_id,
  v.name AS current_name,
  mp.label,
  (SELECT count(*) FROM happening h WHERE h.primary_venue_id = mp.venue_id) AS happening_refs,
  (SELECT count(*) FROM occurrence o WHERE o.venue_id = mp.venue_id) AS occurrence_refs
FROM _pf_merge_plan mp
JOIN venue v ON v.id = mp.venue_id
ORDER BY mp.group_id, CASE mp.role WHEN 'KEEPER' THEN 0 ELSE 1 END, mp.venue_id;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2: Happening reassignments (discard → keeper)
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 2: Happening Reassignments ──' AS section;

SELECT
  d.group_id,
  h.id AS happening_id,
  h.title AS happening_title,
  h.primary_venue_id AS current_venue_id,
  v_old.name AS current_venue_name,
  d.keeper_id AS new_venue_id,
  v_new.name AS new_venue_name
FROM _pf_discards d
JOIN happening h ON h.primary_venue_id = d.discard_id
JOIN venue v_old ON v_old.id = d.discard_id
JOIN venue v_new ON v_new.id = d.keeper_id
ORDER BY d.group_id, h.title;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 3: Occurrence reassignments (discard → keeper)
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 3: Occurrence Reassignments ──' AS section;

SELECT
  d.group_id,
  o.id AS occurrence_id,
  o.venue_id AS current_venue_id,
  v_old.name AS current_venue_name,
  d.keeper_id AS new_venue_id,
  v_new.name AS new_venue_name
FROM _pf_discards d
JOIN occurrence o ON o.venue_id = d.discard_id
JOIN venue v_old ON v_old.id = d.discard_id
JOIN venue v_new ON v_new.id = d.keeper_id
ORDER BY d.group_id, o.id;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 4: Keeper field updates (current → canonical)
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 4: Keeper Field Updates ──' AS section;

SELECT
  kc.keeper_id,
  'name' AS field,
  v.name AS current_value,
  kc.canonical_name AS new_value,
  CASE WHEN v.name = kc.canonical_name THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END AS action
FROM _pf_keeper_canonical kc
JOIN venue v ON v.id = kc.keeper_id

UNION ALL

SELECT kc.keeper_id, 'address_line1',
  COALESCE(v.address_line1, '(null)'), kc.address_line1,
  CASE WHEN v.address_line1 IS NOT DISTINCT FROM kc.address_line1 THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END
FROM _pf_keeper_canonical kc JOIN venue v ON v.id = kc.keeper_id

UNION ALL

SELECT kc.keeper_id, 'postal_code',
  COALESCE(v.postal_code, '(null)'), kc.postal_code,
  CASE WHEN v.postal_code IS NOT DISTINCT FROM kc.postal_code THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END
FROM _pf_keeper_canonical kc JOIN venue v ON v.id = kc.keeper_id

UNION ALL

SELECT kc.keeper_id, 'locality',
  COALESCE(v.locality, '(null)'), kc.locality,
  CASE WHEN v.locality IS NOT DISTINCT FROM kc.locality THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END
FROM _pf_keeper_canonical kc JOIN venue v ON v.id = kc.keeper_id

UNION ALL

SELECT kc.keeper_id, 'lat',
  COALESCE(v.lat::text, '(null)'), kc.lat::text,
  CASE WHEN v.lat IS NOT DISTINCT FROM kc.lat THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END
FROM _pf_keeper_canonical kc JOIN venue v ON v.id = kc.keeper_id

UNION ALL

SELECT kc.keeper_id, 'lng',
  COALESCE(v.lng::text, '(null)'), kc.lng::text,
  CASE WHEN v.lng IS NOT DISTINCT FROM kc.lng THEN 'NO_CHANGE' ELSE 'WILL_UPDATE' END
FROM _pf_keeper_canonical kc JOIN venue v ON v.id = kc.keeper_id

ORDER BY keeper_id, field;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 5: Blocker check (schema-safe)
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 5: Blocker Check ──' AS section;

-- Check: any keeper venue_id that does not exist in venue table
SELECT 'missing_keeper' AS blocker, mp.venue_id, '(venue row missing)' AS detail
FROM _pf_merge_plan mp
WHERE mp.role = 'KEEPER'
  AND NOT EXISTS (SELECT 1 FROM venue v WHERE v.id = mp.venue_id);

-- Check: any discard venue_id that does not exist in venue table
SELECT 'missing_discard' AS blocker, d.discard_id AS venue_id, d.label AS detail
FROM _pf_discards d
WHERE NOT EXISTS (SELECT 1 FROM venue v WHERE v.id = d.discard_id);

-- Schema-safe check: discard venues referenced by courses (table may not exist)
DO $$
BEGIN
  IF to_regclass('public.courses') IS NOT NULL THEN
    PERFORM 1 FROM courses c
    WHERE c.venue_id IN (SELECT discard_id FROM _pf_discards);
    IF FOUND THEN
      RAISE NOTICE 'BLOCKER: courses table references discard venue(s)';
    ELSE
      RAISE NOTICE 'OK: courses table exists, no discard references';
    END IF;
  ELSE
    RAISE NOTICE 'OK: courses table does not exist, skipping';
  END IF;
END $$;

DO $$
BEGIN
  IF to_regclass('public.course_sessions') IS NOT NULL THEN
    PERFORM 1 FROM course_sessions cs
    WHERE cs.venue_id IN (SELECT discard_id FROM _pf_discards);
    IF FOUND THEN
      RAISE NOTICE 'BLOCKER: course_sessions table references discard venue(s)';
    ELSE
      RAISE NOTICE 'OK: course_sessions table exists, no discard references';
    END IF;
  ELSE
    RAISE NOTICE 'OK: course_sessions table does not exist, skipping';
  END IF;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 6: Summary counts
-- ═══════════════════════════════════════════════════════════════════════════

SELECT '── SECTION 6: Summary ──' AS section;

SELECT
  (SELECT count(*) FROM _pf_discards) AS total_discards,
  (SELECT count(DISTINCT venue_id) FROM _pf_merge_plan WHERE role = 'KEEPER') AS total_keepers,
  (SELECT count(*) FROM happening h JOIN _pf_discards d ON h.primary_venue_id = d.discard_id) AS happenings_to_reassign,
  (SELECT count(*) FROM occurrence o JOIN _pf_discards d ON o.venue_id = d.discard_id) AS occurrences_to_reassign;


-- ─── Cleanup temp tables ────────────────────────────────────────────────────

DROP TABLE IF EXISTS _pf_merge_plan;
DROP TABLE IF EXISTS _pf_discards;
DROP TABLE IF EXISTS _pf_keeper_canonical;
