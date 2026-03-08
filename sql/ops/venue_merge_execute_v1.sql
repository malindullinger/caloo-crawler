-- =============================================================================
-- VENUE MERGE EXECUTE V1 — Mutation script
-- Performs FK reassignment + keeper canonicalization for 5 approved groups.
-- Does NOT delete discard venue rows (Phase 2).
--
-- Scope: hardcoded to 5 approved Tier A merge groups (10 discards → 5 keepers).
-- Idempotent: safe to run multiple times — produces same result.
-- Transaction-wrapped: all or nothing.
--
-- Schema confirmation (verified via information_schema.columns):
--   venue.updated_at      → timestamp with time zone ✓
--   happening.updated_at  → timestamp with time zone ✓
--   occurrence.updated_at → timestamp with time zone ✓
--
-- Prerequisites:
--   Run venue_merge_preflight_v1.sql first and verify output.
--
-- Usage:
--   psql $DATABASE_URL -f sql/ops/venue_merge_execute_v1.sql
-- =============================================================================

BEGIN;

-- ─── Hardcoded approved merge groups ────────────────────────────────────────

CREATE TEMP TABLE _merge_discards (
  group_id   int NOT NULL,
  discard_id uuid NOT NULL,
  keeper_id  uuid NOT NULL
) ON COMMIT DROP;

INSERT INTO _merge_discards (group_id, discard_id, keeper_id) VALUES
  -- Group 1: Untervogthaus → keeper 27edf7a5
  (1, '0f62aae8-d4e2-4d44-a070-ddf32f42c4de', '27edf7a5-8ed1-42a5-986b-7c493cd38310'),
  (1, '045964d8-9081-4bee-b6d3-28423ea19f0f', '27edf7a5-8ed1-42a5-986b-7c493cd38310'),
  (1, '9bf427e7-48a9-4278-9fd2-31d46dcbcfb2', '27edf7a5-8ed1-42a5-986b-7c493cd38310'),
  -- Group 2: Gemeindesaal → keeper 929c2471
  (2, '049a5fe0-b3e5-47f3-acda-62c6fadd0690', '929c2471-9d86-49c6-b86c-90ec62e2506f'),
  (2, '430f3315-48a3-4889-b9e0-6e1c895e0f3f', '929c2471-9d86-49c6-b86c-90ec62e2506f'),
  (2, '8be85954-ac16-415c-874e-20690921c43a', '929c2471-9d86-49c6-b86c-90ec62e2506f'),
  -- Group 3: Turnhalle Blatten → keeper 4d924e77
  (3, '28b9db0e-4908-4038-a007-4572e5783e1d', '4d924e77-ce9b-4d45-a1e8-87b7913327e4'),
  -- Group 4: Familienzentrum Männedorf → keeper 4d749ceb
  (4, '84de632c-cb32-4058-9311-610422728fdc', '4d749ceb-278e-4625-8bec-dc4c9af3ca6b'),
  (4, '672c22a2-0c4c-4a0f-8722-8988260925cd', '4d749ceb-278e-4625-8bec-dc4c9af3ca6b'),
  -- Group 5: Anna Zemp-Stiftung → keeper f25faacc
  (5, '7a9d858b-4315-4e38-9050-636ae9def606', 'f25faacc-2eb6-4ebb-bb86-2c38bb4872f5');

CREATE TEMP TABLE _keeper_canonical (
  keeper_id     uuid NOT NULL,
  canonical_name text NOT NULL,
  address_line1  text NOT NULL,
  postal_code    text NOT NULL,
  locality       text NOT NULL,
  lat            double precision NOT NULL,
  lng            double precision NOT NULL
) ON COMMIT DROP;

INSERT INTO _keeper_canonical VALUES
  ('27edf7a5-8ed1-42a5-986b-7c493cd38310', 'Untervogthaus',            'Dorfgasse 37',          '8708', 'Männedorf', 47.2532197, 8.690348),
  ('929c2471-9d86-49c6-b86c-90ec62e2506f', 'Gemeindesaal',             'Alte Landstrasse 250',  '8708', 'Männedorf', 47.2533338, 8.6951263),
  ('4d924e77-ce9b-4d45-a1e8-87b7913327e4', 'Turnhalle Blatten',        'Schulstrasse 25',       '8708', 'Männedorf', 47.2540303, 8.6965024),
  ('4d749ceb-278e-4625-8bec-dc4c9af3ca6b', 'Familienzentrum Männedorf', 'Alte Landstrasse 250',  '8708', 'Männedorf', 47.2533338, 8.6951263),
  ('f25faacc-2eb6-4ebb-bb86-2c38bb4872f5', 'Anna Zemp-Stiftung',       'Lönerenweg 10',         '8708', 'Männedorf', 47.2627359, 8.698407);


-- ═══════════════════════════════════════════════════════════════════════════
-- PREFLIGHT ASSERTIONS
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  n_keepers int;
  n_discards int;
  n_overlap int;
  n_multi_keeper int;
  missing_keepers int;
  missing_discards int;
BEGIN
  -- Exactly 5 distinct keepers
  SELECT count(DISTINCT keeper_id) INTO n_keepers FROM _merge_discards;
  IF n_keepers != 5 THEN
    RAISE EXCEPTION 'ABORT: expected 5 distinct keepers, found %', n_keepers;
  END IF;

  -- Exactly 10 distinct discards
  SELECT count(DISTINCT discard_id) INTO n_discards FROM _merge_discards;
  IF n_discards != 10 THEN
    RAISE EXCEPTION 'ABORT: expected 10 distinct discards, found %', n_discards;
  END IF;

  -- Keepers and discards are disjoint
  SELECT count(*) INTO n_overlap
  FROM _merge_discards d
  WHERE d.discard_id IN (SELECT DISTINCT keeper_id FROM _merge_discards);
  IF n_overlap > 0 THEN
    RAISE EXCEPTION 'ABORT: % discard(s) also appear as keepers', n_overlap;
  END IF;

  -- No discard maps to more than one keeper
  SELECT count(*) INTO n_multi_keeper
  FROM (
    SELECT discard_id FROM _merge_discards GROUP BY discard_id HAVING count(DISTINCT keeper_id) > 1
  ) x;
  IF n_multi_keeper > 0 THEN
    RAISE EXCEPTION 'ABORT: % discard(s) map to multiple keepers', n_multi_keeper;
  END IF;

  -- All keepers exist in venue table
  SELECT count(*) INTO missing_keepers
  FROM (SELECT DISTINCT keeper_id FROM _merge_discards) k
  WHERE NOT EXISTS (SELECT 1 FROM venue v WHERE v.id = k.keeper_id);
  IF missing_keepers > 0 THEN
    RAISE EXCEPTION 'ABORT: % keeper venue(s) not found in venue table', missing_keepers;
  END IF;

  -- All discards exist in venue table
  SELECT count(*) INTO missing_discards
  FROM _merge_discards d
  WHERE NOT EXISTS (SELECT 1 FROM venue v WHERE v.id = d.discard_id);
  IF missing_discards > 0 THEN
    RAISE EXCEPTION 'ABORT: % discard venue(s) not found in venue table', missing_discards;
  END IF;

  RAISE NOTICE 'Preflight OK: 5 keepers, 10 discards, disjoint, all exist';
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCKER CHECKS (schema-safe)
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  n_blocked int;
BEGIN
  -- Check courses table (no FK constraint, may not exist)
  IF to_regclass('public.courses') IS NOT NULL THEN
    SELECT count(*) INTO n_blocked
    FROM courses c
    JOIN _merge_discards d ON c.venue_id = d.discard_id;
    IF n_blocked > 0 THEN
      RAISE EXCEPTION 'ABORT: courses table references % discard venue(s) — manual fix required', n_blocked;
    END IF;
    RAISE NOTICE 'Blocker check OK: courses exists, 0 discard references';
  ELSE
    RAISE NOTICE 'Blocker check OK: courses table does not exist';
  END IF;

  -- Check course_sessions table (no FK constraint, may not exist)
  IF to_regclass('public.course_sessions') IS NOT NULL THEN
    SELECT count(*) INTO n_blocked
    FROM course_sessions cs
    JOIN _merge_discards d ON cs.venue_id = d.discard_id;
    IF n_blocked > 0 THEN
      RAISE EXCEPTION 'ABORT: course_sessions table references % discard venue(s) — manual fix required', n_blocked;
    END IF;
    RAISE NOTICE 'Blocker check OK: course_sessions exists, 0 discard references';
  ELSE
    RAISE NOTICE 'Blocker check OK: course_sessions table does not exist';
  END IF;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 1: Reassign happening.primary_venue_id (discard → keeper)
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  rows_updated int;
BEGIN
  UPDATE happening h
  SET    primary_venue_id = d.keeper_id,
         updated_at = now()
  FROM   _merge_discards d
  WHERE  h.primary_venue_id = d.discard_id;

  GET DIAGNOSTICS rows_updated = ROW_COUNT;

  -- Expect 13 rows on first run, 0 on idempotent rerun
  IF rows_updated > 13 THEN
    RAISE EXCEPTION 'ABORT: happening reassignment updated % rows (expected <= 13) — possible overreach', rows_updated;
  END IF;

  RAISE NOTICE 'Step 1: happening reassignment updated % rows', rows_updated;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 2: Reassign occurrence.venue_id (discard → keeper)
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  rows_updated int;
BEGIN
  UPDATE occurrence o
  SET    venue_id = d.keeper_id,
         updated_at = now()
  FROM   _merge_discards d
  WHERE  o.venue_id = d.discard_id;

  GET DIAGNOSTICS rows_updated = ROW_COUNT;

  -- Preflight shows 0 occurrences reference discards — abort on any unexpected rows
  IF rows_updated > 0 THEN
    RAISE EXCEPTION 'ABORT: occurrence reassignment updated % rows (expected 0) — possible overreach', rows_updated;
  END IF;

  RAISE NOTICE 'Step 2: occurrence reassignment updated 0 rows (as expected)';
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 3: Canonicalize keeper venue fields
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  rows_updated int;
BEGIN
  UPDATE venue v
  SET    name          = kc.canonical_name,
         address_line1 = kc.address_line1,
         postal_code   = kc.postal_code,
         locality      = kc.locality,
         lat           = kc.lat,
         lng           = kc.lng,
         updated_at    = now()
  FROM   _keeper_canonical kc
  WHERE  v.id = kc.keeper_id;

  GET DIAGNOSTICS rows_updated = ROW_COUNT;

  IF rows_updated != 5 THEN
    RAISE EXCEPTION 'ABORT: keeper canonicalization updated % rows (expected exactly 5)', rows_updated;
  END IF;

  RAISE NOTICE 'Step 3: canonicalized % keeper venue rows', rows_updated;
END $$;


-- ═══════════════════════════════════════════════════════════════════════════
-- POST-MUTATION VALIDATION
-- ═══════════════════════════════════════════════════════════════════════════

DO $$
DECLARE
  h_remaining int;
  o_remaining int;
  discards_exist int;
  keeper_mismatches int;
BEGIN
  -- 1. No happenings still reference discards
  SELECT count(*) INTO h_remaining
  FROM happening h
  JOIN _merge_discards d ON h.primary_venue_id = d.discard_id;

  IF h_remaining > 0 THEN
    RAISE EXCEPTION 'VALIDATION FAILED: % happenings still reference discard venues', h_remaining;
  END IF;

  -- 2. No occurrences still reference discards
  SELECT count(*) INTO o_remaining
  FROM occurrence o
  JOIN _merge_discards d ON o.venue_id = d.discard_id;

  IF o_remaining > 0 THEN
    RAISE EXCEPTION 'VALIDATION FAILED: % occurrences still reference discard venues', o_remaining;
  END IF;

  -- 3. All 10 discard rows still exist (not deleted in this phase)
  SELECT count(*) INTO discards_exist
  FROM _merge_discards d
  WHERE EXISTS (SELECT 1 FROM venue v WHERE v.id = d.discard_id);

  IF discards_exist != 10 THEN
    RAISE EXCEPTION 'VALIDATION FAILED: expected 10 discard rows still in venue, found %', discards_exist;
  END IF;

  -- 4. All 5 keepers match canonical target values
  SELECT count(*) INTO keeper_mismatches
  FROM _keeper_canonical kc
  JOIN venue v ON v.id = kc.keeper_id
  WHERE v.name          IS DISTINCT FROM kc.canonical_name
     OR v.address_line1 IS DISTINCT FROM kc.address_line1
     OR v.postal_code   IS DISTINCT FROM kc.postal_code
     OR v.locality      IS DISTINCT FROM kc.locality
     OR v.lat           IS DISTINCT FROM kc.lat
     OR v.lng           IS DISTINCT FROM kc.lng;

  IF keeper_mismatches > 0 THEN
    RAISE EXCEPTION 'VALIDATION FAILED: % keeper(s) do not match canonical target values', keeper_mismatches;
  END IF;

  RAISE NOTICE 'Post-mutation validation PASSED:';
  RAISE NOTICE '  - 0 orphaned happening refs';
  RAISE NOTICE '  - 0 orphaned occurrence refs';
  RAISE NOTICE '  - 10 discard rows preserved';
  RAISE NOTICE '  - 5 keepers match canonical values';
END $$;


COMMIT;

-- Phase 2 (future): DELETE discard venue rows after confirming no regressions.
