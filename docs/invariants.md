# Database Invariants

> **This document is normative.** These constraints are enforced at the database level.

---

## Time Contract Invariant

The unknown-time contract is enforced via CHECK constraints on `source_happenings` and `course_sessions`.

### Rules

| date_precision | start_at | end_at | Valid? |
|----------------|----------|--------|--------|
| `'date'` | NULL | NULL | ✅ Yes |
| `'date'` | any value | any | ❌ No |
| `'datetime'` | NOT NULL | any | ✅ Yes |
| `'datetime'` | NULL | any | ❌ No |

### Constraint Definition

```sql
CHECK (
  (date_precision = 'date' AND start_at IS NULL AND end_at IS NULL)
  OR
  (date_precision = 'datetime' AND start_at IS NOT NULL)
)

```

---

## Verification Examples

> **Prerequisite:** Apply migrations 001–007 before running these verification inserts.

### Failing Insert #1: date precision with time (VIOLATES CONTRACT)

```sql
-- This MUST fail: date_precision='date' but start_at is set
INSERT INTO source_happenings (
  source_id,
  source_type,
  source_tier,
  external_id,
  title_raw,
  date_precision,
  start_at  -- NOT ALLOWED when date_precision='date'
) VALUES (
  'test_source',
  'crawler',
  'A',
  'test-001',
  'Test Event',
  'date',
  '2026-03-15T00:00:00+00:00'  -- VIOLATION: using midnight as placeholder
);

-- Expected error:
-- ERROR: new row for relation "source_happenings" violates check constraint "source_happenings_time_contract"
```

### Failing Insert #2: datetime precision without start (VIOLATES CONTRACT)

```sql
-- This MUST fail: date_precision='datetime' but start_at is NULL
INSERT INTO source_happenings (
  source_id,
  source_type,
  source_tier,
  external_id,
  title_raw,
  date_precision,
  start_at  -- NULL not allowed when date_precision='datetime'
) VALUES (
  'test_source',
  'crawler',
  'A',
  'test-002',
  'Test Event with Time',
  'datetime',
  NULL  -- VIOLATION: datetime requires start_at
);

-- Expected error:
-- ERROR: new row for relation "source_happenings" violates check constraint "source_happenings_time_contract"
```

### Passing Insert: date-only record (VALID)

```sql
-- This MUST succeed: date_precision='date' with no times
INSERT INTO source_happenings (
  source_id,
  source_type,
  source_tier,
  external_id,
  title_raw,
  date_precision,
  start_at,
  end_at
) VALUES (
  'test_source',
  'crawler',
  'A',
  'test-003',
  'All-Day Event',
  'date',
  NULL,  -- Correct: no start time
  NULL   -- Correct: no end time
);

-- Expected: INSERT 0 1 (success)
```

### Passing Insert: datetime record (VALID)

```sql
-- This MUST succeed: date_precision='datetime' with start_at
INSERT INTO source_happenings (
  source_id,
  source_type,
  source_tier,
  external_id,
  title_raw,
  date_precision,
  start_at,
  end_at
) VALUES (
  'test_source',
  'crawler',
  'A',
  'test-004',
  'Timed Event',
  'datetime',
  '2026-03-15T14:00:00+00:00',  -- Correct: has meaningful time
  '2026-03-15T16:00:00+00:00'   -- Optional end time
);

-- Expected: INSERT 0 1 (success)
```

---

## Cleanup After Testing

```sql
-- Remove test records
DELETE FROM source_happenings WHERE source_id = 'test_source';
```

---

## Why This Matters

Without DB-level enforcement:
- Code bugs could insert `00:00` as a placeholder time
- Manual SQL updates could violate the contract
- Data integrity depends entirely on application code

With DB-level enforcement:
- Contract is guaranteed regardless of how data is inserted
- Invalid states are impossible
- Bugs are caught at insert time, not at display time
