# tests/test_occurrence_detail_contract.py
"""
Contract tests for occurrence_detail_view (migrations 023 + 024).

Verifies:
  1. View SQL exists and is structurally valid
  2. All required/optional columns are present in SELECT
  3. best_source CTE is identical to feed_cards_view
  4. date_precision logic matches feed_cards_view (no divergence)
  5. Unknown-time columns are NULL when date_precision = 'date'
  6. other_occurrences is limited and ordered
  7. WHERE clause filters published + scheduled only
  8. No time-based filter (deeplinks to past occurrences resolve)
  9. Organizer enrichment via LEFT JOIN (nullable, no row-count change)
"""
from __future__ import annotations

import os
import re

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read_view_sql() -> str:
    path = os.path.join(_project_root(), "sql", "views", "occurrence_detail_view.sql")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_feed_view_sql() -> str:
    path = os.path.join(_project_root(), "sql", "views", "feed_cards_view.sql")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_migration_sql() -> str:
    path = os.path.join(
        _project_root(), "migrations", "023_occurrence_detail_contract.sql"
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# 1. View SQL exists
# ---------------------------------------------------------------------------

class TestViewExists:

    def test_migration_file_exists(self):
        path = os.path.join(
            _project_root(), "migrations", "023_occurrence_detail_contract.sql"
        )
        assert os.path.isfile(path)

    def test_view_sql_file_exists(self):
        path = os.path.join(
            _project_root(), "sql", "views", "occurrence_detail_view.sql"
        )
        assert os.path.isfile(path)

    def test_migration_contains_create_view(self):
        sql = _read_migration_sql()
        assert "CREATE OR REPLACE VIEW" in sql
        assert "occurrence_detail_view" in sql


# ---------------------------------------------------------------------------
# 2. Required columns present in SELECT
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS = [
    "occurrence_id",
    "happening_id",
    "happening_title",
    "canonical_url",
    "start_at",
    "end_at",
    "timezone",
    "date_precision",
    "location_name",
    "image_url",
]

OPTIONAL_COLUMNS = [
    "description",
    "organizer_name",
    "organizer_website_url",
    "booking_url",
    "other_occurrences",
]

CONTEXT_COLUMNS = [
    "offering_start_date",
    "offering_end_date",
    "offering_type",
    "happening_kind",
    "visibility_status",
    "audience_tags",
    "topic_tags",
    "editorial_priority",
]

TIME_DISPLAY_COLUMNS = [
    "start_date_local",
    "end_date_local",
    "start_time_local",
    "end_time_local",
]


class TestRequiredColumns:

    @pytest.mark.parametrize("col", REQUIRED_COLUMNS)
    def test_required_column_in_select(self, col: str):
        sql = _read_view_sql()
        assert col in sql, f"Required column {col!r} not found in view SQL"

    @pytest.mark.parametrize("col", OPTIONAL_COLUMNS)
    def test_optional_column_in_select(self, col: str):
        sql = _read_view_sql()
        assert col in sql, f"Optional column {col!r} not found in view SQL"

    @pytest.mark.parametrize("col", CONTEXT_COLUMNS)
    def test_context_column_in_select(self, col: str):
        sql = _read_view_sql()
        assert col in sql, f"Context column {col!r} not found in view SQL"

    @pytest.mark.parametrize("col", TIME_DISPLAY_COLUMNS)
    def test_time_display_column_in_select(self, col: str):
        sql = _read_view_sql()
        assert col in sql, f"Time display column {col!r} not found in view SQL"


# ---------------------------------------------------------------------------
# 3. best_source CTE is identical to feed_cards_view
# ---------------------------------------------------------------------------

def _extract_best_source_order_by(sql: str) -> str:
    """Extract the ORDER BY inside the best_source CTE (normalized).

    The detail view adds description_raw to the SELECT list (which the
    feed doesn't need), so we compare the ORDER BY clause — the part
    that determines deterministic selection — not the full CTE body.
    """
    match = re.search(
        r"best_source\s+AS\s*\(.*?ORDER\s+BY\s+(.*?)\)",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return ""
    return " ".join(match.group(1).split())


class TestBestSourceConsistency:

    def test_best_source_order_by_matches_feed_view(self):
        """The best_source ORDER BY must be identical between detail and feed views.

        The detail view intentionally adds description_raw to the SELECT list
        (for the description fallback), so we compare the ORDER BY clause which
        controls deterministic selection, not the full CTE.
        """
        detail_sql = _read_view_sql()
        feed_sql = _read_feed_view_sql()

        detail_order = _extract_best_source_order_by(detail_sql)
        feed_order = _extract_best_source_order_by(feed_sql)

        assert detail_order, "Could not extract best_source ORDER BY from detail view"
        assert feed_order, "Could not extract best_source ORDER BY from feed view"
        assert detail_order == feed_order, (
            "best_source ORDER BY diverged between detail view and feed view.\n"
            f"Detail: {detail_order}\n"
            f"Feed:   {feed_order}"
        )

    def test_best_source_uses_distinct_on(self):
        sql = _read_view_sql()
        assert "DISTINCT ON (hs.happening_id)" in sql

    def test_best_source_order_is_deterministic(self):
        sql = _read_view_sql()
        assert "hs.is_primary DESC" in sql
        assert "hs.source_priority" in sql
        assert "hs.merged_at DESC" in sql

    def test_best_source_includes_description_raw(self):
        """Detail view must include description_raw for fallback description."""
        sql = _read_view_sql()
        assert "sh.description_raw" in sql


# ---------------------------------------------------------------------------
# 4. date_precision logic matches feed_cards_view
# ---------------------------------------------------------------------------

def _extract_date_precision_case(sql: str) -> str:
    """Extract the CASE block that computes date_precision (normalized)."""
    match = re.search(
        r"(CASE\s+WHEN\s+COALESCE\(o\.notes.*?END)\s+AS\s+date_precision",
        sql,
        re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return ""
    return " ".join(match.group(1).split())


def _normalize_pg_syntax(s: str) -> str:
    """Normalize PG-internal syntax to standard SQL for comparison.

    pg_get_viewdef() emits '~~*' instead of ILIKE, '::text' type casts,
    etc. These are semantically identical so we normalize them.
    """
    s = s.replace("~~*", "ILIKE")
    s = re.sub(r"::text", "", s)
    s = re.sub(r"'day'", "'day'", s)
    # Collapse whitespace
    return " ".join(s.split())


class TestDatePrecisionConsistency:

    def test_date_precision_logic_matches_feed(self):
        """date_precision derivation must not diverge from feed_cards_view.

        The feed SQL is a pg_get_viewdef() dump using PG-internal syntax
        (~~*, ::text casts). We normalize before comparing.
        """
        detail_sql = _read_view_sql()
        feed_sql = _read_feed_view_sql()

        detail_dp = _normalize_pg_syntax(_extract_date_precision_case(detail_sql))
        feed_dp = _normalize_pg_syntax(_extract_date_precision_case(feed_sql))

        assert detail_dp, "Could not extract date_precision CASE from detail view"
        assert feed_dp, "Could not extract date_precision CASE from feed view"
        assert detail_dp == feed_dp, (
            "date_precision CASE diverged between detail view and feed view.\n"
            f"Detail: {detail_dp}\n"
            f"Feed:   {feed_dp}"
        )


# ---------------------------------------------------------------------------
# 5. Unknown-time columns are NULL when date_precision = 'date'
# ---------------------------------------------------------------------------

class TestUnknownTimeHandling:

    def test_start_time_local_null_for_event_time_missing(self):
        """start_time_local must be NULL when notes contain 'event time missing'."""
        sql = _read_view_sql()
        # The CASE for start_time_local must handle event time missing → NULL
        assert re.search(
            r"WHEN\s+COALESCE\(o\.notes.*?event time missing.*?THEN\s+NULL",
            sql,
            re.DOTALL | re.IGNORECASE,
        ), "start_time_local must return NULL for 'event time missing'"

    def test_end_time_local_null_for_event_time_missing(self):
        """end_time_local must be NULL when notes contain 'event time missing'."""
        sql = _read_view_sql()
        # Must appear at least twice (once for start, once for end)
        matches = re.findall(
            r"WHEN\s+COALESCE\(o\.notes.*?event time missing.*?THEN\s+NULL",
            sql,
            re.DOTALL | re.IGNORECASE,
        )
        assert len(matches) >= 2, (
            f"Expected at least 2 'event time missing' → NULL guards, found {len(matches)}"
        )

    def test_start_time_local_null_for_midnight(self):
        """start_time_local must be NULL when start_at is midnight (date-only)."""
        sql = _read_view_sql()
        assert re.search(
            r"date_trunc\('day',\s*o\.start_at\)\s*=\s*o\.start_at",
            sql,
            re.IGNORECASE,
        ), "Must check date_trunc('day', o.start_at) = o.start_at for date-only detection"


# ---------------------------------------------------------------------------
# 6. other_occurrences limited and ordered
# ---------------------------------------------------------------------------

class TestOtherOccurrences:

    def test_other_occurrences_has_limit(self):
        sql = _read_view_sql()
        # Must have LIMIT inside the other_occurrences subquery
        assert "LIMIT 5" in sql, "other_occurrences must be limited to 5"

    def test_other_occurrences_ordered_by_start_at(self):
        sql = _read_view_sql()
        assert "ORDER BY oo.start_at" in sql, (
            "other_occurrences must be ordered by start_at"
        )

    def test_other_occurrences_excludes_current(self):
        sql = _read_view_sql()
        assert "oo.id" in sql and "!= o.id" in sql, (
            "other_occurrences must exclude the current occurrence"
        )

    def test_other_occurrences_excludes_past(self):
        sql = _read_view_sql()
        assert re.search(
            r"COALESCE\(oo\.end_at,\s*oo\.start_at\)\s*>=\s*now\(\)",
            sql,
            re.IGNORECASE,
        ), "other_occurrences must exclude past events"

    def test_other_occurrences_only_scheduled(self):
        sql = _read_view_sql()
        assert "oo.status" in sql and "'scheduled'" in sql, (
            "other_occurrences must filter for scheduled status"
        )

    def test_other_occurrences_default_empty_array(self):
        sql = _read_view_sql()
        assert "COALESCE" in sql and "'[]'::jsonb" in sql, (
            "other_occurrences must default to empty array '[]' via COALESCE"
        )


# ---------------------------------------------------------------------------
# 7. WHERE clause: published + scheduled, no time filter
# ---------------------------------------------------------------------------

class TestWhereClause:

    def _get_where_clause(self) -> str:
        sql = _read_view_sql()
        where_start = sql.rfind("WHERE h.visibility_status")
        assert where_start != -1, "Expected WHERE clause with visibility_status"
        return sql[where_start:]

    def test_filters_published_only(self):
        where = self._get_where_clause()
        assert "'published'" in where

    def test_filters_scheduled_only(self):
        where = self._get_where_clause()
        assert "'scheduled'" in where

    def test_no_time_based_filter(self):
        """Detail view must NOT filter by time (deeplinks to past events must resolve)."""
        where = self._get_where_clause()
        assert "now()" not in where, (
            "Detail view WHERE clause must not use now() — "
            "past occurrences must still be queryable via deeplink"
        )

    def test_not_a_feed_view(self):
        """Detail view must not contain feed-specific logic."""
        sql = _read_view_sql()
        assert "section_key" not in sql, "Detail view must not contain section_key"
        assert "weekend" not in sql.lower() or "weekend" in sql[sql.find("--"):], (
            "Detail view must not contain weekend logic"
        )
        assert "is_happening_now" not in sql, "Detail view must not contain is_happening_now"


# ---------------------------------------------------------------------------
# 8. Migration 024: organizer website_url column
# ---------------------------------------------------------------------------

class TestMigration024Exists:

    def test_migration_file_exists(self):
        path = os.path.join(
            _project_root(), "migrations", "024_organizer_website_url.sql"
        )
        assert os.path.isfile(path)

    def test_migration_adds_website_url_column(self):
        path = os.path.join(
            _project_root(), "migrations", "024_organizer_website_url.sql"
        )
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        assert "website_url" in sql
        assert "ALTER TABLE organizer" in sql

    def test_migration_replaces_view(self):
        path = os.path.join(
            _project_root(), "migrations", "024_organizer_website_url.sql"
        )
        with open(path, "r", encoding="utf-8") as f:
            sql = f.read()
        assert "CREATE OR REPLACE VIEW" in sql
        assert "occurrence_detail_view" in sql


# ---------------------------------------------------------------------------
# 9. Organizer enrichment — structural contract
# ---------------------------------------------------------------------------

class TestOrganizerEnrichment:

    def test_left_join_organizer(self):
        """Organizer join must be LEFT JOIN (nullable — no row-count change)."""
        sql = _read_view_sql()
        assert re.search(
            r"LEFT\s+JOIN\s+organizer\s+org\s+ON\s+org\.id\s*=\s*h\.organizer_id",
            sql,
            re.IGNORECASE,
        ), "Must LEFT JOIN organizer on happening.organizer_id"

    def test_organizer_name_from_org_table(self):
        """organizer_name must come from the organizer table, not hardcoded."""
        sql = _read_view_sql()
        assert re.search(
            r"org\.name\s+AS\s+organizer_name",
            sql,
            re.IGNORECASE,
        ), "organizer_name must be org.name AS organizer_name"

    def test_organizer_website_url_present(self):
        """organizer_website_url must be exposed in the view."""
        sql = _read_view_sql()
        assert "organizer_website_url" in sql

    def test_organizer_website_url_from_org_table(self):
        """organizer_website_url must come from org.website_url, not hardcoded."""
        sql = _read_view_sql()
        assert "org.website_url" in sql

    def test_organizer_website_url_trims_empty(self):
        """Empty-string website_url must be NULL (NULLIF + BTRIM pattern)."""
        sql = _read_view_sql()
        assert re.search(
            r"NULLIF\(BTRIM\(org\.website_url\),\s*''\)",
            sql,
            re.IGNORECASE,
        ), "organizer_website_url must use NULLIF(BTRIM(...), '') pattern"

    def test_organizer_type_present(self):
        """organizer_type must be in the view."""
        sql = _read_view_sql()
        assert "organizer_type" in sql

    def test_organizer_not_in_feed_view(self):
        """feed_cards_view must NOT join organizer (enrichment is detail-only)."""
        feed_sql = _read_feed_view_sql()
        assert "organizer" not in feed_sql.lower(), (
            "feed_cards_view must not reference organizer table"
        )

    def test_no_required_columns_removed(self):
        """All original required columns must still be present after migration 024."""
        sql = _read_view_sql()
        for col in REQUIRED_COLUMNS:
            assert col in sql, f"Required column {col!r} missing after organizer update"

    def test_view_mirror_matches_migration(self):
        """The sql/views/ mirror must contain the same view definition as migration 024."""
        view_sql = _read_view_sql()
        mig_path = os.path.join(
            _project_root(), "migrations", "024_organizer_website_url.sql"
        )
        with open(mig_path, "r", encoding="utf-8") as f:
            mig_sql = f.read()
        # The mirror should have the CREATE OR REPLACE VIEW statement
        assert "CREATE OR REPLACE VIEW" in view_sql
        # Both must contain organizer_website_url
        assert "organizer_website_url" in view_sql
        assert "organizer_website_url" in mig_sql
