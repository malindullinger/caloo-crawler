# tests/test_low_confidence_dashboard.py
"""
Contract tests for low_confidence_dashboard_view (migration 028).

Verifies:
  1. Migration and mirror files exist
  2. All expected columns are present
  3. View uses best_source CTE with correct priority
  4. Sorted by confidence_score ASC
  5. Filters to published happenings only
  6. No feed logic modification
  7. Read-only diagnostic
  8. Mirror matches migration
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


def _read_migration_sql() -> str:
    path = os.path.join(
        _project_root(), "migrations", "028_low_confidence_dashboard.sql"
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_view_sql() -> str:
    path = os.path.join(
        _project_root(), "sql", "views",
        "low_confidence_dashboard_view.sql",
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Expected columns
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = [
    "happening_id",
    "title",
    "confidence_score",
    "source_tier",
    "date_precision",
    "has_image",
    "has_description",
    "canonical_url_present",
    "extraction_method",
]


# ---------------------------------------------------------------------------
# 1. Files exist
# ---------------------------------------------------------------------------

class TestFilesExist:

    def test_migration_file_exists(self):
        path = os.path.join(
            _project_root(), "migrations",
            "028_low_confidence_dashboard.sql",
        )
        assert os.path.isfile(path)

    def test_view_sql_mirror_exists(self):
        path = os.path.join(
            _project_root(), "sql", "views",
            "low_confidence_dashboard_view.sql",
        )
        assert os.path.isfile(path)

    def test_migration_creates_view(self):
        sql = _read_migration_sql()
        assert "CREATE OR REPLACE VIEW" in sql
        assert "low_confidence_dashboard_view" in sql


# ---------------------------------------------------------------------------
# 2. Columns present
# ---------------------------------------------------------------------------

class TestColumnsPresent:

    @pytest.mark.parametrize("column_name", EXPECTED_COLUMNS)
    def test_column_in_view(self, column_name: str):
        sql = _read_view_sql()
        assert f"AS {column_name}" in sql, (
            f"Column {column_name!r} not found in view SQL"
        )

    def test_exactly_nine_columns(self):
        """View must define exactly 9 output columns."""
        sql = _read_view_sql()
        # Count AS <column_name> patterns in the main SELECT (not the CTE)
        # The CTE has its own AS aliases, so look after "SELECT" that follows
        # the closing paren of the CTE
        main_select_idx = sql.rfind("SELECT")
        main_select = sql[main_select_idx:]
        found = re.findall(r"AS\s+(\w+)", main_select)
        assert len(found) == 9, (
            f"Expected 9 columns, found {len(found)}: {found}"
        )


# ---------------------------------------------------------------------------
# 3. Best-source CTE
# ---------------------------------------------------------------------------

class TestBestSourceCTE:

    def test_uses_best_source_cte(self):
        sql = _read_view_sql()
        assert "WITH best_source AS" in sql

    def test_cte_uses_distinct_on(self):
        sql = _read_view_sql()
        assert "DISTINCT ON (hs.happening_id)" in sql

    def test_cte_priority_is_primary_desc(self):
        """CTE must prioritize primary sources first."""
        sql = _read_view_sql()
        assert "is_primary DESC" in sql

    def test_cte_priority_source_priority(self):
        sql = _read_view_sql()
        assert "hs.source_priority" in sql

    def test_cte_priority_merged_at_desc(self):
        sql = _read_view_sql()
        assert "merged_at DESC" in sql


# ---------------------------------------------------------------------------
# 4. Sorting
# ---------------------------------------------------------------------------

class TestSorting:

    def test_sorted_by_confidence_asc(self):
        """View must be sorted by confidence_score ASC (worst first)."""
        sql = _read_view_sql()
        assert re.search(
            r"ORDER\s+BY\s+h\.confidence_score\s+ASC",
            sql,
            re.IGNORECASE,
        )


# ---------------------------------------------------------------------------
# 5. Filtering
# ---------------------------------------------------------------------------

class TestFiltering:

    def test_filters_published_only(self):
        """Only published happenings should appear."""
        sql = _read_view_sql()
        assert "visibility_status = 'published'" in sql

    def test_no_confidence_score_filter(self):
        """View should NOT filter by confidence_score — it shows all published."""
        sql = _read_view_sql()
        # The main WHERE should only have visibility_status, not confidence
        main_select_idx = sql.rfind("SELECT")
        main_section = sql[main_select_idx:]
        where_idx = main_section.find("WHERE")
        assert where_idx > 0
        where_clause = main_section[where_idx:]
        # confidence_score should appear only in SELECT, not WHERE
        assert "confidence_score <" not in where_clause
        assert "confidence_score >" not in where_clause


# ---------------------------------------------------------------------------
# 6. No feed modification
# ---------------------------------------------------------------------------

class TestNoFeedModification:

    def test_does_not_modify_feed_cards_view(self):
        sql = _read_migration_sql()
        views_created = re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\S*(\w+)",
            sql,
            re.IGNORECASE,
        )
        for v in views_created:
            assert v != "feed_cards_view", (
                "Migration must not modify feed_cards_view"
            )

    def test_does_not_modify_system_integrity_view(self):
        sql = _read_migration_sql()
        views_created = re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\S*(\w+)",
            sql,
            re.IGNORECASE,
        )
        for v in views_created:
            assert v != "system_integrity_view", (
                "Migration must not modify system_integrity_view"
            )

    def test_view_is_read_only(self):
        """View must not contain any DML statements."""
        sql = _read_view_sql().upper()
        for keyword in ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER TABLE"]:
            assert keyword not in sql, (
                f"Diagnostic view must not contain {keyword}"
            )


# ---------------------------------------------------------------------------
# 7. Mirror matches migration
# ---------------------------------------------------------------------------

class TestMirrorMatchesMigration:

    def test_both_have_same_view_name(self):
        view_sql = _read_view_sql()
        mig_sql = _read_migration_sql()
        assert "low_confidence_dashboard_view" in view_sql
        assert "low_confidence_dashboard_view" in mig_sql

    def test_both_have_all_columns(self):
        view_sql = _read_view_sql()
        mig_sql = _read_migration_sql()
        for col in EXPECTED_COLUMNS:
            assert f"AS {col}" in view_sql, f"{col} missing from mirror"
            assert f"AS {col}" in mig_sql, f"{col} missing from migration"

    def test_both_have_best_source_cte(self):
        view_sql = _read_view_sql()
        mig_sql = _read_migration_sql()
        assert "WITH best_source AS" in view_sql
        assert "WITH best_source AS" in mig_sql
