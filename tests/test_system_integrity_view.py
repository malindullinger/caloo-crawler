# tests/test_system_integrity_view.py
"""
Contract tests for system_integrity_view (migrations 025 + 027).

Verifies:
  1. Migration and mirror files exist
  2. All 10 check names are present in the SQL
  3. Each check uses the OK/FAIL status pattern
  4. View uses UNION ALL to combine checks
  5. Output columns are correct
  6. View is read-only (no DML)
  7. Does not modify existing views
  8. New checks I + J (migration 027) have correct structure
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


def _read_migration_025_sql() -> str:
    path = os.path.join(
        _project_root(), "migrations", "025_system_integrity_view.sql"
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_migration_027_sql() -> str:
    path = os.path.join(
        _project_root(), "migrations",
        "027_system_integrity_confidence_extension.sql",
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _read_view_sql() -> str:
    path = os.path.join(
        _project_root(), "sql", "views", "system_integrity_view.sql"
    )
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Expected check names (all 10)
# ---------------------------------------------------------------------------

ORIGINAL_CHECKS = [
    "orphan_occurrences",
    "orphan_offerings",
    "unpublished_future_happenings",
    "negative_duration_occurrences",
    "missing_timezone_occurrences",
    "happenings_without_sources",
    "feed_vs_occurrence_count_drift",
    "detail_vs_feed_visibility_mismatch",
]

NEW_CHECKS = [
    "low_confidence_happenings",
    "tier_b_without_image_ratio",
]

ALL_CHECKS = ORIGINAL_CHECKS + NEW_CHECKS


# ---------------------------------------------------------------------------
# 1. Files exist
# ---------------------------------------------------------------------------

class TestFilesExist:

    def test_migration_025_file_exists(self):
        path = os.path.join(
            _project_root(), "migrations", "025_system_integrity_view.sql"
        )
        assert os.path.isfile(path)

    def test_migration_027_file_exists(self):
        path = os.path.join(
            _project_root(), "migrations",
            "027_system_integrity_confidence_extension.sql",
        )
        assert os.path.isfile(path)

    def test_view_sql_mirror_exists(self):
        path = os.path.join(
            _project_root(), "sql", "views", "system_integrity_view.sql"
        )
        assert os.path.isfile(path)

    def test_migration_025_creates_view(self):
        sql = _read_migration_025_sql()
        assert "CREATE OR REPLACE VIEW" in sql
        assert "system_integrity_view" in sql

    def test_migration_027_replaces_view(self):
        sql = _read_migration_027_sql()
        assert "CREATE OR REPLACE VIEW" in sql
        assert "system_integrity_view" in sql


# ---------------------------------------------------------------------------
# 2. All 10 check names present
# ---------------------------------------------------------------------------

class TestCheckNamesPresent:

    @pytest.mark.parametrize("check_name", ALL_CHECKS)
    def test_check_name_in_view(self, check_name: str):
        sql = _read_view_sql()
        assert f"'{check_name}'" in sql, (
            f"Check name {check_name!r} not found in view SQL"
        )

    def test_exactly_ten_checks(self):
        """View must contain exactly 10 check names."""
        sql = _read_view_sql()
        found = re.findall(r"'(\w+)'::text\s+AS\s+check_name", sql)
        assert len(found) == 1, (
            f"Expected 1 typed check_name (first SELECT defines column), "
            f"found {len(found)}"
        )
        all_checks = []
        for name in ALL_CHECKS:
            if f"'{name}'" in sql:
                all_checks.append(name)
        assert len(all_checks) == 10, (
            f"Expected 10 check names, found {len(all_checks)}: {all_checks}"
        )

    def test_original_eight_preserved_in_027(self):
        """Migration 027 must preserve all 8 original checks."""
        sql = _read_migration_027_sql()
        for name in ORIGINAL_CHECKS:
            assert f"'{name}'" in sql, (
                f"Original check {name!r} missing from migration 027"
            )


# ---------------------------------------------------------------------------
# 3. Status pattern: OK / FAIL
# ---------------------------------------------------------------------------

class TestStatusPattern:

    def test_all_checks_use_ok_fail_status(self):
        """Every check must produce only OK or FAIL status values."""
        sql = _read_view_sql()
        # Match all THEN 'X' patterns in CASE expressions
        then_values = re.findall(r"THEN\s+'(\w+)'", sql)
        else_values = re.findall(r"ELSE\s+'(\w+)'", sql)
        all_status_values = set(then_values + else_values)
        assert all_status_values == {"OK", "FAIL"}, (
            f"Expected only OK/FAIL, found: {all_status_values}"
        )

    def test_simple_checks_use_standard_pattern(self):
        """Checks A-I use the standard THEN 'OK' ELSE 'FAIL' pattern."""
        sql = _read_view_sql()
        simple_pattern_count = len(
            re.findall(r"THEN\s+'OK'\s+ELSE\s+'FAIL'", sql)
        )
        # A-I = 9 simple patterns (J uses multi-branch CASE)
        assert simple_pattern_count == 9, (
            f"Expected 9 simple OK/FAIL patterns, found {simple_pattern_count}"
        )


# ---------------------------------------------------------------------------
# 4. Structural invariants
# ---------------------------------------------------------------------------

class TestStructure:

    def test_union_all_joins_checks(self):
        """View must use UNION ALL to combine all 10 checks."""
        sql = _read_view_sql()
        union_count = len(re.findall(r"UNION\s+ALL", sql, re.IGNORECASE))
        assert union_count == 9, (
            f"Expected 9 UNION ALL (10 checks), found {union_count}"
        )

    def test_output_columns(self):
        """First SELECT must define the 4 output columns."""
        sql = _read_view_sql()
        assert "AS check_name" in sql
        assert "AS status" in sql
        assert "AS metric_value" in sql
        assert "AS details" in sql

    def test_view_is_read_only(self):
        """View must not contain any DML statements."""
        sql = _read_view_sql().upper()
        for keyword in ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER TABLE"]:
            assert keyword not in sql, (
                f"Diagnostic view must not contain {keyword}"
            )

    def test_does_not_modify_feed_cards_view(self):
        """Must not CREATE OR REPLACE feed_cards_view."""
        sql = _read_migration_027_sql()
        views_created = re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\S*(\w+)",
            sql,
            re.IGNORECASE,
        )
        for v in views_created:
            assert v != "feed_cards_view", (
                "Migration must not modify feed_cards_view"
            )

    def test_does_not_modify_occurrence_detail_view(self):
        """Must not CREATE OR REPLACE occurrence_detail_view."""
        sql = _read_migration_027_sql()
        views_created = re.findall(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+\S*(\w+)",
            sql,
            re.IGNORECASE,
        )
        for v in views_created:
            assert v != "occurrence_detail_view", (
                "Migration must not modify occurrence_detail_view"
            )

    def test_mirror_matches_migration_027(self):
        """Mirror and migration 027 must both contain all 10 check names."""
        view_sql = _read_view_sql()
        mig_sql = _read_migration_027_sql()
        for name in ALL_CHECKS:
            assert f"'{name}'" in view_sql, f"{name} missing from mirror"
            assert f"'{name}'" in mig_sql, f"{name} missing from migration"


# ---------------------------------------------------------------------------
# 5. New checks (migration 027)
# ---------------------------------------------------------------------------

class TestLowConfidenceCheck:
    """Check I: low_confidence_happenings."""

    def test_check_references_confidence_score(self):
        sql = _read_view_sql()
        # Find the section for this check
        assert "confidence_score < 50" in sql

    def test_check_filters_published_only(self):
        """Only published happenings should be checked."""
        sql = _read_view_sql()
        # The check must reference visibility_status = 'published'
        # Find the low_confidence section
        idx = sql.find("'low_confidence_happenings'")
        assert idx > 0
        # Next UNION ALL or end of file marks the boundary
        next_union = sql.find("UNION ALL", idx)
        section = sql[idx:next_union] if next_union > 0 else sql[idx:]
        assert "published" in section

    def test_details_include_min_confidence(self):
        sql = _read_view_sql()
        assert "min_confidence" in sql

    def test_threshold_is_50(self):
        """The threshold for low confidence is score < 50."""
        sql = _read_view_sql()
        idx = sql.find("'low_confidence_happenings'")
        assert idx > 0
        next_union = sql.find("UNION ALL", idx)
        section = sql[idx:next_union] if next_union > 0 else sql[idx:]
        assert "< 50" in section


class TestTierBImageRatioCheck:
    """Check J: tier_b_without_image_ratio."""

    def test_check_references_source_tier_b(self):
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "source_tier" in section
        assert "'B'" in section

    def test_threshold_is_20_percent(self):
        """FAIL threshold is > 0.20 (20%)."""
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "0.20" in section

    def test_uses_primary_source(self):
        """Should filter on is_primary = true for deterministic results."""
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "is_primary" in section

    def test_zero_total_returns_ok(self):
        """When there are no Tier B happenings, status should be OK."""
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "tb.total = 0 THEN 'OK'" in section

    def test_details_include_ratio(self):
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "ratio=" in section

    def test_metric_value_is_percentage(self):
        """metric_value should be ratio * 100 (integer percentage)."""
        sql = _read_view_sql()
        idx = sql.find("'tier_b_without_image_ratio'")
        assert idx > 0
        section = sql[idx:]
        assert "* 100" in section
