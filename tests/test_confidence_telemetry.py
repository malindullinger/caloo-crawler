"""
Phase 9: Confidence Telemetry — Contract Tests

Verifies:
  1. Bucket boundary mapping (edges and interior)
  2. StatsAccumulator min/max/avg determinism
  3. Histogram update correctness
  4. Per-source independence
  5. None/empty telemetry behavior
  6. Clamping of values outside [0, 1]
  7. ConfidenceTelemetry.as_source_json() structure
"""
from __future__ import annotations


# ===========================================================================
# Part 1: Bucket mapping boundaries
# ===========================================================================

def test_bucket_key_zero():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.0) == "0_50"


def test_bucket_key_just_below_50():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.49) == "0_50"


def test_bucket_key_at_50():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.5) == "50_70"


def test_bucket_key_at_70():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.7) == "70_85"


def test_bucket_key_at_85():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.85) == "85_95"


def test_bucket_key_at_95():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.95) == "95_99"


def test_bucket_key_at_99():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.99) == "99_100"


def test_bucket_key_at_100():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(1.0) == "99_100"


def test_bucket_key_just_below_99():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(0.989) == "95_99"


# ===========================================================================
# Part 2: Clamping
# ===========================================================================

def test_bucket_key_clamps_negative():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(-0.5) == "0_50"


def test_bucket_key_clamps_above_one():
    from src.db.confidence_telemetry import bucket_key
    assert bucket_key(1.5) == "99_100"


def test_stats_accumulator_clamps_values():
    from src.db.confidence_telemetry import StatsAccumulator
    acc = StatsAccumulator()
    acc.add(-0.5)
    acc.add(1.5)
    assert acc.min == 0.0
    assert acc.max == 1.0
    assert acc.count == 2


# ===========================================================================
# Part 3: StatsAccumulator
# ===========================================================================

def test_stats_accumulator_empty():
    from src.db.confidence_telemetry import StatsAccumulator
    acc = StatsAccumulator()
    assert acc.count == 0
    assert acc.min is None
    assert acc.max is None
    assert acc.avg is None


def test_stats_accumulator_single_value():
    from src.db.confidence_telemetry import StatsAccumulator
    acc = StatsAccumulator()
    acc.add(0.75)
    assert acc.count == 1
    assert acc.min == 0.75
    assert acc.max == 0.75
    assert acc.avg == 0.75


def test_stats_accumulator_multiple_values():
    from src.db.confidence_telemetry import StatsAccumulator
    acc = StatsAccumulator()
    acc.add(0.5)
    acc.add(0.8)
    acc.add(1.0)
    assert acc.count == 3
    assert acc.min == 0.5
    assert acc.max == 1.0
    assert abs(acc.avg - 0.7666666666666667) < 1e-10


def test_stats_accumulator_deterministic():
    """Same inputs always produce the same stats."""
    from src.db.confidence_telemetry import StatsAccumulator
    values = [0.92, 0.71, 0.85, 1.0, 0.63]

    acc1 = StatsAccumulator()
    acc2 = StatsAccumulator()
    for v in values:
        acc1.add(v)
        acc2.add(v)

    assert acc1.min == acc2.min
    assert acc1.max == acc2.max
    assert acc1.avg == acc2.avg
    assert acc1.count == acc2.count


# ===========================================================================
# Part 4: Histogram
# ===========================================================================

def test_new_histogram_all_zeros():
    from src.db.confidence_telemetry import new_histogram, BUCKET_KEYS
    hist = new_histogram()
    assert set(hist.keys()) == set(BUCKET_KEYS)
    assert all(v == 0 for v in hist.values())


def test_update_histogram_increments_correct_bucket():
    from src.db.confidence_telemetry import new_histogram, update_histogram
    hist = new_histogram()
    update_histogram(hist, 0.75)
    assert hist["70_85"] == 1
    assert sum(hist.values()) == 1


def test_update_histogram_multiple_scores():
    from src.db.confidence_telemetry import new_histogram, update_histogram
    hist = new_histogram()
    scores = [0.3, 0.6, 0.8, 0.9, 0.97, 1.0]
    for s in scores:
        update_histogram(hist, s)
    assert hist["0_50"] == 1
    assert hist["50_70"] == 1
    assert hist["70_85"] == 1
    assert hist["85_95"] == 1
    assert hist["95_99"] == 1
    assert hist["99_100"] == 1


# ===========================================================================
# Part 5: Per-source independence
# ===========================================================================

def test_per_source_stats_independent():
    """Scores for source A do not affect source B stats."""
    from src.db.confidence_telemetry import ConfidenceTelemetry
    t = ConfidenceTelemetry()
    t.add("source_a", 0.5)
    t.add("source_a", 0.6)
    t.add("source_b", 0.9)

    src_json = t.as_source_json()
    assert src_json is not None
    assert src_json["source_a"]["min"] == 0.5
    assert src_json["source_a"]["max"] == 0.6
    assert src_json["source_b"]["min"] == 0.9
    assert src_json["source_b"]["max"] == 0.9


def test_per_source_histograms_independent():
    from src.db.confidence_telemetry import ConfidenceTelemetry
    t = ConfidenceTelemetry()
    t.add("source_a", 0.3)
    t.add("source_b", 0.99)

    src_json = t.as_source_json()
    assert src_json["source_a"]["hist"]["0_50"] == 1
    assert src_json["source_a"]["hist"]["99_100"] == 0
    assert src_json["source_b"]["hist"]["99_100"] == 1
    assert src_json["source_b"]["hist"]["0_50"] == 0


# ===========================================================================
# Part 6: Empty telemetry
# ===========================================================================

def test_empty_telemetry_global_stats_none():
    from src.db.confidence_telemetry import ConfidenceTelemetry
    t = ConfidenceTelemetry()
    assert t.global_stats.min is None
    assert t.global_stats.avg is None
    assert t.global_stats.max is None


def test_empty_telemetry_global_hist_all_zeros():
    from src.db.confidence_telemetry import ConfidenceTelemetry, BUCKET_KEYS
    t = ConfidenceTelemetry()
    assert all(t.global_hist[k] == 0 for k in BUCKET_KEYS)


def test_empty_telemetry_source_json_is_none():
    from src.db.confidence_telemetry import ConfidenceTelemetry
    t = ConfidenceTelemetry()
    assert t.as_source_json() is None


# ===========================================================================
# Part 7: as_source_json structure
# ===========================================================================

def test_as_source_json_structure():
    """Verify the JSON output matches the expected JSONB column format."""
    from src.db.confidence_telemetry import ConfidenceTelemetry, BUCKET_KEYS
    t = ConfidenceTelemetry()
    t.add("zurich_gemeinde", 0.92)
    t.add("zurich_gemeinde", 0.71)
    t.add("winterthur_stadt", 0.85)

    result = t.as_source_json()
    assert result is not None

    # Both sources present
    assert "zurich_gemeinde" in result
    assert "winterthur_stadt" in result

    # Required keys per source
    for src_id in ("zurich_gemeinde", "winterthur_stadt"):
        entry = result[src_id]
        assert "min" in entry
        assert "avg" in entry
        assert "max" in entry
        assert "hist" in entry
        assert set(entry["hist"].keys()) == set(BUCKET_KEYS)

    # Specific values
    assert result["zurich_gemeinde"]["min"] == 0.71
    assert result["zurich_gemeinde"]["max"] == 0.92
    assert result["winterthur_stadt"]["min"] == 0.85
    assert result["winterthur_stadt"]["max"] == 0.85
