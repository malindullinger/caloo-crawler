# src/db/confidence_telemetry.py
"""
Confidence telemetry — deterministic histogram and stats for merge runs.
Pure Python, no DB access. Used by merge_loop to collect per-run metrics.

Phase 9: observability only — does not affect merge decisions.

Bucket edges (fixed):
  [0.0, 0.5), [0.5, 0.7), [0.7, 0.85), [0.85, 0.95), [0.95, 0.99), [0.99, 1.0]

Keys: "0_50", "50_70", "70_85", "85_95", "95_99", "99_100"
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Bucket definitions (fixed, deterministic)
# ---------------------------------------------------------------------------

# (lower_bound_inclusive, upper_bound_exclusive, key)
# Last bucket is inclusive on both ends: [0.99, 1.0]
BUCKET_DEFS: list[tuple[float, float, str]] = [
    (0.0, 0.5, "0_50"),
    (0.5, 0.7, "50_70"),
    (0.7, 0.85, "70_85"),
    (0.85, 0.95, "85_95"),
    (0.95, 0.99, "95_99"),
    (0.99, 1.01, "99_100"),  # upper > 1.0 so 1.0 is included
]

BUCKET_KEYS: list[str] = [b[2] for b in BUCKET_DEFS]


def bucket_key(score: float) -> str:
    """Map a confidence score (0.0–1.0) to its histogram bucket key."""
    clamped = max(0.0, min(1.0, score))
    for lower, upper, key in BUCKET_DEFS:
        if lower <= clamped < upper:
            return key
    # Fallback (should not happen with clamping + upper > 1.0)
    return BUCKET_KEYS[-1]


def new_histogram() -> dict[str, int]:
    """Return a fresh histogram with all bucket keys at 0."""
    return {k: 0 for k in BUCKET_KEYS}


def update_histogram(hist: dict[str, int], score: float) -> None:
    """Increment the correct bucket for the given score."""
    key = bucket_key(score)
    hist[key] = hist.get(key, 0) + 1


# ---------------------------------------------------------------------------
# Stats accumulator
# ---------------------------------------------------------------------------

@dataclass
class StatsAccumulator:
    """Track count, min, max, sum for incremental avg computation."""
    count: int = 0
    min: float | None = None
    max: float | None = None
    sum: float = 0.0

    def add(self, score: float) -> None:
        clamped = max(0.0, min(1.0, score))
        self.count += 1
        self.sum += clamped
        if self.min is None or clamped < self.min:
            self.min = clamped
        if self.max is None or clamped > self.max:
            self.max = clamped

    @property
    def avg(self) -> float | None:
        if self.count == 0:
            return None
        return self.sum / self.count


# ---------------------------------------------------------------------------
# Per-source stats
# ---------------------------------------------------------------------------

@dataclass
class SourceStats:
    """Stats + histogram for a single source_id."""
    stats: StatsAccumulator = field(default_factory=StatsAccumulator)
    hist: dict[str, int] = field(default_factory=new_histogram)


# ---------------------------------------------------------------------------
# Top-level telemetry collector
# ---------------------------------------------------------------------------

@dataclass
class ConfidenceTelemetry:
    """
    Collects confidence scores globally and per source_id.
    Thread-unsafe (single merge_loop is single-threaded).
    """
    global_stats: StatsAccumulator = field(default_factory=StatsAccumulator)
    global_hist: dict[str, int] = field(default_factory=new_histogram)
    _per_source: dict[str, SourceStats] = field(default_factory=dict)

    def add(self, source_id: str, score: float) -> None:
        """Record a confidence score for global and per-source telemetry."""
        self.global_stats.add(score)
        update_histogram(self.global_hist, score)

        if source_id not in self._per_source:
            self._per_source[source_id] = SourceStats()
        src = self._per_source[source_id]
        src.stats.add(score)
        update_histogram(src.hist, score)

    def as_source_json(self) -> dict[str, Any] | None:
        """
        Return per-source telemetry as a JSON-serialisable dict
        for the source_confidence JSONB column.

        Returns None if no scores were recorded.
        """
        if not self._per_source:
            return None
        result: dict[str, Any] = {}
        for src_id, src_stats in self._per_source.items():
            result[src_id] = {
                "min": src_stats.stats.min,
                "avg": src_stats.stats.avg,
                "max": src_stats.stats.max,
                "hist": src_stats.hist,
            }
        return result
