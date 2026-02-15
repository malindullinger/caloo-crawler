# src/db/canonical_field_history.py
"""
Canonical field history — log old→new transitions on merge updates.
Pure observability — must never affect merge behavior.

Deterministic change_key = sha256(happening_id|field_name|old_value|new_value).
source_happening_id is excluded so the same logical change from any source
produces the same key → INSERT ON CONFLICT DO NOTHING guarantees idempotency.

Phase 7: log_field_changes uses DB-side RPC (insert_field_history_batch)
to safely count actual inserts without broad exception swallowing.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Mapping

from supabase import Client


# Canonical field → source_happenings field mapping
TRACKED_FIELDS: dict[str, str] = {
    "title": "title_raw",
    "description": "description_raw",
}


@dataclass
class FieldChange:
    field_name: str
    old_value: str | None
    new_value: str | None


def compute_change_key(
    happening_id: str,
    field_name: str,
    old_value: str | None,
    new_value: str | None,
) -> str:
    """
    Deterministic change key.
    source_happening_id is excluded so the same logical change
    from any source produces the same key.
    """
    seed = "|".join([
        happening_id,
        field_name,
        old_value or "",
        new_value or "",
    ])
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def diff_happening_fields(
    current: Mapping[str, Any],
    source_row: Mapping[str, Any],
) -> list[FieldChange]:
    """
    Compare tracked fields between the current happening and the source row.
    Returns changes only where the source has a non-null value AND it differs.
    """
    changes: list[FieldChange] = []
    for canonical_field, source_field in TRACKED_FIELDS.items():
        new_val = source_row.get(source_field)
        if new_val is None:
            continue
        old_val = current.get(canonical_field)
        # Normalize to str for comparison
        old_str = str(old_val) if old_val is not None else None
        new_str = str(new_val)
        if old_str != new_str:
            changes.append(FieldChange(
                field_name=canonical_field,
                old_value=old_str,
                new_value=new_str,
            ))
    return changes


def log_field_changes(
    supabase: Client,
    happening_id: str,
    source_happening_id: str,
    changes: list[FieldChange],
) -> int:
    """
    Batch-insert history rows via DB RPC (insert_field_history_batch).
    Returns count of rows actually inserted (not attempts).

    The RPC uses INSERT ... ON CONFLICT (change_key) DO NOTHING
    and GET DIAGNOSTICS ROW_COUNT to return the real insert count.
    """
    if not changes:
        return 0

    payloads = []
    for change in changes:
        change_key = compute_change_key(
            happening_id, change.field_name, change.old_value, change.new_value,
        )
        payloads.append({
            "happening_id": happening_id,
            "source_happening_id": source_happening_id,
            "field_name": change.field_name,
            "old_value": change.old_value,
            "new_value": change.new_value,
            "change_key": change_key,
        })

    resp = supabase.rpc(
        "insert_field_history_batch", {"changes": payloads},
    ).execute()

    return int(resp.data) if resp.data is not None else 0
