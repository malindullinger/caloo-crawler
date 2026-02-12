from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass(frozen=True)
class DbSourceRow:
    source_id: str
    adapter: str
    seed_url: str
    max_items: int
    tier: int
    is_enabled: bool
    notes: Optional[str] = None
    timezone: Optional[str] = None


class DbSourcesLoader:
    """
    DB-first source loader for the crawler.

    Reads from public.sources using service role (bypasses RLS).
    Matches actual schema:
      - source_id (text)
      - adapter (text)
      - seed_url (text)
      - timezone (text)
      - tier (text)
      - max_items (int)
      - is_enabled (bool)
      - notes (text)
    """

    def __init__(self, supabase_url: str, supabase_key: str) -> None:
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key

    @classmethod
    def from_env(cls) -> "DbSourcesLoader":
        url = os.getenv("SUPABASE_URL", "").strip()
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
        if not url:
            raise RuntimeError("Missing SUPABASE_URL")
        if not key:
            raise RuntimeError("Missing SUPABASE_SERVICE_ROLE_KEY")
        return cls(url, key)

    def load_enabled_sources(self) -> List[DbSourceRow]:
        try:
            from supabase import create_client  # type: ignore
        except Exception as e:
            raise RuntimeError("supabase-py not installed in this environment") from e

        client = create_client(self.supabase_url, self.supabase_key)

        resp = (
            client.table("sources")
            .select(
                "source_id,adapter,seed_url,timezone,tier,max_items,is_enabled,notes,created_at"
            )
            .eq("is_enabled", True)
            # stable-ish ordering: tier desc (after coercion), created_at asc
            .order("tier", desc=True)
            .order("created_at", desc=False)
            .execute()
        )

        data: Any = getattr(resp, "data", None)
        if not data:
            return []

        rows: List[DbSourceRow] = []
        for r in data:
            source_id = str(r.get("source_id", "")).strip()
            adapter = str(r.get("adapter", "")).strip()
            seed_url = str(r.get("seed_url", "")).strip()

            # tier is TEXT in your schema -> coerce safely
            tier_raw = r.get("tier", None)
            try:
                tier_int = int(str(tier_raw).strip()) if tier_raw is not None else 0
            except Exception:
                tier_int = 0

            max_items_raw = r.get("max_items", 100)
            try:
                max_items_int = int(max_items_raw) if max_items_raw is not None else 100
            except Exception:
                max_items_int = 100

            rows.append(
                DbSourceRow(
                    source_id=source_id,
                    adapter=adapter,
                    seed_url=seed_url,
                    max_items=max_items_int,
                    tier=tier_int,
                    is_enabled=bool(r.get("is_enabled", True)),
                    notes=r.get("notes"),
                    timezone=r.get("timezone"),
                )
            )

        # Defensive: never crash pipeline because of malformed rows
        rows = [x for x in rows if x.source_id and x.adapter and x.seed_url]
        return rows
