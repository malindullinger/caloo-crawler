from datetime import datetime, timezone
from supabase import create_client

from .config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY


def main():
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    row = {
        "source_id": "test",
        "source_url": "https://example.com",
        "item_url": None,
        "content_hash": "test-hash",
        "raw_payload": {"hello": "world"},
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "status": "ok",
        "error": None,
    }

    res = client.table("event_raw").insert(row).execute()
    print("✅ insert ok:", res.data[0]["id"] if res.data else res)


if __name__ == "__main__":
    main()
