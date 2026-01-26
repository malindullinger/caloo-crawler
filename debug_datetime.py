from datetime import timezone

from src.normalize import parse_datetime_or_range
from src.storage import insert_schedules

# ✅ 1) Paste a SINGLE-event datetime string here:
DATETIME_RAW = "Sa, 24.01.2026, 15:00"  # <-- replace with your real one

# ✅ 2) If you know the event type, set it here:
EVENT_TYPE = "single"  # "single" or "date_range"

# ✅ 3) Set the event timezone (should match NormalizedEvent.timezone)
EVENT_TZ = "Europe/Zurich"

# ✅ 4) External id for testing
# ⚠️ Must already exist in public.events or FK will fail
EVENT_EXTERNAL_ID = "3f16dd8f55996db3d31a9aa587200e3a798a62497bde0f66e51edaef61cc7272"


def main():
    print("RAW:", DATETIME_RAW)

    # ⏱ Parse using the real function
    result = parse_datetime_or_range(DATETIME_RAW)
    print("parse_datetime_or_range() ->", result)

    if not result:
        print("❌ Parser returned nothing.")
        return

    # Expected shape: (start_dt, end_dt, event_type?) — we only care about start
    start_dt = result[0]

    if not start_dt:
        print("❌ Could not extract start datetime → no session possible.")
        return

    print("LOCAL ->", start_dt)

    dt_utc = start_dt.astimezone(timezone.utc)
    print("UTC   ->", dt_utc)

    print("\n--- Would insert schedule row with:")
    print("event_type:", EVENT_TYPE)
    print("event_tz:", EVENT_TZ)
    print("event_start_at_utc:", dt_utc)

    print("\n--- Calling insert_schedules() (⚠️ writes to Supabase!)")
    insert_schedules(
        event_external_id=EVENT_EXTERNAL_ID,
        raw_datetime=DATETIME_RAW,
        event_type=EVENT_TYPE,
        event_start_at_utc=dt_utc,
        event_tz=EVENT_TZ,
    )

    print("✅ insert_schedules() called.")
    print("→ Check Supabase: public.event_schedules for", EVENT_EXTERNAL_ID)


if __name__ == "__main__":
    main()
