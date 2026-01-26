import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Zurich")

# Fail fast if required env vars are missing
_missing = []
if not SUPABASE_URL:
    _missing.append("SUPABASE_URL")
if not SUPABASE_SERVICE_ROLE_KEY:
    _missing.append("SUPABASE_SERVICE_ROLE_KEY")
if _missing:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(_missing)}. "
        "Copy .env.example to .env and fill in your Supabase credentials."
    )
