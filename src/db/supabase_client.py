from __future__ import annotations

import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()  # loads .env and .env.local if present

def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)
