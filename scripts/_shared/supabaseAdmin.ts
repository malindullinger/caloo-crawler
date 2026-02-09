import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function assertServiceKeyLooksRight(key: string) {
  const k = key.trim();

  // Supabase publishable/anon keys often start like this now:
  if (k.startsWith("sb_publishable_")) {
    throw new Error(
      "SUPABASE_SERVICE_ROLE_KEY looks like a PUBLISHABLE/ANON key (sb_publishable_*). " +
        "You must paste the secret service role key (Project Settings → API → service_role).",
    );
  }

  // Old anon keys are JWTs and start with eyJ...
  if (k.startsWith("eyJ")) {
    throw new Error(
      "SUPABASE_SERVICE_ROLE_KEY looks like a JWT/ANON key (starts with eyJ...). " +
        "You must paste the secret service role key (Project Settings → API → service_role).",
    );
  }
}

export function getSupabaseAdmin() {
  const supabaseUrl = requireEnv("SUPABASE_URL");
  const serviceRoleKey = requireEnv("SUPABASE_SERVICE_ROLE_KEY");

  assertServiceKeyLooksRight(serviceRoleKey);

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: { persistSession: false },
  });
}
