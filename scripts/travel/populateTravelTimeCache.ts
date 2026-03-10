// scripts/travel/populateTravelTimeCache.ts
//
// Phase 5D.3b M3 — Travel-time cache population
//
// Reads active origin clusters and geocoded venues from the DB,
// queries Google Distance Matrix API for all (cluster × venue × mode)
// combinations, and upserts results into venue_travel_time.
//
// Idempotent: safe to rerun. Uses ON CONFLICT on the natural key
// (origin_cluster_id, venue_id, transport_mode) to update existing rows.
//
// Usage:
//   npx tsx scripts/travel/populateTravelTimeCache.ts [flags]
//
// Flags:
//   --dry-run          Log what would be sent; do not call Google or write DB (default: false)
//   --cluster <slug>   Process only this cluster (default: all active)
//   --mode <mode>      Process only this transport mode (default: all four)
//   --limit <n>        Limit to first N venues (for testing)
//   --batch-size <n>   Destinations per Google API request (default: 25, max: 25)
//   --delay-ms <n>     Delay between API requests in ms (default: 200)
//
// Environment:
//   GOOGLE_MAPS_API_KEY   — required (unless --dry-run)
//   SUPABASE_URL          — required
//   SUPABASE_SERVICE_ROLE_KEY — required

declare const process: {
  argv: string[];
  env: Record<string, string | undefined>;
  exit(code?: number): never;
};

import { getSupabaseAdmin } from "../_shared/supabaseAdmin.ts";

// ── Types ────────────────────────────────────────────────────────────

type OriginCluster = {
  id: string;
  name: string;
  slug: string;
  lat: number;
  lng: number;
};

type Venue = {
  id: string;
  name: string;
  lat: number;
  lng: number;
};

type TransportMode = "transit" | "driving" | "walking" | "bicycling";

const ALL_MODES: TransportMode[] = ["transit", "driving", "walking", "bicycling"];

type GoogleElement = {
  status: string; // OK, ZERO_RESULTS, NOT_FOUND, MAX_ROUTE_LENGTH_EXCEEDED, etc.
  duration?: { value: number; text: string }; // value in seconds
  distance?: { value: number; text: string };
};

type GoogleResponse = {
  status: string; // OK, INVALID_REQUEST, MAX_ELEMENTS_EXCEEDED, etc.
  origin_addresses: string[];
  destination_addresses: string[];
  rows: Array<{
    elements: GoogleElement[];
  }>;
  error_message?: string;
};

type UpsertRow = {
  origin_cluster_id: string;
  venue_id: string;
  transport_mode: TransportMode;
  travel_minutes: number | null;
  google_status: string;
  computed_at: string;
};

type FailedBatch = {
  cluster: string;
  mode: TransportMode;
  batchIdx: number;
  reason: string;
};

// ── CLI argument parsing ─────────────────────────────────────────────

type CliArgs = {
  dryRun: boolean;
  clusterSlug: string | null;
  mode: TransportMode | null;
  limit: number | null;
  batchSize: number;
  delayMs: number;
};

function parseArgs(argv: string[]): CliArgs {
  const args: CliArgs = {
    dryRun: false,
    clusterSlug: null,
    mode: null,
    limit: null,
    batchSize: 25,
    delayMs: 200,
  };

  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--dry-run") {
      args.dryRun = true;
    } else if (arg === "--cluster" && argv[i + 1]) {
      args.clusterSlug = argv[++i];
    } else if (arg === "--mode" && argv[i + 1]) {
      const m = argv[++i] as TransportMode;
      if (!ALL_MODES.includes(m)) {
        throw new Error(`Invalid mode: ${m}. Must be one of: ${ALL_MODES.join(", ")}`);
      }
      args.mode = m;
    } else if (arg === "--limit" && argv[i + 1]) {
      args.limit = parseInt(argv[++i], 10);
      if (!Number.isFinite(args.limit) || args.limit < 1) {
        throw new Error(`Invalid --limit: ${argv[i]}`);
      }
    } else if (arg === "--batch-size" && argv[i + 1]) {
      args.batchSize = parseInt(argv[++i], 10);
      if (!Number.isFinite(args.batchSize) || args.batchSize < 1 || args.batchSize > 25) {
        throw new Error(`Invalid --batch-size: ${argv[i]}. Must be 1-25.`);
      }
    } else if (arg === "--delay-ms" && argv[i + 1]) {
      args.delayMs = parseInt(argv[++i], 10);
      if (!Number.isFinite(args.delayMs) || args.delayMs < 0) {
        throw new Error(`Invalid --delay-ms: ${argv[i]}. Must be >= 0.`);
      }
    } else if (arg === "--help" || arg === "-h") {
      console.log(`
Usage: npx tsx scripts/travel/populateTravelTimeCache.ts [flags]

Flags:
  --dry-run          Log what would be sent; skip Google API and DB writes
  --cluster <slug>   Process only this cluster (e.g. "maennedorf")
  --mode <mode>      Process only this transport mode (transit|driving|walking|bicycling)
  --limit <n>        Limit to first N venues (for testing)
  --batch-size <n>   Destinations per API request (default: 25, max: 25)
  --delay-ms <n>     Delay between API requests in ms (default: 200)

Environment:
  GOOGLE_MAPS_API_KEY          Required (unless --dry-run)
  SUPABASE_URL                 Required
  SUPABASE_SERVICE_ROLE_KEY    Required
`);
      process.exit(0);
    } else {
      throw new Error(`Unknown argument: ${arg}. Use --help for usage.`);
    }
  }

  return args;
}

// ── Helpers ──────────────────────────────────────────────────────────

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function durationSecondsToMinutes(seconds: number): number {
  return Math.round(seconds / 60);
}

// ── Google Distance Matrix API ───────────────────────────────────────

const GOOGLE_API_URL = "https://maps.googleapis.com/maps/api/distancematrix/json";
const FETCH_TIMEOUT_MS = 30_000;

// Top-level Google API statuses that indicate a fatal request-shape or
// authentication error. The script must stop immediately on these.
const FATAL_GOOGLE_STATUSES = new Set([
  "REQUEST_DENIED",
  "OVER_DAILY_LIMIT",
  "OVER_QUERY_LIMIT",
  "INVALID_REQUEST",
  "MAX_ELEMENTS_EXCEEDED",
  "MAX_DIMENSIONS_EXCEEDED",
]);

async function queryDistanceMatrix(
  apiKey: string,
  origin: { lat: number; lng: number },
  destinations: Array<{ lat: number; lng: number }>,
  mode: TransportMode,
): Promise<GoogleResponse> {
  const originStr = `${origin.lat},${origin.lng}`;
  const destStr = destinations.map((d) => `${d.lat},${d.lng}`).join("|");

  const params = new URLSearchParams({
    origins: originStr,
    destinations: destStr,
    mode: mode,
    key: apiKey,
  });

  const url = `${GOOGLE_API_URL}?${params.toString()}`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, { signal: controller.signal });

    if (!res.ok) {
      throw new Error(`Google API HTTP ${res.status}: ${await res.text()}`);
    }

    return (await res.json()) as GoogleResponse;
  } finally {
    clearTimeout(timeout);
  }
}

// ── Main ─────────────────────────────────────────────────────────────

async function main() {
  const args = parseArgs(process.argv);
  const supabase = getSupabaseAdmin();

  const apiKey = process.env.GOOGLE_MAPS_API_KEY ?? "";
  if (!apiKey && !args.dryRun) {
    throw new Error(
      "GOOGLE_MAPS_API_KEY is required. Set it in .env or environment.\n" +
        "Use --dry-run to test without an API key.",
    );
  }

  console.log("=== Travel-time cache population (M3) ===");
  console.log(`  dry-run:    ${args.dryRun}`);
  console.log(`  cluster:    ${args.clusterSlug ?? "all active"}`);
  console.log(`  mode:       ${args.mode ?? "all four"}`);
  console.log(`  limit:      ${args.limit ?? "none"}`);
  console.log(`  batch-size: ${args.batchSize}`);
  console.log(`  delay-ms:   ${args.delayMs}`);
  console.log("");

  // ── 1. Load clusters ──────────────────────────────────────────────

  let clusterQuery = supabase
    .from("origin_cluster")
    .select("id, name, slug, lat, lng")
    .eq("is_active", true)
    .order("slug");

  if (args.clusterSlug) {
    clusterQuery = clusterQuery.eq("slug", args.clusterSlug);
  }

  const { data: clusters, error: clusterErr } = await clusterQuery;
  if (clusterErr) throw clusterErr;

  const clusterRows = (clusters ?? []) as OriginCluster[];
  if (clusterRows.length === 0) {
    console.log("No active clusters found. Nothing to do.");
    return;
  }
  console.log(`Clusters: ${clusterRows.length} active`);
  for (const c of clusterRows) {
    console.log(`  - ${c.slug} (${c.name}) @ ${c.lat}, ${c.lng}`);
  }

    // ── 2. Load venues with coordinates ───────────────────────────────
  // venue_travel_cache_candidates_v1 derives lat/lng from the canonical
  // geo_point column via ST_X/ST_Y, since PostgREST cannot evaluate
  // SQL expressions in .select().

  let venueQuery = supabase
    .from("venue_travel_cache_candidates_v1")
    .select("id, name, lat, lng")
    .order("name");

  if (args.limit) {
    venueQuery = venueQuery.limit(args.limit);
  }

  const { data: venues, error: venueErr } = await venueQuery;
  if (venueErr) throw venueErr;

  const venueRows: Venue[] = (venues ?? []) as unknown as Venue[];
  if (venueRows.length === 0) {
    console.log("No geocoded venues found. Nothing to do.");
    return;
  }
  console.log(`Venues:   ${venueRows.length} with coordinates`);
  console.log("");

  // ── 3. Determine modes to process ─────────────────────────────────

  const modes: TransportMode[] = args.mode ? [args.mode] : ALL_MODES;

  // ── 4. Process: cluster × mode × venue batches ────────────────────

  // Planning counters (always incremented, dry-run and real)
  let plannedApiCalls = 0;
  let plannedElements = 0;

  // Execution counters (only incremented during real runs)
  let actualApiCalls = 0;
  let actualElements = 0;
  let totalUpserted = 0;
  const statusCounts: Record<string, number> = {};
  const modeStats: Record<string, { ok: number; nonOk: number }> = {};
  const failedBatches: FailedBatch[] = [];

  for (const mode of modes) {
    modeStats[mode] = { ok: 0, nonOk: 0 };
  }

  for (const cluster of clusterRows) {
    for (const mode of modes) {
      const venueBatches = chunk(venueRows, args.batchSize);

      console.log(
        `Processing: ${cluster.slug} × ${mode} (${venueRows.length} venues, ${venueBatches.length} batches)`,
      );

      for (let batchIdx = 0; batchIdx < venueBatches.length; batchIdx++) {
        const batch = venueBatches[batchIdx];
        plannedApiCalls += 1;
        plannedElements += batch.length;

        if (args.dryRun) {
          console.log(
            `  [DRY RUN] batch ${batchIdx + 1}/${venueBatches.length}: ` +
              `${batch.length} venues (${batch[0].name.substring(0, 30)}…)`,
          );
          continue;
        }

        // Call Google Distance Matrix API
        let response: GoogleResponse;
        try {
          response = await queryDistanceMatrix(
            apiKey,
            { lat: cluster.lat, lng: cluster.lng },
            batch.map((v) => ({ lat: v.lat, lng: v.lng })),
            mode,
          );
        } catch (err: any) {
          const reason =
            err?.name === "AbortError"
              ? `fetch timeout (${FETCH_TIMEOUT_MS}ms)`
              : (err?.message ?? String(err));
          console.error(`  FAILED batch ${batchIdx + 1}: ${reason}`);
          failedBatches.push({
            cluster: cluster.slug,
            mode,
            batchIdx: batchIdx + 1,
            reason,
          });
          continue;
        }

        actualApiCalls += 1;

        // Validate top-level response
        if (response.status !== "OK") {
          const msg = `${response.status} — ${response.error_message ?? "no message"}`;
          console.error(`  Google API error: ${msg}`);

          if (FATAL_GOOGLE_STATUSES.has(response.status)) {
            throw new Error(
              `Google API fatal: ${response.status}: ${response.error_message ?? "check API key, billing, and request shape"}`,
            );
          }

          failedBatches.push({
            cluster: cluster.slug,
            mode,
            batchIdx: batchIdx + 1,
            reason: `top-level status: ${msg}`,
          });
          continue;
        }

        // Parse elements (1 origin → N destinations)
        const elements = response.rows[0]?.elements ?? [];
        if (elements.length !== batch.length) {
          const reason = `element count mismatch: expected ${batch.length}, got ${elements.length}`;
          console.error(`  FAILED batch ${batchIdx + 1}: ${reason}`);
          failedBatches.push({
            cluster: cluster.slug,
            mode,
            batchIdx: batchIdx + 1,
            reason,
          });
          continue;
        }

        const now = new Date().toISOString();
        const upsertRows: UpsertRow[] = [];

        for (let i = 0; i < elements.length; i++) {
          const el = elements[i];
          const venue = batch[i];

          actualElements += 1;
          statusCounts[el.status] = (statusCounts[el.status] ?? 0) + 1;

          const isOk = el.status === "OK";
          const travelMinutes =
            isOk && el.duration ? durationSecondsToMinutes(el.duration.value) : null;

          if (isOk) {
            modeStats[mode].ok += 1;
          } else {
            modeStats[mode].nonOk += 1;
          }

          upsertRows.push({
            origin_cluster_id: cluster.id,
            venue_id: venue.id,
            transport_mode: mode,
            travel_minutes: travelMinutes,
            google_status: el.status,
            computed_at: now,
          });
        }

        // Upsert batch into venue_travel_time
        const { error: upsertErr } = await supabase
          .from("venue_travel_time")
          .upsert(upsertRows, {
            onConflict: "origin_cluster_id,venue_id,transport_mode",
          });

        if (upsertErr) {
          const reason = `DB upsert: ${upsertErr.message}`;
          console.error(`  FAILED batch ${batchIdx + 1}: ${reason}`);
          if (upsertErr.code) console.error(`    code: ${upsertErr.code}`);
          if (upsertErr.details) console.error(`    details: ${upsertErr.details}`);
          failedBatches.push({
            cluster: cluster.slug,
            mode,
            batchIdx: batchIdx + 1,
            reason,
          });
        } else {
          totalUpserted += upsertRows.length;
          console.log(
            `  batch ${batchIdx + 1}/${venueBatches.length}: ` +
              `${upsertRows.length} rows upserted`,
          );
        }

        // Rate-limit delay between batches
        if (batchIdx < venueBatches.length - 1 && args.delayMs > 0) {
          await sleep(args.delayMs);
        }
      }
    }
  }

  // ── 5. Summary ────────────────────────────────────────────────────

  console.log("");
  console.log("=== Summary ===");
  console.log(`  Clusters processed:    ${clusterRows.length}`);
  console.log(`  Venues processed:      ${venueRows.length}`);
  console.log(`  Transport modes:       ${modes.join(", ")}`);
  console.log("");

  if (args.dryRun) {
    console.log(`  [DRY RUN] Planned API calls:   ${plannedApiCalls}`);
    console.log(`  [DRY RUN] Planned elements:    ${plannedElements}`);
    console.log("");
    console.log("[DRY RUN] No API calls made, no DB writes performed.");
  } else {
    console.log(`  API calls made:        ${actualApiCalls}`);
    console.log(`  Elements processed:    ${actualElements}`);
    console.log(`  Rows upserted:         ${totalUpserted}`);
    console.log(`  Batches failed:        ${failedBatches.length}`);
    console.log("");

    if (Object.keys(statusCounts).length > 0) {
      console.log("  Google status breakdown:");
      for (const [status, count] of Object.entries(statusCounts).sort()) {
        console.log(`    ${status}: ${count}`);
      }
      console.log("");
    }

    if (Object.keys(modeStats).length > 0) {
      console.log("  Per-mode OK vs non-OK:");
      for (const [mode, stats] of Object.entries(modeStats)) {
        if (stats.ok + stats.nonOk > 0) {
          console.log(
            `    ${mode.padEnd(10)} OK: ${stats.ok}  non-OK: ${stats.nonOk}`,
          );
        }
      }
      console.log("");
    }

    if (failedBatches.length > 0) {
      console.log("  Failed batches:");
      for (const fb of failedBatches) {
        console.log(`    ${fb.cluster} × ${fb.mode} batch ${fb.batchIdx}: ${fb.reason}`);
      }
      console.log("");
    }

    console.log("Done. Run validation queries to verify cache state.");
  }

  if (failedBatches.length > 0 && !args.dryRun) {
    process.exit(2);
  }
}

main().catch((err: any) => {
  console.error("[travel-cache] ERROR:", err?.message ?? err);
  if (err?.code) console.error("[travel-cache] code:", err.code);
  if (err?.details) console.error("[travel-cache] details:", err.details);
  process.exit(1);
});
