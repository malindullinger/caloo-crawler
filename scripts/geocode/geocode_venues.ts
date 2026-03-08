// scripts/geocode/geocode_venues.ts
//
// Phase 5D.0 — Venue geocoding via Nominatim (fallback: geo.admin.ch)
//
// Writes ONLY to venue_geocode_result staging table.
// venue.lat/lng updated separately after validation.
//
// Usage:  npx tsx scripts/geocode/geocode_venues.ts
//
// Idempotent: safe to rerun. Uses UPSERT on (venue_id, provider).
// Rate-limited: 1 request/sec for Nominatim (usage policy).

import { getSupabaseAdmin } from "../_shared/supabaseAdmin";

/* ─── constants ─── */

/** Set to 0 to process all venues. Set to e.g. 5 for debug runs. */
const DEBUG_LIMIT = 0;

const SWISS_BBOX = {
  latMin: 45.8,
  latMax: 47.8,
  lngMin: 5.9,
  lngMax: 10.5,
};

const NOMINATIM_DELAY_MS = 1100; // >1s between requests per Nominatim usage policy
const NOMINATIM_USER_AGENT = "caloo-crawler/1.0 (family-activity-platform)";

/* ─── failure counters ─── */

const failures: Record<string, number> = {
  nominatim_http_error: 0,
  nominatim_no_results: 0,
  nominatim_invalid_coords: 0,
  nominatim_outside_bbox: 0,
  geoadmin_http_error: 0,
  geoadmin_no_results: 0,
  geoadmin_invalid_coords: 0,
  geoadmin_outside_bbox: 0,
  upsert_error: 0,
};

/* ─── types ─── */

type VenueRow = {
  id: string;
  name: string;
  address_line1: string | null;
  postal_code: string | null;
  locality: string | null;
};

type GeocodeResult = {
  lat: number;
  lng: number;
  confidence: number;
  raw: any;
};

type ParsedAddress = {
  streetLine: string | null; // "Alte Landstrasse 250"
  postalCode: string | null; // "8708"
  locality: string | null; // "Männedorf"
};

/* ─── address parsing ─── */

/**
 * Parse a Swiss venue name that embeds address info.
 * Common patterns:
 *   "Familienzentrum Männedorf, Alte Landstrasse 250, 8708 Männedorf"
 *   "Bibliothek Männedorf, Schulstrasse 15, 8708 Männedorf"
 *   "EMK, Liebegasse 7, 8708 Männedorf"
 *   "Bibliothek Männedorf" (no address embedded)
 */
function parseVenueName(name: string): ParsedAddress {
  // Split on comma and trim
  const parts = name.split(",").map((p) => p.trim());

  let streetLine: string | null = null;
  let postalCode: string | null = null;
  let locality: string | null = null;

  // Look for a part matching "NNNN Locality" (Swiss postal code pattern)
  const postalPattern = /^(\d{4})\s+(.+)$/;
  for (const part of parts) {
    const m = part.match(postalPattern);
    if (m) {
      postalCode = m[1];
      locality = m[2].trim();
      break;
    }
  }

  // Look for a street line: contains a number but is NOT the postal code part
  const streetPattern = /^(.+?)\s+(\d{1,4})\s*$/;
  for (const part of parts) {
    if (part.match(postalPattern)) continue; // skip postal code part
    const m = part.match(streetPattern);
    if (m) {
      streetLine = part.replace(/\s+/g, " ").trim();
      break;
    }
  }

  // If no locality found, try extracting from the venue name part
  // e.g. "Bibliothek Männedorf" → locality might be "Männedorf"
  if (!locality && parts.length >= 1) {
    // Check last part for just a place name (no digits)
    const lastPart = parts[parts.length - 1];
    if (lastPart && !lastPart.match(/\d/) && parts.length > 1) {
      locality = lastPart;
    }
  }

  return { streetLine, postalCode, locality };
}

/**
 * Build a geocoding query string from venue data.
 * Prioritizes structured address fields, falls back to parsed venue name.
 *
 * Strategy:
 *   1. If we have a street + postal/locality → use that (most precise)
 *   2. If we have only postal + locality → use that (town-level)
 *   3. Fallback → full venue name (let the geocoder try)
 */
function buildQuery(venue: VenueRow): string {
  const parsed = parseVenueName(venue.name);

  const street = venue.address_line1 || parsed.streetLine;
  const postal = venue.postal_code || parsed.postalCode;
  const loc = venue.locality || parsed.locality;

  const parts: string[] = [];
  if (street) parts.push(street);
  if (postal && loc) {
    parts.push(`${postal} ${loc}`);
  } else if (loc) {
    parts.push(loc);
  } else if (postal) {
    parts.push(postal);
  }

  // Always append Switzerland for precision
  if (parts.length > 0) {
    parts.push("Switzerland");
    return parts.join(", ");
  }

  // Fallback: use the full venue name
  return `${venue.name}, Switzerland`;
}

/**
 * For venues where buildQuery produces only a name (no address components),
 * build a simpler query that geo.admin.ch can resolve to at least a town.
 * Returns null if we can't improve on the original query.
 */
function buildFallbackQuery(venue: VenueRow): string | null {
  const parsed = parseVenueName(venue.name);

  // If we already had address components, fallback won't help
  if (parsed.streetLine || parsed.postalCode || parsed.locality) return null;

  // Try sending just the venue name to geo.admin (it handles Swiss POI names)
  return venue.name;
}

/* ─── geocoding providers ─── */

function isInSwissBbox(lat: number, lng: number): boolean {
  return (
    lat >= SWISS_BBOX.latMin &&
    lat <= SWISS_BBOX.latMax &&
    lng >= SWISS_BBOX.lngMin &&
    lng <= SWISS_BBOX.lngMax
  );
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Geocode via Nominatim (OpenStreetMap).
 * Respects usage policy: 1 request/sec, custom User-Agent.
 */
async function geocodeNominatim(query: string): Promise<GeocodeResult | null> {
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("q", query);
  url.searchParams.set("format", "jsonv2");
  url.searchParams.set("countrycodes", "ch");
  url.searchParams.set("limit", "1");
  url.searchParams.set("addressdetails", "1");

  let res: Response;
  try {
    res = await fetch(url.toString(), {
      headers: { "User-Agent": NOMINATIM_USER_AGENT },
    });
  } catch (err: any) {
    console.warn(`  [nominatim] fetch error: ${err?.message}`);
    failures.nominatim_http_error++;
    return null;
  }

  if (!res.ok) {
    console.warn(`  [nominatim] HTTP ${res.status} ${res.statusText}`);
    failures.nominatim_http_error++;
    return null;
  }

  const data = await res.json();
  console.log(`  [nominatim] HTTP ${res.status} | results: ${Array.isArray(data) ? data.length : "N/A"}`);

  if (!Array.isArray(data) || data.length === 0) {
    failures.nominatim_no_results++;
    return null;
  }

  const top = data[0];
  console.log(`  [nominatim] top candidate: "${top.display_name}" (type: ${top.type}, importance: ${top.importance})`);

  const lat = parseFloat(top.lat);
  const lng = parseFloat(top.lon);

  if (isNaN(lat) || isNaN(lng)) {
    console.warn(`  [nominatim] invalid coords: lat=${top.lat} lon=${top.lon}`);
    failures.nominatim_invalid_coords++;
    return null;
  }

  if (!isInSwissBbox(lat, lng)) {
    console.warn(`  [nominatim] REJECTED: outside Swiss bbox (${lat}, ${lng})`);
    failures.nominatim_outside_bbox++;
    return null;
  }

  return {
    lat,
    lng,
    confidence: parseFloat(top.importance ?? "0") || 0,
    raw: top,
  };
}

/**
 * Geocode via geo.admin.ch (Swiss federal geocoder).
 * Free, no key required, no strict rate limit.
 */
async function geocodeGeoAdmin(query: string): Promise<GeocodeResult | null> {
  const url = new URL("https://api3.geo.admin.ch/rest/services/api/SearchServer");
  url.searchParams.set("searchText", query);
  url.searchParams.set("type", "locations");
  url.searchParams.set("limit", "1");
  url.searchParams.set("sr", "4326"); // WGS84

  let res: Response;
  try {
    res = await fetch(url.toString());
  } catch (err: any) {
    console.warn(`  [geo.admin] fetch error: ${err?.message}`);
    failures.geoadmin_http_error++;
    return null;
  }

  if (!res.ok) {
    console.warn(`  [geo.admin] HTTP ${res.status} ${res.statusText}`);
    failures.geoadmin_http_error++;
    return null;
  }

  const data = await res.json();
  const results = data?.results;
  console.log(`  [geo.admin] HTTP ${res.status} | results: ${Array.isArray(results) ? results.length : "N/A"}`);

  if (!Array.isArray(results) || results.length === 0) {
    failures.geoadmin_no_results++;
    return null;
  }

  const top = results[0];
  console.log(`  [geo.admin] top candidate: "${top?.attrs?.label}" (origin: ${top?.attrs?.origin}, rank: ${top?.attrs?.rank})`);

  const lat = top?.attrs?.lat;
  const lng = top?.attrs?.lon;

  if (typeof lat !== "number" || typeof lng !== "number") {
    console.warn(`  [geo.admin] invalid coords: lat=${lat} lon=${lng}`);
    failures.geoadmin_invalid_coords++;
    return null;
  }

  if (!isInSwissBbox(lat, lng)) {
    console.warn(`  [geo.admin] REJECTED: outside Swiss bbox (${lat}, ${lng})`);
    failures.geoadmin_outside_bbox++;
    return null;
  }

  return {
    lat,
    lng,
    confidence: top?.attrs?.rank ?? 0,
    raw: top,
  };
}

/* ─── main ─── */

async function main() {
  const supabase = getSupabaseAdmin();

  // 1. Load all venues
  const { data: venues, error: venueErr } = await supabase
    .from("venue")
    .select("id, name, address_line1, postal_code, locality")
    .order("name");

  if (venueErr) throw venueErr;
  const rows = (venues ?? []) as VenueRow[];
  console.log(`[geocode] Loaded ${rows.length} venues`);

  if (rows.length === 0) {
    console.log("[geocode] No venues to geocode. Done.");
    return;
  }

  // 2. Dedupe by name to avoid geocoding duplicate venue names
  // (multiple venue rows may share the same name due to pending dedup)
  const uniqueNames = new Map<string, VenueRow[]>();
  for (const v of rows) {
    const key = v.name.trim();
    if (!uniqueNames.has(key)) uniqueNames.set(key, []);
    uniqueNames.get(key)!.push(v);
  }

  const totalDistinct = uniqueNames.size;
  const limit = DEBUG_LIMIT > 0 ? Math.min(DEBUG_LIMIT, totalDistinct) : totalDistinct;
  console.log(`[geocode] ${totalDistinct} distinct venue names across ${rows.length} rows`);
  if (DEBUG_LIMIT > 0) {
    console.log(`[geocode] DEBUG_LIMIT=${DEBUG_LIMIT} — processing first ${limit} only`);
  }

  let geocoded = 0;
  let failed = 0;
  let processed = 0;

  for (const [name, venueGroup] of uniqueNames) {
    if (processed >= limit) break;
    processed++;

    const representative = venueGroup[0];
    const query = buildQuery(representative);
    const parsed = parseVenueName(representative.name);

    console.log(`\n[${processed}/${limit}] "${name}" (×${venueGroup.length} rows)`);
    console.log(`  parsed  → street: ${parsed.streetLine ?? "∅"} | postal: ${parsed.postalCode ?? "∅"} | locality: ${parsed.locality ?? "∅"}`);
    console.log(`  query   → "${query}"`);

    // Try Nominatim first (only useful when we have address components)
    await sleep(NOMINATIM_DELAY_MS);
    let result = await geocodeNominatim(query);
    let provider = "nominatim";

    // Fallback to geo.admin.ch on Nominatim failure
    if (!result) {
      console.log("  → Nominatim failed, trying geo.admin.ch...");

      // First try with the same query
      result = await geocodeGeoAdmin(query);
      provider = "geo_admin_ch";

      // If that also fails, try a simpler fallback query
      if (!result) {
        const fallbackQuery = buildFallbackQuery(representative);
        if (fallbackQuery && fallbackQuery !== query) {
          console.log(`  → geo.admin also failed, retrying with fallback query: "${fallbackQuery}"`);
          result = await geocodeGeoAdmin(fallbackQuery);
        }
      }
    }

    if (!result) {
      console.log("  ✗ FAILED: no geocode result from any provider");
      failed += venueGroup.length;
      continue;
    }

    console.log(`  ✓ [${provider}] → ${result.lat}, ${result.lng} (confidence: ${result.confidence})`);

    // 3. UPSERT into venue_geocode_result for ALL venue rows sharing this name
    for (const venue of venueGroup) {
      const { error: upsertErr } = await supabase
        .from("venue_geocode_result")
        .upsert(
          {
            venue_id: venue.id,
            provider,
            query,
            lat: result.lat,
            lng: result.lng,
            confidence: result.confidence,
            raw_response: result.raw,
            created_at: new Date().toISOString(),
          },
          { onConflict: "venue_id,provider" },
        );

      if (upsertErr) {
        console.warn(`  UPSERT error for venue ${venue.id}: ${upsertErr.message}`);
        failures.upsert_error++;
        failed++;
      } else {
        geocoded++;
      }
    }
  }

  // 4. Summary
  console.log("\n" + "=".repeat(60));
  console.log(`[geocode] DONE (${processed}/${totalDistinct} distinct names processed)`);
  console.log(`  Geocoded: ${geocoded} venue rows`);
  console.log(`  Failed:   ${failed} venue rows`);
  console.log(`  Total:    ${rows.length} venue rows`);

  // 5. Failure breakdown
  const activeFailures = Object.entries(failures).filter(([, v]) => v > 0);
  if (activeFailures.length > 0) {
    console.log("\n  Failure breakdown:");
    for (const [reason, count] of activeFailures) {
      console.log(`    ${reason}: ${count}`);
    }
  } else {
    console.log("\n  No failures recorded.");
  }

  // 6. Quick staging table stats
  const { data: stats } = await supabase
    .from("venue_geocode_result")
    .select("provider", { count: "exact" });

  console.log(`\n  Staging table rows: ${stats?.length ?? 0}`);
  console.log("\n[geocode] Next step: validate results, then run venue lat/lng promotion.");
}

main().catch((err: any) => {
  console.error("[geocode] ERROR:", err?.message ?? err);
  if (err?.code) console.error("[geocode] code:", err.code);
  if (err?.details) console.error("[geocode] details:", err.details);
  process.exit(1);
});
