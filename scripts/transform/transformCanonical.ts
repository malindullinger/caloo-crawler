// scripts/transform/transformCanonical.ts
import { getSupabaseAdmin } from "../_shared/supabaseAdmin";

type HappeningKeyInput = {
  country: string; // "ch"
  region: string; // "zh"
  municipality: string; // "maennedorf"
  conceptKind: "series" | "one_off";
  title: string;
  locationName?: string | null;
};

function buildHappeningKey(input: HappeningKeyInput): string {
  const { country, region, municipality, conceptKind, title, locationName } = input;

  const titleKey = canonicalKeyPart(title);
  const locationKey = locationName ? canonicalKeyPart(locationName) : null;

  const geo = `${country}-${region}-${municipality}`;

  return ["happening_key:v1", geo, conceptKind, titleKey, locationKey].filter(Boolean).join("||");
}

// ---- types (local to this script) ----
type SourceSystemRow = {
  id: string;
  key: string;
};

type MappingRow = {
  entity_id: string;
};

// This matches the VIEW we created: source_document_latest
type SourceDocLatestRow = {
  id: string;
  source_system_id: string;
  source_url: string | null;
  source_entity_key: string | null;
  content_type: string;
  payload: any;
  observed_at: string;
  content_hash: string;
  created_at: string;
};

type ParsedTemporal =
  | {
      kind: "range";
      startDate: string;
      endDate: string;
      startTime?: string | null; // "HH:MM"
      endTime?: string | null; // "HH:MM"
      raw?: string;
    }
  | {
      kind: "single";
      startDate: string;
      startTime?: string | null; // "HH:MM"
      endTime?: string | null; // "HH:MM"
      raw?: string;
    }
  | { kind: "unknown"; raw?: string };

function normalizeText(v: unknown): string {
  return (typeof v === "string" ? v : "").trim();
}

// Used for building stable mapping keys (title/location)
function canonicalKeyPart(v: unknown): string {
  const s = normalizeText(v).toLowerCase();
  return s
    .normalize("NFKD")
    .replace(/[^\p{L}\p{N} ]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

function toISODateFromSwiss(ddmmyyyy: string): string | null {
  const m = ddmmyyyy.trim().match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!m) return null;
  const [, dd, mm, yyyy] = m;
  return `${yyyy}-${mm}-${dd}`;
}

const MONTHS_DE: Record<string, number> = {
  jan: 1,
  januar: 1,
  feb: 2,
  februar: 2,
  mär: 3,
  maer: 3,
  märz: 3,
  maerz: 3,
  apr: 4,
  april: 4,
  mai: 5,
  jun: 6,
  juni: 6,
  jul: 7,
  juli: 7,
  aug: 8,
  august: 8,
  sep: 9,
  sept: 9,
  september: 9,
  okt: 10,
  oktober: 10,
  nov: 11,
  november: 11,
  dez: 12,
  dezember: 12,
};

function toISODateFromGerman(dayStr: string, monthStr: string, yearStr: string): string | null {
  const day = parseInt(dayStr, 10);
  const year = parseInt(yearStr, 10);
  const key = monthStr.trim().toLowerCase().replace(".", "");
  const month = MONTHS_DE[key];
  if (!day || !month || !year) return null;
  return `${year}-${pad2(month)}-${pad2(day)}`;
}

function normalizeTimeDE(t: string): string | null {
  const m = t.trim().match(/^(\d{1,2})\.(\d{2})$/);
  if (!m) return null;
  const hh = parseInt(m[1], 10);
  const mm = parseInt(m[2], 10);
  if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
  return `${pad2(hh)}:${pad2(mm)}`;
}

/**
 * Handles formats:
 * - "06.01.2026 - 10.02.2026"
 * - "31.01.2026"
 * - "8. März 2026, 9.30 Uhr - 11.30 Uhr"
 * - "28. Feb. 2026, 10.30 Uhr"
 * - "10. März 2026 - 14. Apr. 2026, 16.00 Uhr - 16.45 Uhr, 45 Minuten"
 */
function parseDatetimeRaw(datetimeRaw: string): ParsedTemporal {
  const raw = datetimeRaw.trim();

  // A) Swiss numeric range
  {
    const r = raw.match(/^(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})$/);
    if (r) {
      const start = toISODateFromSwiss(r[1]);
      const end = toISODateFromSwiss(r[2]);
      if (start && end) return { kind: "range", startDate: start, endDate: end, raw };
    }
  }

  // B) Swiss numeric single
  {
    const s = toISODateFromSwiss(raw);
    if (s) return { kind: "single", startDate: s, raw };
  }

  // C) German single date with time range
  {
    const m = raw.match(
      /^(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß\.]+)\s*(\d{4}),\s*(\d{1,2}\.\d{2})\s*Uhr\s*-\s*(\d{1,2}\.\d{2})\s*Uhr/i,
    );
    if (m) {
      const date = toISODateFromGerman(m[1], m[2], m[3]);
      const startTime = normalizeTimeDE(m[4]);
      const endTime = normalizeTimeDE(m[5]);
      if (date) return { kind: "single", startDate: date, startTime, endTime, raw };
    }
  }

  // D) German single date with single time
  {
    const m = raw.match(/^(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß\.]+)\s*(\d{4}),\s*(\d{1,2}\.\d{2})\s*Uhr/i);
    if (m) {
      const date = toISODateFromGerman(m[1], m[2], m[3]);
      const startTime = normalizeTimeDE(m[4]);
      if (date) return { kind: "single", startDate: date, startTime, raw };
    }
  }

  // E) German date range + time range
  {
    const m = raw.match(
      /^(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß\.]+)\s*(\d{4})\s*-\s*(\d{1,2})\.\s*([A-Za-zÄÖÜäöüß\.]+)\s*(\d{4}),\s*(\d{1,2}\.\d{2})\s*Uhr\s*-\s*(\d{1,2}\.\d{2})\s*Uhr/i,
    );
    if (m) {
      const startDate = toISODateFromGerman(m[1], m[2], m[3]);
      const endDate = toISODateFromGerman(m[4], m[5], m[6]);
      const startTime = normalizeTimeDE(m[7]);
      const endTime = normalizeTimeDE(m[8]);
      if (startDate && endDate) return { kind: "range", startDate, endDate, startTime, endTime, raw };
    }
  }

  return { kind: "unknown", raw };
}

// NOTE: Simplification (assumes +01:00). Good enough for winter datasets.
function buildStartAt(dateISO: string, timeHHMM?: string | null): string {
  const t = timeHHMM && timeHHMM.trim() ? `${timeHHMM}:00` : "00:00:00";
  return `${dateISO}T${t}+01:00`;
}

function buildEndAtSameDay(dateISO: string, timeHHMM?: string | null): string | null {
  if (!timeHHMM || !timeHHMM.trim()) return null;
  return `${dateISO}T${timeHHMM}:00+01:00`;
}

// ---- municipality resolver (extend as you add more sources) ----
function resolveMunicipalityFromSourceKey(sourceKey: string | null): string {
  const s = normalizeText(sourceKey).toLowerCase();
  if (s === "maennedorf_portal" || s === "maennedorf_anlaesse") return "maennedorf";
  return "unknown";
}

// Universal invariant for offerings (to match offering_unique_v1):
// - if startDate is present, endDate is ALWAYS present
// - series: endDate = parsed endDate ?? startDate
// - one_off: endDate = startDate
function normalizeOfferingDates(params: {
  offeringType: "series" | "one_off";
  startDate: string | null;
  endDate: string | null;
}): { startDate: string; endDate: string } {
  const { offeringType, startDate } = params;
  let { endDate } = params;

  if (!startDate) throw new Error("SKIP_OFFERING_NO_START_DATE");

  if (offeringType === "one_off") {
    endDate = startDate;
  } else {
    endDate = endDate ?? startDate;
  }

  return { startDate, endDate };
}

/**
 * Your source_document.payload exists in 2 shapes:
 * A) flat raw payload: { title_raw, datetime_raw, location_raw, extra, item_url, ... }
 * B) wrapped snapshot: { row: { raw_payload: { ... }, ... }, kind, source_table, ... }
 * This normalizes to the raw payload shape (A).
 */
function extractRawEventPayload(payload: any): any {
  if (!payload || typeof payload !== "object") return {};
  if (payload?.row?.raw_payload && typeof payload.row.raw_payload === "object") return payload.row.raw_payload;
  if (payload?.raw_payload && typeof payload.raw_payload === "object") return payload.raw_payload;
  return payload;
}

// Heuristic: treat listing/test docs as non-items
function isNonItemDoc(params: {
  itemUrl: string;
  sourceKey: string | null;
  title: string;
  datetimeRaw: string;
}): boolean {
  const { itemUrl, sourceKey, title, datetimeRaw } = params;

  if (itemUrl.includes("example.com")) return true;
  if (sourceKey === "test") return true;

  const looksLikeListing =
    itemUrl.includes("/anlaesseaktuelles") &&
    (itemUrl.endsWith("/anlaesseaktuelles") || itemUrl.includes("anlaesseaktuelles?"));

  if (looksLikeListing && (!title || !datetimeRaw)) return true;
  if (!title && !datetimeRaw) return true;

  return false;
}

const TIME_MISSING_NOTE = "TEvent time missing";

async function main() {
  const supabase = getSupabaseAdmin();

  const { data: sourceSystems, error: ssErr } = await supabase.from("source_system").select("id, key");
  if (ssErr) throw ssErr;

  const sourceSystemKeyById = new Map<string, string>();
  (sourceSystems ?? []).forEach((s: SourceSystemRow) => {
    sourceSystemKeyById.set(s.id, s.key);
  });

  function resolveSourceSystemKey(sourceSystemId: string | null): string | null {
    if (!sourceSystemId) return null;
    return sourceSystemKeyById.get(sourceSystemId) ?? null;
  }

  const { data: latestDocs, error: docErr } = await supabase
    .from("source_document_latest")
    .select("id, source_system_id, source_url, source_entity_key, content_type, payload, observed_at, content_hash, created_at")
    .limit(5000);

  if (docErr) throw docErr;

  const rows = (latestDocs ?? []) as SourceDocLatestRow[];
  console.log(`[transform:canonical] Loaded source_document_latest rows: ${rows.length}`);

  // ------------------------------------------------------------
  // PASS 1: detect recurring concepts by conceptKey -> distinct startDates
  // ------------------------------------------------------------
  const conceptDates = new Map<string, Set<string>>();

  for (const r of rows) {
    const sourceKey = resolveSourceSystemKey(r.source_system_id);
    const raw = extractRawEventPayload(r.payload);

    const itemUrl = normalizeText(raw.item_url) || normalizeText(r.source_entity_key) || normalizeText(r.source_url);
    const title = normalizeText(raw.title_raw);
    const locationName = normalizeText(raw.location_raw);
    const datetimeRaw = normalizeText(raw.datetime_raw);

    if (isNonItemDoc({ itemUrl, sourceKey, title, datetimeRaw })) continue;
    if (!itemUrl || !title) continue;
    if (title.toLowerCase() === "kopfzeile") continue;

    const temporal = datetimeRaw ? parseDatetimeRaw(datetimeRaw) : ({ kind: "unknown" } as const);
    const startDate =
      temporal.kind === "range" ? temporal.startDate : temporal.kind === "single" ? temporal.startDate : null;

    if (!startDate) continue;

    const municipality = resolveMunicipalityFromSourceKey(sourceKey);

    const conceptKey = buildHappeningKey({
      country: "ch",
      region: "zh",
      municipality,
      conceptKind: "series",
      title,
      locationName,
    });

    if (!conceptDates.has(conceptKey)) conceptDates.set(conceptKey, new Set<string>());
    conceptDates.get(conceptKey)!.add(startDate);
  }

  function isRecurringConcept(conceptKey: string): boolean {
    const s = conceptDates.get(conceptKey);
    return !!s && s.size > 1;
  }

  // ------------------------------------------------------------
  // PASS 2: canonicalize
  // ------------------------------------------------------------
  let insertedH = 0;
  let insertedMap = 0;
  let upsertedOffering = 0;
  let upsertedOcc = 0;

  let skipped = 0;
  let skippedDuplicate = 0;
  let missingSourceSystem = 0;
  let skippedKopfzeile = 0;
  let skippedNoStartDate = 0;
  let skippedNonItem = 0;
  let occurrencesWithMidnightFallback = 0;

  const seen = new Set<string>();

  for (const r of rows) {
    const sourceKey = resolveSourceSystemKey(r.source_system_id);
    const raw = extractRawEventPayload(r.payload);

    if (raw && typeof raw === "object" && (raw.hello || raw.test)) {
      skipped += 1;
      continue;
    }

    const itemUrl = normalizeText(raw.item_url) || normalizeText(r.source_entity_key) || normalizeText(r.source_url);
    const title = normalizeText(raw.title_raw);
    const description = normalizeText(raw.description_raw);
    const locationName = normalizeText(raw.location_raw);
    const organizerName = normalizeText(raw?.extra?.organizer);
    const datetimeRaw = normalizeText(raw.datetime_raw);

    if (isNonItemDoc({ itemUrl, sourceKey, title, datetimeRaw })) {
      skipped += 1;
      skippedNonItem += 1;
      continue;
    }

    if (!itemUrl || !title) {
      skipped += 1;
      continue;
    }

    if (title.toLowerCase() === "kopfzeile") {
      skipped += 1;
      skippedKopfzeile += 1;
      continue;
    }

    if (!sourceKey) {
      missingSourceSystem += 1;
      skipped += 1;
      continue;
    }

    const temporal = datetimeRaw ? parseDatetimeRaw(datetimeRaw) : ({ kind: "unknown" } as const);

    const startDate =
      temporal.kind === "range" ? temporal.startDate : temporal.kind === "single" ? temporal.startDate : null;
    const parsedEndDate = temporal.kind === "range" ? temporal.endDate : null;

    const startTime = temporal.kind === "range" || temporal.kind === "single" ? temporal.startTime ?? "" : "";
    const endTime = temporal.kind === "range" || temporal.kind === "single" ? temporal.endTime ?? "" : "";

    const temporalRaw = temporal.kind === "unknown" ? datetimeRaw : temporal.raw || datetimeRaw;

    const parsedKey = `${r.source_system_id}||${itemUrl}||${startDate ?? ""}||${startTime}||${parsedEndDate ?? ""}||${endTime}`;
    if (seen.has(parsedKey)) {
      skipped += 1;
      skippedDuplicate += 1;
      continue;
    }
    seen.add(parsedKey);

    const municipality = resolveMunicipalityFromSourceKey(sourceKey);

    const conceptKey = buildHappeningKey({
      country: "ch",
      region: "zh",
      municipality,
      conceptKind: "series",
      title,
      locationName,
    });

    const offeringType: "series" | "one_off" =
      temporal.kind === "range" || isRecurringConcept(conceptKey) ? "series" : "one_off";

    // Normalize offering dates to match offering_unique_v1
    let startDateNorm: string;
    let endDateNorm: string;
    try {
      const norm = normalizeOfferingDates({
        offeringType,
        startDate,
        endDate: parsedEndDate,
      });
      startDateNorm = norm.startDate;
      endDateNorm = norm.endDate;
    } catch (e: any) {
      if (String(e?.message ?? "") === "SKIP_OFFERING_NO_START_DATE") {
        skipped += 1;
        skippedNoStartDate += 1;
        continue;
      }
      throw e;
    }

    const sourceEntityId = buildHappeningKey({
      country: "ch",
      region: "zh",
      municipality,
      conceptKind: offeringType,
      title,
      locationName,
    });

    const { data: existingMap, error: mapReadErr } = await supabase
      .from("source_mapping")
      .select("entity_id")
      .eq("source_system_id", r.source_system_id)
      .eq("entity_type", "happening")
      .eq("source_entity_id", sourceEntityId)
      .maybeSingle();

    if (mapReadErr) throw mapReadErr;

    let happeningId: string | null = (existingMap as MappingRow | null)?.entity_id ?? null;

    if (!happeningId) {
      let organizerId: string | null = null;
      if (organizerName) {
        try {
          const { data: org, error: orgErr } = await supabase
            .from("organizer")
            .insert({ name: organizerName })
            .select("id")
            .single();
          if (!orgErr && org?.id) organizerId = org.id;
        } catch {
          // ignore
        }
      }

      let venueId: string | null = null;
      if (locationName) {
        try {
          const { data: v, error: vErr } = await supabase.from("venue").insert({ name: locationName }).select("id").single();
          if (!vErr && v?.id) venueId = v.id;
        } catch {
          // ignore
        }
      }

      const { data: h, error: hErr } = await supabase
        .from("happening")
        .insert({
          title,
          description: description || null,
          primary_venue_id: venueId,
          organizer_id: organizerId,
          visibility_status: "published",
          happening_kind: "event",
        })
        .select("id")
        .single();

      if (hErr) throw hErr;

      happeningId = h.id;
      insertedH += 1;

      const { error: mapInsErr } = await supabase.from("source_mapping").insert({
        source_system_id: r.source_system_id,
        source_entity_id: sourceEntityId,
        entity_type: "happening",
        entity_id: happeningId,
        confidence: "high",
      });

      if (mapInsErr) throw mapInsErr;
      insertedMap += 1;
    }

    if (!happeningId) {
      skipped += 1;
      continue;
    }

    const { data: off, error: offErr } = await supabase
      .from("offering")
      .upsert(
        {
          happening_id: happeningId,
          offering_type: offeringType,
          timezone: "Europe/Zurich",
          start_date: startDateNorm,
          end_date: endDateNorm,
        },
        { onConflict: "happening_id,offering_type,timezone,start_date,end_date" },
      )
      .select("id")
      .single();

    if (offErr) throw offErr;
    upsertedOffering += 1;

    // ✅ Occurrence: ALWAYS create if we have a start date.
    // If no time was parsed, we fall back to 00:00 and mark notes.
    const hasParsedTime = Boolean(startTime && startTime.trim());
    const startAt = buildStartAt(startDateNorm, hasParsedTime ? startTime : null);
    const endAtVal = hasParsedTime ? buildEndAtSameDay(startDateNorm, endTime || null) : null;

    let notes = temporalRaw || null;
    if (!hasParsedTime) {
      occurrencesWithMidnightFallback += 1;
      const base = notes || datetimeRaw || "";
      notes = base ? `${TIME_MISSING_NOTE} | ${base}` : TIME_MISSING_NOTE;
    }

    const { error: occErr } = await supabase.from("occurrence").upsert(
      {
        offering_id: off.id,
        start_at: startAt,
        end_at: endAtVal,
        status: "scheduled",
        notes,
      },
      { onConflict: "offering_id,start_at" },
    );

    if (occErr) throw occErr;
    upsertedOcc += 1;
  }

  console.log(`[transform:canonical] DONE`, {
    insertedH,
    insertedMap,
    upsertedOffering,
    upsertedOcc,
    occurrencesWithMidnightFallback,
    skipped,
    skippedNonItem,
    skippedDuplicate,
    missingSourceSystem,
    skippedKopfzeile,
    skippedNoStartDate,
  });

  if (missingSourceSystem > 0) {
    console.log(
      `[transform:canonical] NOTE: ${missingSourceSystem} rows were skipped because source_document_latest.source_system_id has no matching source_system row.`,
    );
  }

  if (occurrencesWithMidnightFallback > 0) {
    console.log(
      `[transform:canonical] NOTE: occurrencesWithMidnightFallback=${occurrencesWithMidnightFallback}. These occurrences use 00:00 because event time is missing. This will be improved when the crawler/time parser matures.`,
    );
  }
}

main().catch((err: any) => {
  console.error("[transform:canonical] ERROR:", err?.message ?? err);
  if (err?.code) console.error("[transform:canonical] code:", err.code);
  if (err?.details) console.error("[transform:canonical] details:", err.details);
  process.exit(1);
});
