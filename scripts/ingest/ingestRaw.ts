// scripts/ingest/ingestRaw.ts
import { getSupabaseAdmin } from "../_shared/supabaseAdmin";
import crypto from "crypto";

type EventRawRow = {
  id: string;
  source_id: string | null; // ex: "maennedorf_portal"
  source_url: string | null;
  item_url: string | null;
  raw_payload: any;
  fetched_at: string | null; // timestamptz-ish string
  status: string | null; // "ok"
};

function normalizeText(v: unknown): string {
  return (typeof v === "string" ? v : "").trim();
}

function stableJsonStringify(input: any): string {
  const seen = new WeakSet();

  const sorter = (val: any): any => {
    if (val && typeof val === "object") {
      if (seen.has(val)) return "[Circular]";
      seen.add(val);

      if (Array.isArray(val)) return val.map(sorter);

      const out: Record<string, any> = {};
      Object.keys(val)
        .sort()
        .forEach((k) => {
          out[k] = sorter(val[k]);
        });
      return out;
    }
    return val;
  };

  return JSON.stringify(sorter(input));
}

function sha256(s: string): string {
  return crypto.createHash("sha256").update(s, "utf8").digest("hex");
}

function toMillis(ts: string): number {
  const ms = Date.parse(ts);
  return Number.isFinite(ms) ? ms : 0;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function main() {
  const supabase = getSupabaseAdmin();

  // 0) Resolve source_system_id by source_system.key
  const { data: sourceSystems, error: ssErr } = await supabase.from("source_system").select("id, key");
  if (ssErr) throw ssErr;

  const sourceSystemIdByKey = new Map<string, string>();
  (sourceSystems ?? []).forEach((s: { id: string; key: string }) => {
    sourceSystemIdByKey.set(s.key, s.id);
  });

  function resolveSourceSystemId(sourceKey: string | null): string | null {
    const k = normalizeText(sourceKey);
    if (!k) return null;
    return sourceSystemIdByKey.get(k) ?? null;
  }

  // 1) Load raw rows
  const { data: rawRows, error: rawErr } = await supabase
    .from("event_raw")
    .select("id, source_id, source_url, item_url, raw_payload, fetched_at, status")
    .eq("status", "ok")
    .limit(5000);

  if (rawErr) throw rawErr;

  const rows = (rawRows ?? []) as EventRawRow[];
  console.log(`[ingest:raw] Loaded event_raw rows: ${rows.length}`);

  // 2) Build docs
  const docs = rows
    .map((r) => {
      const sourceSystemId = resolveSourceSystemId(r.source_id);
      if (!sourceSystemId) return null;

      const payload = r.raw_payload ?? {};
      const itemUrl = normalizeText(payload?.item_url) || normalizeText(r.item_url) || "";
      const sourceUrl = normalizeText(r.source_url) || normalizeText(r.item_url) || itemUrl || null;

      const sourceEntityKey = itemUrl || sourceUrl || `event_raw:${r.id}`; // must be NOT NULL
      const observedAt = normalizeText(r.fetched_at) || new Date().toISOString();

      const payloadStr = stableJsonStringify(payload);
      const contentHash = sha256(payloadStr);

      return {
        source_system_id: sourceSystemId,
        source_url: sourceUrl,
        source_entity_key: sourceEntityKey,
        content_type: "json",
        payload,
        observed_at: observedAt,
        content_hash: contentHash,
      };
    })
    .filter(Boolean) as Array<{
      source_system_id: string;
      source_url: string | null;
      source_entity_key: string;
      content_type: string;
      payload: any;
      observed_at: string;
      content_hash: string;
    }>;

  console.log(`[ingest:raw] Prepared source_document rows: ${docs.length}`);

  if (docs.length === 0) {
    console.log("[ingest:raw] No docs to write. Done.");
    return;
  }

  // 3) ✅ DEDUPE within the batch by the EXACT onConflict key
  // key = (source_system_id, source_entity_key, content_hash)
  const byKey = new Map<string, (typeof docs)[number]>();

  for (const d of docs) {
    const k = `${d.source_system_id}||${d.source_entity_key}||${d.content_hash}`;
    const prev = byKey.get(k);

    if (!prev) {
      byKey.set(k, d);
      continue;
    }

    // Keep the newest observed_at (and prefer non-null source_url)
    const prevMs = toMillis(prev.observed_at);
    const curMs = toMillis(d.observed_at);

    if (curMs > prevMs) {
      byKey.set(k, {
        ...d,
        source_url: d.source_url || prev.source_url,
      });
    } else {
      byKey.set(k, {
        ...prev,
        source_url: prev.source_url || d.source_url,
      });
    }
  }

  const deduped = Array.from(byKey.values());
  const removed = docs.length - deduped.length;
  if (removed > 0) {
    console.log(`[ingest:raw] Deduped ${removed} rows within batch (prevents 21000).`);
  }

  // 4) Upsert (chunked for safety)
  const conflictTarget = "source_system_id,source_entity_key,content_hash";
  const chunks = chunk(deduped, 500);

  for (let i = 0; i < chunks.length; i++) {
    const batch = chunks[i];
    const { error: upErr } = await supabase.from("source_document").upsert(batch, {
      onConflict: conflictTarget,
    });
    if (upErr) throw upErr;
    console.log(`[ingest:raw] Upserted batch ${i + 1}/${chunks.length} (${batch.length} rows)`);
  }

  // 5) Quick sanity count
  const { count: rawCount, error: cntErr } = await supabase
    .from("source_document")
    .select("id", { count: "exact", head: true });

  if (cntErr) throw cntErr;

  console.log(`[ingest:raw] Done. source_document count = ${rawCount ?? 0}`);
}

main().catch((err: any) => {
  console.error("[ingest:raw] ERROR:", err?.message ?? err);
  if (err?.code) console.error("[ingest:raw] code:", err.code);
  if (err?.details) console.error("[ingest:raw] details:", err.details);
  process.exit(1);
});
