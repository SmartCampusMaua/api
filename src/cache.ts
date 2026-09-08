import { createHash } from "node:crypto";
import { mkdir, readdir, stat, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

// Two tiers, no Redis: an in-memory tier for small JSON-shaped data (typed sensor rows — the
// "hot dashboard" path), and an on-disk (/tmp) tier for larger raw byte blobs (export
// csv/json/parquet). Both are per-process — see the k8s multi-replica note in README.md: with
// several replicas behind a load-balanced Service, each pod keeps its own copy rather than
// sharing one, which dilutes the hit rate but doesn't affect correctness (only read-only
// historical query results are ever cached here).
//
// TTL is intentionally split by whether the cached range is still "open" (its end is at or after
// now, so InfluxDB may still be receiving writes for it — e.g. today's UTC day) or "closed" (its
// end is firmly in the past, so a repeat query should return the same rows) — see rangeTtlMs().

export const OPEN_RANGE_TTL_MS = 15_000;
export const CLOSED_RANGE_TTL_MS = 6 * 60 * 60 * 1000;

export function rangeTtlMs(endNs: bigint): number {
  const nowNs = BigInt(Date.now()) * 1_000_000n;
  return endNs <= nowNs ? CLOSED_RANGE_TTL_MS : OPEN_RANGE_TTL_MS;
}

function stableStringify(obj: Record<string, string | number>): string {
  return Object.keys(obj)
    .sort()
    .map((k) => `${k}=${obj[k]}`)
    .join("&");
}

// Every cache key is scoped by target host (not org slug, and never the token) so different
// InfluxDB instances never collide — same pattern columnExists() in influxdb.ts already uses.
// `kind` discriminates query shape (typed-rows vs export-csv vs export-json, etc.) since the same
// day/filters can be queried in more than one shape.
export function buildCacheKey(parts: {
  host: string;
  database: string;
  table: string;
  kind: string;
  startNs: bigint;
  endNs: bigint;
  bindParams: Record<string, string | number>;
}): string {
  return [
    parts.host,
    parts.database,
    parts.table,
    parts.kind,
    parts.startNs.toString(),
    parts.endNs.toString(),
    stableStringify(parts.bindParams),
  ].join("::");
}

// ─────────────────────────────────────────────────────────────────────────────
// Tier 1: in-memory (Map-based LRU + TTL)
// ─────────────────────────────────────────────────────────────────────────────

const MEMORY_MAX_ENTRIES = 5000;
const memoryStore = new Map<string, { value: unknown; expiresAt: number }>();

function memoryGet<T>(key: string): T | undefined {
  const entry = memoryStore.get(key);
  if (!entry) return undefined;
  if (Date.now() > entry.expiresAt) {
    memoryStore.delete(key);
    return undefined;
  }
  // Re-insert to move this key to the back of the Map's iteration order (Maps preserve insertion
  // order), which is what makes "delete the first key" below an LRU eviction, not a random one.
  memoryStore.delete(key);
  memoryStore.set(key, entry);
  return entry.value as T;
}

function memorySet<T>(key: string, value: T, ttlMs: number): void {
  if (memoryStore.size >= MEMORY_MAX_ENTRIES) {
    const oldestKey = memoryStore.keys().next().value;
    if (oldestKey !== undefined) memoryStore.delete(oldestKey);
  }
  memoryStore.set(key, { value, expiresAt: Date.now() + ttlMs });
}

export async function withMemoryCache<T>(key: string, ttlMs: number, fetchFn: () => Promise<T>): Promise<T> {
  const cached = memoryGet<T>(key);
  if (cached !== undefined) return cached;
  const value = await fetchFn();
  memorySet(key, value, ttlMs);
  return value;
}

// ─────────────────────────────────────────────────────────────────────────────
// Tier 2: on-disk (/tmp) — for larger byte blobs (export bytes)
// ─────────────────────────────────────────────────────────────────────────────

const DISK_CACHE_DIR = join(tmpdir(), "smartcampus-api-cache");
const DISK_MAX_BYTES = 500 * 1024 * 1024;
const SWEEP_INTERVAL_MS = 60_000;

let diskDirReady: Promise<void> | undefined;
function ensureDiskCacheDir(): Promise<void> {
  if (!diskDirReady) diskDirReady = mkdir(DISK_CACHE_DIR, { recursive: true }).then(() => undefined);
  return diskDirReady;
}

function diskCachePath(key: string): string {
  // sha256 rather than Bun's faster non-crypto hash: a filename collision between two different
  // cache keys would silently serve the wrong bytes for one of them, not just cause a harmless
  // extra miss — worth the trivial extra cost to make that practically impossible.
  const hash = createHash("sha256").update(key).digest("hex");
  return join(DISK_CACHE_DIR, `${hash}.bin`);
}

let lastSweep = 0;
async function sweepDiskCache(): Promise<void> {
  const now = Date.now();
  if (now - lastSweep < SWEEP_INTERVAL_MS) return;
  lastSweep = now;
  const names = await readdir(DISK_CACHE_DIR).catch(() => [] as string[]);
  const entries = await Promise.all(
    names.map(async (name) => {
      const path = join(DISK_CACHE_DIR, name);
      const s = await stat(path).catch(() => undefined);
      return s ? { path, size: s.size, mtimeMs: s.mtimeMs } : undefined;
    })
  );
  const files = entries.filter((e): e is { path: string; size: number; mtimeMs: number } => e !== undefined);
  let total = files.reduce((sum, f) => sum + f.size, 0);
  if (total <= DISK_MAX_BYTES) return;
  files.sort((a, b) => a.mtimeMs - b.mtimeMs); // oldest first
  for (const f of files) {
    if (total <= DISK_MAX_BYTES) break;
    await unlink(f.path).catch(() => {});
    total -= f.size;
  }
}

async function diskGet(key: string, ttlMs: number): Promise<ArrayBuffer | undefined> {
  await ensureDiskCacheDir();
  const path = diskCachePath(key);
  const stats = await stat(path).catch(() => undefined);
  if (!stats || Date.now() - stats.mtimeMs > ttlMs) return undefined;
  return Bun.file(path).arrayBuffer();
}

async function diskSet(key: string, bytes: ArrayBuffer): Promise<void> {
  await ensureDiskCacheDir();
  await Bun.write(diskCachePath(key), bytes);
  sweepDiskCache().catch((err) => console.error(`[cache] disk sweep failed: ${err instanceof Error ? err.message : String(err)}`));
}

export async function withDiskCache(key: string, ttlMs: number, fetchFn: () => Promise<ArrayBuffer>): Promise<ArrayBuffer> {
  const cached = await diskGet(key, ttlMs);
  if (cached !== undefined) return cached;
  const value = await fetchFn();
  await diskSet(key, value);
  return value;
}
