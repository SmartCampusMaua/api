import type { OrgTarget } from "./config";

export class InfluxDBError extends Error {
  constructor(message: string, public status: number) {
    super(message);
    this.name = "InfluxDBError";
  }
}

export type InfluxRow = Record<string, string | number | boolean | null>;

const MAX_CONNECTION_RETRIES = 2;

// Bun's fetch can fail at the connection level (fetch() itself throwing, or the connection
// dropping mid-stream while reading the body) even when InfluxDB and the network are otherwise
// healthy — most often a stale pooled keep-alive connection getting reused after the remote
// side silently closed it, which a fresh attempt on a new connection resolves. This is distinct
// from InfluxDB responding with a real error status (already an InfluxDBError by the time it
// reaches here, and retrying a genuine 4xx/5xx wouldn't help), so only non-InfluxDBError
// failures get a couple of automatic retries here, transparent to the caller. `op` must redo
// the whole fetch-and-consume-body cycle on each attempt, not just the part that failed — once
// a connection breaks mid-read, that Response's body stream is unusable and a fresh fetch() is
// the only way to get a working connection.
//
// Note: retries can't help every case — a query that deterministically fails against InfluxDB
// itself (e.g. the server resetting the connection mid-response for a specific query/data
// combination, confirmed independently via curl during investigation of this class of error)
// will keep failing on every attempt. The logging below exists to tell those two cases apart:
// look for whether `code`/`errno` (and whether raw curl reproduces the same request) point at a
// one-off blip or a deterministic upstream problem.
async function withRetry<T>(host: string, op: () => Promise<T>): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= MAX_CONNECTION_RETRIES; attempt++) {
    try {
      return await op();
    } catch (err) {
      if (err instanceof InfluxDBError) throw err;
      lastErr = err;
      const cause = (err as { cause?: { code?: string; errno?: number } })?.cause;
      console.error(
        `[influxdb] attempt ${attempt + 1}/${MAX_CONNECTION_RETRIES + 1} failed: ${err instanceof Error ? err.message : String(err)}` +
          (cause ? ` (code=${cause.code} errno=${cause.errno})` : "")
      );
    }
  }
  throw new InfluxDBError(
    `Failed to reach InfluxDB at ${host} after ${MAX_CONNECTION_RETRIES + 1} attempts: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
    502
  );
}

async function query(
  sql: string,
  format: "json" | "csv" | "parquet",
  params: Record<string, string | number> | undefined,
  database: string,
  target: OrgTarget
): Promise<Response> {
  const response = await fetch(`${target.host}/api/v3/query_sql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${target.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ db: database, q: sql, format, params }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new InfluxDBError(
      `InfluxDB query failed (${response.status}): ${body}`,
      response.status
    );
  }

  return response;
}

export async function querySql(
  sql: string,
  params: Record<string, string | number> | undefined,
  database: string,
  target: OrgTarget
): Promise<InfluxRow[]> {
  return withRetry(target.host, async () => {
    const response = await query(sql, "json", params, database, target);
    return (await response.json()) as InfluxRow[];
  });
}

export async function queryRaw(
  sql: string,
  format: "csv" | "parquet" | "json",
  params: Record<string, string | number> | undefined,
  database: string,
  target: OrgTarget
): Promise<ArrayBuffer> {
  return withRetry(target.host, async () => {
    const response = await query(sql, format, params, database, target);
    return response.arrayBuffer();
  });
}

const columnExistsCache = new Map<string, boolean>();

// InfluxDB3's schema is write-driven: a tag/field column only exists in a table's schema once
// at least one row has actually been written with it — a column that's documented/planned but
// never yet written causes a hard "No field named X" query-planning error, not a per-row null
// like an ordinary SQL table would give. This checks (and caches for the process's lifetime — a
// real schema change is a deliberate, rare event, and a restart is a reasonable way to pick one
// up) whether a column is actually queryable right now, so callers can build SQL that only
// references it when doing so is safe.
export async function columnExists(
  database: string,
  table: string,
  column: string,
  target: OrgTarget
): Promise<boolean> {
  const cacheKey = `${target.host}::${database}::${table}::${column}`;
  const cached = columnExistsCache.get(cacheKey);
  if (cached !== undefined) return cached;

  const rows = await querySql(
    "SELECT column_name FROM information_schema.columns WHERE table_name = $table AND column_name = $column",
    { table, column },
    database,
    target
  );
  const exists = rows.length > 0;
  columnExistsCache.set(cacheKey, exists);
  return exists;
}
