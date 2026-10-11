import { Elysia, t } from "elysia";
import { zipSync } from "fflate";
import { config, type OrgTarget } from "../config";
import { InfluxDBError, querySql, queryRaw, columnExists, type InfluxRow } from "../influxdb";
import { withMemoryCache, withDiskCache, buildCacheKey, rangeTtlMs } from "../cache";
import { sensorRegistry, resolveSensorId, sensorsByDeviceId, sensorsByAssetId, sensorsByAssetSlug } from "../sensors";

// Each :org routes to a distinct InfluxDB instance (its own host + token) — this is the API's
// own multi-instance registry, configured via infra.json in src/config.ts. It's unrelated
// to InfluxDB3 Core's own org concept, which doesn't exist (see README.md). Org slugs are
// always lowercase, so the incoming URL segment is normalized before lookup — "IMT" and "imt"
// resolve the same way.
function resolveOrg(org: string): { ok: true; target: OrgTarget } | { ok: false; status: number; error: string } {
  const target = config.influxdb.orgs[org.toLowerCase()];
  if (!target) {
    return {
      ok: false,
      status: 400,
      error: `Unknown org "${org}". Configured: ${Object.keys(config.influxdb.orgs).join(", ")}`,
    };
  }
  return { ok: true, target };
}

// dev_eui only exists for these LoRaWAN providers; mac_address only for "custom" (IP-based).
// See https://github.com/smartcampusmaua/parse-mqtt-to-influxdb3#device-type-determination
const LORAWAN_PROVIDERS = new Set(["chirpstackv4", "everynet"]);

// Mirrors LORAWAN_PROVIDERS above, computed in SQL for the export route since it hands back
// raw query results rather than rows we reshape in JS.
const DEVICE_TYPE_EXPR = `CASE WHEN "provider" IN ('${[...LORAWAN_PROVIDERS].join("','")}') THEN 'lorawan' WHEN "provider" = 'custom' THEN 'ip' ELSE NULL END AS device_type`;

// URLs (paths/query params) use camelCase; InfluxDB columns and JSON response bodies stay
// snake_case (InfluxDB's own convention, and what the ingestion pipeline already writes).
// This is the one place that bridges the two, so every tag filter name a caller can type in
// a URL maps to exactly one real column.
const TAG_CAMEL_TO_SNAKE: Record<string, string> = {
  devEui: "dev_eui",
  deviceId: "device_id",
  deviceModel: "device_model",
  macAddress: "mac_address",
  provider: "provider",
  sensorType: "sensor_type",
  sensorId: "sensor_id",
  assetId: "asset_id",
  eventType: "event_type",
};

// sensor_type is the one tag value callers reference by name often enough to want the same
// camelCase convention on its way in — e.g. ?sensorType=airTemp instead of ?sensorType=air_temp
// — even though the value stored in InfluxDB (and in response bodies) is snake_case. This is
// deliberately narrower than TAG_CAMEL_TO_SNAKE: only sensor_type's *value* gets this
// treatment, not device_model/provider/etc., and not the response side (which always mirrors
// InfluxDB's own snake_case value verbatim). Already-snake_case input passes through
// unchanged (no uppercase letters to rewrite), so ?sensorType=air_temp keeps working too.
function camelValueToSnake(value: string): string {
  return value.replace(/([A-Z])/g, "_$1").toLowerCase();
}

// Per-table schema registry. Tables in this database aren't uniform: sensor_data has a single
// exclusive value per row (value_bool/value_float/value_int) plus LoRaWAN/IP identity columns;
// audit_iot.raw has a text blob and no sensor_type/value columns at all. Shaped output (the
// base/series route) needs to know which shape it's dealing with; export doesn't (SELECT * works
// for any of them), which is why `shaped: false` only disables the former.
// `tags` here are the camelCase URL names (keys of TAG_CAMEL_TO_SNAKE above).
type TableConfig = {
  tags: readonly string[];
  hasIdentifier: boolean;
  valueColumns: readonly string[];
  valueMode: "exclusive" | "none";
  shaped: boolean;
};

const TABLES: Record<string, TableConfig> = {
  sensor_data: {
    tags: ["devEui", "deviceId", "deviceModel", "macAddress", "provider", "sensorType", "sensorId", "assetId"],
    hasIdentifier: true,
    valueColumns: ["value_bool", "value_float", "value_int"],
    valueMode: "exclusive",
    shaped: true,
  },
  raw: {
    tags: ["deviceId", "eventType"],
    hasIdentifier: false,
    valueColumns: ["raw_data"],
    valueMode: "none",
    shaped: false,
  },
};

// Union of every table's tags, for the Elysia query schema (which is static regardless of
// which table a given request targets).
const ALL_TAGS = [...new Set(Object.values(TABLES).flatMap((t) => t.tags))];

// URL-facing domain/resource names, decoupled from InfluxDB's own database/table names — the
// URL no longer exposes "iot_sensors"/"sensor_data"/etc. directly. Both iot_sensors and
// audit_iot contain "iot", so domain "iot" + the other half of each name ("sensors"/"audit")
// is what maps back to them.
//
// A "vehicle" domain (vehicle_telemetry / audit_vehicle) would follow the same shape once
// there's real schema to build it from: both databases exist on the InfluxDB instance already
// (the ingestion pipeline opens clients for them), but neither has ever had a table written to
// it, and parse-mqtt-to-influxdb3's source has no write/record code for them yet either — so
// there's currently nothing to verify a table/column registry entry against.
type ResourceConfig = { database: string; table: string };

const DOMAIN_RESOURCES: Record<string, Record<string, ResourceConfig>> = {
  iot: {
    sensors: { database: "iot_sensors", table: "sensor_data" },
    audit: { database: "audit_iot", table: "raw" },
  },
};

function resolveResource(
  domain: string,
  resource: string
): { ok: true; resource: ResourceConfig } | { ok: false; status: number; error: string } {
  const resources = DOMAIN_RESOURCES[domain];
  if (!resources) {
    return { ok: false, status: 400, error: `Unknown domain "${domain}". Supported: ${Object.keys(DOMAIN_RESOURCES).join(", ")}` };
  }
  const resourceConfig = resources[resource];
  if (!resourceConfig) {
    return {
      ok: false,
      status: 400,
      error: `Unknown resource "${resource}" for domain "${domain}". Supported: ${Object.keys(resources).join(", ")}`,
    };
  }
  return { ok: true, resource: resourceConfig };
}

const DEFAULT_LIMIT = 1000;
const MAX_LIMIT = 10000;
const DEFAULT_INTERVAL_MINUTES = 1;

// parquet's deliverable is a zip bundle of one .parquet file per UTC day (see queryExportBytes) —
// :format=parquet still selects Parquet as the per-row format, the container is just zip.
const EXPORT_FORMATS = {
  parquet: { contentType: "application/zip", extension: "zip" },
  csv: { contentType: "text/csv", extension: "csv" },
  json: { contentType: "application/json", extension: "json" },
} as const;
type ExportFormat = keyof typeof EXPORT_FORMATS;

// e.g. imt_iot_sensors_sensor_data_20260816T143022Z.parquet — org/database/table identify
// the content without opening the file, the UTC timestamp (filename-safe: no ":") is when
// the export was generated (not a data timestamp) so repeated exports never collide.
function exportFilename(org: string, database: string, table: string, extension: string): string {
  const timestamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  return `${org.toLowerCase()}_${database}_${table}_${timestamp}.${extension}`;
}

type TimeseriesQuery = Partial<Record<string, string>> & {
  interval?: string;
  startTime?: string;
  endTime?: string;
  limit?: string;
};

type TimeRange = { startNs: bigint; endNs: bigint };

// Accepts a date-only string (interpreted as UTC midnight — JS's own Date parser already does
// this correctly) or a full datetime with an explicit "Z"/offset. Rejects a datetime with a
// time component but no timezone marker (e.g. "2026-08-16T14:30:00" or "2026-08-16 14:30:00")
// — that form is parsed by JS as *server-local* time, so the exact same URL would silently
// resolve to a different instant depending on where this process happens to be deployed. Since
// this API is otherwise strictly UTC (nanosecond epochs everywhere else), a silent-guess here
// would be a real bug, not a convenience — better to reject it and say so.
const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;
const DATETIME_WITH_TZ = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

function validateTimestamp(param: string, value: string): string | undefined {
  if (!DATE_ONLY.test(value) && !DATETIME_WITH_TZ.test(value)) {
    return `Query param '${param}' must be a date (e.g. 2026-08-16) or a full datetime with an explicit timezone (e.g. 2026-08-16T14:30:00Z or 2026-08-16T14:30:00-03:00) — a datetime without "Z"/an offset is ambiguous and rejected rather than guessed.`;
  }
  if (Number.isNaN(new Date(value).getTime())) {
    return `Query param '${param}' is not a valid date`;
  }
  return undefined;
}

// Two ways to bound a query: a relative lookback ("interval", in minutes, from now) or an
// absolute calendar range ("startTime"/"endTime", ISO 8601). Exactly one applies — mixing
// them is rejected rather than guessing which one wins.
function resolveTimeRange(query: TimeseriesQuery): TimeRange | { error: string } {
  const hasInterval = query.interval !== undefined;
  const hasStart = query.startTime !== undefined;
  const hasEnd = query.endTime !== undefined;

  if (hasInterval && (hasStart || hasEnd)) {
    return { error: "Use either 'interval' or 'startTime'+'endTime', not both" };
  }
  if (hasStart !== hasEnd) {
    return { error: "'startTime' and 'endTime' must be provided together" };
  }

  if (hasStart && hasEnd) {
    const startError = validateTimestamp("startTime", query.startTime as string);
    if (startError) return { error: startError };
    const endError = validateTimestamp("endTime", query.endTime as string);
    if (endError) return { error: endError };

    const start = new Date(query.startTime as string);
    const end = new Date(query.endTime as string);
    if (start.getTime() >= end.getTime()) {
      return { error: "'startTime' must be before 'endTime'" };
    }
    return {
      startNs: BigInt(start.getTime()) * 1_000_000n,
      endNs: BigInt(end.getTime()) * 1_000_000n,
    };
  }

  const interval = query.interval ? Number(query.interval) : DEFAULT_INTERVAL_MINUTES;
  if (!Number.isFinite(interval) || interval <= 0) {
    return { error: "Query param 'interval' must be a positive number of minutes" };
  }
  const nowMs = Date.now();
  return {
    // Compare against the raw int64 nanosecond epoch InfluxDB stores internally, rather than
    // letting the query planner evaluate now() - INTERVAL per request.
    startNs: BigInt(Math.round(nowMs - interval * 60_000)) * 1_000_000n,
    endNs: BigInt(nowMs) * 1_000_000n,
  };
}

const DAY_MS = 24 * 60 * 60 * 1000;
const MAX_DAY_CHUNKS = 366;

// Splits [startNs, endNs] into UTC-calendar-day-aligned chunks, newest first. This is the
// mitigation for InfluxDB3 Core's per-query Parquet-file-scan cap: querying (say) 30 days in
// one shot risks that cap, especially unfiltered, so instead this runs up to one query per day
// and the caller concatenates results — the same workaround an API consumer would otherwise
// have to hand-roll themselves. Each chunk stays within a single UTC day, so the worst case any
// one InfluxDB query has to survive is one day's write density, not the whole requested range's.
// This isn't an absolute guarantee (an extremely write-dense single day could still trip the
// cap on its own), but it's a large, reliable improvement over one query for the whole range.
function splitIntoDayChunks(startNs: bigint, endNs: bigint): Array<{ startNs: bigint; endNs: bigint }> {
  const startMs = Number(startNs / 1_000_000n);
  const endMs = Number(endNs / 1_000_000n);
  const chunks: Array<{ startNs: bigint; endNs: bigint }> = [];
  let chunkEndMs = endMs;
  while (chunkEndMs > startMs) {
    const dayStartMs = Math.floor((chunkEndMs - 1) / DAY_MS) * DAY_MS;
    const chunkStartMs = Math.max(startMs, dayStartMs);
    chunks.push({ startNs: BigInt(chunkStartMs) * 1_000_000n, endNs: BigInt(chunkEndMs) * 1_000_000n });
    chunkEndMs = chunkStartMs;
  }
  return chunks;
}

type ResolvedFilter =
  | {
      ok: true;
      table: string;
      config: TableConfig;
      whereTags: string;
      tagBindParams: Record<string, string>;
      startNs: bigint;
      endNs: bigint;
      chunks: Array<{ startNs: bigint; endNs: bigint }>;
      limit: number;
      sensorIdAvailable: boolean;
      assetIdAvailable: boolean;
    }
  | { ok: false; status: number; error: string };

// `sensorIdAvailable`/`assetIdAvailable` reflect whether sensor_id/asset_id currently exist as
// real columns on this table (see columnExists() in src/influxdb.ts) — InfluxDB3 only registers a
// tag column once at least one row has been written with it, so "documented in the ingestion
// schema" and "actually queryable right now" are different questions. asset_id in particular
// lags further behind than sensor_id: it's only populated for devices that have actually been
// assigned to an asset in assets.json (optional, most devices don't have one yet), on top of the
// same "no data written with it yet" gap sensor_id has. When a tag isn't available, it's dropped
// from the filterable tags entirely (an explicit ?sensorId=/?assetId= is rejected with a clear
// 400 rather than silently ignored, since silently ignoring a filter the caller explicitly asked
// for could make unfiltered results look filtered).
function resolveFilter(table: string, query: TimeseriesQuery, sensorIdAvailable: boolean, assetIdAvailable: boolean): ResolvedFilter {
  const config = TABLES[table];
  if (!config) {
    return {
      ok: false,
      status: 400,
      error: `Unsupported table "${table}". Supported: ${Object.keys(TABLES).join(", ")}`,
    };
  }

  if (!sensorIdAvailable && query.sensorId !== undefined) {
    return {
      ok: false,
      status: 400,
      error: "Query param 'sensorId' is not available yet — no data in this database has been written with sensor_id yet.",
    };
  }
  if (!assetIdAvailable && query.assetId !== undefined) {
    return {
      ok: false,
      status: 400,
      error: "Query param 'assetId' is not available yet — no data in this database has been written with asset_id yet.",
    };
  }
  let tags: readonly string[] = config.tags;
  if (!sensorIdAvailable) tags = tags.filter((tag) => tag !== "sensorId");
  if (!assetIdAvailable) tags = tags.filter((tag) => tag !== "assetId");

  // Tag filters are optional: with none given, the time bound below is the only
  // restriction, matching every device (and every sensor type, where applicable) in the window.
  const filters = tags.filter((tag) => query[tag] !== undefined);

  const range = resolveTimeRange(query);
  if ("error" in range) {
    return { ok: false, status: 400, error: range.error };
  }

  const chunks = splitIntoDayChunks(range.startNs, range.endNs);
  if (chunks.length > MAX_DAY_CHUNKS) {
    return {
      ok: false,
      status: 400,
      error: `Requested time range spans ${chunks.length} days, exceeding the ${MAX_DAY_CHUNKS}-day limit per request (this API queries InfluxDB one UTC day at a time to stay under its file-scan cap, so a very wide range means a very large number of sequential queries). Split it into multiple requests instead, e.g. one per year.`,
    };
  }

  const limit = query.limit ? Number(query.limit) : DEFAULT_LIMIT;
  if (!Number.isInteger(limit) || limit <= 0 || limit > MAX_LIMIT) {
    return { ok: false, status: 400, error: `Query param 'limit' must be an integer between 1 and ${MAX_LIMIT}` };
  }

  // Tag names are translated from the URL's camelCase to their real snake_case column names
  // via the fixed TAG_CAMEL_TO_SNAKE map above, never from raw path/query text directly, so
  // interpolating them here can't lead to SQL injection.
  //
  // Any tag filter may be a comma-separated list (e.g. ?sensorId=a,b,c) — a single value builds
  // "col" = $tag as before; multiple values build a parameterized "col" IN ($tag0, $tag1, ...)
  // instead. Values are sorted before binding, purely so the same *set* of values always produces
  // the same bind-param object (and so the same cache key) regardless of the order they were
  // given in — the SQL result is identical either way, IN() is order-agnostic.
  const tagClauses: string[] = [];
  const tagBindParams: Record<string, string> = {};
  for (const tag of filters) {
    const column = TAG_CAMEL_TO_SNAKE[tag];
    const rawValue = query[tag] as string;

    // No comma: identical to this filter's previous behavior (including an empty-string value,
    // which just matches zero rows rather than erroring — not a case worth special-casing).
    if (!rawValue.includes(",")) {
      tagBindParams[tag] = tag === "sensorType" ? camelValueToSnake(rawValue) : rawValue;
      tagClauses.push(`"${column}" = $${tag}`);
      continue;
    }

    const values = rawValue
      .split(",")
      .map((v) => v.trim())
      .filter((v) => v.length > 0)
      .map((v) => (tag === "sensorType" ? camelValueToSnake(v) : v))
      .sort();
    if (values.length === 0) {
      return { ok: false, status: 400, error: `Query param '${tag}' is a comma-separated list but resolved to zero values — check for stray commas.` };
    }
    const placeholders = values.map((value, i) => {
      const paramName = `${tag}${i}`;
      tagBindParams[paramName] = value;
      return `$${paramName}`;
    });
    tagClauses.push(`"${column}" IN (${placeholders.join(", ")})`);
  }
  const whereTags = tagClauses.length ? `${tagClauses.join(" AND ")} AND` : "";

  return { ok: true, table, config, whereTags, tagBindParams, startNs: range.startNs, endNs: range.endNs, chunks, limit, sensorIdAvailable, assetIdAvailable };
}

// Translates ?assetSlug= into the equivalent ?assetId= before resolveFilter runs, since InfluxDB
// itself only ever has asset_id as a real tag — asset_slug is a registry-only concept (see
// src/sensors.ts) that never reaches a SQL query directly. Merges with any assetId already given
// (union, not AND) rather than making the caller pick one or the other, since both are just
// different ways of saying "this asset." If assetIdAvailable turns out to be false, resolveFilter
// still rejects this with a 400 — the message just names "assetId" rather than "assetSlug" in
// that case, a minor wording rough edge not worth extra plumbing for right now.
function resolveAssetSlugFilter(query: TimeseriesQuery): TimeseriesQuery {
  if (query.assetSlug === undefined) return query;
  const slugs = query.assetSlug.split(",").map((s) => s.trim()).filter((s) => s.length > 0);
  const resolvedIds = slugs.flatMap((slug) => sensorsByAssetSlug(slug).map((s) => s.asset_id)).filter((id): id is string => !!id);
  const existingIds = query.assetId ? query.assetId.split(",").map((s) => s.trim()).filter((s) => s.length > 0) : [];
  const merged = [...new Set([...existingIds, ...resolvedIds])];
  // A given assetSlug that matches nothing must still narrow to zero rows, not silently fall back
  // to "no assetId filter at all" — force a value that can never equal a real asset_id.
  const assetId = merged.length > 0 ? merged.join(",") : "__no-assetSlug-match__";
  return { ...query, assetId };
}

// Maps an InfluxDB "file scan limit" planning error to a 400 the caller can act on
// (narrow the query), as opposed to a generic 502 for unrelated upstream failures.
function toErrorResponse(err: unknown, set: { status?: number | string }, tags: readonly string[]) {
  if (err instanceof InfluxDBError) {
    if (err.message.includes("exceeding the file limit")) {
      set.status = 400;
      return {
        error: `Time range too wide for this query (too many files to scan), even after per-day chunking — at least one individual day is too write-dense on its own. Narrow the range further or add a tag filter (${tags.join(", ")}).`,
      };
    }
    set.status = 502;
    return { error: err.message };
  }
  throw err;
}

// sensor_data stores one active value per row across several mutually-exclusive typed columns;
// pick whichever is non-null.
function extractValue(row: InfluxRow, valueColumns: readonly string[]): number | boolean | null {
  for (const col of valueColumns) {
    if (row[col] !== null && row[col] !== undefined) return row[col] as number | boolean;
  }
  return null;
}

function extractIdentifier(row: InfluxRow, config: TableConfig): Record<string, string> {
  if (!config.hasIdentifier) return {};
  if (LORAWAN_PROVIDERS.has(row.provider as string) && row.dev_eui) {
    return { dev_eui: row.dev_eui as string };
  }
  if (row.provider === "custom" && row.mac_address) {
    return { mac_address: row.mac_address as string };
  }
  return {};
}

// One query per day-chunk (see splitIntoDayChunks), cached at that same day granularity — see
// src/cache.ts. Each day-chunk is always queried/cached at a fixed MAX_LIMIT cap rather than the
// caller's own (possibly much smaller) `limit`: the caller's `limit` is a whole-request budget
// that depends on how many rows earlier (newer) chunks already produced, so baking it into the
// per-day SQL would make the exact query — and therefore the cache key — different for every
// differently-limited request, defeating reuse. Caching the day's full (capped) result once and
// trimming to the caller's actual remaining budget afterward lets any number of differently
// `limit`-ed requests share the same cached day.
async function queryDayChunkTyped(
  resolved: Extract<ResolvedFilter, { ok: true }>,
  selectCols: string,
  chunk: { startNs: bigint; endNs: bigint },
  database: string,
  target: OrgTarget
): Promise<InfluxRow[]> {
  // time is cast to VARCHAR (not just BIGINT) so InfluxDB emits it as a JSON string. Nanosecond
  // epoch values exceed Number.MAX_SAFE_INTEGER, so if it came back as a JSON number it would
  // already be rounded by the time our JSON parser touches it.
  const sql = `
    SELECT CAST(CAST(time AS BIGINT) AS VARCHAR) AS time, ${selectCols}
    FROM "${resolved.table}"
    WHERE ${resolved.whereTags} time >= arrow_cast(CAST($start AS BIGINT), 'Timestamp(Nanosecond, None)')
      AND time <= arrow_cast(CAST($end AS BIGINT), 'Timestamp(Nanosecond, None)')
    ORDER BY time DESC
    LIMIT ${MAX_LIMIT}
  `;
  const bindParams = { ...resolved.tagBindParams, start: chunk.startNs.toString(), end: chunk.endNs.toString() };
  const cacheKey = buildCacheKey({
    host: target.host,
    database,
    table: resolved.table,
    kind: `typed-rows:sid=${resolved.sensorIdAvailable}:aid=${resolved.assetIdAvailable}:cols=${selectCols}`,
    startNs: chunk.startNs,
    endNs: chunk.endNs,
    bindParams: resolved.tagBindParams,
  });
  return withMemoryCache(cacheKey, rangeTtlMs(chunk.endNs), () => querySql(sql, bindParams, database, target));
}

// Same columns, same casts, same filters regardless of caller — grouping into the series shape
// happens downstream of this. Queries newest chunk first, stopping as soon as the overall
// `limit` is satisfied so a request for recent data doesn't pay for querying (or even reading
// from cache) older days at all.
async function queryTypedRows(resolved: Extract<ResolvedFilter, { ok: true }>, database: string, target: OrgTarget): Promise<InfluxRow[]> {
  const { config } = resolved;
  const identifierColumns = config.hasIdentifier ? ["dev_eui", "mac_address", "provider"] : [];
  const sensorIdColumn = resolved.sensorIdAvailable ? ["sensor_id"] : [];
  const assetIdColumn = resolved.assetIdAvailable ? ["asset_id"] : [];
  const columns = ["device_id", ...sensorIdColumn, ...assetIdColumn, ...identifierColumns, "sensor_type", ...config.valueColumns];
  const selectCols = columns.map((c) => `"${c}"`).join(", ");

  const rows: InfluxRow[] = [];
  for (const chunk of resolved.chunks) {
    const remaining = resolved.limit - rows.length;
    if (remaining <= 0) break;
    const dayRows = await queryDayChunkTyped(resolved, selectCols, chunk, database, target);
    rows.push(...dayRows.slice(0, remaining));
  }
  return rows;
}

// The base/series response always carries sensor_id, sensor_type, and series — every other key
// (asset_id, device_id, dev_eui/mac_address) is opt-in/opt-out via ?fields=, so a caller who only
// wants the mandatory three doesn't have to receive (or filter out client-side) tags it doesn't
// need. Names match the camelCase tag names already used for filtering (TAG_CAMEL_TO_SNAKE).
const OPTIONAL_SERIES_FIELDS = ["deviceId", "assetId", "devEui", "macAddress"] as const;
type OptionalSeriesField = (typeof OPTIONAL_SERIES_FIELDS)[number];

const filterQuerySchema = t.Object({
  interval: t.Optional(t.String()),
  startTime: t.Optional(t.String()),
  endTime: t.Optional(t.String()),
  limit: t.Optional(t.String()),
  // Only meaningful on /export (required there, validated manually — see the same
  // clean-400-over-raw-schema-error reasoning used for every other param in this schema); the
  // base/series route simply never reads it, same as it already ignores any other tag that
  // doesn't apply to the resource being queried.
  format: t.Optional(t.String()),
  // Only meaningful on the base/series route (see OPTIONAL_SERIES_FIELDS above) — /export ignores
  // it, since export is a raw SELECT * passthrough with no per-field shaping to opt in/out of.
  fields: t.Optional(t.String()),
  // Not a real InfluxDB tag (unlike everything in ALL_TAGS below) — resolved into the equivalent
  // assetId via resolveAssetSlugFilter() before filter resolution runs. See src/sensors.ts.
  assetSlug: t.Optional(t.String()),
  ...Object.fromEntries(ALL_TAGS.map((tag) => [tag, t.Optional(t.String())])),
});

const NOT_SHAPED_ERROR = (table: string) =>
  `Table "${table}" has no sensor_type/value columns to shape into series. Use /export?format=... instead.`;

export const timeseries = new Elysia()
  .get(
    "/api/timeseries/v0.4/:org/:domain/:resource",
    async ({ params, query, set }) => {
      const org = resolveOrg(params.org);
      if (!org.ok) {
        set.status = org.status;
        return { error: org.error };
      }
      const resourceLookup = resolveResource(params.domain, params.resource);
      if (!resourceLookup.ok) {
        set.status = resourceLookup.status;
        return { error: resourceLookup.error };
      }
      const { database, table } = resourceLookup.resource;

      let sensorIdAvailable = false;
      let assetIdAvailable = false;
      try {
        sensorIdAvailable = TABLES[table]?.tags.includes("sensorId")
          ? await columnExists(database, table, "sensor_id", org.target)
          : false;
        assetIdAvailable = TABLES[table]?.tags.includes("assetId")
          ? await columnExists(database, table, "asset_id", org.target)
          : false;
      } catch (err) {
        return toErrorResponse(err, set, TABLES[table]?.tags ?? []);
      }

      const resolved = resolveFilter(table, resolveAssetSlugFilter(query), sensorIdAvailable, assetIdAvailable);
      if (!resolved.ok) {
        set.status = resolved.status;
        return { error: resolved.error };
      }
      if (!resolved.config.shaped) {
        set.status = 400;
        return { error: NOT_SHAPED_ERROR(table) };
      }

      let selectedFields: Set<OptionalSeriesField> | undefined;
      if (query.fields !== undefined) {
        const requested = query.fields.split(",").map((f) => f.trim()).filter((f) => f.length > 0);
        const unknown = requested.filter((f) => !(OPTIONAL_SERIES_FIELDS as readonly string[]).includes(f));
        if (unknown.length > 0) {
          set.status = 400;
          return { error: `Query param 'fields' has unknown value(s): ${unknown.join(", ")}. Supported: ${OPTIONAL_SERIES_FIELDS.join(", ")}` };
        }
        selectedFields = new Set(requested as OptionalSeriesField[]);
      }
      const includeField = (field: OptionalSeriesField) => !selectedFields || selectedFields.has(field);

      try {
        const rows = await queryTypedRows(resolved, database, org.target);
        const { config } = resolved;

        // One series per (device, sensor_type, sensor_id) when sensor_id is available — NOT
        // just (device, sensor_type): a device can carry several sensors sharing one sensor_type
        // (e.g. lnv3_sm3dl's three soil_moisture_raw probes, disambiguated by device_index
        // upstream), and each is its own independent time series. Grouping by (device,
        // sensor_type) alone would silently interleave multiple physical sensors' readings into
        // one array. When sensor_id isn't available at all, every reading's sensorId is
        // undefined, so the group key collapses to (device, sensor_type) — the shape this always
        // degraded to before sensor_id existed.
        type SeriesGroup = {
          device_id: string;
          sensor_id?: string;
          asset_id?: string;
          identifier: Record<string, string>;
          sensor_type: string;
          series: unknown[][];
        };
        const seriesByKey = new Map<string, SeriesGroup>();
        for (const row of rows) {
          const deviceId = row.device_id as string;
          const sensorType = row.sensor_type as string;
          // Prefer InfluxDB's own sensor_id when it's real (authoritative for that specific
          // row); fall back to the registry-resolved one otherwise — see resolveSensorId()'s
          // own comment for why that fallback is only used when it's unambiguous.
          const sensorId = (row.sensor_id as string) || resolveSensorId(deviceId, sensorType);
          const assetId = (row.asset_id as string) || undefined;
          const key = `${deviceId}::${sensorType}::${sensorId ?? ""}`;
          let group = seriesByKey.get(key);
          if (!group) {
            group = { device_id: deviceId, sensor_id: sensorId, asset_id: assetId, identifier: extractIdentifier(row, config), sensor_type: sensorType, series: [] };
            seriesByKey.set(key, group);
          }
          group.series.push([row.time, extractValue(row, config.valueColumns)]);
        }

        // sensor_id, sensor_type, and series are always present — every other key is opt-in/out
        // via ?fields= (see includeField above), and ordered sensor_id, asset_id, device_id,
        // identifier (dev_eui/mac_address), sensor_type, series regardless of which are included.
        return [...seriesByKey.values()].map((group) => {
          const response: Record<string, unknown> = { sensor_id: group.sensor_id };
          if (includeField("assetId") && group.asset_id) response.asset_id = group.asset_id;
          if (includeField("deviceId")) response.device_id = group.device_id;
          for (const [column, value] of Object.entries(group.identifier)) {
            const field: OptionalSeriesField = column === "dev_eui" ? "devEui" : "macAddress";
            if (includeField(field)) response[column] = value;
          }
          response.sensor_type = group.sensor_type;
          response.series = group.series;
          return response;
        });
      } catch (err) {
        return toErrorResponse(err, set, resolved.config.tags);
      }
    },
    {
      params: t.Object({ org: t.String(), domain: t.String(), resource: t.String() }),
      query: filterQuerySchema,
      detail: {
        summary: "Series, grouped by (device, sensor_type, sensor_id)",
        description:
          "One object per (device, sensor_type, sensor_id) with a `series` array of compact [time, value] tuples. `sensor_id`, `sensor_type`, and `series` are always present; `asset_id`/`device_id`/`dev_eui`/`mac_address` are optional and controlled by `?fields=` (comma-separated subset of deviceId,assetId,devEui,macAddress — omit to get all of them, same as before). Only works for resources whose table has sensor_type/value columns (`iot/sensors`) — 400 for raw resources like `iot/audit`, use /export instead. See README.md for the full response shape and day-chunking on wide ranges.",
        tags: ["timeseries"],
      },
    }
  )
  .get(
    "/api/timeseries/v0.4/:org/:domain/:resource/export",
    async ({ params, query, set }) => {
      const org = resolveOrg(params.org);
      if (!org.ok) {
        set.status = org.status;
        return { error: org.error };
      }
      const resourceLookup = resolveResource(params.domain, params.resource);
      if (!resourceLookup.ok) {
        set.status = resourceLookup.status;
        return { error: resourceLookup.error };
      }
      const { database, table } = resourceLookup.resource;

      if (query.format === undefined || !(query.format in EXPORT_FORMATS)) {
        set.status = 400;
        return {
          error: `Query param 'format' is required and must be one of: ${Object.keys(EXPORT_FORMATS).join(", ")} (got ${query.format === undefined ? "nothing" : `"${query.format}"`})`,
        };
      }
      const format = query.format as ExportFormat;

      // Export uses SELECT * (schema-agnostic — see queryExportBytes below), so this check is
      // only for the WHERE clause: filtering by ?sensorId=/?assetId= would still fail if the
      // column doesn't exist, same as the base route.
      let sensorIdAvailable = false;
      let assetIdAvailable = false;
      try {
        sensorIdAvailable = TABLES[table]?.tags.includes("sensorId")
          ? await columnExists(database, table, "sensor_id", org.target)
          : false;
        assetIdAvailable = TABLES[table]?.tags.includes("assetId")
          ? await columnExists(database, table, "asset_id", org.target)
          : false;
      } catch (err) {
        return toErrorResponse(err, set, TABLES[table]?.tags ?? []);
      }

      const resolved = resolveFilter(table, resolveAssetSlugFilter(query), sensorIdAvailable, assetIdAvailable);
      if (!resolved.ok) {
        set.status = resolved.status;
        return { error: resolved.error };
      }

      try {
        const bytes = await queryExportBytes(resolved, format, database, org.target);
        const { contentType, extension } = EXPORT_FORMATS[format];
        set.headers["content-type"] = contentType;
        set.headers["content-disposition"] = `attachment; filename="${exportFilename(params.org, database, table, extension)}"`;
        return bytes;
      } catch (err) {
        return toErrorResponse(err, set, resolved.config.tags);
      }
    },
    {
      params: t.Object({ org: t.String(), domain: t.String(), resource: t.String() }),
      query: filterQuerySchema,
      detail: {
        summary: "Raw export (csv/json/parquet)",
        description:
          "?format= (required: csv, json, or parquet) selects the output — same query/rows either way, only the serialization differs, which is why it's a query param here rather than a path segment the way /export itself is (export is a genuinely different code path from the base endpoint: raw SELECT *, no series grouping). SELECT * against the resource's underlying table, for every resource including non-shaped ones like `iot/audit`. csv/json are day-chunked and merged; parquet is day-chunked and bundled into a .zip (one file per UTC day) — see README.md for why, and the `limit` = rows-per-day caveat that's specific to parquet.",
        tags: ["timeseries"],
      },
    }
  )
  .get(
    "/api/timeseries/v0.4/_meta",
    () => ({
      orgs: Object.keys(config.influxdb.orgs),
      domains: Object.fromEntries(
        Object.entries(DOMAIN_RESOURCES).map(([domain, resources]) => [
          domain,
          Object.fromEntries(
            Object.entries(resources).map(([resource, { table }]) => [
              resource,
              { tags: TABLES[table].tags, shaped: TABLES[table].shaped },
            ])
          ),
        ])
      ),
    }),
    {
      detail: {
        summary: "Registry metadata (orgs, domains, resources, tags)",
        description:
          "Live snapshot of the org registry and DOMAIN_RESOURCES/TABLES registries — lets a client (e.g. the query builder UI at /) build its form from the real, current registry instead of a hardcoded copy. Not a stable/versioned contract in the way the timeseries routes are.",
        tags: ["timeseries"],
      },
    }
  )
  .get(
    "/api/timeseries/v0.4/:org/sensors",
    ({ params, query, set }) => {
      const org = resolveOrg(params.org);
      if (!org.ok) {
        set.status = org.status;
        return { error: org.error };
      }
      if (sensorRegistry.error) {
        set.status = 502;
        return { error: `Sensor registry unavailable: ${sensorRegistry.error}` };
      }

      let result = sensorRegistry.list;
      // Comma-separated multi-value per filter (OR within one filter), AND between deviceId and
      // the asset filters when combined — same convention as every tag filter on the main routes.
      // assetId and assetSlug narrow the same "asset" dimension (two spellings of the same thing),
      // so they union together rather than AND — same reasoning as resolveAssetSlugFilter below.
      // An id/slug with no match is a normal empty result, not an error: this is a static registry
      // filter, not a live-schema check like the main routes' ?sensorId= gate.
      if (query.deviceId !== undefined) {
        const ids = query.deviceId.split(",").map((v) => v.trim()).filter((v) => v.length > 0);
        const matched = new Set(ids.flatMap((id) => sensorsByDeviceId(id).map((s) => s.sensor_id)));
        result = result.filter((s) => matched.has(s.sensor_id));
      }
      if (query.assetId !== undefined || query.assetSlug !== undefined) {
        const ids = query.assetId ? query.assetId.split(",").map((v) => v.trim()).filter((v) => v.length > 0) : [];
        const slugs = query.assetSlug ? query.assetSlug.split(",").map((v) => v.trim()).filter((v) => v.length > 0) : [];
        const matched = new Set([
          ...ids.flatMap((id) => sensorsByAssetId(id).map((s) => s.sensor_id)),
          ...slugs.flatMap((slug) => sensorsByAssetSlug(slug).map((s) => s.sensor_id)),
        ]);
        result = result.filter((s) => matched.has(s.sensor_id));
      }
      return result;
    },
    {
      params: t.Object({ org: t.String() }),
      query: t.Object({ deviceId: t.Optional(t.String()), assetId: t.Optional(t.String()), assetSlug: t.Optional(t.String()) }),
      detail: {
        summary: "Sensor + device + asset registry (for building a sensor picker)",
        description:
          "Flat list of {sensor_id, sensor_type, sensor_name?, device_id, device_model?, dev_eui?, mac_address?, asset_id?, asset_name?, asset_slug?}, joined from parse-mqtt-to-influxdb3's sensors.json + devices.json + assets.json — the canonical source sensor_id is assigned from, not derived from InfluxDB itself. Currently the same list regardless of :org (only used for its existence check); 502 if the registry files aren't readable. `sensor_name` is only present on the small number of sensors that actually need a human label to disambiguate (e.g. multiple probes on one device) — fall back to `sensor_type` otherwise. `asset_id`/`asset_name`/`asset_slug` are only present for devices actually assigned to an asset in assets.json — most aren't yet. `asset_slug` is the stable, lowercase-kebab-case filter key; `asset_name` is a free-text display label with no filtering role. Optional `?deviceId=`/`?assetId=`/`?assetSlug=` narrow the list (index-backed, not a linear scan — see src/sensors.ts), each accepting a comma-separated list (OR'd); `assetId` and `assetSlug` union together (two spellings of the same filter), combined with AND against `deviceId` when given. An id/slug with no match just returns fewer results, not a 400.",
        tags: ["timeseries"],
      },
    }
  );

// SELECT * hands back the table's native columns and types as InfluxDB stores them, for tools
// (pandas/pyarrow, DuckDB, etc.) that want the raw data rather than our JSON reshaping;
// device_type is the one addition on top, computed the same way as extractIdentifier() above,
// and only for tables that actually have a provider column. The InfluxDB token never leaves
// this server — callers hit this endpoint, not InfluxDB directly.
//
// All three formats are day-chunked (see splitIntoDayChunks). json/csv merge their day-chunks
// into one file the cheap way (string/array concatenation, see below) — but Parquet's binary
// footer (schema + row-group offsets) has to be correct for the whole file, so pasting multiple
// Parquet byte streams together the same way would produce a corrupt file, not a valid bigger
// one, without a real Arrow/Parquet-writing dependency to re-encode row groups. Rather than add
// that (or fall back to a single unchunked query with the file-scan cap exposure that implies —
// see README.md's "Caching" section for the history here), each day's Parquet bytes are kept as
// their own independently-valid file and bundled into a zip — exactly the "partitioned dataset"
// shape big-data tools (DuckDB, pandas+pyarrow, Polars, Spark) already read natively via a
// glob/multi-file dataset API, so nothing needs to be merged/re-encoded to use it. zipSync uses
// level:0 (store, no compression): Parquet's column chunks are already Snappy/Zstd-compressed
// internally, so compressing the zip container on top buys close to nothing and just costs CPU.
// Floor for bisecting a single write-dense day (see fetchParquetPieces below). Matches InfluxDB3
// Core's default gen1-duration (10m): a sub-window entirely inside one gen1 bucket overlaps the
// same set of files as any other sub-window inside that bucket (confirmed by direct inspection —
// see the write-fragmentation investigation in README/git history), so bisecting any finer can't
// reduce the file count a query has to scan. A failure that survives all the way down to this
// floor is treated as a real error rather than chunked away further.
const MIN_PARQUET_SUBCHUNK_MS = 10 * 60 * 1000;

function hhmm(ns: bigint): string {
  return new Date(Number(ns / 1_000_000n)).toISOString().slice(11, 16).replace(":", "");
}

type ParquetPiece = { startNs: bigint; endNs: bigint; bytes: Uint8Array };

// Fetches one day-chunk's Parquet bytes; on InfluxDB3's file-scan-limit error, bisects the range
// in half and retries each half independently (recursively), rather than letting one write-dense
// day abort the entire multi-day export — see toErrorResponse's "exceeding the file limit" case,
// and the production incident this generalizes from (a single day dense enough to trip the cap
// even under a narrow tag filter). Each half is cached under its own startNs/endNs exactly like a
// whole day already is (buildCacheKey takes the exact range, not "which day"), so this costs
// nothing extra on a subsequent identical request. Returns one piece per successful leaf; a day
// that didn't need bisecting returns exactly one piece spanning the whole requested range.
async function fetchParquetPieces(
  startNs: bigint,
  endNs: bigint,
  limit: number,
  selectExtra: string,
  buildQuery: (startNs: bigint, endNs: bigint, limit: number) => { sql: string; bindParams: Record<string, string> },
  exportCacheKey: (kind: string, startNs: bigint, endNs: bigint) => string,
  database: string,
  target: OrgTarget
): Promise<ParquetPiece[]> {
  const { sql, bindParams } = buildQuery(startNs, endNs, limit);
  const cacheKey = exportCacheKey(`export-parquet-day:devtype=${!!selectExtra}:limit=${limit}`, startNs, endNs);
  try {
    const bytes = await withDiskCache(cacheKey, rangeTtlMs(endNs), () => queryRaw(sql, "parquet", bindParams, database, target));
    return bytes.byteLength === 0 ? [] : [{ startNs, endNs, bytes: new Uint8Array(bytes) }];
  } catch (err) {
    const tooWide = err instanceof InfluxDBError && err.message.includes("exceeding the file limit");
    const spanMs = Number((endNs - startNs) / 1_000_000n);
    if (!tooWide || spanMs <= MIN_PARQUET_SUBCHUNK_MS) throw err;
    const midNs = startNs + (endNs - startNs) / 2n;
    const [first, second] = await Promise.all([
      fetchParquetPieces(startNs, midNs, limit, selectExtra, buildQuery, exportCacheKey, database, target),
      fetchParquetPieces(midNs, endNs, limit, selectExtra, buildQuery, exportCacheKey, database, target),
    ]);
    return [...first, ...second];
  }
}

async function queryExportBytes(
  resolved: Extract<ResolvedFilter, { ok: true }>,
  format: ExportFormat,
  database: string,
  target: OrgTarget
): Promise<Uint8Array> {
  const selectExtra = resolved.config.hasIdentifier ? `, ${DEVICE_TYPE_EXPR}` : "";
  const buildQuery = (startNs: bigint, endNs: bigint, limit: number) => ({
    sql: `
      SELECT *${selectExtra}
      FROM "${resolved.table}"
      WHERE ${resolved.whereTags} time >= arrow_cast(CAST($start AS BIGINT), 'Timestamp(Nanosecond, None)')
        AND time <= arrow_cast(CAST($end AS BIGINT), 'Timestamp(Nanosecond, None)')
      ORDER BY time DESC
      LIMIT ${limit}
    `,
    bindParams: { ...resolved.tagBindParams, start: startNs.toString(), end: endNs.toString() },
  });

  // Cached on the disk (/tmp) tier, not memory — export payloads are meant for bulk/historical
  // pulls and can be considerably larger than the typed-rows JSON the base route caches.
  const exportCacheKey = (kind: string, startNs: bigint, endNs: bigint) =>
    buildCacheKey({ host: target.host, database, table: resolved.table, kind, startNs, endNs, bindParams: resolved.tagBindParams });

  if (format === "parquet") {
    // Unlike json/csv (which decouple `limit` from the cache key by always fetching/caching a
    // fixed MAX_LIMIT-capped day and trimming afterward — see queryDayChunkTyped), each day's
    // Parquet file here is capped at the caller's own `resolved.limit` directly, baked into that
    // day's SQL/cache key. Trimming rows out of an already-fetched Parquet file would mean
    // parsing and re-encoding it — exactly the cost this design avoids — so there's no cheap way
    // to decouple. The practical effect: `limit` means "rows per day" for this format, not "rows
    // total" like every other endpoint — documented in README.md. Cache reuse across genuinely
    // identical repeated requests (the common hot-export case) is unaffected; only requests that
    // hit the same day with a *different* limit value miss each other, which is rarer.
    const entries: Record<string, [Uint8Array, { level: 0 }]> = {};
    for (const chunk of resolved.chunks) {
      const pieces = await fetchParquetPieces(chunk.startNs, chunk.endNs, resolved.limit, selectExtra, buildQuery, exportCacheKey, database, target);
      const day = new Date(Number(chunk.startNs / 1_000_000n)).toISOString().slice(0, 10);
      if (pieces.length === 1) {
        entries[`${resolved.table}_${day}.parquet`] = [pieces[0].bytes, { level: 0 }];
        continue;
      }
      // The day had to be bisected (see fetchParquetPieces) — each piece is its own independently
      // valid Parquet file (same reason whole days are never byte-merged, see the comment atop
      // queryExportBytes), so every piece gets its own zip entry named by its actual time range.
      for (const piece of pieces) {
        entries[`${resolved.table}_${day}_${hhmm(piece.startNs)}-${hhmm(piece.endNs)}.parquet`] = [piece.bytes, { level: 0 }];
      }
    }
    return zipSync(entries);
  }

  if (format === "json") {
    const merged: unknown[] = [];
    for (const chunk of resolved.chunks) {
      const remaining = resolved.limit - merged.length;
      if (remaining <= 0) break;
      const { sql, bindParams } = buildQuery(chunk.startNs, chunk.endNs, MAX_LIMIT);
      const cacheKey = exportCacheKey(`export-json:devtype=${!!selectExtra}`, chunk.startNs, chunk.endNs);
      const bytes = await withDiskCache(cacheKey, rangeTtlMs(chunk.endNs), () => queryRaw(sql, format, bindParams, database, target));
      const dayRows = JSON.parse(new TextDecoder().decode(bytes)) as unknown[];
      merged.push(...dayRows.slice(0, remaining));
    }
    return new TextEncoder().encode(JSON.stringify(merged));
  }

  // csv: keep the header line from the first (most recent) non-empty chunk; drop it from
  // every subsequent chunk so the merged file has exactly one header row.
  let header: string | undefined;
  const bodyLines: string[] = [];
  for (const chunk of resolved.chunks) {
    const remaining = resolved.limit - bodyLines.length;
    if (remaining <= 0) break;
    const { sql, bindParams } = buildQuery(chunk.startNs, chunk.endNs, MAX_LIMIT);
    const cacheKey = exportCacheKey(`export-csv:devtype=${!!selectExtra}`, chunk.startNs, chunk.endNs);
    const bytes = await withDiskCache(cacheKey, rangeTtlMs(chunk.endNs), () => queryRaw(sql, format, bindParams, database, target));
    const text = new TextDecoder().decode(bytes).replace(/\n+$/, "");
    if (!text) continue;
    const [chunkHeader, ...rows] = text.split("\n");
    if (header === undefined) header = chunkHeader;
    bodyLines.push(...rows.slice(0, remaining));
  }
  const csv = header === undefined ? "" : [header, ...bodyLines].join("\n") + "\n";
  return new TextEncoder().encode(csv);
}
