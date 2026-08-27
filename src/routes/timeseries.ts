import { Elysia, t } from "elysia";
import { config, type OrgTarget } from "../config";
import { InfluxDBError, querySql, queryRaw, type InfluxRow } from "../influxdb";

// Each :org routes to a distinct InfluxDB instance (its own host + token) — this is the API's
// own multi-instance registry, configured via influxdb.json in src/config.ts. It's unrelated
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

// Per-table schema registry. Tables in this database aren't uniform: sensor_data has a
// single exclusive value per row (value_bool/value_float/value_int) plus LoRaWAN/IP identity
// columns; sensor_calibration has three concurrent numeric params and no identity columns;
// audit_iot.raw has a text blob and no sensor_type/value columns at all. Shaped output (the
// base route and /series) needs to know which shape it's dealing with; export/:format doesn't
// (SELECT * works for any of them), which is why `shaped: false` only disables the former.
// `tags` here are the camelCase URL names (keys of TAG_CAMEL_TO_SNAKE above).
type TableConfig = {
  tags: readonly string[];
  hasIdentifier: boolean;
  valueColumns: readonly string[];
  valueMode: "exclusive" | "multi" | "none";
  shaped: boolean;
};

const TABLES: Record<string, TableConfig> = {
  sensor_data: {
    tags: ["devEui", "deviceId", "deviceModel", "macAddress", "provider", "sensorType", "sensorId"],
    hasIdentifier: true,
    valueColumns: ["value_bool", "value_float", "value_int"],
    valueMode: "exclusive",
    shaped: true,
  },
  sensor_calibration: {
    tags: ["deviceId", "sensorType", "sensorId"],
    hasIdentifier: false,
    valueColumns: ["offset", "power", "scale"],
    valueMode: "multi",
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
// is what maps back to them. sensor_calibration has no standalone resource of its own — it's
// only reachable via "sensors"' auto-join (see autoJoinCalibration below) — but the underlying
// table (and its TABLES entry) still exists for that join's queries to use.
//
// A "vehicle" domain (vehicle_telemetry / audit_vehicle) would follow the same shape once
// there's real schema to build it from: both databases exist on the InfluxDB instance already
// (the ingestion pipeline opens clients for them), but neither has ever had a table written to
// it, and parse-mqtt-to-influxdb3's source has no write/record code for them yet either — so
// there's currently nothing to verify a table/column registry entry against.
type ResourceConfig = { database: string; table: string; autoJoinCalibration: boolean };

const DOMAIN_RESOURCES: Record<string, Record<string, ResourceConfig>> = {
  iot: {
    sensors: { database: "iot_sensors", table: "sensor_data", autoJoinCalibration: true },
    audit: { database: "audit_iot", table: "raw", autoJoinCalibration: false },
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

const EXPORT_FORMATS = {
  parquet: { contentType: "application/vnd.apache.parquet", extension: "parquet" },
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
    }
  | { ok: false; status: number; error: string };

function resolveFilter(table: string, query: TimeseriesQuery): ResolvedFilter {
  const config = TABLES[table];
  if (!config) {
    return {
      ok: false,
      status: 400,
      error: `Unsupported table "${table}". Supported: ${Object.keys(TABLES).join(", ")}`,
    };
  }

  // Tag filters are optional: with none given, the time bound below is the only
  // restriction, matching every device (and every sensor type, where applicable) in the window.
  const filters = config.tags.filter((tag) => query[tag] !== undefined);

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
  const tagFilters = filters.map((tag) => `"${TAG_CAMEL_TO_SNAKE[tag]}" = $${tag}`).join(" AND ");
  const whereTags = tagFilters ? `${tagFilters} AND` : "";
  const tagBindParams: Record<string, string> = {};
  for (const tag of filters) {
    const value = query[tag] as string;
    tagBindParams[tag] = tag === "sensorType" ? camelValueToSnake(value) : value;
  }

  return { ok: true, table, config, whereTags, tagBindParams, startNs: range.startNs, endNs: range.endNs, chunks, limit };
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

// "exclusive" tables (sensor_data) store one active value per row across several
// mutually-exclusive typed columns; pick whichever is non-null.
function extractValue(row: InfluxRow, valueColumns: readonly string[]): number | boolean | null {
  for (const col of valueColumns) {
    if (row[col] !== null && row[col] !== undefined) return row[col] as number | boolean;
  }
  return null;
}

// "multi" tables (sensor_calibration) store several concurrent named values per row; include
// them all as-is.
function extractValues(row: InfluxRow, valueColumns: readonly string[]): Record<string, unknown> {
  return Object.fromEntries(valueColumns.map((col) => [col, row[col]]));
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

// Shared by the base route and /series: same columns, same casts, same filters — they only
// differ in how the resulting rows get grouped/shaped into JSON afterward. Runs one query per
// day-chunk (see splitIntoDayChunks), newest chunk first, stopping as soon as the overall
// `limit` is satisfied so a request for recent data doesn't pay for querying older days at all.
async function queryTypedRows(resolved: Extract<ResolvedFilter, { ok: true }>, database: string, target: OrgTarget): Promise<InfluxRow[]> {
  const { config } = resolved;
  const identifierColumns = config.hasIdentifier ? ["dev_eui", "mac_address", "provider"] : [];
  const columns = ["device_id", "sensor_id", ...identifierColumns, "sensor_type", ...config.valueColumns];
  // Columns are double-quoted since a couple (e.g. sensor_calibration's "offset") collide with
  // reserved SQL keywords otherwise.
  const selectCols = columns.map((c) => `"${c}"`).join(", ");

  const rows: InfluxRow[] = [];
  for (const chunk of resolved.chunks) {
    const remaining = resolved.limit - rows.length;
    if (remaining <= 0) break;

    // time is cast to VARCHAR (not just BIGINT) so InfluxDB emits it as a JSON string.
    // Nanosecond epoch values exceed Number.MAX_SAFE_INTEGER, so if it came back as a JSON
    // number it would already be rounded by the time our JSON parser touches it.
    const sql = `
      SELECT CAST(CAST(time AS BIGINT) AS VARCHAR) AS time, ${selectCols}
      FROM "${resolved.table}"
      WHERE ${resolved.whereTags} time >= arrow_cast(CAST($start AS BIGINT), 'Timestamp(Nanosecond, None)')
        AND time <= arrow_cast(CAST($end AS BIGINT), 'Timestamp(Nanosecond, None)')
      ORDER BY time DESC
      LIMIT ${remaining}
    `;
    const bindParams = { ...resolved.tagBindParams, start: chunk.startNs.toString(), end: chunk.endNs.toString() };
    rows.push(...(await querySql(sql, bindParams, database, target)));
  }
  return rows;
}

// ─────────────────────────────────────────────────────────────────────────────
// Calibration join (sensor_data + sensor_calibration)
// ─────────────────────────────────────────────────────────────────────────────
// sensor_calibration has no time alignment with sensor_data — it's written rarely, whenever a
// sensor is (re)calibrated — so "the calibration for this reading" means the most recent
// sensor_calibration row for that sensor_id at or before the reading's time. InfluxDB3 doesn't
// support this as a single SQL join yet (ASOF JOIN is recognized by the SQL parser but rejected
// by the query planner as "not implemented" — verified against the live server), so it's done
// here: one extra query for every relevant sensor's calibration history, then an in-memory
// as-of lookup per reading.
//
// Keyed by sensor_id, not (device_id, sensor_type): a device can carry several sensors sharing
// one sensor_type (e.g. three soil_moisture_raw probes on one lnv3_sm3dl, disambiguated by
// device_index — see parse-mqtt-to-influxdb3's README), each with its own calibration curve, so
// device_id+sensor_type alone can no longer identify which curve a row belongs to. A reading
// with no sensor_id (not yet resolved to a sensors.json entry upstream) simply gets no
// calibration — same as a sensor with none configured.

type CalibrationRecord = { timeNs: bigint; offset: unknown; power: unknown; scale: unknown };

async function fetchCalibrationLookup(
  sensorIds: string[],
  asOfNs: bigint,
  database: string,
  target: OrgTarget
): Promise<Map<string, CalibrationRecord[]>> {
  const valueColumns = TABLES.sensor_calibration.valueColumns;
  const ids = sensorIds.filter((id) => id); // drop empty/unresolved sensor_id values
  const lookup = new Map<string, CalibrationRecord[]>();
  if (ids.length === 0) return lookup;

  const bindParams: Record<string, string> = { end: asOfNs.toString() };
  const idPlaceholders = ids.map((id, i) => {
    bindParams[`s${i}`] = id;
    return `$s${i}`;
  });

  const sql = `
    SELECT CAST(CAST(time AS BIGINT) AS VARCHAR) AS time, sensor_id, ${valueColumns.map((c) => `"${c}"`).join(", ")}
    FROM sensor_calibration
    WHERE sensor_id IN (${idPlaceholders.join(", ")})
      AND time <= arrow_cast(CAST($end AS BIGINT), 'Timestamp(Nanosecond, None)')
    ORDER BY time DESC
  `;
  const rows = await querySql(sql, bindParams, database, target);

  for (const row of rows) {
    const key = row.sensor_id as string;
    const list = lookup.get(key) ?? [];
    // Already newest-first per key, since the SQL is ORDER BY time DESC and rows for the same
    // key appear in that same relative order.
    list.push({ timeNs: BigInt(row.time as string), offset: row.offset, power: row.power, scale: row.scale });
    lookup.set(key, list);
  }
  return lookup;
}

function findApplicableCalibration(
  lookup: Map<string, CalibrationRecord[]>,
  sensorId: string,
  timeNs: bigint
): CalibrationRecord | undefined {
  if (!sensorId) return undefined;
  return lookup.get(sensorId)?.find((c) => c.timeNs <= timeNs);
}

// Calibration changes rarely — often not at all within a single query's window — so repeating
// identical offset/power/scale on every reading is wasted payload. Instead, calibration fields
// are only emitted on the *first* reading (in response order, i.e. newest-first) where a given
// calibration record applies, and omitted on every subsequent reading for that same
// (device, sensor_type) stream as long as it's still the same record — the run-length-encoding
// equivalent for this "mostly constant, occasionally steps to a new value" shape. A reading's
// own calibration state is "whichever explicit calibration most recently appeared at or before
// it in the array" — so this only makes sense read sequentially, not accessed at random.
// `tracker` is one Map per request/endpoint call, keyed by sensor_id (falling back to
// `${deviceId}::${sensorType}` for a reading with no sensor_id yet), holding the identity (a
// calibration record's own timeNs, or "none") last emitted for that key — identity is the
// calibration row's own timestamp rather than its values, so two genuinely distinct calibration
// events that happen to share identical numbers still count as a real change (unlikely in this
// data, but not assumed away).
function calibrationIdentity(calibration: CalibrationRecord | undefined): string {
  return calibration ? calibration.timeNs.toString() : "none";
}

function calibrationChanged(tracker: Map<string, string>, key: string, calibration: CalibrationRecord | undefined): boolean {
  const identity = calibrationIdentity(calibration);
  const changed = tracker.get(key) !== identity;
  tracker.set(key, identity);
  return changed;
}

const filterQuerySchema = t.Object({
  interval: t.Optional(t.String()),
  startTime: t.Optional(t.String()),
  endTime: t.Optional(t.String()),
  limit: t.Optional(t.String()),
  ...Object.fromEntries(ALL_TAGS.map((tag) => [tag, t.Optional(t.String())])),
});

const NOT_SHAPED_ERROR = (table: string) =>
  `Table "${table}" has no sensor_type/value columns to shape into readings or series. Use /export/:format instead.`;

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
      const { database, table, autoJoinCalibration } = resourceLookup.resource;

      const resolved = resolveFilter(table, query);
      if (!resolved.ok) {
        set.status = resolved.status;
        return { error: resolved.error };
      }
      if (!resolved.config.shaped) {
        set.status = 400;
        return { error: NOT_SHAPED_ERROR(table) };
      }

      try {
        const rows = await queryTypedRows(resolved, database, org.target);
        const { config } = resolved;

        const calibLookup = autoJoinCalibration && rows.length > 0
          ? await fetchCalibrationLookup([...new Set(rows.map((r) => r.sensor_id as string))], resolved.endNs, database, org.target)
          : undefined;
        const calibTracker = new Map<string, string>();

        const devices = new Map<string, { device_id: string; identifier: Record<string, string>; readings: Record<string, unknown>[] }>();
        for (const row of rows) {
          const deviceId = row.device_id as string;
          const sensorId = (row.sensor_id as string) || undefined;
          let device = devices.get(deviceId);
          if (!device) {
            device = { device_id: deviceId, identifier: extractIdentifier(row, config), readings: [] };
            devices.set(deviceId, device);
          }
          // Calibration fields are included only on the first reading (newest-first) where a
          // given calibration record applies, and omitted afterward for as long as it's still
          // the same record — see calibrationChanged() above. Combined with "omitted, not
          // null-filled, when no calibration exists at all", omission covers both "same as
          // previous reading" and "no calibration ever" — a reader walking the array in order
          // (carrying forward the last explicit value) reconstructs the correct state either way.
          const calibration = calibLookup && sensorId ? findApplicableCalibration(calibLookup, sensorId, BigInt(row.time as string)) : undefined;
          const calibrationChangedHere = calibrationChanged(calibTracker, sensorId ?? `${deviceId}::${row.sensor_type}`, calibration);
          const calibrationFields =
            calibration && calibrationChangedHere
              ? { offset: calibration.offset, power: calibration.power, scale: calibration.scale }
              : {};
          // sensor_id is included on every reading (not just when calibration changes) — unlike
          // calibration, which is genuinely redundant to repeat, sensor_id is the only thing that
          // tells apart multiple physical sensors sharing one sensor_type on the same device
          // (e.g. lnv3_sm3dl's three soil_moisture_raw probes) — omitting it would make those
          // indistinguishable in this route's per-row shape.
          device.readings.push(
            config.valueMode === "exclusive"
              ? { time: row.time, sensor_id: sensorId, [row.sensor_type as string]: extractValue(row, config.valueColumns), ...calibrationFields }
              : { time: row.time, sensor_id: sensorId, sensor_type: row.sensor_type, ...extractValues(row, config.valueColumns) }
          );
        }

        return [...devices.values()].map((device) => ({
          device_id: device.device_id,
          ...device.identifier,
          readings: device.readings,
        }));
      } catch (err) {
        return toErrorResponse(err, set, resolved.config.tags);
      }
    },
    {
      params: t.Object({ org: t.String(), domain: t.String(), resource: t.String() }),
      query: filterQuerySchema,
    }
  )
  .get(
    "/api/timeseries/v0.4/:org/:domain/:resource/series",
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
      const { database, table, autoJoinCalibration } = resourceLookup.resource;

      const resolved = resolveFilter(table, query);
      if (!resolved.ok) {
        set.status = resolved.status;
        return { error: resolved.error };
      }
      if (!resolved.config.shaped) {
        set.status = 400;
        return { error: NOT_SHAPED_ERROR(table) };
      }

      try {
        const rows = await queryTypedRows(resolved, database, org.target);
        const { config } = resolved;

        const calibLookup = autoJoinCalibration && rows.length > 0
          ? await fetchCalibrationLookup([...new Set(rows.map((r) => r.sensor_id as string))], resolved.endNs, database, org.target)
          : undefined;
        const calibTracker = new Map<string, string>();

        // One series per (device, sensor_type, sensor_id) — NOT just (device, sensor_type): a
        // device can carry several sensors sharing one sensor_type (e.g. lnv3_sm3dl's three
        // soil_moisture_raw probes, disambiguated by device_index upstream), and each is its own
        // independent time series. Grouping by (device, sensor_type) alone would silently
        // interleave multiple physical sensors' readings into one array. sensor_id is included
        // in the grouping key even when empty (unresolved sensors all collapse into one group in
        // that case, same degraded-but-not-wrong behavior as before sensor_id existed).
        //
        // "exclusive" tables (sensor_data) get [time, value], extended to [time, value, offset,
        // power, scale] only on the first tuple (newest-first) where a given calibration record
        // applies — omitted on subsequent tuples for as long as it's still the same record, same
        // run-length reasoning as the base route's readings (see calibrationChanged() above) — so
        // tuple length can vary within one response; check length rather than assuming a fixed
        // arity, and carry forward the last 3-element tuple's values for any 2-element tuple in
        // between. "multi" tables (sensor_calibration) get [time, ...valueColumns] in that
        // column's fixed order, unaffected by calibration joining (it doesn't apply to that table).
        type SeriesGroup = {
          device_id: string;
          sensor_id?: string;
          identifier: Record<string, string>;
          sensor_type: string;
          series: unknown[][];
        };
        const seriesByKey = new Map<string, SeriesGroup>();
        for (const row of rows) {
          const deviceId = row.device_id as string;
          const sensorType = row.sensor_type as string;
          const sensorId = (row.sensor_id as string) || undefined;
          const key = `${deviceId}::${sensorType}::${sensorId ?? ""}`;
          let group = seriesByKey.get(key);
          if (!group) {
            group = { device_id: deviceId, sensor_id: sensorId, identifier: extractIdentifier(row, config), sensor_type: sensorType, series: [] };
            seriesByKey.set(key, group);
          }
          if (config.valueMode === "exclusive") {
            const tuple: unknown[] = [row.time, extractValue(row, config.valueColumns)];
            const calibration = calibLookup && sensorId ? findApplicableCalibration(calibLookup, sensorId, BigInt(row.time as string)) : undefined;
            const calibrationChangedHere = calibrationChanged(calibTracker, key, calibration);
            if (calibration && calibrationChangedHere) tuple.push(calibration.offset, calibration.power, calibration.scale);
            group.series.push(tuple);
          } else {
            group.series.push([row.time, ...config.valueColumns.map((col) => row[col])]);
          }
        }

        return [...seriesByKey.values()].map((group) => ({
          device_id: group.device_id,
          sensor_id: group.sensor_id,
          ...group.identifier,
          sensor_type: group.sensor_type,
          series: group.series,
        }));
      } catch (err) {
        return toErrorResponse(err, set, resolved.config.tags);
      }
    },
    {
      params: t.Object({ org: t.String(), domain: t.String(), resource: t.String() }),
      query: filterQuerySchema,
    }
  )
  .get(
    "/api/timeseries/v0.4/:org/:domain/:resource/export/:format",
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

      if (!(params.format in EXPORT_FORMATS)) {
        set.status = 400;
        return {
          error: `Unsupported export format "${params.format}". Supported: ${Object.keys(EXPORT_FORMATS).join(", ")}`,
        };
      }
      const format = params.format as ExportFormat;

      const resolved = resolveFilter(table, query);
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
      params: t.Object({ org: t.String(), domain: t.String(), resource: t.String(), format: t.String() }),
      query: filterQuerySchema,
    }
  );

// SELECT * hands back the table's native columns and types as InfluxDB stores them, for tools
// (pandas/pyarrow, DuckDB, etc.) that want the raw data rather than our JSON reshaping;
// device_type is the one addition on top, computed the same way as extractIdentifier() above,
// and only for tables that actually have a provider column. The InfluxDB token never leaves
// this server — callers hit this endpoint, not InfluxDB directly.
//
// json and csv are day-chunked and merged the same way queryTypedRows is (see there for why).
// parquet is NOT chunked: unlike a JSON array or CSV rows, multiple Parquet byte streams can't
// be concatenated into one valid file without re-encoding at the row-group level, which this
// doesn't implement — so a parquet export still runs as a single query over the full range,
// with the same file-scan-limit exposure the other formats had before chunking existed. Narrow
// the range or add a tag filter if a wide parquet export hits that cap.
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

  if (format === "parquet") {
    const { sql, bindParams } = buildQuery(resolved.startNs, resolved.endNs, resolved.limit);
    return new Uint8Array(await queryRaw(sql, format, bindParams, database, target));
  }

  if (format === "json") {
    const merged: unknown[] = [];
    for (const chunk of resolved.chunks) {
      const remaining = resolved.limit - merged.length;
      if (remaining <= 0) break;
      const { sql, bindParams } = buildQuery(chunk.startNs, chunk.endNs, remaining);
      const bytes = await queryRaw(sql, format, bindParams, database, target);
      merged.push(...(JSON.parse(new TextDecoder().decode(bytes)) as unknown[]));
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
    const { sql, bindParams } = buildQuery(chunk.startNs, chunk.endNs, remaining);
    const bytes = await queryRaw(sql, format, bindParams, database, target);
    const text = new TextDecoder().decode(bytes).replace(/\n+$/, "");
    if (!text) continue;
    const [chunkHeader, ...rows] = text.split("\n");
    if (header === undefined) header = chunkHeader;
    bodyLines.push(...rows);
  }
  const csv = header === undefined ? "" : [header, ...bodyLines].join("\n") + "\n";
  return new TextEncoder().encode(csv);
}
