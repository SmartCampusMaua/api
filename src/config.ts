// Mirrors the devices.json registry pattern in parse-mqtt-to-influxdb3
// (cmd/parse-mqtt-to-influxdb3/main.go): a JSON file is the single source of truth for a
// registry that changes independently of a deploy, loaded once at startup (crashing on a
// bad/missing file, same as that service does for devices.json — better to fail loudly at
// boot than serve requests with no orgs configured), then re-read on a timer. A reload
// failure logs and keeps the previous good config rather than wiping it out or crashing —
// same reasoning as that service's "keep the last known-good" comment for its own registry.
//
// infra.json is broader than this API's own needs (it also holds an "mqtt" broker block, for
// other services) — this only ever reads the "influxdb" key, ignoring the rest, so other
// sections can be added/edited without this process caring.

import { readFileSync } from "fs";

export type OrgTarget = { host: string; token: string };

type OrgEntry = { org: string; host: string; token: string; allow?: boolean };
type InfraFile = { version?: string; influxdb: { orgs: OrgEntry[] } };

const CONFIG_FILE = process.env.INFRA_CONFIG_FILE ?? "infra.json";
const REFRESH_SEC = Number(process.env.INFRA_CONFIG_REFRESH_SEC) || 30;

function parseOrgsFile(path: string): Record<string, OrgTarget> {
  let raw: string;
  try {
    raw = readFileSync(path, "utf-8");
  } catch (err) {
    throw new Error(`read ${path}: ${err instanceof Error ? err.message : err}`);
  }

  let parsed: InfraFile;
  try {
    parsed = JSON.parse(raw);
  } catch (err) {
    throw new Error(`parse ${path}: ${err instanceof Error ? err.message : err}`);
  }

  if (!Array.isArray(parsed.influxdb?.orgs)) {
    throw new Error(`${path}: "influxdb.orgs" must be an array`);
  }

  const orgs: Record<string, OrgTarget> = {};
  for (const [i, entry] of parsed.influxdb.orgs.entries()) {
    if (entry.allow === false) continue;
    if (!entry.org) throw new Error(`${path}: influxdb.orgs[${i}] is missing "org"`);
    if (!entry.host) throw new Error(`${path}: influxdb.orgs[${i}] ("${entry.org}") is missing "host"`);
    if (!entry.token) throw new Error(`${path}: influxdb.orgs[${i}] ("${entry.org}") is missing "token"`);
    orgs[entry.org.toLowerCase()] = { host: entry.host, token: entry.token };
  }
  if (Object.keys(orgs).length === 0) {
    throw new Error(`${path}: no enabled orgs (empty "influxdb.orgs" array, or every entry has "allow": false)`);
  }
  return orgs;
}

// Fail fast on a bad initial file — same as devices.json's GetDevicesMap being called with
// `panic()` on error at startup in the Go service.
let orgs: Record<string, OrgTarget> = parseOrgsFile(CONFIG_FILE);
console.log(`[config] loaded ${CONFIG_FILE}: orgs=${Object.keys(orgs).join(", ")}`);

setInterval(() => {
  try {
    const next = parseOrgsFile(CONFIG_FILE);
    orgs = next;
    console.log(`[config] reloaded ${CONFIG_FILE}: orgs=${Object.keys(next).join(", ")}`);
  } catch (err) {
    console.error(`[config] reload of ${CONFIG_FILE} failed, keeping previous config: ${err}`);
  }
}, REFRESH_SEC * 1000).unref();

export const config = {
  influxdb: {
    // A getter, not a plain field: every access re-reads the current value of `orgs`, so a
    // background reload takes effect immediately without needing a restart. Reassigning
    // `orgs` wholesale (not mutating it in place) means readers never see a half-updated map
    // — JS variable reassignment is atomic, so no locking is needed the way the Go version
    // needs its sync.RWMutex.
    get orgs() {
      return orgs;
    },
  },
};
