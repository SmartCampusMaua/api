// Loads the sensor/device/asset registries straight from parse-mqtt-to-influxdb3 — the sibling
// repo that's the actual source of truth for sensor_id assignment (see its own sensors.json/
// devices.json/assets.json). This is an interim measure: hardcoded relative paths only work when this repo
// and parse-mqtt-to-influxdb3 are checked out side by side on the same machine, which is true
// for local dev but not for a deployed image (there's no sibling checkout inside a Docker
// container). Once both files are made available to this service properly (a shared registry
// service, or mirrored/mounted the way infra.json is), swap the paths below for that — no
// other code here needs to change, since everything downstream only depends on `sensorRegistry`.
//
// Unlike config.ts's infra.json, a bad/missing file here does NOT crash the process: this
// registry is a convenience for the sensor-picker UI and the /:org/sensors endpoint, not
// something the core timeseries routes depend on to function. Missing it should degrade that one
// feature, not take the whole API down.

import { readFileSync } from "fs";

type SensorEntry = {
  sensor_id: string;
  sensor_type: string;
  device_id: string;
  device_index?: number;
  sensor_name?: string;
};
type DeviceEntry = {
  device_id: string;
  device_model: string;
  device_type: string;
  serial_number: string;
  dev_eui?: string;
  mac_address?: string;
};
// device_ids is authored on the asset side (one asset -> many devices), same as
// parse-mqtt-to-influxdb3's AssetEntry/GetAssetsMap — resolved below into a
// device_id -> asset lookup, the inverse direction SensorInfo actually needs.
// asset_name/asset_slug are optional since older entries may not have them yet — asset_slug is
// the stable, lowercase-kebab-case filter key (?assetSlug=), asset_name is a free-text display
// label with no filtering role, same split as sensor_id/sensor_name already have for sensors.
type AssetEntry = {
  asset_id: string;
  asset_name?: string;
  asset_slug?: string;
  device_ids?: string[];
};

export type SensorInfo = {
  sensor_id: string;
  sensor_type: string;
  sensor_name?: string;
  device_id: string;
  device_model?: string;
  dev_eui?: string;
  mac_address?: string;
  asset_id?: string;
  asset_name?: string;
  asset_slug?: string;
};

const SENSORS_FILE = process.env.SENSORS_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/sensors.json";
const DEVICES_FILE = process.env.DEVICES_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/devices.json";
const ASSETS_FILE = process.env.ASSETS_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/assets.json";
const REFRESH_SEC = Number(process.env.SENSORS_CONFIG_REFRESH_SEC) || 30;

function readJson<T>(path: string): T {
  let raw: string;
  try {
    raw = readFileSync(path, "utf-8");
  } catch (err) {
    throw new Error(`read ${path}: ${err instanceof Error ? err.message : err}`);
  }
  try {
    return JSON.parse(raw) as T;
  } catch (err) {
    throw new Error(`parse ${path}: ${err instanceof Error ? err.message : err}`);
  }
}

// Joins sensors.json's {sensor_id, sensor_type, device_id} against devices.json's device
// identity/model, so the picker UI (and the /sensors endpoint) can show something more useful
// than a bare device_id — device_model + dev_eui/mac_address, and sensor_name when the sensor
// registry has one (only ~2% of entries do — the multi-probe/multi-valve sensors that actually
// need a human label to disambiguate; everything else falls back to sensor_type as its label).
// asset_id comes from a second join (assets.json), same one-to-many resolution as
// parse-mqtt-to-influxdb3's GetAssetsMap: most devices aren't assigned to an asset yet, so this
// is undefined far more often than device_model/dev_eui/mac_address are.
function loadRegistry(): SensorInfo[] {
  const sensorsFile = readJson<{ sensors: SensorEntry[] }>(SENSORS_FILE);
  const devicesFile = readJson<{ devices: DeviceEntry[] }>(DEVICES_FILE);
  const assetsFile = readJson<{ assets: AssetEntry[] }>(ASSETS_FILE);
  if (!Array.isArray(sensorsFile.sensors)) throw new Error(`${SENSORS_FILE}: "sensors" must be an array`);
  if (!Array.isArray(devicesFile.devices)) throw new Error(`${DEVICES_FILE}: "devices" must be an array`);
  if (!Array.isArray(assetsFile.assets)) throw new Error(`${ASSETS_FILE}: "assets" must be an array`);

  const devicesById = new Map(devicesFile.devices.map((d) => [d.device_id, d]));
  const assetByDeviceId = new Map<string, AssetEntry>();
  for (const asset of assetsFile.assets) {
    for (const deviceId of asset.device_ids ?? []) {
      assetByDeviceId.set(deviceId, asset);
    }
  }
  return sensorsFile.sensors.map((s) => {
    const device = devicesById.get(s.device_id);
    const asset = assetByDeviceId.get(s.device_id);
    return {
      sensor_id: s.sensor_id,
      sensor_type: s.sensor_type,
      sensor_name: s.sensor_name,
      device_id: s.device_id,
      device_model: device?.device_model,
      dev_eui: device?.dev_eui,
      mac_address: device?.mac_address,
      asset_id: asset?.asset_id,
      asset_name: asset?.asset_name,
      asset_slug: asset?.asset_slug,
    };
  });
}

// Four lookups built in one pass over `list`, all keyed for O(1) access instead of an O(registry
// size) scan per row/request — sensorIdIndex gets called once per reading/tuple in a series
// response, and deviceIdIndex/assetIdIndex/assetSlugIndex back the /sensors endpoint's own
// ?deviceId=/?assetId=/?assetSlug= filters (see below), so all need to stay cheap even as the
// registry grows toward thousands of sensors. Rebuilt alongside `registry` on every load/reload,
// never mutated in place (same reasoning as config.ts's `orgs`: reassignment is atomic, so a
// reader never sees a half-rebuilt index).
let sensorIdIndex = new Map<string, string[]>(); // "${device_id}::${sensor_type}" -> sensor_id[]
let deviceIdIndex = new Map<string, SensorInfo[]>(); // device_id -> every sensor on that device
let assetIdIndex = new Map<string, SensorInfo[]>(); // asset_id -> every sensor on that asset's devices
let assetSlugIndex = new Map<string, SensorInfo[]>(); // asset_slug -> same, keyed by the human-typeable slug instead

function buildIndexes(list: SensorInfo[]) {
  const sensorIds = new Map<string, string[]>();
  const deviceIds = new Map<string, SensorInfo[]>();
  const assetIds = new Map<string, SensorInfo[]>();
  const assetSlugs = new Map<string, SensorInfo[]>();
  for (const s of list) {
    const sensorKey = `${s.device_id}::${s.sensor_type}`;
    const sensorMatches = sensorIds.get(sensorKey);
    if (sensorMatches) sensorMatches.push(s.sensor_id);
    else sensorIds.set(sensorKey, [s.sensor_id]);

    const deviceMatches = deviceIds.get(s.device_id);
    if (deviceMatches) deviceMatches.push(s);
    else deviceIds.set(s.device_id, [s]);

    if (s.asset_id) {
      const assetMatches = assetIds.get(s.asset_id);
      if (assetMatches) assetMatches.push(s);
      else assetIds.set(s.asset_id, [s]);
    }

    if (s.asset_slug) {
      const slugMatches = assetSlugs.get(s.asset_slug);
      if (slugMatches) slugMatches.push(s);
      else assetSlugs.set(s.asset_slug, [s]);
    }
  }
  return { sensorIds, deviceIds, assetIds, assetSlugs };
}

function applyIndexes(list: SensorInfo[]): void {
  const { sensorIds, deviceIds, assetIds, assetSlugs } = buildIndexes(list);
  sensorIdIndex = sensorIds;
  deviceIdIndex = deviceIds;
  assetIdIndex = assetIds;
  assetSlugIndex = assetSlugs;
}

// Resolves a row's sensor_id from the registry when InfluxDB's own sensor_id column doesn't have
// it yet (see the sensorId footnote in README.md) — device_id + sensor_type alone only
// unambiguously identifies a sensor for the common case of one sensor per type per device.
// Multi-probe devices (e.g. three soil_moisture_raw entries on one lnv3_sm3dl, disambiguated in
// the registry by device_index/sensor_name but *not* by anything sensor_data itself stores) have
// more than one candidate here, and there's no way to tell them apart from a plain InfluxDB row
// — returning any one of them would be a guess dressed up as data, so this returns undefined for
// that case rather than a plausible-looking wrong sensor_id.
export function resolveSensorId(deviceId: string, sensorType: string): string | undefined {
  const matches = sensorIdIndex.get(`${deviceId}::${sensorType}`);
  return matches?.length === 1 ? matches[0] : undefined;
}

// Backs the /sensors endpoint's ?deviceId=/?assetId=/?assetSlug= filters — an unknown id/slug is
// a normal empty result, not an error, same as any other tag filter elsewhere in this API.
export function sensorsByDeviceId(deviceId: string): SensorInfo[] {
  return deviceIdIndex.get(deviceId) ?? [];
}
export function sensorsByAssetId(assetId: string): SensorInfo[] {
  return assetIdIndex.get(assetId) ?? [];
}
// The main timeseries/export routes resolve ?assetSlug= into the equivalent asset_id(s) via this
// (see resolveAssetSlugFilter in routes/timeseries.ts) — InfluxDB itself only ever has asset_id as
// a real tag/column, never asset_slug, so the slug never reaches a SQL query directly.
export function sensorsByAssetSlug(assetSlug: string): SensorInfo[] {
  return assetSlugIndex.get(assetSlug) ?? [];
}

let registry: SensorInfo[] = [];
let loadError: string | undefined;

try {
  registry = loadRegistry();
  applyIndexes(registry);
  console.log(`[sensors] loaded ${SENSORS_FILE} + ${DEVICES_FILE} + ${ASSETS_FILE}: ${registry.length} sensors`);
} catch (err) {
  loadError = err instanceof Error ? err.message : String(err);
  console.error(`[sensors] initial load failed — the /sensors endpoint will report this error until it's fixed: ${loadError}`);
}

setInterval(() => {
  try {
    registry = loadRegistry();
    applyIndexes(registry);
    loadError = undefined;
  } catch (err) {
    loadError = err instanceof Error ? err.message : String(err);
    console.error(`[sensors] reload failed, keeping previous registry: ${loadError}`);
  }
}, REFRESH_SEC * 1000).unref();

export const sensorRegistry = {
  get list(): SensorInfo[] {
    return registry;
  },
  get error(): string | undefined {
    return loadError;
  },
};
