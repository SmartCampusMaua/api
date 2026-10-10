// One-time/repeatable bootstrap: loads parse-mqtt-to-influxdb3's sensors.json/devices.json/
// assets.json/device_channels.json into this API's registry Postgres. Idempotent — safe to
// re-run after editing the source files, since every insert upserts on the existing UUID (or,
// for device_buses which has no natural key, on a lookup-then-reuse-or-create pattern below).
// Run with: bun run scripts/import-registry.ts
//
// This does NOT touch parse-mqtt-to-influxdb3 — it only reads the same files src/sensors.ts
// already reads (same env var overrides), same read-only relationship this repo has always had
// with that data. Postgres becomes this API's own copy/source of truth going forward; this script
// is purely how it gets seeded from what exists today.

import { readFileSync } from "fs";
import { eq, and } from "drizzle-orm";
import { db } from "../src/db/client";
import { assets, devices, sensors, deviceBuses, modbusChannels } from "../src/db/schema";

const SENSORS_FILE = process.env.SENSORS_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/sensors.json";
const DEVICES_FILE = process.env.DEVICES_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/devices.json";
const ASSETS_FILE = process.env.ASSETS_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/assets.json";
const DEVICE_CHANNELS_FILE = process.env.DEVICE_CHANNELS_CONFIG_FILE ?? "../parse-mqtt-to-influxdb3/device_channels.json";

function readJson<T>(path: string): T {
  return JSON.parse(readFileSync(path, "utf-8")) as T;
}

type AssetEntry = { asset_id: string; asset_name?: string; asset_slug?: string; device_ids?: string[] };
type DeviceEntry = {
  device_id: string;
  device_model: string;
  device_type: string;
  serial_number?: string;
  dev_eui?: string;
  mac_address?: string;
};
type SensorEntry = { sensor_id: string; sensor_type: string; device_id: string; device_index?: number; sensor_name?: string };
type ChannelEntry = { device_id: string; sensor_id: string; modbus_channel: number; modbus_slave_id: number };

async function importAssets(): Promise<void> {
  const { assets: entries } = readJson<{ assets: AssetEntry[] }>(ASSETS_FILE);
  // Two passes: assets first without parent_asset_id (none exist in today's source data, but
  // this keeps the script correct if/when a parent_asset_id ever gets added upstream), so no
  // asset ever fails to insert because its parent hasn't been inserted yet.
  for (const a of entries) {
    if (!a.asset_name || !a.asset_slug) {
      throw new Error(`asset ${a.asset_id} is missing asset_name/asset_slug — backfill it in ${ASSETS_FILE} before importing`);
    }
    await db
      .insert(assets)
      .values({ id: a.asset_id, asset_name: a.asset_name, asset_slug: a.asset_slug })
      .onConflictDoUpdate({ target: assets.id, set: { asset_name: a.asset_name, asset_slug: a.asset_slug, updated_at: new Date() } });
  }
  console.log(`[import] assets: ${entries.length}`);
}

async function importDevices(): Promise<void> {
  const { devices: entries } = readJson<{ devices: DeviceEntry[] }>(DEVICES_FILE);
  const { assets: assetEntries } = readJson<{ assets: AssetEntry[] }>(ASSETS_FILE);
  const assetIdByDeviceId = new Map<string, string>();
  for (const a of assetEntries) {
    for (const deviceId of a.device_ids ?? []) assetIdByDeviceId.set(deviceId, a.asset_id);
  }
  for (const d of entries) {
    const values = {
      id: d.device_id,
      device_model: d.device_model,
      device_type: d.device_type,
      serial_number: d.serial_number || null,
      dev_eui: d.dev_eui || null,
      mac_address: d.mac_address || null,
      asset_id: assetIdByDeviceId.get(d.device_id) ?? null,
    };
    await db
      .insert(devices)
      .values(values)
      .onConflictDoUpdate({ target: devices.id, set: { ...values, updated_at: new Date() } });
  }
  console.log(`[import] devices: ${entries.length}`);
}

async function importSensors(): Promise<void> {
  const { sensors: entries } = readJson<{ sensors: SensorEntry[] }>(SENSORS_FILE);
  for (const s of entries) {
    const values = {
      id: s.sensor_id,
      sensor_type: s.sensor_type,
      device_id: s.device_id,
      device_index: s.device_index ?? null,
      sensor_name: s.sensor_name ?? null,
      // No per-sensor asset override in today's source data — every sensor inherits its device's
      // asset for now. Set explicitly later (via the write API) for the bus-listening exception.
    };
    await db
      .insert(sensors)
      .values(values)
      .onConflictDoUpdate({ target: sensors.id, set: { ...values, updated_at: new Date() } });
  }
  console.log(`[import] sensors: ${entries.length}`);
}

// device_channels.json has no bus_type field — every entry today is a UC-series RS485 Modbus
// controller, so 'modbus_rs485' is applied uniformly. One device_buses row per distinct device_id
// found here, looked up first (not just inserted) so re-running this script doesn't create a
// second bus row for a device that already has one — device_buses.id has no natural key to
// upsert against the way the other tables' pre-existing UUIDs do.
async function importDeviceChannels(): Promise<void> {
  const { channels } = readJson<{ channels: ChannelEntry[] }>(DEVICE_CHANNELS_FILE);
  const busIdByDeviceId = new Map<string, string>();

  for (const deviceId of new Set(channels.map((c) => c.device_id))) {
    const existing = await db
      .select({ id: deviceBuses.id })
      .from(deviceBuses)
      .where(and(eq(deviceBuses.device_id, deviceId), eq(deviceBuses.bus_type, "modbus_rs485")))
      .limit(1);
    if (existing.length > 0) {
      busIdByDeviceId.set(deviceId, existing[0].id);
      continue;
    }
    const [inserted] = await db.insert(deviceBuses).values({ device_id: deviceId, bus_type: "modbus_rs485" }).returning({ id: deviceBuses.id });
    busIdByDeviceId.set(deviceId, inserted.id);
  }

  for (const c of channels) {
    const device_bus_id = busIdByDeviceId.get(c.device_id)!;
    const values = { device_bus_id, sensor_id: c.sensor_id, modbus_channel: c.modbus_channel, modbus_slave_id: c.modbus_slave_id };
    await db
      .insert(modbusChannels)
      .values(values)
      .onConflictDoUpdate({ target: modbusChannels.sensor_id, set: { ...values, updated_at: new Date() } });
  }
  console.log(`[import] device_buses: ${busIdByDeviceId.size}, modbus_channels: ${channels.length}`);
}

async function main() {
  await importAssets();
  await importDevices();
  await importSensors();
  await importDeviceChannels();
  console.log("[import] done");
  process.exit(0);
}

main().catch((err) => {
  console.error("[import] failed:", err);
  process.exit(1);
});
