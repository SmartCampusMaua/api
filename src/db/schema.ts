// Registry schema — devices/assets/sensors/bus routing, migrating off the sibling repo's
// sensors.json/devices.json/assets.json files. See README.md's "Registry (Postgres)" section for
// the full design rationale (why device_buses sits above modbus_channels, why the asset override
// lives on sensors rather than modbus_channels, etc.) — this file is just the resulting shape.
//
// Column properties are named snake_case (matching the actual DB column, not the more common
// Drizzle convention of a camelCase TS property mapped to a snake_case column) because Drizzle
// serializes query results using these TS property names verbatim — and this project's own
// documented convention (see README.md's "Naming convention") is that JSON response bodies are
// snake_case throughout, matching InfluxDB's own column names. Table *variable* names
// (deviceBuses, modbusChannels below) stay camelCase since those are just TS import identifiers,
// never serialized.
//
// id columns on devices/assets/sensors reuse the UUIDv7 values already assigned by
// parse-mqtt-to-influxdb3's registries (the same ids already used throughout the timeseries API
// and InfluxDB itself) rather than generating new surrogate keys — there is exactly one identity
// per entity, never a dual id/external-id pair to keep in sync. device_buses/modbus_channels have
// no pre-existing external id, so those get a DB-generated UUID.

import { pgTable, uuid, text, boolean, jsonb, integer, timestamp, unique, type AnyPgColumn } from "drizzle-orm/pg-core";

const timestamps = {
  created_at: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updated_at: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
};

// parent_asset_id enables an asset hierarchy (e.g. "Pump Station" containing "Pump A"/"Pump B") —
// used so a bus-listening device can attach to one shared parent while individual sensors still
// resolve to their own specific child asset via sensors.asset_id (see below). Hierarchy-aware
// queries (roll up a parent's descendants) are intentionally not built yet — just the column.
export const assets = pgTable("assets", {
  id: uuid("id").primaryKey(),
  asset_name: text("asset_name").notNull(),
  asset_slug: text("asset_slug").notNull().unique(),
  asset_coords: jsonb("asset_coords"),
  parent_asset_id: uuid("parent_asset_id").references((): AnyPgColumn => assets.id),
  ...timestamps,
});

// asset_id is the device's default/"home" asset — where it's physically mounted. A bus-listening
// device (e.g. a Modbus RS485 controller also relaying readings for other physical assets on the
// same bus) sets this to wherever it actually sits; sensors for anything else it relays set their
// own sensors.asset_id override instead of this ever becoming a many-to-many relationship.
export const devices = pgTable("devices", {
  id: uuid("id").primaryKey(),
  device_model: text("device_model").notNull(),
  device_type: text("device_type").notNull(), // "lorawan" | "ip"
  serial_number: text("serial_number"),
  dev_eui: text("dev_eui"),
  mac_address: text("mac_address"),
  battery_voltage_max: integer("battery_voltage_max"),
  battery_voltage_min: integer("battery_voltage_min"),
  probe_range_m: integer("probe_range_m"),
  allow: boolean("allow").notNull().default(true),
  asset_id: uuid("asset_id").references(() => assets.id),
  ...timestamps,
});

// asset_id here is an override, null by default (meaning "same as this sensor's device"). Only
// set explicitly for the exception case — a specific channel on a bus-listening device actually
// belongs to a different physical asset than the device's own home. Effective asset for any
// sensor is `sensor.asset_id ?? device.asset_id`, computed wherever the registry is read.
export const sensors = pgTable("sensors", {
  id: uuid("id").primaryKey(),
  sensor_type: text("sensor_type").notNull(),
  device_id: uuid("device_id").notNull().references(() => devices.id),
  device_index: integer("device_index"),
  sensor_name: text("sensor_name"),
  asset_id: uuid("asset_id").references(() => assets.id),
  ...timestamps,
});

// One device can have more than one bus simultaneously (e.g. a UC300's RS485 and RS232 are
// independently-addressed Modbus networks — slave 1 on one is unrelated to slave 1 on the other),
// so bus identity has to live in its own table rather than a single devices.bus_protocol column.
// bus_type stays plain text (not a DB enum) for the same reason sensor_type/device_model do —
// avoiding a Postgres enum that has to be kept in lockstep with the Go ingestion pipeline's own
// definitions in a separate repo.
export const deviceBuses = pgTable("device_buses", {
  id: uuid("id").primaryKey().defaultRandom(),
  device_id: uuid("device_id").notNull().references(() => devices.id),
  bus_type: text("bus_type").notNull(), // "modbus_rs485" | "modbus_rs232" | ...
  label: text("label"),
  ...timestamps,
});

// Routing only (channel/slave -> sensor_id) — never asset attribution, which lives on `sensors`
// so it uniformly covers non-Modbus sensors too rather than only ones with a channel row.
export const modbusChannels = pgTable(
  "modbus_channels",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    device_bus_id: uuid("device_bus_id").notNull().references(() => deviceBuses.id),
    sensor_id: uuid("sensor_id").notNull().unique().references(() => sensors.id),
    modbus_channel: integer("modbus_channel").notNull(),
    modbus_slave_id: integer("modbus_slave_id").notNull(),
    ...timestamps,
  },
  (table) => [unique().on(table.device_bus_id, table.modbus_channel, table.modbus_slave_id)]
);
