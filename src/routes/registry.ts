// Device/asset/sensor/bus registry — CRUD on top of the Postgres tables in src/db/schema.ts,
// replacing parse-mqtt-to-influxdb3's sensors.json/devices.json/assets.json/device_channels.json
// as the source of truth (see scripts/import-registry.ts for the one-time bootstrap from those
// files). Reads are open, same posture as the rest of this API; every write requires the shared
// bearer token from src/registryAuth.ts — a stopgap ahead of real per-user auth, not a permanent
// design.
//
// Request and response bodies are snake_case throughout (asset_name, device_id, ...), matching
// this project's own documented convention for JSON bodies (see README.md's "Naming convention")
// and the schema's own column-matching TS property names — there's no camelCase/snake_case
// translation layer anywhere in this file.
//
// Endpoints return the raw stored rows, not computed/joined values (e.g. a sensor's *effective*
// asset — `sensor.asset_id ?? device.asset_id` — is a concern for whoever consumes this data for
// filtering/display, not something baked into the registry's own CRUD surface).

import { Elysia, t } from "elysia";
import { eq, and } from "drizzle-orm";
import { db } from "../db/client";
import { assets, devices, sensors, deviceBuses, modbusChannels } from "../db/schema";
import { verifyWriteToken } from "../registryAuth";

const ASSET_SLUG_PATTERN = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const DEVICE_TYPES = ["lorawan", "ip"] as const;

// Postgres error codes: 23505 = unique_violation, 23503 = foreign_key_violation. Both are
// expected, user-correctable input errors (a duplicate slug, a reference to an id that doesn't
// exist) — worth a clean 4xx instead of a raw 500 with a driver-specific error message.
//
// Drizzle wraps the real driver error in a DrizzleQueryError; Bun's native SQL client (see
// src/db/client.ts) then nests the actual Postgres error under `.cause`, with the SQLSTATE at
// `.cause.errno` — *not* `.cause.code`, which instead holds a generic Bun-specific string
// ("ERR_POSTGRES_SERVER_ERROR") that's the same for every server-side Postgres error regardless
// of which constraint failed. Confirmed by triggering a real unique-violation and logging the
// full error shape rather than guessing.
function pgErrorCode(err: unknown): string | undefined {
  if (typeof err !== "object" || err === null || !("cause" in err)) return undefined;
  const cause = (err as { cause?: unknown }).cause;
  if (typeof cause !== "object" || cause === null || !("errno" in cause)) return undefined;
  return (cause as { errno?: string }).errno;
}

function requireWriteToken(headers: Record<string, string | undefined>, set: { status?: number | string }): { error: string } | undefined {
  if (!verifyWriteToken(headers.authorization)) {
    set.status = 401;
    return { error: "Missing or invalid write token — 'Authorization: Bearer <token>' is required for this route." };
  }
  return undefined;
}

const idParam = t.Object({ id: t.String() });

export const registry = new Elysia()
  // ── assets ──────────────────────────────────────────────────────────────────────────────
  .get("/api/registry/v0.1/assets", () => db.select().from(assets))
  .get(
    "/api/registry/v0.1/assets/:id",
    async ({ params, set }) => {
      const [row] = await db.select().from(assets).where(eq(assets.id, params.id));
      if (!row) {
        set.status = 404;
        return { error: "asset not found" };
      }
      return row;
    },
    { params: idParam }
  )
  .post(
    "/api/registry/v0.1/assets",
    async ({ body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      if (!ASSET_SLUG_PATTERN.test(body.asset_slug)) {
        set.status = 400;
        return { error: "asset_slug must be lowercase letters, digits, and hyphens only (e.g. 'pump-station-3')" };
      }
      try {
        const [row] = await db
          .insert(assets)
          .values({
            id: crypto.randomUUID(),
            asset_name: body.asset_name,
            asset_slug: body.asset_slug,
            asset_coords: body.asset_coords ?? null,
            parent_asset_id: body.parent_asset_id ?? null,
          })
          .returning();
        set.status = 201;
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23505") {
          set.status = 409;
          return { error: `asset_slug "${body.asset_slug}" already exists` };
        }
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "parent_asset_id does not reference an existing asset" };
        }
        throw err;
      }
    },
    {
      body: t.Object({
        asset_name: t.String(),
        asset_slug: t.String(),
        asset_coords: t.Optional(t.Unknown()),
        parent_asset_id: t.Optional(t.String()),
      }),
    }
  )
  .patch(
    "/api/registry/v0.1/assets/:id",
    async ({ params, body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      if (body.asset_slug !== undefined && !ASSET_SLUG_PATTERN.test(body.asset_slug)) {
        set.status = 400;
        return { error: "asset_slug must be lowercase letters, digits, and hyphens only" };
      }
      try {
        const [row] = await db
          .update(assets)
          .set({ ...body, updated_at: new Date() })
          .where(eq(assets.id, params.id))
          .returning();
        if (!row) {
          set.status = 404;
          return { error: "asset not found" };
        }
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23505") {
          set.status = 409;
          return { error: `asset_slug "${body.asset_slug}" already exists` };
        }
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "parent_asset_id does not reference an existing asset" };
        }
        throw err;
      }
    },
    {
      params: idParam,
      body: t.Object({
        asset_name: t.Optional(t.String()),
        asset_slug: t.Optional(t.String()),
        asset_coords: t.Optional(t.Unknown()),
        parent_asset_id: t.Optional(t.String()),
      }),
    }
  )

  // ── devices ─────────────────────────────────────────────────────────────────────────────
  .get("/api/registry/v0.1/devices", () => db.select().from(devices))
  .get(
    "/api/registry/v0.1/devices/:id",
    async ({ params, set }) => {
      const [row] = await db.select().from(devices).where(eq(devices.id, params.id));
      if (!row) {
        set.status = 404;
        return { error: "device not found" };
      }
      return row;
    },
    { params: idParam }
  )
  .post(
    "/api/registry/v0.1/devices",
    async ({ body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      if (!DEVICE_TYPES.includes(body.device_type as (typeof DEVICE_TYPES)[number])) {
        set.status = 400;
        return { error: `device_type must be one of: ${DEVICE_TYPES.join(", ")}` };
      }
      try {
        const [row] = await db
          .insert(devices)
          .values({ id: crypto.randomUUID(), ...body })
          .returning();
        set.status = 201;
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "asset_id does not reference an existing asset" };
        }
        throw err;
      }
    },
    {
      body: t.Object({
        device_model: t.String(),
        device_type: t.String(),
        serial_number: t.Optional(t.String()),
        dev_eui: t.Optional(t.String()),
        mac_address: t.Optional(t.String()),
        asset_id: t.Optional(t.String()),
      }),
    }
  )
  // Attaching/detaching a device to/from an asset is just this: set or null out asset_id. Same
  // route the "attach from a device list" UI flow uses, not a separate attach/detach endpoint.
  .patch(
    "/api/registry/v0.1/devices/:id",
    async ({ params, body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      if (body.device_type !== undefined && !DEVICE_TYPES.includes(body.device_type as (typeof DEVICE_TYPES)[number])) {
        set.status = 400;
        return { error: `device_type must be one of: ${DEVICE_TYPES.join(", ")}` };
      }
      try {
        const [row] = await db
          .update(devices)
          .set({ ...body, updated_at: new Date() })
          .where(eq(devices.id, params.id))
          .returning();
        if (!row) {
          set.status = 404;
          return { error: "device not found" };
        }
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "asset_id does not reference an existing asset" };
        }
        throw err;
      }
    },
    {
      params: idParam,
      body: t.Object({
        device_model: t.Optional(t.String()),
        device_type: t.Optional(t.String()),
        serial_number: t.Optional(t.String()),
        dev_eui: t.Optional(t.String()),
        mac_address: t.Optional(t.String()),
        allow: t.Optional(t.Boolean()),
        asset_id: t.Optional(t.Nullable(t.String())),
      }),
    }
  )

  // ── sensors ─────────────────────────────────────────────────────────────────────────────
  .get("/api/registry/v0.1/sensors", () => db.select().from(sensors))
  .get(
    "/api/registry/v0.1/sensors/:id",
    async ({ params, set }) => {
      const [row] = await db.select().from(sensors).where(eq(sensors.id, params.id));
      if (!row) {
        set.status = 404;
        return { error: "sensor not found" };
      }
      return row;
    },
    { params: idParam }
  )
  .post(
    "/api/registry/v0.1/sensors",
    async ({ body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      try {
        const [row] = await db
          .insert(sensors)
          .values({ id: crypto.randomUUID(), ...body })
          .returning();
        set.status = 201;
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "device_id or asset_id does not reference an existing row" };
        }
        throw err;
      }
    },
    {
      body: t.Object({
        sensor_type: t.String(),
        device_id: t.String(),
        device_index: t.Optional(t.Number()),
        sensor_name: t.Optional(t.String()),
        // Override only — leave unset to inherit the sensor's device's own asset. See
        // src/db/schema.ts's comment on sensors.asset_id for the bus-listening-device rationale.
        asset_id: t.Optional(t.String()),
      }),
    }
  )
  .patch(
    "/api/registry/v0.1/sensors/:id",
    async ({ params, body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      try {
        const [row] = await db
          .update(sensors)
          .set({ ...body, updated_at: new Date() })
          .where(eq(sensors.id, params.id))
          .returning();
        if (!row) {
          set.status = 404;
          return { error: "sensor not found" };
        }
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "device_id or asset_id does not reference an existing row" };
        }
        throw err;
      }
    },
    {
      params: idParam,
      body: t.Object({
        sensor_type: t.Optional(t.String()),
        device_id: t.Optional(t.String()),
        device_index: t.Optional(t.Number()),
        sensor_name: t.Optional(t.String()),
        // Explicit null clears the override (back to inheriting the device's asset) — distinct
        // from omitting the field entirely, which leaves whatever was already set untouched.
        asset_id: t.Optional(t.Nullable(t.String())),
      }),
    }
  )

  // ── modbus channels ─────────────────────────────────────────────────────────────────────
  // Flat, joined view — callers think in terms of "device X, channel Y, slave Z", not the
  // device_buses indirection underneath (see src/db/schema.ts for why that table exists).
  .get("/api/registry/v0.1/modbus-channels", () =>
    db
      .select({
        id: modbusChannels.id,
        device_id: deviceBuses.device_id,
        bus_type: deviceBuses.bus_type,
        sensor_id: modbusChannels.sensor_id,
        modbus_channel: modbusChannels.modbus_channel,
        modbus_slave_id: modbusChannels.modbus_slave_id,
      })
      .from(modbusChannels)
      .innerJoin(deviceBuses, eq(modbusChannels.device_bus_id, deviceBuses.id))
  )
  .post(
    "/api/registry/v0.1/modbus-channels",
    async ({ body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      const bus_type = body.bus_type ?? "modbus_rs485";
      try {
        // Reuse this device's existing bus of the same type if one exists — a device can have
        // more than one bus (e.g. RS485 + RS232), but not two rows for the *same* bus.
        let [bus] = await db
          .select({ id: deviceBuses.id })
          .from(deviceBuses)
          .where(and(eq(deviceBuses.device_id, body.device_id), eq(deviceBuses.bus_type, bus_type)));
        if (!bus) {
          [bus] = await db.insert(deviceBuses).values({ device_id: body.device_id, bus_type }).returning({ id: deviceBuses.id });
        }
        const [row] = await db
          .insert(modbusChannels)
          .values({
            device_bus_id: bus.id,
            sensor_id: body.sensor_id,
            modbus_channel: body.modbus_channel,
            modbus_slave_id: body.modbus_slave_id,
          })
          .returning();
        set.status = 201;
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23505") {
          set.status = 409;
          return { error: "this sensor already has a channel mapping, or this channel/slave combination is already used on this bus" };
        }
        if (pgErrorCode(err) === "23503") {
          set.status = 400;
          return { error: "device_id or sensor_id does not reference an existing row" };
        }
        throw err;
      }
    },
    {
      body: t.Object({
        device_id: t.String(),
        bus_type: t.Optional(t.String()),
        sensor_id: t.String(),
        modbus_channel: t.Number(),
        modbus_slave_id: t.Number(),
      }),
    }
  )
  .patch(
    "/api/registry/v0.1/modbus-channels/:id",
    async ({ params, body, headers, set }) => {
      const authError = requireWriteToken(headers, set);
      if (authError) return authError;
      try {
        const [row] = await db
          .update(modbusChannels)
          .set({ ...body, updated_at: new Date() })
          .where(eq(modbusChannels.id, params.id))
          .returning();
        if (!row) {
          set.status = 404;
          return { error: "modbus channel not found" };
        }
        return row;
      } catch (err) {
        if (pgErrorCode(err) === "23505") {
          set.status = 409;
          return { error: "this channel/slave combination is already used on this bus" };
        }
        throw err;
      }
    },
    {
      params: idParam,
      body: t.Object({
        modbus_channel: t.Optional(t.Number()),
        modbus_slave_id: t.Optional(t.Number()),
      }),
    }
  );
