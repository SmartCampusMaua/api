CREATE TABLE "assets" (
	"id" uuid PRIMARY KEY NOT NULL,
	"asset_name" text NOT NULL,
	"asset_slug" text NOT NULL,
	"asset_coords" jsonb,
	"parent_asset_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "assets_asset_slug_unique" UNIQUE("asset_slug")
);
--> statement-breakpoint
CREATE TABLE "device_buses" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"device_id" uuid NOT NULL,
	"bus_type" text NOT NULL,
	"label" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "devices" (
	"id" uuid PRIMARY KEY NOT NULL,
	"device_model" text NOT NULL,
	"device_type" text NOT NULL,
	"serial_number" text,
	"dev_eui" text,
	"mac_address" text,
	"battery_voltage_max" integer,
	"battery_voltage_min" integer,
	"probe_range_m" integer,
	"allow" boolean DEFAULT true NOT NULL,
	"asset_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "modbus_channels" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"device_bus_id" uuid NOT NULL,
	"sensor_id" uuid NOT NULL,
	"modbus_channel" integer NOT NULL,
	"modbus_slave_id" integer NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "modbus_channels_sensor_id_unique" UNIQUE("sensor_id"),
	CONSTRAINT "modbus_channels_device_bus_id_modbus_channel_modbus_slave_id_unique" UNIQUE("device_bus_id","modbus_channel","modbus_slave_id")
);
--> statement-breakpoint
CREATE TABLE "sensors" (
	"id" uuid PRIMARY KEY NOT NULL,
	"sensor_type" text NOT NULL,
	"device_id" uuid NOT NULL,
	"device_index" integer,
	"sensor_name" text,
	"asset_id" uuid,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "assets" ADD CONSTRAINT "assets_parent_asset_id_assets_id_fk" FOREIGN KEY ("parent_asset_id") REFERENCES "public"."assets"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "device_buses" ADD CONSTRAINT "device_buses_device_id_devices_id_fk" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "devices" ADD CONSTRAINT "devices_asset_id_assets_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."assets"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "modbus_channels" ADD CONSTRAINT "modbus_channels_device_bus_id_device_buses_id_fk" FOREIGN KEY ("device_bus_id") REFERENCES "public"."device_buses"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "modbus_channels" ADD CONSTRAINT "modbus_channels_sensor_id_sensors_id_fk" FOREIGN KEY ("sensor_id") REFERENCES "public"."sensors"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sensors" ADD CONSTRAINT "sensors_device_id_devices_id_fk" FOREIGN KEY ("device_id") REFERENCES "public"."devices"("id") ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "sensors" ADD CONSTRAINT "sensors_asset_id_assets_id_fk" FOREIGN KEY ("asset_id") REFERENCES "public"."assets"("id") ON DELETE no action ON UPDATE no action;