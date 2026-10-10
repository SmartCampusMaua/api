import { defineConfig } from "drizzle-kit";

// DATABASE_URL is required at migration time (drizzle-kit generate/migrate) the same way it's
// required at runtime (src/db/client.ts) — no fallback default, since silently defaulting a
// database connection string is exactly the kind of guess that should fail loudly instead.
if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is required — see .env.example (local dev points at docker-compose's postgres service).");
}

export default defineConfig({
  schema: "./src/db/schema.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.DATABASE_URL,
  },
});
