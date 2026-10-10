// Bun's native SQL client (zero extra runtime dependency beyond drizzle-orm itself) — drizzle-kit
// (the CLI, used only for generating/applying migrations) needs its own separate driver, see
// drizzle.config.ts, but the running application only ever goes through this.
import { drizzle } from "drizzle-orm/bun-sql";
import * as schema from "./schema";

if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is required — see .env.example (the registry Postgres connection).");
}

export const db = drizzle(process.env.DATABASE_URL, { schema });
