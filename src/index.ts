import { Elysia } from "elysia";
import { openapi } from "@elysiajs/openapi";
import { config } from "./config";
import { timeseries } from "./routes/timeseries";
import { registry } from "./routes/registry";

const app = new Elysia()
  // Static, dependency-free query builder UI — reads /api/timeseries/:version/_meta at load time
  // to build its form from the live org/domain/resource/tags registry rather than a hardcoded
  // copy, and offers a version picker so it keeps working once a v0.5+ exists alongside v0.4.
  // Path is relative to CWD (same convention as config.ts's infra.json path) rather than
  // import.meta.dir, since the latter needs ESM module output and this project's tsconfig
  // resolves module format from package.json's absent "type" field as CommonJS.
  //
  // Lives at /api/timeseries (not the bare /api umbrella) since it's specifically this product's
  // own tool — /api is reserved as the umbrella entry for whatever else ends up alongside
  // timeseries later (auth, other domains), so it shouldn't be the query builder's permanent
  // address. /api itself 301s here so nothing that bookmarked the old location breaks.
  .get("/api", ({ redirect }) => redirect("/api/timeseries", 301))
  .get("/api/timeseries", () => Bun.file("src/public/index.html"))
  // Device/asset/sensor/bus registry UI — same dependency-free static-page pattern as the query
  // builder above, reading/writing /api/registry/v0.1/... (src/routes/registry.ts).
  .get("/api/registry", () => Bun.file("src/public/registry.html"))
  // Liveness: process is up and can handle HTTP — no external dependency check. A k8s liveness
  // probe should never depend on InfluxDB being reachable; if it did, a transient InfluxDB blip
  // would make every replica fail liveness simultaneously and get restarted in a loop, turning a
  // partial upstream outage into a full self-inflicted one.
  .get("/healthz", () => ({ status: "ok" }))
  // Readiness: config actually loaded successfully (src/config.ts crashes the process on a bad
  // initial infra.json, so this only meaningfully differs from "always ready" in the narrow
  // window before that first load completes). Deliberately still doesn't touch InfluxDB itself,
  // for the same cascading-failure reason as above.
  .get("/readyz", ({ set }) => {
    const orgCount = Object.keys(config.influxdb.orgs).length;
    if (orgCount === 0) {
      set.status = 503;
      return { status: "not ready", reason: "no orgs configured" };
    }
    return { status: "ok", orgs: orgCount };
  })
  .use(timeseries)
  .use(registry)
  .use(
    openapi({
      // Same /api-prefix reasoning as the query builder UI above — the default /openapi path
      // would be unreachable through a proxy that only forwards /api to this service. specPath
      // defaults to `${path}/json`, so this also moves the raw spec to /api/openapi/json.
      path: "/api/openapi",
      // exclude.staticFile defaults to true, and treats any route path containing a "." as a
      // static file request (e.g. /favicon.ico) to exclude from the generated docs — which
      // silently swallowed every one of our routes, since our URL version segment ("v0.4")
      // contains a literal dot. Confirmed via direct source inspection of @elysiajs/openapi
      // (toOpenAPISchema's `route.path.includes(".")` check) after the generated spec came back
      // with only the trivial "/", "/healthz", "/readyz" routes and none of the timeseries ones.
      exclude: { staticFile: false },
      documentation: {
        info: {
          title: "SmartCampusMaua API",
          version: "1.0.0",
          description: "IoT sensor time series over InfluxDB3. See README.md for the full guide — this is the generated route/schema reference.",
        },
      },
    })
  )
  .listen(3333);

console.log(
  `🦊 Elysia is running at ${app.server?.hostname}:${app.server?.port}`
);

// k8s sends SIGTERM before killing a pod (rolling deploy, scale-down, node drain) — without this,
// in-flight requests get cut off mid-response instead of finishing. app.stop() (no `true` arg)
// stops accepting new connections but lets existing ones complete.
for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.on(signal, async () => {
    console.log(`[shutdown] received ${signal}, draining in-flight requests...`);
    await app.stop();
    process.exit(0);
  });
}
