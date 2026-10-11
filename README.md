# SmartCampusMaua API

Elysia + Bun API that exposes IoT sensor time series stored in InfluxDB3.

## Getting Started

Install dependencies:

```bash
bun install
```

Copy the env template and fill in your local values:

```bash
cp .env.example .env
```

Start the development server:

```bash
bun run dev
```

The API listens on `http://localhost:3333`.

## Naming convention

**URLs (path segments and query param names) use camelCase; JSON response bodies use snake_case.** E.g. the query param is `?sensorType=airTemp`, but the field in the response is `"sensor_type": "air_temp"`. This is deliberate, not inconsistent: snake*case in responses matches InfluxDB's own column names and values (and what the [ingestion pipeline](https://github.com/smartcampusmaua/parse-mqtt-to-influxdb3) already writes) 1:1, while camelCase in URLs is the more common REST/JS convention for query params. `src/routes/timeseries.ts`'s `TAG_CAMEL_TO_SNAKE` map is the single place that bridges tag \_names* between the two — every tag name a caller can type in a URL maps to exactly one real column. **A snake_case query param name (e.g. the old `?sensor_type=`) is silently ignored, not an error** — it just doesn't match any known filter, so double check you're using the camelCase form if a filter doesn't seem to apply.

`sensorType`'s **value** gets the same treatment, and only `sensorType` — `?sensorType=airTemp` and `?sensorType=air_temp` both match `air_temp` in the database (the conversion is a no-op on already-snake_case input, so both keep working). Other tag values (`deviceModel`, `provider`, etc.) are **not** converted — those must still be typed exactly as InfluxDB stores them (e.g. `deviceModel=em500_swl`, not `em500Swl`), since `sensor_type` is the one value callers reference by name often enough to warrant it. Response bodies are never affected either way — a value is always returned exactly as InfluxDB stores it (snake_case), regardless of which form you filtered with.

Org slugs are **always lowercase** (`imt`, not `IMT`) — see below.

## Configuration

### `infra.json` — the org → InfluxDB instance registry (and other infra config)

Org config lives in `infra.json` (gitignored, like `.env`), not environment variables — copy `infra.example.json` to `infra.json` and fill in real tokens:

```json
{
  "version": "1",
  "influxdb": {
    "orgs": [
      { "org": "imt", "host": "http://influxdb.maua.br:8181", "token": "..." }
    ]
  },
  "mqtt": {
    "broker": "mqtt.maua.br:1883",
    "username": "...",
    "password": "..."
  }
}
```

`infra.json` is deliberately broader than just this API — it's a shared infrastructure config (InfluxDB org registry here, an `mqtt` broker block for other services) rather than a file scoped to one repo's needs. This API only ever reads the `influxdb` key; everything else (e.g. `mqtt`) is ignored here, present only so other services sharing this file have somewhere to put their own config.

| Field                   | Required            | Description                                                                                                                       |
| ----------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `influxdb.orgs[].org`   | yes                 | Org slug, used in the URL and normalized to **lowercase** internally (write it lowercase).                                        |
| `influxdb.orgs[].host`  | yes                 | Base URL of that org's InfluxDB instance.                                                                                         |
| `influxdb.orgs[].token` | yes                 | Auth token for that org's InfluxDB instance. See warning below.                                                                   |
| `influxdb.orgs[].allow` | no (default `true`) | Set to `false` to disable an org without deleting its config/token — the entry is skipped entirely, as if it weren't in the file. |

This deliberately mirrors the `devices.json` pattern in [parse-mqtt-to-influxdb3](https://github.com/SmartCampusMaua/parse-mqtt-to-influxdb3/blob/develop/cmd/parse-mqtt-to-influxdb3/main.go) — a JSON registry, not scattered env vars, loaded once at startup and **re-read on a timer** (`src/config.ts`):

- **Startup**: a missing/invalid `infra.json` (or a missing/malformed `influxdb.orgs` within it) crashes the process immediately (`No orgs configured` or a parse error) — same reasoning as that service's `panic()` on a bad `devices.json`: better to fail loudly at boot than silently serve zero configured orgs.
- **Reload**: every `INFRA_CONFIG_REFRESH_SEC` seconds (default `30`, override in `.env`), the file is re-read. On success, the new config takes effect immediately — no restart needed, so adding/rotating an org is just editing the file. On failure (bad JSON, missing field), the reload is skipped, the error is logged, and the **previous good config keeps serving** — a typo while hand-editing the file doesn't take the API down, same as that service keeps its last-known-good registry on a bad reload.
- `INFRA_CONFIG_FILE` (default `infra.json`) and `INFRA_CONFIG_REFRESH_SEC` are the only environment variables this API still uses — set them in `.env` if you need non-default values.

### Why `:org` is ours, not InfluxDB's

`:org` in every route below selects **which InfluxDB instance** this API talks to — it's this API's own multi-instance routing, for when different customers/campuses each get their own InfluxDB deployment. It is deliberately _not_ InfluxDB's own concept: InfluxDB3 **Core** (confirmed via the server's own `x-influxdb-build: Core` response header) has no org/multi-tenancy of its own — sending an `org` field to its query API is silently accepted and has zero effect (verified). Don't confuse the two: this `:org` never reaches InfluxDB at all, it's resolved entirely on our side into a `{host, token}` pair before any request goes out.

Today only `imt` is registered, pointing at the one InfluxDB instance in use. Onboarding a new org later is just adding an entry to `infra.json` — no restart, no code change.

### ⚠️ InfluxDB tokens must stay internal

Every org's InfluxDB token grants direct read access to that org's InfluxDB instance and must **never** be exposed to clients or committed to source control:

- Tokens are only ever read server-side, in `src/config.ts` → `src/influxdb.ts`, and attached to the outbound request to InfluxDB as an `Authorization: Bearer` header — keyed by the resolved `:org`, never mixed up between orgs.
- They're **never** forwarded to, logged for, or readable by API callers — routes only return the transformed sensor readings, never raw upstream request/response details.
- `infra.json` is listed in `.gitignore` — only the placeholder `infra.example.json` (no real tokens) is committed.
- Don't paste tokens into chat tools, issues, PR descriptions, or logs. Rotate a token in InfluxDB if it's ever leaked.
- If you deploy this API, mount `infra.json` from your host's secret manager (e.g. a Kubernetes Secret volume), not a file baked into the image.

## Caching (`src/cache.ts`)

Every route above re-queries InfluxDB on every request. To cut redundant load for repeated/hot queries — the same dashboard tile polling every few seconds, the same historical range pulled by more than one caller — query results are cached in two tiers, both in-process, **no Redis**:

| Tier              | Used for                                | Storage                                        | Why                                                                                                                                                                                   |
| ----------------- | --------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Memory**        | Typed sensor rows (the series response) | `Map`-based LRU + TTL                          | Small, JSON-shaped, accessed very frequently — the "hot dashboard" path.                                                                                                              |
| **Disk (`/tmp`)** | `/export` bytes (csv/json/parquet)      | `Bun.write`/`Bun.file`, one file per cache key | Export payloads can be considerably larger; keeping them off the JS heap is good hygiene, and (unlike memory) this tier survives the dev server's `bun run --watch` process restarts. |

**Key granularity:** cache keys are `host :: database :: table :: kind :: startNs :: endNs :: sorted-bind-params` — one entry per **UTC day**, not per whole request. A wide multi-day request is internally several day-chunk queries anyway (see the day-chunking note above, and the `parquet` export's zip-bundling below); each day is cached independently, so two requests that only partially overlap in time range or differ only in `limit` still share whatever days they have in common. For the base route and the `csv`/`json` exports, `limit` is deliberately **not** part of the row-fetching cache key: each day-chunk is always queried/cached at a fixed high cap (`MAX_LIMIT` rows), and the caller's actual `limit` is applied by trimming the cached (or freshly-fetched) rows afterward — otherwise every differently-`limit`-ed request would look like a different query and nothing would ever be reused. The `parquet` export is the one exception: `limit` **is** baked into its per-day cache key, since trimming rows out of an already-fetched Parquet file would mean parsing and re-encoding it — see "`limit` means rows-per-day for `parquet`" below.

**TTL depends on whether the cached range is still "open":** a UTC day whose end is still in the future (i.e. today, still receiving writes) gets a short TTL (15s) so cached data doesn't lag reality by more than a few seconds; a day that's already fully elapsed gets a long TTL (6h), since IoT sensor data for a closed day essentially never changes after the fact.

**Verified behavior:** a repeated identical request returns byte-identical data with a noticeably lower response time (e.g. a 3-day chunked `csv` export dropped from ~0.8s to ~0.003s on a cache hit), and a request with a smaller `limit` than an already-cached day still correctly returns only its own trimmed subset rather than the full cached set.

> **Impact of running multiple replicas (e.g. Kubernetes):** both tiers are **per-process**, not shared. Behind a k8s `Service`'s default round-robin/random load balancing across replicas, the same repeated query only has good odds of hitting a warm cache once _each_ replica has independently missed-then-cached it — so with 3 replicas, effective hit rate for a given hot query converges to what a single-instance cache would give, but only after all 3 have seen it at least once, and each replica holds its own copy (3x the aggregate memory/disk for the same working set). This is a real efficiency loss, not a correctness one — only read-only, already-computed historical query results are ever cached, so a cache miss on the "wrong" replica just means a normal InfluxDB query, never stale-but-wrong data beyond what the TTLs above already allow. A cheap, complementary mitigation if you control the Kubernetes `Service`: set `sessionAffinity: ClientIP` so a given caller (e.g. one dashboard's polling loop) consistently lands on the same pod — this meaningfully improves hit rate for exactly the "same client, same hot query, repeated often" pattern without needing a shared cache at all. If this API is ever scaled further and a truly shared cache becomes worth the operational cost, that's the point to introduce Redis (or another shared store) — not before.

The disk tier also self-limits: a lazy sweep (at most once a minute, triggered on writes) deletes the oldest cache files once the cache directory exceeds ~500MB, so it can't grow unbounded.

## Registry (Postgres)

Devices, assets, sensors, and Modbus bus/channel routing — previously `parse-mqtt-to-influxdb3`'s `sensors.json`/`devices.json`/`assets.json`/`device_channels.json`, now a Postgres database (`src/db/schema.ts`, migrated with [Drizzle](https://orm.drizzle.team/)) that this API owns and can actually write to, via `/api/registry/v0.1/...` and the `/api/registry` webpage. The JSON files were always hand-maintained with no validation, no referential integrity, and no way to edit them short of hand-editing JSON in a text editor; this replaces that with real constraints (foreign keys, a unique asset slug) and a UI.

> **Current-state caveat, worth being precise about:** this Postgres registry is a new, additional read/write surface — it is **not yet** the data source for this API's own `/sensors` endpoint or the main `/:org/:domain/:resource` series route's sensor_id resolution, both of which still read the JSON files directly via `src/sensors.ts`, completely unaffected by anything in this section. Editing data through `/api/registry` today updates Postgres only; it won't be reflected anywhere else in this API until that wiring is a deliberate follow-up. Postgres was seeded once from the JSON files (see "Import" below) as a starting point, not a live sync.

### Schema

```
assets            id (uuid, PK), asset_name, asset_slug (unique), asset_coords (jsonb),
                  parent_asset_id (FK -> assets.id, nullable — asset hierarchy)

devices           id (uuid, PK), device_model, device_type, serial_number, dev_eui, mac_address,
                  battery_voltage_max/min, probe_range_m, allow,
                  asset_id (FK -> assets.id, nullable — the device's "home" asset)

sensors           id (uuid, PK), sensor_type, device_id (FK -> devices.id, not null),
                  device_index, sensor_name,
                  asset_id (FK -> assets.id, nullable — override; effective asset for a sensor
                  is `sensor.assetId ?? device.assetId`, computed by whoever reads this data)

device_buses      id (uuid, PK, generated), device_id (FK -> devices.id), bus_type, label

modbus_channels   id (uuid, PK, generated), device_bus_id (FK -> device_buses.id),
                  sensor_id (FK -> sensors.id, unique), modbus_channel, modbus_slave_id,
                  unique(device_bus_id, modbus_channel, modbus_slave_id)
```

`id` columns on `assets`/`devices`/`sensors` reuse the exact UUIDv7 values `parse-mqtt-to-influxdb3` already assigned — the same ids already used throughout the timeseries API and InfluxDB itself — rather than generating new surrogate keys; there's exactly one identity per entity, never a dual id/external-id pair to keep in sync. `device_buses`/`modbus_channels` have no pre-existing external id, so those get a Postgres-generated `uuid`.

A few design decisions worth knowing the reasoning behind, since they're not obvious from the columns alone:

- **`sensors.asset_id` is an override, not the primary place asset attribution lives.** A device's `asset_id` is its default/"home" location; a sensor only sets its own when it genuinely differs — the case that motivated this: a Modbus RS485 "bus-listening" controller (e.g. a UC300) physically mounted on one generator can also relay readings for two _other_ generators reachable on the same bus. The device attaches to its own home asset; only the sensors for the _other_ generators get an explicit override. Leaving it unset is the common case.
- **`device_buses` sits above `modbus_channels`, not folded into `devices`,** because a single device can have more than one bus simultaneously (e.g. a UC300's RS485 and RS232 are independently-addressed Modbus networks — slave 1 on one is unrelated to slave 1 on the other). A single `devices.bus_protocol` column couldn't represent that; `bus_type` stays plain text rather than a Postgres enum, mirroring `sensor_type`/`device_model`, since locking it to an enum would mean keeping it in lockstep with the Go ingestion pipeline's own definitions in a separate repo.
- **`assets.parent_asset_id` enables a hierarchy** (e.g. "Pump Station" containing "Pump A"/"Pump B") but nothing here builds hierarchy-aware queries yet (rolling up a parent's descendants) — just the column, cheap to have now, expensive to retrofit later.

### Local development

Points at a dedicated `iot` database + role inside whatever Postgres instance `DATABASE_URL` names (`.env`) — named to match `deploy/iot-postgres.yaml`'s naming (Secret, Service, StatefulSet all `iot`/`iot-postgres`), so the same name means the same thing locally and in the cluster. On this machine that's an `iot` database created inside the pre-existing shared `smartcampus-postgres` container, kept isolated from that instance's other, unrelated databases (it also hosts an unrelated app's tables). Bring your own local Postgres and point `DATABASE_URL` at it if you don't have a shared instance to reuse.

```bash
bunx drizzle-kit generate   # after changing src/db/schema.ts — writes drizzle/000X_*.sql
bunx drizzle-kit migrate    # applies pending migrations
bun run scripts/import-registry.ts   # idempotent: JSON files -> Postgres, safe to re-run
```

The import script reads the exact same env-var-configurable paths `src/sensors.ts` already uses (`SENSORS_CONFIG_FILE`/`DEVICES_CONFIG_FILE`/`ASSETS_CONFIG_FILE`, plus a new `DEVICE_CHANNELS_CONFIG_FILE`) — it's read-only toward those files, same relationship this API has always had with them, and safe to re-run after editing the source JSON (every insert upserts on the existing UUID; `device_buses`, which has no natural key, is looked up by `(device_id, bus_type)` first and reused rather than duplicated).

### Write auth

`/api/registry/v0.1/...` reads (`GET`) are open, same posture as the rest of this API. Writes (`POST`/`PATCH`) require `Authorization: Bearer <token>`, checked against a SHA-256 hash in `REGISTRY_WRITE_TOKEN_HASH` (`.env`) — the plaintext is never stored anywhere, only its hash, and comparison is constant-time (`src/registryAuth.ts`). An unconfigured hash fails every write closed, not open. This is a deliberate stopgap — a single shared secret, not per-user identity — ahead of real auth (better-auth, not implemented yet); the `/api/registry` webpage prompts for the token once and holds it in `sessionStorage` for that tab.

Generate a token + hash:

```bash
bun -e "const t = crypto.randomUUID().replace(/-/g,'') + crypto.randomUUID().replace(/-/g,''); console.log('token:', t); console.log('hash: ', require('crypto').createHash('sha256').update(t).digest('hex'))"
```

### API and webpage

`GET/POST/PATCH` on `/api/registry/v0.1/{assets,devices,sensors,modbus-channels}` (and `/:id` for single-row GET/PATCH) — see `src/routes/registry.ts` for the full set; `POST`/`PATCH` return a clean `400`/`409` on a foreign-key or unique-constraint violation rather than a raw database error. `GET /api/registry` is the webpage — the same dependency-free static-HTML-plus-vanilla-JS pattern as the query builder at `/api/timeseries`, with tabs for each entity, a form that doubles as create/edit, and — directly addressing "attach a device to an asset from either list" — an inline attach-device control on every asset row _and_ an asset dropdown on every device's own edit form, both just writing to `devices.asset_id`.

## Docker / Kubernetes

### Building and running the image

`docker/Dockerfile` is a multi-stage build (see its own comments for the full reasoning per stage): dependencies are installed into separate "dev" (full, used only to typecheck) and "prod" (`--production`, what actually ships) trees so Docker's layer cache only invalidates on `package.json`/`bun.lock` changes, not on every source edit; `tsc --noEmit` runs as a build-time gate in its own stage whose output is never copied into the final image, so a type error fails the build instead of shipping; and the final image contains only production dependencies and source, running as the image's built-in non-root `bun` user.

```bash
# from the repo root — note the build context is "." even though the Dockerfile lives in docker/
docker build -f docker/Dockerfile -t smartcampusmaua/api .

docker run -d -p 3333:3333 \
  -v "$(pwd)/infra.json:/etc/api/infra.json:ro" \
  smartcampusmaua/api
```

`infra.json` is deliberately **not** baked into the image (it's excluded via `.dockerignore`, same token-security reasoning as the warning above) — it must be mounted at runtime. `docker/Dockerfile` sets `ENV INFRA_CONFIG_FILE=/etc/api/infra.json`, a fixed absolute path baked into the image (the same pattern this project's sibling Go service, `mqtt-topic-rewrite-lns-chirpstackv4`, already uses for its own `devices.json` via `DEVICE_REGISTRY_FILE`) — so every deployment mounts to the same well-known path rather than needing to know an app-internal working directory.

### Kubernetes: config files as a Secret volume

**Use a `Secret`, not a `ConfigMap`, for this directory** — `infra.json` embeds a live InfluxDB bearer token (and mqtt broker credentials), and a `ConfigMap`'s contents are stored and displayed as plain text to anyone who can `kubectl get configmap -o yaml` in the namespace. `sensors.json`/`devices.json`/`assets.json` hold no secrets of their own, but they're colocated in the _same_ `Secret` object anyway — see "mount the whole directory" below for why splitting them into a separate `ConfigMap` mounted at the same path would actually break hot-reload. A `Secret` is the k8s-native primitive for exactly this case (base64-encoded at rest by default, tighter default RBAC, and the natural integration point if the cluster later adds real encryption-at-rest or an external secret manager).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: smartcampusmaua-api-config
type: Opaque
stringData:
  infra.json: |
    {
      "version": "1",
      "influxdb": {
        "orgs": [
          { "org": "imt", "host": "http://influxdb.maua.br:8181", "token": "..." }
        ]
      },
      "mqtt": { "broker": "mqtt.maua.br:1883", "username": "...", "password": "..." }
    }
  sensors.json: |
    { "sensors": [ ... ] }
  devices.json: |
    { "devices": [ ... ] }
  assets.json: |
    { "assets": [ ... ] }
  # Registry Postgres (see "Kubernetes: Postgres StatefulSet for the registry" below) and the
  # write-token hash (see "Registry (Postgres)") — plain env-var keys in the same Secret object,
  # consumed via secretKeyRef below rather than the volume mount, alongside the four file keys
  # above. One Secret for all of this API's runtime secrets, regardless of how each key is consumed.
  DATABASE_URL: postgres://iot:...@iot-postgres:5432/iot
  REGISTRY_WRITE_TOKEN_HASH: "..."
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smartcampusmaua-api
spec:
  replicas: 3
  selector:
    matchLabels: { app: smartcampusmaua-api }
  template:
    metadata:
      labels: { app: smartcampusmaua-api }
    spec:
      containers:
        - name: api
          image: smartcampusmaua/api:0.2
          ports:
            - containerPort: 3333
          env:
            - name: DATABASE_URL
              valueFrom:
                {
                  secretKeyRef:
                    { name: smartcampusmaua-api-config, key: DATABASE_URL },
                }
            - name: REGISTRY_WRITE_TOKEN_HASH
              valueFrom:
                {
                  secretKeyRef:
                    {
                      name: smartcampusmaua-api-config,
                      key: REGISTRY_WRITE_TOKEN_HASH,
                    },
                }
          volumeMounts:
            - name: api-config
              mountPath: /etc/api
              readOnly: true
          livenessProbe:
            httpGet: { path: /healthz, port: 3333 }
          readinessProbe:
            httpGet: { path: /readyz, port: 3333 }
      volumes:
        - name: api-config
          secret:
            secretName: smartcampusmaua-api-config
```

**Mount the whole directory (`/etc/api`), not individual files via `subPath`.** This is the one detail worth getting right on the first try: a `subPath` mount (`mountPath: /etc/api/infra.json` + `subPath: infra.json`) is a common way to mount just one key from a `Secret`/`ConfigMap`, but subPath-mounted files are **not** updated when the underlying `Secret`/`ConfigMap` changes — only a full pod restart picks up the new content. Mounting the directory instead (as above) lets kubelet sync the projected volume's contents on its own (typically within about a minute of the `Secret` changing), which is what actually lets each file's own reload timer (`INFRA_CONFIG_REFRESH_SEC` for `infra.json`, `SENSORS_CONFIG_REFRESH_SEC` for the other three) pick up a change **without a redeploy** — the entire reason that reload logic exists in the first place. Using `subPath` for any of these four files would silently defeat it for that file specifically: the app would keep re-reading the same stale content until something forced a pod restart. This is also why `sensors.json`/`devices.json`/`assets.json` live in the _same_ `Secret`/mount as `infra.json` rather than a separate `ConfigMap` volume mounted alongside it at a different subpath — a container can only have one thing mounted at a given directory, so keeping all four in one object is what makes "mount the whole directory once" possible at all.

Updating any of the four files: `kubectl create secret generic smartcampusmaua-api-config --from-file=infra.json --from-file=sensors.json --from-file=devices.json --from-file=assets.json --dry-run=client -o yaml | kubectl apply -f -` (or edit the `Secret` in place) — no rollout needed, the running pods pick it up on their own within the sync interval above. `DATABASE_URL`/`REGISTRY_WRITE_TOKEN_HASH` are plain string keys in the same `Secret`, so `kubectl edit secret smartcampusmaua-api-config` (or re-applying with `--from-literal=DATABASE_URL=...`) works the same way — a pod restart is needed for those two specifically, since they're consumed as env vars (fixed at container start), not from the live-synced volume mount the other four use.

### Kubernetes: Postgres StatefulSet for the registry

See "Registry (Postgres)" above for what this holds (devices/assets/sensors/bus routing) and why it replaced the JSON files as the source of truth. The actual manifest lives in `deploy/iot-postgres.yaml` — tracked in this repo rather than copy-pasted here, so there's exactly one version of it to keep correct, not two that can quietly drift apart. It defines a Secret (`POSTGRES_USER`/`PASSWORD`/`DB`, all `iot`), a headless Service, and a `StatefulSet`, all named `iot`/`iot-postgres` to match this API's own `DATABASE_URL`. A `StatefulSet`, not a plain `Deployment`, is the right primitive even at a single replica — it's what gives the pod a stable identity and a `PersistentVolumeClaim` that survives rescheduling, via `volumeClaimTemplates`, rather than a `Deployment`'s pods being fully interchangeable.

Three things it deliberately does **not** cover, worth deciding on separately rather than assuming they're handled:

- **No replication/HA.** `replicas: 1` — a genuine single point of failure. Reasonable for "build it smoothly for now" at today's data size (a few hundred rows across 5 tables), but worth revisiting before this is load-bearing for anything critical.
- **No backup strategy.** If this PVC is lost, the registry data is gone — there's no `pg_dump`-to-object-storage CronJob or equivalent here. This matters more now than it would have with the old JSON files, since Postgres is meant to become the actual source of truth, not just a convenience cache of files someone else maintains.
- **No automated migrations on deploy.** The first time this StatefulSet comes up, someone needs to run `bunx drizzle-kit migrate` (and seed the initial dataset — see "Local development" above) against it manually, e.g. via `kubectl port-forward svc/iot-postgres 5432:5432` and a local `DATABASE_URL` override — nothing here runs them automatically as part of the rollout. Planned as a deliberate follow-up, not solved here yet.

### Health endpoints

| Route          | Purpose   | Checks                                                                                            |
| -------------- | --------- | ------------------------------------------------------------------------------------------------- |
| `GET /healthz` | Liveness  | Only that the process is up and can handle HTTP — no external dependency.                         |
| `GET /readyz`  | Readiness | That `infra.json` loaded successfully (`config.influxdb.orgs` is non-empty) — `503` otherwise. |

Neither endpoint calls InfluxDB. This is deliberate: a k8s liveness/readiness probe that depends on an upstream service being reachable means a transient InfluxDB blip fails the probe on **every** replica at once, turning a partial upstream outage into a full self-inflicted one by having k8s restart or de-pool the whole fleet simultaneously. In practice, `/readyz`'s `503` branch is close to unreachable today — `src/config.ts` crashes the process immediately on a bad/empty initial `infra.json` (see "Configuration" above), so the process never reaches a listening state with zero orgs configured; the check exists mainly as a defensive signal in case that fail-fast behavior ever changes, verified by actually forcing an empty-orgs config through the container and confirming the crash-before-listening path.

Example k8s probe config:

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 3333 }
readinessProbe:
  httpGet: { path: /readyz, port: 3333 }
```

The process also handles `SIGTERM`/`SIGINT` (sent by k8s before killing a pod during a rolling deploy, scale-down, or node drain) by stopping the HTTP listener and letting in-flight requests finish, rather than cutting them off mid-response.

### CI (`.github/workflows/ci.yaml`)

On every push to the `develop` branch, this builds `docker/Dockerfile` (via `docker/build-push-action`, with QEMU + Buildx set up first) and pushes it to Docker Hub as `smartcampusmaua/api:0.1`.

Two things worth knowing before relying on this:

- It needs `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` configured as repo secrets (Settings → Secrets and variables → Actions) — without them, the `docker/login-action` step fails.
- The tag is a **static** `0.1` — every push to `develop` overwrites the same tag on Docker Hub rather than publishing a new one per commit/version. That's the workflow as copied in; if you want per-commit or per-version tags (e.g. the short SHA, or a value read from `package.json`'s `version`), that's a change to make deliberately, not something assumed here.
- **This repo currently only has a `main` branch — no `develop`.** The workflow is wired up but won't run until a `develop` branch exists and gets pushed to.

## Exploring the API: query builder UI & OpenAPI docs

Two dependency-light ways to explore the API without reading this whole document first — both served under `/api`, deliberately not at the domain root, so a k8s Ingress/reverse-proxy that only forwards an `/api` path prefix to this service (the expected setup once this sits behind a shared gateway/domain rather than its own subdomain) reaches them too. `/healthz`/`/readyz` are the one exception and stay at the root — kubelet calls those directly against the pod's own address, never through an external ingress path, so they were never affected by this. `GET /api` itself is neither of these two tools — it's a permanent `301` to `/api/timeseries` (see below), kept only so nothing that bookmarked the old location breaks:

- **`GET /api/timeseries`** — a minimalist, dependency-free query builder page (`src/public/index.html`, plain HTML/CSS/vanilla JS, no build step, no framework). It lives here rather than at the bare `/api` root because it's specifically this product's own tool — `/api` itself is reserved as a stable umbrella entry point for whatever else ends up alongside timeseries later, so `GET /api` just `301`s here. A **version** dropdown (hardcoded today to the one real version, `v0.4` — extended in code whenever a `v0.5`+ ships) drives which `_meta` endpoint it reads. On load it fetches `GET /api/timeseries/<version>/_meta` (a small live-registry endpoint — see below) and builds its form from the real, current `orgs`/`domains`/`resources`/`tags`, not a hardcoded copy that could drift out of sync with `DOMAIN_RESOURCES`/`TABLES` in `src/routes/timeseries.ts`. Pick a version/org/resource, fill in whichever tag filters you want, choose a relative interval or absolute range, and hit **Build & Run**: it shows the exact URL it built (so you can copy it into a script) and either renders the JSON inline (**Response**) or opens the export in a new tab so the browser downloads it (**Export**: parquet/csv/json — Content-Disposition is already set server-side). Picking a resource that doesn't support shaped output (e.g. `iot/audit`) automatically steers you to Export instead of letting you hit the 400. A "Response fields" checkbox row (device_id/asset_id/dev_eui+mac_address, all checked by default) builds the `?fields=` param for you — see below; it's hidden for Export, since that param has no effect there. An `assetSlug` text field is appended to the tag filters whenever the resource has `assetId` available — it isn't a real InfluxDB tag (so it never comes from `_meta`'s `tags` list), just a convenience for typing a stable `asset-1`-style slug instead of an opaque `asset_id` UUID.
- **`GET /api/openapi`** — an auto-generated API reference ([Scalar](https://github.com/scalar/scalar) UI by default, via [`@elysiajs/openapi`](https://github.com/elysiajs/elysia-openapi)), built from the same Elysia route/schema definitions that actually run — the query/params validation shown there is the real validation, not hand-copied docs that can drift. Raw spec at `GET /api/openapi/json`.

> **If you're wiring up `@elysiajs/openapi` elsewhere and routes silently don't show up:** its `exclude.staticFile` option defaults to `true` and treats **any path containing a literal `.`** as a static file request (e.g. `/favicon.ico`) to exclude from the docs — which silently ate every route in this API, since the URL version segment (`v0.4`) contains a dot. `src/index.ts` sets `exclude: { staticFile: false }` to fix it. This is easy to miss because it fails silently (no error, no console warning) — the spec just quietly returns fewer paths than expected. Confirmed by direct source inspection of the plugin's `toOpenAPISchema` (the `route.path.includes(".")` check) after the generated spec came back with only 3 trivial routes and none of the real API ones.

`GET /api/timeseries/v0.4/_meta` itself (the endpoint the UI reads) returns:

```json
{
  "orgs": ["imt"],
  "domains": {
    "iot": {
      "sensors": {
        "tags": [
          "devEui",
          "deviceId",
          "deviceModel",
          "macAddress",
          "provider",
          "sensorType",
          "sensorId"
        ],
        "shaped": true
      },
      "audit": { "tags": ["deviceId", "eventType"], "shaped": false }
    }
  }
}
```

It's a convenience for tooling (this UI, or your own scripts), not a stable/versioned contract the way the timeseries routes themselves are documented to be below.

### Sensor picker (`sensorId`) and `GET /api/timeseries/v0.4/:org/sensors`

`sensor_id` is the actual atomic identity in this data model — a UUIDv7 tied to one physical sensor, which is inherently pinned to one device and one `sensor_type`. `device_id` is a dimension of that identity (which device it lives on) and a real InfluxDB tag in its own right (needed for the audit log and as a way to _find_ a sensor_id), but it isn't meant to be authoritative over it. Typing raw sensor_id UUIDs by hand is impractical, though, so the query builder's `sensorId` field is a proper picker instead of a text box: grouped by device (`device_model` + `dev_eui`/`mac_address` as a collapsible header — there are ~850 sensors across ~70 devices, too many to show flat), a checkbox per sensor labeled with `sensor_name` when the registry has one (only sensors that actually need a human label to disambiguate — e.g. multiple probes on one device — have it; everything else falls back to `sensor_type`), and a search box that filters by device model, `dev_eui`/`mac_address`, `sensor_type`, or `sensor_name`. Checking several sensors builds a comma-separated `?sensorId=` value (see "Multi-value tag filters" below).

The picker is fed by `GET /api/timeseries/v0.4/:org/sensors`:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/sensors" | head -c 300
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-71c1-8ff2-4d58c2d11657",
    "sensor_type": "battery_level",
    "device_id": "019c2f32-0637-7304-8bc3-bc5c58b2961c",
    "device_model": "dtl200",
    "dev_eui": "a8404123415f13fe"
  },
  {
    "sensor_id": "01a03992-e6c6-75d1-9ba4-0999e88e0c37",
    "sensor_type": "soil_moisture_raw",
    "sensor_name": "Depth 10cm",
    "device_id": "019f708d-1d7f-7d24-9029-e84713cddb96",
    "device_model": "lnv3_sm3dl",
    "dev_eui": "0004a30b00e9be97"
  }
]
```

**`?deviceId=`/`?assetId=`/`?assetSlug=`** narrow the list — index-backed (`deviceIdIndex`/`assetIdIndex`/`assetSlugIndex` in `src/sensors.ts`, built alongside the existing sensor_id lookup in one pass over the registry), not a linear scan, so this stays cheap as the registry grows toward thousands of sensors. Same convention as every tag filter on the main routes: each accepts a comma-separated list (OR'd — `?deviceId=a,b` means device `a` **or** `b`). `assetId` and `assetSlug` narrow the same "asset" dimension — two spellings of the same filter — so they **union** together rather than AND when both are given; `deviceId` combines with either via AND (`?deviceId=a&assetSlug=x` means sensors on device `a` **and** assigned to asset `x`). An id/slug with no match just returns fewer results, not a `400` — this is filtering a static in-memory list, not gating on live InfluxDB schema the way `?sensorId=` does on the base route.

`asset_slug` is a stable, lowercase-kebab-case identifier — safe to type in a URL/query filter by hand, unlike the opaque `asset_id` UUID — and `asset_name` is a free-text display label with no filtering role of its own (same split `sensor_id`/`sensor_name` already have for sensors). Both are optional, present only once an asset has them assigned in `assets.json`. `?assetSlug=` also works directly on the **base and export routes** (`iot/sensors` and its `/export`), not just this registry endpoint — it's resolved to the equivalent `?assetId=` value before the InfluxDB query is built (`resolveAssetSlugFilter` in `src/routes/timeseries.ts`), since InfluxDB itself only ever has `asset_id` as a real tag; the slug never reaches a SQL query directly:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?assetSlug=asset-1&interval=10080&limit=5"
# identical results to the equivalent ?assetId=01a08185-4622-71f4-8471-766ba2f002d0
```

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/sensors?deviceId=019c2f32-0637-7304-8bc3-bc5c58b2961c"
```

This is a flat list, joined from [parse-mqtt-to-influxdb3](https://github.com/SmartCampusMaua/parse-mqtt-to-influxdb3)'s `sensors.json` (the canonical source `sensor_id` is actually assigned from — not derived from InfluxDB, which doesn't have `sensor_id` populated yet at all, see the footnote further down), `devices.json` (device identity/model), and `assets.json` (asset assignment). `src/sensors.ts` reads all three from `SENSORS_CONFIG_FILE`/`DEVICES_CONFIG_FILE`/`ASSETS_CONFIG_FILE`, defaulting to `../parse-mqtt-to-influxdb3/*.json` — a relative path that only resolves for a local checkout sitting next to this repo, so it's local-dev-only. The Docker image overrides all three to `/etc/api/{sensors,devices,assets}.json`, the same mounted directory `infra.json` already uses — see "Kubernetes: config files as a Secret volume" above. Nothing downstream depends on _how_ the files are reached, only on `sensorRegistry`'s output, so pointing the env vars anywhere else (a different mount, a shared registry service later) is a config change, not a code change. Unlike `infra.json`, a missing/broken registry here does **not** crash the process — hot-reloaded on the same timer as `infra.json` (`SENSORS_CONFIG_REFRESH_SEC`, default `30`), a load failure just makes `/sensors` (and so the picker) report the error and keeps the rest of the API working, since nothing about the core timeseries routes actually depends on this registry.

## API

### Supported domains & resources

Every route below takes `:org/:domain/:resource` in its path — **not** InfluxDB's own database/table names directly. `src/routes/timeseries.ts`'s `DOMAIN_RESOURCES` registry is what maps a clean, semantic URL onto the actual `{database, table}` it queries:

| Org   | Domain | Resource  | → Database.Table          | Tags (camelCase, in URL)                                                                  | Values                                                                                  | Identity (`dev_eui`/`mac_address`) | Base (series)            | `/export?format=` |
| ----- | ------ | --------- | ------------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- | ---------------------------------- | ------------------------ | ----------------- |
| `imt` | `iot`  | `sensors` | `iot_sensors.sensor_data` | `devEui`, `deviceId`, `deviceModel`, `macAddress`, `provider`, `sensorType`, `sensorId`\* | One exclusive value per row (`value_bool`/`value_float`/`value_int` — whichever is set) | Yes                                | Yes                      | Yes               |
| `imt` | `iot`  | `audit`   | `audit_iot.raw`           | `deviceId`, `eventType`                                                                   | None — `raw_data` is a JSON/text blob, and there's no `sensor_type`                     | No                                 | **No** (400 — see below) | Yes               |

\* **`sensorId` is not usable yet.** It's documented in [parse-mqtt-to-influxdb3](https://github.com/SmartCampusMaua/parse-mqtt-to-influxdb3) as the stable identity of a measurement stream (a UUIDv7, resolved from a `sensors.json` registry) — the intended way to disambiguate multiple sensors sharing one `sensor_type` on the same device (e.g. three `soil_moisture_raw` probes on one `lnv3_sm3dl`). It's genuinely part of the target schema, but **as of now, zero rows in the live database have ever been written with it** — and InfluxDB3's schema is write-driven, so a column nothing has ever written to doesn't just read as empty, it doesn't exist for query purposes at all yet. This API checks column existence live (cached for the process's lifetime — see `columnExists()` in `src/influxdb.ts`) and adapts automatically:

> - `sensor_id` is included in `SELECT`/filtering only once it's actually queryable. Until then, it's silently left out of every query this API builds (no broken "column not found" errors).
> - An **explicit** `?sensorId=...` filter, however, is **not** silently ignored — it gets a clear `400` instead, since silently dropping a filter you explicitly asked for could make unfiltered results look filtered:
>
> ```bash
> curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?sensorId=019f708d-1d7f-7d24-9029-e84713cddb96&interval=5"
> # {"error":"Query param 'sensorId' is not available yet — no data in this database has been written with sensor_id yet."}
> ```
>
> Once real data with `sensor_id` starts flowing, all of this activates automatically — no code or config change needed.

`:org` picks which InfluxDB instance to talk to (see above); unknown → `400`. `:domain` groups related resources — today only `iot` exists. A `vehicle` domain (for the `vehicle_telemetry`/`audit_vehicle` databases the [ingestion pipeline](https://github.com/SmartCampusMaua/parse-mqtt-to-influxdb3) already opens InfluxDB clients for) would follow the same `vehicle/telemetry` + `vehicle/audit` shape later — but isn't built yet: both databases exist and are completely empty (no tables have ever been written), and the ingestion pipeline's own source has no write/record code for them yet either, so there's currently no real schema to build a registry entry against. Unknown domain → `400`. `:resource` is the specific thing within that domain; unknown → `400`.

Where a domain's name and a database's name share a word, that's not a coincidence: `iot_sensors` and `audit_iot` both contain `iot`, so domain `iot` plus the other half of each name (`sensors`, `audit`) is exactly how they're distinguished.

`iot/sensors` behaves the way the rest of this document describes. `iot/audit` is the auditable/raw-ingestion resource — it exists to be exported as-is, not queried as a time series, so the base route **intentionally** returns `400` for it. That `400` is correct, working behavior, not something to work around:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/audit?interval=30"
# {"error":"Table \"raw\" has no sensor_type/value columns to shape into series. Use /export?format=... instead."}
```

`/export` is the one that actually returns `iot/audit`'s data — filterable by `deviceId`/`eventType`, same `interval`/`limit`/safety-cap rules as everywhere else. Verified working (`format=json` here so it's inline instead of a downloaded file; `raw_data` is InfluxDB's own stored value, truncated below for brevity):

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/audit/export?format=json&eventType=payload_ingest&interval=15&limit=1"
```

```json
[
  {
    "device_id": "019f7092-1baa-779f-b712-a9b740f7a6bc",
    "event_type": "payload_ingest",
    "raw_data": "{\"deduplicationId\":\"b3f61612-81c8-43f5-bfb2-4481fac71a58\",\"time\":\"2026-09-05T03:04:57.100734+00:00\",\"deviceInfo\":{...,\"devEui\":\"0004a30b00e94314\",...},...}",
    "time": "2026-09-05T03:04:57.449088338"
  }
]
```

Or to a file, e.g. for CSV:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/audit/export?format=csv&eventType=payload_ingest&interval=15" \
  -o audit.csv
```

### `GET /api/timeseries/v0.4/:org/:domain/:resource`

Returns a time series from a supported resource, filtered by one or more tags, over a time window.

**Query parameters**

| Param                                                                                                            | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| tag filters (resource-dependent, camelCase — see the registry above, e.g. `sensorType`, `deviceId`, `devEui`...) | no       | —       | Combined with `AND` when more than one is given. Omitting a tag just widens what it matches — e.g. no `sensorType` returns every sensor type for the given device; no filters at all returns every device (and every sensor type, where applicable) in the window. Any tag accepts a **comma-separated list** — see below.                                                                                                                                                                                                                                                     |
| `interval`                                                                                                       | no       | `1`     | **Relative** lookback window in minutes. Only readings from the last `interval` minutes are returned. Defaults to the last minute, which is deliberately tight — a request with no query params at all should be cheap, not an accidental full scan. Mutually exclusive with `startTime`/`endTime`.                                                                                                                                                                                                                                                                            |
| `startTime`, `endTime`                                                                                           | no       | —       | **Absolute** calendar range instead of a relative lookback, supporting both a whole day (`2026-08-16`) and a precise date **and** time (`2026-08-16T14:30:00Z`) — see format rules below. Must be given together (one without the other is `400`); `startTime` must be before `endTime`; mutually exclusive with `interval` (giving both is `400`). Use this over `interval` whenever you actually mean "August 16th, 2-3pm" rather than "the last N minutes" — it says what you mean and doesn't silently drift as time passes between building the request and it executing. |
| `limit`                                                                                                          | no       | `1000`  | Max **rows** scanned before grouping (1–10000), across the whole request even when it spans multiple days internally (see below) — not per-day.                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `fields`                                                                                                         | no       | _(all)_ | Comma-separated subset of `deviceId`, `assetId`, `devEui`, `macAddress` — controls which **optional** response keys are included (see "Response" below). Unknown values are a `400`.                                                                                                                                                                                                                                                                                                                                                                                           |

> **Multi-value tag filters:** any tag accepts a comma-separated list of values — e.g. `?deviceId=019be6fa-...,019f70a9-...` or `?sensorType=airTemp,batteryLevel` — matched with SQL `IN (...)` rather than `=`, joined with `AND` against any other tags given (so `?deviceId=a,b&sensorType=airTemp` means _(deviceId a OR b) AND sensorType airTemp_, not every combination of the two). Nothing about it is `sensorId`-specific, though that's the case it's most useful for (see the sensor picker below): `?sensorId=uuid1,uuid2,uuid3` pulls several specific sensors in one request instead of one request each. A value that's _only_ commas (e.g. `?sensorType=,,`) is a clean `400`, not a query that silently matches nothing.

> **`startTime`/`endTime` format rules:** either a date-only string (`2026-08-16`, interpreted as UTC midnight) or a full datetime **with an explicit timezone marker** (`2026-08-16T14:30:00Z` or `2026-08-16T14:30:00-03:00`). A datetime with a time component but **no** `Z`/offset — e.g. `2026-08-16T14:30:00` or the space-separated `2026-08-16 14:30:00` — is rejected with `400`, not guessed: JavaScript's date parser (which this API is built on) silently treats an unmarked datetime as _server-local_ time, so the same literal URL would resolve to a different real-world instant depending on where the API happens to be deployed. Since every other timestamp in this API is strictly UTC (nanosecond epochs, no ambiguity), silently guessing here would be a correctness bug, not a convenience — so it's a hard `400` instead.

> **Note on wide time ranges — automatic per-day chunking:** InfluxDB3 Core caps how many Parquet files a single query may scan (a Core-vs-Enterprise limitation, not something we impose), and that ceiling tracks file count, not elapsed time — a busy day can trip it just as easily as a 30-day unfiltered range can. Rather than reject wide requests outright, **this endpoint transparently splits the requested range into one query per UTC calendar day and merges the results** — a 30-day request is up to 30 sequential InfluxDB queries under the hood, not one. Queries run newest-day-first and **stop as soon as `limit` is satisfied**, so a request for recent data doesn't pay for querying older days at all (a `limit=3` request against a 30-day range typically only touches the most recent day). This means most wide/unfiltered queries that used to `400` now just work — but it isn't an absolute guarantee: an extremely write-dense _single_ day can still exceed the cap on its own, in which case you'll still get the usual `400` telling you to narrow further or add a tag filter. There's also a **366-day cap on the range itself** (independent of the file-scan issue) — past that, the sheer number of sequential per-day queries would make a single request unreasonably slow; split it into multiple requests (e.g. one per year) instead. `/export` chunks the same way for all three formats, including `parquet` — see that section below for why `parquet` bundles its chunks into a zip instead of merging them into one file.

**Response**

An array of **series** — one object per **(device, sensor_type, sensor_id)** combination, each with a `series` array of ordered time+value tuples. "Series" is standard TSDB terminology: a series is one unique tag combination's ordered time+value points. Keys are always in this order when present:

- **`sensor_id`** — **mandatory**. Present once real data has it, or when the [sensor registry](https://github.com/SmartCampusMaua/parse-mqtt-to-influxdb3) can resolve it unambiguously (see the `sensorId` footnote above and the sensor picker section below) — absent otherwise, same as any other JSON key with an `undefined` value.
- **`asset_id`** — optional (see `fields` above). Only present for devices actually assigned to an asset (most aren't yet — see the sensor picker section).
- **`device_id`** — optional.
- exactly one of **`dev_eui`** (LoRaWAN devices, `provider` is `chirpstackv4` or `everynet`) or **`mac_address`** (IP/WiFi devices, `provider` is `custom`) — optional, whichever identifies the device on its network, per the [ingestion schema](https://github.com/smartcampusmaua/parse-mqtt-to-influxdb3).
- **`sensor_type`** — **mandatory**. The reading's own sensor type. A device emitting several sensor types (or several sensors sharing one type, once `sensor_id` disambiguates them — e.g. three `soil_moisture_raw` probes on one `lnv3_sm3dl`) becomes several separate array entries, never merged into one.
- **`series`** — **mandatory**. An array, newest first, of `[time, value]` tuples.
  - **`time`** (first tuple element) is a **string** of epoch **nanoseconds**, e.g. `"1786993714000000000"`. It's InfluxDB's native on-disk precision, returned as-is rather than formatted into an RFC3339 string, so there's no timezone/parsing ambiguity and no server-side date-formatting cost. It's a string, not a JSON number, because nanosecond epoch values exceed `Number.MAX_SAFE_INTEGER` (2^53) — as a JSON number, standard JSON parsers (including JS's) silently round it, losing 100s of ns; parse it with `BigInt(tuple[0])` if you need to do arithmetic on it.
  - **value** (second element) may be a `number` (from `value_float`/`value_int`) or a `boolean` (from `value_bool`) depending on the sensor type — callers should not assume a fixed type per series.

`asset_id`/`device_id`/`dev_eui`/`mac_address` are the **optional** keys `?fields=` controls (e.g. `?fields=deviceId` returns only `sensor_id`, `device_id`, `sensor_type`, `series` — no `asset_id`, no `dev_eui`/`mac_address`); omitting `fields` entirely returns all of them, same as before this param existed. Since `devEui` and `macAddress` are themselves tags in `iot/sensors`, they also work directly as a filter regardless of whether they're selected in `fields` — e.g. `?devEui=24e124136f315508&interval=60` returns only that device's series, with no need to separately know whether it's LoRaWAN or IP-based.

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?sensorType=airTemp&interval=5"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-754d-9428-7cccd6811011",
    "device_id": "019e3bb0-142c-727c-85d6-d76ced954e5a",
    "dev_eui": "f803320100030977",
    "sensor_type": "air_temp",
    "series": [["1788575862307522774", 16.45]]
  }
]
```

A device with multiple sensor types (e.g. filtering by `devEui` alone, no `sensorType`) yields one array entry per sensor type it emits, each with its own tuple stream:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?devEui=24e124136f484616&interval=60"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-7209-bc48-02167f85bc6c",
    "device_id": "019be6fa-0dee-72bf-9e40-96939916414e",
    "dev_eui": "24e124136f484616",
    "sensor_type": "air_temp",
    "series": [["1788575382423470960", 22]]
  },
  {
    "sensor_id": "01a03992-e6c6-7208-ada4-8c42b91ae986",
    "device_id": "019be6fa-0dee-72bf-9e40-96939916414e",
    "dev_eui": "24e124136f484616",
    "sensor_type": "battery_level",
    "series": [["1788575382423470960", 93]]
  },
  {
    "sensor_id": "01a03992-e6c6-720a-8282-90221cad619e",
    "device_id": "019be6fa-0dee-72bf-9e40-96939916414e",
    "dev_eui": "24e124136f484616",
    "sensor_type": "air_rh",
    "series": [["1788575382423470960", 90]]
  },
  {
    "sensor_id": "01a03992-e6c6-720c-bb60-5dc4351b0b74",
    "device_id": "019be6fa-0dee-72bf-9e40-96939916414e",
    "dev_eui": "24e124136f484616",
    "sensor_type": "pulse_count",
    "series": [["1788575382423470960", 786123]]
  }
]
```

A device with **multiple sensors sharing one `sensor_type`** (e.g. `lnv3_svc`'s three solenoid valves, each reporting `solenoid_valve_raw`) is exactly the case `sensor_id` exists to disambiguate — each valve gets its own array entry, correctly split apart instead of being interleaved into one:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?deviceId=019f7092-1baa-779f-b712-a9b740f7a6bc&sensorType=solenoidValveRaw&interval=2880"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-75d6-91f4-7db26f0b3e29",
    "device_id": "019f7092-1baa-779f-b712-a9b740f7a6bc",
    "dev_eui": "0004a30b00e94314",
    "sensor_type": "solenoid_valve_raw",
    "series": [["1788575940464139841", 0]]
  },
  {
    "sensor_id": "01a03992-e6c6-75d7-a2f3-c9542d03824c",
    "device_id": "019f7092-1baa-779f-b712-a9b740f7a6bc",
    "dev_eui": "0004a30b00e94314",
    "sensor_type": "solenoid_valve_raw",
    "series": [["1788575940464139841", 0]]
  },
  {
    "sensor_id": "01a03992-e6c6-75d8-96dc-dd07a027d9df",
    "device_id": "019f7092-1baa-779f-b712-a9b740f7a6bc",
    "dev_eui": "0004a30b00e94314",
    "sensor_type": "solenoid_valve_raw",
    "series": [["1788575940464139841", 0]]
  }
]
```

A caller that only needs `sensor_id`/`sensor_type`/`series` — e.g. to keep dashboard payloads small when it already has device/asset context client-side — can drop every optional key with `?fields=` (empty):

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?sensorType=airTemp&interval=5&fields="
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-754d-9428-7cccd6811011",
    "sensor_type": "air_temp",
    "series": [["1788575862307522774", 16.45]]
  }
]
```

> **Note on data density:** LoRaWAN devices report far less often than WiFi/IP ones (duty-cycle limits on the radio side), so a short `interval` can legitimately return zero LoRaWAN points while IP-based devices for the same `sensorType` show up fine. Widen the time range if you expect a specific LoRaWAN device to be present and it's missing.

#### Example: absolute calendar range, whole day (`startTime`/`endTime`)

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?sensorType=airTemp&startTime=2026-09-04&endTime=2026-09-05&limit=2"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-7576-8fa0-68c320a4cc2f",
    "device_id": "019e3bb0-142c-729b-a6a2-3c7198c217f5",
    "dev_eui": "f8033201000385fa",
    "sensor_type": "air_temp",
    "series": [["1788566354168331925", 15.95]]
  },
  {
    "sensor_id": "01a03992-e6c6-71ea-9b2e-6fffb5ef67ab",
    "device_id": "019be6fa-0dee-729a-84c8-e468fd11af62",
    "dev_eui": "24e124136f483595",
    "sensor_type": "air_temp",
    "series": [["1788559431974862346", 20]]
  }
]
```

#### Example: absolute calendar range, precise date **and** time

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?sensorType=airTemp&startTime=2026-09-04T14:00:00Z&endTime=2026-09-04T15:00:00Z&limit=2"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-7516-ae53-a980e491d388",
    "device_id": "019e3bb0-142c-7101-9e95-df3c3c7ebe27",
    "dev_eui": "f803320100028a5f",
    "sensor_type": "air_temp",
    "series": [
      ["1788532992862696577", 29.75],
      ["1788532991641037386", 29.75]
    ]
  }
]
```

#### Example: multiple tag filters combined (`AND`)

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors?deviceModel=em500_swl&sensorType=waterLevel&interval=60"
```

```json
[
  {
    "sensor_id": "01a03992-e6c6-722b-abfc-a4e24894f57d",
    "device_id": "019be6eb-b5bc-7751-839b-c05990ec0ea6",
    "dev_eui": "24e124126f422141",
    "sensor_type": "water_level",
    "series": [["1788575077136011094", 0.73]]
  }
]
```

#### Error responses

| Status | Cause                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `400`  | Unknown `org`, `domain`, or `resource`; the resource doesn't support shaped output (`iot/audit` — use `/export?format=...`); invalid/missing `interval`/`limit`; `interval` combined with `startTime`/`endTime`; `startTime`/`endTime` given without the other, invalid, or out of order; the range spans more than 366 days; or a single day within the range is still too wide (scans too many Parquet files even after per-day chunking — narrow further or add a tag filter) |
| `502`  | InfluxDB query failed for another reason (e.g. bad auth token, database unreachable)                                                                                                                                                                                                                                                                                                                                                                                             |

### `GET /api/timeseries/v0.4/:org/:domain/:resource/export`

Same path params, same query params (tag filters, `interval` or `startTime`/`endTime`, `limit`), same validation and safety caps — including the file-scan-limit `400` — as the endpoint above, but works for **every** supported resource, including `iot/audit` (which the base route rejects). Instead of JSON, it streams back **raw** bytes in the requested format for data-analytics tools (pandas, polars, DuckDB, Jupyter, etc.): it's `SELECT *` against the resource's underlying table (`sensor_data` for `iot/sensors`), native InfluxDB column types, as InfluxDB itself stores them. The one addition on top of the raw columns is a derived `device_type` column (`lorawan` or `ip`, from `provider`) — but only for resources whose table actually has a `provider` column (`iot/sensors`); `iot/audit` exports have no `device_type`, since there's nothing to derive it from. Since this is a genuine `SELECT *`, `sensor_id` needs no special handling here the way it does on the base endpoint: it simply appears as a normal column once real data has it, and is absent from the exported columns entirely until then — the same live-schema-check that gates it elsewhere still applies to an explicit `?sensorId=` filter, though (a `400` if it isn't queryable yet), since that's shared filter-resolution logic, not part of the `SELECT *` itself.

**`?format=` is required** and selects the output — `parquet`, `csv`, or `json`; anything else (or omitting it) is a clean `400`. It's a query param rather than a path segment (`/export/:format`, as it used to be): all three formats run the exact same query over the exact same rows, only the serialization differs, which is the same "same resource, different representation" reasoning `shape` follows elsewhere in this API — unlike `/export` itself, which really is a different code path from the base endpoint (raw `SELECT *`, no series grouping) and so stays a distinct path.

| `format`  | Content-Type       | Day-chunking                                                 | Notes                                                                                                                                                                                                                                       |
| --------- | ------------------ | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `parquet` | `application/zip`  | Yes — one `.parquet` file per UTC day, bundled into a `.zip` | Binary, columnar, per file. `time` is a native timestamp column — no precision-loss concerns like the JSON endpoint has, since Parquet isn't limited to float64. See "Why a zip of files, not one merged file" below.                       |
| `csv`     | `text/csv`         | Yes                                                          | Text. `time` is RFC3339 with nanosecond-precision decimals, e.g. `2026-08-18T14:46:18.457631609`. Chunked results are merged into a single CSV with exactly one header row (the header from later chunks is stripped before concatenating). |
| `json`    | `application/json` | Yes                                                          | A flat JSON array of raw rows (not grouped by device like the main endpoint) — `time` as RFC3339, same as `csv`. Chunked results are merged by concatenating each chunk's array before re-serializing.                                      |

#### Why a zip of files, not one merged file (`parquet`)

`csv`/`json` merge their day-chunks the cheap way — string/array concatenation (strip the repeated header, `[...a, ...b]` the arrays). A Parquet file can't be merged that way: its binary footer (schema + row-group offsets) has to describe the whole file correctly, so pasting two Parquet byte streams together produces a corrupt file, not a valid bigger one — actually merging them means re-encoding row groups with an Arrow/Parquet-writing library, which this API doesn't depend on.

Rather than add that dependency, or fall back to a single unchunked query (which just re-exposes the file-scan-limit `400` this whole day-chunking mechanism exists to avoid), each day's Parquet bytes are kept as their own independently-valid file — `<table>_<YYYY-MM-DD>.parquet` — and bundled into one `.zip` per request. This isn't a workaround: it's the same "partitioned dataset" shape big-data tools already read natively without any merge step —

```python
import duckdb
df = duckdb.read_parquet("extracted/*.parquet").df()   # or a pyarrow.dataset / pd.concat over the files
```

— and it's arguably _better_ for read performance than one merged file, since the engine can skip whole files by name/date instead of scanning row-group metadata inside one large blob. The zip container itself is built with **no compression** (store mode): Parquet's column chunks are already Snappy/Zstd-compressed internally, so compressing the zip on top would cost CPU for close to no size reduction. A day with zero matching rows is simply omitted from the zip rather than included as an empty file.

#### `limit` means rows-per-day for `parquet`, not rows-total

Every other endpoint in this API (base and `csv`/`json` export) treats `limit` as a budget for the **whole request** — day-chunks are queried newest-first and capped so the total never exceeds `limit`, regardless of how many days that spans. `parquet` export can't do that: since each day's file is never parsed after being fetched (that's the whole point — no re-encoding), there's no cheap way to trim rows out of an already-downloaded Parquet file the way a JSON array or CSV's lines can be sliced. So for `parquet` specifically, **`limit` caps each day's own `sensor_data` query independently** — `?limit=20` over a 3-day range can return up to 60 rows total (20 for each day that has data), not a strict 20. This is a genuine, deliberate difference from the rest of the API; if you need a hard total-row cap, use `csv`/`json` instead.

**Filename:** the response's `Content-Disposition` header suggests `<org>_<database>_<table>_<timestamp>.<extension>`, e.g. `imt_iot_sensors_sensor_data_20260819T170149Z.zip` for `parquet` (`.csv`/`.json` for those formats). The timestamp is UTC in ISO 8601 basic format (`YYYYMMDDTHHMMSSZ` — no `:`, since that's invalid in Windows filenames) and marks when the export was generated, not a data timestamp; org/database/table are included so downloaded files are self-describing and multiple exports (even of the same table, from different orgs or moments) never collide on disk.

This exists so external tools never need the InfluxDB token: they call this endpoint (which is what should get its own auth/access-control layer later, e.g. per-partner API keys) and the token stays server-side, exactly as described in the warning above. InfluxDB3 Core's `/api/v3/query_sql` natively supports Parquet/CSV/JSON output, so this is a thin, safe passthrough (plus the zip bundling for `parquet`) — not a filesystem/object-storage integration.

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export?format=parquet&sensorType=airTemp&interval=5" \
  -o air_temp.zip
```

```python
import duckdb, zipfile

with zipfile.ZipFile("air_temp.zip") as z:
    z.extractall("air_temp")

df = duckdb.read_parquet("air_temp/*.parquet").df()
```

Or straight from the response, without touching disk:

```python
import duckdb, requests, io, zipfile

resp = requests.get(
    "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export",
    params={"format": "parquet", "sensorType": "air_temp", "interval": 5},
)
z = zipfile.ZipFile(io.BytesIO(resp.content))
tables = [duckdb.read_parquet(io.BytesIO(z.read(name))).df() for name in z.namelist()]
```

Verified with a single-day range (one file in the zip):

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export?format=parquet&sensorType=airTemp&interval=5" -o day.zip
unzip -l day.zip
#   Length      Date    Time    Name
# ---------  ---------- -----   ----
#      5870  09-05-2026 00:03   sensor_data_2026-09-05.parquet
```

And a 2-day range (two `sensor_data` files, newest first):

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export?format=parquet&deviceId=019f7092-1baa-779f-b712-a9b740f7a6bc&startTime=2026-09-03&endTime=2026-09-05" -o two_days.zip
unzip -l two_days.zip
#   Length      Date    Time    Name
# ---------  ---------- -----   ----
#      8118  09-05-2026 00:03   sensor_data_2026-09-04.parquet
#      8030  09-05-2026 00:03   sensor_data_2026-09-03.parquet
```

`csv`/`json` export, verified against a device with multiple sensors sharing one `sensor_type` (three solenoid valves, each correctly getting its own row via `sensor_id`, not interleaved):

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export?format=csv&deviceId=019f7092-1baa-779f-b712-a9b740f7a6bc&sensorType=solenoidValveRaw&interval=2880&limit=1"
```

```
dev_eui,device_id,device_model,mac_address,provider,sensor_id,sensor_type,time,value_bool,value_float,value_int,device_type
0004a30b00e94314,019f7092-1baa-779f-b712-a9b740f7a6bc,lnv3_svc,,chirpstackv4,01a03992-e6c6-75d7-a2f3-c9542d03824c,solenoid_valve_raw,2026-09-05T03:03:05.870159970,,,0,lorawan
```

A wide, unfiltered, multi-day `csv`/`json` export — the kind that used to `400` before day-chunking — works via the same chunk-and-merge as the base endpoint, verified with a single header row and a valid single JSON array:

```bash
curl "http://localhost:3333/api/timeseries/v0.4/imt/iot/sensors/export?format=csv&startTime=2026-09-02&endTime=2026-09-05&limit=20" \
  -o three_days.csv
# 1 header line + 20 data rows, not 3 headers + 20 rows
```
