# syntax=docker/dockerfile:1
#
# Multi-stage build — build context is the repo root (see .github/workflows/ci.yaml, which
# builds this with `context: .` / `file: docker/Dockerfile`), so every COPY below is relative to
# the repo root, not this docker/ directory.
#
# Stages:
#   base      — shared starting point, nothing built yet.
#   install   — dependencies only, split into a "dev" tree (full, for typechecking) and a "prod"
#               tree (--production, for the final image) so Docker's layer cache only invalidates
#               on package.json/bun.lock changes, not on every source edit.
#   typecheck — runs `tsc --noEmit` as a build-time gate. Its output is never copied into the
#               final image — a type error fails the docker build (and so CI) instead of shipping.
#   release   — the actual runtime image: prod-only node_modules, source, no devDependencies,
#               no build tooling, non-root user.

FROM oven/bun:1-alpine AS base
WORKDIR /usr/src/app

FROM base AS install

RUN mkdir -p /temp/dev
COPY package.json bun.lock /temp/dev/
RUN cd /temp/dev && bun install --frozen-lockfile

RUN mkdir -p /temp/prod
COPY package.json bun.lock /temp/prod/
RUN cd /temp/prod && bun install --frozen-lockfile --production

FROM base AS typecheck
COPY --from=install /temp/dev/node_modules node_modules
COPY package.json bun.lock tsconfig.json ./
COPY src src
RUN bun run typecheck

FROM base AS release
ENV NODE_ENV=production
COPY --from=install /temp/prod/node_modules node_modules
COPY --from=typecheck /usr/src/app/src src
COPY package.json .

# influxdb.json is deliberately NOT copied in (see .dockerignore and the token-security warning
# in README.md) — same pattern as DEVICE_REGISTRY_FILE in mqtt-topic-rewrite-lns-chirpstackv4's
# own Dockerfile: a fixed, well-known absolute path baked into the image, with the actual file
# supplied at runtime (a k8s Secret volume — see README.md's "Docker / Kubernetes" section for
# why Secret, not ConfigMap, and the subPath gotcha that silently breaks hot-reload). src/config.ts
# re-reads this path every INFLUXDB_CONFIG_REFRESH_SEC and fails fast at startup if nothing's
# there, same as running locally with a missing influxdb.json.
ENV INFLUXDB_CONFIG_FILE=/etc/api/influxdb.json
# sensors.json/devices.json/assets.json follow the exact same pattern, colocated in the same
# /etc/api directory — src/sensors.ts's default (a relative ../parse-mqtt-to-influxdb3/*.json
# path) only ever worked for a local checkout sitting next to this repo, never inside a
# container. Unlike influxdb.json these hold no secrets, but they still live in the same mounted
# directory (rather than their own volume) so the whole-directory-mount rule that makes
# influxdb.json's hot-reload work applies to them too — see the Secret/subPath note above.
ENV SENSORS_CONFIG_FILE=/etc/api/sensors.json
ENV DEVICES_CONFIG_FILE=/etc/api/devices.json
ENV ASSETS_CONFIG_FILE=/etc/api/assets.json
USER bun
EXPOSE 3333/tcp
ENTRYPOINT ["bun", "run", "src/index.ts"]
