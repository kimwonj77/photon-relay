# Photon relay

[한국어](README.ko.md)

A small Go round-robin relay for Photon-compatible geocoding, including ChibiGeo.
No database server, Redis, or geocoding dataset. Linux container; one replica.

## What it does

- GET /api and /reverse; round-robin among eligible providers.
- Per-provider request spacing (at least 1 second) and UTC daily/monthly caps.
- Durable quota reservation before each outbound attempt, including failed attempts.
- Bounded failover (at most four providers within 4.3 seconds total), cooldowns for errors and Retry-After.
- File-mounted provider keys sent only as X-Api-Key to their configured provider.
- /healthz and Prometheus /metrics. No coordinate, key or response-body logging.

## Configuration

Copy providers.example.json to providers.json and replace the placeholder Photon URL
with an endpoint you are permitted to use. This example is not a public-server list.
The ChibiGeo URL includes /v1/photon; key_file contains only your API key.
ChibiGeo is an equal round-robin member, not a fallback-only provider.

interval_ms is the minimum time between request starts. daily/monthly are local caps;
0 disables that particular cap. Examples are not promises about provider allowances.
Confirm your plan and operator policy. Quotas reset at midnight UTC / start of UTC month.

IMPORTANT: counts cover requests made by this relay only. If an API key has existing
usage or other consumers, reserve that usage in your operating budget before enabling it.
Do not rotate accounts or IPs to bypass provider limits.

## Run

Build with docker build -f Containerfile -t photon-relay:local .
Mount providers.json at /config/providers.json (read-only), your key at
/run/secrets/chibigeo (read-only), and persistent writable storage at /data.
The container runs as UID/GID 10001; both secrets and storage must be accessible to it.

CONFIG_FILE defaults to /config/providers.json; STATE_FILE to /data/quota.json.
Listen port is 8080. Keep the relay on a trusted internal network: it has NO inbound
authentication. Restrict access to application and monitoring clients; do not expose it publicly.

For Dawarich use PHOTON_API_HOST=photon-relay:8080 and PHOTON_API_USE_HTTPS=false
on the protected internal network. Provider credentials belong to the relay, not clients.
Deployment-specific DNS, secrets and manifests do not belong in this repository.

## Operational limits

Only one process/replica may own the quota file. A process lock prevents shared-path
double starts. Preserve /data across updates; deleting it resets accounting.
Corrupt or unwritable quota state fails closed. No distributed quota coordination,
response cache, automatic requeue or provider discovery is implemented.
Timeout is 1.7 seconds per upstream attempt; this may be too short for distant providers.
When all providers are unavailable or capped, clients receive 503, not fake empty data.
A valid empty FeatureCollection is returned without retry.

## Development and release

go test -race ./...
go vet ./...

CI tests and builds the container, then checks /healthz without contacting providers.
A vMAJOR.MINOR.PATCH tag publishes the tested linux/amd64 image to
ghcr.io/kimwonj77/photon-relay with its version and Git revision labels.
No cluster credentials or deployment commands are used by CI.
GHCR package visibility must be checked separately; a public Git repository alone
does not make a newly published package publicly pullable.

See [GitHub container registry docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
and [ChibiGeo API docs](https://chibigeo.com/docs/api-doc/).
