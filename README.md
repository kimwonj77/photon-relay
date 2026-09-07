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
Each upstream attempt gets at most 2.5 seconds within the 4.3-second total budget;
later attempts preserve an 800 ms fallback window where possible. Distant providers may still time out.
When all providers are unavailable or capped, clients receive 503, not fake empty data.
A valid empty FeatureCollection is returned without retry.

## Durable daily accounting and metrics

State remains a small JSON file on persistent storage, not SQLite or a separate DB.
Each provider has schema version 1, UTC day/month usage, pre-tracking baseline,
lifetime admitted attempts/results, latency buckets, and the last 35 recorded usage
days. Day/month views reset on UTC calendar boundaries even when idle; the next
admission archives the prior day. Clock regression cannot reopen an allowance.
State is locked per process, written to a temporary file, fsynced, renamed and its
directory fsynced. Failed persistence stops new upstream calls.

Version 0 state is migrated without clearing quota. The original bytes are retained
once at STATE_FILE.pre-v1 before the migrated file is saved. Existing usage becomes
a baseline: it may include external usage or a conservative first-day reservation,
so it is NOT advertised as newly observed traffic. Keep both files and the PVC.
This checkpoint is not an off-node backup. Do not restore old quota snapshots while
spending the same account allowance, or downgrade to 0.1.x: old binaries discard
new history/result fields on their next write.

Metrics use Prometheus text 0.0.4 with HELP/TYPE declarations and cumulative histogram
buckets; CI validates the endpoint with promtool. Provider names and fixed outcomes
are the only provider-series labels. No URLs, coordinates, API keys or user labels.

| Metric (prefix photon_relay_) | Meaning |
|---|---|
| daily_quota_used / monthly_quota_used | Budget charged, INCLUDING baseline reservations |
| daily_quota_baseline / monthly_quota_baseline | Pre-tracking/external reservation, NOT observed requests |
| daily_requests / monthly_requests | New admitted attempts in the current period, excluding baseline |
| attempts_total | Durable admitted attempts since schema upgrade |
| success_total / failure_total / outcomes_total | Durable observed results and fixed failure categories |
| upstream_request_duration_seconds | Durable histogram, including failed attempt latency |
| daily_remaining / monthly_remaining | Remaining local allowance; -1 means no local cap |
| quota_reset_timestamp_seconds | Next UTC day/month boundary; midnight UTC is 09:00 KST |
| eligible / next_eligible_timestamp_seconds / cooldown_until_seconds | Current eligibility and cooldown; not an uptime guarantee |
| last_success_timestamp_seconds / last_failure_timestamp_seconds / last_http_status | Last observed outcome; timestamp/status 0 means none |
| quota_storage_healthy / quota_state_write_errors_total / quota_state_size_bytes | Persistence health, write errors and file size |
| client_requests_total / inflight_requests / process_start_time_seconds | Client responses, current concurrency and process start; probes excluded |

Admitted attempts are persisted BEFORE sending. A crash in between may overcount a
network request; a crash before outcome persistence may miss the last result. There
is no exactly-once guarantee across an external API. Provider counters/histograms
survive restarts; client response and write-error counters restart with the process.
Daily/monthly request metrics are gauges, not counters: use attempts_total for rates.
Version 0.1's daily_requests included baseline; 0.2 separates it explicitly.

Useful PromQL (filter by your job/namespace as appropriate):

```promql
sum by (provider) (rate(photon_relay_attempts_total[5m]))
sum by (provider, outcome) (increase(photon_relay_outcomes_total[1h]))
histogram_quantile(0.95, sum by (provider, le) (rate(photon_relay_upstream_request_duration_seconds_bucket[15m])))
photon_relay_daily_remaining
```

See [Prometheus exposition rules](https://prometheus.io/docs/instrumenting/exposition_formats/).

## Development and release commands

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
