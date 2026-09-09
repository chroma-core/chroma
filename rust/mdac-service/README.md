# Token bucket service

An Axum harness around `mdac::TokenBucket`, using the frontend service's shared-state
router, JSON extraction, body limit, TCP serving, and graceful shutdown pattern.
The library exports `router(Arc<TokenBuckets>)` for embedding and `serve` for running
on an existing listener. The binary handles configuration, logging, and SIGTERM/SIGINT.
`Config::buckets()` constructs the shared registry.

Run from the repository root:

```sh
CONFIG_PATH=rust/mdac-service/sample_config.yaml cargo run -p mdac-service --bin token_bucket_service
```

Test a deployment against Modal's staging environment from the repository root:

```sh
modal deploy --env staging rust/deploy_mdac.py
```

Modal builds `rust/Dockerfile.mdac` remotely. Its builder stage runs the release
Cargo build, and its runtime stage copies `token_bucket_service` into the image.
The image also copies `rust/mdac-service/config/modal-main.yaml` to
`/config.yaml`, which Figment loads through `CONFIG_PATH` when the service starts.
Merges to `main` that change the service or its build inputs deploy automatically
to Modal's `main` environment through `.github/workflows/modal-mdac-deploy.yml`.
The workflow can also be run manually against the `main` branch.

Configuration comes from optional `CONFIG_PATH` YAML, overridden by `MDAC_`
environment variables. `buckets` maps exact names to required `capacity` and
`interval_ns` fields; `listen_address`
defaults to `127.0.0.1:8001`. For example, `MDAC_LISTEN_ADDRESS=0.0.0.0:8001` binds
all interfaces. Define different allowances for each bucket:

```yaml
buckets:
  "tenant-a/reads":
    capacity: 10
    interval_ns: 100000000
  "tenant-b/reads":
    capacity: 1000
    interval_ns: 10000000
```

Each bucket refills one token per its configured `interval_ns` nanoseconds and
is constructed full at startup. Definitions are loaded at startup; changes require a restart.
Zero values and burst durations exceeding `u64::MAX`
nanoseconds are startup errors.

Telemetry uses the frontend's `chroma-tracing` OTLP exporter and HTTP middleware:

```yaml
open_telemetry:
  endpoint: "http://localhost:4317"
  service_name: "mdac-service"
  filters:
    - crate_name: "mdac_service"
      filter_level: "trace"
    - crate_name: "token_bucket_service"
      filter_level: "trace"
```

The sample config enables OTEL; set `endpoint` to your collector's OTLP gRPC endpoint.
The service reuses `chroma_tracing::OpenTelemetryConfig` and
`chroma_tracing::init_server_otel_tracing`. Set `service_name` and `filters`
explicitly as shown; the shared defaults are `chromadb` and `chroma_frontend=trace`.
As in the frontend,
OTEL initialization also enables stdout tracing, panic reporting, and Tokio runtime
metrics. The HTTP middleware propagates incoming trace context and includes
`chroma-trace-id` on error responses. `RUST_LOG` overrides the tracing filters;
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` optionally overrides the metrics destination.
For local use without a collector, omit `open_telemetry` and set `stdout_tracing: true`.
Embedders initialize tracing once via `mdac_service::init_otel_tracing` or their host's
existing tracing setup before serving requests.

## API

`GET /api/v1/healthcheck` returns HTTP 200 independently of token availability.

`POST /api/v1/token-bucket/put-back-and-drain` accepts JSON with a required string
`name` and required unsigned 32-bit `excess` and `need` fields:

```sh
curl -i http://127.0.0.1:8001/api/v1/token-bucket/put-back-and-drain \
  -H 'Content-Type: application/json' \
  -d '{"name":"tenant-a/reads","excess":0,"need":10}'
```

The name selects an existing configured bucket. Unknown names
return HTTP 404 with `{"admitted":false}` without creating state or applying refunds.
Names match exactly, including
case and whitespace; empty strings are valid names. Different names have independent
allowances, and concurrent requests for the same name share one bucket.

The service first refunds `excess`, capped at capacity, then attempts the entire
drain. Success returns HTTP 200 with `{"admitted":true}`. Insufficient allowance
returns HTTP 429 with `{"admitted":false}`. **The refund is retained even on 429.**
Use `excess: 0` for a drain and `need: 0` for a refund. Requests larger than 1 KiB
are rejected; malformed bodies, unknown fields, and out-of-range values are
rejected before changing the balance.

## Deployment semantics

All clients of one process share one registry of named buckets. State is in memory:
restart restores full bursts, and each additional replica creates independent
allowances. A shared limit requires routing all requests for its name to the same
instance. All configured buckets are constructed at startup and retained for the
process lifetime. The registry is immutable; requests look up the name and update
that bucket's atomic state without a registry lock or background maintenance.

This is an internal service for trusted callers; it does not authenticate clients
or verify that refunded tokens were previously drained. Place it behind the
deployment's access controls before exposing it beyond trusted callers.

Updates are not idempotent. A lost response may follow a committed mutation;
automatically retrying can double-drain or double-refund. A caller receiving 429
must clear the already-applied `excess` before retrying its drain. The service
does not persist request IDs or deduplicate retries.

Run the configuration and real HTTP integration tests with:

```sh
cargo test -p mdac-service
```
