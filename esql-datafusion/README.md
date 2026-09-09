# ES|QL DataFusion worker and rendezvous

Standalone Rust workspace. **Not** part of the Elasticsearch Gradle build.
Licensed under the Elastic License 2.0.

Elasticsearch remains the query planner and Lucene engine. These binaries are the federated scan pool:

- `esql-df-rendezvous` — workers heartbeat here; ES lists live `flight_addr`s
- `esql-df-worker` — Arrow Flight server wrapping DataFusion (single-node, no shuffle)

See `docs/internal/esql-datafusion-end-to-end-architecture.md` in the Elasticsearch tree.

## Build

Requires Rust 1.88+ (see `rust-toolchain.toml`). DataFusion 45 pulls Arrow 54.

```bash
cd esql-datafusion
cargo build --release
cargo test
```

## Run locally

```bash
# terminal 1
cargo run -p esql-df-rendezvous -- --bind 0.0.0.0:8090 --ttl-ms 5000

# terminal 2
cargo run -p esql-df-worker -- \
  --bind 0.0.0.0:47470 \
  --advertise 127.0.0.1:47470 \
  --rendezvous http://127.0.0.1:8090 \
  --worker-id local-1
```

List members:

```bash
curl -s http://127.0.0.1:8090/v1/workers | jq
```

## Flight API (v1)

| Call | Purpose |
|---|---|
| `DoAction("health")` | `{ status, version, inflight_jobs }` |
| `DoAction("submit_job")` | body = [`SubmitJobRequest`](crates/ir/src/lib.rs) JSON → `{ job_id }` |
| `DoGet` | ticket = utf-8 `job_id` → RecordBatches |
| `DoAction("cancel_job")` | `{ job_id }` |

v1 scan format: **parquet** only. `store.scheme` is `file` (local paths) or `s3` (IRSA/`AWS_*` env, no secrets in the job).

## Rendezvous API

| HTTP | Purpose |
|---|---|
| `GET /health` | liveness |
| `PUT /v1/workers/:id` | heartbeat `{ "flight_addr", "capacity"? }` |
| `GET /v1/workers` | live workers (TTL eviction) |
| `DELETE /v1/workers/:id` | graceful leave |

The rendezvous does **not** proxy Flight. Elasticsearch assigns files to a `flight_addr` and dials that worker directly.
