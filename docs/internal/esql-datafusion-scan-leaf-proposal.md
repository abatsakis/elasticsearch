# ES|QL DataFusion scan-leaf proposal

**Status:** draft  
**Scope:** federated (non-Lucene) scans only  
**Does not change:** ES|QL language, analyzer, verifier, Lucene execution, or `FROM index, dataset` union

## Summary

Keep ES|QL as the query language and compute engine. Keep Lucene as the indexed leaf. Add Apache DataFusion as a **scan runtime** for federated datasets: Elasticsearch discovers files, assigns them to a standing pool of N DataFusion workers, those workers read object storage, and Elasticsearch gathers Arrow batches into ES|QL Pages and unions them with Lucene Pages.

DataFusion does not parse ES|QL, does not plan Lucene shards, and does not replace `ComputeService`. It is a remote, independently scaled replacement for the Java object-store readers that sit under `ExternalSourceExec` today.

## Motivation

ES|QL Data Federation already queries Parquet/CSV/NDJSON in object storage through `ExternalRelation` → splits → `ExternalSourceExec`. Scan CPU, footer reads, and format decode run **on Elasticsearch nodes** (`esql_external_io` plus compute drivers). That couples federated scan capacity to the ES cluster, competes with search/ingest for heap and CPU, and reimplements a vectorized scan stack Elasticsearch does not need to own.

DataFusion is a vectorized scan and parquet engine. ES|QL is the language, the Lucene integration, and the coordinator. The useful composition is: **ES schedules, DF scans, ES unions.**

## Goals

- Preserve ES|QL syntax and semantics for every query, including mixed `FROM index, dataset`.
- Preserve the Lucene path (`EsRelation` → shard drivers) unchanged.
- Run federated scans on a pool of N DataFusion workers that scale independently of ES node count.
- Elasticsearch remains the source of truth for file discovery, schema reconciliation, authorization, and split assignment.
- Push only an allowlisted prefix into DataFusion (projection, filter, limit hints). Remainder stays ES|QL operators.
- Fail closed when a construct is not in the allowlist: execute it in ES|QL after the scan, never “close enough” in DataFusion.

## Non-goals

- Translating ES|QL to SQL and running the query in DataFusion.
- Replacing `ComputeService`, the mapper, or the exchange layer.
- Teaching DataFusion to read Lucene shards.
- DataFusion-owned cluster shuffle for `STATS` / `SORT` (ES `ExchangeExec` remains the gather).
- Sidecar-per-ES-node as the primary deployment (allowed later as an optimization, not the architecture).
- Pushing grok, enrich, lookup join, MV-specific functions, runtime `MATCH`, or spatial into DataFusion in v1.

## Current seams (what we reuse)

| Stage | Owner today | Change in this proposal |
|---|---|---|
| Parse, views, `FROM <dataset>` rewrite | `EsqlSession`, `DatasetResolver`, `DatasetRewriter` | None |
| Authz, DLS/FLS rejection, CCS remote-dataset gate | `EsqlResolveDatasetAction`, field-caps | None |
| File listing, partition prune, schema merge | `ExternalSourceResolver` | Keep on the ES coordinator |
| Logical leaf | `ExternalRelation` inside `FragmentExec` | None |
| Lucene leaf | `EsRelation` | None |
| Mixed query | `UnionAll` of the two leaves | None |
| Split discovery | `SplitDiscoveryPhase` / `FileSplitProvider` | Keep file-level splits; stop assigning them to ES `DiscoveryNode`s |
| ES node fan-out | `AdaptiveStrategy` → `DataNodeComputeHandler` | **Do not use** for DF scans |
| Scan operator | `LocalExecutionPlanner.planExternalSource` → Java `FormatReader` | Replace with Flight `DoGet` to DF workers |
| Arrow → Pages | `ArrowToEsql` (Flight path) | Reuse |
| Downstream `EVAL` / `STATS` / `SORT` | ES|QL compute | None (v1) |

The Flight connector already maps `GetFlightInfo` endpoints to `FlightSplit` tickets (`FlightSplitProvider`, `FlightConnector`). This proposal inverts that slightly: **Elasticsearch creates the tickets** (file buckets) and DataFusion workers consume them, instead of DataFusion inventing endpoints.

## Proposed architecture

```
                    ES|QL query
                         │
              EsqlSession (unchanged)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         EsRelation           ExternalRelation
         (Lucene)             (federated dataset)
              │                     │
              │                     │  coordinator
              │                     ├─ glob / schema / stats  (ES)
              │                     ├─ assign files → N buckets
              │                     └─ N Flight tickets
              │                     │
         ES data nodes              ▼
         shard scans         DF workers (standing pool)
              │              read assigned files from object store
              │                     │
              │              Arrow RecordBatches
              │                     │
              ▼                     ▼
              └──── Pages ──── UnionAll / Exchange ──── remaining ES|QL ops
```

### Control plane (Elasticsearch)

1. Dataset rewrite and analysis run exactly as today. The federated leaf is still `ExternalRelation` with a coordinator-resolved `FileList` and unified schema.
2. The coordinator **does not** send `ExternalSourceExec` to ES search nodes for these scans.
3. A new assignment step (same algorithm as `WeightedRoundRobinStrategy.assignByWeight`, different worker list) partitions files onto N DataFusion workers from a **worker registry**, not from `DiscoveryNodes`.
4. Each worker receives **one job**: the list of `StoragePath`s, projection, optional pushed predicates, optional limit *hint*, schema/read spec, and dataset identity for `_index`.
5. The coordinator opens N streams (`DoGet`) and converts batches with `ArrowToEsql`.
6. Lucene fragments run in parallel on ES data nodes as today. `UnionAll` merges both.

One scheduler. ES search/index nodes stay on the Lucene branch. DF workers are not ES cluster members.

### Data plane (DataFusion workers)

- Standing processes exposing Arrow Flight (or Flight SQL with a custom ticket payload).
- Idle until a ticket arrives; no ES membership, no shard state.
- For each ticket: open the listed objects, apply projection and allowlisted predicates, stream RecordBatches, exit to idle.
- Intra-worker parallelism (threads per job) is DataFusion’s problem. Inter-worker parallelism is Elasticsearch’s file assignment.
- Workers read object storage **directly**. Elasticsearch does not proxy bytes.

### Worker registry

The N endpoints are configuration, not cluster state of Elasticsearch nodes. Suggested v1: data-source settings (list of Flight locations) with optional cluster-level defaults. Later: service discovery.

Required properties:

- Static or slowly changing list of `host:port`.
- Liveness probe before assignment (skip dead workers; if none live, fail the federated branch — do not silently scan on the coordinator unless an explicit fallback setting is on).
- No use of `DiscoveryNodeRole` / `INDEX_ROLE` eligibility. That predicate exists to keep S3 scans off ingest nodes; DF workers are a different fleet.

### File assignment

Reuse the existing LPT / round-robin idea:

- Input: file splits with `estimatedSizeInBytes` when known (listing / footer).
- Output: `Map<workerId, List<ExternalSplit>>` with **N entries**, N = live DF workers.
- **Batch per worker**, never one Flight RPC per file. 10k files and 8 workers → 8 tickets.

Do not also run `AdaptiveStrategy` on ES nodes for the same splits. Two assigners will disagree on `LIMIT` and partial aggregations.

`CoordinatorOnlyStrategy` is the right *ES* distribution choice: the coordinator (or a small dedicated gather set) is the Flight client. The fan-out is to DF, not to `DataNodeComputeHandler`.

### Pushdown allowlist (v1)

DataFusion may evaluate only what `FilterPushdownSupport` / projection / limit already mean for a scan leaf:

| Pushed | Where enforced |
|---|---|
| Column projection (`KEEP` / prune) | DF (must) |
| Partition prune | ES at glob time (already) |
| Simple comparison / `AND` / `IN` / `IS NULL` on physical columns | DF if `canPush == YES`; else ES `FilterExec` |
| `LIMIT` | Hint in the ticket for IO reduction; **authoritative limit after gather** so `LIMIT 100` is not `100 × N` |
| Ungrouped `COUNT`/`MIN`/`MAX` from footer stats | Keep today’s ES `PushStatsToExternalSource` on the coordinator **or** omit and scan; do not implement a second stats path in DF until proven equal |

Default for any other `Expression` is `NO`. Remainder stays in ES|QL. That is how mixed `FROM index, dataset` keeps one semantics.

### Union with Lucene

No special operator. `Mapper` already wraps both leaves in `FragmentExec`. After this change:

- Lucene fragment → ES data nodes → Pages (unchanged).
- Federated fragment → N DF streams → Pages on the coordinator.
- `UnionAll` / exchange merge as today.

Schema remains an ES analyzer job (`ResolveExternalRelations`, type widening, `union_by_name`). DataFusion must emit the **agreed** Arrow schema (ES supplies field names and types in the ticket). Conflicting types still fail in the verifier, not in DataFusion.

Metadata columns (`_index` = dataset name, `_file.path`, `_id`, …) are produced either by DF from ticket instructions or by a thin ES operator after the stream. v1 should specify this in the ticket so `_index` does not become a path or a worker id.

## Why not the alternatives

**Sidecar per ES node.** Couples DF capacity to ES search-node count. Still runs scan traffic next to search. Useful only if Flight to a remote fleet is the bottleneck and local sockets are required.

**DataFusion invents splits via `GetFlightInfo`.** Fights ES file discovery, partition pruning, schema cache, and dataset mappings. Two catalogs.

**Lower the whole federated fragment into DataFusion.** Higher performance later; v1 semantic risk (`STATS`, `EVAL`, MV, datetimes). Grow the allowlist after csv-spec parity on the scan leaf.

**JNI in the ES JVM.** Avoid in v1. Process isolation matches “own fleet,” keeps native crashes off ES nodes, and matches the existing Flight connector.

## Phased delivery

### Phase 1 — remote scan leaf (this proposal)

- Worker registry on the data source.
- Coordinator assignment of file buckets → N Flight tickets.
- `SourceOperator` that `DoGet`s those tickets and uses `ArrowToEsql`.
- Projection + filter allowlist; authoritative `LIMIT` after gather.
- Mixed `FROM index, dataset` integration tests.
- Federation gates unchanged (`esql.federation.enabled`, data-node backstop unused for this path because ES data nodes never see `ExternalSourceExec`).

### Phase 2 — pushdown hardening

- Expand `FilterPushdownSupport` with csv-spec tests on DF and Java readers for the same queries.
- Optional limit hint in DF.
- Cancellation and circuit-breaker propagation into Flight.

### Phase 3 — optional fragment offload

- Allowlisted `STATS BY` partials on DF workers, merge in ES|QL.
- Only after Phase 2 parity. Still no Lucene in DataFusion.

## Semantic contract

The federated leaf must match ES|QL dataset semantics already documented for Data Federation:

- Same type widening and `schema_resolution` strategies.
- Parquet LIST `null` elements dropped (current ES|QL quirk) — either DF matches this or ES post-processes. Do not “fix” it only on the DF path.
- Runtime `MATCH` / `_score` stay in ES|QL (row-by-row after scan) until explicitly designed.
- Dataset is not a `LOOKUP JOIN` target; `TS` is rejected — verifier unchanged.
- CCS: local datasets only — unchanged.
- `allow_partial_results`: a failed DF worker fails or partials **that worker’s files**, surfaced through existing `EsqlExecutionInfo` partial machinery, not a silent hole.

Circuit breakers: DF workers should honor a byte/row budget in the ticket; ES still accounts Pages in `BlockFactory` on ingest. Two budgets are OK; neither is optional.

## Security

- Dataset `read` and data-source credentials stay in Elasticsearch cluster state (encryption as today).
- **Do not** put long-lived cloud secrets in Flight tickets.
- DF workers need their own object-store identity (instance role / federated identity / operator-managed). The ticket names buckets and keys, not secrets.
- Flight TLS and auth between ES and DF (mTLS or token) is required before any non-dev deployment. v1 protocol can start insecure in snapshot tests only, matching `FlightConnector`’s current `forGrpcInsecure` — that must not ship as default.
- Authorization of *which* files remains ES (dataset resource patterns). A worker must not be asked to list the bucket; it only reads paths ES already resolved.

## Observability

Reuse `ExternalSourceMetrics` / `PlanTelemetry.externalSource()` for query-level counters. Add:

- per-worker assigned file count and bytes
- Flight stream duration and rows
- worker skipped/unhealthy
- bytes read as reported by DF (optional) vs bytes ingested into ES Pages

Profile (`EXPLAIN`) should show the federated fragment as a remote scan with worker assignment, not as Lucene shards.

## Testing

- Unit: assignment (LPT, dead workers, empty pool, single file, more workers than files).
- Spec: existing federation csv-spec against DF workers vs Java readers for the allowlisted subset; diffs are bugs.
- Mixed: `FROM index, dataset` row identity via `METADATA _index`.
- Failure: kill one DF worker mid-scan; cancellation; breaker.
- Do not require DF in the default `./gradlew test` matrix if the engine is an optional test cluster; gate a `javaRestTest` suite like other datasource QA.

## Open questions

1. **Gather location.** Coordinator-only Flight clients are simplest. If N streams overwhelm one node, a later step can run gather on a small set of ES nodes — still not “every search node reads S3.”
2. **Split granularity.** File vs Parquet row-group. v1 file-level is enough; row-group tickets are a Phase 2 size-balance tweak.
3. **Registry home.** Data-source settings vs independent “compute pool” cluster setting vs external discovery.
4. **Fallback.** If the DF pool is empty, fail vs fall back to today’s Java `FormatReader` on ES. Recommendation: fail closed, explicit setting to enable fallback.
5. **Ticket encoding.** Opaque bytes like `FlightSplit` (cap 16 KiB today) vs a first-class `DataFusionSplit` NamedWriteable with path lists. Large file lists will exceed 16 KiB — path lists should live in the DoGet payload or a side channel, not in a cluster-serialized split shipped to ES data nodes (they should not be shipped there at all).
6. **Who compresses/decodes.** DF native parquet vs ES codecs for `.csv.gz`. v1 can restrict the DF path to Parquet (and uncompressed/column-compressed parquet) and leave text formats on the Java readers.

## Code touch points (implementation map)

New:

- `DataFusionWorkerRegistry` + data-source settings for endpoints.
- `DataFusionAssignment` (LPT over workers; copy the idea from `WeightedRoundRobinStrategy`, do not subclass it onto `DiscoveryNode`).
- `DataFusionSplit` / ticket payload (file bucket + read spec).
- `DataFusionConnector` / `SourceOperatorFactoryProvider` registered via `DataSourcePlugin`.
- QA module alongside `esql-datasource-parquet` / `esql-datasource-grpc`.

Unchanged on purpose:

- `DatasetRewriter`, `Analyzer.ResolveExternalRelations`, `Mapper` wrapping `ExternalRelation`, Lucene `EsPhysicalOperationProviders`, `UnionAll`.
- `Federation` feature gates and dataset CRUD.

Avoid:

- Routing DF file splits through `DataNodeComputeHandler.handleExternalSourceRequest`.
- `FormatReader` JNI wrappers as the integration point.
- A second optimizer that rewrites ES|QL logical plans into DataFusion logical plans.

## Success criteria

- `FROM dataset | KEEP cols | WHERE simple | LIMIT n` matches Java-reader results on the same files.
- `FROM index, dataset METADATA _index` returns both sources with correct `_index` values.
- Adding DF workers reduces federated scan time without adding ES nodes.
- Adding ES search nodes does not change federated assignment (only Lucene parallelism).
- Queries that use non-allowlisted functions still succeed, with that work in ES|QL after the scan.
- With federation disabled, behavior remains “feature does not exist” (`Unknown index`, no DF traffic).
