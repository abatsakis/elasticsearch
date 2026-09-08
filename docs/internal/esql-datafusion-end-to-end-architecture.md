# ES|QL + DataFusion — end-to-end architecture

**Status:** draft  
**Companion docs:** [scan-leaf](esql-datafusion-scan-leaf-proposal.md) · [branch planner plan](esql-datafusion-branch-planner-plan.md)

This document is the single picture: how an ES|QL query moves from REST through Elasticsearch planning, splits into a Lucene arm and a DataFusion arm, executes on shards and on a pool of DataFusion workers, and merges back into one result.

---

## 1. Invariant

| Always Elasticsearch | Never DataFusion |
|---|---|
| ES|QL language (parse, analyze, verifier) | Parse ES|QL or SQL |
| Dataset catalog, glob, schema merge, authz | List/glob the bucket |
| Lucene shards | Open a Lucene index |
| `UnionAll` and combiner `Aggregate` | Merge with Lucene |
| REST, CCS dataset gate, federation feature flags | Cluster membership with ES |

**Elasticsearch produces the logical plan for the federated branch. DataFusion physicalizes and runs that branch only. It must not change results.**

DataFusion does **not** run a distributed query engine in v1. Elasticsearch already distributes: it assigns **files** to N independent workers. Each worker is a single-node DataFusion runtime (multi-threaded locally). Workers do not shuffle with each other. Partial aggregates meet on the ES coordinator, next to Lucene partials.

Today the coordinator already sends a **plan**, not ES|QL text (`DataNodeRequest` carries `FragmentExec(LogicalPlan)`). The federated arm uses the same idea: a DataFusion IR + file snapshot, over Flight instead of `indices:data/read/esql/data`.

---

## 2. System context

```
                       ┌──────────────────────────────────────┐
  client               │         Elasticsearch cluster          │
  POST /_query         │                                        │
       │               │  coordinating node                     │
       │               │    parse → analyze → logical opt        │
       │               │    BranchCutter                         │
       │               │         │                               │
       │               │         ├──── Lucene arm ────┐          │
       │               │         │                    │          │
       │               │         └──── DF arm ────────┼───┐      │
       │               │                              │   │      │
       │               │  data nodes                  ▼   │      │
       │               │    DataNodeRequest + shards      │      │
       │               │    localPlan() → Lucene scan     │      │
       │               │    Pages ───────────────────┐    │      │
       │               └─────────────────────────────┼────┼──────┘
       │                                             │    │
       │               ┌─────────────────────────────┼────┼──────┐
       │               │  DataFusion worker pool (N)      │      │
       │               │    Flight submit + DoGet    ◄────┘      │
       │               │    IR → ExecutionPlan                   │
       │               │    S3 GET assigned files only           │
       │               │    RecordBatches                        │
       │               └──────────────┬──────────────────────────┘
       │                              │
       │               object store   ▼
       │               (S3, …)    parquet objects
       │
       ▼
  ES coordinator: ArrowToEsql → Pages
                  UnionAll / combiner Aggregate
                  remaining ES|QL ops → response
```

Three independent scale axes:

- **ES search/data nodes** — Lucene parallelism (shards).
- **DataFusion workers** — federated scan/partial-agg parallelism (file buckets).
- **Object store** — throughput of `GET`/`GET range`.

Adding ES search nodes must not change DF file assignment. Adding DF workers must not change Lucene shard routing.

---

## 3. End-to-end: mixed query (the full path)

Example:

```esql
FROM metrics, s3_logs
| WHERE ts > now() - 1 day AND status >= 500
| STATS bytes = SUM(bytes) BY host
| SORT bytes DESC
| LIMIT 20
```

`metrics` is a Lucene index. `s3_logs` is a federation dataset.

### 3.1 Coordinator: language and catalog (unchanged)

1. **REST** `TransportEsqlQueryAction` → `PlanExecutor.esql` → `EsqlSession.execute` (search thread).
2. **Parse** ES|QL. Views / `IN` subqueries resolved.
3. **Dataset rewrite** (`DatasetResolver` / `DatasetRewriter`): `s3_logs` becomes `UnresolvedExternalRelation` if federation is on and the caller may `read` it. Unauthorized explicit names look like `Unknown index`. Remote `cluster:dataset` is not rewritten (CCS later rejects).
4. **Pre-analysis (parallel):**
   - **Lucene:** field-caps for `metrics` (`IndexResolver`).
   - **Federated:** `ExternalSourceResolver` on `esql_external_io` — glob `s3_logs` resources, partition-prune from `WHERE` hints, footer/schema reconcile, optional stats. Listing/schema caches apply. **This is still Elasticsearch**, not DataFusion.
5. **Analyze / verify:** `EsRelation` + `ExternalRelation`. Reject `TS` / dataset-as-lookup-target / KNN-on-dataset, etc.

Federation off: skip step 3; `s3_logs` is an unknown index. No DF traffic.

### 3.2 Coordinator: logical optimize (the important rewrite)

`LogicalPlanOptimizer` (surrogates first: `AVG` → `SUM`/`COUNT`, …):

- `PushDownFilterAndLimitIntoUnionAll` copies pushable `WHERE` (and some `LIMIT`) onto **each** leaf.
- `PushAggregateThroughUnionAll` (only `isLeafUnionAll`) splits `STATS` into **per-arm partials** + a **combiner** above the union.

Sketch after those rules:

```
TopN[bytes DESC, 20]
  Aggregate[bytes = SUM($$partial$$bytes)] BY [host]     ← combiner, ES only
    UnionAll
      Aggregate[$$partial$$bytes = SUM(bytes)] BY [host] ← Lucene inner
        Filter[ts, status]
          EsRelation[metrics]
      Aggregate[$$partial$$bytes = SUM(bytes)] BY [host] ← federated inner
        Filter[ts, status]
          ExternalRelation[s3_logs]
```

`SORT`+`LIMIT` stay above the combiner (authoritative TopN). Per-arm limits, if any, are hints.

### 3.3 Coordinator: cut and map

**BranchCutter** (after logical opt, before or as part of `Mapper`):

- Walk up from each `ExternalRelation` while the subtree is federated-only and allowlisted.
- Replace the federated inner `Aggregate+Filter+ExternalRelation` with `DataFusionExec(ir, fileList, schema)`.
- Leave `UnionAll`, combiner, TopN, and the Lucene inner tree untouched.

**Mapper** (existing):

- Lucene inner tree → `FragmentExec(LogicalPlan)` + `ExchangeExec` (same as today).
- `DataFusionExec` stays on the coordinator (`CoordinatorOnlyStrategy` for the DF arm). It is **not** stuffed into `DataNodeComputeHandler` as if it had shards.

Allowlisted v1 operators on the DF side: `Scan`, `Filter`, `Project`, algebraic `Aggregate` (`COUNT`/`SUM`/`MIN`/`MAX`), `Limit`. Non-allowlisted (`GROK`, runtime `MATCH`, MV, …): cut **below** that operator; DF only scans; ES runs the rest.

### 3.4 Lucene arm (today’s data-node path)

`ComputeService` / `DataNodeComputeHandler`:

1. `searchShards` → nodes that hold `metrics` shards.
2. Open exchange sinks.
3. `DataNodeRequest`: `PhysicalPlan` = `ExchangeSinkExec` wrapping `FragmentExec(logical inner aggregate)`, plus **shard ids**, alias filters, `Configuration` (query string is for logging/settings, **not re-parsed**).
4. Data node `PlannerUtils.localPlan()`: local logical opt using `SearchStats` → `LocalMapper` → local physical opt (`PushFiltersToSource` → Lucene query) → drivers.
5. Pages flow back on `internal:data/read/esql/exchange`.

Index-role isolation for **external** scans does not apply here; this is normal Lucene ES|QL.

### 3.5 Federated arm: assign files, not shards

Still on the coordinator, using the `FileList` from step 3.1:

1. Live workers from the **worker registry** (data-source or cluster setting: list of Flight endpoints). Empty pool → fail closed (optional explicit fallback to Java `FormatReader`).
2. **LPT / round-robin** on file sizes (`estimatedSizeInBytes` when known) → `N` buckets. Same algorithm idea as `WeightedRoundRobinStrategy`, **not** `DiscoveryNode`s.
3. **One job per worker**, many files each. Never one RPC per file.
4. For each bucket: Flight **submit** then **DoGet** (see §6).
5. `ArrowToEsql` turns RecordBatches into Pages with combiner-compatible column names (`$$partial$$bytes`, `host`).

Workers read object storage **directly**. ES does not proxy parquet bytes.

### 3.6 Merge

Coordinator compute:

```
TopN
  Aggregate[SUM($$partial$$bytes)] BY host     ← Lucene partials ∪ DF partials
    UnionAll / exchange gather
      Pages from ES data nodes
      Pages from N Flight streams
```

Algebraic merge: `COUNT`/`SUM` → `SUM` of partials; `MIN`/`MAX` stay `MIN`/`MAX`. Sketch aggs (`COUNT_DISTINCT`, t-digest) stay **out of mixed v1** unless DF emits ES `PARTIAL_AGG` bytes.

Then remaining ES|QL (here TopN) and the REST response.

---

## 4. End-to-end: dataset-only

```esql
FROM s3_logs
| WHERE status >= 500
| STATS n = COUNT(*) BY host
```

No `EsRelation`, no `UnionAll`. After logical opt the whole remaining tree can be one `DataFusionExec` if every operator is allowlisted.

**v1 still uses file buckets (mode B):** each worker runs `COUNT(*) BY host` in **PARTIAL** mode; a small ES combiner `SUM`s the `n` columns. That reuses the same combiner machinery as mixed queries and does **not** require DataFusion distributed shuffle.

**Later mode A:** one job, full file list, DF-internal shuffle, `FINAL` agg. ES is a single Flight client. Optional; not required for correctness.

If `GROK` sits in the pipeline, the cut is below grok: DF scans+filters; ES groks and aggregates (scan-leaf performance, still correct).

---

## 5. What travels on the wire

### Lucene: `DataNodeRequest`

Transport action `indices:data/read/esql/data`:

- `PhysicalPlan` (`ExchangeSinkExec` + `FragmentExec`)
- shard list, alias filters
- `Configuration` (pragmas, locale, original query for logs)
- not used for DF

CCS adds `ClusterComputeRequest` (`RemoteClusterPlan`) to the remote coordinator; that node then issues `DataNodeRequest`s. **Remote datasets are unsupported**; only local datasets enter the DF arm.

### DataFusion: Flight job (not ES|QL, not SQL)

Large payloads must not use `FlightSplit`’s 16 KiB ticket cap.

1. `DoAction("submit_job", payload)` → `job_id`
2. `DoGet(job_id)` → RecordBatch stream
3. Cancel = abort the Flight call; worker drops the session

Payload (versioned):

| Field | Source |
|---|---|
| IR tree | BranchCutter / translator (scan, filter, project, partial agg, …) |
| `paths[]` | File bucket assigned to this worker |
| `schema` / `DeclaredReadSpec` | `ExternalSourceResolver` + dataset mapping |
| `agg_mode` | `PARTIAL` (v1) |
| `_index` stamp | Dataset name |
| budgets | row/byte caps, timeout identity |

The worker must emit **exactly** that schema (including `$$partial$$*` names the combiner already allocated). ES does not re-resolve names after Flight.

`GetFlightInfo` multi-endpoint splitting is unused: ES already split by files.

---

## 6. DataFusion worker (single-node server)

DataFusion is a **library**. The worker is a thin Flight server around it. It is not Ballista, not Flight SQL, not a catalog.

```
                    ┌─────────────────────────────────────────┐
                    │           df-worker process              │
 ES                 │  Flight: health, submit_job, DoGet      │
 ──────────────────►│  Job table: id → session                 │
                    │           │                              │
                    │           ▼                              │
                    │  IR decoder (fail closed on unknown op)  │
                    │           │                              │
                    │           ▼                              │
                    │  ExecutionPlan builder (physicalize)     │
                    │           │                              │
                    │           ▼                              │
                    │  Per-job RuntimeEnv                      │
                    │    memory pool ← ticket budget           │
                    │    target_partitions ← local cores       │
                    │    object_store ← S3 get/get_range only  │
                    │           │                              │
                    │           ▼                              │
                    │  RecordBatches → DoGet                   │
                    └─────────────────────────────────────────┘
```

**Lifecycle:** idle → submit (register job) → DoGet (execute + stream) → EOS/error/cancel → drop session. No state across jobs. Jobs do not share a `SessionContext`.

**Local parallelism only:** `target_partitions` uses cores on that box (parquet row-groups, hash agg). There is no `RepartitionExec` to another worker.

**I/O:** `object_store` with instance role / IRSA. Ticket contains paths, not secrets. No `list()`. Missing object fails **that job**; ES may mark the query partial.

**Concurrency:** several jobs per process if each has its own memory pool and a cap on in-flight file opens. Process crash fails only that replica’s in-flight buckets.

**Stickiness:** ES’s registry is an explicit list of worker addresses. A L4 load balancer that hashes `DoGet` to a random pod breaks assignment.

---

## 7. IR → DataFusion operators (v1)

| ES|QL / IR | DataFusion |
|---|---|
| `Scan(paths, schema, projection)` | Parquet `FileScanExec` / `FileScanConfig` with **explicit files** |
| `Filter` | `FilterExec`; parquet stats pushdown when the expr is safe |
| `Project` / allowlisted scalar `Eval` | `ProjectionExec` |
| `Aggregate(PARTIAL, COUNT/SUM/MIN/MAX, groups)` | `AggregateExec` partial |
| `Limit` | `LimitExec` (hint on mixed) |
| Anything else | Job failure, not skip |

Logical optimizer features in DataFusion that can change ES|QL meaning stay **disabled**. Physical parquet/agg decisions are allowed.

---

## 8. Control vs data plane

| Concern | Owner |
|---|---|
| Parse, analyze, verify | ES coordinator |
| Glob, partition prune, schema | ES `ExternalSourceResolver` (`esql_external_io`) |
| Cut allowlisted federated subtree | ES `BranchCutter` |
| File → worker assignment | ES LPT on registry |
| Lucene execution | ES data nodes + compute drivers |
| Parquet decode + partial agg on S3 | DF workers |
| Combine partials, TopN, output | ES coordinator compute |
| Object bytes | S3 ↔ DF workers (not via ES) |
| Result bytes (small) | DF → ES Flight; Lucene → ES exchange |

Thread pools stay isolated: `SEARCH` / `esql_worker` for ES compute; `esql_external_io` for ES-side discovery only; DF uses its own runtime. Do not run DF decode on `esql_worker`.

---

## 9. Failure, partials, security

- **Dead worker at assign time:** skip it; if none live, fail (or explicit Java fallback).
- **Worker dies mid-stream:** that file bucket fails; `allow_partial_results` can mark the query partial via `EsqlExecutionInfo` — no silent missing groups.
- **Cancel:** client cancel → ES cancels `DataNodeRequest`s and Flight streams.
- **Breakers:** ticket budget on DF; `BlockFactory` on Pages ingested into ES. Both apply.
- **Authz:** dataset `read` on ES; DLS/FLS on datasets rejected as today. Workers may read any path in the ticket; IAM still applies. Do not put long-lived cloud secrets in the job payload.
- **Flight TLS/mTLS** before non-dev. Snapshot tests may use insecure, matching current `FlightConnector`.
- **Federation off:** no rewrite, no workers, feature looks absent.

---

## 10. Observability

- `PlanTelemetry.externalSource()` and `ExternalSourceMetrics` as today.
- EXPLAIN: Lucene `FragmentExec` and `DataFusionExec` as sibling arms; combiner on the coordinator; worker id + file counts on the DF arm — not fake Lucene shards.
- Per worker: assigned files/bytes, Flight duration, rows, errors.
- Distinguishing “bytes read from S3” (DF) vs “bytes ingested as Pages” (ES) is required to see whether pushdown is working.

---

## 11. Explicit non-goals (v1)

- ES|QL or SQL string to DataFusion
- DataFusion distributed shuffle / Ballista
- Lucene `TableProvider` inside DF
- JNI DataFusion in the ES JVM
- Sidecar-per-ES-node as the primary deploy
- Sketch aggs on mixed plans without ES `PARTIAL_AGG` parity
- Remote-cluster datasets
- DF listing the bucket or owning dataset CRUD

---

## 12. Worked data flow (bytes)

```
S3 objects  ──GET──►  DF workers (decode, filter, partial STATS)
                         │ RecordBatches (groups, not raw rows)
                         ▼
                    ES coordinator  ◄── Pages ── ES data nodes (Lucene partial STATS)
                         │
                         ▼
                    combiner + TopN
                         │
                         ▼
                    HTTP response
```

If the cut cannot include `STATS` (non-allowlisted op below it), DF streams **rows** and ES aggregates — correct, but the Flight hop carries much more data. That is why the allowlist and cutter exist.

---

## 13. Mapping onto existing code

| Step | Code |
|---|---|
| Query entry | `TransportEsqlQueryAction`, `EsqlSession` |
| Dataset rewrite | `DatasetResolver`, `DatasetRewriter` |
| File/schema discovery | `ExternalSourceResolver`, `ExternalSourceCacheService` |
| UnionAll filter/agg pushdown | `PushDownFilterAndLimitIntoUnionAll`, `PushAggregateThroughUnionAll` |
| Lucene ship | `Mapper` → `FragmentExec`, `DataNodeRequest`, `PlannerUtils.localPlan` |
| New: cut | `BranchCutter`, `DataFusionExec`, expression allowlist |
| New: assign | worker registry, LPT buckets (not `AdaptiveStrategy` onto ES nodes) |
| New: client | Flight submit/DoGet, `ArrowToEsql` |
| New: worker | out-of-tree (or `esql-datasource-datafusion`) Flight + DataFusion + `object_store` |
| Feature gate | `Federation` (`esql.federation.enabled`) |

---

## 14. Related docs

- [Scan-leaf proposal](esql-datafusion-scan-leaf-proposal.md) — why DF as a scan runtime, worker registry, what not to plug (FormatReader JNI, SQL).
- [Branch planner plan](esql-datafusion-branch-planner-plan.md) — cutter algorithm, allowlist, phases, IR, testing, worker protocol details.
