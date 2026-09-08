# ES|QL DataFusion branch planner — implementation plan

**Status:** draft  
**Depends on:** [scan-leaf proposal](esql-datafusion-scan-leaf-proposal.md) (Flight workers, file snapshot, `ArrowToEsql`)  
**Does not change:** ES|QL language, analyzer, verifier, Lucene shards, or `UnionAll` merge on the coordinator

## Goal

After ES|QL logical optimization, **cut every federated-only subtree** and run it in DataFusion against S3. Lucene arms stay ES. The coordinator still unions rows and **combines partial aggregates**.

This is not “send ES|QL text to DataFusion” and not “DF plans Lucene.” It matches how ES already ships work today: the coordinator sends a **plan**, data nodes do not re-parse the query. The federated arm becomes a DataFusion IR plus a file snapshot, in the same role as `FragmentExec` + shards.

## Why this, not whole-query DF

Mixed `FROM index, dataset | STATS` is already rewritten by `PushAggregateThroughUnionAll` into per-branch **partials** and a coordinator **combiner**. DataFusion can run the external inner aggregate. It must not run the combiner or the Lucene inner aggregate.

Dataset-only queries are the same cut with one arm: if the whole remaining tree is allowlisted, DF returns the result set (or partials that ES merges across workers).

## Invariant

> Elasticsearch produces the **logical** plan for the federated branch. DataFusion is the **physical planner and runtime** for that branch only. It must not change results.

Disable DF logical rewrites that are not ES|QL-equivalent. Physical parquet/agg decisions inside the branch are in scope.

---

## Current ES seams to reuse

| Seam | Role for this plan |
|---|---|
| `EsqlSession` parse → analyze → `LogicalPlanOptimizer` | Unchanged; cut **after** this |
| `PushDownFilterAndLimitIntoUnionAll` | Puts `WHERE` / some `LIMIT` on each leaf, including `ExternalRelation` |
| `PushAggregateThroughUnionAll` | Inner `Aggregate` per arm + combiner `SUM`/`MIN`/`MAX`/`FromPartial` |
| `PushDownUtils.isLeafUnionAll` | Detects heterogeneous `FROM` (direct `EsRelation` / `ExternalRelation` children) |
| `Mapper` → `FragmentExec(LogicalPlan)` on the wire | Today Lucene (and Java federation) ship **logical fragments**, not ES|QL text (`DataNodeRequest.plan`) |
| `PlannerUtils.localPlan()` | Data node: local logical opt → `LocalMapper` → local physical opt |
| Scan-leaf: worker registry, LPT file buckets, Flight, `ArrowToEsql` | Transport and workers for the DF branch |

`PushAggregateThroughUnionAll` already decomposes:

- Algebraic: `COUNT`/`SUM`/`MIN`/`MAX` → per-branch value, combiner uses `SUM`/`MIN`/`MAX`
- Sketches: `COUNT_DISTINCT`/`PERCENTILE`/`STDDEV` → `ToPartial` / `FromPartial` (`PARTIAL_AGG`)
- `AVG`/`MEDIAN` are surrogates **before** this rule; they decompose transitively

Mixed DF push of sketches is **out of v1** unless DF emits the **same** `PARTIAL_AGG` bytes as ES. Algebraic mixed `STATS` is in v1.

---

## Architecture

```
ES|QL text                         coordinator only (unchanged)
   ▼
analyzed + logically optimized plan
   ▼
BranchCutter: maximal DF-capable subtrees whose leaves are ExternalRelation
   ▼
┌──────────────────────────┬─────────────────────────────────┐
│ Lucene FragmentExec      │ DataFusionExec(IR, files)       │
│ DataNodeRequest + shards │ Flight tickets to DF workers    │
│ localPlan() → Lucene     │ DF physicalizes + runs on S3    │
└────────────┬─────────────┴──────────────┬──────────────────┘
             │         Pages              │
             ▼                            ▼
        UnionAll / combiner Aggregate / remaining ES|QL ops
```

### Cut algorithm

After `LogicalPlanOptimizer` (surrogates have fired):

1. Find every `ExternalRelation`.
2. Walk **up** while all of:
   - every leaf under the node is federated (no `EsRelation`);
   - every operator and expression is on the allowlist;
   - node is not `UnionAll`, `Fork`, `Enrich`, `LookupJoin`, inference, grok/dissect, or runtime `MATCH`.
3. Replace that subtree with `DataFusionExec`.
4. Nodes above the cut stay ES|QL (combiner, Lucene union, non-allowlisted `EVAL`, lookup join, …).

If a non-allowlisted operator sits in the middle of an otherwise-good arm, cut **below** it (DF scans; ES runs grok/`MATCH`).

### Two execution modes

| Mode | When | Who shuffles `STATS BY` |
|---|---|---|
| **B — ES buckets files (default)** | Mixed queries; also dataset-only | Each ticket is a file subset + IR in **PARTIAL** agg mode. ES combiner is the existing outer `Aggregate`. DF workers do not talk to each other. |
| **A — one DF job** | Dataset-only, cut is the whole remaining plan, optional later | Ticket is the full file list + IR. DF cluster shuffles. ES is one Flight client. |

v1 implements **B** only. Mode A is a later optimization for dataset-only, not required for Lucene union.

### What is on the ticket (not SQL)

A resolved IR plus the read snapshot ES already has:

- Authorized `StoragePath`s (bucket of files for this worker)
- Unified schema, `DeclaredReadSpec`, partition columns, dataset name for `_index`
- Operator tree: `Scan → Filter? → Project? → Eval? → Aggregate(partial)? → Limit?`
- Expressions from the allowlist only
- Agg mode: `PARTIAL` (v1) or `FINAL` (mode A later)
- Breaker/row budgets, cancellation token identity

Path lists must not rely on `FlightSplit`’s 16 KiB ticket cap. Put the IR + paths in the DoGet payload (or a side channel). `DataFusionExec` is coordinator-local in v1 (`CoordinatorOnlyStrategy`); do not ship it through `DataNodeComputeHandler` as if it were a Lucene fragment.

Safer v1: **ES logical plan is final; DF only physicalizes** (parquet prune, hash vs sort agg, decode). Do not enable DF logical rewrites that can change ES|QL meaning.

---

## DataFusion side: a worker server, not a query cluster

DataFusion is a **Rust library**. It does not accept jobs, speak Flight, or sit in a replica set by itself. Something must wrap it.

**Yes: build a small, stateless Flight worker.**  
**No: do not build Ballista, Flight SQL, a catalog, or a second coordinator.** Elasticsearch is already the scheduler.

```
ES coordinator                    DF worker replica (× N)
  BranchCutter + file buckets       Arrow Flight server
  DoAction(submit) / DoGet    →     decode ticket
                                    DataFusion ExecutionPlan from IR
                                    object_store → S3
                                    stream RecordBatches
```

N copies of **the same binary**. ES’s worker registry is an explicit list of those processes. Do not put a round-robin load balancer in front and hope: ES picked *this* worker for *these* files; the stream must stick to that process.

### What the worker implements

| Piece | Role |
|---|---|
| Arrow Flight (`DoGet`, plus `DoAction` if the job is larger than a ticket) | Only RPC ES needs. Matches the existing `FlightConnector` client shape (`DoGet` + Arrow schema). |
| Ticket / job decoder | IR + file list + schema + `PARTIAL`/`FINAL` + budgets. Versioned. Not SQL. |
| Plan builder | Map IR operators onto DataFusion `ExecutionPlan` (`ListingTable`/`FileScanConfig` for parquet, `FilterExec`, `ProjectionExec`, `AggregateExec`). **Physicalize only.** |
| `object_store` crate | Read the paths ES already authorized. No `list()`. |
| RecordBatch stream | Output schema exactly as the IR specified (including `$$partial$$*` names). |
| Liveness | Cheap Flight action or HTTP `/health` for the registry. |
| Memory / cancel | DataFusion memory pool capped by the ticket budget; abort when ES cancels the Flight stream. |

Suggested stack: `datafusion` + `arrow-flight` + `object_store` (S3 first). One binary, config = bind address + object-store identity (IRSA / instance role / env). **No long-lived cloud keys from ES in the ticket.**

Large jobs: `DoAction("submit_job", payload)` → opaque job id → `DoGet(job id)`. That avoids `FlightSplit`’s 16 KiB ticket cap.

### What not to run on the worker

- **Parser / SQL.** No Flight SQL server (`datafusion-flight-sql`). ES already planned.
- **Catalog / Iceberg REST / Hive.** ES resolved the dataset and files.
- **Cluster scheduler (Ballista).** Two schedulers will disagree on `LIMIT` and partial `STATS`. Mode A (DF-internal shuffle) is a later, dataset-only opt-in — still not v1.
- **Listing or glob.** If a path is missing, fail that job; do not discover extra objects.
- **Authz.** If the ticket has a path, the worker may read it (IAM still applies). Which paths exist is ES’s problem.
- **JNI inside ES.** The worker is a separate process so scan CPU/crashes stay off the ES JVM.

### Process lifecycle

Workers are idle Flight servers. A job is request-scoped: open store clients, run the plan, stream, drop the session. No sticky query state across jobs. Scale-out = more replicas in the registry, not a DF cluster join protocol.

For local tests, the same binary can run one replica next to Gradle QA (like other datasource ITs). Production is N replicas with ES listing them on the data source or a cluster setting.

### Protocol sketch (v1)

1. ES assigns file bucket `W` to worker `i`.
2. ES `submit_job` `{ ir, paths, schema, agg_mode: PARTIAL, budget }`.
3. Worker builds `ExecutionPlan`, starts execution.
4. ES `DoGet` until EOS; `ArrowToEsql` → Pages.
5. Failure: Flight error → that bucket fails or marks partial; other workers continue.

The worker does not need `GetFlightInfo` multi-endpoint splitting. ES already split by files. `GetSchema` is optional (ES already has the schema); the stream schema must still match the IR.

---

## Allowlist (v1)

**Operators:** `ExternalRelation`/`Scan`, `Filter`, `Project`, `Eval` (scalar subset), `Aggregate` algebraic (`COUNT`, `SUM`, `MIN`, `MAX`; `AVG` after surrogate), `Limit`.

**Expressions:** comparisons, `AND`/`OR`/`NOT`, `IS NULL`, `IN` (foldable), arithmetic, simple datetime (`DATE_TRUNC` once csv-spec-equal). Same YES/NO/RECHECK spirit as `FilterPushdownSupport`.

**Out of v1:** MV functions, grok/dissect, enrich, lookup (join stays ES; left scan may still be DF), spatial, runtime `MATCH`/`_score`, `COUNT_DISTINCT`/`PERCENTILE`/`STDDEV` on mixed plans, `TopN` inside DF on mixed plans (authoritative TopN stays above `UnionAll`).

`LIMIT` on mixed: per-arm limit is a **hint**; the limit/TopN above `UnionAll` is authoritative (same as today’s fork pushdown). Dataset-only may apply `LIMIT` in DF.

---

## Phases and work items

### Phase 0 — prerequisites (scan-leaf)

Do not start the cutter until these exist (or stub them with a single in-process DF worker in tests):

- Worker registry + liveness
- File listing still in `ExternalSourceResolver`
- LPT assignment of **files** to N workers (not ES `DiscoveryNode`s)
- Flight `DoGet` + `ArrowToEsql`
- Federation gates unchanged

Deliverable: scan-leaf `FROM dataset | KEEP | WHERE simple | LIMIT` matches Java readers.

### Phase 1 — IR + cutter (no DF shuffle)

**ES**

- `DataFusionExec` physical leaf (`NamedWriteable` if ever shipped; v1 coordinator-only is enough).
- `BranchCutter` after `LogicalPlanOptimizer`, before `Mapper` (or a mapper rule that emits `DataFusionExec` instead of `FragmentExec(ExternalRelation)` when the fragment is fully allowlisted).
- `ExpressionTranslator` + `OperatorAllowlist`; default `NO`.
- Ticket encoder: IR + file bucket + schema + `PARTIAL` flag.
- `DataFusionSourceOperator`: one Flight stream per assigned worker; Pages feed the existing exchange/union/combiner.

**DF worker**

- Decode ticket; build DF **physical** plan from IR (no SQL).
- Execute scan + filter + project + algebraic partial agg on assigned files.
- Stream Arrow matching the IR output schema (`PARTIAL` columns named as `PushAggregateThroughUnionAll` expects, e.g. `$$partial$$…`, shared `NameId`s already allocated on the ES side — translator must preserve output attribute **names** the combiner uses).

**Tests (no mixed Lucene yet)**

- Unit: cut on dataset-only `Filter+Project+Limit`; refuse grok (cut below); refuse `EsRelation` in subtree.
- Unit: IR round-trip + allowlist.
- csv-spec subset vs Java federation reader: `WHERE` + `KEEP` + `COUNT`/`SUM`/`MIN`/`MAX` + `BY`.

Deliverable: dataset-only `FROM ds | WHERE | STATS SUM(x) BY k` returns the same as Java readers; EXPLAIN shows `DataFusionExec` not Lucene shards.

### Phase 2 — mixed `FROM index, dataset`

- Rely on existing UnionAll pushdown; cutter only replaces **external** inner trees.
- Combiner `Aggregate` and Lucene `FragmentExec` unchanged.
- `METADATA _index` still dataset name vs index name (ticket must stamp `_index` as today).
- Partial column names/types/order must match Lucene inner aggregate output of `PushAggregateThroughUnionAll`.

**Tests**

- `FROM idx, ds | WHERE | STATS COUNT(*) BY host` vs both sources ingested as an index (or vs Java federation mixed path).
- Filter that is Lucene-only (`KNN` / index `MATCH`) must not appear in the DF IR.
- One DF worker killed: partial/fail via `EsqlExecutionInfo`, not silent missing groups.

Deliverable: mixed algebraic `STATS` equals today’s Java federation + Lucene path.

### Phase 3 — harden and optional mode A

- Expand scalar `Eval` allowlist with csv-spec gates.
- Cancellation + breaker into Flight.
- Optional dataset-only **mode A** (one job, DF shuffle, `FINAL` agg) behind a flag; mixed stays mode B.
- Sketch aggs (`ToPartial`) only if DF can emit ES `PARTIAL_AGG` bytes.

---

## Suggested code layout

| Piece | Where |
|---|---|
| `BranchCutter`, allowlist, translator | `x-pack/plugin/esql/.../datasources/datafusion/` (or `planner/`) |
| `DataFusionExec` | `plan/physical/` next to `ExternalSourceExec` |
| Mapper hook | `planner/mapper/Mapper.java`: `ExternalRelation` / fragment → `DataFusionExec` when cut applies |
| Worker + Flight | new `esql-datasource-datafusion` plugin implementing `DataSourcePlugin` / `ExternalSourceFactory` |
| DF worker process | out of ES JVM (Rust); ticket schema shared (proto or compact JSON versioned) |
| QA | `esql-datasource-datafusion/qa` csv-spec + mixed javaRestTest; DF cluster optional like other datasource QA |

Do **not**: send `request.query()` to DF; JNI `FormatReader`; route DF tickets through `handleExternalSourceRequest` as Lucene-shaped shard computes; enable DF SQL planner on the original string.

Insert the cutter **after** `LogicalPlanOptimizer.operators()` so UnionAll pushdown and surrogates have run. Do not run it before `PushAggregateThroughUnionAll`.

---

## Testing strategy

- **Cutter unit tests** on in-memory logical plans (`EsRelation` vs `ExternalRelation`, grok cut, nested subquery UnionAll **not** `isLeafUnionAll` — do not push into subquery-shaped forks in v1).
- **Translator tests**: each allowlisted `Expression` subclass → IR → (optional) DF explain.
- **Parity:** existing federation csv-spec on DF vs Java; mixed tests against `PushAggregateThroughUnionAll` expected partial shapes.
- **Negative:** capability off → Java path; empty worker pool → fail closed (scan-leaf policy).
- Default `./gradlew test` must not require a DF binary; rest/IT module is opt-in.

---

## Risks and decisions

| Risk | Mitigation |
|---|---|
| Mixed `STATS` wrong if partial names/types drift | Golden tests on `PushAggregateThroughUnionAll` output vs DF IR output schema |
| DF logical optimizer changes meaning | v1: physicalize only |
| `LIMIT 100` × N workers | Authoritative limit above union; per-arm limit is a hint |
| `COUNT_DISTINCT` sketches | Out of mixed v1 |
| Large IR + file lists | Payload on DoGet, not `FlightSplit` ticket bytes |
| Subquery-shaped `UnionAll` | v1: only `isLeafUnionAll` + dataset-only trees |
| Two physical planners on two arms | Remainder above union must be valid for both; Lucene-only predicates stay off DF IR |

**Open decisions (resolve in Phase 1 design review)**

1. Cutter before vs inside `Mapper` (recommendation: after logical opt, Mapper emits `DataFusionExec` instead of `FragmentExec` for cut trees).
2. IR format: versioned proto vs JSON vs Substrait (no Substrait in repo today; a small ES IR is enough for v1 allowlist).
3. Whether `Eval` is in Phase 1 or 2 (Phase 1 can skip `Eval` to ship mixed `STATS` faster).
4. Fallback to Java `ExternalSourceExec` when cutter fails vs always fail the DF path when workers are configured.

---

## Success criteria

- Dataset-only algebraic `STATS` matches Java federation csv-spec.
- Mixed `FROM index, dataset \| STATS COUNT/SUM/MIN/MAX BY k` matches today’s ES results.
- `EXPLAIN` shows Lucene `FragmentExec` and `DataFusionExec` as sibling arms, combiner on the coordinator.
- Non-allowlisted operators still succeed (cut below; work in ES).
- Adding DF workers speeds the S3 arm without adding ES nodes; adding ES search nodes only changes Lucene parallelism.
- Coordinator never sends ES|QL source to DF; workers never list the bucket.

## Order of delivery (checklist)

1. Scan-leaf Phase 0 (workers + Flight + files).
2. IR schema + allowlist + round-trip tests.
3. `BranchCutter` + `DataFusionExec` + dataset-only Phase 1.
4. Wire combiner names; mixed Phase 2.
5. EXPLAIN, metrics, failure/cancellation.
6. Optional mode A and sketch aggs.
