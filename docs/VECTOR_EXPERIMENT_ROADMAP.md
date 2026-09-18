# Vector experiments: retention decision and integration gates

Decision for [#228](https://github.com/nowledge-co/hawdb/issues/228): **retain the
existing exports as explicitly experimental, benchmark-only building blocks**,
with the prerequisites below. Do not wire them into embedded serving or add new
experimental features now. This selects the issue's tracking-plan acceptance
option, not its deletion option and not a claim that integration is complete.
Source audit baseline: `f6a8d0b153a6d557ab1bb63f6f43aa77085e3d92`.

The retention purpose is bounded: keep executable comparison fixtures while
deciding whether these implementations belong in the actual search path. API
removal, new public interfaces, and persisted formats require separate review
and owner approval. No new crate, feature flag, runtime, or CI job is needed for
this decision. The embedded `hawdb` library remains the application facade;
this roadmap grants no stable-release or production-admission permission.

## Current ownership and consumers

| Export family | Current non-test use | Missing serving contract |
| --- | --- | --- |
| `HnswIndex`, `HnswBuildConfig` | [`hnsw_vs_exact_scan`](../benches/hnsw_vs_exact_scan.rs) | Entirely resident graph with raw vectors; no artifact reader/writer, generation identity, update/delete lifecycle, or candidate filter. |
| `DeltaBuffer`, `search_with_delta`, `DeltaMergedSearchOutput` | [`vector_projection_delta_fraction`](../benches/vector_projection_delta_fraction.rs) | In-memory upserts only; no tombstones, epoch binding, publication, replay, or maintenance owner. |
| `IndexAdvisor`, `IndexPlanner`, `AutoIndexPolicy`, `WorkloadSample`, `IndexRecommendation`, `IndexAction`, `QueryPath` | [`index_advisor_recommendation_quality`](../benches/index_advisor_recommendation_quality.rs) exercises advice; no embedded consumer for the family | Caller-supplied availability and heuristic thresholds are not generation validation, resource admission, a query optimizer, or a build scheduler. |

The crate's unit tests exercise these modules, and
[`SEARCH_ROBUSTNESS_CONTRACTS.md`](SEARCH_ROBUSTNESS_CONTRACTS.md) records the delta
shadowing oracle. Neither is a production consumer. The exported items link
back to this roadmap through their generated API documentation.

The actual resident path is
[`execute_search_vector_plan`](../crates/search/src/vector_execution.rs), using
scalar candidates or a RaBitQ projection followed by canonical raw reranking.
The out-of-core path is
[`scan_vector_scores`](../crates/search/src/out_of_core/vector_serving.rs), with
its existing disabled/preferred/required behavior. Its
[`RaBitQ generation sink`](../crates/search/src/out_of_core/generation_writer/rabitq.rs)
streams the generation into a `ProjectionWriter`; it does not use `DeltaBuffer`
or make checkpoint publication incremental across generations.

RaBitQ defaults to **1-bit**; 4-bit is opt-in. An exhaustive quantized scan is
not exact raw-vector scoring. HNSW's retained raw vectors are an implementation
choice, not proof that quantized traversal cannot work or that HNSW wins on
every workload. The current HNSW API has no candidate filter; post-filtering
an overfetched ANN result cannot substitute for the eligibility boundary.

## Delivery sequence and exit conditions

### 1. Qualify need before adding a serving consumer

Input: a named active Mem replacement workload, the existing RaBitQ/raw-scan
baseline, and its memory/latency/recall acceptance thresholds. The retained
synthetic benches are development comparisons only. Their smoke outputs and
advisor fixture agreement do not establish production quality or calibrated
thresholds for the current 1-bit default.

- [ ] Measure the same representative corpus and held-out queries with raw
  cosine truth, default 1-bit RaBitQ plus rerank, and the proposed alternative.
  Report candidate recall separately from final reranked recall, stable ties,
  P50/P95/P99, build time, admitted memory, measured peak/steady RSS, and bytes
  read/written. Identify source revision, configuration, and dataset.
- [ ] Include metadata/ACL-filtered workloads and a dataset larger than the
  admitted search memory budget. The current resident HNSW graph cannot claim
  out-of-core qualification merely because the RaBitQ baseline passes it.
- [ ] Obtain an independent need/resource decision before implementing HNSW
  persistence or automatic index selection. If no active workload justifies
  the extra graph, propose removing the HNSW/advisor APIs rather than adding
  unused optimization features. Preserve useful benchmark/oracle evidence.

Exit: an accepted workload and measured benefit justify a scoped follow-up,
or a separately approved removal change. This document is not that evidence.

### 2. Resolve delta identity and tombstones with incremental publication

Owner boundary: derived search persistence in `hawdb-search`, tracked by
[#291](https://github.com/nowledge-co/hawdb/issues/291), after its prerequisite
[#206](https://github.com/nowledge-co/hawdb/issues/206) is independently qualified
and merged. Do not start an overlapping segment-format implementation here.
This lane does not require HNSW, and HNSW is not a prerequisite for #291.

- [ ] Specify upsert/delete/reinsert ordering, latest-write-wins shadowing,
  epoch visibility, and stable document identity. Current projection ordinals
  are generation-local: a numeric delta ID must not silently alias a different
  document after rebuild or compaction.
- [ ] Represent tombstones for both base and delta-only documents; account for
  their retained memory and preserve them until no pinned reader needs the old
  generation. Removing an entry from an upsert buffer is not a base deletion.
- [ ] Publish a checksummed immutable segment set last, bound to canonical
  epoch, source/embedding identity, and the correct ordinal mapping. Recover
  only complete sets and retain pinned closures during cleanup. Rebuild from
  canonical embeddings, never from lossy codes as if they were the originals.
- [ ] Decide whether to adapt `DeltaBuffer` or remove it in favor of the owned
  persistence implementation. Do not maintain two competing mutation buffers
  merely to retain an export. Preserve full shadowing headroom or prove a new
  bound against an independent oracle; never silently truncate on admission.

Exit: #291's incremental bytes-written, bounded merge/RSS, cancellation,
consistent recovery, and score-parity gates pass. A tombstone API alone does
not qualify incremental serving. Update this roadmap and the export docs in
the same PR that adopts or retires the prototype.

### 3. Add HNSW lifecycle only after the need gate

- [ ] Design an immutable derived artifact with an explicit format, integrity
  validation, graph topology/offset bounds, dimension/metric/build parameters,
  embedding identity, source digest/epoch, generation, and ordinal mapping.
  No serialization of the in-memory struct is assumed to be a valid format.
- [ ] Define build/scratch/cache/reader memory ownership under the existing
  governor. Account for raw vectors and adjacency, cancellation, failed builds,
  reopen, and concurrent pinned generations; an estimate is not measured RSS.
- [ ] Implement publish-last, failed-publication cleanup, stale-versus-corrupt
  classification, quarantine/rebuild, and pinned-generation reclamation.
  Resolve base updates/deletes via a qualified overlay or rebuild contract;
  a stale graph must not be selected as current.
- [ ] Specify eligibility before candidate scoring. Until a filtered HNSW path
  is separately qualified, filtered queries must remain on an eligible existing
  path. Keep canonical raw reranking and observable fallback/required behavior.

Exit: lifecycle and resource evidence is complete for the intended residency
mode. Existing synthetic recall tests do not satisfy this gate.

### 4. Integrate selection, then separately consider automatic maintenance

Only qualified, current, admitted artifacts may become candidates for the
existing optimizer/executor path. `IndexPlanner::choose(true, false)` proves
none of those conditions. Do not expose it as a second production planner.

- [ ] Bind dispatch and query reports to validated artifact capabilities,
  freshness, eligibility, precision, and admitted resources. Recalibrate advisor
  heuristics against observed workload outcomes, not expected fixture labels.
- [ ] Keep recommendations read-only initially. Before auto-apply, define
  background admission, work ownership, bounded scheduling units, cancellation,
  shadow-build validation, publish/rollback, and foreground latency safeguards.
  `AutoIndexPolicy::is_eligible` is not authorization to execute maintenance.
- [ ] Reuse the embedded typed readiness/qualification path. No production CLI,
  environment-variable control plane, hidden thread pool, or direct host-side
  ANN routing. Remove or consolidate unused experimental selectors when the
  real integration contract supersedes them.

Exit: independent review and generation-bound production qualification for the
actual library dispatch path, followed by an explicit activation decision.

## Required verification for future implementation

Use the [RaBitQ serving contract](specs/RABITQ_VECTOR_PROJECTION_SPEC.md) and
existing [production vector collector](../crates/qualification/src/production_vector.rs)
as the baseline; do not lower their gates to admit an experiment.

- Differential state-machine fuzz must cover upsert/delete/reinsert, duplicate
  IDs, ties, filters, checkpoint/reopen, rebuild/merge, and old pinned readers.
  Compare with an independent latest-value map and raw-vector oracle, not the
  candidate generator itself. Check candidates and final reranked results
  separately; approximate ANN recall is not exact-result equality.
- Mutants must expose lost tombstones, stale ordinal reuse, omitted eligibility,
  insufficient shadowing headroom, and premature publication/reclamation.
  Corruption/truncation and crash probes must fail closed without partial state.
- Include admission just below/at limits, cancellation during build/scan/merge,
  sustained foreground/background load, and native Linux, macOS, and Windows
  lifecycle checks. Record missing platform/evidence rows as incomplete.
- Keep campaigns reproducible with explicit v1 seed/replay/shard/resume identity,
  available through Bazel and local/manual only. Retain TLA+, ordinary tests,
  and crash verification; do not add fuzz to default or dedicated CI jobs.

For this documentation-only decision, verify generated API links, unchanged
executable Rust, the existing vector/search regressions, and mandatory local
fuzz. These checks validate the retained baseline, not the unchecked gates above:

```sh
cargo doc -p hawdb-vector-projection --no-deps
bazel test //crates/vector-projection:hawdb_vector_projection_tests \
  //crates/search:hawdb_search_tests \
  //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results
```
