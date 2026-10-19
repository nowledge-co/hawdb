# Query limits and partial-result inventory

Issue Number: ref [#996](https://github.com/nowledge-co/hawdb/issues/996)

Source audit: main `923879de`. This is the first inventory and report-schema
guard for #996, not completion of its resource-governance requirements. A query
resource limit must preserve the complete requested result through admitted
spill or terminate with an error. Reporting a truncated result does not make a
resource-induced partial success correct.

## Classification

- **A / semantic:** the caller or query explicitly selects the result domain,
  such as `LIMIT`, offset, top-K, a retrieval window or a schema-summary size.
  Memory pressure must not silently reduce that selected domain.
- **B / resource partial success:** an implementation resource cap removes
  requested results and still returns success. Such a row needs conversion;
  it cannot be justified by adding a report flag.
- **C / resource failure or complete spill:** exhaustion fails, or the stage
  spills and continues with the same requested results. The table distinguishes
  structured errors from legacy diagnostic strings.
- **D / diagnostic or bounded unit:** a report copies an existing outcome, or
  a batch/read boundary continues elsewhere without discarding query work.

The source review found one B path in the compatibility graph-context reader:
its per-direction input prefix preceded scope filtering and deduplication. The
candidate replaces that prefix with admitted streaming; see the regression
case below. This is not an exhaustive proof of runtime completeness or allocator
accounting.
In particular, class C does not establish that every error is structured or that
all allocations are admitted. The memory/spill and contextual morsel rows below
include the structured-error and cooperative-cancellation follow-up described in
the [execution contract](EXECUTION_REQUEST_CONTRACT.md#resource-refusal-metadata).
Callbacks without checkpoints still cannot be interrupted while executing.

## Execution and admission

| Stage / source | Class | Boundary and current outcome | Remaining obligation |
| --- | --- | --- | --- |
| [ExecutionLimit](../crates/executor/src/limit.rs), `from_user_max_rows` / `child_for_limit` | A + C | Semantic LIMIT includes its offset. A resource row ceiling retains one additional detection row; it is not a successful prefix. | Preserve detection when adding pushdown or another result producer. |
| [QueryOutputAccumulator](../crates/executor/src/result_delivery.rs), `emit` | C | Excess rows or payload return `ReadBudgetExceeded` with resource identity. Owned result capacity is separately charged. | Streaming consumers can observe earlier callbacks before an error; they must treat the operation as failed. |
| [QueryMemoryLedger](../crates/executor/src/memory_ledger.rs), `reserve` | C | Per-account and query reservations fail before counters change, with structured `ResourceBudgetExceeded` metadata. Governed spill quota refusals preserve the same resource/scope/owner/request/usage/budget/origin contract. | Other operator-local checks may still return `Execution(String)`; do not infer that every resource failure has been converted. |
| [GraphExpansionExecutionState](../crates/executor/src/graph.rs), `try_admit_payload` | C | Default seeded expansions retain admitted identity state and complete counters without cumulative row/payload caps. Explicit host caps still return `GraphExpansionCandidateLimitExceeded` or `GraphExpansionPayloadLimitExceeded`; live-memory refusal is typed. | Expansion state is not spillable and must fail if its live grant is exhausted. Preserve admission and complete-cohort scoring. |
| [Graph lowering](../crates/optimizer-graph/src/graph/lowering.rs), seeded `AdjacencyExpandExec` | C | AST/plan-derived seed identity enables reports and memory accounting. No seed-fanout heuristic or fixed cumulative payload cap precedes the live ledger. | Explicit caller traversal caps are separate from live memory. Query/runtime grants and operator/batch budgets still apply; this does not complete the wider Mem contract decision. |
| [Sort / TopN](../crates/executor/src/blocking/sort.rs), `push`, `spill_heap`, merge output | A + C | TopN selects the requested order/window. Working-state pressure produces governed runs; spill failure propagates. | Preserve full-input ranking and spill cleanup when changing either path. Any `SortKey::Expression` uses serial spill-run compaction with shared-query admission for expression scratch; property, column and ID keys retain parallel compaction. This can reduce spill-compaction parallelism for inline computed `ORDER BY` expressions. |
| [Hash joins](../crates/executor/src/blocking/admitted_hash_join.rs), `push_build` and repartition/replay | C | Hash state spills or refuses admission; partitioning is not a result limit. | Skew, oversized records and shared spill exhaustion must remain failures. |
| [Aggregate](../crates/executor/src/blocking/aggregate.rs), [partial states](../crates/executor/src/blocking/aggregate/partial.rs) | C | State limits spill partial aggregates or fail; grouping is not stopped after a resource-selected prefix. | Preserve complete groups across merge/cancel/error. |
| [Distinct](../crates/executor/src/blocking/distinct.rs) | A + C | Deduplication spills as needed; final `execution_limit` is a semantic/detection boundary. | An output limit must not become a cap on input examined for distinctness. |
| [Host scoring](../crates/executor/src/transform/host_scoring.rs) | A + C | The finite input cohort is retained under admission before scoring. Final K is applied afterward; a memory failure is not a smaller cohort. | Keep complete-cohort normalization and late-winner regressions. |
| [Numeric production](../crates/executor/src/numeric.rs), [retained selection](../crates/executor/src/numeric/retained.rs) | A + C + D | Selection honors query limits; production waves resume the iterator. Retained delivery reserves its owner and buffers. | Lifetime/task-reservation qualification is independent of this report inventory. |
| [Pipeline batches](../crates/executor/src/pipeline.rs), [morsels](../crates/executor/src/morsel.rs), [concurrent work](../crates/executor/src/concurrent.rs) | D + C | Contextual morsel schedulers signal resource exhaustion before joining in-flight workers and preserve the originating typed refusal. The numeric parallel pipeline uses a child context to isolate a reusable parent. | Cancellation is cooperative. Handled reservation refusals do not cancel; generic result-agnostic maps and ordinary callback errors retain their existing behavior. |
| [Relational runtime](../crates/relational/src/query.rs) and [execution](../crates/relational/src/row_runtime.rs) | A + C | SQL semantic limits are separate from result/working allowances and shared blocking operators. | Preserve structured result-limit causes and spill semantics at the SQL facade. |

The blocking-memory follow-up converts shared tracker growth/transfer, oversized
items, partial-aggregate state and spill-merge item/fan-in refusals to structured
`ResourceBudgetExceeded`. The existing refusal messages and spill choices are
preserved. The effective per-item allowance is reported separately from current
tracker usage. Counter overflow fails before accounting changes. Ledger/account
failures retain their own scope and budget origin; malformed spill framing and
invalid transfers are not reclassified as memory exhaustion.

The embedded regression
`blocking_memory_refusals_expose_effective_limits_and_preserve_complete_results`
covers Sort, TopN, partial aggregation and buffered aggregation at a 64-byte
blocking limit, with complete-result controls at 8192 bytes. Tracker and spill
regressions cover exact limits, overflow, failed transfers, temporary decode
cleanup and provenance. These source guards require CI execution; they do not
establish that every remaining resource failure in this inventory is structured.

## Search and retrieval

| Stage / source | Class | Boundary and current outcome | Remaining obligation |
| --- | --- | --- | --- |
| [Resident Search](../crates/search/src/lib.rs), ranked page construction | A | `limit` / `offset` choose the requested page. `SearchTruncationReasonCode::LimitExceeded` means more matching hits exist beyond that page. | Do not reuse this reason for a resource-induced loss of candidates. |
| [Out-of-core Search](../crates/search/src/out_of_core.rs), page construction | A + C | Same page semantics; score-window requests exceeding `max_score_entries` fail before work. | Keep resident/out-of-core page parity. |
| [Lexical query admission](../crates/search/src/lexical_projection.rs), `tokenize_accounted`, score entry points | C | Too many query terms, oversized terms, score-state or working admission failures return errors. The query is not reduced to its first terms. | Several errors remain storage/execution diagnostic strings. |
| [Lexical ScoreCollector](../crates/search/src/lexical_projection.rs) | A + C | An explicit retained top-K window selects the best scores while visiting eligible input. Full collection exceeding its entry allowance fails. | Filtering must precede top-K; pruning must preserve the requested ranking. |
| [Metadata candidate spill](../crates/search/src/out_of_core.rs), `build_candidate_set` | C | Candidate blocks spill under byte limits. An oversized block or exhausted total spill allowance returns an error. | This is complete candidate enumeration, not a top-K producer. |
| [Raw-vector collector](../crates/search/src/out_of_core/vector_serving.rs) and `BoundedScoreCollector` | A + C | Requested top scores are selected over scanned input; insufficient score admission and uniqueness-state capacity fail. | Preserve exact raw ranking independently of approximate scan policy. |
| [RaBitQ candidate scan](../crates/search/src/out_of_core/vector_serving.rs), `scan_rabitq_vector_scores` | A + C | Approximate retrieval has an explicit configured candidate window. A requested minimum larger than that window fails; working-memory admission also fails. | This is approximate retrieval, not a claim of exhaustive raw-vector recall. Do not describe its candidate policy as harmless lossless memory truncation. |
| [Projection scan](../crates/vector-projection/src/scan.rs) | A + C | Requested top-K uses a bounded collector over scanned allowed codes. Worker/global workspace admission returns typed `ResourceBudgetExceeded`. | Native kernel parity, cancellation and workspace qualification remain independent. |
| [Vector executor](../crates/executor/src/vector.rs) | A | Candidate/rerank windows are part of the vector physical plan. Residual search widens within that plan's candidate bound; final top-K follows reranking. | A source must not hide resource exhaustion as an incomplete candidate batch. |
| [Text seeds](../crates/executor/src/external/seed/text.rs), [vector seeds](../crates/executor/src/external/seed.rs) | A + C | Physical seed top-K and the result detection allowance bound delivery. External results are validated and charged. | A producer's memory limit must not silently become a smaller seed window. |
| [Pinned text scorer](../crates/search/src/out_of_core/text_seed.rs) | A + C | Metadata/visibility predicates run before the requested producer top-K. Postings, ranking references and IDs remain admitted. | `Ok(false)` in visibility/filter predicates rejects a nonmatching row; it is not resource exhaustion. |
| [Knowledge retrieval](../src/api/mod.rs), `retrieve_knowledge_from_search` | A + C | Request seed/context/rank windows define the compatibility retrieval domain. `candidate_limit` is applied after candidate scoring. Working/payload accounting can fail independently. | Do not turn these request policies into implicit memory fallbacks or add route-specific production APIs. |
| [Knowledge graph context](../src/api/mod.rs), `expand_knowledge_context`, [edge visitor](../src/api/knowledge_graph_read.rs) | B at baseline; candidate A + C | The old reader capped raw edges before scope/deduplication. The candidate streams admitted edges and applies the requested context limit only after those checks. | Require scope, repeated-edge, ordering and failure regressions. This context contract remains distinct from an unbounded Cypher MATCH or a complete scorer cohort. |
| [Search evidence](../crates/search/src/candidate_evidence/report.rs), [Mem report](../src/nowledge_mem.rs), [readiness](../crates/readiness/src/nowledge_mem_query_report.rs) | D | Copy page/expansion diagnostics. They do not authorize an upstream resource-limited partial success. | A new reason requires tracing its producer, not merely updating JSON serialization. |
| [GraphRAG schema summaries](../crates/core/src/graph_rag/mod.rs) | A | Caller-selected label/property/path summary sizes intentionally bound prompt context. | These are summaries, not complete query outputs or resource-error substitutes. |
| [Bounded decoding](../crates/search/src/out_of_core/hydration.rs) | D + C | Framed reads and `take(expected_len + 1)` detect excess/truncated payloads and fail validation. | Byte framing is not successful query-row truncation. |

The audit follows caps, `truncated`/reason producers, `.take`, `.truncate`, and
`Ok(false)` in the executor and Search, then their facade/contract consumers.
Option ownership transfers, filesystem `truncate(true)`, malformed-record
diagnostics, test corruption, substring length and resumable work batches are
not result-changing resource caps. Build/publication operation budgets,
checkpoint retention and fixture sample counts are separate lifecycle or
evidence contracts; #988 and #392 retain their own continuation/resource gates.

## Graph-context prefix finding

At the audited baseline, `expansion_edges` reads at most
`remaining_context_rows + 1` raw edges per direction. Its caller subsequently
checks scope and already-seen relationships. With a requested context limit of
one, four earlier neighbors in another Space and two later eligible neighbors,
the first two raw edges are rejected and the later eligible edge is never
visited. The caller can return an empty context without a truncation reason.
Similarly, revisiting two already-returned parallel edges can consume the
lookahead window and hide a later new relationship. These conclusions follow
from source order; they are not claimed as executed baseline test failures.

The candidate passes each admitted edge directly to the existing context
builder. Scope and seed-relative deduplication run before checking the output
limit. An additional eligible unique edge proves that the explicitly requested
window is full; no extra edge collection or unadmitted fallback is introduced.
Outgoing-before-incoming and ordered adjacency traversal remain unchanged.
The source allocation permit stays alive through the callback and is released
on continue, stop or error.

`knowledge_retrieval_context_filters_before_output_limit` covers both directions
with four earlier rejected neighbors and limits below/equal/above the eligible
count. `knowledge_retrieval_context_deduplicates_before_output_limit` covers a
two-hop revisit with parallel relationships. Existing ordered/dense context and
pipeline resource tests remain relevant. The edge-visitor regression checks
source admission during the callback, release after completion/stop/error,
stopping before the incoming direction, and refusal before an oversized payload
reaches the callback. Ordinary regression execution is left
to CI; source inspection and successful compilation alone do not prove them.

## Report-schema guard

[query-limit-reports.json](query-limit-reports.json) classifies every currently
observed public `*truncat*` field and public `*Truncation*` reason enum in the
eight declared query/contract source roots. It currently covers 25 field
declarations and three enums. Diagnostic copies have separate entries so adding
another public partial-result surface cannot silently reuse an old inventory
record.

```console
python3 scripts/check-query-limit-inventory.py
bazel test //:query_limit_inventory_test //:query_limit_inventory_script_test
```

Both targets are included in `hawdb_unit_tests`, which the existing Prow root
job explicitly selects. Standalone targets are not automatically discovered
by that job. The inventory test
receives all Rust sources in the declared roots, including newly added files;
missing roots fail instead of producing an empty successful scan. Negative
controls add a new partial-success field, change a reason enum, use inline and
restricted-visibility fields, remove a source root, and leave a stale record.

The checker is deliberately a conservative declaration scanner, not a Rust
parser or a proof of control flow. It notices new/removed reported truncation
surfaces and enum changes. It cannot detect a new unreported early stop or a
changed producer that reuses an unchanged field and reason. Existing behavioral
tests remain essential, particularly graph expansion overflow, full-cohort
scoring, filtered top-K, result detection and spill failure tests. A commented
declaration can also require review; do not weaken the inventory to hide a new
runtime path.

When a PR touches a capped stage, trace the cap to its caller/configuration and
record its class here. For B, convert the touched stage to admitted spill or a
typed failure and test a late result beyond the old cap. For A, retain an
unbounded/reference ranking comparison and prove the bound was explicitly
requested. For C, test refusal/cleanup rather than accepting a shortened result.
Update report inventory records only after that producer review.

## Remaining #996 requirements

- Derive graph expansion capacity from admitted memory/spill policy instead of
  treating fixed planner constants as host budget scaling.
- Convert remaining operator-local string-only refusals as those stages are
  touched; ledger and governed spill refusals already expose structured metadata.
- Preserve contextual sibling cancellation and original-cause propagation when
  adding parallel stages; the existing worker/consumer, handled-refusal and
  parent-reuse regressions cover the current morsel and numeric paths, not every
  possible host callback or executor stage.
- Qualify every converted stage above its old row/byte cap, with complete spill
  output or typed failure, including cleanup and cancellation.
- Record the Mem consumer contract decision before treating informational
  pagination reasons as permission for resource-driven partial success.

This inventory does not close #996, qualify host integration, or replace those
behavioral and resource measurements.
