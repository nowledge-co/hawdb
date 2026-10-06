# Scoring rerank execution

This records the physical scoring operator's resource contract for
[issue #293](https://github.com/nowledge-co/hawdb/issues/293). It is not the
complete Mem route migration. The ordinary query attachment and cache contract
below are implemented; actual graph-hop/seed provenance and a Mem consumer
remain part of that issue's acceptance boundary.

`ScoringRerankExec` validates its `ScoringSpec`, captures time once for the
operator execution, and evaluates every candidate before selecting its result
window. A final K never becomes an upstream candidate limit. Equal combined
scores retain input order, including through spills. Signed zeros compare as
numerically equal for scoring: only its ordering expression normalizes zero;
the output column retains the original arithmetic sign bit. A zero result window
does not execute the candidate source. Invalid specifications and non-finite
combined arithmetic fail before any ranked output is emitted.

The scoring source checkpoints cancellation before and after evaluation,
accounts incoming batches, and reserves/checks the scored row before adding
its score column. `ScoringSpec::evaluate_score` and the full diagnostic
`evaluate` share one arithmetic implementation; the scalar path allocates no
per-row contribution, decay or missing-feature vectors. Missing features keep
the existing neutral arithmetic; callers needing provenance use `evaluate`.

Ranking uses the existing TopN heap, query/blocking accounts, spill admission
and merge implementation. In-memory Sort and TopN output transfer ownership
to `AccountedBindingBatch`, with the same row and payload caps as spilled
output. Both paths propagate consumer stop/error and release their accounts. Resident
rows beyond a parent output cap are destroyed and their retention charges
released before the final batch asks a parent transform to admit output.
Scoring memory reports use `ScoringRerankExec`; blocking inventory traverses
the shared plan-child contract, including scoring inputs.

The existing `ScoringSpec`, `Binding` and `QueryStreamOptions` fields and
physical operator signature remain intact. This resource correction does not
introduce product ranking weights or change SearchIndex RRF. The binding
feature source still reads its declared score column and returned numeric
property values; actual expansion-hop and graph-seed provenance are not yet
attached to the binding feature source. The legacy operator captures its own
time; the new ordinary request path below uses one request-wide time anchor.

## Ordinary query scoring programs

The embedded `hawdb` facade exports `ScoringProgram`, `ScoringRequest` and
the borrowed `QueryRequest`. `Database::query_request` and
`DatabaseReadTransaction::{query_request,query_request_streaming}` accept the
same Cypher text and parameters with optional scoring, access control, task
context and restrictive read-output caps. Existing string overloads and public
`Binding`, `ScoringSpec` and `QueryStreamOptions` fields remain intact.
Read snapshots can use the existing external-read operator through
`query_request_streaming_with_external`; this is a generic library boundary.
Materialized snapshot requests preserve EXPLAIN and EXPLAIN ANALYZE through
the existing snapshot explain executor, including current scoring, task context
and restrictive read caps. Both plain and analyzed diagnostic rows pass the
same final-output admission as read data, using the smaller database/request
row and payload caps plus the admitted result-memory budget. Inactive database
sessions use the same final admission with their configured caps; active
session transactions keep their existing EXPLAIN rejection. ANALYZE retains
its inner data caps independently; zero inner rows do not exempt its report.
Streaming requests retain their existing EXPLAIN
rejection. Parsing and clock capture occur once in either request path.

Programs validate the legacy specification and explicitly declare composition:

- `WeightedSum` computes the ordered sum of `weight * value`, preserving the
  legacy arithmetic bits.
- `WeightedProduct` computes the ordered product of `value.powf(weight)`.
  Weights are exponents; a zero weight contributes one. Negative bases with
  fractional weights and overflow fail when their combined result is nonfinite.
- Both multiply the same declared exponential decay factors afterward.
  A half-life is measured in hops for `HopDistance` and seconds for timestamp
  properties, which are read as epoch milliseconds.
- `Reject` reports a missing or nonfinite declared signal as an error.
  `Neutral` uses zero for missing sum terms and one for missing product terms
  or decay factors. These are explicit request choices, with distinct shapes.

Property names refer to returned value aliases in this ordinary attachment.
The declared score column supplies `SearchScore`; this does not by itself prove
that an arbitrary host expression came from a vector or text retriever.
`GraphSeedScore` and `HopDistance` remain absent until their actual producer
provenance is attached. Strict programs requesting them fail; no graph bound
or fabricated seed value is substituted. The existing knowledge-retrieval
pipeline's canonical feature source and neutral legacy policy are unchanged.

`ScoringRequest` carries the final K independently of the Cypher candidate
source. The planner preserves every existing query operator and appends the
known, costed `ScoringProgramExec`, which streams every candidate through the
shared accounted TopN implementation. A query with an existing LIMIT/OFFSET
requires `with_candidate_window()`; that explicit window remains in force.
Neither K nor a smaller result cap becomes an upstream seed or graph limit.
The combined result column is `scoring_rerank_score`. Timestamp decay uses one
anchor captured before parsing/planning, or the host's explicit
`with_reference_time_millis` input. Zero K retains its no-source-read behavior.

The real plan-cache key includes the typed ordered program shape, composition,
missing policy, input score-column name, final K and candidate-window policy.
Coefficients, half-lives, floors and reference time are execution values.
Cached physical templates hold neutral coefficients and a zero time anchor;
both hits and misses validate the structure and rebind the current program
and anchor. Query parameter and access-scope binding descend through scoring.
Unscored requests use a distinct key. EXPLAIN refreshes the executed program,
its current expression and estimated scoring cardinality/cost. Scalar program
evaluation allocates no per-row diagnostic vectors.

This does not yet complete https://github.com/nowledge-co/hawdb/issues/293:
actual expansion provenance, one real Mem route and full delivery qualification
remain required. No product weights or runtime
activation are selected by these templates.

## Host-scoring escape hatch

The facade exports the batch-oriented `HostScorer` declaration required by the
issue, with a validated borrowed `HostScorerDescriptor` (name, version and
nonzero logical CPU units per row) and `HostScorerBatch`. Batch inputs borrow
feature rows, one fixed clock, task context and the existing query scratch
account. The host writes into an engine-owned score slice in input order;
scratch allocation must reserve the supplied account first and release its
leases before returning. The descriptor identifies deterministic behavior,
including any host configuration that affects scores.

No query request registers or invokes this trait yet. A concrete non-template
formula must establish the need before callback execution is added. That future
attachment must capture a stable descriptor, include identity/version in the
cache key or bypass it, account the input/output buffers, checkpoint before and
after the callback, and reject incomplete/nonfinite output before ranking or
delivery. Mid-call work must check the borrowed cancellation context. Native
shared-library discovery, WASM UDFs and a new scorer runtime are outside this
contract. This declaration supplies an extension contract, not execution or
resource-acceptance evidence for a callback pipeline.

## Regression coverage

The resource guards exercise row admission, cancellation without upstream
checkpoints, zero K, complete candidate consumption, stable ties, late winners
from combined properties, invalid/overflow arithmetic, and actual resident
versus spill equivalence with released admissions. Separate Sort and TopN
guards exercise in-memory output payload limits, ownership transfer, all four
completion/stop/error/cancellation exits, and tight-budget parent admission
after a smaller output cap.
Blocking inventory coverage includes a scoring parent over nested Sort/TopN.
The former tuple-sort helper test now exercises the production scoring path
with the same ranking and zero-window assertions.

Use the registered executor/core targets and existing local sort/transform
differential campaigns. Browser-target Clippy is compilation/lint evidence;
it does not establish browser runtime acceptance or a Mem consumer migration.

## Required-test admission

The protected object is the physical scoring and Sort/TopN execution contract
above, together with the executor transfer caps in `ExecutionMemoryConfig`.
The oracle layer is actual production kernel output, complete ranking, source
consumption, independent stable expected rows, query-ledger ownership and actual
spill/run release. Public `ScoringSpec` diagnostic arithmetic remains a separate
core oracle.

| Guard group | Behavioral RED | Unique terminal evidence |
| --- | --- | --- |
| Scoring blocking admission, cancellation, zero window | Old main `1a3e532d` with unchanged new guards: three assertion failures | Over-budget success, cancelled source read, zero-window source read |
| Resident Sort/TopN output payload and ownership | Old shared production: both guard payload assertions fail | Changed resident output helper; expands the same guards with stop/error/cancel and ledger release rather than inferring them from spill |
| Blocking inventory and scoring report | Previous inventory/report production: missing nested operators and incorrect TopN attribution | Scoring parent traversal and operator attribution, beyond ranking-only tests |
| Invalid specification / arithmetic overflow | Separate precise validation and finite-result bypasses at `396a6c2`; unchanged guard fails on source-read count 1 vs 0 and unexpected success, respectively | Reject invalid specs before reading, and reject finite-input overflow before output |
| Complete stream / combined-property late winner | Precise candidate cap at two and SearchScore-only bypass at `396a6c2`; complete output becomes ordinals 1,0 instead of 1,2 and winner 0 instead of 3 | No seed-only early truncation or loss of graph property contribution; old-code positive PASS is not relabeled RED |
| Signed-zero ties and score bits | `396a6c2` production plus new guard chooses ordinal 1 instead of 0 | Numerical ties across batch boundaries and K=1/2; original reported score bits; expanded native oracle also checks zeros through actual spills |
| Capped resident parent admission | `396a6c2` partial-batch guard rejects ProjectExec at 1,079 bytes against 1,024; at `1a18e8f`, changing only batch rows 4 to 1 reproduces the same failure independently for Sort and TopN | Partial and full terminal batches (cap 1, batch rows 1/4; cap 2, batch rows 2 and query budget 800); exact prefix, successful parent admission and zero retained ledger |
| Scoring input parameter binding | `1a18e8f` retains a parameter-slot Map instead of current request value `first` | Two request values beneath the scoring parent, missing-slot rejection, unchanged score specification/operator and immutable stored template |
| EXISTS input parameter binding | `064df131` retains a parameter-slot Map instead of current request value `first` | Two request values beneath EXISTS, missing-slot rejection, unchanged relationship/direction/input and immutable stored template; the physical binder exhaustively classifies all operators |
| Weighted product / fixed clock through resident and spill | Production mutations replace exponentiation with weighted multiplication, and bind the operator clock to zero | Independent complete binding/score-bit oracle, whole candidate stream, resident versus actual spill, caps and released ledger/run admissions |
| Typed program shape and signal policy | Separate production mutations collapse composition or missing policy in the shape, ignore required missing values, and accept a nonfinite result | Distinct template identities, explicit missing-value failure and finite arithmetic; core scalar/diagnostic parity |
| Ordinary request cache and read boundaries | Nine separate production mutations skip hit coefficient binding, retain a zero hit clock, collapse the scoring cache key, bypass candidate-window or write admission, or omit payload/row/task/access request forwarding; each fails its unchanged guard, and byte-restored ACL execution passes all four | Real cache hits and structural isolation; explicit retained query window; no ranked mutation; validated no-callback row/payload/cancellation failures; actual ACL-scoped winner |
| Snapshot request EXPLAIN | The initial materialized snapshot request delegates to streaming and rejects EXPLAIN; the unchanged new guard fails its first actual request | Shared snapshot EXPLAIN/ANALYZE, cache-hit coefficients and time, executed final K, payload cap and precancelled request |
| EXPLAIN final report budgets | Frozen `cd0c59d` fails eight unchanged API guards, one per Database/snapshot × EXPLAIN/ANALYZE × row/payload limit; ANALYZE K=0 isolates the final report | Common final report admission; original unbounded report succeeds, oversized report fails; inner ANALYZE data limits remain |
| Session EXPLAIN final report budgets | Four additional unchanged session API guards fail the original direct report return, separately crossing EXPLAIN/ANALYZE and configured rows/payload; an empty inner read isolates the report | The same report admission for inactive sessions, preserving inner ANALYZE limits and active-transaction rejection |
| Cached candidate-window admission | A precise production mutation changes only the cache key's window flag; the unchanged guard fails after an allowed template is cached | Reject the undeclared candidate window both before and after cached explicit-window execution |

Focused replay/verification uses the existing executor and plan-cache unit owners:

```sh
cargo test --locked -p hawdb-executor --lib transform::tests::scoring -- --nocapture
cargo test --locked -p hawdb-executor --lib blocking::sort::tests -- --nocapture
cargo test --locked -p hawdb-plan-cache --lib
bazel test //crates/executor:hawdb_executor_tests --nocache_test_results --test_output=errors
bazel test //crates/plan-cache:hawdb_plan_cache_tests --nocache_test_results --test_output=errors
```

The complete-stream bypass keeps all inputs and terminal assertions; only the
assertion order was changed in the replay so the output oracle runs before the
source-request diagnostic. Bypass worktrees restore production files exactly.
RED and GREEN are behavioral outcomes, not compiler failures or assertion
mutations. The final exact head, commands and receipts belong to the delivery
packet and PR; the wider required fuzz result remains a separate gate.

Implementing and regression-guard owner is @hawkingrei. Registered owners and
unchanged test budgets are:

| Guard family | Existing Bazel owner | Existing lane / local boundary | Target timeout |
| --- | --- | --- | --- |
| Typed program shape, missing/finite policy and scalar parity | `//crates/core:hawdb_core_tests` | `ci/skein-bazel-test-crates` | default medium, 300 s |
| Ranking, resident/spill, caps, cancellation and dispatch | `//crates/executor:hawdb_executor_tests` (unit member `hawdb_executor_unit_tests`) | `ci/skein-bazel-test-crates` | unit default medium, 300 s |
| Template and parameter binding | `//crates/plan-cache:hawdb_plan_cache_tests` | `ci/skein-bazel-test-crates` | default medium, 300 s |
| Ordinary request, EXPLAIN, cache and default-capability behavior | `//:hawdb_unit_fast_tests` | `ci/skein-bazel-test-root` | existing large, 900 s |
| The same request guards with actual ACL capability | `//:hawdb_storage_crash_recovery_tests` | opt-in manual target, local-only evidence here | existing large, 900 s |

The BUILD source globs and unit-suite registration discover these tests; no CI
job, retry, feature or timeout is added. The lane names identify existing owners;
exact-head discovery/results and human approval remain delivery-packet evidence,
not an inference from these registrations. Existing manual differential owners
stay local-only. The final-report replay's 13 ordinary request guards took 0.01 s
under default features and 0.32 s with ACL. The preceding checkpoint's focused
root group executed 67 tests in 8.51 s and focused ACL group 61 in 7.81 s. Those
latter groups include existing cache/observability guards, and neither is full
root or full crash-recovery qualification. On the recorded
macOS focused run, scoring's eight tests took 1.66 seconds and sort's nine
ordinary tests 7.03 seconds (one manual test ignored). These are observations,
not CI p95 or an incremental before/after claim. The separate plan-cache suite, including the new binding guard, executed 21
tests in 0.01 seconds in the isolated native replay. These measurements are not
an incremental CI duration claim. The small guard additions fit the existing
unit targets; exact-head CI duration still needs its own receipt.

Nonspill cases use isolated in-memory ledgers. Native spill qualification uses
one exclusively created synthetic temporary directory and removes only that
directory, with independent run-release checks. It covers the original ranking
case and signed-zero case under both resident and forced-spill budgets. A
one-entry TopN whose row fits its blocking cap cannot naturally spill; K=1 is
qualified on the resident path, while larger windows prove the spill comparator.
Minimal WASM compilation covers portable guards, but it does not prove browser
runtime, filesystem behavior, or a Mem query route.

An independently existing DISTINCT output analogue remains tracked by
[issue #880](https://github.com/nowledge-co/hawdb/issues/880). Its resident and
spill emitters do not enforce the transfer payload cap or use independent
output ownership. SQL directly consumes these kernels; Graph prepared dispatch
independently re-bounds transport payload. This correction does not claim the
DISTINCT kernel or all blocking operators are repaired. The owned follow-up
requires actual resident/spill RED/GREEN, complete DISTINCT semantics, parent
caps, stop/error/cancellation and downstream admission with ledger/run release.
