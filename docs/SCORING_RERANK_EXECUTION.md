# Scoring rerank execution

This records the physical scoring operator's resource contract for
[issue #293](https://github.com/nowledge-co/hawdb/issues/293). It is not the
complete Mem route migration. The ordinary query attachment and cache contract,
opt-in vector/text and observed-hop provenance, and canonical graph-seed producer
below are implemented. The Mem route migration, complete runtime acceptance and
delivery qualification remain part of that issue's acceptance boundary.

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
property values; the legacy returned-alias source still has no graph-hop/seed provenance; the
opt-in vector source below obtains actual observed hops. The legacy operator captures its own
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

Timestamp properties accept only a nonnegative `Value::Int` representing epoch
milliseconds. Floats, strings, negative integers and NULL count as missing.
Future timestamps have age zero. Under `Neutral`, a missing or invalid timestamp
has decay factor one and may outrank a valid recent timestamp; hosts must choose
this policy deliberately. `Reject` fails the entire scored request when any
candidate lacks a declared signal, including an OPTIONAL NULL candidate. It
does not discard that row, rank it last, or publish a partial ranking.

Equal combined scores retain candidate arrival order in resident and spilled
execution. Reproducibility across executions requires a total upstream
`ORDER BY` with an explicit tie key; a graph scan's incidental order is not a
stable ranking contract.

Property names refer to returned value aliases in this ordinary attachment.
The declared score column supplies `SearchScore`; this does not by itself prove
that an arbitrary host expression came from a vector or text retriever.
`GraphSeedScore` remains absent in this ordinary attachment. `HopDistance` is
absent without the explicit producer attachment below. Strict programs requesting them fail; no graph bound
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
the actual Mem route, complete runtime acceptance and delivery qualification
remain required. The canonical graph-seed producer below supplies the distinct
graph-relevance signal in the ordinary query pipeline. No product weights or runtime
activation are selected by these templates.

## Vector provenance through graph expansion

`ScoringRequest::with_vector_graph_input(seed_variable, candidate_variable)`
opts into one validated producer chain. `SearchScore` reads the actual vector
seed's raw similarity, even if WITH/RETURN omits or replaces its public alias.
Numeric/timestamp properties read the declared candidate's canonical node held
by the same snapshot; returned lookalike properties cannot replace those inputs.
`GraphSeedScore` retains its distinct graph-side query-term meaning and is not
supplied by a vector similarity.

Admission proves an actual VectorSeedScan, unmodified producer ID-column
lineage, one nonoptional canonical ID lookup and a connected expansion chain.
Both AdjacencyExpandExec and connected GraphMatchExec use the traversal's
observed hop, accumulated over expansions. GraphMatch imports, unrelated scans,
extra lookups, branches, variable rebinding, aggregates, joins and DISTINCT are
rejected instead of guessing provenance. Optional unmatched expansions report
missing hop/properties. A genuine node with internal ID0 and hop0 is valid;
node IDs cannot substitute for the producer's actual match result.

Two reserved engine annotations travel in existing Binding values. Existing
row/pipeline/blocking/spill accounting charges their full footprint; projections
preserve them only for the query-owned opt-in descriptor, and ranked output
strips them before adding the public score. Reserved output aliases and direct
projection reads are rejected. Public Binding fields remain compatible. No
thread-local state or host-provided score maps participate. The existing observer
exposes one immutable descriptor to kernel contexts through a default method.
Typed cache keys, physical fingerprints and EXPLAIN include the descriptor;
coefficient/clock rebind is unchanged. Vector procedure plans retain the current
explicit cache bypass, so this does not claim a vector cache-hit execution.
The cost model charges two additional logical bookkeeping units per candidate;
these units are not wall-clock coefficients.

Actual embedded guards use controlled external seed rows under the existing
VectorSeedScan resource contract and a synthetic stored graph. An independent
ranking/score-bit oracle covers hop0/1/2, a late winner, complete input, K1 versus
seed admission8, public alias replacement, two expansions, optional missing,
unsupported lookup, descriptor identity, EXPLAIN and marker non-leakage. Both
ordinary and multi-stage query results must agree. The default parser migration
in https://github.com/nowledge-co/hawdb/pull/868 now sends both forms through the
clause pipeline; the same scoring, provenance and parity guards qualify that
integration. Post-MATCH projections exercise the public entrypoint. This is controlled
producer/engine graph evidence, not RaBitQ retrieval, browser or Mem route proof.

## Text provenance through graph expansion

`CALL text_search($query, topK := $window)` uses a separate logical text seed
and `TextSeedScan`, with the `FullTextSearch` runtime capability. Query text must be a string parameter; the nonnegative producer window may
be a literal or bound parameter and defaults to 10. Plain CALL/RETURN and YIELD aliases expose document `id` and raw `score`.
A graph read requires YIELD followed immediately by a nonoptional seeded
MATCH. It uses the producer's canonical `external_id`, independently of its
document ID, and shares the existing bounded graph expansion contract.

`ScoringRequest::with_text_graph_input(seed_variable, candidate_variable)`
validates a real text producer and the same canonical lookup/connected
expansion lineage. `SearchScore` reads raw BM25; canonical properties and
observed hops survive projections that replace public score/property/hop
aliases. A vector descriptor cannot certify a text producer. Both seed kinds
use private query annotations; `GraphSeedScore` remains absent. Descriptor
kind participates in scoring-request and physical instance identity. Text
procedure plans retain the vector procedure's explicit cache bypass, so
coefficient/parameter changes do not constitute a text cache-hit claim.
Lookup visibility predicates use the same reserved-reference admission as
Filter and MATCH, including nested expressions and either comparison operand.
A hand-built text or vector scoring plan cannot read private seed/hop
annotations through that executable predicate slot.

The external text method defaults to an unavailable-projection error, preserving
existing vector providers. Hosts receive separate working and result accounts
backed by an up-front reservation in the current query root. The result builder
reserves row storage before allocation and ID payload before copying. It retains
the reservation with its rows; borrowing cannot detach ownership. A builder
transitions from healthy to permanently failed after any rejected append, and
execution refuses its staged rows even if a provider catches that error. Foreign
result accounts are refused. The executor checkpoints before and after the read,
charges binding copies before allocation, and retains those charges through
delivery. Empty producer/final windows skip the source.

Canonical `NodeColumnLookup` applies the request's node visibility predicate
before accepting the node, independently of projection metadata. This applies
to text and vector seeds and ordinary normalized column lookups. A forbidden
optional match remains unmatched and emits its null row; it cannot retain the
forbidden node. The predicate survives logical rewrites, physical lowering,
parameter binding, instance fingerprinting and EXPLAIN. Ordinary cached lookup
plans rebind the actor's policy values, while text/vector procedures keep their
explicit cache bypass. Projection selection before the producer window is
still the host adapter's responsibility; this graph check does not supply that
missing actual Mem adapter.

Owned seed emission reuses the producer's allocation for short results. Larger
results stage only actual rows, with the next batch allocated after the previous
emit returns. Empty results allocate no staging batch. Configured `batch_rows`
therefore cannot amplify one admitted row into a large unused staging buffer.
The text producer retains its binding-copy charge through Stop/error/delivery;
the same shared owned-row emitter serves vector and graph-algorithm outputs.
A direct capacity/root guard covers a one-row result with `batch_rows=8192`,
multiple batches, consumer Stop/error, zero windows and terminal lease release.
Ordinary projection, filter, limit, graph expansion, generic MATCH and
Cartesian output buffers also allocate slots only for admitted actual rows.
Shared accounted batches and owned sets avoid configured empty capacity and
eager replacement. Transforms bound row-slot capacity by their admitted payload
allowance. The lookup wrapper owns a separate bounded output lease, retained
through final delivery, and propagates the consumer's Stop/error. Real physical
text pipelines cover these independent buffers and terminal release; these
guards do not claim complete allocator/RSS accounting of every producer.

The immutable `SearchOutOfCoreReader::text_seed_scores_with_context` provides
raw BM25 over the complete live corpus, bounded retained scores, accounted
analysis/lexical work and caller-supplied scope selection before its producer
window. Its document-byte report includes lexical ID/length mapping blocks;
it is not a document-body hydration counter. `topK` bounds output, not corpus
or posting work. Text scan estimates currently provide an output-transfer
floor for an opaque host producer, not a calibrated estimate of all reader work.

Ordinary-query tests exercise this real immutable reader with distinct document
and canonical IDs, a stored graph, spoofed public aliases, a late graph winner,
independent final K, EXPLAIN, parameter/weight changes, cancellation, unavailable
capability/provider and result ownership refusal. Their pinned ID adapter is a
test fixture. The actual Mem scoped provider, complete-cohort score/reason parity,
graph seed and final qualification/delivery remain required for the full issue.

## Host-scoring escape hatch

The planned identity is checked before candidate staging and retained through
the callback's pre/post checks. Direct cohort kernels capture their identity
before executing the source. A source cannot change shared callback name,
version or declared cost and establish a replacement baseline. Explicit
candidate windows complete at their LIMIT owner; consumer stop and incomplete
upstream execution retain their distinct stop behavior.

Scoring attachments admit only candidate queries supported by the batch
pipeline. An unsupported administrative procedure such as `project_graph`
returns a semantic refusal for execution, EXPLAIN and ANALYZE; the same
statement remains available without scoring. Manually constructed legacy,
Program and Host scoring plans also return a typed refusal if their input
forces the materialized fallback, including plans under unary or binary
ancestors. The materialized owner checks the complete plan before executing
any unsupported scoring input.

The facade exports the batch-oriented `HostScorer` contract required by the
issue, with a validated borrowed `HostScorerDescriptor` (name, version and
nonzero logical CPU units per row) and `HostScorerBatch`. Batch inputs borrow
feature rows, one fixed clock, task context and the existing query scratch
account. The host writes into an engine-owned score slice in input order;
scratch allocation must reserve the supplied account first and release its
leases before returning. Name/version identify the scoring implementation and
semantic policy; per-request parameters remain on the current callback.

Mem's adopted policy establishes the concrete demand: PageRank normalization
uses the exact complete cut candidate window, while lifecycle only refines
contiguous exact direct-score and byte-identical reason ties. That policy cannot
be evaluated independently per transfer batch. `HostScoringRequest` borrows a
callback and attaches through `QueryRequest::with_host_scoring`. Its cached
`HostScoringPlan` owns only validated name/version/cost, input column, finite
candidate cap, final K and optional vector/text producer metadata. Cache identity
includes all these structural fields and the explicit candidate-window choice;
the clock is captured once before parsing and rebound on every invocation.
The executing request supplies its own callback, including its current
parameters. A recursive borrow of the same callback returns an execution error.

`HostScoringExec` stages the complete admitted stream, with candidate capacity,
retained payload, feature/trait-view arrays and callback identity/output charged
before retention or allocation. Candidate cap is independent of final K and
output admission; an existing query LIMIT/OFFSET requires the explicit retained
candidate-window opt-in. Cohort-dependent callbacks require their finite cohort
to fit resident memory. Cohort and downstream TopN share the blocking allowance;
insufficient capacity fails closed. TopN still uses its existing bounded,
stable, spillable implementation. Callback output starts as NaN and cannot
reach ranking until cancellation, complete finite output and unchanged identity
validate. Mid-call work must check the borrowed cancellation context.

`ScoringFeatureSource::returned_value` borrows declared result metadata, such as
the exact reason/tie key; it neither exposes private NUL annotations nor proves
retrieval provenance. Vector/text score, hop and candidate properties retain their
producer attachment. The actual Mem text provider and distinct GraphSeed attachments, the
actual private Mem route and adopted full-result parity remain required for
whole-issue acceptance. The reusable callback/query fixtures alone do not prove
that route. Native shared-library discovery, WASM UDFs and a new scorer runtime
remain outside this contract.

## Projection admission around scoring inputs

Streaming projection borrows existing literals, columns, properties and selected
default/CASE values until the complete output row is admitted. Its row estimate
uses the same deterministic `binding_memory_bytes` rules as the owned result,
including unique aliases, retained graph bindings and private seed annotations.
A large literal or several repeated columns can therefore refuse without first
copying those payloads. CASE/COALESCE retain lazy branch selection; LEFT can
construct a short result from a borrowed large string.

Expressions that construct strings or node/relationship maps admit their
temporary values to a separate `ProjectExec expressions` working account before
allocation. Working map/alias storage is also admitted before construction.
These leases remain live while their temporary values are retained. They release
before the consumer boundary, on output ownership transfer, and on refusal,
stop or error. The output builder splits on actual aggregate row bytes. When
the next row needs a new batch, its preparation is discarded before offering
the existing batch to the consumer and repeated only after Continue. Consumer
Stop prevents that next owned row.

These are deterministic value and batch accounting contracts, not process-RSS
or every-allocation measurements. Owned expression helpers outside this streaming
projection retain their existing caller-owned validation contract. Dedicated
allocation observations cover the executing test thread. TextSeed guards begin
after input admission; stored-property guards include the actual scan producer,
property selection, and hydration. Fixture and plan construction are excluded.

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
| Genuine vector/graph producer provenance | Independent production mutations of original seed similarity, GraphMatch bounded hop, cumulative hop, canonical properties, projection transfer and public annotation stripping fail unchanged actual stored-graph queries; real internal NodeId0 failed before its correction | Complete independent ID/score-bit oracle, legacy/pipeline parity, downstream K1 with upstream admission8, missing optional hop and absent public annotations |
| Declared vector producer policy and identity | Separate mutations of unrelated-scan, duplicate-alias, reserved-annotation and collapsing-source admission and typed descriptor identity fail their existing guards | Actual pre-external query rejection where applicable; independent physical policy/identity assertions, not ANN or runtime vector cache-hit proof |
| Graph scope reintroduction | Old producer checkpoint admits actual queries that drop seed or middle scope then MATCH the same variable name; unchanged guards fail | Reject before external execution; retain valid connected traversal, node-only/optional analogue and whole query ranking |
| Reserved scalar references | Old producer checkpoint admits specialized/nested private column projections and private filter/order expressions; unchanged physical-policy guards fail | Exhaustive binding-name checks through projection, filter, MATCH predicate, Sort and TopN; literal data and ordinary public references remain valid |
| Scored OPTIONAL NULL source | Frozen scoring checkpoint `38cff412` traverses a real node from an unmatched OPTIONAL; the unchanged public guard fails | Required traversal after NULL yields no rows across zero/one/two WITH stages, using SearchScore alone so missing-hop rejection cannot hide an incorrect graph result |
| Scored unknown-type OPTIONAL | Frozen scoring checkpoint `38cff412` drops the unmatched row; the unchanged public guard fails | One NULL candidate row retains the genuine seed score across zero/one/two WITH stages |
| Scored unknown-type zero hop | Changing only GraphMatch's unknown-type zero-hop callback to report hop1 produces score0.35 instead of0.7; the unchanged actual query guard fails at two WITH stages | The real seed candidate and canonical property, genuine similarity, observed hop0, unchanged upstream admission8 and absence of public annotations across native and generic paths |


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
| Vector producer lineage, reserved-expression policy and scope admission | `//crates/plan-cypher:hawdb_plan_cypher_tests` | `ci/skein-bazel-test-crates` | default medium, 300 s |
| Template and parameter binding | `//crates/plan-cache:hawdb_plan_cache_tests` | `ci/skein-bazel-test-crates` | default medium, 300 s |
| Ordinary request, EXPLAIN, cache and default-capability behavior | `//:hawdb_unit_fast_tests` | `ci/skein-bazel-test-root` | existing large, eternal (3600 s), inherited from main |
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

The DISTINCT output analogue in
[issue #880](https://github.com/nowledge-co/hawdb/issues/880) was independently
fixed by [PR #883](https://github.com/nowledge-co/hawdb/pull/883), now included
from main. Its resident and spill emitters enforce transfer payload caps and
independent output ownership through the shared batch contract. SQL consumes
these kernels; Graph prepared dispatch also bounds transport payload. This
integration retains those guards and separately verifies the executor and
relational owners. It does not claim every blocking operator is repaired.

The vector producer additions register six actual facade queries and three
initial physical-policy guards; the scope correction adds three facade guards
and four physical-policy guards. The OPTIONAL integration adds two facade guards
for NULL-source and unknown-type row preservation, plus one scored unknown-type
zero-hop guard. The latter executes all zero/one/two WITH variants with a strict
similarity/property/hop program; its default-feature execution took 0.03 seconds.
The two NULL guards together took 0.01 seconds in the integrated focused replay.
Their distinct evidence boundaries are listed above. Each belongs to the
existing root or plan-cypher owner, with unchanged
budgets. The initial default focused root group of 85 and ACL group of 79 ran in 8.04 and
10.68 seconds; three policy guards ran in 0.00 seconds. Correction measurements and exact-source
results belong in its final qualification packet. These are local observations,
not an incremental CI timing claim. Current parser selection and hidden
normalization qualification remain separate from the actual public
legacy/pipeline parity oracle.


## Canonical graph-seed provenance

`CALL graph_seed_search($query, label := $label, topK := $window)` yields
`node` and `score`. Query text must be a string parameter; label is a required
nonempty string literal or parameter. `topK` (also spelled `limit`) is a
nonnegative seed window, defaulting to 10. Unknown labels, empty term sets and
zero windows produce no candidates. YIELD aliases preserve the node's native
canonical binding, including when different nodes share the same business ID.
There is no projection-ID rebinding or host score map.

The producer preserves the existing knowledge graph retriever's relevance:
for each of `id`, `title`, `name`, `summary`, `content`, `body`, and `text`,
count distinct query terms matching distinct property terms; add two for a
case-insensitive ASCII substring match and eight for an exact `id` match.
Term boundaries use Unicode alphanumeric characters and underscore. Other
property types use the existing external-ID text representation. Scores select
the bounded seed window in descending order, breaking ties by canonical node
ID. Request visibility predicates filter canonical nodes before this window.
Ordinary WHERE clauses retain their explicit position in the query pipeline.

`ScoringRequest::with_graph_seed_input(seed_variable, candidate_variable)`
(and the equivalent host-scoring attachment) supplies `GraphSeedScore`, observed
cumulative hops and pinned canonical candidate properties. `SearchScore` stays
absent for this producer; replacing a public score or property alias cannot
change these features. Connected expansion remains bounded to two hops and
uses the existing fail-closed producer-chain admission: imported graph
identities, rebinding, unrelated scans, joins and aggregates remain unsupported.

The scan admits every owned canonical record before cloning or decoding and
retains only the requested best seeds. Token buffers, non-string property text,
retained candidates and emission ownership share the query ledger; runtime
checkpoints cover admission, per-property/per-term scoring and emission.
Operator, query-root and validated result budgets fail the whole query.
Candidate K and final ranking K remain independent. Plan kinds, costed full-label
scan work, fingerprints, parameter/visibility traversal and EXPLAIN identify the
producer and its typed scoring kind. Procedure plans retain the explicit cache
bypass used by text/vector procedures; no cached procedure execution is claimed.


## Seeded expansion resource limits

Every budgeted graph, text or vector expansion refuses the query on resource
exhaustion, including ordinary materialized reads, scalar ranking, and inputs
to host scoring through Sort or TopN. Candidate exhaustion returns
`HawDBError::GraphExpansionCandidateLimitExceeded { requested, limit }` in rows;
payload exhaustion returns `GraphExpansionPayloadLimitExceeded` in bytes.
These errors propagate from the expansion admission owner before accepting the
next row. A scorer is never invoked with a resource-truncated cohort. An explicit
Cypher LIMIT or declared retrieval result window retains its query semantics.

The default expansion allowance is
`max(topK, min(max(topK, 1) * max(hops, 1) * 64, 8192))` rows and 8 MiB of
cumulative logical payload. These values double the previous allowances. A
host can set `DatabaseConfig.execution_memory.graph_expansion_budget` to
`Some(GraphExpansionBudget { candidate_limit, payload_byte_limit })`; `None`
uses the optimizer-derived allowance. This override applies to the actual
execution of each budgeted expansion, including reused plans. The error names
the matching configuration field and its unit. Raising either expansion limit
does not raise `query_memory_bytes`, `blocking_operator_bytes` or
`batch_payload_bytes`; live allocations must still fit those memory budgets.
The cumulative expansion payload limit bounds traversal work, rather than
measuring current resident memory after filtering.

Shared `binding_memory_bytes` now estimates retained resident ownership,
including property-map container storage and nested List/Map values. Sort,
TopN, joins, aggregation and owned batches all use that estimate. The same
numeric cap can therefore refuse a row accepted by the former logical-payload
estimate. Standalone expansion also checks this estimate and, with a query
account, admits simultaneous source and output ownership before copying. Its
supplied numeric cap and existing execution-error family remain, while its
accepted-input set may be narrower.

Public Filter and Limit kernels admit a complete owned row and flush on byte
boundaries before pushing it. Public projection and optimized projection
producers recheck the supplied task after a flush callback returns Continue,
before recreating expressions or owning the next payload. Native dispatcher
checks remain additional protection; custom public sources need not implement
the dispatcher's task or byte validation.

Contiguous evidence bands use the caller-declared public score and reason
columns in stream order. They do not validate how the caller ordered or
constructed those columns. Private producer provenance still protects
SearchScore, GraphSeedScore, hop distance and canonical candidate properties.


## Query memory and retained operator state

The policy adopted for https://github.com/nowledge-co/hawdb/issues/293 uses
`query_memory_bytes` as the shared allowance for concurrently owned, accounted
memory. Native node/index/projection reads, graph hydration, numeric input and
predicate/expression temporaries use this allowance. Reading a large payload
does not consume `blocking_operator_bytes`. That independent allowance limits
state kept for sorting, aggregation, deduplication, traversal and selected
GraphSeed top-K candidates. Blocking reports describe that retained state;
source and result materialization remain visible in the query ledger.

A GraphSeed read holds a source grant through visibility and relevance checks.
Rejecting it releases that grant. Accepting it transfers the same charge to the
retained account atomically, without increasing the query total. A rejected
transfer preserves the original grant and ledger counters. Replacing the worst
retained candidate releases the displaced candidate before admitting its
replacement. Ordered relationship keys and cold equality/union/lookup dedup
keys also take the retained allowance before their containers grow. Direct
public scan contexts attach their supplied retained limit at this read boundary;
the outer union and cold reader reuse one account for concurrently retained
keys. Standalone allocators retain their explicit numeric policy. Streaming
analytics stages canonical output identities under result materialization on
the query root, keeping that grant while the identity map is alive. Its scalar
algorithm result vector still obeys the retained-state allowance.

Native full-node Filter pushes its complete predicate into the scan; optional
property pruning does not replace predicate evaluation. Its other store-backed
input paths return after each produced binding, so dropped rows can release
source grants before the next source admission. The caller's output batch
limits remain in force. External text/vector result cohorts retain their existing
batching and ownership. Lookup can materialize multiple matches for one input:
unconsumed matches are actually live and keep their grants through downstream
callbacks, including Stop and errors. Public arbitrary multirow sources do not
provide per-row release notifications; no equivalent per-row promise is made
for their callback sidecars.

Source-wave scratch is a bounded conservative reservation while the storage
visit runs. It is released before fallback and final output callbacks. As with
other codec and I/O allowances, this is not a proof of all decoder heap usage.
Transfer batches may reserve their whole configured payload allowance before
allocation; spill children and workers retain their admitted allowances. These
charges describe live reservations and estimated ownership, not exact allocator
bytes or process RSS. Releasing an owner returns its allowance. Reading many
batches does not cumulatively consume query memory. The separate cumulative
seeded-expansion payload limit still bounds traversal work after filtering.

Every failed admission remains a query error rather than a successful truncated
result. No default query or blocking memory size is changed by this policy; the
expanded candidate and logical-payload defaults described above remain in force.
