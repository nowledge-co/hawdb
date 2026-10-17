# Read-free creation planning proof obligations

This note describes the private optimizer-statistics change for
[issue #1001](https://github.com/nowledge-co/hawdb/issues/1001). It is a source-level proof outline, not a machine-checked verification
of the optimizer, executor, or durability subsystem.

## Definitions and eligible shapes

Let `G` be the committed graph, `C` its live catalog, `P` the supplied parameter
map, and `L = bind(parse(Q), P)` the logical plan after the existing semantic
planner and optional access-control transformation. The new predicate accepts
exactly these two root shapes:

- `CreateNode { label, properties }`.
- `UnwindMutation { rows, variable, operation: CreateNode { .. } }`.

All other shapes use the existing catalog/statistics policy, including MERGE,
matched mutations, reads, graph algorithms, and FTS. Eligible mutations retain
their existing plan-cache bypass; this change never adds parameterized mutation
plan-cache entries.

## Physical-plan equivalence

For each eligible `L`, `graph/lowering/mutation.rs` copies its bound operands
directly into the corresponding physical leaf. Its input arity is zero. Single
CREATE has constant local cost, while UNWIND mutation cost depends only on the
bound row-list length; neither local cost reads graph statistics. Therefore,
for every graph `G`, full statistics `S(G)` and basic statistics `B(G)` produce
the same mutation operands and physical operation for these shapes. Live schema
and ordered-range capability keys still come from `C` and the current store.
The existing Cascades optimizer and search directive remain in the call path;
no directive is silently ignored or converted to an alternate executor.

The conclusion concerns operation and bound operands. Trace decisions explicitly
identify the basic-count path; they intentionally differ from full-statistics
refresh decisions. Future optimizer rules that add graph-dependent choices to
these leaves must revisit this eligibility predicate and proof.

## Shared-publication preservation

Let `K` contain the shared statistics, source epoch, publication key, generation,
and optimizer-catalog cache. Eligible catalog construction reads the generation
and creates an ephemeral catalog from current basic counts, but writes none of
these fields. Thus creation planning maps `(G, C, K)` to `(G, C, K)` before
execution. The executor commits or rejects exactly the existing mutation.
Subsequent graph-dependent planning still calls `ensure_statistics` with the
same lag policy, so it cannot mistake the ephemeral basic-count catalog for an
advanced-statistics publication. Existing successful write/schema invalidation
behavior is preserved; this claim concerns the planning step itself.

Creation consequently stops incidental publication of a new advanced-statistics
generation. Cached read plans remain semantically legal across data changes;
their executor still reads the current snapshot. The physical cache additionally
tracks the existing small-label index cost boundary described below. A cache
miss refreshes graph-dependent planning with the existing lag policy. General
mixed-workload plan quality remains a performance qualification obligation.

## Small-label index cost boundary

Let `T = NODE_INDEX_SMALL_LABEL_SCAN_THRESHOLD` from the optimizer's costing
module. For each distinct label with an equality, range or composite scalar
index, let `b(S, label) = (maintained_count(S, label) > T)` for read snapshot `S`.
The read cache key includes the sorted `(LabelId, b)` vector in addition to its
existing schema, parameter and published-statistics identities. Full-text-only
and unindexed labels contribute no entry. The threshold is shared with the
optimizer rather than duplicated in the cache.

For fixed schema and parameters, crossing `T` changes one vector component.
The old/new keys therefore differ even when CREATE leaves the published
statistics generation unchanged. Full key equality resolves hash collisions,
so the old key cannot produce a hit. The normal miss path refreshes statistics
according to the configured lag policy and optimizes again. With default zero
lag and a selective indexed equality, the new large-label plan can choose the
existing index alternative. Shrinking back across `T` also changes the key.
Remaining in the same class does not invalidate a plan solely because of these
maintained counts. This proves threshold observation, not globally optimal
costs for arbitrary distributions or a measured latency benefit.

Counts come from the supplied read view, not the live database head. Thus an
older snapshot retains its own class after newer writes. The classification
reads maintained metadata and does not request/publish advanced statistics or
walk graph records. For `I` scalar index descriptors and `L` labels/relationship
types, its native metadata work is O(I log I + L), independent of graph row count;
the key stores at most one entry per indexed label. Existing metadata allocation
is not a whole-query memory proof.

Read-free CREATE/UNWIND CREATE bypass the physical plan cache and construct an
empty cost-class vector. They do not call this classification helper or clone
basic counts a second time. The new field is cost-only: explicit prepared-plan
execution compatibility continues to check schema and ordering capabilities,
not data/statistics/cost classes. No operand, result, WAL or durability rule
changes. The assumptions are consistent maintained counts and the existing
cache miss/parameter-binding/execution contracts; these are source-level proof
obligations, not machine-checked whole-system verification.

## Failure and snapshot preservation

Missing parameters and semantic errors are rejected by the unchanged binding
path before catalog construction. Runtime type/constraint validation, mutation
limits, cancellation, undo/WAL, commit publication, and snapshot acquisition
are unchanged and receive the same physical operands. Consequently, this change
adds no mutation before execution and no recovery dependency. Regression tests
must still exercise these shared paths rather than treating this argument as
whole-system verification.

## Complexity bound

For N successful independent creates into an initially empty materialized graph,
full statistics before operation k visit at least k-1 existing nodes. With the
default zero commit-lag policy and mutation cache bypass, those visits sum to
`N(N-1)/2`. The new eligible catalog path uses maintained basic counters and
schema/capability enumeration. It performs no graph-record statistics walk.
For fixed schema, that particular planning term becomes O(N), excluding bound
parameter copying, indexes, execution, WAL, fsync, and result verification.

This does not imply total insertion becomes linear, a general write speedup,
or a measured performance ratio. Identical default-opt before/after workloads
with per-phase timings, parity, RSS, and unfavorable samples remain required.

## Regression checks and measured scope

The four `creation_planning_` regressions in `src/api/plan_cache.rs` cover
single/bulk/empty creation, shared statistics publication, subsequent read and
MERGE refresh, live type descriptors, missing parameters, failed-write epoch
stability, old snapshot visibility, bulk unique-constraint rejection,
cancellation, WAL reopen, observable shape selection, and unchanged optimizer
search directives. Before the production change, the publication and shape
regressions fail; the other two establish preserved execution behavior.

The `point_plan_cache_` regressions exercise template lookup without force-refresh
EXPLAIN: growth within the small class hits, the 8-to-9 transition misses and
selects an index, growth within the large class hits, shrinking to eight misses,
old snapshots retain their class, and unindexed label growth remains reusable.
They verify exact values and unchanged CREATE statistics publication. The
original implementation fails at the expected boundary hit/miss assertion.

Use the repository's pinned toolchain and the focused root test target:

```console
cargo test --locked -p hawdb --lib plan_cache
bazel test //:hawdb_unit_fast_tests --test_arg=plan_cache
```

[Ordinary controls](../bindings/benchmarks/RUST_ORDINARY_RESULTS.md) preserve the
separate historical controls and budget refusals, including a complete frozen
CREATE control at runtime `6ce232ca`. Its write improvements do not qualify this
subsequent cache-boundary change. The negative fixed-parameter hot-point control
remains recorded. New three-producer controls at 1,000/10,000 rows and a complete ordinary
320-record control freeze this fix with unchanged fixtures, calls, compiler
profile, defaults and consumers. They improve hot/amortized points and ordinary
writes against main, but preserve unfavorable first-call, scan and bulk samples
and unchanged wide-result budget refusals. They do not complete performance
or whole-operation memory acceptance.

## Bounded statistics for ordinary indexed point reads

The first ordinary read after read-free creation must not hide an O(N) advanced
statistics walk behind a single-row lookup. A query-local catalog is eligible
only for a parameterized template shaped as `Project(Filter(PropertyEq,
NodeScan))`, with matching variables and node/property/id/literal projections.
Access control, scoring, non-scalar or non-finite operands, out-of-core stores,
undeclared indexes, unsupported readers, complex operators and bound EXPLAIN
continue through the existing full-statistics path.

The operand uses the plan-cache crate's existing marker binder. The planner then
uses the same complete-index, same-snapshot and materialized-store contract as
the execution reader. Its borrowed iterator visits at most nine candidates,
without cloning their graph payloads. If it exhausts after K <= 8 candidates,
K is an exact snapshot-local equality count. A ninth candidate instead rejects
the query-local catalog. No result limit or storage interface is introduced.

The ephemeral optimizer catalog contains maintained basic counts and one
query-local equality row count. It does not impersonate whole-index size or
distinct-count statistics, and it is never published in the shared statistics
or catalog cache. Existing estimates still clamp zero to one. Existing physical
optimization, parameter rebinding, projection lowering and execution remain in
use; this is not a handwritten point executor.

The physical cache key additionally contains `(label, property, K, B)`, where B
is the equality index/scan access-cost decision from the optimizer's shared cost
function. K changes invalidate the key, as does crossing the access boundary
while K remains constant. Different bound operands with the same K and B can
reuse the template: this eligible catalog contains no value-specific histogram,
and execution binds the operand again. Existing parameter-shape/exact keys,
schema, capabilities, published generation and small-label classes still apply.
Full key equality, not hash uniqueness, determines reuse. A retained snapshot
derives K and B from its own reader, never from live-head candidates.

The temporary point-statistics descriptor borrows the label and property from
the parameterized logical plan. The persistent cache key still owns both names,
and a cache miss gives the ephemeral optimizer catalog its own names. This
removes two temporary string allocations per eligible planning call without
omitting any cache identity field. Query normalization alone is not used to
prove label/property identity. The owned key may outlive the logical plan;
neither the cache nor its catalog retains a borrowed planning reference.

Under the existing index-completeness and maintained-count contracts, counting
K contributes a bounded index probe rather than a full graph statistics scan.
Metadata enumeration, parameter/value cloning and ordinary result allocation
remain outside that bound. Borrowed candidate iteration proves no graph payload
copy in this probe only; it is not source-to-host zero copy or a process memory
bound. Dense values and unsupported shapes retain their original planning costs.

The `point_statistics_` regressions cover cold publication, preservation of an
earlier publication, parameter reuse, empty/nonempty results, duplicate-key cost
boundaries, snapshots, whole-node results, missing parameters, dense fallback,
aggregation, null operands and bound EXPLAIN. DDL can itself leave a statistics
publication, so the cold regression explicitly invalidates that planning cache;
the separate preservation regression checks Arc identity and generation.
First-call, hot-call, total and tail latency controls are required before
admitting this production change. This remains a source-level argument, not a
machine-checked whole-system proof or comprehensive performance acceptance.

## Shared synchronous admission templates

The synchronous embedded admitted path prepares against the runtime's existing
template cache and optimizer-planning generation owner. Its execution read
transaction shares those same two owners while pinning its own catalog/store
view. Ordinary public read transactions and snapshot forks retain independent
template caches. Admission and execution still parse, semantically bind, derive
the full environment/key from their supplied view, and check output limits.
This eliminates the repeated empty-cache optimization, not those validation
steps or the execution-owned physical-plan binding.

Let a cached entry be `(key, Arc(template, trace, slots))`. Cache lookup clones
only the Arc handle. Binding clones the immutable template into a private
physical plan and mutates only that private plan; reporting clones the trace.
Eviction drops a cache handle and cannot invalidate a live lookup handle.
Neither entry stores a borrowed caller parameter, store payload or read view.
The full existing key, including schema, generation, parameter variants and
snapshot-local point count/cost class, remains the reuse condition. Sharing a
template therefore shares structure, not bound values or snapshot contents.

The admission's prepared/bound plan is dropped while its planning permit is
still live. The execution permit is acquired before the read transaction is
created and remains live through parameter binding, streaming and collection.
No owned prepared plan crosses an uncharged phase boundary, no extra execution
slot is retained, and no result budget or durability policy is relaxed.
Non-cacheable mutations still bind separately at execution. Read cache
eligibility now rejects `CurrentTimestamp` recursively in pattern properties,
predicates, scalar/CASE/list expressions, ordering, pagination and graph options.
The AST checks are exhaustive over the corresponding variants; parameterized
`timestamp($now)` and string literals containing a function name remain
deterministic. Volatile reads therefore rebind at execution instead of storing
a planning-time clock value. Persistent cache entries use the existing cache
lifetime/entry-limit contract (default 128 entries); this is not a new byte cap
or a whole-process memory proof.

The `admitted_sync_` regressions cover a cold miss followed by an execution hit,
parameter rebinding on both subsequent planning calls, disabled caches, missing
parameters, shared-cache older snapshots, schema changes, the eight-to-nine
index cost-class transition, volatile read bypass, cancellation, admission
rejection and permit release. The volatile eligibility regression also covers
nested expressions and deterministic timestamp parameters. The measured
controls and remaining performance gates are recorded in
[RUST_ORDINARY_RESULTS.md](../bindings/benchmarks/RUST_ORDINARY_RESULTS.md).
This mechanism does not establish Arrow source-to-host zero copy or
comprehensive performance qualification.
