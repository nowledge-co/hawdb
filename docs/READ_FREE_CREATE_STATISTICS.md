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
