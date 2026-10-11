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
generation. Already cached read plans remain reusable under the existing rule
that data changes affect cost, not plan legality; their executor still reads
the current snapshot. An uncached graph-dependent plan refreshes with the
existing lag policy. Mixed read/write workload plan quality is a performance
qualification obligation rather than a semantic equivalence claim about costs.

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

Use the repository's pinned toolchain and the focused root test target:

```console
cargo test --locked -p hawdb --lib plan_cache
bazel test //:hawdb_unit_fast_tests --test_arg=creation_planning_
```

[Ordinary controls](../bindings/benchmarks/RUST_ORDINARY_RESULTS.md) preserve the
completed pre-fix observations and budget refusals. They do not measure this
statistics change. A measured after-fix performance claim requires a new frozen
producer and the same fixtures, calls, compiler profile, defaults and consumers.
