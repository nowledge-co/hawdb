# Relational Executor Convergence

Tracking issue: [#215](https://github.com/nowledge-co/skein/issues/215).

## Scope and status

The first phase splits the relational query implementation into private modules.
It does not switch algorithms, remove the relational executor, or complete the
convergence described below. The embedded `skein` facade, SQL semantics, query
limits, planner decisions, and v1 spill encoding remain unchanged.

Convergence means one implementation of each reusable operator, not merely two
implementations with shared allocation helpers. Move storage-independent kernels
into the existing `skein-executor` crate, then migrate their consumers and delete
the displaced implementation. Do not add another crate or expose SQL execution
internals as a production integration API.

## Private module ownership

All paths in this table are relative to `src/relational_sql/query/`.

| Module | Responsibility |
| --- | --- |
| `../query.rs` | Embedded query entry points, admitted resources, and bound-row types |
| `preparation.rs`, `physical.rs`, `join_order.rs` | Bind the statement, select access paths and join trees, and describe admitted physical execution |
| `execution.rs`, `pipeline.rs` | Dispatch prepared execution, enforce pipeline budgets, count work, and rehydrate typed locators |
| `access.rs` | SQL predicates and storage access-path selection/visitation |
| `join.rs` | Shared relational join context, keys, row ownership, and relation visitation |
| `join_index.rs`, `join_merge.rs`, `join_hash.rs` | Batched-index, index-merge, and bounded grace-hash join execution |
| `projection.rs`, `streaming_projection.rs` | Buffered ordering/distinct adapters and streaming/index-ordered projection |
| `aggregate.rs`, `aggregate_state.rs`, `columnar_aggregate.rs` | Aggregate dispatch, grouped locator processing, SQL aggregate states, and columnar fast paths |
| `expression.rs`, `streaming_binding.rs` | SQL evaluation/coercion and borrowed-row expression binding |
| `locator.rs` | Typed row-set locators and relational sort records |
| `explain.rs` | SQL EXPLAIN rendering from prepared plans and execution reports |
| `tests.rs` | Existing private query regressions, retaining the `query::tests` test path |

Keep execution modules around or below 2,000 lines as later phases evolve them.
Split by ownership when needed; moving text into `include!` fragments does not
establish a reusable operator boundary.

## Operator mapping

| Relational path | Shared destination and retained frontend adapter | Removal gate |
| --- | --- | --- |
| `DISTINCT` and single `COUNT(DISTINCT ...)` | Already use `blocking::stream_distinct_batches`; SQL retains projection, column identity, null filtering, and result shaping | Preserve the existing shared path; do not introduce a second distinct implementation |
| Buffered `ORDER BY` / top-N | Already use executor external-order and blocking batch primitives; SQL retains typed locator hydration, SQL null order, and tie/order binding | Delete any displaced relational sorting kernel only after ordered-result and spill parity |
| Grouped aggregation and columnar aggregate fast paths | Bind SQL aggregates into reusable executor aggregate kernels; preserve frontend-specific null, empty-input, numeric, and overflow semantics | Replace locator-sort/state machinery only when the shared operator supports its resource and semantic contract; remove the old group/state execution in the same delivery |
| Grace-hash equi-join in `join_hash.rs` | Extract build/probe, partition/spill, and accounting machinery into an executor hash-join operator; keep relation access, SQL key conversion, null extension, and output binding in adapters | Relational and graph consumers use the same kernel; remove the extracted relational algorithm rather than leave a copied implementation |
| Probe and batched-index join | Shared join lifecycle with a storage-access/probe adapter; SQL planning still owns which index/key to probe | Preserve cache budgets, candidate-work limits, bag multiplicity, and outer-join behavior before deleting the old loop/cache machinery |
| Index-merge join | Shared merge-join kernel over ordered input adapters; storage retains index scans and SQL retains ordering/key compatibility checks | Prove duplicate-run, null, ordering, stop, and memory behavior before deleting the relational merge loop |
| Materialized/cross-product join | Reuse an executor binary operator once its binding and outer-join contract covers the relational path | Preserve explicit join predicates and null-extension semantics; do not substitute a Cartesian product for a missing hash-join implementation |
| Streaming scan/filter/project | Shared expression and batch execution where it preserves the borrowed-row contract; storage adapters retain lending and hydration | No new per-row owned materialization or unbounded batching; retire duplicate evaluation only after differential and allocation evidence |

Sharing `SpillBudgetTracker`, `QueryMemoryLedger`, or `ColumnarBatch` alone is not
completion of any remaining row in this table. Likewise, improving shared graph
aggregation does not prove that SQL `GROUP BY` uses that implementation.

## Phases and dependencies

### 1. Establish reviewable modules

Input: the current relational query implementation and its tests.

Move complete Rust items, retaining function bodies, attributes, test names, and
existing entry-point visibility. Limit visibility additions to the private query
module boundary. Keep planner and execution algorithm changes out of this phase.

Exit: default/minimal feature builds, root SQL regressions, the relational join
rewrite differential oracle, and local fuzz accept the split. Review moved-item
equivalence separately from compilation. This phase leaves #215 open.

### 2. Extract and qualify reusable kernels

Input: isolated relational operators and explicit frontend semantic adapters.

Start with the grace-hash join needed by
[#184](https://github.com/nowledge-co/skein/issues/184). Establish the common
build/probe/spill lifecycle before wiring a graph consumer; do not copy
`join_hash.rs` into a second permanent implementation. Aggregate extraction must
likewise reconcile SQL aggregate semantics with executor kernels before reuse.

Keep lowering convergence ([#183](https://github.com/nowledge-co/skein/issues/183))
and costed algorithm selection
([#204](https://github.com/nowledge-co/skein/issues/204)) distinct from kernel
extraction. Neither a new physical-plan label nor a cost estimate proves that the
selected runtime operator exists or is shared.

Exit: both consumer-specific and shared-kernel tests prove null behavior, bag
multiplicity, empty inputs, skew/hot partitions, spill cleanup, memory accounting,
candidate-work limits, cancellation, and error propagation. Preserve v1 encoding;
do not introduce a format migration solely for this refactor.

### 3. Migrate consumers and delete duplication

Input: qualified shared operators with the required SQL binding and storage
adapter contracts. Migrate one operator family per reviewable delivery.

Keep SQL parameter/type binding, storage access, borrowed-row lifetimes, result
column naming, and EXPLAIN formatting in the frontend. Bind planner choices to
real shared operators and preserve their observable execution reports. Delete
the absorbed relational implementation in the same change; do not retain a
hidden legacy path after qualification.

Exit: every remaining operator mapping has a shared implementation or a specific
documented frontend-only responsibility, no duplicated absorbed algorithms
remain, and the full SQL regression/differential/resource evidence passes.
Only then evaluate closing #215.

## Verification contract

For the mechanical split, the existing SQL tests and differential oracles are
the behavioral reference; moving test code must not change test selection.
Later semantic changes require new regressions and independent expected results,
not only agreement between two implementations that share the same kernel.

```bash
cargo check -p skein
cargo check -p skein --no-default-features
bazel test //:skein_unit_tests
bazel test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
cargo fmt --all -- --check
git diff --check
```

The fuzz suite includes the relational join rewrite differential campaign. Keep
these fuzz targets available for local use without adding them to routine or
dedicated CI jobs. Record actual target execution separately from cache hits and
bind remote CI claims to the exact PR head.

Kernel replacement also needs focused in-memory/forced-spill comparisons,
stop/error/cancel cleanup, memory-ledger readback, and EXPLAIN/profile parity.
Measure representative allocation, CPU, and latency costs before claiming that
an operator migration improves performance.
