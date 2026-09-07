# Selected-plan costing without repeated subtrees

Selected-plan traces use the same single-operator cost rules as ordinary graph
costing. Child costs are computed first, and each completed cost is copied into
the existing cardinality output slot. Slots are reserved in preorder, matching
`visit_plan_with_ids`: parent, left subtree, then right subtree. The root result
also supplies the trace's scalar cost and component breakdown.

For an n-node unary plan, the preceding trace path invoked operator costing
`n + n * (n + 1) / 2` times. The new path invokes it n times. The regression
reproduces 594 evaluations for 33 operators before the change and requires 33
afterward. This is an operation-count claim, not a wall-clock benchmark.

No cost formulas, component weights, cardinality defaults, public types, plan
selection policy or cache keys change. There is no persistent memo table or
second cost formula implementation. Child traversal finishes before entering
the large single-operator match, so its stack frame does not accumulate with
plan depth in unoptimized builds.

This fixes the repeated-subtree portion of #216, tracked independently in #326.
Per-node cardinality lookup work, histogram costs, explain rendering and
fingerprint materialization remain separate. In particular, it does not claim
that complete trace generation is O(n) or that the other #216 items are complete.
The deep-cost test covers 128 and 256 nodes without rendering explain output;
an initial full-trace test at 128 nodes overflowed in the unchanged explain
renderer under the default debug test stack. No stack limit was increased and
no renderer fix is claimed here.

## Verification

Normal regressions cover exact evaluation counts, deep cost collection, all
eleven unary cost rules, asymmetric binary branches, stable IDs, histogram-backed
range estimates, parameter refresh and saturating arithmetic. The differential
oracle retains the former per-node recursive costing strategy and an independent
preorder metadata walk.

The ignored local campaign generates bounded plans with three fixed seeds
(`326`, `7`, `0x5eed`), 256 cases per seed, at most 64 nodes and depth 10. Every
case checks root components, scalar cost, every node's ID/kind/cardinality and
exactly one cost evaluation per node. It is deliberately not part of ordinary
Cargo tests or default CI target expansion.

```sh
cargo test --locked -p skein-optimizer
cargo test --locked -p skein-optimizer selected_trace_cost_differential_campaign -- --ignored --nocapture
cargo clippy --locked -p skein-optimizer --all-targets --all-features -- -D warnings
bazel test //crates/optimizer:skein_optimizer_tests //crates/optimizer:planner_golden_test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test --nocache_test_results
```

The named manual Bazel campaign is
`//crates/optimizer:skein_optimizer_trace_cost_fuzz_tests`; the required local
fuzz suite includes it. No dedicated or default CI fuzz job is added.
