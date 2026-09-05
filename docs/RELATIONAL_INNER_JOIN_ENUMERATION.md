# Connected Inner-Join Enumeration

The `InnerJoinMemo` fallback enumerates connected left-deep plans. Its physical
contract consists of a base access followed by singleton probes; a memo
expression therefore joins a connected prefix to one new binding. The richer
CSG-CMP rewrite planner remains the first SQL planning attempt. The fallback is
still reachable, for example when a `LIKE` post-filter cannot be represented by
the current null-rejection binder.

## Enumeration and admission

Start with singleton groups, then expand each admitted group once. A predicate
produces a singleton complement only if exactly one of its bindings is absent
from the prefix. All predicates activated by that complement are attached in
input order. This also handles hyperedges: intersecting a prefix is insufficient
if two or more bindings are still missing. No Cartesian step is introduced.

Every connected prefix/singleton pair is admitted separately. A new expression
checks the expression budget, and a new group additionally checks the group
budget, before mutating the memo. Exhaustion reports the next required insertion,
not a hypothetical `2^n - 1` subset count. No all-subset list or fixed-width subset
mask is allocated. Alternative expressions share a group and are all available
before best-plan selection, whose costs, access dependencies, required-property
cache and deterministic tie breaks are unchanged.

For an `n`-relation chain the memo has `n(n+1)/2` groups and `n^2` expressions
(including singleton expressions). Thirteen relations require 91 groups and
169 expressions; 64 require 2,080 and 4,096. Connectivity does not make every
shape polynomial: a 13-relation star has 4,108 connected groups and must still
exhaust a 4,095-group budget at the next required group, 4,096.

The inner API defaults to 4,095 groups and 32,768 expressions. The embedded
`Database` facade instead inherits the general optimizer's default of 128
groups. Thus 13- and 15-table chains fit that facade budget, while a 17-table
chain falls back at group 129. Both defaults remain unchanged. Query execution
memory admission is independent of memo admission; deep probe chains reserve a
transfer batch per level even for small fixtures.

This is connected-subgraph/complement enumeration for the complete existing
left-deep plan space, not a general bushy DPccp implementation. Consolidating the
inner, rewrite and CSG-CMP planners is tracked separately in #187; adding bushy
physical alternatives or inventing null-rejection metadata is not part of #214.

## Regression coverage

- All 1,024 undirected five-binding graphs are compared with an independent
  exhaustive subset/partition oracle, including disconnected graphs.
- Seeded hypergraphs check noncontiguous IDs, input permutations and simultaneous
  predicate activation; connected permutation oracles compare selected plans.
- Exact group/expression boundaries, failed admission without memo mutation,
  long chains and genuinely exponential shapes keep resource limits observable.
- Public SQL tests use a `LIKE` post-filter to exercise the fallback, compare
  results/schema with explicit syntax order, and assert both successful planning
  and actual-work budget fallback. Their three-row fixtures use smaller transfer
  batches, without raising query or optimizer budgets.

```bash
cargo test -p skein-optimizer
cargo test -p skein --lib connected_enumeration
bazel test --nocache_test_results \
  //crates/optimizer:skein_optimizer_tests \
  //crates/optimizer:planner_golden_test //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

These structural checks do not assert a wall-time or RSS speedup. Fuzz remains a
local verification requirement, not a CI job.
