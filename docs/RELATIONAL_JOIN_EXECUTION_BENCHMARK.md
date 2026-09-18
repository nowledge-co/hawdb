# Relational Join Execution Benchmark

`cargo bench --bench relational_join_execution` records a public-SQL execution
baseline for the relational join implementations. It uses a pinned
`DatabaseReadTransaction`, so preparation, current-state binding, physical
selection, admission, index access, row hydration, blocking memory, spill, and
result construction all use the embedded-library path available to hosts.

## Fixture and Protocol

The deterministic fixture contains four query shapes:

- `batched_index` uses repeated outer probe keys and a right-side secondary
  index. It must select `BatchedIndexNestedLoopLeftJoinExec`.
- `merge` uses ordered left and right secondary indexes. It must select
  `MergeJoinExec`.
- `hash` has no usable right-side join index. It must select `HashJoinExec`
  without spill under the default memory configuration.
- `grace_hash` executes the hash shape with a small blocking-memory budget and
  a private spill directory. It must select `HashJoinExec` and produce a
  `RelationalHashJoinGrace` spill report.

For each shape, the harness runs one cold execution, three warmups, and 31
measured executions. Every warm execution must hit the bound-neutral SQL
template cache and therefore report zero parse time. The JSON protocol is
`hawdb-relational-join-execution-v1` and records:

- cold parse and warm bind, plan, and execute P50/P95/P99 timings;
- the stable physical join operator ID, selected operator, estimated and actual
  cardinality, and fully-consumed state;
- intermediate rows, index and row logical/physical pages, cache counters, and
  visited index rows;
- every blocking operator's budget, peak tracked memory, and spill rows/bytes/runs.

The harness fails when an expected physical join path disappears, cardinality
is not observed, a join is not fully consumed, or the constrained hash case
does not report a Grace spill. Timings are machine-local trend evidence only;
they are not fixed performance thresholds or production qualification.

## P2 Decision Gate

This benchmark is deliberately an observation surface, not a runtime-feedback,
cache-sharding, or concurrent-read implementation. Such work requires
repeatable end-to-end evidence beyond an isolated local timing:

- introduce runtime feedback only after the emitted estimate/actual cardinality
  pairs show repeated, material misestimates for a stable workload shape;
- consider cache sharding only after persistent indexed workload evidence shows
  contention or cache admission pressure, alongside the dedicated
  `relational_index_page_cache` benchmark;
- consider concurrent relational reads only after host-level workload evidence
  demonstrates queueing or tail-latency pressure that the current serial read
  path cannot meet.

The benchmark does not authorize HawDB in stable release artifacts and does
not replace the full Mem replacement qualification suite.

## Verification

The Cargo and Bazel benchmark registries both include the harness:

```text
cargo bench --bench relational_join_execution
bazel run //:hawdb_bench_relational_join_execution
```
