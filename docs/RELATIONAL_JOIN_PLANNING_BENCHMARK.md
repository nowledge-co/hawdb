# Relational Join Planning Benchmark

`cargo bench --bench relational_join_planning` records the SQL planning
baseline for connected two- through eight-table joins. It exercises the public
embedded SQL path so parsing, current-state binding, access-path selection,
join enumeration, lowering, admission, and execution remain observable through
the same contract used by a host process.

## Protocol

The deterministic fixture creates eight one-row relational tables. Each table
after the first has an indexed `parent_id`, and every query joins a longer
prefix of that chain while constraining the first primary key. For every table
count the harness performs three warmups followed by 31 measured executions.

The first execution records cold parse time. Every later execution must hit the
bound-neutral SQL template cache and report zero parse time while still
recomputing current-state binding, access paths, join planning, and execution
descriptors. A successful CSG-CMP plan must contain exactly one ordered planning
attempt; an eager `InnerJoinMemo` preflight fails the benchmark.

The JSON report uses protocol `skein-relational-join-planning-v1` and retains:

- table count and SQL byte length;
- cold parse time;
- bind, plan, and execute p50/p95/p99 nanoseconds;
- memo group and expression counts;
- canonical plan cost, selected strategy, and attempt count.

Memo counts and canonical cost must remain identical across all samples for one
shape. The harness also pins the protocol-v1 structural baseline to
`groups/expressions/cost` values `3/7/2`, `6/16/3`, `10/31/4`, `15/54/5`,
`21/87/6`, `28/132/7`, and `36/191/8` for two through eight tables.
Expression counts include physical hash/merge implementations admitted to the
shared memo budget. For this fixture that adds three candidates on the first
filtered edge and two on each later edge; groups and the winning probe plan's
cost are unchanged. The earlier logical-only expression counts were 4, 11, 24,
45, 76, 119 and 176. This is a structural accounting update, not a protocol
version change or a new performance claim. Timing
values are machine-local trend evidence and require a
same-revision, same-target reference before they can enforce a regression
threshold. They are not representative Mem-replica qualification and do not
authorize Skein in stable release artifacts.

## Verification

The Cargo and Bazel benchmark registries both include the harness. The Bazel
smoke runner keeps it in the resource-isolated optimizer benchmark groups:

```text
cargo bench --bench relational_join_planning
bazel run //:skein_bench_relational_join_planning
```
