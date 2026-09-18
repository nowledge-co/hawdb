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

The JSON report uses protocol `hawdb-relational-join-planning-v2` and retains:

- table count and SQL byte length;
- cold parse time;
- bind, plan, and execute p50/p95/p99 nanoseconds;
- memo group and expression counts;
- canonical plan cost, selected strategy, attempt count, join order and access paths.

Memo counts and canonical cost must remain identical across all samples for one
shape. The harness pins the protocol-v2 structural baseline to
`groups/expressions/cost` values `3/7/9`, `6/16/14`, `10/31/19`, `15/54/23`,
`21/87/27`, `28/132/31`, and `36/191/35` for two through eight tables.
Expression counts include physical hash/merge implementations admitted to the
shared memo budget: three candidates on the first filtered edge and two on
each later edge. Group/expression counts remain unchanged. For two through
four tables, the selected chain starts at the constrained primary key (four
units) and adds covering non-unique probes (five units each). At five tables,
the reverse chain becomes cheaper: a one-row scan costs seven units, followed
by four-unit primary-key probes. The crossover compares `4 + 5 * (n - 1)` with
`7 + 4 * (n - 1)`. Every execution checks its result, join order and access kinds;
the report includes those paths so the cost change is visible. See the
[logical cost contract](OPTIMIZER_COST_MODEL.md) for raw components and weights.

Protocol v1 reported CPU-only costs of 2 through 8 for these fixtures. Those
historical values use different cost semantics and must not be compared with
v2 as latency measurements. The earlier logical-only expression counts were
4, 11, 24, 45, 76, 119 and 176. This is not a new performance claim. Timing
values are machine-local trend evidence and require a
same-revision, same-target reference before they can enforce a regression
threshold. They are not representative Mem-replica qualification and do not
authorize HawDB in stable release artifacts.

## Verification

The Cargo and Bazel benchmark registries both include the harness. The Bazel
smoke runner keeps it in the resource-isolated optimizer benchmark groups:

```text
cargo bench --bench relational_join_planning
bazel run //:hawdb_bench_relational_join_planning
```
