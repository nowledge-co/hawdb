# Graph MATCH statistics and cost qualification

This is a bounded optimizer experiment, not full LDBC or Mem acceptance.
The baseline is `ab1330dcdb65ad5858897cdcf69b36def69ef350` plus the identical
manual example from this change. Candidate production sources and both native
binaries are identified by SHA-256 in [receipt.json](receipt.json).

## Motivation and existing benchmark evidence

The inspected CI host's existing LDBC bench5 table reports IC3/IC9/IC14 warm
medians of 184.3/464.9/1784.9 ms, versus round-4 Kuzu values of
29.4/80.2/206.1 ms. Existing profiles attribute much of the remaining gap to
adjacency reads and record/row work. These are prior measurements, not results
of this change; the older Kuzu run and the native binary/source relationship
were not independently requalified. Exact columns/data agree across four saved
passes and fifteen parameter bindings for each of IC3, IC5, IC9, IC10, IC12 and
IC14. That check compares the saved HawDB passes, not Kuzu or this candidate.

Inspected input receipts:

| Existing input | SHA-256 |
| --- | --- |
| bench5 table | `ff04dacfe27a0abbd78ecef613101cf7ca73a30c5d5baafad5af101ddfa91c71` |
| bench5 runner | `241bb869057581447d2ff2e3eedda381b3da8521cec332693560947fcdf74f2a` |
| bench5 native binary | `947faaae7e09027b7d255a697ce78ad49bf925b42aec0fc748674186b6480261` |

The optimizer still multiplies MATCH label populations and ignores actual
seeded-union branches in final cost. Path selection also raises a one-hop
label-pair fanout to a power, even when maintained multi-hop counts describe
heterogeneous intermediate labels. This experiment isolates that planning
failure instead of fitting cost coefficients to the historical latency table.

## Fixture and procedure

The [manual embedded example](../../../examples/optimizer_path_statistics.rs)
creates 2,000 A roots and 20 each of B, X and C, with equality indexes. Every A
has an R edge to its corresponding B and X; each X points to its B, and each B
to its C. There are 2,060 nodes and 4,040 relationships. The query is:

```cypher
MATCH (a:A)-[:R*2..2]->(b:B {key: 0})-[:R]->(c:C)
RETURN a.id AS id ORDER BY id
```

The independent expected output is exactly the 100 IDs `0, 20, ..., 1980`.
Every execution checks all ordered typed IDs and equality between strategies.
On one pinned in-memory snapshot, the example alternates the ordinary optimizer
and `CostBasedPatternStart`-disabled qualification, bypassing physical cache
admission. Each strategy has two warmups and fifteen measured executions.
Timing includes qualification planning and full execution, excluding fixture
setup and result assertions. Native processes ran baseline/candidate/candidate/
baseline sequentially; no build ran during these four measured processes.

Environment: macOS 27.0.1 (26A434), arm64, Rust 1.97.1, repository release
profile, default Cargo features, one query at a time. CPU model, affinity and
background host load were not controlled. The four raw JSON files retain every
sample, selected cost, decisions, execution profile and snapshot epoch.

## Observations

| Ordinary optimizer | Baseline | Candidate |
| --- | ---: | ---: |
| Median of 30 warm measurements | 3.494958 ms | 0.3706045 ms |
| Output rows / estimated rows | 100 / 800,000 | 100 / 100 |
| Positive cardinality Q-error | 8,000 | 1 |
| Selected scalar logical cost | 826,936,852 | 1,854 |
| Profiled query-ledger peak | 16,871,328 B | 16,871,328 B |
| Start | A label scan | B equality seek |

The source comparison is **9.43x** faster on this fixture. Candidate process
medians are 3.564042 -> 0.401542 ms and 3.505875 -> 0.343834 ms when switching
from its own rule-disabled plan to the ordinary plan. These within-process
controls support the plan-change explanation despite host timing variation.
The baseline keeps A because it estimates reverse work at 10,003 versus 4,100;
the candidate's maintained two-hop count lowers the B estimate to 103.

Query-ledger peaks are from the captured third round, not a total-process or
planner-heap bound. Raw RSS/page-fault fields are retained but are not isolated
memory comparisons. The patch introduces no retained statistics map, collector,
sample budget or disk format; temporary costing state scales with the statement.
Unbounded/disjunctive/ambiguous shapes and hash/shortest-path programs retain
explicit fallbacks. Property correlations, degree skew within a label pair,
mixed-direction undirected walks and uniqueness are still approximate.

Initial release compilation into an empty target took 4m33s. With ordinary
dependency cache reuse, the baseline source build took 2m11s and the final
candidate source build 2m33s; example-only relinks took about one minute.
These are local build observations, not CI or query execution speedups.

## Replay and limits

From the candidate checkout:

```console
cargo run --locked -j 2 --release --example optimizer_path_statistics
```

For the source baseline, export the named revision to a separate directory,
copy only the identical example there, and run from that baseline directory:

```console
cargo run --locked -j 2 --release --example optimizer_path_statistics -- --expect-kept-start
```

Preserve each binary before building the other source, then repeat the recorded
ABBA process order. The flag asserts that the old ordinary optimizer keeps its
start; it does not modify planning controls. No coefficient/default, production
API, collector, cache key, admission policy or persistence behavior changes.
Full current-head LDBC timings, deployed Mem behavior, total RSS, cold persistent
reads and cross-hardware calibration remain unverified. The candidate-proof
work in PR #1604 is separate; this change uses the existing typed selected-plan
qualification API and does not duplicate or depend on that PR.
