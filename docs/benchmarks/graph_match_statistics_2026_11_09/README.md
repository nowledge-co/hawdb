# Graph MATCH statistics and cost qualification

This is a bounded optimizer experiment, not full LDBC or Mem acceptance.
The baseline is `eb596e50900d894eb8c9253d5f6b5c96d913f9d9` plus the identical
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
| Median of 30 warm measurements | 1.9479165 ms | 0.145875 ms |
| Output rows / estimated rows | 100 / 800,000 | 100 / 100 |
| Positive cardinality Q-error | 8,000 | 1 |
| Selected scalar logical cost | 826,936,852 | 1,854 |
| Profiled query-ledger peak | 16,871,328 B | 16,871,328 B |
| Start | A label scan | B equality seek |

The source comparison is **13.35x** faster on this fixture. Candidate process
medians are 2.165625 -> 0.151416 ms and 1.98125 -> 0.132625 ms when switching
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

After merging main, the refreshed baseline source build took 3m32s with
ordinary dependency cache reuse. The initial candidate build in an empty, independent
target directory took 4m45s; the bounded-coverage review fix rebuilt with
ordinary candidate-only cache reuse in 3m14s. These are local build observations, not CI or
query execution speedups. An attempted cross-source target-directory reuse
returned the baseline plan and was rejected by the start assertion; that failed
run is excluded from all measurements. Keep source variants in distinct Cargo
target directories when replaying.

## Replay and limits

From the candidate checkout:

```console
CARGO_TARGET_DIR=/tmp/hawdb-cost-candidate-target cargo run --locked -j 2 --release --example optimizer_path_statistics
```

For the source baseline, export the named revision to a separate directory,
copy only the identical example there, and run from that baseline directory:

```console
CARGO_TARGET_DIR=/tmp/hawdb-cost-baseline-target cargo run --locked -j 2 --release --example optimizer_path_statistics -- --expect-kept-start
```

Preserve each binary before building the other source, then repeat the recorded
ABBA process order. The flag asserts that the old ordinary optimizer keeps its
start; it does not modify planning controls. No coefficient/default, collector, cache key, admission policy or canonical
data/WAL transaction changes. The review fix adds a public optional coverage
field/builder and an optional derived-checkpoint metadata record; legacy
records without coverage remain unknown. The positive-count fixture keeps
the same plan. This refreshed timing is not evidence of an additional speedup
caused by the coverage fix.
Full current-head LDBC timings, deployed Mem behavior, total RSS, cold persistent
reads and cross-hardware calibration remain unverified. The candidate-proof
work in PR #1604 is separate; this change uses the existing typed selected-plan
qualification API and does not duplicate or depend on that PR.
