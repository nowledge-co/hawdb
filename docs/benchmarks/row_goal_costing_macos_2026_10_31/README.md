# Graph row-goal costing on Apple Silicon

The final change improves consumed-work diagnostics and conservatively limits
which ordered alternatives can receive a prefix discount. This matrix does
**not establish a query-latency improvement**. It preserves selective seeks on
uniform and tail-clustered cohorts; the prefix-only query was already ordered
on the baseline. All 24 processes, 1,344 calls and 4,047,360 returned rows passed
complete rank/ID parity and selected-plan assertions.

## Sources and reproduction

- Baseline: `0f611b6decbebd8354f2a30889e03c48287d18d2`, with only the identical
  benchmark source and Cargo bench registration overlaid.
- Runtime: `c052829ea9771364dcefb176c9815f6fd487cfb8` (PR #1380).
- Harness: [row_goal_planning.rs](../../../benches/row_goal_planning.rs), SHA-256
  `9048873900bee9e120f245f883b844dcd06de7ede2f928aea97dabe2e2267c62`.
- Apple M3 Max, 128 GiB, native arm64, Rust 1.97.1/LLVM 22.1.6; unchanged
  repository release profile, default features plus `qualification` in both
  versions. Parse/preparation timers are enabled, with no allocation observer.
  These are instrumented calls, not ordinary-build or whole-Mem-endpoint timings.

Build each version in its own worktree from within the repository so that the
shared Cargo configuration applies:

```sh
cargo build --locked --release --bench row_goal_planning --features qualification
# Run the compiler-artifact executable emitted for the bench, once per process:
row_goal_planning 100000 0 31 uniform filtered
row_goal_planning 1000000 0 31 uniform filtered
row_goal_planning 1000000 0 31 skewed filtered
row_goal_planning 1000000 0 31 uniform prefix
```

The arguments are population, offset, repeats, distribution and query mode.
Repeat each cell in three fresh processes per revision, rotating cell order
and counterbalancing baseline/candidate order. Each process loads its fixture
outside timing, creates a selective `kind` index and `(space, rank, id)` ordered
index, and proves non-NULL typed rank/ID and unique ID through DDL. Fixture writes
use 1,024-row transactions; no durability configuration is weakened.

All rows have space `default`. Uniform kind is `id % 100`; clustered kind is
`id / (population / 100)`, selecting kind 99 at the tail. Queries return rank
and ID in ascending order, with LIMIT 20 or full-result sorting. Prefix mode
omits the kind filter and runs only its bounded query. The harness supports
other offsets, but this matrix measures offset zero; nested/offset/zero limits
are regression coverage, not additional benchmark cells.

Each shape explains before opening a read handle, then uses that handle for
one first-call warmup and 31 measured calls. The elapsed window includes the
embedded streaming API, consumer collection and reports. Setup, explain,
reader creation, complete result assertions, output drop and serialization are
outside that window. The first call therefore has warm metadata/explain state;
it is not a cold database or device measurement.

## Measurements

Values are medians of three process medians, in milliseconds. Ratios below one
are lower elapsed time. All per-process samples, preparation/parse observations,
plan text, decisions, source work, host load and RSS remain in [results.json](results.json).
Memory is query-ledger admission peak in bytes, excluding caller-owned collected
rows and the fixture heap. It is neither allocator traffic nor process RSS.
Candidate counts are `candidate_count_before_filter`, not disk I/O or all
cursor setup work.

| Shape | Baseline ms | Final ms | Final / base | Candidate visits base / final | Query admission bytes base / final |
| --- | ---: | ---: | ---: | --- | --- |
| 100,000, uniform, LIMIT 20 | 1.0945 | 1.0828 | 0.989 | [1000] / [1000] | [50450] / [50450] |
| 100,000, uniform, full sort | 1.1861 | 1.1712 | 0.987 | [1000] / [1000] | [480816] / [480816] |
| 1,000,000, skewed, LIMIT 20 | 9.7400 | 10.2668 | 1.054 | [10000] / [10000] | [50450] / [50450] |
| 1,000,000, skewed, full sort | 11.2527 | 11.5260 | 1.024 | [10000] / [10000] | [4469120] / [4469120] |
| 1,000,000, uniform, LIMIT 20 | 11.6635 | 11.2656 | 0.966 | [10000] / [10000] | [50450] / [50450] |
| 1,000,000, uniform, full sort | 13.2372 | 12.6522 | 0.956 | [10000] / [10000] | [4469120] / [4469120] |
| 1,000,000, prefix only, LIMIT 20 | 0.0577 | 0.0560 | 0.970 | [20] / [20] | [16780476] / [16780476] |

For the prefix-only million-row query, selected root cost changes from
4,000,022 (CPU 2,000,020; random work 1,000,001) to 102 (CPU 60; random work 21).
Both versions visit 20 candidates and return the same 20 rows. The final ordered
alternative itself costs 82; the enclosing LIMIT contributes another 20. These
are relative planner weights, not elapsed time, bytes or certified physical I/O.
The 16,780,476-byte admitted source peak remains unchanged: this change does
not remove retained source ownership or add width/buffering costs.

Filtered queries keep the kind seek plus TopN; full-result controls keep the
kind seek plus Sort. Their work and admitted peaks are unchanged. The bounded
clustered case is about 5.4% slower by the aggregate timing; the other bounded
ratios range about 0.966–0.989. Full-sort controls also vary despite unchanged
execution paths. With three process rounds on a shared host, these differences
do not establish a causal speedup or a general latency regression bound.

## Rejected experiment and boundaries

An earlier, uncommitted prototype selected ordered access using average residual
selectivity. It visited 2,000 candidates for 20 outputs on uniform million-row
data, but 990,020 on tail-clustered data, versus the selective seek's 10,000.
It also raised admitted query source memory from 50,450 to 16,780,476 bytes.
Those diagnostic pilots overlapped builds; their timings are excluded from
this qualified comparison and are not a shipped speedup claim.

The final selector allows a discount only when the residual is absent, true,
or exactly implied by equality-prefix constraints. Otherwise it prices the
ordered alternative with complete work. General filter/fanout costs still
express expected consumption; they never become hard executor source caps.
Sort, TopN, grouping, distinct, hash joins and opaque retrieval retain complete
blocking work. See [contract](../../QUERY_ROW_GOALS.md).

This does not fix every existing cardinality heuristic. In these filtered
fixtures, fused projection access reapplies a kind NDV, estimating 100 matches
for the million-row cohort that actually has 10,000. Both measured versions
share that limitation; follow-up #1364 addresses statistics/coverage propagation.
No statistical NDV is used as a logical non-NULL/uniqueness proof here.

## Qualification and limits

The accepted window is 2026-10-30 20:45:36–20:49:55 UTC. All builds/tests for
this task had completed before it began. No rustc process was observed at the
window endpoints; per-process host observations are retained. The host was
shared, not isolated. No x86, device-cold, persistent-I/O, allocator-heap or
whole-service performance claim is made.

On runtime source c052829e, pinned workspace all-target/all-feature strict
Clippy, fmt and changed-file pre-commit checks passed; WASM strict Clippy also
passed. Optimizer unit tests: 131 passed, six existing opt-in campaigns ignored;
facade default: 78 passed; supported minimal selection: 21 passed. Final required
Bazel Shiro/fuzz/optimizer/golden closure: 100 targets passed (28 executed,
72 cached); facade Bazel selection: 78 Rust tests passed. Final MVCC campaign:
37,504 cases in 30,000 ms passed. These are correctness evidence, not throughput
benchmarks or power-loss qualification.

Full-tree hooks retain 618 historical header failures and stop at the license
hook; changed staged files pass. An extra broad minimal-feature observability
selection has one fixture requiring disabled FullTextSearch; it is not claimed
as passing minimal coverage. Assertions and features were preserved.

Final incremental release builds took 3m06s baseline and 5m23s candidate while
verification was active, with existing caches. These are build receipts, not
clean-cache build comparisons. Final Bazel fuzz command wall time was 839.076s
(critical path 436.840s); facade command active wall time was 50.628s, with a
20.8s test action (14.80s Rust cases plus inventory/launcher). Queued caller
waiting is distinct from active command execution. Raw build/process/test logs
and BEP files remain outside Git; provenance contains source/binary hashes.
