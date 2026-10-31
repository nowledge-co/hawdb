# Bounded graph proofs on macOS ARM64

## Experiment

Compare runtime `c5e02f73848c665879d5f01683016ff4621a57a6` with main baseline
`d4ed1267ea6c6b002c6cb2c897b045d11f6a40d6`. The baseline receives only the
identical `property_proofs` harness and Cargo bench registration. Both builds use
pinned Rust 1.97.1, the global release profile and default + `qualification`
features on Apple M3 Max with 128 GiB RAM.
Binary/harness hashes, exact compiler details, process order and aggregate boundary
checks are in [provenance.json](provenance.json).

Run `cargo bench --locked --features qualification --bench property_proofs -- distinct 10000 warm 5`
for one cell. Use each workload name in the table, row count 128 or 10000,
`warm` or `cold`, and five repetitions. Resident in-memory fixtures declare an
INT nonnull UNIQUE identifier, a STRING space and a nullable UNIQUE property;
the nullable property is NULL for half the rows. A coprime 7,919 permutation
provides deterministic arrival order. Equality filtering is unindexed: UNIQUE
metadata alone does not make this fixture an indexed point-lookup experiment.
`schema` adds 64 unrelated tables with eight properties each.

Each process loads a fresh fixture, obtains its selected plan, excludes its
first execution, then times five complete streamed reads. `warm` requires a
physical-template hit with no miss for every measured call. `cold` explicitly
forces Memo replanning through the existing query directive; data stay resident
and the parameter binding template may still be reused. This is not device-cold
or fresh-filesystem I/O. Setup and first explain/execution are separately
recorded and excluded from steady-state medians.

Three rounds rotate cell order and alternate baseline/candidate order. An
initial 20-second gate and every process boundary require no active Bazel
client, Cargo/compiler or HawDB/Shiro test process. The accepted run spans
2026-10-31T05:21:20.964111+00:00 through 2026-10-31T05:21:49.513672+00:00. It covers 32 cells,
192 processes, 1,152 complete executions and 4,740,192 checked
output rows. Every query checks the full reference outside timing and releases
its streaming admission ledger. Ordered cells check exact order; constant-key
peers and the unordered nullable DISTINCT compare complete bags, preserving
NULL and duplicate semantics. No sampling or set-only comparison is used.

## Results

Times are medians of three process medians, in milliseconds; positive change
means slower. Memory is peak query admission accounting, not heap or RSS.
[results.json](results.json) retains per-round aggregates, raw reported plans,
planning medians and measured blocking input/peak/spill counters.

| Workload | Rows | Planning | Baseline ms | Candidate ms | Change | Query peak bytes |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| distinct | 128 | warm | 0.1176 | 0.0867 | -26.3% | 82,994 → 41,472 |
| distinct | 128 | cold | 0.1203 | 0.0885 | -26.4% | 82,994 → 41,472 |
| distinct | 10,000 | warm | 6.9055 | 4.4322 | -35.8% | 5,647,410 → 2,284,064 |
| distinct | 10,000 | cold | 7.5447 | 4.3868 | -41.9% | 5,647,410 → 2,284,064 |
| constant | 128 | warm | 0.1610 | 0.1667 | +3.5% | 75,264 → 75,264 |
| constant | 128 | cold | 0.1637 | 0.1313 | -19.8% | 75,264 → 75,264 |
| constant | 10,000 | warm | 9.2925 | 9.2509 | -0.4% | 4,376,016 → 4,376,016 |
| constant | 10,000 | cold | 9.0216 | 6.6696 | -26.1% | 4,376,016 → 4,504,229 |
| constant-source | 128 | warm | 0.1795 | 0.1827 | +1.8% | 17,212,150 → 17,212,150 |
| constant-source | 128 | cold | 0.1822 | 0.1819 | -0.1% | 17,212,150 → 17,212,150 |
| constant-source | 10,000 | warm | 11.5124 | 11.2652 | -2.1% | 27,602,422 → 27,602,422 |
| constant-source | 10,000 | cold | 11.3804 | 11.7229 | +3.0% | 27,602,422 → 27,602,422 |
| group | 128 | warm | 0.2258 | 0.2214 | -2.0% | 458,496 → 458,496 |
| group | 128 | cold | 0.2228 | 0.2230 | +0.1% | 458,496 → 458,496 |
| group | 10,000 | warm | 14.1299 | 14.4615 | +2.3% | 4,397,288 → 4,347,376 |
| group | 10,000 | cold | 14.3932 | 14.4809 | +0.6% | 4,397,288 → 4,347,376 |
| nullable | 128 | warm | 0.0819 | 0.0828 | +1.2% | 47,824 → 47,824 |
| nullable | 128 | cold | 0.0865 | 0.0871 | +0.8% | 47,824 → 47,824 |
| nullable | 10,000 | warm | 3.6144 | 3.6149 | +0.0% | 2,901,328 → 2,901,328 |
| nullable | 10,000 | cold | 3.6548 | 3.6516 | -0.1% | 2,901,328 → 2,901,328 |
| point | 128 | warm | 0.0353 | 0.0342 | -3.2% | 790 → 790 |
| point | 128 | cold | 0.0396 | 0.0355 | -10.5% | 790 → 790 |
| point | 10,000 | warm | 0.1301 | 0.1309 | +0.6% | 790 → 790 |
| point | 10,000 | cold | 0.1351 | 0.1403 | +3.8% | 790 → 790 |
| computed | 128 | warm | 0.1647 | 0.1637 | -0.6% | 82,994 → 82,994 |
| computed | 128 | cold | 0.1677 | 0.1636 | -2.4% | 82,994 → 82,994 |
| computed | 10,000 | warm | 9.7367 | 9.2831 | -4.7% | 5,647,879 → 5,647,879 |
| computed | 10,000 | cold | 9.6466 | 9.7311 | +0.9% | 5,647,879 → 5,647,879 |
| schema | 128 | warm | 0.2233 | 0.1934 | -13.4% | 82,994 → 41,472 |
| schema | 128 | cold | 0.1755 | 0.1445 | -17.6% | 82,994 → 41,472 |
| schema | 10,000 | warm | 7.0745 | 4.4475 | -37.1% | 5,647,410 → 2,284,064 |
| schema | 10,000 | cold | 6.7505 | 4.3899 | -35.0% | 5,647,410 → 2,284,064 |

## Conclusions

- Proved nonnull UNIQUE eliminates DISTINCT. End-to-end time falls 26.3–41.9%
  across small/bulk and warm/replanned cells. At 10,000 rows the query-ledger
  peak changes 5,647,410 → 2,284,064
  bytes as the DISTINCT buffer disappears and Sort remains. This is admission
  accounting, not a measured Rust heap reduction.
- Singleton grouping removes the second aggregate, but its latency gain does
  not reproduce in the final-source run: the 10,000-row cells are 0.6–2.3%
  slower. Query admission peaks change 4,397,288 → 4,347,376 bytes; one aggregate
  state buffer disappears. Small-group changes are -2.0% to +0.1%. These data
  do not establish a grouping-latency improvement despite reduced logical work.
- Bound Memo replanning removes the projected constant Sort: time falls
  19.8–26.1%. Peak admission does not universally improve:
  its bulk cell changes 4,376,016 → 4,504,229 bytes.
  A faster plan is not automatically a lower peak-memory plan.
- Neutral/adverse controls remain visible. Nullable 128-row DISTINCT retains
  its operator and changes +0.8% to +1.2%; bulk changes are
  -0.1% to +0.0%. Computed-expression controls change
  -4.7% to +0.9% with the same plans. Unindexed equality controls keep
  their access path, so timing differences are not attributed to a new lookup
  optimization. There are only three rounds, not a statistical speed guarantee.
- Schema metadata and proof preparation cost work. Reported 128-row warm
  preparation with 64 unrelated tables is 117.0 → 121.7
  microseconds. Bulk replanned grouping is 40.1 → 49.9 microseconds;
  source-scope ordering is 47.3 → 51.8 microseconds.
  Absolute preparation costs and small-query controls limit general claims.
  No spill occurred in these cells.

The `constant` query projects the parameter-filtered space alias. In `warm`
mode its parameter marker does not gain a literal comparator-domain proof and
both revisions keep Sort: bulk changes -0.4% and small changes
+3.5%. In forced bound `cold` planning the actual values are available,
and the named ordering rule removes Sort. `constant-source` orders by an
unprojected source property and retains its scope fallback in both modes.
Selected plans record these differences. Warm-template constant propagation
remains a future optimization; equivalence/evaluator fences have independent
optimizer/API qualification evidence.

Only this independent PR's resident ARM fixture is measured. Results do not
establish combined gains after the other optimizer PRs, x86, persistent reads,
allocator/RSS reductions, remote CI speed or production Mem latency. Width and
row-goal costs remain those of the baseline; the experiment adds no quota,
index or profiling exception. Procedure, aggregates/provenance and conclusions
are committed; process output and build/test logs stay outside Git.
