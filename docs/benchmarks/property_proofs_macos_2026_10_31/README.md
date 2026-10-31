# Bounded graph proofs on macOS ARM64

## Experiment

Compare runtime `bad258d8c117b6ae76ffca7880d37ee90a75a2e8` with main baseline
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
2026-10-31T04:35:29.969780+00:00 through 2026-10-31T04:35:57.814111+00:00. It covers 32 cells,
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
| distinct | 128 | warm | 0.1144 | 0.0841 | -26.5% | 82,994 → 41,472 |
| distinct | 128 | cold | 0.1166 | 0.0869 | -25.5% | 82,994 → 41,472 |
| distinct | 10,000 | warm | 6.5307 | 4.2603 | -34.8% | 5,647,410 → 2,284,064 |
| distinct | 10,000 | cold | 6.4995 | 4.1888 | -35.6% | 5,647,410 → 2,284,064 |
| constant | 128 | warm | 0.1523 | 0.1516 | -0.4% | 75,264 → 75,264 |
| constant | 128 | cold | 0.1525 | 0.1330 | -12.8% | 75,264 → 75,264 |
| constant | 10,000 | warm | 8.4639 | 8.6613 | +2.3% | 4,376,016 → 4,376,016 |
| constant | 10,000 | cold | 8.6037 | 6.1775 | -28.2% | 4,376,016 → 4,504,229 |
| constant-source | 128 | warm | 0.1785 | 0.1676 | -6.1% | 17,212,150 → 17,212,150 |
| constant-source | 128 | cold | 0.1729 | 0.1711 | -1.1% | 17,212,150 → 17,212,150 |
| constant-source | 10,000 | warm | 10.5166 | 10.3845 | -1.3% | 27,602,422 → 27,602,422 |
| constant-source | 10,000 | cold | 10.5003 | 10.3374 | -1.6% | 27,602,422 → 27,602,422 |
| group | 128 | warm | 0.2158 | 0.2133 | -1.2% | 458,496 → 458,496 |
| group | 128 | cold | 0.2199 | 0.2167 | -1.5% | 458,496 → 458,496 |
| group | 10,000 | warm | 13.8419 | 12.8825 | -6.9% | 4,397,288 → 4,347,376 |
| group | 10,000 | cold | 13.6543 | 12.9138 | -5.4% | 4,397,288 → 4,347,376 |
| nullable | 128 | warm | 0.0752 | 0.0791 | +5.2% | 47,824 → 47,824 |
| nullable | 128 | cold | 0.0771 | 0.0813 | +5.4% | 47,824 → 47,824 |
| nullable | 10,000 | warm | 3.5879 | 3.6093 | +0.6% | 2,901,328 → 2,901,328 |
| nullable | 10,000 | cold | 3.6237 | 3.6088 | -0.4% | 2,901,328 → 2,901,328 |
| point | 128 | warm | 0.0360 | 0.0354 | -1.5% | 790 → 790 |
| point | 128 | cold | 0.0354 | 0.0347 | -1.9% | 790 → 790 |
| point | 10,000 | warm | 0.1450 | 0.1365 | -5.9% | 790 → 790 |
| point | 10,000 | cold | 0.1393 | 0.1383 | -0.7% | 790 → 790 |
| computed | 128 | warm | 0.1566 | 0.1533 | -2.1% | 82,994 → 82,994 |
| computed | 128 | cold | 0.1555 | 0.1612 | +3.6% | 82,994 → 82,994 |
| computed | 10,000 | warm | 9.3334 | 9.0945 | -2.6% | 5,647,879 → 5,647,879 |
| computed | 10,000 | cold | 9.1492 | 9.1551 | +0.1% | 5,647,879 → 5,647,879 |
| schema | 128 | warm | 0.2182 | 0.1946 | -10.8% | 82,994 → 41,472 |
| schema | 128 | cold | 0.1812 | 0.1498 | -17.4% | 82,994 → 41,472 |
| schema | 10,000 | warm | 6.5987 | 4.3820 | -33.6% | 5,647,410 → 2,284,064 |
| schema | 10,000 | cold | 6.8640 | 4.2505 | -38.1% | 5,647,410 → 2,284,064 |

## Conclusions

- Proved nonnull UNIQUE eliminates DISTINCT. End-to-end time falls 25.5–35.6%
  across the small/bulk and warm/replanned cells. At 10,000 rows the measured
  query-ledger peak falls from 5,647,410 to 2,284,064 bytes; the 5,500,050-byte
  DISTINCT buffer disappears while Sort remains. This is admission accounting,
  not a measured Rust heap reduction.
- Singleton grouping removes the second aggregate. At 10,000 rows time falls
  5.4–6.9%, with query peaks 4,397,288 → 4,347,376 bytes. Small groups change only
  -1.2 to -1.5%; no substantial small-group gain is established.
- Explicit bound replanning can remove the projected constant Sort: time falls
  12.8% at 128 rows and 28.2% at 10,000 rows. Peak admission does not universally
  improve: the bulk cell changes 4,376,016 → 4,504,229 bytes as pipeline ownership
  changes. A faster plan is not automatically a lower peak-memory plan.
- Neutral/adverse controls remain visible. Nullable 128-row DISTINCT retains
  its operator and is 5.2–5.4% slower (about 4 microseconds); its bulk cells are
  within 0.6%. Computed-expression controls range from -2.6% to +3.6% with the
  same plans. Unindexed equality controls retain the same access path; their
  negative timing differences are not attributed to a new lookup optimization.
- Schema metadata has a cost. The 64-table/512-property control still benefits
  from DISTINCT elimination, but reported warm preparation at 128 rows grows
  115.5 → 123.7 microseconds. Other replanned cells also add preparation work:
  bulk grouping is 29.7 → 34.6 microseconds and bulk source-scope ordering is
  23.2 → 33.7 microseconds. These absolute planning costs and small-query
  regressions limit any general speedup claim. No spill occurred in these cells.

The `constant` query projects the parameter-filtered space alias. In `warm`
mode the parameter template marker does not gain a literal comparator-domain
proof, so both revisions keep Sort: the bulk candidate is +2.3% slower and the
small cell is -0.4%. In forced bound `cold` planning, actual parameter values are
available and the named ordering rule removes Sort. `constant-source` orders by
an unprojected source property and retains its scope fallback in both modes.
These differences are recorded in the selected plans; warm-template constant
propagation remains a future optimization. Equivalence rules and unsafe-evaluator
fences have separate optimizer/API qualification evidence.

Only this independent PR's resident ARM fixture is measured. Results do not
establish combined gains after the other optimizer PRs, x86, persistent reads,
allocator/RSS reductions, remote CI speed or production Mem latency. Width and
row-goal costs remain those of the baseline; the experiment adds no quota,
index or profiling exception. Procedure, aggregates/provenance and conclusions
are committed; process output and build/test logs stay outside Git.
