# Joint NDV planning experiment — macOS ARM64

## Method and sources

Baseline `22e98c2547fe8b5a800992da886686ecefa1437e` has only the identical benchmark/Cargo registration
overlay. Candidate runtime `6e6d3066f29a1e72f4ecb958845a2b55a847072a` adds bounded joint statistics
and the selected-window LIMIT transfer fix. This report is a later documentation
commit. [The estimation contract](../../QUERY_JOINT_STATISTICS.md) describes the
supported domains and fallbacks.

Host: Apple M3 Max, 128 GiB, macOS ARM64, pinned
Rust 1.97.1. Both use the repository release profile (`opt-level=3`, thin LTO,
one codegen unit), default features plus `qualification`. No allocator observer,
target-local compiler override or modified assertion/timeout is used. Qualification
provides query preparation timing counters. Test checks use the separate unchanged
repository checked profile. Candidate release compilation took 15m48s while other
verification compiled; this is build provenance, not a comparable compile-time result.

Run the same harness in each checkout:

```console
cargo build --locked --release --features qualification --bench joint_ndv_planning
<benchmark-executable> <scenario> <rows> 11
```

Scenarios are the seven cells below. Fixture construction uses 1,024-row batches
outside timing, followed by composite `(a,b)` indexes except `group-noindex`.
Grouping uses `a=b=id%keys` for correlated/no-index fixtures; independent keys
are `a=id%keys,b=(id/keys)%keys`. There are 20 keys for 400 rows and 100 keys
for 100,000 rows. Join labels each contain `a=id%100,b=id/100`: the combined
pair is unique, while the executor's single hash key still produces duplicates.
The statements and explicit output row/payload bounds are in the harness.

Three process rounds rotate cell order and alternate baseline/candidate order.
Each process creates one fresh database and retained reader, runs one excluded
first query and 11 measured warm queries. Every call checks all output values,
duplicates and order against the complete fixture reference; join output is all
identity pairs. Every warm query must record one physical-template hit and zero
misses. Every call must be fully streamed with a zero completion memory ledger.
All 504 calls and 262,080 output rows passed.
The timed query includes collecting its full output and excludes assertions,
output drop and printing. Setup, standalone explain and first-call times remain
separate in the compact results; none is silently counted as warm execution.

Measurements started `2026-10-30T22:53:37.035226+00:00` and ended `2026-10-30T22:54:02.147357+00:00`.
Two initial partial runs were excluded when other Cargo processes appeared at
process boundaries; none of their timings enter this table. After a continuous
20-second idle gate, the accepted rerun uses
the same source, binaries, cases and order. Our build/test jobs had retired; each
accepted process boundary observed zero `rustc` and
`cargo` processes. Normal OS activity is uncontrolled. Binary/source hashes,
host/load provenance and aggregate process medians are retained; raw compiler,
process and test logs remain outside the repository. This resident synthetic
experiment does not establish persistent, disk-cold, x86 or production Mem gains.

## Results

This run does **not establish an attributable query-latency improvement**.
Correlated grouping is 2.2–2.6% faster at the aggregate medians, but no-index
controls also improve by 1.3–3.0%; unchanged independent grouping improves by
3.4%. The 200-row join is 2.8% slower, while the larger join changes by -0.7%.
The estimate/plan corrections are demonstrated; the timing differences are not
evidence of a general speedup. Warm planning remains about 15–34 µs here.

Latency is the median of three process warm medians. Lower is better. Planning
is the warm preparation-counter median, in microseconds; query peak is the
admission ledger in bytes, not measured allocated heap or process RSS.

| Scenario / input rows per label | Baseline ms | Candidate ms | Change | Planning µs | Query peak bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| group-correlated / 400 | 0.2993 | 0.2928 | -2.2% | 23.7 → 23.6 | 181,023 → 181,023 |
| group-correlated / 100,000 | 55.9989 | 54.5600 | -2.6% | 31.8 → 30.5 | 1,244,703 → 1,244,703 |
| group-independent / 100,000 | 60.5082 | 58.4578 | -3.4% | 33.6 → 31.8 | 4,495,903 → 4,495,903 |
| group-noindex / 400 | 0.2978 | 0.2938 | -1.3% | 22.5 → 23.0 | 181,023 → 181,023 |
| group-noindex / 100,000 | 55.6365 | 53.9712 | -3.0% | 29.2 → 27.0 | 1,244,703 → 1,244,703 |
| join / 200 | 0.5825 | 0.5988 | +2.8% | 15.8 → 15.2 | 18,047,608 → 18,047,608 |
| join / 3,000 | 37.6761 | 37.4240 | -0.7% | 16.9 → 17.0 | 26,055,187 → 26,055,187 |

The three individual process warm medians (ms) expose repeat variability:

| Scenario / rows | Baseline rounds | Candidate rounds |
| --- | --- | --- |
| group-correlated / 400 | 0.3476/0.2957/0.2993 | 0.2950/0.2922/0.2928 |
| group-correlated / 100,000 | 55.9989/55.3883/56.4810 | 54.5600/53.8873/58.5983 |
| group-independent / 100,000 | 62.3793/60.5082/59.8859 | 64.3804/58.1890/58.4578 |
| group-noindex / 400 | 0.3000/0.2978/0.2968 | 0.2935/0.2938/0.2973 |
| group-noindex / 100,000 | 55.5872/55.6365/57.8561 | 53.9712/54.7743/53.5845 |
| join / 200 | 0.5825/0.5842/0.5806 | 0.5939/0.6058/0.5988 |
| join / 3,000 | 37.2613/37.7109/37.6761 | 37.4240/37.5682/37.3864 |

`results.json` records per-process medians, first/explain/setup observations,
selected estimates/plans, statistics provenance and tracked blocking peaks.
Process maximum RSS covers setup and the whole process; it does not isolate
the query. No actual Rust-heap allocation improvement is inferred from it.

## Plan behavior and memory regression

Correlated grouping has 20/100 groups. Joint statistics correct the previous
independent-key estimates (400/10,000), selecting Sort plus LIMIT in place of
TopN. Independent 100,000-row data retains 10,000 estimated groups and TopN;
no-index controls receive no joint-catalog provenance. Joins now report the
single-key hash intermediate (400 at 200 inputs; 90,000 at 3,000), followed by
the two-key filter estimate (200/3,000). Execution still visits that complete
single-key intermediate; improved final estimates must not erase its CPU cost.

An excluded early pilot revealed a real integration regression: changing the
400-row correlated group plan to Sort plus LIMIT raised query admission from
181,023 to 16,781,516 bytes because LIMIT reserved the full 16-MiB batch ceiling.
The final transfer fix restores 181,023 bytes, equal to baseline. A query that
failed before the fix now returns all 20 groups under an unchanged 2-MiB query
budget. Kernel guards also check selected payload and one-byte-deficit refusal.
This corrects admission; it is not evidence of a 16-MiB physical allocation.

## Qualification and interpretation

Final runtime source passed pinned fmt, whole-workspace/all-target/all-feature
strict Clippy, staged license/fmt/Clippy hooks and the documented minimal WASM
strict Clippy configuration. Default and supported minimal root statistics/
aggregate/joint-statistics selections each passed 46 tests. Bazel optimizer
ownership passed 136 tests (six existing ignored), executor ownership passed
544 (16 existing ignored), and planner goldens passed. The full required local
Shiro/fuzz closure passed 101 tests (90 executed, 11 cached); a separate Bazel
facade selection passed all 46 tests.
The MVCC local campaign passed 8,243 cases in 30,031 ms. Full-tree license audit
still reports the same 618 historical missing/conflicting-header failures;
changed-file hooks pass. No lint, resource limit, assertion or feature is weakened.

NDV estimates improve planning inputs and observable physical choices; they do
not encode skew frequencies or prove semantic constraints. Read the changes
alongside no-index/independent controls and process variability. A latency claim
must be specific to the measured cell and not generalized to filters, joins or
all Mem requests. NULL, missing/sampled/stale data, pinned snapshots, alias
shadowing, duplicate joins, optional unmatched rows and deterministic tied windows
also have exact correctness regressions outside this timing matrix.
