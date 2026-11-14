# Width and blocking-cost experiment — macOS ARM64

## Procedure and sources

Baseline `37bfb4bca8c1296f41eb0d521f2602bd7853459e` contains only the identical manual
benchmark/Cargo registration overlay. Candidate runtime
`f39317f710afc044448f6bba984c876244dc57ed` contains the width/comparison/memory model and
the selected-window LIMIT transfer correction. This report is a later documentation
commit. See [the cost contract](../../QUERY_WIDTH_COSTS.md) for source fallbacks,
logical units, metadata bounds and unsupported incremental-sort behavior.

Host: Apple M3 Max, 128 GiB,
macOS ARM64, pinned Rust 1.97.1. Both binaries use the repository release profile
with default features plus `qualification`; no assertion, timeout, budget or
compiler profile is relaxed for qualification. Raw compiler, process and test
logs remain outside the repository. Binary and harness hashes are in
`provenance.json`.

```console
cargo build --locked --release --features qualification --bench width_cost_planning
<executable> text 10000 2000 4096 random 20 memory 5
```

Arguments are payload kind, input rows, output limit, payload bytes, arrival order,
blocking MiB, residency and warm repeats. The harness creates typed ID/rank/kind/body
properties, loads 256-row batches outside timing, and sorts by rank plus identity.
A deterministic xorshift shuffle supplies random arrival; ascending/descending
and native composite ordering are separate controls. Native ordering adds a
validated unique identity constraint and `(kind,rank,id)` index. Vector(1024)
has a 4,100-byte owned encoding for 4,096 component bytes. `varchar` denotes
Cypher STRING, using existing scalar histogram widths. TEXT uses the explicitly
approximate 4-KiB type proxy, including the adverse 128-byte and 8-KiB cells.
Persistent cells contain 2,000 rows, retain 400 and use 2 MiB of blocking memory;
setup checkpoints and reopens in the named mode. A reopened handle is not a
cold device cache. All cells retain the same 256-MiB query budget and unchanged
runtime hard admission and spill quotas.

Three process rounds rotate the cell order and alternate binary order. Each
fresh process builds its own fixture/reader, excludes its first call, then times
five warm queries including complete output collection. Assertions and output
drop are outside timing. Every call checks every returned rank, identity and
payload against the complete sorted reference, and every warm call requires
one physical-template hit and zero misses. Each call must fully stream and
release its query admission ledger. The accepted matrix contains
114 processes, 684 calls and
1,412,856 exact output rows.

Measurements began `2026-10-31T02:13:14.200248+00:00` and ended `2026-10-31T02:14:41.013459+00:00`.
They followed a continuous 20-second no-build gate. Every process boundary
observed zero Cargo/rustc/cargo-bazel processes; accepted boundaries and load
observations are retained. Normal OS activity and cached filesystem behavior
remain uncontrolled. Timings are synthetic source-bound ARM measurements,
not an x86, device-cold or production Mem claim.

## Results

Latency is the median of three process warm medians. Spill bytes are actual
executor counters per call, separate from model `spill_work`. Query peaks are
logical admission-ledger bytes, **not allocated Rust heap or process RSS**.
Complete per-process medians, first/setup/explain timing, plans, estimates and
blocking reports are in `results.json`.

| Cell | Baseline ms | Candidate ms | Change | Root plan | Spill bytes | Query peak bytes |
| --- | ---: | ---: | ---: | --- | ---: | ---: |
| narrow-small | 0.1647 | 0.1671 | +1.5% | TopN → Sort + Limit | 0 → 0 | 37,477 → 72,064 |
| narrow-bulk | 10.7256 | 10.6885 | -0.3% | Sort + Limit → Sort + Limit | 0 → 0 | 20,618,816 → 4,064,320 |
| text-pressure | 121.4397 | 13.5355 | -88.9% | Sort + Limit → TopN | 81,977,120 → 0 | 26,173,360 → 10,524,096 |
| text-all-fit | 14.4765 | 14.7630 | +2.0% | Sort + Limit → Sort + Limit | 0 → 0 | 61,511,104 → 47,051,712 |
| text-small-payload | 11.0840 | 11.0224 | -0.6% | Sort + Limit → TopN | 0 → 0 | 21,831,104 → 1,621,344 |
| text-small-payload-ascending | 10.3890 | 9.8217 | -5.5% | Sort + Limit → TopN | 0 → 0 | 21,831,104 → 1,621,344 |
| text-8k-k-near-budget | 188.4048 | 189.5764 | +0.6% | Sort + Limit → TopN | 164,791,040 → 164,791,040 | 52,064,096 → 36,701,120 |
| text-ascending | 121.9342 | 12.1282 | -90.1% | Sort + Limit → TopN | 81,977,120 → 0 | 26,173,360 → 10,524,096 |
| text-descending | 121.9578 | 13.4562 | -89.0% | Sort + Limit → TopN | 81,977,120 → 0 | 26,173,360 → 10,524,096 |
| text-native-order | 3.2596 | 3.2407 | -0.6% | Ordered + Limit → Ordered + Limit | 0 → 0 | 26,173,360 → 11,279,376 |
| string-pressure | 123.2195 | 13.6548 | -88.9% | Sort + Limit → TopN | 81,977,120 → 0 | 26,173,360 → 10,524,096 |
| vector-pressure | 137.1569 | 13.7269 | -90.0% | Sort + Limit → TopN | 82,012,868 → 0 | 26,181,360 → 10,533,120 |
| text-8k-proxy-error | 184.0299 | 15.3264 | -91.7% | Sort + Limit → TopN | 164,791,040 → 0 | 34,365,360 → 19,764,672 |
| text-k-exceeds-budget | 223.0488 | 223.9789 | +0.4% | Sort + Limit → Sort + Limit | 167,288,560 → 167,288,560 | 26,173,360 → 11,279,376 |
| text-small-k | 11.7470 | 11.5168 | -2.0% | TopN → TopN | 0 → 0 | 1,330,136 → 1,330,136 |
| text-all-rows | 15.8505 | 15.6103 | -1.5% | Sort + Limit → Sort + Limit | 0 → 0 | 64,200,304 → 49,632,192 |
| canonical | 25.2511 | 4.0187 | -84.1% | Sort + Limit → TopN | 16,385,320 → 0 | 18,612,208 → 2,955,456 |
| materialized-reopen | 24.3153 | 2.9666 | -87.8% | Sort + Limit → TopN | 16,385,320 → 0 | 18,612,208 → 2,955,456 |
| auto-reopen | 24.6502 | 3.0929 | -87.5% | Sort + Limit → TopN | 16,385,320 → 0 | 18,612,208 → 2,955,456 |

Individual process medians expose variability:

| Cell | Baseline rounds, ms | Candidate rounds, ms |
| --- | --- | --- |
| narrow-small | 0.1773/0.1646/0.1647 | 0.1642/0.1690/0.1671 |
| narrow-bulk | 10.9684/10.7256/10.7024 | 10.6767/10.6885/10.7102 |
| text-pressure | 119.4947/121.4397/123.1988 | 13.5355/13.9002/13.1571 |
| text-all-fit | 14.4178/14.4765/14.9360 | 14.1858/14.7944/14.7630 |
| text-small-payload | 11.0425/11.0840/11.0969 | 11.4037/11.0224/10.9155 |
| text-small-payload-ascending | 10.3890/10.4095/10.3067 | 9.8217/9.8810/9.7775 |
| text-8k-k-near-budget | 184.1228/188.9129/188.4048 | 187.8764/192.8660/189.5764 |
| text-ascending | 121.9342/124.6755/121.0550 | 12.1282/12.7280/11.8952 |
| text-descending | 121.9578/124.0045/120.6367 | 13.8615/13.4562/13.2143 |
| text-native-order | 3.1091/3.2596/3.2789 | 3.2296/3.2407/3.3061 |
| string-pressure | 124.5617/123.2195/121.9929 | 13.4552/13.6548/13.8015 |
| vector-pressure | 137.9208/137.1569/136.4862 | 13.5257/14.3840/13.7269 |
| text-8k-proxy-error | 181.3993/185.1944/184.0299 | 15.6855/15.3264/14.9180 |
| text-k-exceeds-budget | 223.0488/227.1278/219.7975 | 223.9789/225.5633/223.5725 |
| text-small-k | 11.7470/11.9826/11.6263 | 11.4614/11.6790/11.5168 |
| text-all-rows | 15.8505/15.9900/15.8217 | 15.5170/15.6103/15.7709 |
| canonical | 26.0342/25.2511/24.9497 | 3.7966/4.0643/4.0187 |
| materialized-reopen | 24.7259/23.9630/24.3153 | 2.9666/2.9605/2.9785 |
| auto-reopen | 25.0475/24.6502/24.5453 | 3.0929/3.0746/3.1926 |

## Conclusions and limitations

The useful gain is conditional on avoiding real spill. At 10,000 input rows,
2,000 retained rows and a 20-MiB blocking budget, 4-KiB TEXT, STRING and vector
fixtures change Sort plus LIMIT to TopN. Actual spill falls from approximately
82 MB per call to zero, and median latency improves from 121–137 ms to
13.5–13.7 ms (about 9–10x). The 8-KiB TEXT case also fits its actual TopN heap,
removes about 165 MB of spill and improves from 184.03 to 15.33 ms. The smaller
persistent cells improve about 6.3–8.2x in these warm filesystem conditions.
These are workload-specific improvements, not a general database speedup.

Controls limit that claim. Narrow bulk is -0.3%, narrow small is +1.5%, and
4-KiB all-fit TEXT is +2.0%; these cells do not establish an execution gain.
Native ordered access stays sort-free and changes by -0.6%. The 128-byte TEXT
proxy overestimate does not produce spill in either implementation (-0.6%
random, -5.5% ascending), but this does not validate the proxy as a real width.
When 4-KiB K=2,000 itself exceeds the 1-MiB budget, both choose Sort, spill the
same 167,288,560 bytes and differ by +0.4%.

The adverse 8-KiB/K=4,000 cell exposes the remaining uncertainty: its 4-KiB TEXT
proxy predicts that TopN fits 20 MiB, but its real heap does not. Candidate TopN
and baseline Sort each spill 164,791,040 bytes; candidate median is +0.6%.
Runtime admission and spill remain correct, but the estimated no-spill choice
is inaccurate. Data-dependent TEXT/aggregate proxies need better source width
information before making stronger retention or broad latency claims. Warm
preparation medians range roughly 11.6–35.0 microseconds here; changes to plan
shape and estimate-refresh work are both included in the timed query.

The LIMIT transfer correction removes an unnecessary full 16-MiB reservation
from selected windows; lower ledger peaks in otherwise identical Sort/native
plans do not demonstrate a corresponding heap allocation reduction. This narrow
fix is also independently carried by the joint-NDV PR. No query output limit,
resource quota or failure behavior is weakened.

TEXT widths remain proxies: favorable 4-KiB results must be read alongside both
misestimated-payload controls and the case where K itself exceeds blocking
memory. Adaptive sort comparisons and heap replacement frequency depend on
arrival order. A smaller estimated cost is not itself performance evidence.
