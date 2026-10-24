# Local layout measurements

Measured 2026-10-23 on Apple M5 Max, arm64, macOS 27.0.1 (26A434), pinned
Rust 1.97.1, repository-wide release profile, from this PR's implementation on
base `8dd9769f7`. The initial dependency build took 89 seconds with a reused
Cargo target directory; subsequent experiment-only release builds took about
3 seconds. This is neither a cold full build nor CI timing evidence.

Commands (fresh output paths required):

```console
cargo run --release --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- bench /tmp/bftree-small 16 2
cargo run --release --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- bench /tmp/bftree-history 512 24
```

Raw reports: [small](measurements/small.json), [history](measurements/history.json).
Each contains all three repetitions, sample counts, p50/p95/p99, totals and
memory observations. Tables below are **medians of the three per-run statistics**,
not percentiles pooled across runs. Layout order rotates in fresh processes;
100-byte values, synchronous complete journal batches, warm reads, in-memory
indexes and local filesystem journals are identical across layouts. Update
batch sizes are 16 and 64 respectively. COW is only a shared-coordinator
component reference, not full production HawDB performance.

## Retained history: 512 keys, 24 overwrite rounds

Latencies are microseconds. Commit values refer to a whole 64-key synchronized
transaction, including construction; they are not per-key times.

| Operation | Versioned p95 / p99 | Hybrid p95 / p99 | COW p95 / p99 |
| --- | ---: | ---: | ---: |
| Current point read | 2.500 / 8.125 | 0.250 / 0.334 | 0.291 / 0.375 |
| Old point read | 1.084 / 2.334 | 1.292 / 5.208 | 0.333 / 0.417 |
| Current full scan | 2091.333 / 2549.791 | 63.042 / 106.625 | 25.584 / 37.625 |
| Old full scan | 2072.833 / 2205.167 | 184.542 / 196.208 | 22.750 / 34.458 |
| Commit batch | 4298.375 / 4426.625 | 4306.625 / 4609.083 | 4424.667 / 8323.667 |

| Resource | Versioned | Hybrid | COW |
| --- | ---: | ---: | ---: |
| Resident / observed peak MiB | 14.375 | 14.484 | 9.328 |
| Reopen median ms | 5.376 | 10.093 | 16.499 |
| Journal bytes | 1466756 | 1466756 | 1466756 |

Hybrid current-scan p95 is about 33 times lower than the simple versioned
layout, with current point p95 about 10 times lower. Historical scans also
benefit from seeking one version per logical head. The tradeoff is real:
historical point p99 is about 2.2 times higher, commit p99 about 4.1% higher,
and replay about 1.9 times slower than versioned. Extra head/history index
writes are not extra journal bytes. The COW reference still scans faster and
uses less resident memory in this bounded working set. These results support
continuing an experiment, not replacing the existing engine backend.

## Small: 16 keys, two overwrite rounds

| Operation | Versioned p95 / p99 us | Hybrid p95 / p99 us | COW p95 / p99 us |
| --- | ---: | ---: | ---: |
| Current point read | 0.208 / 0.250 | 0.167 / 0.167 | 0.084 / 0.125 |
| Old point read | 0.209 / 0.291 | 0.292 / 0.333 | 0.084 / 0.125 |
| Current full scan | 7.541 / 11.458 | 5.167 / 6.167 | 1.666 / 3.208 |
| Old full scan | 8.334 / 11.000 | 5.625 / 6.375 | 0.792 / 1.833 |
| Commit batch | 3867.917 / 3867.917 | 3865.417 / 3865.417 | 3878.083 / 3878.083 |

Only two update batches exist per small run. Its commit p95/p99 are maxima,
not stable tail estimates. Tiny point timings are near the timer resolution.
All scan p99 figures use only 32 samples per process and are sample maxima.
The host was not exclusively reserved for measurement. Scheduling, filesystem
synchronization and cache promotion cause variation;
the raw reports preserve it. The benchmark does not measure mixed concurrent
read/write latency, CPU utilization, storage saturation, large values, GC,
checkpoint costs or out-of-memory index behavior.

## Dependency finding and configuration

An initial run using Bf-Tree 0.5.6's default record read cache panicked in
`mini_page_op.rs::load_base_page_from_buffer` from
`range_scan::promote_or_merge_mini_page`. Source inspection shows that
`try_merge_mini_page` can return `NoSplit` for a clean record-cache mini-page
before loading a base buffer, which the scan path then expects to exist.
No successful timing claim uses that failed configuration. The adapter uses
`read_record_cache(false)` (whole-page read caching) for **both** Bf-Tree
layouts. The alternating point-read/range-scan regression exercises this
configuration, including under the release profile. No dependency source or
repository compilation profile was patched. Native promotion probabilities
still follow Bf-Tree's defaults for the selected profile.

Allocation reductions apply to both comparable paths where applicable:
history seeks compare exact encoded key prefixes without decoding another
logical key; hybrid head reads decode directly from a stack buffer; scan
results reuse their already owned payload buffers. Native scans collect at
most 32 rows before releasing leaf guards, with explicit total-work bounds.


## Follow-up: reduce commit installation overhead

The follow-up removes temporary heap allocations for encoded Bf-Tree keys and
values. Hybrid installation borrows its previous value from existing immutable
retry receipts instead of reading/cache-promoting the Bf-Tree head. Committed
batches are indexed directly by epoch; operation IDs map to that epoch, and
latest-key entries retain the batch entry position. The latest-key ledger adds
one machine word per key, without duplicating value payloads. Updating an
existing ledger entry no longer clones and discards its key. Recovery and commit
share the same receipt-recording function. WAL bytes, synchronization policy,
resource limits and durable-before-install/publication ordering are unchanged.

Paired raw observations: [retained history](measurements/submit-history.json)
and [small](measurements/submit-small.json). Baseline: `ea27cf35e` plus identical
benchmark-only phase instrumentation. Both saved binaries use the same global
release profile; SHA-256 identifiers are in the reports. Five repetitions
reverse before/after order on odd repetitions and rotate all three layouts,
with fresh processes/journals. No compilation or other tests were started
concurrently with measurement; the host was not reserved. Tables are medians
of five per-process percentiles. The initial three-run observations above are
preserved rather than replaced.

To reproduce each observation with saved baseline and optimized executables:

```console
/path/to/baseline bench-one /tmp/before-hybrid-0.journal hybrid 512 24
/path/to/optimized bench-one /tmp/after-hybrid-0.journal hybrid 512 24
```

Repeat for `versioned`, `hybrid`, `cow`; reverse executable order in odd
repetitions and rotate layouts. Use `16 2` for small observations. Normal
`bench` also reports the new phases. `prepare_and_sync` covers validation,
encoding, journal append and synchronization up to Durable; `index_install`
covers Durable to Published. Full commit still includes write-set construction,
receipt bookkeeping and instrumentation. Phase percentiles cannot be added.

| Retained history operation | Before p50 / p95 / p99 us | After p50 / p95 / p99 us |
| --- | ---: | ---: |
| Hybrid full commit | 3965.416 / 4311.375 / 6929.125 | 3939.334 / 4325.667 / 5187.584 |
| Hybrid prepare and sync | 3817.583 / 4182.792 / 6811.875 | 3780.500 / 4181.750 / 4983.292 |
| Hybrid index installation | 115.667 / 269.458 / 528.542 | 101.666 / 248.750 / 375.875 |
| Hybrid old point read | 0.750 / 2.750 / 9.292 | 0.708 / 3.167 / 9.791 |
| Hybrid current point read | 0.292 / 0.417 / 0.708 | 0.250 / 0.458 / 0.791 |
| Hybrid current scan | 85.959 / 114.958 / 159.042 | 81.250 / 104.458 / 150.125 |
| Hybrid old scan | 246.292 / 299.500 / 345.334 | 220.958 / 287.458 / 296.084 |
| Versioned full commit control | 3938.708 / 4315.292 / 4931.167 | 3926.250 / 4314.708 / 5130.375 |
| COW full commit control | 3987.333 / 4298.916 / 7949.000 | 3962.834 / 4324.500 / 6158.500 |

The targeted index-installation p99 is about 29% lower (529 -> 376 us), with
p95 about 7.7% lower and p50 about 12% lower. Full commit p99 is 6.929 ->
5.188 ms in this observation, while p95 is approximately unchanged. The
optimized Hybrid versus optimized versioned p99 gap is about 1.1% here. These
are observations, **not proof of a stable end-to-end improvement or of meeting
a 1.1% regression ceiling**: synchronization dominates and the original 4%
difference did not reproduce consistently. An earlier intermediate-code run
also had noisier full-commit tails despite lower installation work. Do not
select favorable repetitions or weaken durability to manufacture a result.

Old point-read p50 improves slightly, but its retained-history p95/p99 are
higher by about 15%/5%; no long-history tail improvement is established.
Hybrid current/old scan p95 remain lower in this observation. The COW component
continues to scan faster; this is not an end-to-end HawDB speedup claim.

Hybrid resident/observed peak MiB: 14.422 -> 14.484; median reopen ms: 12.928 -> 10.975.

Versioned resident/observed peak MiB: 14.312 -> 14.406; median reopen ms: 5.284 -> 5.168.

Cow resident/observed peak MiB: 9.281 -> 9.297; median reopen ms: 17.392 -> 14.540.

Every retained-history journal remains 1,466,756 bytes.

| Small Hybrid operation | Before p95 / p99 us | After p95 / p99 us |
| --- | ---: | ---: |
| Full commit (two samples/run) | 4045.667 / 4045.667 | 3875.208 / 3875.208 |
| Index installation | 30.125 / 30.125 | 23.708 / 23.708 |
| Old point read | 0.292 / 0.333 | 0.250 / 0.292 |

Small old point p99 improves about 12%; index-installation maximum improves
about 21%. Tiny samples and sub-microsecond timer granularity remain limits.
Concurrent group commit and storage/platform durability-policy changes remain
outside this follow-up.

## Scan-buffer and version-run continuation (2026-10-24)

The [source-bound paired report](measurements/scan-20261024/README.md) records
the next continuation on main 720d98f86 and runtime commit 4c8e93e4c. It uses
identical benchmark/journal sources on an Apple M3 Max, separate from the M5
observations above. Both complete five-pair cohorts, all three layouts and all
small/retained/larger workloads are retained, including adverse tails and
compiler observations. Versioned and Hybrid scans improve in the qualified
cases; synchronization variance prevents a stable commit-tail claim, and COW
still scans faster. The full local 98-target aggregate and final dev/release
adapter tests passed; full-tree historical license debt remains disclosed.

## Manual word/NEON comparison (2026-10-24)

The [three-way Bf-Tree comparison report](measurements/simd-20261024/README.md)
retains 225 fresh processes across short, common-prefix and spread keys, all
three layouts, old/current reads, writes, replay and memory. It includes a
pinned RocksDB source audit, source/binary hashes, all samples and controls.
Neither explicit NEON nor word comparison is selected by default: this noisy
shared-host cohort does not establish a consistent advantage across cases.
The default adapter still uses the unmodified 0.5.6 registry dependency.
