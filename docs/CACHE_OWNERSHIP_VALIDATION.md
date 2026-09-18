# Cache ownership qualification

The #196 implementation replaces unrestricted resident byte Arcs with tracked
immutable handles and fixed-shard pin counters. Hawkingrei approved the coordinated
ownership API on September 13, 2026. Query behavior, persisted formats, integrity
policy, the 16 shards, and the single payload budget are retained.

## Correctness

- The optimized candidate storage binary reports 618 ordinary tests passed,
  with 26 explicitly ignored local campaigns/benchmarks. The frozen baseline
  and padding-only control each report 608 ordinary tests passed.
- The Tokio owner reports 26 tests passed. Strict all-target Clippy passes for
  storage and Tokio, including the final padding implementation.
- The actual cache-source control reports 21 tests passed. Five isolated mutations
  fail their intended assertions: missing clone accounting, premature release,
  duplicate repin charge, copied rejected input, and eviction during final drop.
  No mutation is accepted through a timeout.
- Deterministic tests force final-drop/get and overlapping-final-drop schedules
  against production shard/payload methods. Seeded serial/concurrent campaigns
  retain the independent resident-set oracle and global budget.
- All five admission error classes return the original Vec allocation/capacity.
  Scan clones retain pins; consumer failure releases a whole wave. Cache destruction
  releases unrelated entries. Checkpoint materialization detaches cached output
  only under the existing byte budget.
- Padding checks match the original byte oracle across 111,744 corrupt buffers
  and 776 zero buffers, including unaligned starts and partial words. Both verified
  and uncached page decoding reject corrupted padding boundaries. Mutations of
  the actual helper that omit full words or the tail fail the oracle assertions.

The final immutable-source default gate passes all 85 selected targets across
the full run and one isolated retry. The full run passed 84 targets and hit the
unchanged 300-second limit in the graph-projection residency campaign. The
identical campaign then passed alone in 189.1 seconds, with all 971 source hashes
unchanged. The timeout and workload were retained; timing variability is not
attributed to a proven cause. The PR records both receipts.

## Reader workload

Baseline: `7fedeeee27472762959cb0e048922e351ed81f7e`, from a clean detached checkout.
The padding-only control is an archive of that commit with only the private padding
helper and its call site changed to match the candidate. Every other Rust source
and Cargo manifest in that control matches the baseline.

All three use Rust 1.97.1 / LLVM 22.1.6, the unchanged release profile, and identical
reader benchmark source. Host: AMD Ryzen 7 7735HS, 8 physical cores / 16 logical
CPUs, normal affinity 0-15. Six rounds rotate all three variants through each
execution position and reverse their order. The agent's builds and test campaigns
finish before measurement. No samples are discarded.

The benchmark warms 64 separate stable-identity readers with 16 KiB pages and
performs 50,000 public lookups per thread at 1/2/4/8 threads. It retains the single
hot identity control, requires zero physical reads and no repeated integrity
hashes, and excludes fixture creation/warmup from each elapsed-time measurement.
The table gives millions of reads/second: median [minimum, maximum].

| Workload | Threads | Baseline | Padding only | Candidate |
| --- | ---: | ---: | ---: | ---: |
| distributed | 1 | 0.264 [0.244, 0.277] | 0.738 [0.695, 0.786] | 0.994 [0.887, 1.034] |
| distributed | 2 | 0.512 [0.506, 0.538] | 1.435 [1.362, 1.527] | 1.830 [1.622, 1.929] |
| distributed | 4 | 1.018 [0.958, 1.055] | 2.768 [2.708, 2.961] | 3.614 [2.636, 3.754] |
| distributed | 8 | 1.351 [1.342, 1.401] | 3.856 [3.774, 4.145] | 5.416 [3.608, 6.281] |
| hot | 1 | 0.279 [0.244, 0.289] | 0.849 [0.828, 0.873] | 1.238 [1.232, 1.283] |
| hot | 2 | 0.536 [0.526, 0.556] | 1.538 [1.482, 1.561] | 2.279 [2.207, 2.345] |
| hot | 4 | 1.048 [1.027, 1.064] | 2.942 [2.892, 3.114] | 4.095 [3.974, 4.379] |
| hot | 8 | 1.398 [1.366, 1.575] | 3.970 [3.695, 4.508] | 4.529 [4.181, 4.641] |

The candidate's distributed-reader median scales **3.63x at four threads** and
**5.45x at eight threads** relative to its single-thread median. All candidate
reader medians exceed both controls. This supports four-thread scaling on this
host/workload; it is not a bound for every CPU, key distribution, writer workload,
or cold-I/O path. The ranges include the slower candidate second round.

Total process CPU time for the full 1.5-million-lookup benchmark, including fixture
setup/teardown, is separate from the per-case elapsed times above:

| Variant | Median CPU seconds | Range |
| --- | ---: | ---: |
| baseline | 6.220 | 6.124–6.521 |
| padding-only | 2.264 | 2.160–2.330 |
| candidate | 1.810 | 1.747–2.073 |

## Snapshot and allocation cost

An identical standalone harness compiles each variant's actual cache sources,
holds half the entries pinned, and takes 2,000 snapshots at each entry count.
Values are nanoseconds per snapshot: median [minimum, maximum] over six alternating
rounds. Both snapshot and 2,000 get/clone/drop cycles report zero allocator calls
at every tested nonempty entry count. These counts cover the cache lifecycle,
not decoded query values or reader fixture creation.

| Entries | Baseline ns | Candidate ns |
| ---: | ---: | ---: |
| 0 | 94.9 [91.1, 187.9] | 102.2 [78.6, 157.4] |
| 1 | 95.0 [91.8, 183.2] | 86.2 [76.8, 182.6] |
| 64 | 182.9 [177.9, 191.9] | 89.7 [76.3, 177.1] |
| 4,096 | 13,569.5 [13,301.6, 15,423.7] | 78.5 [76.2, 91.4] |
| 65,536 | 1,165,472.5 [1,117,249.3, 1,315,560.3] | 81.3 [76.5, 93.7] |

The implementation reads a fixed set of 16 pin totals and map lengths. Observed
snapshot cost is independent of entry count. Lock contention can still delay a
snapshot, and entry metadata remains outside the payload budget.

## Padding regression investigation

The initial tracked-handle candidate passed correctness but reduced full reader
throughput by about 48% in five alternating rounds; it was rejected. A direct
cache diagnostic added only about 25 ns per get/clone/drop, far below the roughly
3 microseconds added per complete lookup. The baseline and candidate page decoders
had the same instructions apart from relocations, with different alignment of the
hot scalar padding loop. Instruction layout is a plausible contributor, rather
than a proven hardware-level attribution.

The final decoder still checks every padding byte using 8-byte comparisons and
the original scalar tail check. This removes most branches from sparse page
validation without skipping integrity work or copying cached payloads. The
padding-only control shows this read-path change contributes substantially to
the throughput improvement. Differences between that control and the candidate
also include generated-code and allocation layout; they must not be presented
as an isolated speedup from pin counting.

## Artifact identity and reproduction

| Artifact | SHA-256 |
| --- | --- |
| baseline storage test binary | `f73cb36ac9925af873a5d7ffab1cef6715a7a213ba3d7e8fd696dfb35cd9db19` |
| padding-only storage test binary | `4ada69eccda0397a236e044e521c38b911b6fb53eda569d6222ae22a4c862bb8` |
| candidate storage test binary | `f47d17edd7793c0dd39e143e8c4650b1e3b9d955b47b0a55e78e108698e61d8b` |
| Unchanged reader benchmark source | `479ffa0845b69ce42e886a9a5a38b26093dd972a6fd69cbd2ab833aa6beddc5f` |

Run the identical reader benchmark in each checkout:

```sh
cargo test --release -p hawdb-storage sharded_cache_reader_benchmark -- --ignored --nocapture
```

The final local gate uses default Bazel configuration:

```sh
bazel test //crates/storage:presubmit_tests //crates/runtime-tokio:presubmit_tests //:hawdb_unit_tests //:hawdb_cli_tests //:hawdb_storage_crash_recovery_tests //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

Manual campaigns and release benchmarks remain local. No CI job, timeout,
workload, or resource threshold was weakened.
