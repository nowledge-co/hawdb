# Embedded MVCC entrypoint comparison (2026-11-22)

Issue: [#1890](https://github.com/nowledge-co/hawdb/issues/1890). This is a current-main isolated-source comparison, not the earlier prototype workspace measurement.

## Procedure and scope

Build with `cargo build --locked --example embedded_mvcc_bench -j 2`, then run `target/debug/examples/embedded_mvcc_bench 5 1000 32` and `target/debug/examples/embedded_mvcc_bench 5 100 4096` separately. `/usr/bin/time -l` supplies whole-process resource observations. The recorded build reused downloaded dependencies/check artifacts but compiled the native executable for this checkout/toolchain in 144 seconds; this is not a fully cold download/build measurement.

Both modes run the same admitted Cypher workload in one executable. The control wraps `HawDBEmbedded` in a whole-call mutex; the candidate uses `HawDBConcurrentEmbedded`. Each sample gets a fresh in-memory indexed fixture, three warmup reads, and one or four workers. Reads count rows with a predicate; mixed workloads replace every tenth read with a disjoint point increment. Five rounds alternate control/candidate order. All 80 samples verify exact final acknowledged update counts and complete CPU/memory admission refunds, with zero admission refusals.

macOS 27.0.1, Mac17,7 ARM64, 18 logical CPUs, 36 GiB RAM; pinned Rust 1.99.1. The governor is explicitly pinned to eight effective CPU slots and a 1 GiB memory budget. Repository dev profile: opt-level 1, overflow/debug checks enabled. The host was not reserved. See `manifest.json` for source/binary hashes, commands, build time and resource observations.

## Results

Each table value is the median of five per-sample measurements. Throughput ratios divide those medians; latency columns are medians of sample p99 values, not pooled percentiles. Units are operations/second and milliseconds.

| Rows | Workers | Writes | Control ops/s | MVCC ops/s | Ratio | Read p99 control / MVCC (ms) | Write p99 control / MVCC (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1 | 0% | 26248 | 27757 | 1.06x | 0.054 / 0.057 | 0.000 / 0.000 |
| 32 | 1 | 10% | 24420 | 23774 | 0.97x | 0.082 / 0.082 | 0.079 / 0.147 |
| 32 | 4 | 0% | 23253 | 69334 | 2.98x | 0.067 / 0.114 | 0.000 / 0.000 |
| 32 | 4 | 10% | 20784 | 52114 | 2.51x | 0.088 / 0.158 | 0.091 / 0.256 |
| 4096 | 1 | 0% | 549 | 559 | 1.02x | 2.064 / 2.031 | 0.000 / 0.000 |
| 4096 | 1 | 10% | 531 | 530 | 1.00x | 3.630 / 3.639 | 2.389 / 2.592 |
| 4096 | 4 | 0% | 551 | 1816 | 3.29x | 129.884 / 3.152 | 0.000 / 0.000 |
| 4096 | 4 | 10% | 509 | 1679 | 3.29x | 4.248 / 4.149 | 139.472 / 3.062 |

Four-worker throughput improves 2.51-3.29x in these fixtures. Single-worker throughput ranges from -2.6% to +5.7%. Small-fixture read and write p99 can increase despite higher throughput: the four-worker mixed write p99 rises from 0.091 ms to 0.256 ms. For 4096-row mixed traffic, write p99 falls from 139.472 ms to 3.062 ms. These are observations, not stable guarantees or confidence intervals; larger mixed samples contain only 10 writes per worker, making write tails particularly sensitive to scheduling.

Peak resident set size is 25,821,184 bytes for the complete small-fixture process and 59,916,288 bytes for the large-fixture process. These include both modes, setup and all rounds, so they do not establish a per-mode memory improvement. No swaps or block I/O operations were recorded.

This benchmark does not measure persistent fsync/group commit, FFI JSON conversion, SQL throughput, release-profile behavior, remote CI speed or production behavior. Run the same example with `--release` for a separate release qualification. Deterministic held-owner regressions establish the independent-progress contract without interpreting timing as correctness.

`rows-32.jsonl` and `rows-4096.jsonl` retain every sample; `summary.csv` includes p50/p95/p99. Compilation/test logs stay outside the repository.
