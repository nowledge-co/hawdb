# Independent adjacency checkpoint evidence

Issue: https://github.com/nowledge-co/hawdb/issues/1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Qualification status

The implementation remains Draft. The timed results below belong to `e26c25df3aeaae1939f6647c7eb788195b585433`, whose reader identity occupied 72 bytes. Current revision `0d038b73e3cfb270cdfba9fc999535e2916228ae` packs that identity within the original 56-byte footprint and preserves optional getters, generation zero and Debug output. Its timing qualification is still required. Do not attribute these timings to the packed revision or treat checkpoint improvements as proof of unchanged foreground/tail performance.

## Sources and protocol

- Baseline: `5efcc9a2692fb7682b80093141588b2a741d52bf`; runtime change: `b76b3809991d3aff408970070123e4d5b760c794`; timed harness: `e26c25df3aeaae1939f6647c7eb788195b585433`.
- Rust 1.99.1, macOS 27.2.1 arm64. Repository release profile, default Cargo example features, separate candidate Cargo target, frozen byte-identical harness in both source trees. See [metadata](metadata.json) and per-group binary hashes in [receipts](paired-suite/run-receipts.json).
- SyncOnEveryWrite, OutOfCore, background maintenance explicitly disabled in both variants. No weaker durability setting supplies the improvement.
- Initialize two independent fixtures, then allow only one process to execute a mutation/checkpoint at a time. Alternate A/B and B/A each round: 60 measured pairs, 30 in each order, after two warmup pairs per workload/size. Total: 480 measured and 16 warmup operations.
- Every changed node/relationship value and count is checked outside timing. Both processes wait until timing completes before scrub and reopen. [Actual final manifests](manifest-qualification.json) confirm baseline v2 and candidate v3.
- Checkpoint time includes immutable sealed-root closure validation/publication. New graph bytes count freshly created canonical/spill/projection/adjacency files, including descriptors; they are not total filesystem writes or allocated disk blocks. No CPU-time or RSS improvement is inferred.
- This is a shared workstation, without pinned CPU/I/O isolation. Other Mem Bazel compilation/test activity was subsequently observed. Do not infer production p99, foreground throughput or LDBC/TPC-C acceptance from these samples. With 60 samples per variant/group, reported p99 is the observed maximum.

## Paired checkpoint results (unpacked revision)

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change | New graph bytes, median |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | 537.271 / 672.716 / 687.752 | 477.386 / 595.574 / 620.754 | -9.86% | 167,927.5 -> 60,773.5 |
| 128 / 512 | relationship_property | 531.583 / 737.328 / 980.839 | 529.657 / 680.914 / 792.757 | -0.88% | 167,927.0 -> 167,927.0 |
| 512 / 4096 | node_property | 493.871 / 596.167 / 658.406 | 431.846 / 514.530 / 601.123 | -12.37% | 1,109,565.0 -> 424,107.0 |
| 512 / 4096 | relationship_property | 815.366 / 1305.770 / 1713.312 | 791.571 / 1217.210 / 1547.920 | -0.86% | 1,109,565.0 -> 1,109,565.0 |

Paired changes are medians of per-pair ratios, not ratios of the aggregate medians. Node-property checkpoints wrote zero new adjacency bytes; relationship-property controls continued rebuilding. The node-property new-graph-byte reductions were approximately 63.8% and 61.8%.

## Foreground control and remaining acceptance

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | 4.591 / 5.357 / 5.762 | 4.692 / 5.265 / 6.815 | +0.93% |
| 128 / 512 | relationship_property | 8.333 / 9.911 / 12.517 | 8.212 / 9.293 / 11.172 | -0.91% |
| 512 / 4096 | node_property | 6.491 / 8.230 / 9.130 | 6.552 / 7.739 / 11.753 | +0.42% |
| 512 / 4096 | relationship_property | 40.874 / 73.893 / 225.458 | 42.985 / 92.081 / 247.669 | +2.34% |

Foreground wall-clock variation remains unresolved: the larger relationship-property control had a +2.34% paired median, while node-property mutation maxima also increased. [Full paired statistics](paired-suite/summary.json) retain order-specific and first/second-half medians, absolute differences, all percentiles and combined mutation/checkpoint time. The 56-byte identity revision removes avoidable metadata growth, but that alone does not establish the cause or clear this acceptance gate. Re-run against the packed revision on an idle or dedicated host before marking ready.

## Other measurements and exclusions

[Whole-run exploratory data](qualified-suite/summary.json) uses common durability/residency/background settings but alternates entire runs, with 42 samples per variant/group. It showed substantial phase variation in controls and tails, motivating immediate operation pairing. It remains separate and is not substituted for the paired acceptance results.

An earlier candidate executable selected baseline v2 artifacts through a shared Cargo release cache and was rejected before comparative acceptance. A separate eight-run suite was rejected in full because the baseline output lacked the frozen background-maintenance configuration. Those runs and hashes remain in the local evidence archive; [metadata](metadata.json) records the exclusions. No favorable samples from either invalid qualification are used in these tables.

Build cost is separate from execution: initial full embedded baseline build 6m07s with existing third-party artifacts; isolated candidate empty-target build 6m59s. The frozen step-mode rebuilds took 3m34s baseline and 4m36s candidate. These cache states differ and establish no build or remote CI speedup.

## Reproduction

Use two fresh sibling source archives and a different Cargo target directory for each variant. Copy the exact harness from the timed revision into the baseline archive. Freeze source files until both builds finish; verify v2/v3 and background-maintenance output before accepting samples. Never reuse existing fixture/output paths or overwrite this recorded dataset.

```sh
task_dir="$(mktemp -d /private/tmp/hawdb-pr1961-replay.XXXXXX)"
mkdir -p "$task_dir/baseline" "$task_dir/candidate" "$task_dir/bin"
git archive 5efcc9a2692fb7682b80093141588b2a741d52bf | tar -x -C "$task_dir/baseline"
git archive e26c25df3aeaae1939f6647c7eb788195b585433 | tar -x -C "$task_dir/candidate"
cp "$task_dir/candidate/examples/graph_checkpoint_carry_bench.rs" "$task_dir/baseline/examples/graph_checkpoint_carry_bench.rs"
(cd "$task_dir/baseline" && CARGO_TARGET_DIR="$task_dir/baseline-target" cargo build --locked --release --example graph_checkpoint_carry_bench -j 1)
(cd "$task_dir/candidate" && CARGO_TARGET_DIR="$task_dir/candidate-target" cargo build --locked --release --example graph_checkpoint_carry_bench -j 1)
cp "$task_dir/baseline-target/release/examples/graph_checkpoint_carry_bench" "$task_dir/bin/embedded-paired-baseline"
cp "$task_dir/candidate-target/release/examples/graph_checkpoint_carry_bench" "$task_dir/bin/embedded-paired-candidate"
python3 docs/benchmarks/pr1961/run_paired.py "$task_dir/bin" "$task_dir/preflight" 1 smoke
python3 docs/benchmarks/pr1961/run_paired.py "$task_dir/bin" "$task_dir/samples" 60
python3 docs/benchmarks/pr1961/analyze.py "$task_dir/samples"
```

The reference controller uses Unix pipes/select and creates only a new explicit output root. It runs no benchmark in CI. To inspect these frozen results without changing them:

```sh
python3 docs/benchmarks/pr1961/analyze.py docs/benchmarks/pr1961/paired-suite
```

## Correctness qualification

[Source-bound CI receipts](ci-qualification.json) record the initial storage execution (233.666s) and the `bb5462a13` root/crash qualification on actual runner merge `b9cd19d69ba5c57a1f6e3ab665b3eedcc41fee3f`, over main `7be31b2d`. Root: 77 targets passed, seven executed and 70 cached, including the actual general unit execution at 178.314s. These receipts precede the compact identity revision; final-head CI remains a separate gate. Required formatting, strict Clippy and hooks pass before each commit. The previously stopped heavy local Bazel/fuzz campaign was not restarted.
