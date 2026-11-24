# Independent adjacency checkpoint evidence

Issue: https://github.com/nowledge-co/hawdb/issues/1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Qualification status

The implementation remains Draft. The current prepared-WAL follow-up is `6a378114156967f45972b49d0bc29ee25ed7874b`. It removes a confirmed extra operation-array scan and passes source-bound CI. Its ordinary 60-pair-per-group foreground medians are -1.24%, -1.18%, +0.01% and +0.11%, while some mutation/checkpoint maxima still increase. The earlier 1%-2% median increases did not recur, but this does not establish a causal fix or clear no-regression/tail acceptance. Full results and limits are in the [cause investigation](CAUSE_INVESTIGATION.md). Earlier packed/unpacked datasets below are preserved separately and must not be attributed to the current follow-up.

[Follow-up cause investigation and attempted fix](CAUSE_INVESTIGATION.md) documents the newly removed prepared-WAL operation scan, phase CPU/fsync counters, checkpoint synchronization stacks and source-bound CI at `6a3781141`. Its under-load diagnostic runs are separate from ordinary acceptance and do not clear the Draft gate.

## Sources and protocol

- Baseline: `5efcc9a2692fb7682b80093141588b2a741d52bf`; initial runtime: `b76b3809991d3aff408970070123e4d5b760c794`; packed runtime: `0d038b73e3cfb270cdfba9fc999535e2916228ae`; latest producer: `612cb32ceb778b2200b37bb8eee38980625b56ae`. The unchanged timed harness was introduced in `e26c25df3aeaae1939f6647c7eb788195b585433`.
- Rust 1.99.1, macOS 27.2.1 arm64. Repository release profile, default Cargo example features, separate candidate Cargo target, frozen byte-identical harness in both source trees. See [metadata](metadata.json) and per-group binary hashes in the [packed receipts](paired-packed-suite/run-receipts.json) and [unpacked receipts](paired-suite/run-receipts.json).
- SyncOnEveryWrite, OutOfCore, background maintenance explicitly disabled in both variants. No weaker durability setting supplies the improvement.
- Initialize two independent fixtures, then allow only one process to execute a mutation/checkpoint at a time. Alternate A/B and B/A each round: 60 measured pairs, 30 in each order, after two warmup pairs per workload/size. Total: 480 measured and 16 warmup operations.
- Every changed node/relationship value and count is checked outside timing. Both processes wait until timing completes before scrub and reopen. Each suite records the v2/v3 readiness preflight and its successful carry/rebuild assertions. [Earlier actual final manifests](manifest-qualification.json) also confirm baseline v2 and candidate v3.
- Checkpoint time includes immutable sealed-root closure validation/publication. New graph bytes count freshly created canonical/spill/projection/adjacency files, including descriptors; they are not total filesystem writes or allocated disk blocks. No CPU-time or RSS improvement is inferred.
- This is a shared workstation, without pinned CPU/I/O isolation. Mem Bazel activity was observed after the older suite. Before and after the packed suite, no such compilation or test processes were observed; ordinary desktop load remained. Do not infer production p99, foreground throughput or LDBC/TPC-C acceptance from these samples. With 60 samples per variant/group, reported p99 is the observed maximum.

## Earlier packed revision

The isolated candidate release rebuild took 5m39s. LLVM IR inspection of the actual release library confirmed a 56-byte reader identity, matching the baseline; no link-time size inference was used. Both variants used the same harness and protocol. All four groups completed, including value/count verification, scrub and reopen.

Checkpoint latency:

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|
| 128 / 512 | node_property | 443.093 / 529.080 / 564.061 | 396.314 / 516.643 / 576.771 | -10.65% |
| 128 / 512 | relationship_property | 683.871 / 847.300 / 889.040 | 674.113 / 851.926 / 881.013 | +0.29% |
| 512 / 4096 | node_property | 449.691 / 520.803 / 564.393 | 399.087 / 450.627 / 713.574 | -11.94% |
| 512 / 4096 | relationship_property | 457.422 / 789.095 / 813.443 | 451.232 / 794.480 / 837.062 | -0.26% |

Foreground synchronized mutation latency:

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|
| 128 / 512 | node_property | 4.321 / 4.628 / 5.429 | 4.396 / 4.899 / 5.536 | +2.08% |
| 128 / 512 | relationship_property | 6.957 / 9.082 / 38.208 | 7.251 / 9.223 / 10.470 | +1.12% |
| 512 / 4096 | node_property | 5.552 / 5.894 / 7.172 | 5.661 / 6.354 / 43.721 | +1.51% |
| 512 / 4096 | relationship_property | 19.623 / 27.433 / 31.782 | 19.754 / 27.042 / 30.687 | +0.82% |

Node-property checkpoints again produced zero new adjacency bytes and reduced new graph-file bytes by approximately 63.8% / 61.8%. Relationship-property controls rebuilt normally, with paired checkpoint medians +0.29% / -0.26%. Node-property checkpoint gains persisted in both execution orders and halves; see the [complete packed summary](paired-packed-suite/summary.json).

**Acceptance remains open.** All four foreground paired medians increased (+2.08%, +1.12%, +1.51%, +0.82%). The larger node-property mutation maximum was 43.721 ms versus 7.172 ms, and its checkpoint maximum was 713.574 ms versus 564.393 ms. Smaller node-property checkpoint p99 also increased. With 60 samples these are observed maxima, not production percentile estimates. No sample is removed, and no improvement in combined mutation/checkpoint time waives these failures. The packed representation has not established the cause of the variation. Further foreground-path diagnosis and controlled tail qualification are required before marking ready.

## Identical-binary diagnostic

After the packed comparison, both A/B roles ran the same frozen baseline executable (`e108839fa0a782fd9cbc3d14a2269a56f612e1e68b790db43032ce6c60456ca8`), with separate fixtures, v2 readiness and rebuild expectations. [The exact diagnostic controller](run_null.py) limits the original protocol to node-property workloads; it changes binary/version/expectation selection only. Each size completed 60 pairs plus two warmups, including scrub and reopen. [Raw records and receipts](paired-null-suite/run-receipts.json) preserve both identical hashes.

| Nodes / relationships | Checkpoint median paired change | Mutation median paired change | Mutation A / B p99 (ms) |
|---|---:|---:|---:|
| 128 / 512 | +0.04% | +0.03% | 5.170 / 8.563 |
| 512 / 4096 | +0.67% | +1.57% | 6.679 / 9.624 |

This demonstrates nonzero variation even with identical code. It does not explain every difference in the packed comparison, establish an error bar for future measurements, prove the candidate unchanged, or clear tail acceptance. It is a separate diagnostic, not a replacement dataset or a reason to discard unfavorable samples. [Complete diagnostic statistics](paired-null-suite/summary.json) retain order/half splits and all tails.

To replay the diagnostic with the same frozen binary directory:

```sh
python3 docs/benchmarks/pr1961/run_null.py "$task_dir/bin" "$task_dir/null-samples" 60
python3 docs/benchmarks/pr1961/analyze.py "$task_dir/null-samples"
```

## Earlier paired checkpoint results (unpacked revision)

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change | New graph bytes, median |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | 537.271 / 672.716 / 687.752 | 477.386 / 595.574 / 620.754 | -9.86% | 167,927.5 -> 60,773.5 |
| 128 / 512 | relationship_property | 531.583 / 737.328 / 980.839 | 529.657 / 680.914 / 792.757 | -0.88% | 167,927.0 -> 167,927.0 |
| 512 / 4096 | node_property | 493.871 / 596.167 / 658.406 | 431.846 / 514.530 / 601.123 | -12.37% | 1,109,565.0 -> 424,107.0 |
| 512 / 4096 | relationship_property | 815.366 / 1305.770 / 1713.312 | 791.571 / 1217.210 / 1547.920 | -0.86% | 1,109,565.0 -> 1,109,565.0 |

Paired changes are medians of per-pair ratios, not ratios of the aggregate medians. Node-property checkpoints wrote zero new adjacency bytes; relationship-property controls continued rebuilding. The node-property new-graph-byte reductions were approximately 63.8% and 61.8%.

## Earlier foreground control

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | 4.591 / 5.357 / 5.762 | 4.692 / 5.265 / 6.815 | +0.93% |
| 128 / 512 | relationship_property | 8.333 / 9.911 / 12.517 | 8.212 / 9.293 / 11.172 | -0.91% |
| 512 / 4096 | node_property | 6.491 / 8.230 / 9.130 | 6.552 / 7.739 / 11.753 | +0.42% |
| 512 / 4096 | relationship_property | 40.874 / 73.893 / 225.458 | 42.985 / 92.081 / 247.669 | +2.34% |

Foreground wall-clock variation remains unresolved: the larger relationship-property control had a +2.34% paired median, while node-property mutation maxima also increased. [Full paired statistics](paired-suite/summary.json) retain order-specific and first/second-half medians, absolute differences, all percentiles and combined mutation/checkpoint time. The 56-byte identity revision removes avoidable metadata growth, but that alone does not establish the cause or clear this acceptance gate. The packed rerun above supersedes this remaining measurement step, but does not clear the acceptance gate.

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
git archive 612cb32ceb778b2200b37bb8eee38980625b56ae | tar -x -C "$task_dir/candidate"
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
python3 docs/benchmarks/pr1961/analyze.py docs/benchmarks/pr1961/paired-packed-suite
```

## Correctness qualification

[Source-bound CI receipts](ci-qualification.json) record the initial storage execution (233.666s) and the `bb5462a13` root/crash qualification on actual runner merge `b9cd19d69ba5c57a1f6e3ab665b3eedcc41fee3f`, over main `7be31b2d`. Root: 77 targets passed, seven executed and 70 cached, including the actual general unit execution at 178.314s. These receipts precede the compact identity revision; final-head CI remains a separate gate. Required formatting, strict Clippy and hooks pass before each commit. The previously stopped heavy local Bazel/fuzz campaign was not restarted.

[Packed source-bound CI](ci-packed.json) records all nine successful Prow jobs at head `612cb32ceb778b2200b37bb8eee38980625b56ae`, integrated with base `4a2d7e0ad6762fb3f8cf93e8a61659469d622889`. Root, crates and crash clone records agree on actual runner `86f02312163d00fe3b613c0100e755ad4004c68d`; crash evidence independently reports that source. Complete BEP records distinguish root 77 targets (14 executed, 63 cached), crates 106 targets (21 executed, 85 cached), and an executed crash target at 7.930s. Cached storage-unit results are not reported as fresh executions. GitHub Actions also passed native bindings, Node/Electron, Windows contracts and iOS qualification at this head. A later documentation-only commit requires its own check status; these receipts do not assert an unmeasured future head.
