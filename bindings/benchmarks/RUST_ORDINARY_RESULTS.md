# Current-main ordinary Rust controls

This frozen comparison supports the ordinary API control for
[PR #986](https://github.com/nowledge-co/hawdb/pull/986),
[issue #976](https://github.com/nowledge-co/hawdb/issues/976) and
[the projected point-seek issue](https://github.com/nowledge-co/hawdb/issues/992).
It does not complete the comprehensive performance gate.

## Revisions and measurement boundary

Baseline main is `3cdb2610ba8b984676b80ba22df64821787886d3` with staged tree
`927a6acd92d805c9a8ba8afa3ae24d5024838e66`. The staged additions are the identical
ordinary benchmark harness and untimed opening adapters. Query, conversion,
write and durability implementations remain main's. The earlier main adapter
qualification used `5a5c4639`; the update to `3cdb2610` changes only three test
deadline attributes. The native producer SHA-256 is
`ae0a4d5a25696f0b957e2b5d54a9b992d1ec2f359f71323bf1e2a85be873866d`.

Candidate is `9630934b84a1d69e7713b4652322bfb08e242f86`, tree
`5a720c8c2058ebce6c982af0087e0ba4a0013675`, native producer SHA-256
`b9a82d8fa8bbab8a182255e19afe63a794bdcd40b9d7dc8dda919ba302649cb3`.

Both producers use default `bazel build -c opt //:hawdb_bench_host_boundary`,
pinned Rust 1.97.1 and macOS arm64 on Apple M5 Max with 36 GiB RAM. The five
fixed-seed cases run at 1,000 and 10,000 rows on memory and file backends.
Each group has one discarded pair and seven measured pairs; candidate/baseline
order alternates by iteration. Every iteration opens a fresh database. Reads
include their unchanged warmup. Persistent writes use `SyncOnEveryWrite`.
No Arrow, retained API, allocator instrumentation, budget override or global
compiler override is used. Competing host compilation was present, so these
observations cannot establish an idle-host regression or latency guarantee.

## Observations

Paired speedup is baseline time divided by candidate time. Below one is slower.
The table reports the median of seven within-iteration ratios, rather than the
ratio of the two median times. Query boundary includes verification reads for
write cases. The separate write phase contains only the committed write calls.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1,000 | `select` | memory | 1.045x | - |
| 1,000 | `select` | file | 0.889x | - |
| 1,000 | `point` | memory | 1.128x | - |
| 1,000 | `point` | file | 1.178x | - |
| 1,000 | `fill` | memory | 0.931x | 0.931x |
| 1,000 | `fill` | file | 0.997x | 0.996x |
| 1,000 | `fill_bulk` | memory | 1.049x | 1.034x |
| 1,000 | `fill_bulk` | file | 0.899x | 0.929x |
| 1,000 | `wide` | memory | 0.980x | - |
| 1,000 | `wide` | file | 1.190x | - |
| 10,000 | `select` | memory | 0.915x | - |
| 10,000 | `select` | file | 0.916x | - |
| 10,000 | `point` | memory | 4.013x | - |
| 10,000 | `point` | file | 3.111x | - |
| 10,000 | `fill` | memory | 0.957x | 0.956x |
| 10,000 | `fill` | file | 0.984x | 0.984x |
| 10,000 | `fill_bulk` | memory | 1.017x | 0.999x |
| 10,000 | `fill_bulk` | file | 1.004x | 1.011x |
| 10,000 | `wide` | memory | Refused before timing | - |
| 10,000 | `wide` | file | Refused before timing | - |

All 320 expected records are present: 280 measured and 40 discarded. The 18
successful groups contain 288 records preserving typed checksums, row counts,
durability, profile absence and phase-sum invariants. Fixture, source, staged
index, harness and frozen binary identities pass final verification.

The 10,000-row indexed point workload has median query-boundary speedups of
4.013x in memory and 3.111x on files. Every measured point pair is faster at
that size; minima are 1.443x and 2.521x. It uses the ordinary row path, not
columnar delivery. No conclusion about source-to-host zero-copy follows.

Unfavorable scan and write groups remain in the table and raw evidence.
The 10,000-row memory single-write paired speedup is 0.956x; file is 0.984x.
Shared-host variability and mixed outcomes prevent attribution to retained
cache maintenance or acceptance of write neutrality.

Both 10,000-row wide groups fail during warmup on both producers: the ordinary
`query result (result_materialization)` would charge 16,780,548 bytes against
the unchanged 16,777,216-byte budget. All 32 failed records are preserved.
The native producer reports structured `status=error` with process exit zero;
the driver correctly refuses qualification and exits one. These are budget
refusals, not successful timings or speedups. No result is truncated.

## Evidence and remaining qualification

[Complete evidence JSON](evidence/rust_ordinary_controls_20261011.json) preserves
every record, paired ratio, source/artifact manifest, raw stdout/stderr and their
hashes, and the final audit. The original report SHA-256 is
`d5f1266574a88bf877abf9981e8121af31b679a14f36a446f28e343a46242c09`.
The unchanged driver and fixed fixture hashes make consumer parity auditable.

This comparison predates the shared-directory capacity-cache follow-up. It
does not measure that follow-up, Python/Go, 100,000/1,000,000-row qualification,
cold/warm retained creation, allocation profiles, or whole-query memory bounds.
Process peak RSS includes fixture parsing, setup, warmup and the checksum.
It is not the memory held solely by a retained source or result. The full
five-case cross-language performance and memory gates remain open.

## Shared-directory capacity-cache control
The subsequent control uses candidate `8513f167869013ea72e2066508e7e6662cb26bca`,
tree `017f7ea5608d8c963bc0183b5728583db45ba68e`, against the same main
baseline and binary described above. The candidate binary SHA-256 is
`206f996d383829e1e084f320fd89b95793123cc86c9577ed4191fd1134ff3e6a`.
The harness, fixture hashes, defaults, sizes, backend order, discarded pair,
and seven alternating measured pairs are unchanged. Both producer sources and
binaries stayed frozen throughout the experiment. Competing host compilation
was present; this control does not establish cache-change causality.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1,000 | `select` | memory | 0.951x | - |
| 1,000 | `select` | file | 0.924x | - |
| 1,000 | `point` | memory | 1.141x | - |
| 1,000 | `point` | file | 1.188x | - |
| 1,000 | `fill` | memory | 0.971x | 0.969x |
| 1,000 | `fill` | file | 1.027x | 1.027x |
| 1,000 | `fill_bulk` | memory | 1.013x | 1.017x |
| 1,000 | `fill_bulk` | file | 0.949x | 0.958x |
| 1,000 | `wide` | memory | 0.986x | - |
| 1,000 | `wide` | file | 0.948x | - |
| 10,000 | `select` | memory | 0.994x | - |
| 10,000 | `select` | file | 0.995x | - |
| 10,000 | `point` | memory | 2.581x | - |
| 10,000 | `point` | file | 2.511x | - |
| 10,000 | `fill` | memory | 0.986x | 0.986x |
| 10,000 | `fill` | file | 0.977x | 0.977x |
| 10,000 | `fill_bulk` | memory | 1.072x | 1.081x |
| 10,000 | `fill_bulk` | file | 1.073x | 1.080x |
| 10,000 | `wide` | memory | Refused before timing | - |
| 10,000 | `wide` | file | Refused before timing | - |

All 320 records are present, with 288 successful records and 32 unchanged
10,000-row wide budget refusals. The 10,000-row ordinary point groups have
paired medians of 2.581x (memory) and 2.511x (file); all seven measured pairs
are faster at this size. Scans and several write groups remain unfavorable.
The individual-create write-phase medians of paired ratios are 0.986x (memory)
and 0.977x (file) at 10,000 rows. These outcomes cannot close the comprehensive
performance gate or prove write neutrality. This experiment predates the
[read-free CREATE statistics change](../../docs/READ_FREE_CREATE_STATISTICS.md).

[Complete shared-cache control evidence](evidence/rust_ordinary_shared_cache_controls_20261011.json)
contains raw stdout/stderr, hashes, all ratios and the final audit. The original
report SHA-256 is `6c27ac81694b3812503221a4acf9b6b86665abe0c5a498a465cc82ae1f788949`.
The earlier `9630934b` observations above remain a separate historical control;
comparing their ratios across runs does not isolate the cache change.
