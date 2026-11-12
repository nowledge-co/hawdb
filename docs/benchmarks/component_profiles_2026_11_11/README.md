# Native component observations, 2026-11-11

Tracking: [#1669](https://github.com/nowledge-co/hawdb/issues/1669).
Protocol and caveats: [component profiles](../../COMPONENT_PROFILES.md).

## Source and environment

- Engine source: unchanged main `9679f81e223d64a97e114f1be3da3a59ec821e87`.
  The only Rust addition is `examples/component_profiles.rs` in this delivery.
- Compiled example SHA-256:
  `b1e282fa6b6c8888268774a1c3bdd0af8f97fb05e7db249053bd445e749e50e9`.
- Executable SHA-256:
  `0cd36a2aa7c3b20c4689a1693603786dd73316b252e084149cbc667bf81225ec`.
- macOS arm64, Apple M5 Max, 18 available hardware threads, Rust 1.97.1,
  repository release profile/default features, two Cargo build jobs.
- Observed host load at the 4096-document cohort: 6.26 / 4.42 / 4.56.
  No CPU affinity, isolated machine, cold device cache or statistical
  before/after control is claimed.
- Initial release compilation took 3m24s. Final source relink took 55.83s;
  these build costs are separate from the operation samples below.

Commands, run serially from the repository root:

```console
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo build --locked --release --example component_profiles
cargo run --locked --release --example component_profiles -- search 64 15
cargo run --locked --release --example component_profiles -- search 4096 15
cargo run --locked --release --example component_profiles -- write 15 1
cargo run --locked --release --example component_profiles -- write 15 16
```

Formatting and strict Clippy passed. Both invalid argument controls
(`search 0 15`, `write 15 65`) failed before creating a fixture.
All four cohorts completed their final report and inline oracles; failed setup
and earlier oracle-development attempts are not included as passing samples.

## Search

Each phase/mode has three warmups and fifteen measured calls. Both search
cohorts retain 108 completed samples including warmups. Values below are
measured medians in milliseconds; phases are independent observations and
must not be added or subtracted to derive exclusive stage times.

| Documents | Mode | Candidates | Hydrate fixed IDs | Combined documents |
| ---: | --- | ---: | ---: | ---: |
| 64 | text | 3.946 | 0.058 | 3.953 |
| 64 | hybrid | 4.178 | 0.119 | 4.397 |
| 4096 | text | 5.123 | 0.520 | 5.325 |
| 4096 | hybrid | 6.579 | 0.516 | 6.667 |

Both cohorts use segmented BM25 and hybrid DiskANN out-of-core candidate graph
retrieval; the text-only report's inactive vector retriever is not exercised.
The 4096-document hybrid preflight scores 1753 vector documents, generates 128
vector candidates and admits a 64-MiB vector working budget. This admission
is not measured resident usage. All outputs are deliberately top-k truncated;
there are no fallback reasons and no recall evaluation.

Every repeated candidate/full entry point preserves its frozen ordered IDs,
six score bit patterns and both ranks. Four hybrid raw vector diagnostics
are enriched during full hydration at 4096 documents; the raw signatures record
this existing behavior. Cross-entry-point selected scores, RRF contributions,
text scores and ranks match. Fixed-ID hydration verifies the complete expected
SearchDocument, including full content, title, metadata and vector values.

For 4096 documents the per-call counters are stable across measured rounds:

| Mode / call | Segment range bytes | Lexical document bytes | Vector bytes | Hydration bytes | Candidate spill bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| text candidates | 87776 | 819490 | 0 | 0 | 88464 |
| text hydrate fixed IDs | 2185 | 819490 | 0 | 2130 | 0 |
| text combined | 89961 | 819490 | 0 | 2130 | 88464 |
| hybrid candidates | 234824 | 819490 | 4096 | 0 | 88464 |
| hybrid hydrate fixed IDs | 2285 | 819490 | 0 | 2134 | 0 |
| hybrid combined | 237109 | 819490 | 4096 | 2134 | 88464 |

These counters overlap and are not physical device bytes. The lexical document
counter includes the existing per-ID routing probes, including in fixed-ID
hydration. Body materialization is small in this fixture; routing/probe work is
an investigation lead, not a measured benefit from a proposed cache.
Observed process lifetime peak RSS is 14,893,056 B (64 documents) and
25,559,040 B (4096); neither is an isolated operation or allocator peak.

## Strict persistent writes

Each cohort includes three warmups and fifteen measured transactions, default
SyncOnEveryWrite, group commit disabled, and 128-byte payloads. Values are
medians in milliseconds, on a growing table. Per-column medians and inclusive
coordinator counters cannot be added into a synthetic transaction time.

| Rows / transaction | Begin | Statement preparation | Acknowledged commit | Total | Verified rows |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.005 | 0.014 | 3.753 | 3.774 | 18 |
| 16 | 0.013 | 0.405 | 4.327 | 4.767 | 288 |

The commit includes validation, WAL, synchronization and publication; the
profile cannot attribute it to individual fsync or WAL encoding. Batching
amortizes that observed transaction boundary across more rows, but these two
growing-table cohorts are not a controlled engine before/after comparison.
No durability policy is changed.

| Rows / transaction | Checkpoint ms | Reopen ms | Logical bytes before / after checkpoint | Process lifetime peak RSS B |
| ---: | ---: | ---: | --- | ---: |
| 1 | 452.107 | 5.154 | 46333 / 70463 | 18694144 |
| 16 | 478.082 | 4.452 | 103048 / 310276 | 19218432 |

Every ordered ID and full payload matches before checkpoint and after reopen.
This verifies normal recovery only; it does not model lost writes, torn writes,
write reordering, process crashes or power loss.

## Raw evidence

Gzip files preserve the complete emitted JSONL, including attempt-start records,
all warmup/measured samples and the final report. Decompress with `gzip -dc` and
parse the final JSON object's `report.samples`; the same samples also appear as
individual records, so do not count both copies.

| Artifact | Gzip SHA-256 |
| --- | --- |
| [64-document smoke](search-smoke.jsonl.gz) | `e04f66cfbb2b5344dde517027ff3fd973bf1f489010bb0708db9b7452f19df49` |
| [4096-document search](search-4096.jsonl.gz) | `9c062babbb0e273103f6eb17837669837d15fd42e9ace8351e86f77e8b8d5746` |
| [Single-row writes](write-batch1.jsonl.gz) | `7dfc63d8841777361f6e3a821adbe1a36dcb0c6ccd59e5874596b2604f4e6c33` |
| [16-row writes](write-batch16.jsonl.gz) | `1b3d09879e34b68e3ff2a6c3cf9db985620ae693000d065e00ca3f23fec6459e` |
