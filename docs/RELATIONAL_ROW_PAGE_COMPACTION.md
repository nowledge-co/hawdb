# Relational row-page compaction

`Database::compact_relational_row_pages(config)` is an explicit synchronous
maintenance operation. The `_context` variant accepts a `RuntimeTaskContext` for
cancellation and deadlines. It uses the existing portable filesystem path; no
new backend, runtime, `io_uring`, or independent commit selector is introduced.

## Selection and publication

The active root carries a sorted physical-generation inventory: allocated slots
and live descriptors for each referenced page file. A generation is selected
when `live_pages * 100 <= allocated_pages * max_live_ratio_percent` (default 50).
Ratios use integer arithmetic. Dirty/deleted pages are handled by ordinary
checkpoint planning; surviving selected pages are verified and moved one at a
time during the streamed root merge. Relocation preserves logical page IDs,
source commit epochs, schemas, keys, rows, and overflow references. It changes
physical generation/slot identity and recomputes encoded integrity.

The row publisher only persists a candidate. The canonical checkpoint manifest
selects the complete row/overflow/checkpoint/WAL generation last. Preparation
errors and cancellation discard the candidate so its generation can be retried.
Publication retains the existing committed-acknowledgement boundary: cancellation
does not turn an already-published checkpoint into a retryable cancellation.
Ordinary reader-aware reclamation retains the physical closure of pinned roots.
Releasing a pin does not itself run maintenance; a later checkpoint performs the
normal best-effort reclamation sweep.

A clean-root compaction leaves retained generations above the configured live
ratio and new files fully occupied. Thus active allocation is bounded by live
data divided by the ratio. Pending mutations can lower a previously dense
generation's ratio; a subsequent clean-root compaction converges to that bound.
Pinned or cleanup-pending historical files are separate from active allocation.

## Resource and observability contract

- The default descriptor scan cap is 1,000,000 pages; the default rewrite byte
  cap is 128 GiB. Exceeding either fails before canonical selection.
- Dirty input defaults to 128 pages / 16 MiB, independently of relocated pages.
  The row-change capture is capped before grouping and dirty-page planning.
- Configured runtime governance admits one background task with CPU, memory,
  and I/O capacity before checkpoint work. The permit lasts through publication
  and reclamation. Raw `Database` without a governor remains host-governed.
- `admission_bytes()` estimates transient row planning, manifest/page buffers,
  adjacency/projection writers and canonical record/segment buffers. The operation
  adds a bounded materialized-checkpoint allowance where applicable and the
  enabled columnar shadow's own allowance. Shadow uses the same admission, not a
  nested permit. This is reservation estimation, not allocator/RSS accounting;
  existing checkpoint sidecar builders retain their own limits.
- `RelationalRowPageCompactionReport` reports dirty/relocated/reused pages, old
  and new active allocation, publication identity, and requested memory.
- Typed residency and resource-profile JSON expose `physical_generation_count`,
  `allocated_page_count`, `live_page_bytes`, and `allocated_page_bytes`.
  `page_artifact_bytes` still describes only the current generation's file;
  `canonical_artifact_bytes()` now includes all active physical page allocation.
- Explicit scrub verifies every referenced page, each generation's actual file
  length and descriptor count, and the row/overflow closure. Reader open remains
  metadata-only; no startup full scan is added.

## Development format

This remains v1. The integrity-bound row manifest now requires the physical
inventory flag and trailer. Older development-only manifests fail with an
explicit database-recreation message; absent allocation never means zero.
There is no compatibility migration for old greenfield databases. The logical
root-set digest does not incorporate physical placement. Stable/GA inclusion
policy is unchanged.

## Verification

The facade churn regression creates 16 user tables plus the schema registry,
freezes a different page in each generation, and compares 137 allocated slots
with 17 live slots. It verifies old pinned reads, exact 17-slot allocation after
compaction, actual file-byte reclamation after unpin/checkpoint, scrub, and reopen
in both materialized and authoritative out-of-core modes. Other regressions
exercise canonical publication failpoints, failed-candidate retries, governor
denial/release, one-permit shadow work, and authenticated occupancy corruption.

The COW TLA model permits clean-page relocation without a new logical commit and
checks source-epoch preservation, manifest-last selection and pinned closure.
Its nondeterministic page selection overapproximates threshold selection; numeric
occupancy and implementation resource limits remain executable-test obligations.

Dedicated local fuzz compares SQL update/delete/reinsert and inline/overflow
payloads against an independent row model across compaction, pinned views, failed
rewrite budgets, cleanup and reopen. It is part of the existing local fuzz suite
and exposes deterministic case replay through the existing storage fuzz CLI:

```sh
bazel run //crates/fuzz:hawdb_storage_fuzz -- \
  --row-page-compaction --seed 189 --cases 8
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results
bazel test //docs/tla:HawDBCowPagePublication_check --nocache_test_results
```

Fuzz is local-only. Existing macOS/Windows storage jobs also execute the facade
compaction regressions; cross-compilation alone is not native execution evidence.
