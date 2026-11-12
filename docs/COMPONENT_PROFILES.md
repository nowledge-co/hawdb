# Native search and durable-write component profiles

Tracking: [#1669](https://github.com/nowledge-co/hawdb/issues/1669).

The manual `component_profiles` example uses existing embedded Rust APIs.
It adds no production configuration, dependency, query shortcut or CI job.
Use the pinned toolchain and repository-wide release profile:

```console
cargo run --locked --release --example component_profiles -- search 4096 15 > /tmp/search-components.jsonl
cargo run --locked --release --example component_profiles -- write 15 1 > /tmp/write-single.jsonl
cargo run --locked --release --example component_profiles -- write 15 16 > /tmp/write-batch.jsonl
```

Search accepts 64..100000 documents and 15..100 measured rounds. Writes accept
15..100 measured transactions and 1..64 rows per transaction. Both modes add
three warmups and create an exclusive, unique temporary fixture. Failed
fixtures are retained at the emitted path; successful fixtures are closed and
removed. The example refuses checked/debug-assertion builds. It is compiled
by existing formatting/Clippy checks but is not a CI execution lane.

## Evidence and timing

JSONL records each attempt before entering the engine, flushes the record, then
retains completed samples and a final report. An interrupted log or absent final
report is not a completed profile. Fixture construction/open, validation,
metadata collection and JSON output are outside operation timers. The report
observes the checkout/toolchain/host and hashes the compiled example source and
executable when platform checksum tools are available. Runtime checkout identity
does not attest the compiled engine; retain the exact source/tree, build command
and executable with the raw output.

RSS is observed before/after a call. The process lifetime high-water mark is
not an isolated per-call peak, and allocator peak is unavailable. Page faults,
query working bounds, vector admitted working bytes and process RSS are separate
observations. Fifteen samples do not establish stable p99 or an uncontended
hardware comparison. Run cohorts serially and record other host load.

## Search

The streaming fixture uses deterministic IDs, common/rare terms, 128-byte
filler, eight-dimensional vectors, and a 90% hot/10% cold Space split. Search
filters on the hot Space, returns at most ten rows with a retained rank window of 64,
and records actual lexical/vector backends and fallback/truncation reports.
The build uses a 192-MiB reservation, 4-MiB content segments, a 1-MiB record
ceiling and an 8-MiB lexical build budget. Vector execution is single-worker
with a 64-MiB working limit;
the reader bounds score entries by document count and hydration to ten documents
and 1 MiB. These are typed API limits, not a claim that every ordinary search
allocation is represented by one query ledger.

For text and hybrid modes, separate untimed candidate and hydrated preflights
freeze IDs, all six score bit patterns and both rank channels. Three independent operations rotate order
across warmups and measured rounds:

- candidate selection without document bodies;
- hydration of the fixed selected IDs, without re-scoring;
- combined ranking and document hydration.

Each entry point must match its own complete ordered score signature. Across
entry points, IDs, selected score, text score, RRF contributions and ranks must
match. Existing document hydration recomputes raw vector relevance, including
for lexical-only RRF members outside the vector window. That diagnostic value
may differ from the candidate's vector score; the report retains both full
signatures and counts these enrichment differences rather than treating them
as ranking changes. Every repeat still checks that diagnostic value against
its own frozen preflight.
Hydration must preserve all selected IDs and verify content, Space and vectors.
Counters record requested/decoded ranges, lexical/posting/candidate/vector work,
spill and hydration. They do not measure physical device I/O. These component
calls use independent cache observations; subtracting their medians does not
produce a valid exclusive stage time. A fresh reader does not establish a cold
OS/device cache. This fixture measures no retrieval relevance/recall and does
not reproduce SciFact/LDBC or qualify a different vector backend.

## Persistent writes

The write fixture opens a persistent database with default `SyncOnEveryWrite`
and disabled group commit. Each explicit optimistic transaction inserts unique
IDs and 128-byte payloads. Timers separate begin, statement preparation and
the acknowledged commit (validation/WAL/synchronization/publication together).
Existing opt-in coordinator metrics record completed sequencer wait/hold,
commit execution and published-read-view capture phases. These are inclusive
and can overlap: they must not be summed into transaction latency.

Individual WAL encoding, fsync and head-publication-only durations are
unavailable and remain null. Zero group-durability observations with group commit
disabled do not imply zero filesystem synchronization. The profile does not
change synchronization policy to improve a measurement.

Complete ordered IDs and payloads are verified before an explicit checkpoint
and again after closing/reopening. Checkpoint and reopen timers, plus logical
file lengths before/after checkpoint, are separate from commit samples. Logical
lengths are not allocated physical blocks or unique-inode storage. The fixture
is a growing single-writer table, so batching changes rows per transaction and
must be reported explicitly. Checkpoint/reopen is normal-recovery evidence,
not process-crash or power-loss qualification.

## First native observation

The [2026-11-11 report](benchmarks/component_profiles_2026_11_11/README.md)
retains raw JSONL, source/executable receipts, small/bulk cohorts and measured
boundaries. It is a single-host baseline, not an engine speedup claim.
