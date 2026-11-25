# WAL collection scratch reuse

Issue Number: ref #1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Temporal payload follow-up

The collection buffers still allocated a 28-byte temporary Vec for every
Date, LocalTime, LocalDateTime, DateTime and Duration payload. Since the shared
temporal codec already defines each exact fixed width, the ordinary WAL
encoder now writes its existing tag and length followed by the payload directly
into the output buffer. It uses the same payload codec; no temporal bytes,
field IDs, validation, operation order or synchronization behavior changes.
There is no additional retained buffer or public API.
The encoder reserves the complete fixed-width frame before writing its
components, so an empty output needs only one allocation. Temporal framing
stays outside the recursive value dispatcher to limit its instruction footprint.

The existing storage unit owner covers all five kinds, negative durations,
calendar/offset values, appending to an existing output prefix, and scalar
batches, Lists, Maps and node property records at one and 4096 values. An
already sufficient output capacity must require zero allocation requests.
An empty output must allocate once, without intermediate payload growth.
Prepared batches must require fewer than 64 requests, including output growth,
and match ordinary encoding, the independent checkpoint codec and roundtrips.

The manual `wal_batch_encode` diagnostic now includes temporal scalar batches,
Lists, Maps and property records alongside integer and large-first controls.
Both workers wait for `finish` after every timed pair has completed, so process
teardown cannot overlap the peer's final sample. Use the same updated harness
and controller for both source revisions. These encoding measurements remain
separate from synchronized embedded query and checkpoint acceptance.

## Change

Runtime `744f90cfee1f5d7c8803235f6eefee9f51865b49` extends bounded scratch reuse
to List elements and Map/property entries. Previously each list element created
one temporary Vec, while each map/property entry created a body and value Vec.
The entry layout is identical in Map values and record properties; both now
use the same helper. Existing operation-level scratch is unchanged in behavior.

Each list retains at most one 64 KiB scratch buffer between siblings. Each
entry encoder retains at most two such buffers. Oversized capacities are
released immediately after the element/entry is copied into its output. Before
a nonempty List/Map child recurses, preceding scalar scratch is dropped even
below the cap. This preserves the former fresh-scratch lifetime before descent
instead of retaining preceding scalar capacity at every ancestor. Empty child
containers do not recurse and keep the reuse path.

These bounds cover cached temporary capacity, not the encoded output, the
complete record, peak process memory or RSS. Input validation, field ordering,
length-prefix encoding, empty-container markers, WAL framing/checksums,
LSN/epoch binding and every synchronization barrier remain unchanged. This
follow-up changes neither the persisted format nor a public API. Primitive,
vector and temporal payload encoders keep their existing behavior.

## Allocation contracts on a busy host

The native test allocator observes requests on the calling test thread only.
Inputs are constructed before observation. The control is the frozen former
recursive container implementation, using unchanged primitive encoders; the
optimized implementation runs in a separate observation. Both output buffers
are compared completely. The test also refuses an increase in total requested
allocation bytes, including growth requests. It does not measure live memory.

The following regression bounds passed in the native Linux x86_64 storage
CI test at this source. They are inequalities, not exact allocation totals or
elapsed-time measurements. They use the ordinary checked CI profile; no
release-profile timing or allocation result is inferred. The owning test target
executed freshly; source-bound receipts are linked below.

| Synthetic value | Elements | Control requests | Optimized requests | Requested bytes |
|---|---:|---:|---:|---|
| List of integers | 4096 | >= 4096 | < 64 | <= control |
| List of empty Lists | 4096 | >= 4096 | < 64 | <= control |
| List of empty Maps | 4096 | >= 4096 | < 64 | <= control |
| Map with integer values | 4096 | >= 4096 | < 64 | <= control |

These verified bounds imply at least 98.46% fewer allocation requests for
these four fixed shapes. They are not throughput or latency percentages, and
do not characterize every nested-value shape. The independently implemented
checkpoint encoder checks complete wire equality after nested large/small
values and length boundaries at 127/128 and 16383/16384. Tests also cover empty
markers, decode roundtrips and dropping cached scalar capacity before descent.

## Qualification and limits

Existing owner: `//crates/storage:hawdb_storage_tests`. No new CI lane, target
profile, timeout or feature-matrix reduction is introduced. The ordinary native
allocation observer remains absent from production code and manual ordinary
benchmark binaries. No new instrumentation/control API is added.

The local machine is running other tasks. This iteration runs only the required
format/strict-Clippy checks and hooks, with one Cargo build job. CI owns runtime
regressions. No local release rebuild, throughput benchmark, tail-latency suite,
Bazel run or fuzz campaign is performed for this follow-up. Earlier timings in
[WAL encoding evidence](WAL_ENCODING.md) qualify their earlier source revisions.
Compiler-idle end-to-end performance and foreground tails remain unqualified;
this PR stays Draft and no universal no-regression claim is made.

Local pre-commit checks:

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -j 1 -- -D warnings
```

[Source-bound CI receipts](ci-wal-collections.json) record all nine Prow jobs
passing at runtime `744f90cfee1f5d7c8803235f6eefee9f51865b49`, integrated with
main `95f3dddb879231fb51b918bde5566ac45e91da2e`. Root/crates/crash clone records
agree on runner `f872442a8d2136e6af08ec550820400a37c4758f`; crash evidence
independently reports that source and ten ready recovery cases. Complete BEP
keeps executed and cached test results distinct. The storage unit owner executes
freshly in 226.546 s, including all new allocation/byte regressions. This duration
is execution evidence, not a CI speedup claim. Platform qualification, independent
approval and a later evidence-only head have separate check states.
