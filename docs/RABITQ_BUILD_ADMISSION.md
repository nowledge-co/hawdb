# RaBitQ generation-sink admission

The fused generation's RaBitQ sink shares the existing three-account build root
with input, lexical and document-segment owners. Its private `Admission` keeps
state and directory capacity charged across calls; quantization and finalization
have additional phase leases. This does not add a cross-crate allocator API or
change vector quantization, segment parameters, artifact bytes or public APIs.

## Ownership and allocation order

- Reserve copied identity strings before constructing the projection config or
  calling `resource_admission()`: validation itself clones the identity. Two
  copies can overlap. Caller-owned configuration is not transferred to this lease.
- Reserve the existing component's pending-row, transformed-vector and packed
  buffers before entering `ProjectionWriter::create`. Retain the charge until
  the backend writer and its state have dropped, including failure paths.
- Admit scalar quantization scratch before each backend push. The caller's
  source document remains separately charged. The temporary lease stays live
  through a possible segment flush, then releases when the backend call returns.
- Admit the descriptor directory before any push that can flush, and before a
  partial final segment. Retain a conservative old/replacement growth allowance
  between calls; a per-call directory lease would release live backend storage.
- Admit finalization while state and directory remain live. The backend keeps
  its state and old manifest while constructing JSON and reopening the artifact.
  The phase lease covers that overlap and the reopened manifest until the sink
  releases its local `FileProjection` after checksum verification.
- Only a small artifact summary escapes `finish`. Its file-name allocation is
  admitted before formatting, and its lease moves with the summary through
  generation publication. Heap data fields drop before their leases.

Unexpected vector counts reject before the next backend push. Any failed push
poisons the sink, including quantization/directory denial and invalid vectors;
retry or finish cannot publish a partial prefix. Cancellation checkpoints remain
before create/push/finalize and after each backend push. The zero-vector path
does not create a backend. All errors retain ordinary staging cleanup semantics.

## Source-qualified envelopes

These formulas are based on the embedded `vector-projection` implementation;
recheck them when its data structures or manifest schema change. They measure
requested heap capacity, not allocator metadata, mappings, page cache or RSS.

`ProjectionBuildConfig::resource_admission()` supplies pending rows, transformed
f32s and packed codes plus its existing fixed allowance. Its component estimate
and persisted `peak_build_working_bytes` counter remain unchanged; that counter
is not the aggregate build-root peak or complete allocation telemetry.

Scalar quantization additionally needs an f64 absolute-value array. Four-bit
encoding also retains a usize code array and a `BinaryHeap<RescaleEvent>`, where
each event is `{ f64, usize }` (at most 16 bytes on supported 32/64-bit targets).
Heap length never exceeds dimension D. Admission is:

```text
one-bit scratch  = 8 * D
four-bit scratch = 8 * D + size_of(usize) * D + 3 * max(D, 4) * 16
directory       = 3 * max(segment_count, 4) * size_of(SegmentDescriptor)
```

The factor three covers geometrically grown old/replacement allocations and the
initial four slots. It is retained conservatively, not reduced to logical length.

Let I be the copied identity string byte count and S the final segment count.
The manifest's fixed fields fit in 4 KiB; each six-integer descriptor fits in
256 JSON bytes, including full-width integers and separators. String escaping
expands by at most six bytes per source byte:

```text
J = 4096 + 6 * I + 256 * S
finalization = 4 * J + directory(S) + 4 * max(I, 128) + 1024
```

This includes the writer's old/replacement JSON output growth (3J), reopened raw
JSON (J), parsed directory growth, string/parser scratch and mapping-handle
metadata. Existing state and old-manifest directory/identity leases remain live
alongside it. Arithmetic is checked before reservation. Tests also check actual
manifest length/capacity against the JSON envelope over varied identities.

The private writer reopen is bounded by the exact manifest length it just
serialized. A corrupted footer cannot turn that operation into a larger manifest
allocation, even when the forged length still fits inside the artifact. The
public `FileProjection::open` path is unchanged and remains separate serving-read
admission work; this private writer bound is not a general reader-budget claim.
The phase envelope assumes the immutable writer-produced manifest schema, not
arbitrary serving input with a separately repaired checksum.

## Verification

Normal regressions cover constructor/quantization/directory/finalize rejection
before backend entry, persistent state/directory/finalize snapshots, filename
ownership after caller drop, poisoned retries, missing vectors, arithmetic
overflow, and complete artifact parity with a direct projection-writer oracle.
The real fused writer's RaBitQ root denial preserves every old published file,
removes staging, releases all charges and still permits reopening/hydration.
The vector crate separately checks exact/one-short reopen extents and a damaged
footer, with rejection before the manifest read allocation.

The manual local `skein_search_rabitq_admission_fuzz_tests` target uses seed
`0x2064ab1` and 500 groups. It covers all 64 combinations of eight dimensions,
two bit widths and four segment-row limits, varying finite float bit patterns,
zero/missing vectors, identity escaping, epochs and transform seeds. Complete
artifact bytes match a direct writer. Every group retries its exact and one-short
shared-root budget, requires release to zero with three accounts, and retains
returned filename admission. Sixteen cancelled continuations cannot enter the
backend again. The campaign is ignored in ordinary tests and has no CI job.

Negative controls must detect missing state, quantization, directory or finalize
admission, early filename lease release and bypassing the private reopen bound.
Restore every control before positive verification. See `LEXICAL_FUZZ_CAMPAIGNS.md`
for the mandatory local suite.

## Remaining full-issue gates

Published-reader reopen and persistent query/delta owners still need their own
shared-budget boundaries. Small path/control/configuration allocations and
allocator/ledger overhead are not completely instrumented. Mapped payload pages
and RSS are not covered by these heap-capacity leases. Jieba's persistent HMM
workspace still needs the separate dependency-maintenance decision. Native
exact-head checks and measured representative-corpus posting-size acceptance
remain necessary before the full #206 PR, followed by #291 and #292.
