# Segment build and publication admission

The fused generation writer passes its existing `BuildMemory` through segment
descriptors, text/compressed buffers, retained layouts and publication JSON.
All of these owners share the same three accounts as input and lexical state;
there is no per-segment ledger or independent copy of the operation budget.

## Encoding and ownership

`build_io` measures output without allocating an intermediate string or JSON
document, checks the component limit, then reserves a fixed-capacity buffer
before allocation. Its writer refuses growth beyond the admitted byte count.
The returned buffer owns its lease until after the bytes drop. Checkpoints in
both passes reject cancellation; serialization must emit the measured length.

Document, metadata and vector bodies reuse direct hex/float/metadata writers.
Metadata and vector streams preserve dense vector ordinals independently of
document ordinals, including missing and empty embeddings. Checked arithmetic
rejects ordinal overflow. Compression continues through the existing zstd level
3 stream path; it does not change pledged size, frame parameters or input order.
The compressed buffer, header and final envelope admit their live overlap; the
source body remains independently charged throughout compression.

Descriptors reserve copied endpoint IDs, field-map entries, parsed values and
normalized value-set entries before allocation. Retained values stay charged
with the descriptor vector; transient parsing scratch drops after its values.
Duplicate normalized values release their temporary strings before their
charges. Descriptor and layout vectors reserve old plus replacement slots
before growth, retaining reusable capacity between segments.

`SegmentArtifactOutput` carries the layout's lease beyond builder drop, through
lexical finalization and publication. Publication serializes borrowed layout
and manifest bodies without cloning their trees. Copied manifest strings and
both JSON output buffers remain admitted together. Serialization/root-budget
failure occurs before the publication commit gate, keeping the prior generation
authoritative. After that gate, the existing manifest-last commit intentionally
finishes despite late cancellation. File checksum scans use a fixed 64 KiB stack
buffer, not an unaccounted growable heap buffer.

A failed segment flush poisons the builder: retry or finish cannot publish a
partially written prefix. Invalid source ordinals are still rejected before
mutation and can be corrected without poisoning an otherwise valid builder.

## Dependency envelopes

The compression allowance is 8 MiB plus the Rust stream writer's fixed 32 KiB
buffer. It is qualified for `zstd` 0.13.3 / `zstd-sys` 2.0.16+zstd.1.5.7,
level 3, one worker, no dictionary, LDM or external sequence producer. The
default parameters are windowLog 21, chainLog 16, hashLog 17 and 128 KiB blocks.
The native estimator includes context, tables, sequence/literal scratch, buffered
input/output and alignment. Twice its complete stream estimate fits inside the
8 MiB allowance, covering workspace replacement overlap conservatively.

A local macOS diagnostic linked the pinned archive and called its official
estimators (bytes): `ZSTD_estimateCStreamSize(3) = 3663377`,
`ZSTD_estimateCCtxSize(3) = 1303568`; two complete streams require 7326754 bytes,
below 8388608. This is not native Linux/Windows or allocator/RSS evidence.
The runtime rejects an unqualified zstd version, and the level is asserted at
compile time. Requalify the envelope and Rust wrapper on dependency upgrades.
No dependency or build configuration is changed by this implementation.

For pinned serde_json 1.0.150 label parsing, the existing JSON-list/CSV fallback
semantics remain unchanged. An input of length N has at most comma-count + 1
list elements, and decoded UTF-8 is no longer than its JSON representation.
Scratch admits `8 * max(N, 16) + 6 * element_bound * size_of(Cow<str>)`, covering
parser scratch, vector growth overlap, decoded strings and trimmed copies.
Normalization reserves `12 * max(value_bytes, 16)` alongside a conservative set
node allowance, then retains actual string capacity. Field nodes use a separate
conservative allowance. These are requested-capacity envelopes, not estimates
of allocator metadata or process RSS; review the pinned implementations when
their allocation behavior changes.

## Verification

Normal regressions compare the three bodies, descriptor and JSON envelopes
with independent preceding encoders. They cover Unicode/NUL, arbitrary float
representations, JSON/CSV labels and invalid-JSON fallback, duplicate values,
range summaries, 64-bit ordinals, exact/one-short budgets, old/new slot overlap,
before-allocation denial, output/layout lifetime and poisoned retries.
Compression parity crosses 32 KiB, 128 KiB block and 2 MiB window boundaries.
Actual publication root denial preserves every previous artifact, permits
reopen/hydration, removes staging and releases layout/output charges. Existing
fused byte-parity, sink-failure and pre/post-commit cancellation tests remain.

The manual `skein_search_segment_admission_fuzz_tests` target uses seed
`0x2065e67`: 1,000 document groups exercise 1,000 descriptors and 3,000 payloads.
Each group/stream checks exact and one-short shared-root budgets and release to
zero with three accounts. Thirty-two cancelled encodes must not enter zstd.
It is ignored in ordinary Rust tests and explicitly included in local fuzz,
never a default or dedicated CI fuzz job. See `LEXICAL_FUZZ_CAMPAIGNS.md`.

Negative controls must detect omitted output admission, early layout lease
release, missing list-parsing scratch and omitted compression workspace.
Restore all controls before the final positive verification.

## Remaining issue 206 gates

No persisted encoding is bumped or migrated here. Document and compressed
envelopes stay v1; the pre-existing descriptor header is preserved byte-for-byte.
There is no public API change, new backend, io_uring or Bazel settings override.

This is not complete operation memory admission. RaBitQ construction/finalization,
published-reader reopen, persistent outer query/delta owners, small path/control
allocations and ledger/allocator overhead still need their respective boundaries.
Jieba's persistent HMM workspace needs the separate dependency-maintenance
decision. Native exact-head checks and measured representative-corpus posting-size
acceptance remain necessary before the full #206 PR, followed by #291 and #292.
