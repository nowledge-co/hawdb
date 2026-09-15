# Delta input and ordered hydration ownership

The reviewed private #392 stage from PR518 follows PR515. Its main integration
uses PR513's qualified publication/cleanup implementation and current main.
Delta preparation now uses one task and
operation ledger through input conversion, base hydration, spool handoff,
publication and cleanup. Public query paths, reader policy/manifest snapshots,
identity and epoch checks, logical limits and persisted encoding are unchanged.
The approved context constructors remain private pending external acceptance.

## Input and handoff

After checking logical operation/working-byte limits, preparation admits the
original vectors, spare string/embedding capacity and metadata node allowance.
This pending input owner exists before copying the reader's embedding identity
or converting rows. Identity replacement admits the new copy while retaining the
old options payload. The original row conversion still defines ID construction
and metadata overwrite behavior; its possible allocation overlap is reserved
before the call. Exhausted source-vector storage is freed before releasing its
charge. In-place heapsort checks cancellation during each sift and retains the
original total ID order without auxiliary runs or a changing comparator.

Converted documents share their batch reservation. Moving a document into the
writer therefore needs no second input admission and cannot release its payload
owner prematurely. The writer separately owns retained IDs/field names after
spooling. Shared control blocks are freed before their reservation, including
the final document outliving the batch container. Prepared report strings retain
their own charge through finish; the existing owned public return is the explicit
handoff to the caller. No public report field or wrapper is added.

## Ordered base hydration

Only the private delta source changes. It reads bounded positioned ranges and
retains one encoded line, one admitted decoded document and the previous ID.
Line/header buffers admit replacement overlap before growing. The existing
snapshot parser has a preflight bound for its borrowed field-vector and small
seen-field tree; its grammar is reused. Document decoding uses the existing
admitted spool decoder, with no additional metadata-count cap on old input.
Blank/header lines, CRLF, final lines without a newline and accepted noncanonical
numeric spellings retain their existing meaning.

The entire range, compressed envelope and inflated length/checksum must pass
before successful preparation. A prefix or consumer error cannot hide a later
range read or checksum failure. Consumers write only an unpublished stage;
partial hydration, cancellation, a false length declaration or budget denial
cannot publish that stage. A declared inflated length permits at most one extra
probe byte. Reader-local uncompressed limits remain authoritative.

Source range counts and byte metrics still describe the same single range pass.
`peak_segment_document_bytes` now describes the peak logical decoded document
retained by this streaming source, instead of the old complete-segment sum.
It does not claim to measure encoded buffers, native capacity or process RSS.
The separate operation ledger governs those working allocations.

## Native decoder bound

Pinned zstd 1.5.7 has a fixed DCtx allocation and a retained native input/output
allocation. Before the decoder sees each frame header, the operation reserves
the workspace derived from that frame's window, block maximum and optional
content size. A conservative 256-KiB context allowance precedes construction;
native `sizeof` evidence also checks the actual retained context and buffers.
The default native window limit is preserved. No dictionary is loaded.

The frame preflight covers single-segment content sizes, unknown content sizes,
window mantissas, noncanonical zero dictionary IDs, concatenation and skippable
frames. Reserved or unrepresentable content sizes fail without a panic. Native
resizing frees the old input/output allocation before replacement; otherwise
the admission remains at the maximum retained requirement until decoder drop.
Requalify these bounds when upgrading the native dependency.

Sources: [the pinned frame format](https://github.com/facebook/zstd/blob/v1.5.7/doc/zstd_compression_format.md)
and [the pinned streaming decoder](https://github.com/facebook/zstd/blob/v1.5.7/lib/decompress/zstd_decompress.c).

## Verification and remaining scope

Regressions cover input spare capacity, raw-input admission before conversion,
row conversion and sorting parity, shared batch lifetime, report handoff,
exact/one-short native workspace, short reads/interruption, equivalent-header
range tampering, late corruption, cancellation/deadlines and callback pressure.
A 6-MiB operation streams a segment whose encoded text exceeds 12 MiB, with
requested Rust allocation and native-size evidence. Real-file corruption and
cancelled updates retain the old active generation and remove their stage.
The integration regression prepares an update before another consumer takes
ownership, then checks both the ordinary and context paths reject publication
for an active consumer lease or a newly registered plain/compressed binding.
Rejection preserves the active manifest, bound snapshot and complete old
documents, removes staging, and releases the operation ledger and publication
registration. This covers the composition of reviewed delta and current-main
consumer ownership; its resulting-source test receipt is a separate gate.

The independently built PR515 oracle compares repeated update/delete generations,
complete hydration, exact query results and retained artifact bytes under mixed
and forced-spill inputs. Separate source/target mutations and unchanged default
Bazel/local fuzz supplement default/minimal Cargo and strict Clippy.

This stage does not complete #392. Public context/facade acceptance, shared host
admission, adaptive large-source profiles and the original #206 full-corpus gate
remain separate. The 4-MiB source guard and existing error/report facade remain;
requested-capacity probes are not a process-wide RSS claim.
