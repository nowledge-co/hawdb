# Bounded generation-spool encoding

The private generation spool keeps the existing `SKNSPOL1` format: length and
CRC32C precede each text-encoded document record. Spooling now uses the same wire
grammar for checked sizing, bounded streaming and the existing materializing
encoder. The streamed path encodes hexadecimal fields in a fixed 8192-byte stack
buffer instead of allocating a full encoded record alongside the owned source.

All record, cumulative logical/spool and metadata admission remains before any
encoding pass or frame write. The stream also enforces its precomputed length
before each write and verifies that the complete record filled that length.
Formatting preserves the original I/O error rather than replacing it with a
generic formatting failure. Short writes, interrupted writes and zero progress
follow the standard portable `Write::write_all` behavior.

## Checksum and publication

An admitted record is first streamed into the frame/document-stream checksums,
then emitted after its unchanged header. The immutable borrow prevents source
changes between those passes. This trades two encoding traversals for bounded
scratch and sequential writes, avoiding a seek/flush pair per document. It is a
memory improvement, not a measured throughput-speedup claim. A future one-pass
implementation must retain the wire/error contract and justify its I/O tradeoff.

The document-stream digest commits only after the complete frame was accepted
by the writer. Input failures retain the existing poisoned-writer behavior.
Buffered writes can still fail during `finish`; failed flush, validation or
publication cannot replace the previous active generation. No per-record flush,
seek, platform-specific I/O or io_uring is introduced.

## Verification

### Read-side decoding

Spool scanning decodes admitted frames with a fixed 8192-byte input buffer and
incremental CRC32C. Hex fields are decoded directly into their owned output;
embedding values are parsed one token at a time. Reads never consume bytes past
the admitted logical frame, even when the outer buffered file reader prefetches.
Length admission, full-frame checksum validation and strict document ordering
precede each consumer callback. A syntax error still drains/checks the admitted
frame, so it cannot hide a checksum mismatch or a later truncated/failed read.
Invalid hex/UTF-8 returns a storage error, not a byte-offset string-slicing panic.

Valid records retain the materializing decoder's semantics: exact field count,
optional single final newline, lowercase/uppercase hex and the old two-byte
radix parser's leading-plus forms, empty embedding as None, f32 parsing, empty
metadata keys/values and duplicate-key last-wins behavior. No new token limit is
imposed on noncanonical numeric spellings. A single long numeric token remains
a resident temporary, bounded by existing frame admission. Decoded field/vector
capacity and metadata map overhead also remain resident; the fixed input buffer
does not represent total decoder memory.

The old materializing decoder remains an independent valid-input oracle.
Read-side regressions cover every short-record split/truncation, interrupted and
short reads, all small-record I/O failure boundaries, malformed fields with valid
checksums, admission at exact/one-short sizes, oversized length headers, no
next-frame consumption, no invalid-row callback, and active-generation retention.
The explicit 512-case local decoding campaign adds Unicode and buffer-boundary
inputs, f32 bit patterns, ASCII grammar mutations, invalid UTF-8 and injected
I/O errors. Whole-generation tests retain complete artifact, update/delete,
reopen and hydration coverage. Bounded read requests are observed on the real
spool reader; they are not a whole-process allocation measurement.

### Write-side encoding

The pre-existing independent legacy encoder remains the byte oracle, including
its 1024 seeded cases, Unicode, separators, metadata and floating-point edges.
New checks cover bounded output chunks, materialization-attempt counters,
internal length drift, every small-frame failure boundary, short/interrupted
writes, unchanged stream identity on error/unwind, complete public spool bytes,
generation digests, reopen/hydration and old-generation retention after immediate
write or deferred flush failures. Existing exact/one-short admission checks now
also prove that rejected records never enter the streaming encoder.

The explicit 256-case spool campaign varies fields around physical hex-buffer
boundaries, checks complete frames and concatenated stream digests, and replays
short writes and random failure positions. It is registered only in the existing
local Bazel fuzz suite, not CI.

```sh
cargo test -p skein-search --all-features -- --include-ignored
cargo clippy -p skein-search --all-features --all-targets -- -D warnings
bazel test --nocache_test_results \
  //crates/search:presubmit_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

## Remaining #392 scope

Spool writes and reads no longer retain a complete encoded record. This does not
remove the owned `SearchDocument`, decoded fields and collection capacity,
single-number parser scratch, other artifact encoders/decoders, analyzer
scratch, mini-delta state or host-owned memory. The scratch
and attempt checks are not a whole-process allocator/RSS proof. The existing
`peak_record_bytes` report still means the largest logical encoded record, not
resident memory. Public API and persisted format contracts are unchanged.

The 4 MiB source default remains until replacement safeguards cover the complete
declared lifecycle. Shared resource governance, adaptive profiles, streaming
source and other decoder work and cancellation remain tracked by #392/#186. #325's
long-token policy and #206's complete-corpus qualification remain independent.
