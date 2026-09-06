# Generation input admission before allocation

The streaming generation writer checks metadata-field admission and the smallest
remaining record, logical-document and spool byte allowance before allocating
the encoded record. Exact sizing uses checked arithmetic for hex fields and a
counting `fmt::Write` for floating-point values. Rejection does not construct a
hex-expanded document or a vector of per-value strings.

After admission, the encoder requests one exact-capacity output buffer. The
shared v1 wire writer formats metadata and embeddings into that buffer and uses
a fixed 1 KiB hex scratch array. It does not allocate encoded field copies or
per-value strings. The ordinary encoder delegates to this same wire writer;
the tests preserve the preceding implementation as an independent wire oracle.
The format, source digest and persisted version are unchanged.

Segment admission asks for the exact length without encoding and discarding a
second record. Source spool scanning explicitly drops encoded bytes before
entering the lexical/vector/segment consumers. Metadata-field preflight borrows
keys and clones only newly admitted keys after a successful spool write.

Record decoding splits exactly six fields without allocating a field vector.
Hex validation rejects non-ASCII bytes before taking two-byte string slices;
valid UTF-8 text is not necessarily valid hex, and corruption must not trigger a
character-boundary panic. Invalid-record errors do not echo an arbitrarily large
input line. The real spool corruption regression repairs the frame checksum so
it reaches this decoder rather than stopping at the integrity gate.

## Shared input ownership

`create_with_context` uses the optional task memory reservation as the root of
one operation-local `QueryMemoryLedger`. Its three accounts are shared across
the writer, spool reader, segment document owner and retained/scratch lexical analysis
state (see `LEXICAL_ANALYZER_ADMISSION.md`). Accounts are not created
per document: the ledger retains account metadata until the build ends.

The input chain reserves capacity for:

- owned document string and embedding capacities, including unused capacity;
- encoded spool records, the 8 KiB writer/reader buffer and raw scan records;
- decoded strings, embeddings and metadata containers, before decoding;
- required and discovered metadata keys and retained ordering IDs;
- segment document slots and temporary descriptor input references.

Decoding counts borrowed wire fields before entering the allocating decoder.
Embedding allocation requests the exact number of floats. Metadata uses a
conservative 2 KiB per-entry allowance for tree nodes and transient splits, in
addition to string bytes; metadata-key sets use 1 KiB per entry. Duplicate keys
retain the conservative charge while decoding and release unused capacity after
the final document is known. Even empty metadata reserves one map-node allowance:
removing the last key can retain an allocated empty leaf root. These allowances
are not allocator instrumentation.

The raw record and decoded document overlap under the same root. After decoding,
raw bytes are dropped before the sinks run. The decoded document carries its
lease into the segment owner instead of releasing capacity at the callback
boundary. Clearing the segment drops its documents and their leases. Data fields
precede leases in declaration order so accounting is released after owned data.
Failed input, partial scans, cancellation and abandoned writers release charges
without publishing a partial generation. An exhausted root fails closed; it does
not retry against another account with an independent copy of the task budget.

Without a task reservation, the root uses the address-space maximum and existing
component caps still apply. This does not introduce a new default total budget.

## Evidence and boundaries

Normal tests cover exact and one-byte-short limits, accumulated spool/logical
allowances, metadata admission, cancellation, arithmetic overflow, unchanged v1
bytes, unusual floating-point values, malformed hex, excess fields and failed
generation cleanup. A dedicated ignored 25,000-case campaign is available through
the mandatory local Bazel fuzz suite; it is not added to CI. See
`LEXICAL_FUZZ_CAMPAIGNS.md` for commands and acceptance oracles.

The shared-input regressions also cover spare capacity, exact/one-short decode
admission, malformed and duplicate metadata, fixed account count over 10,000
documents, cross-thread ownership, raw/decoded overlap, actual segment retention,
successful finish and fused-scan exhaustion preserving the previous generation
and allowing a retry. The real partial-scan cancellation regression checks the
shared ledger is empty after cleanup. Temporary missing-reservation and early
release mutations must be rejected by the corresponding preflight/lifetime tests.

Test-only counters mark entry into output allocation and decoding; they are not
allocator or RSS measurements. The shared ledger includes the subsequent
Skein-owned analyzer, lexical artifact, external merge and dictionary owners;
their respective `LEXICAL_*_ADMISSION.md` documents define the boundaries.
Segment descriptors/layouts, encoding/compression and publication are covered in
`SEGMENT_BUILD_ADMISSION.md`. Jieba workspace, RaBitQ construction/finalization
and caller-owned delta conversion still need admission under the same root.
The outer query's concurrent
candidate/vector/score/hydration state is separate remaining work. Input accounting
does not replace #206's aggregate cross-phase build/query reservation or
representative-corpus acceptance gates.
