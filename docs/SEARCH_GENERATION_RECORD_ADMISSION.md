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

## Evidence and boundaries

Normal tests cover exact and one-byte-short limits, accumulated spool/logical
allowances, metadata admission, cancellation, arithmetic overflow, unchanged v1
bytes, unusual floating-point values, malformed hex, excess fields and failed
generation cleanup. A dedicated ignored 25,000-case campaign is available through
the mandatory local Bazel fuzz suite; it is not added to CI. See
`LEXICAL_FUZZ_CAMPAIGNS.md` for commands and acceptance oracles.

The test-only counter marks the output allocation boundary; it is not an allocator
or RSS measurement. The encoder's bound describes requested encoded capacity,
not already-owned input strings, allocator overhead, decoded metadata trees,
segment/compressor buffers, RaBitQ finalization or lexical merge state. This is a
prerequisite for shared build accounting, not a replacement for #206's aggregate
cross-phase build/query reservation or representative-corpus acceptance gates.
