# Block-framed search content segments

Issue: `nowledge-co/hawdb#1917`. Design proposal: §9 of the per-family
artifact generations spec (this change implements its S1 and S2 phases for
content payloads only).

## Problem

An out-of-core content payload (`search_projection_segment_payloads.<g>.hawdb`)
holds descriptor segments of up to 128 documents. Before this change each
segment was one `HAWDB_COMPRESSED_V1` envelope: one zstd stream behind one
range checksum. To release one hit, hydration inflated and verified the whole
segment, so a top-100 query on BEIR SciFact inflated most of the corpus
(89–131 ms per query, against 1–13 ms of ranking).

## Format (`blocks1`)

```text
segment := header (64) | index (index_len) | block[0] .. block[n-1]
header  := magic "HDCBLK01" | layout_version u16 = 1 | codec u8 = 1 (zstd)
           | flags u8 = 0 | dictionary_id u32 = 0 | block_count u32
           | document_count u32 | index_len u32 | uncompressed_len u64
           | reserved [u8; 28] = 0
entry   := compressed_len u32 | uncompressed_len u32 | compressed_crc32c u32
           | uncompressed_crc32c u32 | document_count u32
           | first_id_len u32 | last_id_len u32 | first_id | last_id
block   := one zstd frame (level 3, no frame checksum) of whole `doc` records
```

- Integers are little-endian. Blocks follow the index contiguously; their
  offsets are implied and must tile the descriptor's segment range exactly.
- A block body holds the same `doc\t…` records as a `stream1` segment, without
  the `HAWDB_SEARCH_SEGMENT_V1` line. Records are in strictly increasing id
  order across the segment and are never split across blocks.
- The descriptor's per-segment `payload_range.checksum` is
  `CRC32C(header || index)`. The index holds every block's compressed and
  uncompressed CRC32C, so this is a digest over all block digests. The payload
  file as a whole stays bound by the selector's SHA-256 object reference.
- A block closes when the next record would take it past 16 KiB uncompressed
  (`BLOCK_TARGET_BYTES`). A record larger than that forms its own block. At
  16 KiB a hit inflates and verifies tens of microseconds of data besides its
  own record, while level 3 still sees several records of context; the
  measured payload growth is reported in the change that introduced it.
- Blocks up to 1 MiB declare their content size (a single-segment frame whose
  decode buffer is the block). Larger single-record blocks omit it, keeping
  zstd's bounded streaming window, and are always streamed, never buffered
  whole or cached.
- The index is read only after its digest matches. A nonzero dictionary id,
  flag or reserved byte, or another codec, is a typed
  `UnsupportedFeature { format: SEARCH_PROJECTION_MANIFEST_FORMAT, .. }`; a
  newer layout version is a typed `UnsupportedVersion`. These fields leave room
  for trained dictionaries or other codecs without a new layout.

The index sits after the header, not in a trailer. The writer knows every
block boundary before compressing (they follow from the records' admitted
encoded lengths), so it skips the header and index slots, compresses blocks
straight into the tail and fills the slots last, without the tail shift the
envelope writer needed. A sequential whole-segment reader then meets the index
before the blocks it describes, so every existing streaming consumer keeps one
forward pass over the range.

## Versioning and compatibility

- A content segment entry in the selector records its payload framing:
  `content_encoding: "blocks1"`. The field is omitted for `stream1`.
- Any selector that references a `blocks1` artifact has `min_reader_version`
  3 ([SEARCH_PROJECTION_MANIFEST_VERSION.md](SEARCH_PROJECTION_MANIFEST_VERSION.md)).
  A reader with the version gate and version 2 refuses it with
  `UnsupportedStorageFormat(UnsupportedVersion { format:
  SEARCH_PROJECTION_MANIFEST_FORMAT, found: 3, supported: 2 })` before it opens
  any artifact. Releases without the gate (0.31.0, 0.32.0) report `Storage`.
- This reader still reads `stream1` artifacts: existing projections keep
  working unchanged. Every new generation, delta, compaction output and rebuild
  writes `blocks1`, so `stream1` artifacts disappear as compaction proceeds. A
  selector whose artifacts are all `stream1` keeps its lower version.
- `PERSISTENT_FORMAT_VERSION` does not change: no existing database becomes
  unreadable. The metadata sidecar, the resident snapshot writer and the
  descriptor (`HAWDB_SEARCH_SEGMENTS_V3`) are unchanged.

## Reading

**Hit and by-id hydration** (`hydration::query`, `hydration.rs`) read only
what the requested ids need:

1. The header and index in one positioned read of up to 8 KiB (a second read
   only for a larger index), or a verified copy from the content cache. The
   digest is checked first, then the header and the structure: the blocks
   tile the range, id ranges are ordered and disjoint, and the counts and the
   first and last ids match the descriptor.
2. A binary search per requested id. An id outside every block range is not in
   the segment, as before.
3. For each needed block: one positioned read of its compressed bytes, the
   compressed CRC, a one-shot decode into an exact-size buffer with one reused
   admitted decoder context, the length and the uncompressed CRC. The block's
   records are then scanned like a segment, against the block's own count and
   id bounds, and only the requested records are decoded.

Integrity contract for `blocks1` hit hydration:

- Every released byte is verified by the chain selector → descriptor →
  segment digest → block CRCs.
- A failure in any block read for the request fails the request (`Storage`);
  no partial result is released.
- Blocks a request does not read are not verified by that request. This is the
  one deliberate narrowing of the `stream1` contract, matching the storage page
  model. Those blocks are verified by publication-time hashing, cleanup
  re-hashing, whole-segment consumers and explicit deep verification
  (`SearchOutOfCoreReader::verify_content_segments`).

**Whole-segment consumers** (compaction, mutation targets, verified and
selected bodies, projection scans, metadata-free streaming capture and deep
verification) are unchanged callers of `hydration::read_validated*`. That
reader dispatches on the segment magic, verifies the index digest and
structure, then streams every block once through the admitted decoder while
checking each block's compressed CRC, uncompressed length and CRC, and its
record count and first and last ids against its index entry. Their guarantee
is unchanged: nothing is released from a segment with any damaged block.

## Content cache

Each `SearchOutOfCoreReader` owns a `hawdb_storage::cache::SegmentCache` of
`SearchOutOfCoreConfig::content_cache_bytes` (16 MiB by default; zero
disables it), shared by its queries and carried across `refresh_shared`.

- Entries are verified decompressed blocks (`SearchContentBlock`) and verified
  `header || index` bytes (`SearchContentIndex`). They are admitted with
  `insert_verified` after every check above, tagged with the payload artifact's
  SHA-256; the store id is derived from that digest and the segment id is the
  physical offset, so different files and generations never alias.
- A hit skips the read, the CRCs and the decode. An index hit is re-parsed and
  its digest re-checked, which costs microseconds.
- Cache bytes belong to the cache's fixed capacity, not to a query. A query
  charges its transient buffers (compressed and inflated block, index, decoder
  context and buffers) to its `QueryMemoryAccount` and keeps its existing
  result admission. A lease pins its entry until the request drops it; a full
  or pinned cache refuses admission and the request keeps its owned copy.
- `SearchOutOfCoreReader::content_cache_snapshot` reports occupancy, hits,
  misses, evictions and refused admissions.

## Metrics

`SearchOutOfCoreMetrics` gains `content_index_reads`,
`content_index_cache_hits`, `content_blocks_read`, `content_block_cache_hits`
and `content_block_bytes_inflated`. For `blocks1` segments,
`segment_range_reads`, `segment_bytes_read` and
`hydration_segment_bytes_read` count the positioned reads and bytes actually
read, not the whole range.

## Remaining work

- Records still carry the decimal-text embedding that the vector sidecar also
  stores (spec Q9). Hit hydration of text-only queries leaves it unparsed
  (`DocumentTargetRequest::TextHits`, `decode_record`); vector and hybrid hits
  still parse it to rescore. Removing it from the records needs a vector
  sidecar join for whole-document reads and would shrink blocks several-fold.
- After this change, Documents hydration on BEIR is dominated by matched
  term/span analysis of each hit body and the per-hit lexical id probe, not by
  content reads. Matched terms and spans now take one analyzer pass per field,
  and `SearchHitHydration::Metadata` skips them and the document payload
  entirely (see [STORAGE.md](STORAGE.md), search hit hydration).
- Adjacent needed blocks are read one positioned read each; coalescing reads
  is left for measurement to justify.
- The metadata sidecar keeps the envelope framing.

## Tests

See `docs/TEST_INDEX.md` (search content blocks). Local replay:
`cargo test --locked -p hawdb-search --all-features --lib content_blocks`.
