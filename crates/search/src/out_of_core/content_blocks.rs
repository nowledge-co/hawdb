// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Block-framed content segments (`blocks1`, #1917).
//!
//! A `stream1` descriptor segment is one `HAWDB_COMPRESSED_V1` envelope: one
//! zstd stream behind one range checksum, so a reader must inflate and verify
//! the whole segment to release one document. A `blocks1` segment frames its
//! documents as binary records ([`records`]) in independently compressed and
//! checksummed blocks:
//!
//! ```text
//! segment := header (64) | index (index_len) | block[0] .. block[n-1]
//! header  := magic "HDCBLK01" | layout_version u16 = 2 | codec u8 = 1 (zstd)
//!            | flags u8 = 0 | dictionary_id u32 = 0 | block_count u32
//!            | document_count u32 | index_len u32 | uncompressed_len u64
//!            | reserved [u8; 28] = 0
//! entry   := compressed_len u32 | uncompressed_len u32 | compressed_crc32c u32
//!            | uncompressed_crc32c u32 | document_count u32
//!            | first_id_len u32 | last_id_len u32 | first_id | last_id
//! block   := one zstd frame of whole binary records, in id order
//! ```
//!
//! All integers are little-endian. Blocks follow the index contiguously, so
//! their offsets are implied and must tile the descriptor range exactly. The
//! descriptor's per-segment checksum is CRC32C(header || index): a digest over
//! every block digest. See `docs/SEARCH_CONTENT_BLOCKS.md`.

use super::*;
use crate::build_memory::decoder::BlockDecoder;
use hawdb_core::{StorageFormatError, SEARCH_PROJECTION_MANIFEST_FORMAT};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_integrity::Crc32cHasher;
use hawdb_storage::cache::{
    ContentDigest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheKey,
    SegmentCacheLease, StoreId,
};
use std::cell::RefCell;
use std::io::{self, BufRead, BufReader};

mod frames;
pub(super) mod records;

pub(super) const MAGIC: &[u8; 8] = b"HDCBLK01";
/// Layout 2 stores binary records ([`records`]). Layout 1 held text `doc`
/// lines and was never released; it is refused.
pub(super) const LAYOUT_VERSION: u16 = 2;
const CODEC_ZSTD: u8 = 1;
pub(super) const HEADER_BYTES: usize = 64;
const ENTRY_FIXED_BYTES: usize = 28;
/// Target uncompressed block size. A hit inflates and verifies about this
/// many bytes besides its own record; 16 KiB keeps that cost in the tens of
/// microseconds while zstd level 3 still sees several records of context.
/// A record larger than the target forms its own block.
pub(super) const BLOCK_TARGET_BYTES: u64 = 16 * 1024;
/// Blocks above this hold a single large record. They are written without a
/// declared content size (bounded zstd window) and are always streamed,
/// never buffered whole or cached.
pub(super) const MAX_BUFFERED_BLOCK_BYTES: u64 = 1024 * 1024;
/// Index bytes read together with the header in one positioned read.
const INDEX_PREFETCH_BYTES: u64 = 4 * 1024;

/// How a content artifact frames its descriptor segments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ContentEncoding {
    /// One `HAWDB_COMPRESSED_V1` envelope per segment.
    #[default]
    Stream1,
    /// Independently compressed and checksummed blocks behind an index.
    Blocks1,
}

impl ContentEncoding {
    pub(super) fn is_stream1(&self) -> bool {
        *self == Self::Stream1
    }
}

/// The encoding this release writes for every new content artifact.
pub(super) fn written_content_encoding() -> ContentEncoding {
    #[cfg(test)]
    if tests::WRITE_STREAM1.get() {
        // Reproduces artifacts written before #1917 for compatibility tests.
        return ContentEncoding::Stream1;
    }
    ContentEncoding::Blocks1
}

fn invalid(reason: impl std::fmt::Display) -> HawDBError {
    HawDBError::Storage(format!("search content blocks {reason}"))
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Header {
    pub(super) block_count: u32,
    pub(super) document_count: u32,
    pub(super) index_len: u32,
    pub(super) uncompressed_len: u64,
}

impl Header {
    pub(super) fn encode(&self) -> [u8; HEADER_BYTES] {
        let mut bytes = [0; HEADER_BYTES];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&LAYOUT_VERSION.to_le_bytes());
        bytes[10] = CODEC_ZSTD;
        bytes[16..20].copy_from_slice(&self.block_count.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.document_count.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.index_len.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        bytes
    }

    /// The declared index length, read only to bound the next read. Nothing
    /// in the header is trusted before the segment digest matches.
    fn declared_index_len(bytes: &[u8]) -> Result<u64> {
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid("header is missing"));
        }
        Ok(u64::from(u32_at(bytes, 24)))
    }

    /// Decodes a digest-verified header. Unknown values of the reserved
    /// fields are typed refusals: a newer writer may use them.
    fn decode(bytes: &[u8]) -> Result<Self> {
        let refuse = |feature: &'static str, tag: u64| {
            HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedFeature {
                format: SEARCH_PROJECTION_MANIFEST_FORMAT,
                feature,
                tag,
            })
        };
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid("header is missing"));
        }
        let version = u16::from_le_bytes([bytes[8], bytes[9]]);
        if version < LAYOUT_VERSION {
            return Err(invalid(format_args!(
                "layout version {version} was never released; rebuild the projection"
            )));
        }
        if version > LAYOUT_VERSION {
            return Err(HawDBError::UnsupportedStorageFormat(
                StorageFormatError::UnsupportedVersion {
                    format: SEARCH_PROJECTION_MANIFEST_FORMAT,
                    found: version,
                    supported: LAYOUT_VERSION,
                },
            ));
        }
        if bytes[10] != CODEC_ZSTD {
            return Err(refuse("search content block codec", u64::from(bytes[10])));
        }
        if bytes[11] != 0 {
            return Err(refuse("search content block flags", u64::from(bytes[11])));
        }
        let dictionary = u32_at(bytes, 12);
        if dictionary != 0 {
            return Err(refuse(
                "search content block dictionary",
                u64::from(dictionary),
            ));
        }
        if let Some(at) = bytes[36..HEADER_BYTES].iter().position(|byte| *byte != 0) {
            return Err(refuse(
                "search content block reserved field",
                (36 + at) as u64,
            ));
        }
        Ok(Self {
            block_count: u32_at(bytes, 16),
            document_count: u32_at(bytes, 20),
            index_len: u32_at(bytes, 24),
            uncompressed_len: u64::from_le_bytes(bytes[28..36].try_into().expect("eight bytes")),
        })
    }
}

/// One block as indexed. Offsets are relative to the segment start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) offset: u64,
    pub(super) compressed_len: u32,
    pub(super) uncompressed_len: u32,
    pub(super) compressed_crc: u32,
    pub(super) uncompressed_crc: u32,
    pub(super) document_count: u32,
    first: (usize, usize),
    last: (usize, usize),
}

impl Entry {
    pub(super) fn digest(&self) -> u64 {
        (u64::from(self.compressed_crc) << 32) | u64::from(self.uncompressed_crc)
    }
}

/// The writer's record of one finished block.
pub(super) struct WrittenBlock<'a> {
    pub(super) compressed_len: u32,
    pub(super) uncompressed_len: u32,
    pub(super) compressed_crc: u32,
    pub(super) uncompressed_crc: u32,
    pub(super) document_count: u32,
    pub(super) first: &'a str,
    pub(super) last: &'a str,
}

pub(super) fn entry_len(first: &str, last: &str) -> Result<usize> {
    ENTRY_FIXED_BYTES
        .checked_add(first.len())
        .and_then(|bytes| bytes.checked_add(last.len()))
        .ok_or_else(|| invalid("index entry size overflow"))
}

pub(super) fn encode_entry(output: &mut Vec<u8>, block: &WrittenBlock<'_>) -> Result<()> {
    let first = u32::try_from(block.first.len()).map_err(|_| invalid("document id is too long"))?;
    let last = u32::try_from(block.last.len()).map_err(|_| invalid("document id is too long"))?;
    for value in [
        block.compressed_len,
        block.uncompressed_len,
        block.compressed_crc,
        block.uncompressed_crc,
        block.document_count,
        first,
        last,
    ] {
        output.extend_from_slice(&value.to_le_bytes());
    }
    output.extend_from_slice(block.first.as_bytes());
    output.extend_from_slice(block.last.as_bytes());
    Ok(())
}

/// What the descriptor and reader admission require of one segment.
#[derive(Clone, Copy)]
pub(super) struct Expect<'a> {
    pub(super) length: u64,
    pub(super) checksum: u64,
    pub(super) max_uncompressed_bytes: u64,
    /// The descriptor's count and id bounds, when the caller holds them.
    pub(super) descriptor: Option<(usize, &'a str, &'a str)>,
}

/// A verified segment index over `header || index` bytes.
pub(super) struct Index<B> {
    bytes: B,
    header: Header,
    entries: Vec<Entry>,
    _memory: Option<QueryMemoryLease>,
}

impl<B: std::ops::Deref<Target = [u8]>> Index<B> {
    /// Verifies the segment digest, then the header and the complete block
    /// structure. Nothing is decoded from an index whose digest differs.
    pub(super) fn parse(
        bytes: B,
        expect: Expect<'_>,
        memory: Option<&QueryMemoryAccount>,
    ) -> Result<Self> {
        let mut digest = Crc32cHasher::new();
        digest.update(&bytes);
        if digest.finish() != expect.checksum {
            return Err(invalid("payload checksum mismatch"));
        }
        let header = Header::decode(&bytes)?;
        let index_len = usize::try_from(header.index_len).map_err(|_| invalid("index overflow"))?;
        if bytes.len() != HEADER_BYTES + index_len {
            return Err(invalid("index length mismatch"));
        }
        if header.block_count == 0 || header.document_count == 0 {
            return Err(invalid("segment is empty"));
        }
        if header.uncompressed_len > expect.max_uncompressed_bytes {
            return Err(invalid(format_args!(
                "uncompressed payload requires {} bytes, exceeding {}",
                header.uncompressed_len, expect.max_uncompressed_bytes
            )));
        }
        let count = header.block_count as usize;
        // Every entry occupies at least its fixed bytes; refuse a count the
        // verified index cannot hold before reserving for it.
        if count > index_len / ENTRY_FIXED_BYTES {
            return Err(invalid("block count exceeds its index"));
        }
        let memory = memory
            .map(|memory| memory.reserve(count.saturating_mul(std::mem::size_of::<Entry>())))
            .transpose()?;
        let mut entries = Vec::with_capacity(count);
        let mut at = HEADER_BYTES;
        let mut offset = (HEADER_BYTES + index_len) as u64;
        let (mut documents, mut uncompressed) = (0u64, 0u64);
        for _ in 0..count {
            if bytes.len() - at < ENTRY_FIXED_BYTES {
                return Err(invalid("index entry is truncated"));
            }
            let field = |index: usize| u32_at(&bytes, at + 4 * index);
            let first_len = field(5) as usize;
            let last_len = field(6) as usize;
            let first = (at + ENTRY_FIXED_BYTES, first_len);
            let last_start = first
                .0
                .checked_add(first_len)
                .ok_or_else(|| invalid("index entry overflow"))?;
            let end = last_start
                .checked_add(last_len)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| invalid("index entry is truncated"))?;
            let entry = Entry {
                offset,
                compressed_len: field(0),
                uncompressed_len: field(1),
                compressed_crc: field(2),
                uncompressed_crc: field(3),
                document_count: field(4),
                first,
                last: (last_start, last_len),
            };
            if entry.compressed_len == 0 || entry.uncompressed_len == 0 || entry.document_count == 0
            {
                return Err(invalid("block is empty"));
            }
            let first_id = std::str::from_utf8(&bytes[first.0..last_start])
                .map_err(|_| invalid("index id is not UTF-8"))?;
            let last_id = std::str::from_utf8(&bytes[last_start..end])
                .map_err(|_| invalid("index id is not UTF-8"))?;
            if first_id > last_id || (entry.document_count == 1 && first_id != last_id) {
                return Err(invalid("block id bounds are invalid"));
            }
            if let Some(previous) = entries.last() {
                let previous: &Entry = previous;
                let previous_last = &bytes[previous.last.0..previous.last.0 + previous.last.1];
                if previous_last >= first_id.as_bytes() {
                    return Err(invalid("block id ranges are not ordered"));
                }
            }
            offset = offset
                .checked_add(u64::from(entry.compressed_len))
                .ok_or_else(|| invalid("block offset overflow"))?;
            documents += u64::from(entry.document_count);
            uncompressed += u64::from(entry.uncompressed_len);
            entries.push(entry);
            at = end;
        }
        if at != bytes.len() {
            return Err(invalid("index has trailing bytes"));
        }
        if offset != expect.length {
            return Err(invalid("blocks do not tile the segment range"));
        }
        if documents != u64::from(header.document_count) || uncompressed != header.uncompressed_len
        {
            return Err(invalid("block totals disagree with the header"));
        }
        let index = Self {
            bytes,
            header,
            entries,
            _memory: memory,
        };
        if let Some((count, first, last)) = expect.descriptor
            && (count as u64 != documents
                || index.first(&index.entries[0]) != first
                || index.last(&index.entries[index.entries.len() - 1]) != last)
        {
            return Err(invalid("index does not match its descriptor"));
        }
        Ok(index)
    }

    pub(super) fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub(super) fn first(&self, entry: &Entry) -> &str {
        std::str::from_utf8(&self.bytes[entry.first.0..entry.first.0 + entry.first.1])
            .expect("validated index id")
    }

    pub(super) fn last(&self, entry: &Entry) -> &str {
        std::str::from_utf8(&self.bytes[entry.last.0..entry.last.0 + entry.last.1])
            .expect("validated index id")
    }

    /// The block whose id range holds `id`. An id outside every range is
    /// absent from the segment.
    pub(super) fn find(&self, id: &str) -> Option<usize> {
        self.entries
            .binary_search_by(|entry| {
                if self.last(entry) < id {
                    CmpOrdering::Less
                } else if self.first(entry) > id {
                    CmpOrdering::Greater
                } else {
                    CmpOrdering::Equal
                }
            })
            .ok()
    }

    fn into_bytes(self) -> (B, Header, Vec<Entry>, Option<QueryMemoryLease>) {
        (self.bytes, self.header, self.entries, self._memory)
    }
}

/// Bytes owned by the query or leased from the content cache.
pub(super) enum Held {
    Owned(Vec<u8>, Option<QueryMemoryLease>),
    Cached(SegmentCacheLease),
}

impl std::ops::Deref for Held {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes, _) => bytes,
            Self::Cached(lease) => lease,
        }
    }
}

/// The process-local cache of verified content indexes and blocks, keyed by
/// the payload artifact's SHA-256 so different files never alias.
#[derive(Debug, Clone)]
pub(super) struct ContentCache(Option<Arc<SegmentCache>>);

impl ContentCache {
    pub(super) fn new(capacity_bytes: u64) -> Self {
        Self((capacity_bytes != 0).then(|| Arc::new(SegmentCache::new(capacity_bytes))))
    }

    pub(super) fn snapshot(&self) -> Option<hawdb_storage::cache::SegmentCacheSnapshot> {
        self.0.as_ref().map(|cache| cache.snapshot())
    }
}

/// One artifact's identity in the content cache.
#[derive(Clone, Copy)]
pub(super) struct CacheScope<'a> {
    cache: &'a SegmentCache,
    store: StoreId,
    generation: ManifestGeneration,
    tag: [u8; 32],
}

impl<'a> CacheScope<'a> {
    pub(super) fn new(
        cache: &'a ContentCache,
        payload_sha256: [u8; 32],
        generation: u64,
    ) -> Option<Self> {
        let cache = cache.0.as_deref()?;
        Some(Self {
            cache,
            store: StoreId(u128::from_le_bytes(
                payload_sha256[..16].try_into().expect("sixteen bytes"),
            )),
            generation: ManifestGeneration(generation),
            tag: payload_sha256,
        })
    }

    fn key(&self, offset: u64, digest: u64, representation: RepresentationKind) -> SegmentCacheKey {
        SegmentCacheKey {
            store_id: self.store,
            manifest_generation: self.generation,
            segment_id: offset,
            content_digest: ContentDigest(digest),
            representation,
        }
    }

    fn get(&self, key: &SegmentCacheKey) -> Option<SegmentCacheLease> {
        self.cache.get_verified(key, self.tag)
    }

    /// Admits verified bytes; refused admission returns them for uncached use.
    fn insert(
        &self,
        key: SegmentCacheKey,
        bytes: Vec<u8>,
    ) -> std::result::Result<SegmentCacheLease, Vec<u8>> {
        self.cache
            .insert_verified(key, self.tag, bytes)
            .map_err(|error| error.into_parts().1)
    }
}

/// Positioned reads of one segment, relative to its start.
pub(super) trait SegmentSource {
    fn read_exact_at(&self, offset: u64, output: &mut [u8]) -> Result<()>;
}

/// Read counters for the selective path.
#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Reads {
    pub(super) range_reads: u64,
    pub(super) bytes_read: u64,
    pub(super) index_reads: u64,
    pub(super) index_cache_hits: u64,
    pub(super) blocks_read: u64,
    pub(super) block_cache_hits: u64,
    pub(super) bytes_inflated: u64,
}

impl Reads {
    pub(super) fn record(&self, metrics: &mut SearchOutOfCoreMetrics) {
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(self.range_reads);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(self.bytes_read);
        metrics.hydration_segment_bytes_read = metrics
            .hydration_segment_bytes_read
            .saturating_add(self.bytes_read);
        metrics.content_index_reads = metrics.content_index_reads.saturating_add(self.index_reads);
        metrics.content_index_cache_hits = metrics
            .content_index_cache_hits
            .saturating_add(self.index_cache_hits);
        metrics.content_blocks_read = metrics.content_blocks_read.saturating_add(self.blocks_read);
        metrics.content_block_cache_hits = metrics
            .content_block_cache_hits
            .saturating_add(self.block_cache_hits);
        metrics.content_block_bytes_inflated = metrics
            .content_block_bytes_inflated
            .saturating_add(self.bytes_inflated);
    }
}

fn reserve(memory: Option<&QueryMemoryAccount>, bytes: usize) -> Result<Option<QueryMemoryLease>> {
    memory.map(|memory| memory.reserve(bytes)).transpose()
}

/// Reads and verifies one segment's index, or leases it from the cache.
pub(super) fn load_index(
    source: &impl SegmentSource,
    cache: Option<CacheScope<'_>>,
    segment_offset: u64,
    expect: Expect<'_>,
    memory: Option<&QueryMemoryAccount>,
    reads: &mut Reads,
) -> Result<Index<Held>> {
    let key = cache.map(|cache| {
        (
            cache,
            cache.key(
                segment_offset,
                expect.checksum,
                RepresentationKind::SearchContentIndex,
            ),
        )
    });
    if let Some((cache, key)) = &key
        && let Some(lease) = cache.get(key)
    {
        reads.index_cache_hits += 1;
        return Index::parse(Held::Cached(lease), expect, memory);
    }
    let prefetch = expect
        .length
        .min(HEADER_BYTES as u64 + INDEX_PREFETCH_BYTES);
    if prefetch < HEADER_BYTES as u64 {
        return Err(invalid("segment is shorter than its header"));
    }
    let mut lease = reserve(memory, prefetch as usize)?;
    let mut bytes = vec![0; prefetch as usize];
    source.read_exact_at(0, &mut bytes)?;
    reads.range_reads += 1;
    reads.bytes_read += prefetch;
    let index_len = Header::declared_index_len(&bytes)?;
    if index_len > expect.max_uncompressed_bytes || HEADER_BYTES as u64 + index_len > expect.length
    {
        return Err(invalid("index exceeds its segment"));
    }
    let total = HEADER_BYTES + index_len as usize;
    if total > bytes.len() {
        let extra = total - bytes.len();
        if let Some(lease) = &mut lease {
            lease.grow(extra)?;
        }
        let start = bytes.len();
        bytes.resize(total, 0);
        source.read_exact_at(start as u64, &mut bytes[start..])?;
        reads.range_reads += 1;
        reads.bytes_read += extra as u64;
    } else {
        bytes.truncate(total);
    }
    reads.index_reads += 1;
    let index = Index::parse(Held::Owned(bytes, lease), expect, memory)?;
    let Some((cache, key)) = key else {
        return Ok(index);
    };
    let (held, header, entries, entry_memory) = index.into_bytes();
    let Held::Owned(bytes, lease) = held else {
        unreachable!("a freshly read index is owned")
    };
    let held = match cache.insert(key, bytes) {
        Ok(lease) => Held::Cached(lease),
        Err(bytes) => Held::Owned(bytes, lease),
    };
    Ok(Index {
        bytes: held,
        header,
        entries,
        _memory: entry_memory,
    })
}

/// Reads, verifies and inflates one buffered block, or leases it from the
/// cache. Only blocks up to [`MAX_BUFFERED_BLOCK_BYTES`] take this path.
pub(super) fn load_block(
    source: &impl SegmentSource,
    cache: Option<CacheScope<'_>>,
    segment_offset: u64,
    entry: &Entry,
    decoder: &mut BlockDecoder<'_>,
    memory: Option<&QueryMemoryAccount>,
    reads: &mut Reads,
) -> Result<Held> {
    debug_assert!(u64::from(entry.uncompressed_len) <= MAX_BUFFERED_BLOCK_BYTES);
    let key = cache.map(|cache| {
        (
            cache,
            cache.key(
                segment_offset + entry.offset,
                entry.digest(),
                RepresentationKind::SearchContentBlock,
            ),
        )
    });
    if let Some((cache, key)) = &key
        && let Some(lease) = cache.get(key)
    {
        reads.block_cache_hits += 1;
        return Ok(Held::Cached(lease));
    }
    let compressed_len = entry.compressed_len as usize;
    let uncompressed_len = entry.uncompressed_len as usize;
    let lease = reserve(memory, compressed_len.saturating_add(uncompressed_len))?;
    let mut compressed = vec![0; compressed_len];
    source.read_exact_at(entry.offset, &mut compressed)?;
    reads.range_reads += 1;
    reads.bytes_read += compressed_len as u64;
    let mut digest = Crc32cHasher::new();
    digest.update(&compressed);
    if digest.finish_u32() != entry.compressed_crc {
        return Err(invalid("block compressed checksum mismatch"));
    }
    // The same frame rule as a streamed run, so both accept the same index.
    frames::check_span(&compressed, entry.uncompressed_len.into())?;
    let mut body = vec![0; uncompressed_len];
    decoder.decode(&compressed, &mut body)?;
    drop(compressed);
    let mut digest = Crc32cHasher::new();
    digest.update(&body);
    if digest.finish_u32() != entry.uncompressed_crc {
        return Err(invalid("block checksum mismatch"));
    }
    reads.blocks_read += 1;
    reads.bytes_inflated += uncompressed_len as u64;
    #[cfg(test)]
    tests::record_block_inflated(uncompressed_len as u64);
    let Some((cache, key)) = key else {
        return Ok(Held::Owned(body, lease));
    };
    Ok(match cache.insert(key, body) {
        Ok(cached) => Held::Cached(cached),
        Err(body) => Held::Owned(body, lease),
    })
}

/// The first integrity failure a tap saw; it takes precedence over whatever
/// error the consumer derived from the failed read.
type Failure = RefCell<Option<HawDBError>>;

fn fail(failure: &Failure, error: HawDBError) -> io::Error {
    failure.borrow_mut().get_or_insert(error.clone());
    io::Error::other(error)
}

/// Verifies each block's compressed length and checksum as the decoder
/// consumes it, across a contiguous run of blocks.
struct CompressedTap<'e, R> {
    inner: R,
    entries: &'e [Entry],
    block: usize,
    remaining: u64,
    digest: Crc32cHasher,
    frame: frames::FrameWalk,
    failure: &'e Failure,
    count: u64,
}

impl<R: Read> Read for CompressedTap<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.block == self.entries.len() || output.is_empty() {
            return Ok(0);
        }
        let limit = self.remaining.min(output.len() as u64) as usize;
        let count = self.inner.read(&mut output[..limit])?;
        if count == 0 {
            return Err(fail(self.failure, invalid("block is truncated")));
        }
        self.digest.update(&output[..count]);
        self.count += count as u64;
        self.remaining -= count as u64;
        if self.remaining == 0 {
            let digest = std::mem::replace(&mut self.digest, Crc32cHasher::new());
            if digest.finish_u32() != self.entries[self.block].compressed_crc {
                return Err(fail(
                    self.failure,
                    invalid("block compressed checksum mismatch"),
                ));
            }
        }
        // Each span must hold exactly one frame of its block's length, as a
        // sparse read of that block requires.
        let framed = self
            .frame
            .feed(&output[..count])
            .and_then(|()| match self.remaining {
                0 => self.frame.finish(),
                _ => Ok(()),
            });
        if let Err(error) = framed {
            return Err(fail(self.failure, error));
        }
        if self.remaining == 0 {
            self.block += 1;
            if let Some(entry) = self.entries.get(self.block) {
                self.remaining = u64::from(entry.compressed_len);
                self.frame = frames::FrameWalk::new(u64::from(entry.uncompressed_len));
            }
        }
        Ok(count)
    }
}

/// Verifies each inflated block's length, checksum and record structure
/// against its index entry, across a contiguous run of blocks.
struct InflatedTap<'e, R, B> {
    inner: R,
    index: &'e Index<B>,
    blocks: std::ops::Range<usize>,
    block: usize,
    remaining: u64,
    digest: Crc32cHasher,
    lines: records::RecordWalk,
    failure: &'e Failure,
}

impl<'e, R: Read, B: std::ops::Deref<Target = [u8]>> InflatedTap<'e, R, B> {
    fn begin(&mut self) {
        if let Some(entry) = self
            .index
            .entries()
            .get(self.block)
            .filter(|_| self.block < self.blocks.end)
        {
            self.remaining = u64::from(entry.uncompressed_len);
            let limit = self
                .index
                .first(entry)
                .len()
                .max(self.index.last(entry).len());
            self.lines.start(limit);
        }
    }
}

impl<R: Read, B: std::ops::Deref<Target = [u8]>> Read for InflatedTap<'_, R, B> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.block == self.blocks.end {
            // The decoder must have nothing beyond the last indexed block.
            let mut extra = [0; 1];
            return match self.inner.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(fail(
                    self.failure,
                    invalid("blocks inflate past their index"),
                )),
            };
        }
        let limit = self.remaining.min(output.len() as u64) as usize;
        let count = self.inner.read(&mut output[..limit])?;
        if count == 0 {
            return Err(fail(
                self.failure,
                invalid("block inflates short of its index"),
            ));
        }
        let entry = self.index.entries()[self.block];
        let bytes = &output[..count];
        self.digest.update(bytes);
        self.lines.feed(bytes, self.index.first(&entry));
        self.remaining -= count as u64;
        if self.remaining == 0 {
            let digest = std::mem::replace(&mut self.digest, Crc32cHasher::new());
            if digest.finish_u32() != entry.uncompressed_crc {
                return Err(fail(self.failure, invalid("block checksum mismatch")));
            }
            if !self.lines.finish(&entry, self.index.last(&entry)) {
                return Err(fail(
                    self.failure,
                    invalid("block records disagree with its index"),
                ));
            }
            self.block += 1;
            self.begin();
        }
        Ok(count)
    }
}

/// Streams and verifies the blocks `blocks` of `index` from `input`, which is
/// positioned at the first of them, presenting their concatenated records
/// to `select`. Every block in the run is verified before this returns, even
/// when `select` stops early; a result is never released from a run with a
/// damaged block.
pub(super) fn stream_blocks<T, B: std::ops::Deref<Target = [u8]>>(
    input: impl Read,
    index: &Index<B>,
    blocks: std::ops::Range<usize>,
    admission: Option<super::hydration::DecodeAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    // The text re-encoder's buffer, besides the readers the caller admitted.
    let _text = admission
        .map(|admission| admission.memory.reserve(records::TEXT_RECORDS_BYTES + 8192))
        .transpose()?;
    let failure = Failure::default();
    let entries = &index.entries()[blocks.clone()];
    let expected_compressed: u64 = entries
        .iter()
        .map(|entry| u64::from(entry.compressed_len))
        .sum();
    let expected_inflated: u64 = entries
        .iter()
        .map(|entry| u64::from(entry.uncompressed_len))
        .sum();
    let mut compressed = CompressedTap {
        inner: input,
        entries,
        block: 0,
        remaining: entries
            .first()
            .map_or(0, |entry| u64::from(entry.compressed_len)),
        digest: Crc32cHasher::new(),
        frame: frames::FrameWalk::new(
            entries
                .first()
                .map_or(0, |entry| u64::from(entry.uncompressed_len)),
        ),
        failure: &failure,
        count: 0,
    };
    let compressed_input = BufReader::with_capacity(8192, &mut compressed);
    let run = |decoder: &mut dyn Read| -> (Result<T>, io::Result<u64>) {
        let mut inflated = InflatedTap {
            inner: decoder.take(expected_inflated.saturating_add(1)),
            index,
            blocks: blocks.clone(),
            block: blocks.start,
            remaining: 0,
            digest: Crc32cHasher::new(),
            lines: records::RecordWalk::default(),
            failure: &failure,
        };
        inflated.begin();
        let mut raw = BufReader::with_capacity(8192, &mut inflated);
        // Consumers read canonical text lines; the rest of the run is drained
        // (and verified by the taps) without re-encoding.
        let selected = select(&mut records::TextRecords::new(&mut raw));
        let drained = io::copy(&mut raw, &mut io::sink());
        drop(raw);
        if inflated.block != blocks.end && failure.borrow().is_none() {
            failure
                .borrow_mut()
                .replace(invalid("blocks end before their index"));
        }
        (selected, drained)
    };
    let (selected, drained) = match admission {
        Some(admission) => {
            let mut decoder = crate::build_memory::decoder::Decoder::with_account(
                compressed_input,
                admission.memory,
                admission.task,
            )?;
            run(&mut decoder)
        }
        None => {
            let mut decoder = hawdb_storage::compression::Decoder::with_buffer(compressed_input)?;
            run(&mut decoder)
        }
    };
    if let Some(admission) = admission {
        crate::build_control::checkpoint(admission.task)?;
    }
    if let Some(error) = failure.borrow_mut().take() {
        return Err(error);
    }
    if compressed.block != entries.len() || compressed.count != expected_compressed {
        return Err(invalid("blocks were not consumed exactly"));
    }
    if let Err(error) = drained {
        return selected.and_then(|_| Err(crate::build_memory::read::io_error(error)));
    }
    selected
}

/// Whole-segment read of a `blocks1` segment whose magic has already been
/// consumed from `input`: verifies the index, then streams and verifies every
/// block. Used by every consumer that needs the complete segment.
pub(super) fn read_segment<T>(
    mut input: impl Read,
    length: u64,
    checksum: u64,
    max_uncompressed_bytes: u64,
    admission: Option<super::hydration::DecodeAdmission<'_>>,
    select: impl FnOnce(&mut dyn BufRead) -> Result<T>,
) -> Result<T> {
    let mut header = [0; HEADER_BYTES];
    header[..8].copy_from_slice(MAGIC);
    input
        .read_exact(&mut header[8..])
        .map_err(|_| invalid("header is truncated"))?;
    let index_len = Header::declared_index_len(&header)?;
    if index_len > max_uncompressed_bytes || HEADER_BYTES as u64 + index_len > length {
        return Err(invalid("index exceeds its segment"));
    }
    let total = HEADER_BYTES + index_len as usize;
    let lease = reserve(admission.map(|admission| admission.memory), total)?;
    let mut bytes = vec![0; total];
    bytes[..HEADER_BYTES].copy_from_slice(&header);
    input
        .read_exact(&mut bytes[HEADER_BYTES..])
        .map_err(|_| invalid("index is truncated"))?;
    let index = Index::parse(
        Held::Owned(bytes, lease),
        Expect {
            length,
            checksum,
            max_uncompressed_bytes,
            descriptor: None,
        },
        admission.map(|admission| admission.memory),
    )?;
    let count = index.entries().len();
    stream_blocks(input, &index, 0..count, admission, select)
}

/// What [`SearchOutOfCoreReader::verify_content_segments`] verified.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchContentVerificationReport {
    /// Descriptor segments verified, in either framing.
    pub segments: u64,
    /// Of those, segments framed as content blocks (#1917).
    pub block_segments: u64,
    /// Records whose order, count and segment bounds were checked.
    pub documents: u64,
    /// Payload bytes read and verified.
    pub payload_bytes: u64,
}

impl SearchOutOfCoreReader {
    /// Deep verification of every content segment of this reader's pinned
    /// generation (#1917).
    ///
    /// Hit hydration verifies only the blocks it reads. This reads every
    /// segment once and verifies, for `blocks1`, the index digest and
    /// structure and every block's compressed and uncompressed checksum,
    /// length and record bounds, and for `stream1` the whole envelope, plus
    /// record order, count and descriptor bounds for both. Nothing is cached.
    /// The first failure is returned as `Storage`.
    pub fn verify_content_segments(
        &self,
        task: &hawdb_core::RuntimeTaskContext,
    ) -> Result<SearchContentVerificationReport> {
        let memory = crate::build_memory::BuildMemory::new(task)?;
        let mut report = SearchContentVerificationReport::default();
        for artifact in &self.segments {
            for segment in &artifact.descriptor.segments {
                crate::build_control::checkpoint(task)?;
                let range = segment
                    .payload_range
                    .ok_or_else(|| invalid("segment has no payload range"))?;
                if range
                    .offset
                    .checked_add(range.length)
                    .is_none_or(|end| end > artifact.payload_length)
                {
                    return Err(invalid("segment range exceeds its artifact"));
                }
                super::hydration::read_validated(
                    super::hydration::RangeReader {
                        file: &artifact.payload,
                        offset: range.offset,
                        remaining: range.length,
                    },
                    range.length,
                    range.checksum,
                    self.config.max_uncompressed_segment_bytes.get(),
                    Some(super::hydration::ReadAdmission {
                        memory: &memory,
                        task,
                        max_header_bytes: self.config.max_document_header_bytes.get(),
                    }),
                    |text| {
                        super::hydration::select_documents(
                            text,
                            super::hydration::Bounds::of(segment),
                            super::hydration::Wanted {
                                ids: &BTreeSet::new(),
                                embeddings: true,
                            },
                            super::hydration::HydrationBudget {
                                limit: u64::MAX,
                                used: 0,
                            },
                        )
                    },
                )?;
                report.segments += 1;
                if artifact.content_encoding == ContentEncoding::Blocks1 {
                    report.block_segments += 1;
                }
                report.documents += segment.document_count as u64;
                report.payload_bytes += range.length;
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
pub(super) mod tests;
