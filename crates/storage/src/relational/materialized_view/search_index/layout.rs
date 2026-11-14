// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Physical layout of a search index relation (layout tag 3).
//!
//! Every row has the primary key `(kind BIGINT, name BYTEA, slot BIGINT)` and
//! one `data BYTEA` payload of unsigned LEB128 varints:
//!
//! | kind | name | slot | data |
//! | --- | --- | --- | --- |
//! | 0 statistics | empty | 0 | documents, tokens, next ordinal |
//! | 1 document | encoded source primary key | 0 | ordinal |
//! | 2 lengths | empty | ordinal / 64 | slot count, then per slot: length (0 = no document), key length, key |
//! | 3 term dictionary | term | `i64::MAX` | df, sealed blocks, tail start, tail minimum length, tail postings |
//! | 3 sealed block | term | fence | count, first - fence, last - first, maximum frequency, postings, CRC32C |
//! | 4 block bounds | term | fence | last - fence, count, maximum frequency, minimum length, CRC32C |
//!
//! Documents get dense, never reused ordinals in commit order. A posting list
//! is a count followed by `(ordinal delta, term frequency)` pairs; the first
//! delta is relative to the list's base (a sealed block's fence or the
//! dictionary's tail start) and every later delta is at least 1. A term's
//! postings below its tail start live in sealed blocks keyed by fence; block
//! `f` holds the term's postings in `[f, next fence)`, and the first sealed
//! block's fence is 0. Postings at or above the tail start live inline in the
//! dictionary row, so a rare term is one row and an append rewrites one row;
//! a full tail is flushed into the last sealed block (topped up to 128) and
//! new blocks.
//!
//! Every sealed block has exactly one bounds row under the same fence. It is
//! the block's skip entry: a ranked scan reads the bounds rows of a term in
//! one range scan and decodes only the blocks whose BM25 upper bound can
//! still reach the current top-k. The maximum frequency is exact (it is
//! recomputed whenever the block is rewritten); the minimum length is a
//! conservative lower bound on the lengths of the block's documents (it can
//! only decrease while the block lives, so deletes never invalidate it, and
//! a document that becomes shorter lowers it). The dictionary keeps the same
//! conservative minimum for its tail.
//!
//! Sealed blocks and bounds rows both end in a CRC32C of their payload, so a
//! bound is verified before a ranked scan trusts it to skip a block, and a
//! block's own header (count, first and last ordinal, maximum frequency) is
//! checked against its postings and against its bounds row whenever the
//! block is read.

use super::*;

pub(super) const KIND_STATISTICS: i64 = 0;
pub(super) const KIND_DOCUMENT: i64 = 1;
pub(super) const KIND_LENGTHS: i64 = 2;
pub(super) const KIND_TERM: i64 = 3;
pub(super) const KIND_BOUNDS: i64 = 4;
/// The dictionary row sorts after every sealed block of its term.
pub(super) const DICTIONARY_SLOT: i64 = i64::MAX;
/// Sealed blocks are filled to this many postings.
pub const SEARCH_INDEX_BLOCK_POSTINGS: usize = 128;
/// The dictionary's inline tail is flushed into sealed blocks when it
/// reaches this many postings. Measured against 32: a smaller tail rewrites
/// fewer bytes per append but touches more rows and WAL key footprint.
pub const SEARCH_INDEX_TAIL_POSTINGS: usize = SEARCH_INDEX_BLOCK_POSTINGS;
/// A sealed block that grows past this many postings is split in two.
pub const SEARCH_INDEX_MAX_BLOCK_POSTINGS: usize = 2 * SEARCH_INDEX_BLOCK_POSTINGS;
/// A changed sealed block below this many postings absorbs its successor
/// when the two fit in one block.
pub(super) const MIN_BLOCK_POSTINGS: usize = SEARCH_INDEX_BLOCK_POSTINGS / 4;
/// Documents per lengths row.
pub const SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS: u64 = 64;
/// Ordinals stay below this bound so block numbers and fences fit `BIGINT`
/// and never reach the dictionary slot.
const MAX_ORDINAL: u64 = (i64::MAX as u64) - 1;

fn corrupt(what: &str) -> RelationalError {
    RelationalError::Corruption(format!("invalid search index {what}"))
}

pub(super) fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Strict reader over one row payload: canonical varints only, no trailing
/// bytes once the caller calls `finish`.
pub(super) struct Reader<'a> {
    bytes: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(super) fn new(bytes: &'a [u8], what: &'static str) -> Self {
        Self { bytes, what }
    }

    pub(super) fn varint(&mut self) -> Result<u64, RelationalError> {
        let mut value = 0u64;
        for index in 0..10 {
            let (&byte, rest) = self.bytes.split_first().ok_or_else(|| corrupt(self.what))?;
            self.bytes = rest;
            let payload = u64::from(byte & 0x7f);
            if index == 9 && payload > 1 {
                return Err(corrupt(self.what));
            }
            value |= payload << (7 * index);
            if byte & 0x80 == 0 {
                // Overlong encodings would give one value two spellings.
                if index > 0 && byte == 0 {
                    return Err(corrupt(self.what));
                }
                return Ok(value);
            }
        }
        Err(corrupt(self.what))
    }

    fn bytes(&mut self, len: u64) -> Result<&'a [u8], RelationalError> {
        let len = usize::try_from(len).map_err(|_| corrupt(self.what))?;
        if len > self.bytes.len() {
            return Err(corrupt(self.what));
        }
        let (bytes, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(bytes)
    }

    pub(super) fn finish(self) -> Result<(), RelationalError> {
        self.bytes
            .is_empty()
            .then_some(())
            .ok_or_else(|| corrupt(self.what))
    }
}

/// One posting: a document ordinal and its term frequency (at least 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchPosting {
    pub ordinal: u64,
    pub frequency: u64,
}

/// Bounds available without decoding a sealed block's postings. The checksum
/// covers both these bounds and the posting payload, so a block skipped by a
/// top-k query is still verified before its upper bound is trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchPostingBlockHeader {
    pub count: usize,
    pub first_ordinal: Option<u64>,
    pub last_ordinal: Option<u64>,
    pub max_frequency: u64,
}

pub(super) fn encode_postings(out: &mut Vec<u8>, base: u64, postings: &[SearchPosting]) {
    put_varint(out, postings.len() as u64);
    let mut previous = base;
    for posting in postings {
        put_varint(out, posting.ordinal - previous);
        put_varint(out, posting.frequency);
        previous = posting.ordinal;
    }
}

fn decode_postings(
    reader: &mut Reader<'_>,
    base: u64,
    upper: u64,
    out: &mut Vec<SearchPosting>,
) -> Result<(), RelationalError> {
    let count = reader.varint()?;
    // Each posting takes at least two bytes; refuse counts the payload
    // cannot hold before reserving for them.
    if count > (reader.bytes.len() / 2) as u64 {
        return Err(corrupt(reader.what));
    }
    out.reserve(count as usize);
    let mut previous = base;
    for index in 0..count {
        let delta = reader.varint()?;
        if index > 0 && delta == 0 {
            return Err(corrupt(reader.what));
        }
        let ordinal = previous
            .checked_add(delta)
            .filter(|ordinal| *ordinal < upper)
            .ok_or_else(|| corrupt(reader.what))?;
        let frequency = reader.varint()?;
        if frequency == 0 {
            return Err(corrupt(reader.what));
        }
        out.push(SearchPosting { ordinal, frequency });
        previous = ordinal;
    }
    Ok(())
}

/// Postings of one sealed block, whose fence is the base of its first delta.
pub fn decode_search_sealed_block(
    fence: u64,
    data: &[u8],
    out: &mut Vec<SearchPosting>,
) -> Result<(), RelationalError> {
    let header = decode_search_sealed_block_header(fence, data)?;
    let payload = &data[..data.len() - std::mem::size_of::<u32>()];
    let mut reader = Reader::new(payload, "sealed posting block");
    let count = reader.varint()?;
    if count != header.count as u64 {
        return Err(corrupt("sealed posting block"));
    }
    if count == 0 {
        return reader.finish();
    }
    let _first_delta = reader.varint()?;
    let _last_delta = reader.varint()?;
    let _max_frequency = reader.varint()?;
    let before = out.len();
    decode_posting_pairs(&mut reader, fence, MAX_ORDINAL + 1, count, out)?;
    reader.finish()?;
    let decoded = &out[before..];
    if decoded.first().map(|posting| posting.ordinal) != header.first_ordinal
        || decoded.last().map(|posting| posting.ordinal) != header.last_ordinal
        || decoded
            .iter()
            .map(|posting| posting.frequency)
            .max()
            .unwrap_or_default()
            != header.max_frequency
    {
        return Err(corrupt("sealed posting block bounds"));
    }
    Ok(())
}

pub(super) fn encode_sealed_block(fence: u64, postings: &[SearchPosting]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + postings.len() * 3);
    put_varint(&mut out, postings.len() as u64);
    if let (Some(first), Some(last)) = (postings.first(), postings.last()) {
        put_varint(&mut out, first.ordinal - fence);
        put_varint(&mut out, last.ordinal - first.ordinal);
        put_varint(
            &mut out,
            postings
                .iter()
                .map(|posting| posting.frequency)
                .max()
                .expect("non-empty postings"),
        );
        encode_posting_pairs(&mut out, fence, postings);
    }
    out.extend_from_slice(&hawdb_integrity::crc32c(&out).get().to_le_bytes());
    out
}

fn encode_posting_pairs(out: &mut Vec<u8>, base: u64, postings: &[SearchPosting]) {
    let mut previous = base;
    for posting in postings {
        put_varint(out, posting.ordinal - previous);
        put_varint(out, posting.frequency);
        previous = posting.ordinal;
    }
}

fn decode_posting_pairs(
    reader: &mut Reader<'_>,
    base: u64,
    upper: u64,
    count: u64,
    out: &mut Vec<SearchPosting>,
) -> Result<(), RelationalError> {
    if count > (reader.bytes.len() / 2) as u64 {
        return Err(corrupt(reader.what));
    }
    out.reserve(count as usize);
    let mut previous = base;
    for index in 0..count {
        let delta = reader.varint()?;
        if index > 0 && delta == 0 {
            return Err(corrupt(reader.what));
        }
        let ordinal = previous
            .checked_add(delta)
            .filter(|ordinal| *ordinal < upper)
            .ok_or_else(|| corrupt(reader.what))?;
        let frequency = reader.varint()?;
        if frequency == 0 {
            return Err(corrupt(reader.what));
        }
        out.push(SearchPosting { ordinal, frequency });
        previous = ordinal;
    }
    Ok(())
}

/// The payload of a row that ends in a CRC32C of that payload.
fn verify_checksum<'a>(data: &'a [u8], what: &'static str) -> Result<&'a [u8], RelationalError> {
    let checksum_at = data
        .len()
        .checked_sub(std::mem::size_of::<u32>())
        .ok_or_else(|| corrupt(what))?;
    let (payload, checksum) = data.split_at(checksum_at);
    let expected = u32::from_le_bytes(checksum.try_into().map_err(|_| corrupt(what))?);
    if hawdb_integrity::crc32c(payload).get() != expected {
        return Err(corrupt(what));
    }
    Ok(payload)
}

/// Verifies a sealed block and returns its safe skipping/scoring bounds without
/// materializing any postings.
pub fn decode_search_sealed_block_header(
    fence: u64,
    data: &[u8],
) -> Result<SearchPostingBlockHeader, RelationalError> {
    let payload = verify_checksum(data, "sealed posting block")?;
    let mut reader = Reader::new(payload, "sealed posting block");
    let count = reader.varint()?;
    let count = usize::try_from(count).map_err(|_| corrupt("sealed posting block"))?;
    if count > SEARCH_INDEX_MAX_BLOCK_POSTINGS {
        return Err(corrupt("sealed posting block count"));
    }
    if count == 0 {
        reader.finish()?;
        return Ok(SearchPostingBlockHeader {
            count,
            first_ordinal: None,
            last_ordinal: None,
            max_frequency: 0,
        });
    }
    let first = fence
        .checked_add(reader.varint()?)
        .filter(|ordinal| *ordinal <= MAX_ORDINAL)
        .ok_or_else(|| corrupt("sealed posting block bounds"))?;
    let last = first
        .checked_add(reader.varint()?)
        .filter(|ordinal| *ordinal <= MAX_ORDINAL)
        .ok_or_else(|| corrupt("sealed posting block bounds"))?;
    let max_frequency = reader.varint()?;
    if max_frequency == 0 || count > reader.bytes.len() / 2 {
        return Err(corrupt("sealed posting block bounds"));
    }
    Ok(SearchPostingBlockHeader {
        count,
        first_ordinal: Some(first),
        last_ordinal: Some(last),
        max_frequency,
    })
}

/// A term's dictionary entry: its document frequency, how many sealed blocks
/// hold its postings below `tail_start`, and the tail postings themselves.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchTermDictionary {
    pub document_frequency: u64,
    pub sealed_blocks: u64,
    pub tail_start: u64,
    /// At most the length of every document in the tail; 0 exactly when the
    /// tail is empty.
    pub tail_min_length: u64,
    pub tail: Vec<SearchPosting>,
}

impl SearchTermDictionary {
    pub fn decode(data: &[u8]) -> Result<Self, RelationalError> {
        let mut reader = Reader::new(data, "term dictionary");
        let document_frequency = reader.varint()?;
        let sealed_blocks = reader.varint()?;
        let tail_start = reader.varint()?;
        if tail_start > MAX_ORDINAL + 1 || (sealed_blocks == 0) != (tail_start == 0) {
            return Err(corrupt("term dictionary"));
        }
        let tail_min_length = reader.varint()?;
        let mut tail = Vec::new();
        decode_postings(&mut reader, tail_start, MAX_ORDINAL + 1, &mut tail)?;
        reader.finish()?;
        if (tail.len() as u64) > document_frequency
            || tail.len() >= SEARCH_INDEX_TAIL_POSTINGS
            || tail.is_empty() != (tail_min_length == 0)
        {
            return Err(corrupt("term dictionary"));
        }
        Ok(Self {
            document_frequency,
            sealed_blocks,
            tail_start,
            tail_min_length,
            tail,
        })
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.tail.len() * 3);
        put_varint(&mut out, self.document_frequency);
        put_varint(&mut out, self.sealed_blocks);
        put_varint(&mut out, self.tail_start);
        put_varint(
            &mut out,
            if self.tail.is_empty() {
                0
            } else {
                self.tail_min_length
            },
        );
        encode_postings(&mut out, self.tail_start, &self.tail);
        out
    }
}

/// The skip entry of one sealed block: its last posting's ordinal, how many
/// postings it holds, their exact maximum term frequency and a lower bound on
/// the lengths of their documents. An empty block (only the retained fence-0
/// block can be empty) is all zeros relative to its fence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SearchBlockBounds {
    pub last: u64,
    pub count: u64,
    pub max_frequency: u64,
    pub min_length: u64,
}

impl SearchBlockBounds {
    /// Exact bounds of `postings` with a conservative minimum length.
    pub(super) fn of(fence: u64, postings: &[SearchPosting], min_length: u64) -> Self {
        match postings.last() {
            None => Self {
                last: fence,
                ..Self::default()
            },
            Some(last) => Self {
                last: last.ordinal,
                count: postings.len() as u64,
                max_frequency: postings
                    .iter()
                    .map(|posting| posting.frequency)
                    .max()
                    .unwrap_or(0),
                min_length,
            },
        }
    }

    pub fn decode(fence: u64, data: &[u8]) -> Result<Self, RelationalError> {
        let data = verify_checksum(data, "block bounds")?;
        let mut reader = Reader::new(data, "block bounds");
        let span = reader.varint()?;
        let bounds = Self {
            last: fence
                .checked_add(span)
                .filter(|last| *last <= MAX_ORDINAL)
                .ok_or_else(|| corrupt("block bounds"))?,
            count: reader.varint()?,
            max_frequency: reader.varint()?,
            min_length: reader.varint()?,
        };
        reader.finish()?;
        let valid = if bounds.count == 0 {
            span == 0 && bounds.max_frequency == 0 && bounds.min_length == 0
        } else {
            bounds.count <= SEARCH_INDEX_MAX_BLOCK_POSTINGS as u64
                && bounds.count <= span.saturating_add(1)
                && bounds.max_frequency > 0
                && bounds.min_length > 0
        };
        valid
            .then_some(bounds)
            .ok_or_else(|| corrupt("block bounds"))
    }

    pub(super) fn encode(&self, fence: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        put_varint(&mut out, self.last - fence);
        put_varint(&mut out, self.count);
        put_varint(&mut out, self.max_frequency);
        put_varint(&mut out, self.min_length);
        out.extend_from_slice(&hawdb_integrity::crc32c(&out).get().to_le_bytes());
        out
    }

    /// Whether a sealed block's verified header agrees with these bounds.
    pub fn describes_header(&self, fence: u64, header: &SearchPostingBlockHeader) -> bool {
        self.count == header.count as u64
            && self.max_frequency == header.max_frequency
            && match (header.first_ordinal, header.last_ordinal) {
                (Some(first), Some(last)) => first >= fence && self.last == last,
                _ => self.last == fence,
            }
    }

    /// Whether `postings` decoded from the block at `fence` agree with these
    /// bounds: the same count and last ordinal and the same maximum
    /// frequency. Lengths are checked by the reader as documents are scored.
    pub fn describes(&self, fence: u64, postings: &[SearchPosting]) -> bool {
        let exact = Self::of(fence, postings, self.min_length);
        exact == *self && postings.first().is_none_or(|first| first.ordinal >= fence)
    }
}

/// Corpus statistics: live documents, their total token count and the next
/// unassigned ordinal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SearchIndexStatistics {
    pub documents: u64,
    pub tokens: u64,
    pub next_ordinal: u64,
}

impl SearchIndexStatistics {
    pub fn decode(data: &[u8]) -> Result<Self, RelationalError> {
        let mut reader = Reader::new(data, "statistics");
        let statistics = Self {
            documents: reader.varint()?,
            tokens: reader.varint()?,
            next_ordinal: reader.varint()?,
        };
        reader.finish()?;
        if statistics.documents > statistics.next_ordinal
            || statistics.documents > statistics.tokens
            || statistics.next_ordinal > MAX_ORDINAL + 1
        {
            return Err(corrupt("statistics"));
        }
        Ok(statistics)
    }

    pub(super) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12);
        put_varint(&mut out, self.documents);
        put_varint(&mut out, self.tokens);
        put_varint(&mut out, self.next_ordinal);
        out
    }

    pub(super) fn allocate_ordinal(&mut self) -> Result<u64, RelationalError> {
        let ordinal = self.next_ordinal;
        if ordinal > MAX_ORDINAL {
            return Err(RelationalError::Constraint(
                "search index document ordinals are exhausted".into(),
            ));
        }
        self.next_ordinal += 1;
        Ok(ordinal)
    }
}

/// One live document in a lengths block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDocumentEntry {
    pub length: u64,
    /// The source row's encoded primary key.
    pub key: Vec<u8>,
}

/// Lengths and source keys of up to 64 consecutive ordinals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchLengthBlock {
    pub entries: Vec<Option<SearchDocumentEntry>>,
}

impl SearchLengthBlock {
    pub fn decode(data: &[u8]) -> Result<Self, RelationalError> {
        let mut reader = Reader::new(data, "lengths block");
        let slots = reader.varint()?;
        if slots == 0 || slots > SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS {
            return Err(corrupt("lengths block"));
        }
        let mut entries = Vec::with_capacity(slots as usize);
        for _ in 0..slots {
            let length = reader.varint()?;
            entries.push(if length == 0 {
                None
            } else {
                let key_len = reader.varint()?;
                let key = reader.bytes(key_len)?;
                if key.is_empty() {
                    return Err(corrupt("lengths block"));
                }
                Some(SearchDocumentEntry {
                    length,
                    key: key.to_vec(),
                })
            });
        }
        reader.finish()?;
        if entries.last().is_some_and(Option::is_none) {
            return Err(corrupt("lengths block"));
        }
        Ok(Self { entries })
    }

    /// `None` when no slot is occupied: the row is then removed.
    pub(super) fn encode(&self) -> Option<Vec<u8>> {
        let slots = self.entries.iter().rposition(Option::is_some)? + 1;
        let mut out = Vec::new();
        put_varint(&mut out, slots as u64);
        for entry in &self.entries[..slots] {
            match entry {
                None => put_varint(&mut out, 0),
                Some(entry) => {
                    put_varint(&mut out, entry.length);
                    put_varint(&mut out, entry.key.len() as u64);
                    out.extend_from_slice(&entry.key);
                }
            }
        }
        Some(out)
    }

    pub fn get(&self, ordinal: u64) -> Option<&SearchDocumentEntry> {
        let slot = (ordinal % SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS) as usize;
        self.entries.get(slot).and_then(Option::as_ref)
    }

    pub(super) fn set(&mut self, ordinal: u64, entry: Option<SearchDocumentEntry>) {
        let slot = (ordinal % SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS) as usize;
        if self.entries.len() <= slot {
            self.entries.resize(slot + 1, None);
        }
        self.entries[slot] = entry;
    }
}

pub(super) fn decode_document_ordinal(data: &[u8]) -> Result<u64, RelationalError> {
    let mut reader = Reader::new(data, "document row");
    let ordinal = reader.varint()?;
    reader.finish()?;
    (ordinal <= MAX_ORDINAL)
        .then_some(ordinal)
        .ok_or_else(|| corrupt("document row"))
}

fn key(kind: i64, name: Vec<u8>, slot: i64) -> RelationalKey {
    RelationalKey(vec![
        RelationalValue::BigInt(kind),
        RelationalValue::Bytea(name),
        RelationalValue::BigInt(slot),
    ])
}

pub(super) fn slot_value(ordinal: u64) -> i64 {
    // Ordinals and block numbers never exceed MAX_ORDINAL.
    i64::try_from(ordinal).expect("bounded search index ordinal")
}

pub fn search_index_statistics_key() -> RelationalKey {
    key(KIND_STATISTICS, Vec::new(), 0)
}

pub(super) fn document_key(document: &[u8]) -> RelationalKey {
    key(KIND_DOCUMENT, document.to_vec(), 0)
}

/// The lengths row holding `ordinal`.
pub fn search_index_length_block_key(ordinal: u64) -> RelationalKey {
    key(
        KIND_LENGTHS,
        Vec::new(),
        slot_value(ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS),
    )
}

/// The lengths rows holding ordinals `first..=last`, in ordinal order.
pub fn search_index_length_block_range(
    first: u64,
    last: u64,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    let lower = RelationalValue::BigInt(slot_value(first / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS));
    let upper = RelationalValue::BigInt(slot_value(last / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS));
    RelationalPrimaryKeyRange::from_prefix_bounds(
        RelationalKey(vec![
            RelationalValue::BigInt(KIND_LENGTHS),
            RelationalValue::Bytea(Vec::new()),
        ]),
        Some((&lower, true)),
        Some((&upper, true)),
    )
}

/// The first ordinal covered by a lengths row read back from the relation.
pub fn search_index_length_block_start(slot: &RelationalValue) -> Result<u64, RelationalError> {
    match slot {
        RelationalValue::BigInt(slot) if *slot >= 0 => (*slot as u64)
            .checked_mul(SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS)
            .filter(|start| *start <= MAX_ORDINAL)
            .ok_or_else(|| corrupt("lengths key")),
        _ => Err(corrupt("lengths key")),
    }
}

pub fn search_index_dictionary_key(term: &str) -> RelationalKey {
    key(KIND_TERM, term.as_bytes().to_vec(), DICTIONARY_SLOT)
}

pub(super) fn sealed_block_key(term: &str, fence: u64) -> RelationalKey {
    key(KIND_TERM, term.as_bytes().to_vec(), slot_value(fence))
}

pub(super) fn bounds_key(term: &str, fence: u64) -> RelationalKey {
    key(KIND_BOUNDS, term.as_bytes().to_vec(), slot_value(fence))
}

/// Every block bounds row of `term`, in fence order.
pub fn search_index_block_bounds_range(
    term: &str,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    let lower = RelationalValue::BigInt(0);
    let upper = RelationalValue::BigInt(DICTIONARY_SLOT);
    RelationalPrimaryKeyRange::from_prefix_bounds(
        RelationalKey(vec![
            RelationalValue::BigInt(KIND_BOUNDS),
            RelationalValue::Bytea(term.as_bytes().to_vec()),
        ]),
        Some((&lower, true)),
        Some((&upper, false)),
    )
}

/// The sealed blocks of `term` with fences in `first..=last`, in fence order.
pub fn search_index_sealed_block_window(
    term: &str,
    first: u64,
    last: u64,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    sealed_block_range(term, (first, true), Some((last, true)))
}

/// Sealed blocks of `term` with fences in `[lower, upper)`.
pub(super) fn sealed_block_range(
    term: &str,
    lower: (u64, bool),
    upper: Option<(u64, bool)>,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    let lower_value = RelationalValue::BigInt(slot_value(lower.0));
    let upper_value =
        upper.map(|(fence, inclusive)| (RelationalValue::BigInt(slot_value(fence)), inclusive));
    let dictionary = RelationalValue::BigInt(DICTIONARY_SLOT);
    RelationalPrimaryKeyRange::from_prefix_bounds(
        RelationalKey(vec![
            RelationalValue::BigInt(KIND_TERM),
            RelationalValue::Bytea(term.as_bytes().to_vec()),
        ]),
        Some((&lower_value, lower.1)),
        Some(
            upper_value
                .as_ref()
                .map_or((&dictionary, false), |(value, inclusive)| {
                    (value, *inclusive)
                }),
        ),
    )
}

/// Every sealed block of `term`, in fence order.
pub fn search_index_sealed_block_range(
    term: &str,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    sealed_block_range(term, (0, true), None)
}

/// Sealed blocks of `term` after an optional fence, in fence order. A query
/// uses this to fetch one block at a time without retaining the posting list.
pub fn search_index_sealed_block_after_range(
    term: &str,
    after: Option<u64>,
) -> Result<RelationalPrimaryKeyRange, RelationalError> {
    sealed_block_range(term, after.map_or((0, true), |fence| (fence, false)), None)
}

/// The fence of a sealed block row read back from the index relation.
pub fn search_index_block_fence(slot: &RelationalValue) -> Result<u64, RelationalError> {
    match slot {
        RelationalValue::BigInt(slot) if (0..DICTIONARY_SLOT).contains(slot) => Ok(*slot as u64),
        _ => Err(corrupt("sealed block key")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(value: u64) -> usize {
        let mut out = Vec::new();
        put_varint(&mut out, value);
        let mut reader = Reader::new(&out, "test");
        assert_eq!(reader.varint().unwrap(), value);
        reader.finish().unwrap();
        out.len()
    }

    fn checksummed(mut payload: Vec<u8>) -> Vec<u8> {
        payload.extend_from_slice(&hawdb_integrity::crc32c(&payload).get().to_le_bytes());
        payload
    }

    #[test]
    fn varints_round_trip_at_every_width_and_refuse_bad_spellings() {
        assert_eq!(round_trip(0), 1);
        assert_eq!(round_trip(127), 1);
        assert_eq!(round_trip(128), 2);
        assert_eq!(round_trip(16_383), 2);
        assert_eq!(round_trip(16_384), 3);
        assert_eq!(round_trip(u64::from(u32::MAX)), 5);
        assert_eq!(round_trip(u64::MAX - 1), 10);
        assert_eq!(round_trip(u64::MAX), 10);
        for bad in [
            &[][..],
            &[0x80],                                                       // truncated
            &[0x80, 0x00],                                                 // overlong zero
            &[0xff, 0x00],                                                 // overlong 127
            &[0xff; 10],                                                   // eleventh byte needed
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02], // > u64::MAX
        ] {
            assert!(Reader::new(bad, "test").varint().is_err(), "{bad:?}");
        }
        let mut trailing = Reader::new(&[1, 2], "test");
        trailing.varint().unwrap();
        assert!(trailing.finish().is_err());
    }

    #[test]
    fn posting_blocks_round_trip_and_refuse_unordered_or_zero_frequencies() {
        let postings = [
            SearchPosting {
                ordinal: 1_000,
                frequency: 1,
            },
            SearchPosting {
                ordinal: 1_001,
                frequency: 300,
            },
            SearchPosting {
                ordinal: MAX_ORDINAL,
                frequency: u64::MAX,
            },
        ];
        let encoded = encode_sealed_block(1_000, &postings);
        let mut decoded = Vec::new();
        decode_search_sealed_block(1_000, &encoded, &mut decoded).unwrap();
        assert_eq!(decoded, postings);
        assert_eq!(
            decode_search_sealed_block_header(1_000, &encoded).unwrap(),
            SearchPostingBlockHeader {
                count: postings.len(),
                first_ordinal: Some(1_000),
                last_ordinal: Some(MAX_ORDINAL),
                max_frequency: u64::MAX,
            }
        );
        // An empty block (the retained fence-0 block) remains checksummed.
        let empty = encode_sealed_block(0, &[]);
        assert_eq!(empty.len(), 1 + std::mem::size_of::<u32>());
        assert_eq!(
            decode_search_sealed_block_header(0, &empty).unwrap(),
            SearchPostingBlockHeader {
                count: 0,
                first_ordinal: None,
                last_ordinal: None,
                max_frequency: 0,
            }
        );
        for bad in [vec![], vec![0], vec![0, 0, 0, 0]] {
            assert!(
                decode_search_sealed_block(0, &bad, &mut Vec::new()).is_err(),
                "{bad:?}"
            );
        }
        for bad in [
            // Repeated ordinal.
            checksummed(vec![2, 0, 0, 1, 0, 1, 0, 1]),
            // Zero frequency.
            checksummed(vec![1, 0, 0, 1, 0, 0]),
            // Header maximum disagrees with the selected payload.
            checksummed(vec![1, 0, 0, 1, 0, 2]),
            // Count cannot fit the remaining posting pairs.
            checksummed(vec![2, 0, 1, 1, 0, 1]),
            // A physical block cannot exceed its split bound.
            checksummed(vec![0x81, 0x02]),
            // Canonical trailing payload is refused even with a valid CRC.
            checksummed(vec![1, 0, 0, 1, 0, 1, 9]),
        ] {
            assert!(
                decode_search_sealed_block(0, &bad, &mut Vec::new()).is_err(),
                "{bad:?}"
            );
        }
        for offset in 0..encoded.len() {
            let mut bad = encoded.clone();
            bad[offset] ^= 1;
            assert!(decode_search_sealed_block(1_000, &bad, &mut Vec::new()).is_err());
        }
        // An ordinal past the bound is refused even with valid varints.
        let past = encode_sealed_block(
            MAX_ORDINAL,
            &[SearchPosting {
                ordinal: MAX_ORDINAL + 1,
                frequency: 1,
            }],
        );
        assert!(decode_search_sealed_block(MAX_ORDINAL, &past, &mut Vec::new()).is_err());
    }

    #[test]
    fn dictionary_statistics_and_lengths_round_trip() {
        let dictionary = SearchTermDictionary {
            document_frequency: 130,
            sealed_blocks: 1,
            tail_start: 128,
            tail_min_length: 5,
            tail: vec![
                SearchPosting {
                    ordinal: 128,
                    frequency: 2,
                },
                SearchPosting {
                    ordinal: 129,
                    frequency: 1,
                },
            ],
        };
        assert_eq!(
            SearchTermDictionary::decode(&dictionary.encode()).unwrap(),
            dictionary
        );
        // Sealed blocks and a zero tail start contradict each other.
        let inconsistent = SearchTermDictionary {
            sealed_blocks: 1,
            tail_start: 0,
            ..dictionary.clone()
        };
        assert!(SearchTermDictionary::decode(&inconsistent.encode()).is_err());
        // A tail without a length bound, or a bound without a tail, is refused.
        let spelled = |tail_min_length: u64, tail: &[SearchPosting]| {
            let mut out = Vec::new();
            put_varint(&mut out, 130);
            put_varint(&mut out, 1);
            put_varint(&mut out, 128);
            put_varint(&mut out, tail_min_length);
            encode_postings(&mut out, 128, tail);
            out
        };
        assert_eq!(spelled(5, &dictionary.tail), dictionary.encode());
        assert!(SearchTermDictionary::decode(&spelled(0, &dictionary.tail)).is_err());
        assert!(SearchTermDictionary::decode(&spelled(5, &[])).is_err());
        let empty = SearchTermDictionary {
            tail: Vec::new(),
            ..dictionary.clone()
        };
        assert_eq!(
            empty.encode(),
            spelled(0, &[]),
            "an empty tail encodes no bound"
        );
        assert_eq!(
            SearchTermDictionary::decode(&empty.encode()).unwrap(),
            SearchTermDictionary {
                tail_min_length: 0,
                ..empty
            }
        );
        let statistics = SearchIndexStatistics {
            documents: 3,
            tokens: 40,
            next_ordinal: 9,
        };
        assert_eq!(
            SearchIndexStatistics::decode(&statistics.encode()).unwrap(),
            statistics
        );
        let mut block = SearchLengthBlock::default();
        block.set(
            70,
            Some(SearchDocumentEntry {
                length: 12,
                key: vec![1, 2, 3],
            }),
        );
        block.set(65, None);
        let encoded = block.encode().unwrap();
        let decoded = SearchLengthBlock::decode(&encoded).unwrap();
        assert_eq!(decoded.get(70).unwrap().length, 12);
        assert!(decoded.get(64).is_none());
        block.set(70, None);
        assert_eq!(block.encode(), None);
        assert_eq!(
            search_index_length_block_key(63),
            search_index_length_block_key(0)
        );
        assert_ne!(
            search_index_length_block_key(64),
            search_index_length_block_key(63)
        );
    }

    #[test]
    fn block_bounds_round_trip_describe_their_block_and_refuse_bad_spellings() {
        let postings = [
            SearchPosting {
                ordinal: 1_003,
                frequency: 2,
            },
            SearchPosting {
                ordinal: 1_010,
                frequency: 7,
            },
            SearchPosting {
                ordinal: 1_200,
                frequency: 1,
            },
        ];
        let bounds = SearchBlockBounds::of(1_000, &postings, 9);
        assert_eq!(
            bounds,
            SearchBlockBounds {
                last: 1_200,
                count: 3,
                max_frequency: 7,
                min_length: 9,
            }
        );
        let encoded = bounds.encode(1_000);
        assert_eq!(SearchBlockBounds::decode(1_000, &encoded).unwrap(), bounds);
        assert!(bounds.describes(1_000, &postings));
        assert!(!bounds.describes(1_000, &postings[..2]));
        let mut raised = postings;
        raised[2].frequency = 8;
        assert!(!bounds.describes(1_000, &raised), "max frequency is exact");
        // The retained empty block is all zeros relative to its fence.
        let empty = SearchBlockBounds::of(0, &[], 0);
        assert_eq!(empty.encode(0), checksummed(vec![0, 0, 0, 0]));
        assert_eq!(
            SearchBlockBounds::decode(0, &checksummed(vec![0, 0, 0, 0])).unwrap(),
            empty
        );
        // Every bounds row is checksummed: a flipped bit is refused.
        for offset in 0..encoded.len() {
            let mut bad = encoded.clone();
            bad[offset] ^= 1;
            assert!(SearchBlockBounds::decode(1_000, &bad).is_err());
        }
        assert!(SearchBlockBounds::decode(0, &[0, 0, 0, 0]).is_err());
        // The bounds describe the block's own verified header.
        let block = encode_sealed_block(1_000, &postings);
        let header = decode_search_sealed_block_header(1_000, &block).unwrap();
        assert!(bounds.describes_header(1_000, &header));
        assert!(!SearchBlockBounds { count: 2, ..bounds }.describes_header(1_000, &header));
        assert!(!SearchBlockBounds {
            max_frequency: 8,
            ..bounds
        }
        .describes_header(1_000, &header));
        let empty_header =
            decode_search_sealed_block_header(0, &encode_sealed_block(0, &[])).unwrap();
        assert!(empty.describes_header(0, &empty_header));
        assert!(!bounds.describes_header(1_000, &empty_header));
        assert!(empty.describes(0, &[]));
        for bad in [
            vec![0, 0, 1, 0],               // empty block with a frequency
            vec![0, 0, 0, 3],               // empty block with a length
            vec![4, 0, 0, 0],               // empty block with a span
            vec![1, 3, 1, 1],               // more postings than ordinals in the span
            vec![9, 2, 0, 1],               // zero maximum frequency
            vec![9, 2, 1, 0],               // zero minimum length
            vec![9, 2, 1, 1, 0],            // trailing bytes
            vec![0xff, 0x7f, 255, 3, 1, 1], // more than a block can hold
        ] {
            assert!(
                SearchBlockBounds::decode(0, &checksummed(bad.clone())).is_err(),
                "{bad:?}"
            );
        }
        let mut past = Vec::new();
        put_varint(&mut past, 2);
        put_varint(&mut past, 1);
        put_varint(&mut past, 1);
        put_varint(&mut past, 1);
        assert!(SearchBlockBounds::decode(MAX_ORDINAL, &checksummed(past)).is_err());
    }
}
