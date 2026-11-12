// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The search index access method: posting cursors, document lengths and
//! ranked rounds over one search index relation.
//!
//! This is the only code in the query layer that knows the physical layout
//! (`hawdb_storage::relational::search_index_*`). Everything above it sees
//! [`Bm25PostingCursor`]s and [`Bm25Documents`], and the shared
//! [`block_max_wand`] kernel ranks them. Every row is read through one row
//! runtime, so the scan observes exactly the caller's snapshot.

use super::super::*;
use hawdb_executor::bm25::{
    block_max_wand, idf, term_upper_bound, Bm25Block, Bm25Documents, Bm25Hit, Bm25PostingCursor,
    Bm25Term,
};
use hawdb_storage::relational::{
    decode_search_sealed_block, search_index_block_bounds_range, search_index_block_fence,
    search_index_dictionary_key, search_index_length_block_range, search_index_length_block_start,
    search_index_sealed_block_window, search_index_statistics_key, SearchBlockBounds,
    SearchIndexStatistics, SearchLengthBlock, SearchPosting, SearchTermDictionary,
    SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

/// Every range read has a fixed cost that dominates small reads on a
/// snapshot with a large live overlay, so reads are windows: a cursor reads
/// this many competitive blocks at once, doubling up to the maximum while it
/// decodes consecutive blocks.
const MIN_BLOCK_READAHEAD: usize = 4;
const MAX_BLOCK_READAHEAD: usize = 64;
/// Lengths rows (64 documents each) per window: scored documents can be
/// scattered (a rare term), so one window covers 8,192 ordinals.
const LENGTHS_WINDOW: u64 = 128;

fn integrity(error: hawdb_storage::relational::RelationalError) -> HawDBError {
    HawDBError::StorageIntegrity(error.to_string())
}

fn corrupt(message: &str) -> HawDBError {
    HawDBError::StorageIntegrity(format!("search index {message}"))
}

fn data(row: &RelationalReadRow) -> Result<&[u8]> {
    match row.value(3)? {
        RelationalValue::Bytea(data) => Ok(data),
        _ => Err(corrupt("row has an invalid payload")),
    }
}

/// Work the scan did, summed over its rounds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndexScanEvidence {
    pub index: String,
    pub table: String,
    /// Distinct analyzed query terms, and how many occur in the index.
    pub query_terms: usize,
    pub matched_terms: usize,
    /// Accepted rows the scan had to produce (`top_k`, or `OFFSET + LIMIT`
    /// when the output is in rank order).
    pub needed: usize,
    pub limit_pushdown: bool,
    /// Ranking rounds: 1 without a residual filter; each further round
    /// quadrupled the ranked prefix because hydrated candidates failed WHERE.
    pub rounds: usize,
    /// The size of the last round's ranked prefix, and how many documents
    /// it ranked (fewer when the match set is smaller).
    pub final_k: usize,
    pub ranked_rows: usize,
    /// Postings of the queried terms (their summed document frequencies).
    pub postings_total: u64,
    /// Postings decoded, summed over rounds.
    pub postings_visited: u64,
    /// Sealed blocks of the queried terms, per round.
    pub blocks_total: u64,
    /// Sealed blocks read from storage (decoded or read ahead).
    pub blocks_read: u64,
    pub blocks_decoded: u64,
    /// Sealed blocks never read, summed over rounds.
    pub blocks_skipped: u64,
    /// Pivots whose block bounds fell below the top-k floor.
    pub block_skips: u64,
    pub documents_scored: u64,
    pub lengths_rows_read: u64,
    /// Candidates hydrated from the source table and checked against WHERE.
    pub candidates_hydrated: usize,
    /// Ranking stopped early because nothing left could reach the floor.
    pub terminated_early: bool,
}

/// Shared scan state: the index runtime, work and memory admission.
pub(super) struct ScanContext<'a> {
    pub runtime: &'a RelationalRowRuntime<'a>,
    pub index: &'a str,
    pub task: Option<&'a hawdb_core::RuntimeTaskContext>,
    pub max_work: usize,
    pub work: Cell<usize>,
    pub memory: RefCell<QueryMemoryLease>,
    pub evidence: RefCell<SearchIndexScanEvidence>,
}

impl ScanContext<'_> {
    pub(super) fn charge(&self, amount: u64) -> Result<()> {
        let amount = usize::try_from(amount).unwrap_or(usize::MAX);
        let work = self.work.get().saturating_add(amount);
        self.work.set(work);
        if work > self.max_work {
            return Err(HawDBError::read_budget_exceeded(
                hawdb_core::ReadBudgetResource::Rows,
                self.max_work,
                "text_search exceeds SQL candidate work admission",
            ));
        }
        Ok(())
    }

    pub(super) fn admit(&self, bytes: usize) -> Result<()> {
        self.memory.borrow_mut().grow(bytes)
    }

    fn release(&self, bytes: usize) {
        self.memory.borrow_mut().shrink(bytes);
    }

    fn checkpoint(&self) -> Result<()> {
        hawdb_executor::pipeline::runtime_checkpoint(self.task)
    }

    pub(super) fn statistics(&self) -> Result<Option<SearchIndexStatistics>> {
        self.charge(1)?;
        self.runtime
            .read_point(self.index, &search_index_statistics_key())?
            .map(|row| SearchIndexStatistics::decode(data(&row)?).map_err(integrity))
            .transpose()
    }
}

/// One block of a term: a sealed block (by fence) or the dictionary's tail.
#[derive(Debug, Clone, Copy)]
struct TermBlock {
    fence: u64,
    bounds: SearchBlockBounds,
    tail: bool,
}

/// A query term's dictionary entry and skip entries, read once per query.
pub(super) struct TermPostings {
    term: String,
    idf: f64,
    average_length: f64,
    max_bound: f64,
    tail: Vec<SearchPosting>,
    blocks: Vec<TermBlock>,
    sealed_blocks: u64,
}

impl TermPostings {
    /// The term's dictionary row and every bounds row of its sealed blocks;
    /// `None` when the term is absent. No block payload is read.
    pub(super) fn load(
        context: &ScanContext<'_>,
        term: &str,
        statistics: &SearchIndexStatistics,
        average_length: f64,
    ) -> Result<Option<Self>> {
        context.checkpoint()?;
        context.charge(1)?;
        let Some(row) = context
            .runtime
            .read_point(context.index, &search_index_dictionary_key(term))?
        else {
            return Ok(None);
        };
        let encoded = data(&row)?;
        // Tail postings decode into 16 bytes each from at least two.
        context.admit(encoded.len().saturating_mul(8))?;
        let dictionary = SearchTermDictionary::decode(encoded).map_err(integrity)?;
        if dictionary.document_frequency == 0
            || dictionary.document_frequency > statistics.documents
        {
            return Err(corrupt("dictionary disagrees with the corpus statistics"));
        }
        let blocks = usize::try_from(dictionary.sealed_blocks)
            .unwrap_or(usize::MAX)
            .saturating_add(1);
        context.admit(blocks.saturating_mul(std::mem::size_of::<TermBlock>()))?;
        let mut entries = Vec::with_capacity(blocks);
        let mut postings = 0u64;
        if dictionary.sealed_blocks > 0 {
            context.runtime.visit_primary_key_range(
                context.index,
                &search_index_block_bounds_range(term).map_err(integrity)?,
                |row| {
                    context.charge(1)?;
                    let fence = search_index_block_fence(row.value(2)?).map_err(integrity)?;
                    let bounds =
                        SearchBlockBounds::decode(fence, data(&row)?).map_err(integrity)?;
                    if entries.len() as u64 >= dictionary.sealed_blocks
                        || entries.last().is_some_and(|previous: &TermBlock| {
                            previous.bounds.last >= fence || previous.fence >= fence
                        })
                        || bounds.last >= dictionary.tail_start
                        || (entries.is_empty() && fence != 0)
                    {
                        return Err(corrupt("block bounds are out of order"));
                    }
                    postings = postings.saturating_add(bounds.count);
                    entries.push(TermBlock {
                        fence,
                        bounds,
                        tail: false,
                    });
                    Ok(true)
                },
            )?;
            if entries.len() as u64 != dictionary.sealed_blocks {
                return Err(corrupt("block bounds disagree with their dictionary"));
            }
        }
        if let Some(last) = dictionary.tail.last() {
            entries.push(TermBlock {
                fence: dictionary.tail_start,
                bounds: SearchBlockBounds {
                    last: last.ordinal,
                    count: dictionary.tail.len() as u64,
                    max_frequency: dictionary
                        .tail
                        .iter()
                        .map(|posting| posting.frequency)
                        .max()
                        .unwrap_or(0),
                    min_length: dictionary.tail_min_length,
                },
                tail: true,
            });
            postings = postings.saturating_add(dictionary.tail.len() as u64);
        }
        if postings != dictionary.document_frequency {
            return Err(corrupt("postings disagree with their document frequency"));
        }
        let idf = idf(statistics.documents, dictionary.document_frequency);
        let max_bound =
            entries
                .iter()
                .filter(|block| block.bounds.count > 0)
                .fold(0.0f64, |bound, block| {
                    bound.max(term_upper_bound(
                        idf,
                        block.bounds.max_frequency,
                        block.bounds.min_length,
                        average_length,
                    ))
                });
        Ok(Some(Self {
            term: term.to_owned(),
            idf,
            average_length,
            max_bound,
            tail: dictionary.tail,
            blocks: entries,
            sealed_blocks: dictionary.sealed_blocks,
        }))
    }

    pub(super) fn document_frequency(&self) -> u64 {
        self.blocks.iter().map(|block| block.bounds.count).sum()
    }

    pub(super) fn sealed_blocks(&self) -> u64 {
        self.sealed_blocks
    }
}

/// A cursor over one term's blocks. Positions are lower bounds until
/// [`Bm25PostingCursor::materialize`] decodes the block that holds them.
pub(super) struct IndexCursor<'a> {
    context: &'a ScanContext<'a>,
    postings: &'a TermPostings,
    hint: usize,
    position: Option<u64>,
    /// `(block, index)` of the exact current posting.
    exact: Option<(usize, usize)>,
    loaded: Option<usize>,
    decoded: Vec<SearchPosting>,
    decoded_bytes: usize,
    /// Payloads read ahead, by block index, not yet decoded.
    ahead: VecDeque<(usize, Vec<u8>)>,
    ahead_bytes: usize,
    window: usize,
    /// Blocks bounded below this cannot reach the top-k: never read ahead.
    threshold: f64,
}

impl<'a> IndexCursor<'a> {
    pub(super) fn new(context: &'a ScanContext<'a>, postings: &'a TermPostings) -> Self {
        let mut cursor = Self {
            context,
            postings,
            hint: 0,
            position: Some(0),
            exact: None,
            loaded: None,
            decoded: Vec::new(),
            decoded_bytes: 0,
            ahead: VecDeque::new(),
            ahead_bytes: 0,
            window: 1,
            threshold: f64::NEG_INFINITY,
        };
        if cursor.block_index(0).is_none() {
            cursor.position = None;
        }
        cursor
    }

    fn block_index(&mut self, target: u64) -> Option<usize> {
        let blocks = &self.postings.blocks;
        while self.hint < blocks.len()
            && (blocks[self.hint].bounds.count == 0 || blocks[self.hint].bounds.last < target)
        {
            self.hint += 1;
        }
        (self.hint < blocks.len()).then_some(self.hint)
    }

    /// Reads the payload of sealed block `index`, with the following blocks
    /// when the cursor is decoding consecutive blocks. The returned payload
    /// stays admitted until the caller releases it.
    fn payload(&mut self, index: usize) -> Result<Vec<u8>> {
        while self.ahead.front().is_some_and(|(ahead, _)| *ahead < index) {
            let (_, skipped) = self.ahead.pop_front().expect("front exists");
            self.ahead_bytes -= skipped.len();
            self.context.release(skipped.len());
        }
        if let Some((ahead, _)) = self.ahead.front()
            && *ahead == index
        {
            let (_, payload) = self.ahead.pop_front().expect("front exists");
            self.ahead_bytes -= payload.len();
            return Ok(payload);
        }
        for (_, stale) in self.ahead.drain(..) {
            self.context.release(stale.len());
        }
        self.ahead_bytes = 0;
        // Windows grow while the cursor decodes consecutive blocks, and hold
        // only blocks that can still reach the floor once the heap is full.
        self.window = if self.loaded.is_some_and(|loaded| loaded + 1 == index) {
            (self.window * 2).min(MAX_BLOCK_READAHEAD)
        } else {
            MIN_BLOCK_READAHEAD
        };
        let blocks = &self.postings.blocks;
        let threshold = self.threshold;
        let competitive = |block: &TermBlock| {
            term_upper_bound(
                self.postings.idf,
                block.bounds.max_frequency,
                block.bounds.min_length,
                self.postings.average_length,
            ) >= threshold
        };
        let last = index
            + blocks[index + 1..]
                .iter()
                .take(self.window.saturating_sub(1))
                .take_while(|block| !block.tail && block.bounds.count > 0 && competitive(block))
                .count();
        let mut found = None;
        let mut expected = index;
        let context = self.context;
        let mut ahead = Vec::new();
        context.runtime.visit_primary_key_range(
            context.index,
            &search_index_sealed_block_window(
                &self.postings.term,
                blocks[index].fence,
                blocks[last].fence,
            )
            .map_err(integrity)?,
            |row| {
                let fence = search_index_block_fence(row.value(2)?).map_err(integrity)?;
                if expected > last || blocks[expected].fence != fence {
                    return Err(corrupt("sealed blocks disagree with their bounds"));
                }
                context.charge(blocks[expected].bounds.count.max(1))?;
                let payload = data(&row)?;
                context.admit(payload.len())?;
                let mut evidence = context.evidence.borrow_mut();
                evidence.blocks_read = evidence.blocks_read.saturating_add(1);
                if expected == index {
                    found = Some(payload.to_vec());
                } else {
                    ahead.push((expected, payload.to_vec()));
                }
                expected += 1;
                Ok(true)
            },
        )?;
        if expected != last + 1 {
            return Err(corrupt("sealed block is missing"));
        }
        for (block, payload) in ahead {
            self.ahead_bytes += payload.len();
            self.ahead.push_back((block, payload));
        }
        found.ok_or_else(|| corrupt("sealed block is missing"))
    }

    fn load(&mut self, index: usize) -> Result<()> {
        if self.loaded == Some(index) {
            return Ok(());
        }
        self.context.checkpoint()?;
        let block = self.postings.blocks[index];
        let count = usize::try_from(block.bounds.count).unwrap_or(usize::MAX);
        let bytes = count.saturating_mul(std::mem::size_of::<SearchPosting>());
        self.context.admit(bytes)?;
        self.context.release(self.decoded_bytes);
        self.decoded_bytes = bytes;
        self.decoded.clear();
        if block.tail {
            self.decoded.extend_from_slice(&self.postings.tail);
        } else {
            let payload = self.payload(index)?;
            let decoded = decode_search_sealed_block(block.fence, &payload, &mut self.decoded);
            self.context.release(payload.len());
            decoded.map_err(integrity)?;
            if !block.bounds.describes(block.fence, &self.decoded) {
                return Err(corrupt("block disagrees with its bounds"));
            }
            let mut evidence = self.context.evidence.borrow_mut();
            evidence.blocks_decoded = evidence.blocks_decoded.saturating_add(1);
        }
        let mut evidence = self.context.evidence.borrow_mut();
        evidence.postings_visited = evidence
            .postings_visited
            .saturating_add(self.decoded.len() as u64);
        self.loaded = Some(index);
        Ok(())
    }
}

impl Drop for IndexCursor<'_> {
    fn drop(&mut self) {
        self.context
            .release(self.decoded_bytes.saturating_add(self.ahead_bytes));
    }
}

fn kernel_block(bounds: SearchBlockBounds) -> Bm25Block {
    Bm25Block {
        last: bounds.last,
        max_frequency: bounds.max_frequency,
        min_length: bounds.min_length,
    }
}

impl Bm25PostingCursor for IndexCursor<'_> {
    fn position(&self) -> Option<u64> {
        self.position
    }

    fn is_exact(&self) -> bool {
        self.exact.is_some()
    }

    fn block_at(&mut self, target: u64) -> Result<Option<Bm25Block>> {
        Ok(self
            .block_index(target)
            .map(|index| kernel_block(self.postings.blocks[index].bounds)))
    }

    fn skip_to(&mut self, target: u64) -> Result<()> {
        let Some(position) = self.position else {
            return Ok(());
        };
        if target <= position {
            return Ok(());
        }
        self.exact = None;
        self.position = self.block_index(target).map(|_| target);
        Ok(())
    }

    fn materialize(&mut self) -> Result<()> {
        let Some(position) = self.position else {
            return Ok(());
        };
        let Some(index) = self.block_index(position) else {
            self.position = None;
            return Ok(());
        };
        self.load(index)?;
        let offset = self
            .decoded
            .partition_point(|posting| posting.ordinal < position);
        let posting = self
            .decoded
            .get(offset)
            .ok_or_else(|| corrupt("block bounds overstate its last posting"))?;
        self.position = Some(posting.ordinal);
        self.exact = Some((index, offset));
        Ok(())
    }

    fn competitive_threshold(&mut self, threshold: f64) {
        self.threshold = threshold;
    }

    fn current(&self) -> Result<(u64, Bm25Block)> {
        let (index, offset) = self
            .exact
            .ok_or_else(|| HawDBError::Execution("BM25 cursor is not positioned".into()))?;
        Ok((
            self.decoded[offset].frequency,
            kernel_block(self.postings.blocks[index].bounds),
        ))
    }
}

/// Document lengths and keys, read in forward windows of lengths rows.
pub(super) struct IndexDocuments<'a> {
    context: &'a ScanContext<'a>,
    rows: BTreeMap<u64, (SearchLengthBlock, usize)>,
}

impl<'a> IndexDocuments<'a> {
    pub(super) fn new(context: &'a ScanContext<'a>) -> Self {
        Self {
            context,
            rows: BTreeMap::new(),
        }
    }

    fn entry(&mut self, ordinal: u64) -> Result<&hawdb_storage::relational::SearchDocumentEntry> {
        let row = ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
        if !self.rows.contains_key(&row) {
            // Rounds score in ascending ordinal order: rows behind the
            // requested one are not needed again in this round.
            let keep = self.rows.split_off(&row);
            for (_, (_, bytes)) in std::mem::replace(&mut self.rows, keep) {
                self.context.release(bytes);
            }
            let last = row.saturating_add(LENGTHS_WINDOW - 1);
            let context = self.context;
            let rows = &mut self.rows;
            context.runtime.visit_primary_key_range(
                context.index,
                &search_index_length_block_range(
                    row * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
                    last.saturating_mul(SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS),
                )
                .map_err(integrity)?,
                |read| {
                    context.charge(1)?;
                    let start =
                        search_index_length_block_start(read.value(2)?).map_err(integrity)?;
                    if rows.contains_key(&(start / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS)) {
                        return Ok(true);
                    }
                    let data = data(&read)?;
                    let bytes = data.len().saturating_mul(2).saturating_add(96);
                    context.admit(bytes)?;
                    let block = SearchLengthBlock::decode(data).map_err(integrity)?;
                    rows.insert(start / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS, (block, bytes));
                    let mut evidence = context.evidence.borrow_mut();
                    evidence.lengths_rows_read = evidence.lengths_rows_read.saturating_add(1);
                    Ok(true)
                },
            )?;
        }
        self.rows
            .get(&row)
            .and_then(|(block, _)| block.get(ordinal))
            .ok_or_else(|| corrupt("posting names an absent document"))
    }
}

impl Drop for IndexDocuments<'_> {
    fn drop(&mut self) {
        for (_, (_, bytes)) in std::mem::take(&mut self.rows) {
            self.context.release(bytes);
        }
    }
}

impl Bm25Documents for IndexDocuments<'_> {
    fn length(&mut self, ordinal: u64) -> Result<u64> {
        Ok(self.entry(ordinal)?.length)
    }

    fn key(&mut self, ordinal: u64) -> Result<Vec<u8>> {
        // The kernel admits a key before retaining it in the heap.
        Ok(self.entry(ordinal)?.key.clone())
    }

    fn checkpoint(&mut self) -> Result<()> {
        self.context.checkpoint()
    }

    fn admit(&mut self, bytes: usize) -> Result<()> {
        self.context.admit(bytes)
    }

    fn release(&mut self, bytes: usize) {
        self.context.release(bytes);
    }
}

/// Exact top-`k` of the loaded terms through the shared Block-Max WAND
/// kernel. Each call is one round; its work is added to the evidence.
pub(super) fn rank_top_k(
    context: &ScanContext<'_>,
    terms: &[TermPostings],
    average_length: f64,
    k: usize,
) -> Result<Vec<Bm25Hit>> {
    let mut cursors = terms
        .iter()
        .map(|postings| Bm25Term {
            cursor: IndexCursor::new(context, postings),
            idf: postings.idf,
            max_bound: postings.max_bound,
        })
        .collect::<Vec<_>>();
    let mut documents = IndexDocuments::new(context);
    let top = block_max_wand(&mut cursors, average_length, k, &mut documents)?;
    let mut evidence = context.evidence.borrow_mut();
    evidence.rounds += 1;
    evidence.final_k = k;
    evidence.ranked_rows = top.hits.len();
    evidence.blocks_total = terms.iter().map(TermPostings::sealed_blocks).sum();
    evidence.documents_scored = evidence
        .documents_scored
        .saturating_add(top.stats.documents_scored);
    evidence.block_skips = evidence.block_skips.saturating_add(top.stats.block_skips);
    evidence.terminated_early |= top.stats.terminated_early;
    evidence.blocks_skipped = (evidence.blocks_total.saturating_mul(evidence.rounds as u64))
        .saturating_sub(evidence.blocks_read);
    Ok(top.hits)
}
