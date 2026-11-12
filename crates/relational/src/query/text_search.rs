// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `FROM text_search(index, query, top_k => k)`: BM25 over an engine-owned
//! search index. Each query term's dictionary row and posting blocks are read
//! from the index relation in the same snapshot as the source rows, scored
//! exactly with per-document lengths, and hydrated in descending
//! score order until `k` rows satisfy WHERE. Projection, ORDER BY, LIMIT and
//! OFFSET then apply to those rows.

use super::projection::add_relational_order_keys;
use super::vector_index::{
    execute_candidate_select, validate_candidate_select, CandidateScore, VectorIndexCandidates,
    VectorIndexInput, VectorIndexSqlExecution,
};
use super::*;
use hawdb_sql::SqlTextSearch;
use hawdb_storage::relational::{
    decode_search_sealed_block, decode_search_sealed_block_header, search_index_block_fence,
    search_index_dictionary_key, search_index_length_block_key, search_index_length_block_range,
    search_index_length_block_start, search_index_sealed_block_after_range,
    search_index_sealed_block_range, search_index_statistics_key, RelationalColumnSchema,
    SearchDocumentEntry, SearchIndexAnalyzer, SearchIndexStatistics, SearchLengthBlock,
    SearchPosting, SearchPostingBlockHeader, SearchTermDictionary, SEARCH_INDEX_COLUMN_COUNT,
    SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS, SEARCH_INDEX_MAX_BLOCK_POSTINGS,
};
use std::cmp::Reverse;
use std::collections::btree_map::Entry;
use std::collections::BinaryHeap;

/// Okapi BM25 with the conventional Lucene defaults.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
/// Below this posting count, exact exhaustive streaming costs less than WAND.
const BLOCK_MAX_PRUNING_MIN_POSTINGS: u64 = 4_096;
/// Four header varints, two worst-case varints per posting and the checksum.
const MAX_ENCODED_POSTING_BLOCK_BYTES: usize =
    4 * 10 + SEARCH_INDEX_MAX_BLOCK_POSTINGS * 2 * 10 + std::mem::size_of::<u32>();
pub const TEXT_SEARCH_SCORE_COLUMN: &str = "score";
/// Upper bound on `top_k`; larger result sets should page with OFFSET.
pub const TEXT_SEARCH_MAX_TOP_K: usize = 10_000;

/// Estimated resident bytes of one entry of a B-tree map or set: twice the
/// entry (nodes other than the root are at least half full) plus headers.
const fn btree_entry_bytes<K, V>() -> usize {
    2 * (std::mem::size_of::<K>() + std::mem::size_of::<V>()) + 16
}

fn semantic(message: impl Into<String>) -> HawDBError {
    HawDBError::Semantic(message.into())
}

fn bind<'a>(value: &'a SqlValue, parameters: &'a [Value], name: &str) -> Result<&'a Value> {
    match value {
        SqlValue::Literal(value) => Ok(value),
        SqlValue::Parameter(position) => position
            .checked_sub(1)
            .and_then(|position| parameters.get(position))
            .ok_or_else(|| semantic(format!("missing text_search {name} parameter"))),
        SqlValue::Expression(_) => Err(semantic(format!(
            "text_search {name} must be a literal or parameter"
        ))),
    }
}

struct BoundTextSearch<'a> {
    index: &'a str,
    query: &'a str,
    top_k: usize,
}

fn bind_call<'a>(call: &'a SqlTextSearch, parameters: &'a [Value]) -> Result<BoundTextSearch<'a>> {
    let Value::String(index) = bind(&call.index, parameters, "index")? else {
        return Err(semantic("text_search index must be a string"));
    };
    let Value::String(query) = bind(&call.query, parameters, "query")? else {
        return Err(semantic("text_search query must be a string"));
    };
    let top_k = match bind(&call.top_k, parameters, "top_k")? {
        Value::Int(value) => usize::try_from(*value)
            .ok()
            .filter(|value| (1..=TEXT_SEARCH_MAX_TOP_K).contains(value)),
        _ => None,
    }
    .ok_or_else(|| {
        semantic(format!(
            "text_search top_k must be an integer between 1 and {TEXT_SEARCH_MAX_TOP_K}"
        ))
    })?;
    Ok(BoundTextSearch {
        index,
        query,
        top_k,
    })
}

struct RankedCandidates(Vec<(String, f64)>);

impl VectorIndexCandidates for RankedCandidates {
    fn row_count(&self) -> usize {
        self.0.len()
    }

    fn row(&self, ordinal: usize) -> Result<(&str, f64)> {
        let (identity, score) = self
            .0
            .get(ordinal)
            .ok_or_else(|| HawDBError::Execution("text_search ordinal is out of range".into()))?;
        Ok((identity, *score))
    }
}

fn identity(document: &[u8]) -> String {
    use std::fmt::Write;
    let mut identity = String::with_capacity(document.len().saturating_mul(2).saturating_add(2));
    identity.push_str("r:");
    for byte in document {
        write!(&mut identity, "{byte:02x}").expect("String write");
    }
    identity
}

fn integrity(error: hawdb_storage::relational::RelationalError) -> HawDBError {
    HawDBError::StorageIntegrity(error.to_string())
}

fn data(row: &RelationalReadRow) -> Result<&[u8]> {
    match row.value(3)? {
        RelationalValue::Bytea(data) => Ok(data),
        _ => Err(HawDBError::StorageIntegrity(
            "search index row has an invalid payload".into(),
        )),
    }
}

fn bm25_term_score(idf: f64, frequency: u64, document_len: u64, average_len: f64) -> f64 {
    let frequency = frequency as f64;
    let normalization =
        BM25_K1 * (1.0 - BM25_B + BM25_B * document_len as f64 / average_len.max(1.0));
    idf * frequency * (BM25_K1 + 1.0) / (frequency + normalization)
}

fn bm25_term_upper_bound(idf: f64, max_frequency: u64, average_len: f64) -> f64 {
    bm25_term_score(idf, max_frequency, 0, average_len)
}

fn charge_candidate_work(work: &mut usize, amount: usize, maximum: usize) -> Result<()> {
    *work = work.saturating_add(amount);
    if *work > maximum {
        return Err(HawDBError::read_budget_exceeded(
            hawdb_core::ReadBudgetResource::Rows,
            maximum,
            "text_search postings exceed SQL candidate work admission",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct ScoredDocument {
    identity: String,
    score: f64,
    memory_bytes: usize,
}

impl PartialEq for ScoredDocument {
    fn eq(&self, other: &Self) -> bool {
        self.identity == other.identity && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredDocument {}

impl PartialOrd for ScoredDocument {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredDocument {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| other.identity.cmp(&self.identity))
    }
}

struct ScoreCollector {
    limit: usize,
    heap: BinaryHeap<Reverse<ScoredDocument>>,
}

impl ScoreCollector {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit.saturating_add(1)),
        }
    }

    fn floor(&self) -> Option<f64> {
        (self.heap.len() == self.limit)
            .then(|| self.heap.peek().map(|Reverse(worst)| worst.score))
            .flatten()
    }

    fn push(&mut self, document: &[u8], score: f64, memory: &mut QueryMemoryLease) -> Result<()> {
        let full = self.heap.len() == self.limit;
        if full
            && self
                .heap
                .peek()
                .is_some_and(|Reverse(worst)| score.total_cmp(&worst.score).is_lt())
        {
            return Ok(());
        }
        // Admit a conservative identity allocation before constructing it.
        // The retained charge travels with the heap entry and is refunded on
        // rejection or eviction.
        let memory_bytes = document.len().saturating_mul(4).saturating_add(4);
        memory.grow(memory_bytes)?;
        let identity = identity(document);
        if full
            && self.heap.peek().is_some_and(|Reverse(worst)| {
                !score
                    .total_cmp(&worst.score)
                    .then_with(|| worst.identity.as_str().cmp(&identity))
                    .is_gt()
            })
        {
            memory.shrink(memory_bytes);
            return Ok(());
        }
        if full {
            let Reverse(evicted) = self.heap.pop().expect("retained score heap is full");
            memory.shrink(evicted.memory_bytes);
        }
        self.heap.push(Reverse(ScoredDocument {
            identity,
            score,
            memory_bytes,
        }));
        Ok(())
    }

    fn finish(self) -> Vec<(String, f64)> {
        let mut ranked = self
            .heap
            .into_iter()
            .map(|Reverse(document)| (document.identity, document.score))
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        ranked
    }
}

#[derive(Default)]
struct TextSearchWork {
    postings_visited: usize,
    posting_blocks_read: usize,
    posting_blocks_skipped: usize,
}

struct TermPostingStream {
    term: String,
    idf: f64,
    sealed_blocks: u64,
    sealed_blocks_read: u64,
    after_fence: Option<u64>,
    tail_start: u64,
    tail: Vec<SearchPosting>,
    tail_loaded: bool,
    pending_fence: Option<u64>,
    pending_encoded: Vec<u8>,
    current_block: Vec<SearchPosting>,
    position: usize,
    current: Option<SearchPosting>,
    bounds: Option<SearchPostingBlockHeader>,
}

impl TermPostingStream {
    fn new(term: String, idf: f64, dictionary: SearchTermDictionary) -> Self {
        Self {
            term,
            idf,
            sealed_blocks: dictionary.sealed_blocks,
            sealed_blocks_read: 0,
            after_fence: None,
            tail_start: dictionary.tail_start,
            tail: dictionary.tail,
            tail_loaded: false,
            pending_fence: None,
            pending_encoded: Vec::with_capacity(MAX_ENCODED_POSTING_BLOCK_BYTES),
            current_block: Vec::with_capacity(SEARCH_INDEX_MAX_BLOCK_POSTINGS),
            position: 0,
            current: None,
            bounds: None,
        }
    }

    fn peek_ordinal(
        &mut self,
        runtime: &RelationalRowRuntime<'_>,
        index: &str,
        target: u64,
        work: &mut TextSearchWork,
        candidate_work: &mut usize,
        max_candidate_work: usize,
    ) -> Result<Option<u64>> {
        if self.current.is_none() {
            self.advance_to(
                runtime,
                index,
                target,
                work,
                candidate_work,
                max_candidate_work,
            )?;
        }
        Ok(self.current.map(|posting| posting.ordinal))
    }

    fn take(
        &mut self,
        work: &mut TextSearchWork,
        candidate_work: &mut usize,
        max_candidate_work: usize,
    ) -> Result<Option<SearchPosting>> {
        let Some(current) = self.current.take() else {
            return Ok(None);
        };
        if current.frequency != 0 {
            return Ok(Some(current));
        }
        self.decode_pending_block(work, candidate_work, max_candidate_work)?;
        let posting = self.current_block.first().copied().ok_or_else(|| {
            HawDBError::StorageIntegrity("search index sealed block is unexpectedly empty".into())
        })?;
        if posting.ordinal != current.ordinal {
            return Err(HawDBError::StorageIntegrity(
                "search index sealed block header disagrees with its first posting".into(),
            ));
        }
        self.position = 1;
        Ok(Some(posting))
    }

    fn block_upper_bound(&self, average_length: f64) -> f64 {
        self.bounds.map_or(f64::INFINITY, |bounds| {
            bm25_term_upper_bound(self.idf, bounds.max_frequency, average_length)
        })
    }

    fn global_upper_bound(&self, average_length: f64) -> f64 {
        bm25_term_upper_bound(self.idf, u64::MAX, average_length)
    }

    fn unread_blocks(&self) -> usize {
        usize::try_from(self.sealed_blocks.saturating_sub(self.sealed_blocks_read))
            .unwrap_or(usize::MAX)
            .saturating_add(usize::from(self.pending_fence.is_some()))
            .saturating_add(usize::from(!self.tail_loaded && !self.tail.is_empty()))
    }

    #[allow(clippy::too_many_arguments)]
    fn advance_to(
        &mut self,
        runtime: &RelationalRowRuntime<'_>,
        index: &str,
        target: u64,
        work: &mut TextSearchWork,
        candidate_work: &mut usize,
        max_candidate_work: usize,
    ) -> Result<()> {
        if self
            .current
            .is_some_and(|posting| posting.ordinal >= target)
        {
            return Ok(());
        }
        self.current = None;
        loop {
            if self.pending_fence.is_some() {
                if self
                    .bounds
                    .and_then(|bounds| bounds.last_ordinal)
                    .is_some_and(|last| last < target)
                {
                    self.pending_fence = None;
                    self.pending_encoded.clear();
                    self.bounds = None;
                    work.posting_blocks_skipped = work.posting_blocks_skipped.saturating_add(1);
                } else {
                    self.decode_pending_block(work, candidate_work, max_candidate_work)?;
                }
            }
            if self.position < self.current_block.len() {
                self.position = self.current_block[self.position..]
                    .partition_point(|posting| posting.ordinal < target)
                    .saturating_add(self.position);
                if let Some(posting) = self.current_block.get(self.position).copied() {
                    self.position += 1;
                    self.current = Some(posting);
                    return Ok(());
                }
            }
            if !self.load_next_block(
                runtime,
                index,
                target,
                work,
                candidate_work,
                max_candidate_work,
            )? {
                self.bounds = None;
                return Ok(());
            }
            if self.current.is_some() {
                return Ok(());
            }
        }
    }

    fn decode_pending_block(
        &mut self,
        work: &mut TextSearchWork,
        candidate_work: &mut usize,
        max_candidate_work: usize,
    ) -> Result<()> {
        let fence = self.pending_fence.take().ok_or_else(|| {
            HawDBError::StorageIntegrity("search index sealed block payload is absent".into())
        })?;
        self.current_block.clear();
        decode_search_sealed_block(fence, &self.pending_encoded, &mut self.current_block)
            .map_err(integrity)?;
        self.pending_encoded.clear();
        work.postings_visited = work
            .postings_visited
            .saturating_add(self.current_block.len());
        charge_candidate_work(candidate_work, self.current_block.len(), max_candidate_work)?;
        self.position = 0;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn load_next_block(
        &mut self,
        runtime: &RelationalRowRuntime<'_>,
        index: &str,
        target: u64,
        work: &mut TextSearchWork,
        candidate_work: &mut usize,
        max_candidate_work: usize,
    ) -> Result<bool> {
        loop {
            self.current_block.clear();
            self.position = 0;
            if self.sealed_blocks_read < self.sealed_blocks {
                let range = search_index_sealed_block_after_range(&self.term, self.after_fence)
                    .map_err(integrity)?;
                let mut loaded: Option<(u64, SearchPostingBlockHeader)> = None;
                runtime.visit_primary_key_range(index, &range, |row| {
                    let fence = search_index_block_fence(row.value(2)?).map_err(integrity)?;
                    let header =
                        decode_search_sealed_block_header(fence, data(&row)?).map_err(integrity)?;
                    if header.count > SEARCH_INDEX_MAX_BLOCK_POSTINGS
                        || header
                            .last_ordinal
                            .is_some_and(|last| last >= self.tail_start)
                    {
                        return Err(HawDBError::StorageIntegrity(
                            "search index sealed block exceeds its dictionary bounds".into(),
                        ));
                    }
                    if header.last_ordinal.is_some_and(|last| last >= target) {
                        self.pending_encoded.extend_from_slice(data(&row)?);
                    }
                    loaded = Some((fence, header));
                    Ok(false)
                })?;
                let (fence, header) = loaded.ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "search index sealed block count disagrees with its dictionary".into(),
                    )
                })?;
                self.after_fence = Some(fence);
                self.sealed_blocks_read += 1;
                work.posting_blocks_read = work.posting_blocks_read.saturating_add(1);
                if self.pending_encoded.is_empty() {
                    if header.count != 0 {
                        work.posting_blocks_skipped = work.posting_blocks_skipped.saturating_add(1);
                    }
                    continue;
                }
                self.pending_fence = Some(fence);
                self.bounds = Some(header);
                self.current = header.first_ordinal.map(|ordinal| SearchPosting {
                    ordinal,
                    // Zero is an internal marker: persisted frequencies are
                    // strictly positive and are decoded only if this block is
                    // selected for scoring.
                    frequency: 0,
                });
                return Ok(true);
            }
            if self.tail_loaded {
                return Ok(false);
            }
            self.tail_loaded = true;
            if self.tail.is_empty() {
                return Ok(false);
            }
            let max_frequency = self
                .tail
                .iter()
                .map(|posting| posting.frequency)
                .max()
                .expect("non-empty tail");
            self.bounds = Some(SearchPostingBlockHeader {
                count: self.tail.len(),
                first_ordinal: self.tail.first().map(|posting| posting.ordinal),
                last_ordinal: self.tail.last().map(|posting| posting.ordinal),
                max_frequency,
            });
            work.posting_blocks_read = work.posting_blocks_read.saturating_add(1);
            work.postings_visited = work.postings_visited.saturating_add(self.tail.len());
            charge_candidate_work(candidate_work, self.tail.len(), max_candidate_work)?;
            std::mem::swap(&mut self.current_block, &mut self.tail);
            return Ok(true);
        }
    }
}

struct DocumentLookup {
    block: Option<u64>,
    lengths: SearchLengthBlock,
}

impl DocumentLookup {
    fn new() -> Self {
        Self {
            block: None,
            lengths: SearchLengthBlock::default(),
        }
    }

    fn get<'a>(
        &'a mut self,
        runtime: &RelationalRowRuntime<'_>,
        index: &str,
        ordinal: u64,
    ) -> Result<&'a SearchDocumentEntry> {
        let block = ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
        if self.block != Some(block) {
            let row = runtime
                .read_point(index, &search_index_length_block_key(ordinal))?
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "search index posting names an absent lengths block".into(),
                    )
                })?;
            self.lengths = SearchLengthBlock::decode(data(&row)?).map_err(integrity)?;
            self.block = Some(block);
        }
        self.lengths.get(ordinal).ok_or_else(|| {
            HawDBError::StorageIntegrity("search index posting names an absent document".into())
        })
    }
}

struct RankedSearch {
    rows: Vec<(String, f64)>,
    row_evidence: RelationalRowExecutionEvidence,
    work: TextSearchWork,
}

#[derive(Clone, Copy)]
struct RankRequest<'a> {
    index: &'a str,
    analyzer: SearchIndexAnalyzer,
    query: &'a str,
    top_k: usize,
}

/// Exact, bounded top-k BM25 over storage-native posting streams. The pivot
/// and block-boundary rules mirror the resident lexical projection's WAND
/// path; score accumulation remains in analyzer term order for bit equality.
fn rank_top_k<R: RelationalRowStoreReader>(
    state: &RelationalState,
    request: RankRequest<'_>,
    row_mode: RelationalRowReadMode<'_, R>,
    resources: &RelationalQueryResourceContext<'_>,
    ledger: &QueryMemoryLedger,
) -> Result<RankedSearch> {
    let RankRequest {
        index,
        analyzer,
        query,
        top_k,
    } = request;
    let default_task = hawdb_core::RuntimeTaskContext::default();
    let task = resources.task_context.unwrap_or(&default_task);
    let runtime = row_mode.open_runtime(
        state,
        crate::field_plan::RelationalFieldPlan::all_columns(index, SEARCH_INDEX_COLUMN_COUNT),
        resources.limits.row_read,
        resources.limits.hydration,
        task,
    )?;
    let Some(statistics) = runtime.read_point(index, &search_index_statistics_key())? else {
        return Ok(RankedSearch {
            rows: Vec::new(),
            row_evidence: runtime.evidence(),
            work: TextSearchWork::default(),
        });
    };
    let statistics = SearchIndexStatistics::decode(data(&statistics)?).map_err(integrity)?;
    if statistics.documents == 0 || statistics.tokens == 0 {
        return Ok(RankedSearch {
            rows: Vec::new(),
            row_evidence: runtime.evidence(),
            work: TextSearchWork::default(),
        });
    }
    let documents = statistics.documents as f64;
    let average_length = statistics.tokens as f64 / documents;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "TextSearch WAND",
        resources.execution_memory.blocking_operator_bytes,
    );
    let mut memory = account.reserve(
        top_k
            .saturating_add(1)
            .saturating_mul(std::mem::size_of::<ScoredDocument>()),
    )?;
    let terms = analyzer.admitted_query_terms(query, |bytes| memory.grow(bytes))?;
    memory.grow(
        terms.len().saturating_mul(
            std::mem::size_of::<TermPostingStream>()
                .saturating_add(std::mem::size_of::<(u64, usize)>()),
        ),
    )?;
    let mut streams = Vec::with_capacity(terms.len());
    let mut candidate_postings = 0u64;
    for term in terms {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let Some(row) = runtime.read_point(index, &search_index_dictionary_key(&term))? else {
            continue;
        };
        let dictionary = SearchTermDictionary::decode(data(&row)?).map_err(integrity)?;
        if dictionary.document_frequency == 0 {
            continue;
        }
        candidate_postings = candidate_postings.saturating_add(dictionary.document_frequency);
        memory.grow(
            SEARCH_INDEX_MAX_BLOCK_POSTINGS
                .saturating_mul(std::mem::size_of::<SearchPosting>().saturating_add(2 * 10))
                .saturating_add(4 * 10 + std::mem::size_of::<u32>())
                .saturating_add(dictionary.tail.capacity() * std::mem::size_of::<SearchPosting>())
                .saturating_add(term.capacity()),
        )?;
        let frequency = dictionary.document_frequency as f64;
        let idf = (1.0 + (documents - frequency + 0.5) / (frequency + 0.5)).ln();
        streams.push(TermPostingStream::new(term, idf, dictionary));
    }
    let prune = candidate_postings >= BLOCK_MAX_PRUNING_MIN_POSTINGS;
    let mut collector = ScoreCollector::new(top_k);
    let mut documents = DocumentLookup::new();
    let mut order = Vec::<(u64, usize)>::with_capacity(streams.len());
    let mut work = TextSearchWork::default();
    let mut candidate_work = 0usize;
    loop {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        order.clear();
        for (stream_index, stream) in streams.iter_mut().enumerate() {
            if let Some(ordinal) = stream.peek_ordinal(
                &runtime,
                index,
                0,
                &mut work,
                &mut candidate_work,
                resources.limits.max_candidate_work,
            )? {
                order.push((ordinal, stream_index));
            }
        }
        order.sort_unstable();
        let Some(&(ordinal, _)) = order.first() else {
            break;
        };
        if prune && let Some(floor) = collector.floor() {
            let pivot_index = order.partition_point(|&(candidate, _)| {
                let upper = streams.iter().fold(0.0, |upper, stream| {
                    if stream
                        .current
                        .is_some_and(|posting| posting.ordinal <= candidate)
                    {
                        upper + stream.block_upper_bound(average_length)
                    } else {
                        upper
                    }
                });
                upper < floor
            });
            let pivot = order.get(pivot_index).map(|&(candidate, _)| candidate);
            if pivot.is_none() {
                let remaining_upper = streams.iter().fold(0.0, |upper, stream| {
                    if stream.current.is_some() {
                        upper + stream.global_upper_bound(average_length)
                    } else {
                        upper
                    }
                });
                if remaining_upper < floor {
                    work.posting_blocks_skipped = work.posting_blocks_skipped.saturating_add(
                        streams
                            .iter()
                            .map(TermPostingStream::unread_blocks)
                            .sum::<usize>(),
                    );
                    break;
                }
            }
            let boundary = order
                .iter()
                .filter_map(|&(_, stream_index)| streams[stream_index].bounds?.last_ordinal)
                .min()
                .unwrap_or(ordinal)
                .checked_add(1)
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "search index posting block boundary overflow".into(),
                    )
                })?;
            let target = pivot.map_or(boundary, |pivot| pivot.min(boundary));
            if target > ordinal {
                for &(candidate, stream_index) in &order {
                    if candidate < target {
                        streams[stream_index].advance_to(
                            &runtime,
                            index,
                            target,
                            &mut work,
                            &mut candidate_work,
                            resources.limits.max_candidate_work,
                        )?;
                    }
                }
                continue;
            }
        }
        let entry = documents.get(&runtime, index, ordinal)?;
        let mut score = 0.0;
        for stream in &mut streams {
            if stream
                .current
                .is_some_and(|posting| posting.ordinal == ordinal)
            {
                let posting = stream
                    .take(
                        &mut work,
                        &mut candidate_work,
                        resources.limits.max_candidate_work,
                    )?
                    .expect("matching posting is present");
                if posting.frequency > entry.length {
                    return Err(HawDBError::StorageIntegrity(
                        "search index term frequency exceeds document length".into(),
                    ));
                }
                score +=
                    bm25_term_score(stream.idf, posting.frequency, entry.length, average_length);
            }
        }
        if score > 0.0 {
            collector.push(&entry.key, score, &mut memory)?;
        }
    }
    Ok(RankedSearch {
        rows: collector.finish(),
        row_evidence: runtime.evidence(),
        work,
    })
}

/// Exact BM25 scores of every document containing at least one query term,
/// in descending score order with ties broken by encoded primary key.
fn rank_exhaustive<R: RelationalRowStoreReader>(
    state: &RelationalState,
    request: RankRequest<'_>,
    row_mode: RelationalRowReadMode<'_, R>,
    resources: &RelationalQueryResourceContext<'_>,
    ledger: &QueryMemoryLedger,
) -> Result<RankedSearch> {
    let RankRequest {
        index,
        analyzer,
        query,
        ..
    } = request;
    let default_task = hawdb_core::RuntimeTaskContext::default();
    let task = resources.task_context.unwrap_or(&default_task);
    let runtime = row_mode.open_runtime(
        state,
        crate::field_plan::RelationalFieldPlan::all_columns(index, SEARCH_INDEX_COLUMN_COUNT),
        resources.limits.row_read,
        resources.limits.hydration,
        task,
    )?;
    let Some(statistics) = runtime.read_point(index, &search_index_statistics_key())? else {
        return Ok(RankedSearch {
            rows: Vec::new(),
            row_evidence: runtime.evidence(),
            work: TextSearchWork::default(),
        });
    };
    let statistics = SearchIndexStatistics::decode(data(&statistics)?).map_err(integrity)?;
    if statistics.documents == 0 || statistics.tokens == 0 {
        return Ok(RankedSearch {
            rows: Vec::new(),
            row_evidence: runtime.evidence(),
            work: TextSearchWork::default(),
        });
    }
    let documents = statistics.documents as f64;
    let average_length = statistics.tokens as f64 / documents;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "TextSearch scores",
        resources.execution_memory.blocking_operator_bytes,
    );
    let mut memory = account.reserve(0)?;
    // Query terms, the needed-lengths set and the score map are admitted
    // before each allocation, like the postings and identities below.
    let terms = analyzer.admitted_query_terms(query, |bytes| memory.grow(bytes))?;
    memory.grow(
        terms
            .len()
            .saturating_mul(std::mem::size_of::<(f64, Vec<SearchPosting>)>()),
    )?;
    let mut work = 0usize;
    let mut postings_visited = 0usize;
    let mut posting_blocks_read = 0usize;
    let max_work = resources.limits.max_candidate_work;
    let mut charge_work = |amount: usize| {
        work = work.saturating_add(amount);
        if work > max_work {
            return Err(HawDBError::read_budget_exceeded(
                hawdb_core::ReadBudgetResource::Rows,
                max_work,
                "text_search postings exceed SQL candidate work admission",
            ));
        }
        Ok(())
    };
    // Phase 1: every query term's postings and IDF, in query-term order.
    let mut gathered = Vec::<(f64, Vec<SearchPosting>)>::with_capacity(terms.len());
    let mut needed = BTreeSet::<u64>::new();
    for term in &terms {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let Some(row) = runtime.read_point(index, &search_index_dictionary_key(term))? else {
            continue;
        };
        let dictionary = SearchTermDictionary::decode(data(&row)?).map_err(integrity)?;
        // The dictionary carries the document frequency, so an over-budget
        // term is refused before any of its blocks is read.
        let frequency = usize::try_from(dictionary.document_frequency).unwrap_or(usize::MAX);
        charge_work(frequency)?;
        postings_visited = postings_visited.saturating_add(frequency);
        memory.grow(frequency.saturating_mul(std::mem::size_of::<SearchPosting>()))?;
        let mut postings = Vec::with_capacity(frequency);
        if dictionary.sealed_blocks > 0 {
            let mut blocks = 0u64;
            runtime.visit_primary_key_range(
                index,
                &search_index_sealed_block_range(term).map_err(integrity)?,
                |row| {
                    blocks += 1;
                    let fence = search_index_block_fence(row.value(2)?).map_err(integrity)?;
                    decode_search_sealed_block(fence, data(&row)?, &mut postings)
                        .map_err(integrity)?;
                    Ok(true)
                },
            )?;
            if blocks != dictionary.sealed_blocks {
                return Err(HawDBError::StorageIntegrity(
                    "search index sealed block count disagrees with its dictionary".into(),
                ));
            }
            posting_blocks_read =
                posting_blocks_read.saturating_add(usize::try_from(blocks).unwrap_or(usize::MAX));
        }
        posting_blocks_read =
            posting_blocks_read.saturating_add(usize::from(!dictionary.tail.is_empty()));
        postings.extend_from_slice(&dictionary.tail);
        if postings.len() as u64 != dictionary.document_frequency
            || postings
                .windows(2)
                .any(|pair| pair[0].ordinal >= pair[1].ordinal)
        {
            return Err(HawDBError::StorageIntegrity(
                "search index postings disagree with their document frequency".into(),
            ));
        }
        if postings.is_empty() {
            continue;
        }
        // Ordinals ascend within a term, so each block is checked once.
        let mut previous = None;
        for posting in &postings {
            let block = posting.ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
            if previous != Some(block) {
                previous = Some(block);
                if !needed.contains(&block) {
                    memory.grow(btree_entry_bytes::<u64, ()>())?;
                    needed.insert(block);
                }
            }
        }
        let frequency = postings.len() as f64;
        let idf = (1.0 + (documents - frequency + 0.5) / (frequency + 0.5)).ln();
        gathered.push((idf, postings));
    }
    let (Some(&first), Some(&last)) = (needed.first(), needed.last()) else {
        return Ok(RankedSearch {
            rows: Vec::new(),
            row_evidence: runtime.evidence(),
            work: TextSearchWork::default(),
        });
    };
    // Phase 2: the lengths rows of every scored document, in one ordered
    // range scan rather than one point read per row.
    let mut lengths = BTreeMap::<u64, SearchLengthBlock>::new();
    runtime.visit_primary_key_range(
        index,
        &search_index_length_block_range(
            first * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
            last * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
        )
        .map_err(integrity)?,
        |row| {
            charge_work(1)?;
            let block = search_index_length_block_start(row.value(2)?).map_err(integrity)?
                / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
            if needed.contains(&block) {
                let data = data(&row)?;
                memory.grow(data.len().saturating_mul(2).saturating_add(96))?;
                lengths.insert(block, SearchLengthBlock::decode(data).map_err(integrity)?);
            }
            Ok(true)
        },
    )?;
    let entry = |ordinal: u64| {
        lengths
            .get(&(ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS))
            .and_then(|block| block.get(ordinal))
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("search index posting names an absent document".into())
            })
    };
    // Phase 3: scores, accumulated per document in query-term order with the
    // same floating-point operations as a brute-force evaluation.
    let mut scores = BTreeMap::<u64, f64>::new();
    for (idf, postings) in &gathered {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        for posting in postings {
            let length = entry(posting.ordinal)?.length;
            let term_frequency = posting.frequency as f64;
            let normalization = BM25_K1 * (1.0 - BM25_B + BM25_B * length as f64 / average_length);
            let contribution =
                idf * term_frequency * (BM25_K1 + 1.0) / (term_frequency + normalization);
            let score = match scores.entry(posting.ordinal) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // A document's first matching term admits its map entry.
                    memory.grow(btree_entry_bytes::<u64, f64>())?;
                    entry.insert(0.0)
                }
            };
            *score += contribution;
        }
    }
    drop(gathered);
    memory.grow(
        scores
            .len()
            .saturating_mul(std::mem::size_of::<(String, f64)>()),
    )?;
    let mut ranked = Vec::with_capacity(scores.len());
    for (ordinal, score) in scores {
        let document = &entry(ordinal)?.key;
        memory.grow(document.len().saturating_mul(2).saturating_add(2))?;
        ranked.push((identity(document), score));
    }
    // Hex identities preserve the encoded-key byte order used for ties.
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(RankedSearch {
        rows: ranked,
        row_evidence: runtime.evidence(),
        work: TextSearchWork {
            postings_visited,
            posting_blocks_read,
            posting_blocks_skipped: 0,
        },
    })
}

fn output_schema(select: &SelectStatement, source: &RelationalTableSchema) -> Result<QuerySchema> {
    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectProjection::Wildcard => {
                columns.extend(source.columns.iter().map(|column| column.name.clone()));
                columns.push(TEXT_SEARCH_SCORE_COLUMN.to_owned());
            }
            SelectProjection::Expression { expression, alias } => {
                columns.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| crate::query_value::expression_name(expression)),
                );
            }
        }
    }
    QuerySchema::try_new(columns)
}

fn validate_output_bindings(
    select: &SelectStatement,
    parameters: &[Value],
    source: &RelationalTableSchema,
) -> Result<()> {
    if source.column_position(TEXT_SEARCH_SCORE_COLUMN).is_some() {
        return Err(semantic(format!(
            "text_search reserves the {TEXT_SEARCH_SCORE_COLUMN} output column"
        )));
    }
    let mut schema = RelationalTableSchema {
        materialized_view: None,
        name: source.name.clone(),
        columns: source
            .columns
            .iter()
            .map(|column| RelationalColumnSchema {
                name: column.name.clone(),
                scalar_type: column.scalar_type,
                nullable: column.nullable,
                default: None,
            })
            .collect(),
        primary_key: Vec::new(),
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    schema.columns.push(RelationalColumnSchema {
        name: TEXT_SEARCH_SCORE_COLUMN.into(),
        scalar_type: RelationalScalarType::DoublePrecision,
        nullable: false,
        default: None,
    });
    let qualifier = select.from_alias.as_deref().unwrap_or("text_search");
    let empty = BoundRow {
        bindings: vec![Binding {
            binding: BindingId::new(0),
            table: &source.name,
            qualifier,
            schema: &schema,
            row: None,
        }],
    };
    let mut projection = project_bound_row(&empty, &select.projection, parameters)?;
    add_relational_order_keys(select, &empty, &mut projection)?;
    let _predicate = select
        .selection
        .as_ref()
        .map(|predicate| {
            BoundStreamingPredicate::bind(predicate, parameters, &schema, &source.name, qualifier)
        })
        .transpose()?;
    Ok(())
}

fn search_access_path(
    index: &str,
    column: &str,
    estimated_rows: usize,
) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::Index,
        name: index.to_owned(),
        index_columns: vec![column.to_owned()],
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        range: None,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: true,
        estimated_rows,
    }
}

pub(super) fn execute_text_search_explain<R: RelationalQueryStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
    analyze: bool,
) -> Result<RelationalQueryOutput> {
    validate_candidate_select(select, "text_search")?;
    let bound = bind_call(
        select.text_search().expect("text_search source"),
        parameters,
    )?;
    let index_schema = state
        .table_schema(bound.index)
        .ok_or_else(|| semantic(format!("unknown search index {}", bound.index)))?;
    let (Some(spec), Some(definition)) = (
        index_schema.search_index(),
        index_schema.materialized_view.as_deref(),
    ) else {
        return Err(semantic(format!("{} is not a search index", bound.index)));
    };
    let source = state.table_schema(&definition.source).ok_or_else(|| {
        HawDBError::StorageIntegrity("search index source table is absent".into())
    })?;
    // EXPLAIN's generic formatter expects a real base table name. Replace only
    // its diagnostic source; execution still sees the original table function.
    let mut explained = select.clone();
    explained.from = Some(hawdb_sql::SqlTableName {
        schema: None,
        name: definition.source.clone(),
    });
    explained.table_function = None;
    let output = if analyze {
        execute_text_search_select(
            select,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        )?
    } else {
        // Bind the same virtual source shape as execution without opening a
        // row or index runtime. Invalid names must not become EXPLAIN-only
        // successes merely because no candidates are hydrated.
        validate_output_bindings(select, parameters, source)?;
        let _schema = output_schema(select, source)?;
        RelationalQueryOutput {
            rows: QueryRows::empty(),
            stage_timings,
            join_planning: RelationalJoinPlanningOutcome::not_eligible(
                RelationalJoinPlanningReason::NoJoin,
                Vec::new(),
                RelationalJoinEnumerationConfig::default(),
            ),
            operator_cardinality_profiles: Vec::new(),
            intermediate_rows: 0,
            hydration: resources.limits.hydration,
            access_path: search_access_path(bound.index, &spec.column, bound.top_k),
            join_access_paths: Vec::new(),
            index_execution_evidence: Vec::new(),
            row_execution_evidence: RelationalRowExecutionEvidence {
                runtime_path: "not_executed",
                ..RelationalRowExecutionEvidence::default()
            },
            blocking_operator_memory_reports: Vec::new(),
        }
    };
    super::format_relational_explain(&explained, parameters, output, analyze, resources.limits)
}

pub(super) fn execute_text_search_select<R: RelationalQueryStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let call = select.text_search().expect("text_search source");
    if select.vector_index().is_some() {
        return Err(semantic(
            "text_search cannot be combined with vector_index_search",
        ));
    }
    validate_candidate_select(select, "text_search")?;
    let bound = bind_call(call, parameters)?;
    let index_schema = state
        .table_schema(bound.index)
        .ok_or_else(|| semantic(format!("unknown search index {}", bound.index)))?;
    let (Some(spec), Some(definition)) = (
        index_schema.search_index(),
        index_schema.materialized_view.as_deref(),
    ) else {
        return Err(semantic(format!("{} is not a search index", bound.index)));
    };
    let source = state.table_schema(&definition.source).ok_or_else(|| {
        HawDBError::StorageIntegrity("search index source table is absent".into())
    })?;
    let schema = output_schema(select, source)?;
    let ledger = QueryMemoryLedger::new(hawdb_executor::memory::enforced_query_memory_budget(
        resources.execution_memory,
        resources.task_context,
    )?);
    // A residual predicate must be evaluated before the rank window is cut.
    // Until it can be pushed into this physical operator, retain exhaustive
    // scoring for that case; unfiltered searches use exact Block-Max WAND.
    let request = RankRequest {
        index: bound.index,
        analyzer: spec.analyzer,
        query: bound.query,
        top_k: bound.top_k,
    };
    let ranked = if select.selection.is_some() {
        rank_exhaustive(state, request, read_modes.row, &resources, &ledger)?
    } else {
        rank_top_k(state, request, read_modes.row, &resources, &ledger)?
    };
    // Keep the rank scan's evidence on the output even when nothing matched.
    let candidate_count = ranked.rows.len();
    let candidates = RankedCandidates(ranked.rows);
    let rows = execute_candidate_select(
        select,
        parameters,
        VectorIndexInput {
            table: &definition.source,
            candidates: &candidates,
        },
        VectorIndexSqlExecution {
            state,
            row_mode: read_modes.row,
            resources,
            memory_ledger: &ledger,
        },
        CandidateScore {
            function: "text_search",
            context: "text_search",
            column: TEXT_SEARCH_SCORE_COLUMN,
            valid: |score| score.is_finite() && score >= 0.0,
            invalid: "invalid text_search BM25 score",
            absent: "text_search candidate is absent from the bound SQL snapshot",
            max_accepted: Some(bound.top_k),
        },
    )?;
    let (rows, mut row_execution) = rows.into_parts();
    row_execution.rows_visited = row_execution
        .rows_visited
        .saturating_add(ranked.row_evidence.rows_visited);
    row_execution.logical_pages = row_execution
        .logical_pages
        .saturating_add(ranked.row_evidence.logical_pages);
    row_execution.logical_bytes = row_execution
        .logical_bytes
        .saturating_add(ranked.row_evidence.logical_bytes);
    row_execution.postings_visited = ranked.work.postings_visited;
    row_execution.posting_blocks_read = ranked.work.posting_blocks_read;
    row_execution.posting_blocks_skipped = ranked.work.posting_blocks_skipped;
    Ok(RelationalQueryOutput {
        rows: QueryRows::try_from_named_rows(schema, rows)?,
        stage_timings,
        join_planning: RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::NoJoin,
            Vec::new(),
            RelationalJoinEnumerationConfig::default(),
        ),
        operator_cardinality_profiles: Vec::new(),
        intermediate_rows: candidate_count,
        hydration: resources.limits.hydration,
        access_path: search_access_path(bound.index, &spec.column, candidate_count),
        join_access_paths: Vec::new(),
        index_execution_evidence: Vec::new(),
        row_execution_evidence: row_execution,
        blocking_operator_memory_reports: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_round_trip_through_primary_key_encoding() {
        let key = hawdb_storage::relational::RelationalKey(vec![RelationalValue::BigInt(7)]);
        let encoded = hawdb_storage::relational::encode_relational_primary_key(&key).unwrap();
        let identity = identity(&encoded);
        assert!(identity.starts_with("r:"));
        assert_eq!(identity.len(), 2 + encoded.len() * 2);
    }
}
