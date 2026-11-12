// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Engine-owned BM25 postings over one TEXT column of a RowPage table.
//!
//! A search index is a synchronously maintained derived relation: its
//! compact posting blocks, per-document lengths and corpus statistics are
//! ordinary rows of a hidden RowPage table that the base transaction writes
//! before it commits (layout in `layout`).
//! WAL replay re-derives them from the same deterministic analyzer, so there is
//! no separately acknowledged projection, watermark or catch-up job.

use super::*;

mod layout;
use layout::{
    decode_document_ordinal, document_key, put_varint, sealed_block_key, sealed_block_range,
    MAX_BLOCK_POSTINGS, MIN_BLOCK_POSTINGS,
};
pub use layout::{
    decode_search_sealed_block, search_index_block_fence, search_index_dictionary_key,
    search_index_length_block_key, search_index_length_block_range,
    search_index_length_block_start, search_index_sealed_block_range, search_index_statistics_key,
    SearchDocumentEntry, SearchIndexStatistics, SearchLengthBlock, SearchPosting,
    SearchTermDictionary, SEARCH_INDEX_BLOCK_POSTINGS, SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
    SEARCH_INDEX_TAIL_POSTINGS,
};

/// Every row of a search index relation is `(kind, name, slot) -> data`; see
/// `layout` for the row kinds and their encodings.
pub const SEARCH_INDEX_KIND_COLUMN: &str = "__hawdb_search_kind";
pub const SEARCH_INDEX_NAME_COLUMN: &str = "__hawdb_search_name";
pub const SEARCH_INDEX_SLOT_COLUMN: &str = "__hawdb_search_slot";
pub const SEARCH_INDEX_DATA_COLUMN: &str = "__hawdb_search_data";
/// Number of physical columns in a search index relation.
pub const SEARCH_INDEX_COLUMN_COUNT: usize = 4;
/// The physical layout recorded with every search index definition. Readers
/// refuse any other layout as an unsupported format.
pub(crate) const SEARCH_INDEX_LAYOUT_TAG: u8 = 1;
/// Tokens longer than this many bytes are not indexed (encoded blobs, hashes).
pub const SEARCH_INDEX_MAX_TOKEN_BYTES: usize = 128;

/// The deterministic text analysis bound into a persistent definition. A new
/// analysis is a new variant; an existing one never changes its output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchIndexAnalyzer {
    /// Lowercased Unicode letter/digit words, with full-width ASCII folded to
    /// ASCII; Han, Kana and Hangul runs indexed as character unigrams and
    /// overlapping bigrams.
    CjkNgram,
}

impl SearchIndexAnalyzer {
    pub const DEFAULT: Self = Self::CjkNgram;

    pub fn name(self) -> &'static str {
        match self {
            Self::CjkNgram => "cjk_ngram",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        name.eq_ignore_ascii_case("cjk_ngram")
            .then_some(Self::CjkNgram)
    }

    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::CjkNgram => 1,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        (tag == 1).then_some(Self::CjkNgram)
    }

    /// Index-side tokens in document order, with repetition. The count is
    /// the BM25 document length. Allocates every token; maintenance uses
    /// [`Self::document_terms`] instead.
    pub fn index_tokens(self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let Ok(()) = self.visit_index_tokens::<std::convert::Infallible>(text, |token| {
            tokens.push(token.to_owned());
            Ok(())
        });
        tokens
    }

    /// Visits index-side tokens in document order, with repetition. Tokens
    /// borrow bounded scratch buffers, so analysis itself allocates nothing
    /// proportional to the text, and `visit` can stop it at any token.
    pub fn visit_index_tokens<E>(
        self,
        text: &str,
        mut visit: impl FnMut(&str) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut buffer = [0u8; 8];
        analyze(text, |piece| match piece {
            Piece::Word(word) => visit(word),
            Piece::Ideograph { previous, ch } => {
                if let Some(previous) = previous {
                    visit(ideographic_token(&mut buffer, Some(previous), ch))?;
                }
                visit(ideographic_token(&mut buffer, None, ch))
            }
            Piece::LoneIdeograph(_) => Ok(()),
        })
    }

    /// One document's distinct index terms with their frequencies, and its
    /// length. `admit` is called with an estimate of the bytes each new
    /// distinct term will occupy before that term is allocated, and with 0
    /// every [`SEARCH_INDEX_ANALYSIS_CHECKPOINT_TOKENS`] tokens as a
    /// cancellation checkpoint; its first error stops the analysis. Repeated
    /// tokens allocate nothing.
    pub fn document_terms<E>(
        self,
        text: &str,
        mut admit: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(BTreeMap<String, u64>, u64), E> {
        let mut terms = BTreeMap::<String, u64>::new();
        let mut length = 0u64;
        self.visit_index_tokens(text, |token| {
            length += 1;
            if length.is_multiple_of(SEARCH_INDEX_ANALYSIS_CHECKPOINT_TOKENS) {
                admit(0)?;
            }
            match terms.get_mut(token) {
                Some(frequency) => *frequency += 1,
                None => {
                    admit(search_index_term_entry_bytes(token))?;
                    terms.insert(token.to_owned(), 1);
                }
            }
            Ok(())
        })?;
        Ok((terms, length))
    }

    /// Distinct query-side terms in ascending order. Multi-character
    /// ideographic runs use their bigrams, which are more selective than the
    /// unigrams also present in the index; a lone character uses its unigram.
    pub fn query_terms(self, text: &str) -> Vec<String> {
        let Ok(terms) = self.admitted_query_terms::<std::convert::Infallible>(text, |_| Ok(()));
        terms
    }

    /// [`Self::query_terms`], calling `admit` with each new distinct term's
    /// estimated bytes before allocating it.
    pub fn admitted_query_terms<E>(
        self,
        text: &str,
        mut admit: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Vec<String>, E> {
        let mut terms = BTreeSet::<String>::new();
        let mut buffer = [0u8; 8];
        let mut insert = |term: &str| {
            if !terms.contains(term) {
                admit(search_index_term_entry_bytes(term))?;
                terms.insert(term.to_owned());
            }
            Ok(())
        };
        analyze(text, |piece| match piece {
            Piece::Word(word) => insert(word),
            Piece::Ideograph {
                previous: Some(previous),
                ch,
            } => insert(ideographic_token(&mut buffer, Some(previous), ch)),
            Piece::Ideograph { previous: None, .. } => Ok(()),
            Piece::LoneIdeograph(ch) => insert(ideographic_token(&mut buffer, None, ch)),
        })?;
        Ok(terms.into_iter().collect())
    }
}

/// The search-specific part of a derived relation definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchIndexSpec {
    pub column: String,
    pub analyzer: SearchIndexAnalyzer,
}

fn fold(ch: char) -> char {
    // Full-width ASCII forms (U+FF01..=U+FF5E) and the ideographic space.
    match ch as u32 {
        0xFF01..=0xFF5E => char::from_u32(ch as u32 - 0xFEE0).unwrap_or(ch),
        0x3000 => ' ',
        _ => ch,
    }
}

pub(crate) fn is_ideographic(ch: char) -> bool {
    matches!(
        ch as u32,
        0x1100..=0x11FF // Hangul Jamo
            | 0x2E80..=0x2FDF // CJK radicals, Kangxi radicals
            | 0x3040..=0x309F // Hiragana
            | 0x30A0..=0x30FF // Katakana
            | 0x3130..=0x318F // Hangul compatibility Jamo
            | 0x31F0..=0x31FF // Katakana phonetic extensions
            | 0x3400..=0x4DBF // CJK extension A
            | 0x4E00..=0x9FFF // CJK unified ideographs
            | 0xAC00..=0xD7AF // Hangul syllables
            | 0xF900..=0xFAFF // CJK compatibility ideographs
            | 0x20000..=0x323AF // CJK extensions B..H
    )
}

/// One analysis event. Words borrow a scratch buffer bounded by
/// [`SEARCH_INDEX_MAX_TOKEN_BYTES`]; ideographic runs are streamed one
/// character at a time, so nothing proportional to the text is buffered.
enum Piece<'a> {
    Word(&'a str),
    /// An ideographic character and its predecessor in the same run.
    Ideograph {
        previous: Option<char>,
        ch: char,
    },
    /// The end of an ideographic run of exactly one character.
    LoneIdeograph(char),
}

/// A unigram (`previous == None`) or bigram encoded into `buffer`.
fn ideographic_token(buffer: &mut [u8; 8], previous: Option<char>, ch: char) -> &str {
    let mut length = 0;
    if let Some(previous) = previous {
        length = previous.encode_utf8(&mut buffer[..]).len();
    }
    length += ch.encode_utf8(&mut buffer[length..]).len();
    std::str::from_utf8(&buffer[..length]).expect("encoded characters are UTF-8")
}

/// Estimated resident bytes of one distinct analysis term in a B-tree map
/// or set: the term's bytes plus twice the entry (B-tree nodes other than
/// the root are at least half full) plus node headers.
pub fn search_index_term_entry_bytes(term: &str) -> usize {
    term.len()
        .saturating_add(2 * (std::mem::size_of::<String>() + std::mem::size_of::<u64>()))
        .saturating_add(16)
}

/// Tokens between analysis cancellation checkpoints.
pub const SEARCH_INDEX_ANALYSIS_CHECKPOINT_TOKENS: u64 = 4096;

struct Scanner {
    word: String,
    /// The current word already exceeds the token bound; the rest of it is
    /// skipped instead of buffered.
    oversized: bool,
    previous: Option<char>,
    run: usize,
}

impl Scanner {
    fn flush_word<E>(
        &mut self,
        emit: &mut impl FnMut(Piece<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let oversized = std::mem::take(&mut self.oversized);
        if !oversized && !self.word.is_empty() {
            emit(Piece::Word(&self.word))?;
        }
        self.word.clear();
        Ok(())
    }

    fn flush_run<E>(&mut self, emit: &mut impl FnMut(Piece<'_>) -> Result<(), E>) -> Result<(), E> {
        let previous = self.previous.take();
        if let (1, Some(ch)) = (std::mem::take(&mut self.run), previous) {
            emit(Piece::LoneIdeograph(ch))?;
        }
        Ok(())
    }
}

fn analyze<E>(text: &str, mut emit: impl FnMut(Piece<'_>) -> Result<(), E>) -> Result<(), E> {
    let mut scanner = Scanner {
        word: String::new(),
        oversized: false,
        previous: None,
        run: 0,
    };
    for ch in text.chars().map(fold) {
        if is_ideographic(ch) {
            scanner.flush_word(&mut emit)?;
            emit(Piece::Ideograph {
                previous: scanner.previous,
                ch,
            })?;
            scanner.previous = Some(ch);
            scanner.run = scanner.run.saturating_add(1);
        } else if ch.is_alphanumeric() {
            scanner.flush_run(&mut emit)?;
            if !scanner.oversized {
                scanner.word.extend(ch.to_lowercase());
                if scanner.word.len() > SEARCH_INDEX_MAX_TOKEN_BYTES {
                    scanner.oversized = true;
                    scanner.word.clear();
                }
            }
        } else {
            scanner.flush_word(&mut emit)?;
            scanner.flush_run(&mut emit)?;
        }
    }
    scanner.flush_word(&mut emit)?;
    scanner.flush_run(&mut emit)
}

pub(super) fn table_schema(
    name: String,
    definition: &MaterializedViewDefinition,
    spec: &SearchIndexSpec,
    source: &RelationalTableSchema,
) -> Result<RelationalTableSchema, RelationalError> {
    if !definition.columns.is_empty()
        || !definition.group_by.is_empty()
        || definition.filter.is_some()
    {
        return Err(RelationalError::Schema(
            "search index definitions have no projection, grouping or filter".into(),
        ));
    }
    let column = source_column(source, &spec.column)?;
    if column.scalar_type != RelationalScalarType::Text {
        return Err(RelationalError::Schema(format!(
            "search index column {} must be TEXT",
            spec.column
        )));
    }
    let internal = |name: &str, scalar_type| RelationalColumnSchema {
        name: name.into(),
        scalar_type,
        nullable: false,
        default: None,
    };
    let schema = RelationalTableSchema {
        name,
        columns: vec![
            internal(SEARCH_INDEX_KIND_COLUMN, RelationalScalarType::BigInt),
            internal(SEARCH_INDEX_NAME_COLUMN, RelationalScalarType::Bytea),
            internal(SEARCH_INDEX_SLOT_COLUMN, RelationalScalarType::BigInt),
            internal(SEARCH_INDEX_DATA_COLUMN, RelationalScalarType::Bytea),
        ],
        primary_key: vec![
            SEARCH_INDEX_KIND_COLUMN.into(),
            SEARCH_INDEX_NAME_COLUMN.into(),
            SEARCH_INDEX_SLOT_COLUMN.into(),
        ],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        materialized_view: Some(Box::new(definition.clone())),
    };
    validate_table_schema(&schema)?;
    Ok(schema)
}

fn analyzed_document(
    spec: &SearchIndexSpec,
    source: &RelationalTableSchema,
    row: Option<&RelationalRow>,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<(BTreeMap<String, u64>, u64), RelationalError> {
    let Some(row) = row else {
        return Ok((BTreeMap::new(), 0));
    };
    let text = match value(source, row, &spec.column) {
        RelationalValue::Null => return Ok((BTreeMap::new(), 0)),
        RelationalValue::Text(text) => text.as_str(),
        _ => {
            return Err(RelationalError::Corruption(
                "search index source value is not resident TEXT".into(),
            ))
        }
    };
    // Each distinct term is admitted against the transaction payload budget
    // before it is allocated, which bounds one document's analysis state;
    // a large ideographic value is refused before it expands into terms.
    spec.analyzer
        .document_terms(text, |bytes| budget.charge(0, bytes))
}

fn corruption(message: &str) -> RelationalError {
    RelationalError::Corruption(format!("search index {message}"))
}

pub(super) struct SearchMaintenance<'a, 'b> {
    pub previous: &'a RelationalState,
    pub next: &'a mut RelationalState,
    pub schema: &'a RelationalTableSchema,
    pub source: &'a RelationalTableSchema,
    pub spec: &'a SearchIndexSpec,
    pub rebuild: bool,
    pub view_keys: &'a mut BTreeSet<RelationalKey>,
    pub overflow_config: RelationalOverflowConfig,
    pub budget: &'a mut MaintenanceBudget<'b>,
}

/// Reads and writes rows of one search index relation inside the commit.
struct IndexRows<'a, 'b> {
    next: &'a mut RelationalState,
    schema: &'a RelationalTableSchema,
    view_keys: &'a mut BTreeSet<RelationalKey>,
    overflow_config: RelationalOverflowConfig,
    budget: &'a mut MaintenanceBudget<'b>,
}

impl IndexRows<'_, '_> {
    fn read(&mut self, key: &RelationalKey) -> Result<Option<Vec<u8>>, RelationalError> {
        let Some(row) = self.budget.read(self.next, &self.schema.name, key)? else {
            return Ok(None);
        };
        match row.values() {
            [_, _, _, RelationalValue::Bytea(data)] => Ok(Some(data.clone())),
            _ => Err(corruption("row has an invalid payload")),
        }
    }

    fn write(&mut self, key: RelationalKey, data: Option<Vec<u8>>) -> Result<(), RelationalError> {
        let row = data.map(|data| {
            let mut values = key.0.clone();
            values.push(RelationalValue::Bytea(data));
            RelationalRow::new(values)
        });
        write_output(
            self.next,
            self.schema,
            key,
            row,
            self.view_keys,
            self.overflow_config,
            self.budget,
        )
    }

    /// The fence of the first (`last == false`) or last sealed block key in
    /// `range`, located without hydrating any block.
    fn fence(
        &mut self,
        range: RelationalPrimaryKeyRange,
        last: bool,
    ) -> Result<Option<u64>, RelationalError> {
        self.budget.charge(1, 0)?;
        let rows = &self
            .next
            .segments
            .get(&self.schema.name)
            .ok_or_else(|| corruption("relation is absent"))?
            .rows;
        let mut keys = rows.range(Some(range)).map(|(key, _)| key);
        let key = if last { keys.next_back() } else { keys.next() };
        key.map(|key| search_index_block_fence(&key.0[2]))
            .transpose()
    }
}

/// Applies ordinal-sorted changes (`Some(tf)` sets, `None` removes) to an
/// ordinal-sorted posting list, keeping `frequency` (the term's document
/// frequency) exact.
fn apply_changes(
    postings: &mut Vec<SearchPosting>,
    changes: &[(u64, Option<u64>)],
    frequency: &mut u64,
) -> Result<(), RelationalError> {
    let mut merged = Vec::with_capacity(postings.len() + changes.len());
    let mut existing = std::mem::take(postings).into_iter().peekable();
    for &(ordinal, change) in changes {
        while let Some(posting) = existing.next_if(|posting| posting.ordinal < ordinal) {
            merged.push(posting);
        }
        let present = existing
            .next_if(|posting| posting.ordinal == ordinal)
            .is_some();
        match change {
            Some(term_frequency) => {
                if !present {
                    *frequency = frequency
                        .checked_add(1)
                        .ok_or_else(|| corruption("document frequency overflows"))?;
                }
                merged.push(SearchPosting {
                    ordinal,
                    frequency: term_frequency,
                });
            }
            None if present => {
                *frequency = frequency
                    .checked_sub(1)
                    .ok_or_else(|| corruption("document frequency is negative"))?;
            }
            None => return Err(corruption("posting to remove is missing")),
        }
    }
    merged.extend(existing);
    *postings = merged;
    Ok(())
}

fn encode_ordinal(ordinal: u64) -> Vec<u8> {
    let mut data = Vec::with_capacity(4);
    put_varint(&mut data, ordinal);
    data
}

pub(super) fn maintain(
    maintenance: SearchMaintenance<'_, '_>,
    keys: &BTreeSet<RelationalKey>,
) -> Result<(), RelationalError> {
    let SearchMaintenance {
        previous,
        next,
        schema,
        source,
        spec,
        rebuild,
        view_keys,
        overflow_config,
        budget,
    } = maintenance;
    let mut index = IndexRows {
        next,
        schema,
        view_keys,
        overflow_config,
        budget,
    };
    let statistics_key = search_index_statistics_key();
    let stored = index
        .read(&statistics_key)?
        .map(|data| SearchIndexStatistics::decode(&data))
        .transpose()?;
    let mut statistics = stored.unwrap_or_default();
    // Changes are gathered for the whole commit first so that each term's
    // dictionary row and each touched block is rewritten once per commit.
    let mut terms = BTreeMap::<String, BTreeMap<u64, Option<u64>>>::new();
    let mut lengths = BTreeMap::<u64, Option<SearchDocumentEntry>>::new();
    for key in keys {
        let old = if rebuild {
            None
        } else {
            index.budget.read(previous, &source.name, key)?
        };
        // Overflowed TEXT is hydrated by the maintenance read before analysis.
        let new = index.budget.read(index.next, &source.name, key)?;
        let (old_terms, old_length) = analyzed_document(spec, source, old.as_ref(), index.budget)?;
        let (new_terms, new_length) = analyzed_document(spec, source, new.as_ref(), index.budget)?;
        if old_terms == new_terms {
            continue;
        }
        let document = encode_relational_primary_key(key)?;
        index.budget.charge(0, document.len())?;
        let document_row = document_key(&document);
        let ordinal = if old_terms.is_empty() {
            // A document entering the corpus gets the next ordinal; WAL replay
            // assigns the same one because it replays the same key order.
            let ordinal = statistics.allocate_ordinal()?;
            index.write(document_row, Some(encode_ordinal(ordinal)))?;
            statistics.documents += 1;
            ordinal
        } else {
            let data = index
                .read(&document_row)?
                .ok_or_else(|| corruption("document row is missing"))?;
            let ordinal = decode_document_ordinal(&data)?;
            if new_terms.is_empty() {
                index.write(document_row, None)?;
                statistics.documents = statistics
                    .documents
                    .checked_sub(1)
                    .ok_or_else(|| corruption("document count is negative"))?;
            }
            ordinal
        };
        statistics.tokens = statistics
            .tokens
            .checked_add(new_length)
            .and_then(|tokens| tokens.checked_sub(old_length))
            .ok_or_else(|| corruption("token count is out of range"))?;
        if new_terms.is_empty() {
            lengths.insert(ordinal, None);
        } else if old_terms.is_empty() || old_length != new_length {
            lengths.insert(
                ordinal,
                Some(SearchDocumentEntry {
                    length: new_length,
                    key: document,
                }),
            );
        }
        for term in old_terms.keys() {
            if !new_terms.contains_key(term) {
                index.budget.charge(0, term.len().saturating_add(16))?;
                terms.entry(term.clone()).or_default().insert(ordinal, None);
            }
        }
        for (term, frequency) in new_terms {
            if old_terms.get(&term) != Some(&frequency) {
                index.budget.charge(0, term.len().saturating_add(16))?;
                terms
                    .entry(term)
                    .or_default()
                    .insert(ordinal, Some(frequency));
            }
        }
    }
    let mut block_changes = BTreeMap::<u64, Vec<(u64, Option<SearchDocumentEntry>)>>::new();
    for (ordinal, entry) in lengths {
        block_changes
            .entry(ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS)
            .or_default()
            .push((ordinal, entry));
    }
    for (block, changes) in block_changes {
        let key = search_index_length_block_key(block * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS);
        let mut lengths = index
            .read(&key)?
            .map(|data| SearchLengthBlock::decode(&data))
            .transpose()?
            .unwrap_or_default();
        for (ordinal, entry) in changes {
            lengths.set(ordinal, entry);
        }
        index.write(key, lengths.encode())?;
    }
    for (term, changes) in terms {
        let changes = changes.into_iter().collect::<Vec<_>>();
        maintain_term(&mut index, &term, &changes)?;
    }
    if stored != Some(statistics) {
        // An empty corpus leaves an empty relation: every document, lengths
        // and term row has been removed with its last document.
        let data = (statistics.documents > 0).then(|| statistics.encode());
        if data.is_some() || stored.is_some() {
            index.write(statistics_key, data)?;
        }
    }
    Ok(())
}

/// Applies one commit's ordinal-sorted changes to one term. Appends touch
/// only the dictionary row (flushing a full tail into sealed blocks); changes to
/// older documents rewrite the sealed blocks that own them. Every rewritten
/// block holds at most `MAX_BLOCK_POSTINGS` postings, so the work is bounded
/// by the number of changes rather than by the term's document frequency.
fn maintain_term(
    index: &mut IndexRows<'_, '_>,
    term: &str,
    changes: &[(u64, Option<u64>)],
) -> Result<(), RelationalError> {
    let dictionary_key = search_index_dictionary_key(term);
    let stored = index.read(&dictionary_key)?;
    let existed = stored.is_some();
    let mut dictionary = stored
        .map(|data| SearchTermDictionary::decode(&data))
        .transpose()?
        .unwrap_or_default();
    let split = changes.partition_point(|(ordinal, _)| *ordinal < dictionary.tail_start);
    let (sealed_changes, tail_changes) = changes.split_at(split);
    // Route each older change to the sealed block that owns its ordinal:
    // the block with the greatest fence at or below it.
    let mut owned = BTreeMap::<u64, Vec<(u64, Option<u64>)>>::new();
    let mut owner: Option<(u64, Option<u64>)> = None;
    for &(ordinal, change) in sealed_changes {
        let fence = match owner {
            Some((fence, upper)) if upper.is_none_or(|upper| ordinal < upper) => fence,
            _ => {
                let fence = index
                    .fence(
                        sealed_block_range(term, (0, true), Some((ordinal, true)))?,
                        true,
                    )?
                    .ok_or_else(|| corruption("posting has no owning block"))?;
                let upper = index.fence(sealed_block_range(term, (fence, false), None)?, false)?;
                owner = Some((fence, upper));
                fence
            }
        };
        owned.entry(fence).or_default().push((ordinal, change));
    }
    let mut touched = Vec::with_capacity(owned.len());
    for (fence, changes) in owned {
        let key = sealed_block_key(term, fence);
        let data = index
            .read(&key)?
            .ok_or_else(|| corruption("sealed block is missing"))?;
        let mut postings = Vec::new();
        decode_search_sealed_block(fence, &data, &mut postings)?;
        apply_changes(&mut postings, &changes, &mut dictionary.document_frequency)?;
        if postings.len() > MAX_BLOCK_POSTINGS {
            // Many older documents gained this term in one commit: split into
            // full blocks keyed by their first ordinals.
            let rest = postings.split_off(SEARCH_INDEX_BLOCK_POSTINGS);
            for chunk in rest.chunks(SEARCH_INDEX_BLOCK_POSTINGS) {
                let chunk_fence = chunk[0].ordinal;
                index.write(
                    sealed_block_key(term, chunk_fence),
                    Some(layout::encode_sealed_block(chunk_fence, chunk)),
                )?;
                dictionary.sealed_blocks += 1;
            }
        }
        if postings.is_empty() && fence != 0 {
            // The predecessor's range absorbs this one. The first block keeps
            // fence 0 even when empty so that every older ordinal has an owner.
            index.write(key, None)?;
            dictionary.sealed_blocks = dictionary
                .sealed_blocks
                .checked_sub(1)
                .ok_or_else(|| corruption("sealed block count is negative"))?;
        } else {
            index.write(key, Some(layout::encode_sealed_block(fence, &postings)))?;
            touched.push(fence);
        }
    }
    apply_changes(
        &mut dictionary.tail,
        tail_changes,
        &mut dictionary.document_frequency,
    )?;
    if dictionary.tail.len() >= SEARCH_INDEX_TAIL_POSTINGS {
        // Flush the tail: first top up the last sealed block to a full block,
        // then seal what remains in blocks of up to a full block each. The
        // dictionary row rewritten by every append stays small, and a sealed
        // block is rewritten once per flush, not once per append.
        let mut tail = std::mem::take(&mut dictionary.tail);
        if dictionary.sealed_blocks > 0 {
            let fence = index
                .fence(sealed_block_range(term, (0, true), None)?, true)?
                .ok_or_else(|| corruption("sealed block is missing"))?;
            let key = sealed_block_key(term, fence);
            let data = index
                .read(&key)?
                .ok_or_else(|| corruption("sealed block is missing"))?;
            let mut postings = Vec::new();
            decode_search_sealed_block(fence, &data, &mut postings)?;
            let room = SEARCH_INDEX_BLOCK_POSTINGS.saturating_sub(postings.len());
            if room > 0 {
                postings.extend(tail.drain(..room.min(tail.len())));
                dictionary.tail_start = postings[postings.len() - 1].ordinal + 1;
                index.write(key, Some(layout::encode_sealed_block(fence, &postings)))?;
            }
        }
        while tail.len() >= SEARCH_INDEX_TAIL_POSTINGS {
            let chunk = tail
                .drain(..SEARCH_INDEX_BLOCK_POSTINGS.min(tail.len()))
                .collect::<Vec<_>>();
            let fence = dictionary.tail_start;
            index.write(
                sealed_block_key(term, fence),
                Some(layout::encode_sealed_block(fence, &chunk)),
            )?;
            dictionary.sealed_blocks += 1;
            dictionary.tail_start = chunk[chunk.len() - 1].ordinal + 1;
        }
        dictionary.tail = tail;
    }
    // Compaction: a changed block that shrank below a quarter merges into its
    // predecessor, or absorbs its successor, when the two fit in one block.
    // Delete-heavy churn therefore cannot leave a run of nearly empty blocks,
    // and each changed block costs at most one extra block rewrite.
    for fence in touched {
        let key = sealed_block_key(term, fence);
        let Some(data) = index.read(&key)? else {
            continue;
        };
        let mut postings = Vec::new();
        decode_search_sealed_block(fence, &data, &mut postings)?;
        if postings.len() >= MIN_BLOCK_POSTINGS {
            continue;
        }
        let predecessor = if fence == 0 {
            None
        } else {
            index.fence(
                sealed_block_range(term, (0, true), Some((fence, false)))?,
                true,
            )?
        };
        let successor = index.fence(sealed_block_range(term, (fence, false), None)?, false)?;
        for (neighbor, absorb) in [(predecessor, false), (successor, true)] {
            let Some(neighbor) = neighbor else {
                continue;
            };
            let neighbor_key = sealed_block_key(term, neighbor);
            let data = index
                .read(&neighbor_key)?
                .ok_or_else(|| corruption("sealed block is missing"))?;
            let mut neighbor_postings = Vec::new();
            decode_search_sealed_block(neighbor, &data, &mut neighbor_postings)?;
            if postings.len() + neighbor_postings.len() > SEARCH_INDEX_BLOCK_POSTINGS {
                continue;
            }
            let (kept, kept_fence, removed) = if absorb {
                postings.extend(neighbor_postings);
                (postings, fence, neighbor_key)
            } else {
                neighbor_postings.extend(postings);
                (neighbor_postings, neighbor, key)
            };
            index.write(removed, None)?;
            index.write(
                sealed_block_key(term, kept_fence),
                Some(layout::encode_sealed_block(kept_fence, &kept)),
            )?;
            dictionary.sealed_blocks = dictionary
                .sealed_blocks
                .checked_sub(1)
                .ok_or_else(|| corruption("sealed block count is negative"))?;
            break;
        }
    }
    if dictionary.document_frequency == 0 {
        // Only the retained empty first block can remain.
        match dictionary.sealed_blocks {
            0 => {}
            1 => index.write(sealed_block_key(term, 0), None)?,
            _ => return Err(corruption("empty term retains sealed blocks")),
        }
        if existed {
            index.write(dictionary_key, None)?;
        }
        return Ok(());
    }
    index.write(dictionary_key, Some(dictionary.encode()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn analyzer_lowercases_words_and_folds_full_width_ascii() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        assert_eq!(
            analyzer.index_tokens("Fix the HawDB-bm25 index, ＡＢＣ v2!"),
            ["fix", "the", "hawdb", "bm25", "index", "abc", "v2"]
        );
        assert_eq!(analyzer.query_terms("the THE index"), ["index", "the"]);
        assert!(analyzer.index_tokens(" ,.;  ").is_empty());
    }

    #[test]
    fn analyzer_indexes_ideographic_unigrams_and_bigrams() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        assert_eq!(
            analyzer.index_tokens("数据库用HawDB"),
            ["数", "数据", "据", "据库", "库", "库用", "用", "hawdb"]
        );
        assert_eq!(analyzer.query_terms("数据库"), ["据库", "数据"]);
        assert_eq!(analyzer.query_terms("库"), ["库"]);
        assert_eq!(
            analyzer.index_tokens("東京タワー 서울"),
            [
                "東", "東京", "京", "京タ", "タ", "タワ", "ワ", "ワー", "ー", "서", "서울", "울"
            ]
        );
    }

    /// A long run of distinct ideographs: about two distinct terms per
    /// character (its unigram and the bigram ending at it).
    fn distinct_cjk(characters: usize) -> String {
        let mut state = 0x2545_f491_u32;
        (0..characters)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                char::from_u32(0x4E00 + state % 0x5200).expect("CJK unified ideograph")
            })
            .collect()
    }

    #[test]
    fn analyzer_streams_document_terms_and_matches_materialized_tokens() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        let long = "y".repeat(SEARCH_INDEX_MAX_TOKEN_BYTES + 1);
        for text in [
            "数据库用HawDB 数据库 the THE 東京タワー 서울 库".to_owned(),
            format!("{long} 数 a{long}b ＡＢＣ v2 数据"),
            distinct_cjk(5_000),
        ] {
            let tokens = analyzer.index_tokens(&text);
            let mut expected = BTreeMap::<String, u64>::new();
            for token in &tokens {
                *expected.entry(token.clone()).or_default() += 1;
            }
            let mut admitted = 0usize;
            let (terms, length) = analyzer
                .document_terms::<()>(&text, |bytes| {
                    admitted += bytes;
                    Ok(())
                })
                .unwrap();
            assert_eq!((terms, length), (expected.clone(), tokens.len() as u64));
            assert_eq!(
                admitted,
                expected
                    .keys()
                    .map(|term| search_index_term_entry_bytes(term))
                    .sum::<usize>()
            );
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn analyzer_refuses_large_cjk_document_before_expanding_it() {
        // 1,000,000 distinct-ish ideographs (3 MB of TEXT) materialize about
        // two million tokens, tens of megabytes of transient strings. The
        // admission callback refuses at 64 KiB of distinct terms, and the
        // analysis must stop there without having expanded the rest.
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        let text = distinct_cjk(1_000_000);
        let limit = 64 * 1024;
        let mut admitted = 0usize;
        let mut checks = 0usize;
        let observation = crate::test_allocator::AllocationObservation::start_all();
        let result = analyzer.document_terms(&text, |bytes| {
            checks += 1;
            if admitted + bytes > limit {
                return Err(admitted);
            }
            admitted += bytes;
            Ok(())
        });
        let allocated = observation.finish_bytes();
        assert!(result.is_err(), "the oversized document must be refused");
        assert!(admitted <= limit);
        // Refused within the first few thousand terms, not after two million.
        assert!(
            checks < 2 * limit / search_index_term_entry_bytes("数据"),
            "{checks}"
        );
        assert!(
            allocated < 4 * limit,
            "analysis allocated {allocated} bytes before refusing"
        );
    }

    #[test]
    fn analyzer_drops_oversized_tokens_and_keeps_names_stable() {
        let analyzer = SearchIndexAnalyzer::CjkNgram;
        let long = "x".repeat(SEARCH_INDEX_MAX_TOKEN_BYTES + 1);
        assert_eq!(analyzer.index_tokens(&format!("a {long} b")), ["a", "b"]);
        assert_eq!(SearchIndexAnalyzer::parse("CJK_NGRAM"), Some(analyzer));
        assert_eq!(SearchIndexAnalyzer::parse("jieba"), None);
        assert_eq!(
            SearchIndexAnalyzer::from_tag(analyzer.tag()),
            Some(analyzer)
        );
    }
}
