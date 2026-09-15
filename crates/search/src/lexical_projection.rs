use super::cjk_tokenizer::ANALYZER_FORMAT_VERSION;
use super::{
    document_token_fields, visit_token_list, SearchAnalyzerLexicon, SearchDocument,
    TokenOccurrence, BM25_B, BM25_K1,
};
use crate::bounded_file::read_bounded_file;
use crate::build_control::checkpoint;
use crate::build_memory::{BuildMemory, MAP_ENTRY_BYTES};
use crate::build_term::Term;
use crate::error::{Result, SkeinError};
use serde::{Deserialize, Serialize};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher as Digest;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod analysis_tests;

mod artifacts;
mod block_encoding;
use artifacts::ArtifactBuilder;
mod build_manifest;
mod document_frequency;
mod spill_memory;
use spill_memory::{PendingPostings, RunPosting};
mod manifest_encoding;

#[cfg(test)]
mod positioned_read_tests;

#[cfg(test)]
mod spill_tests;

#[cfg(test)]
mod term_policy_tests;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINLEXICAL0001";
const BLOCK_HEADER: &[u8; 8] = b"SKNLEX01";
const RUN_HEADER: &[u8; 8] = b"SKNLEXR1";
const SPILL_IO_BUFFER_BYTES: usize = 8192;
pub(super) const DEFAULT_MAX_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MANIFEST_FILE: &str = "search_lexical.manifest.skein";

pub(super) fn artifact_file(generation: u64) -> String {
    format!("search_lexical.{generation}.skein")
}

pub(super) fn manifest_generation(path: &Path, max_bytes: u64) -> Result<Option<u64>> {
    // Invalid candidates remain skippable. Admission or I/O failures must not
    // hide existing generations and allow their identities to be reused.
    let bytes = read_bounded_file(path, max_bytes)?;
    Ok(ManifestBody::decode(&bytes)
        .ok()
        .map(|body| body.generation))
}

pub(super) fn admitted_manifest_generation(
    bytes: &[u8],
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Option<u64>> {
    let capacity = crate::build_control::json::decode_capacity(
        bytes,
        std::mem::size_of::<BlockDescriptor>().max(std::mem::size_of::<TermStatistics>()),
        2,
        task,
    )?;
    let capacity = crate::build_memory::checked_add(capacity, 3 * 128)?;
    let _decode = memory.spool.reserve(capacity)?;
    let result = ManifestBody::decode_with_context(bytes, Some(task));
    // A cancelled checksum/validation must never become a skippable candidate.
    checkpoint(task)?;
    Ok(result.ok().map(|body| body.generation))
}

pub(super) fn analyzer_digest(analyzer: &SearchAnalyzerLexicon) -> u64 {
    let mut digest = Digest::new();
    digest.update(ANALYZER_FORMAT_VERSION);
    for rule in &analyzer.alias_rules {
        for input in &rule.inputs {
            digest.update(input.as_bytes());
            digest.update(&[0]);
        }
        digest.update(&[1]);
        for alias in &rule.aliases {
            digest.update(alias.as_bytes());
            digest.update(&[0]);
        }
        digest.update(&[2]);
    }
    for stopword in &analyzer.stopwords {
        digest.update(stopword.as_bytes());
        digest.update(&[0]);
    }
    digest.finish()
}

pub(super) fn documents_digest(documents: &BTreeMap<String, SearchDocument>) -> u64 {
    let mut digest = Digest::new();
    for document in documents.values() {
        digest.update(super::encode_search_document_line(document).as_bytes());
    }
    digest.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LexicalProjectionConfig {
    pub max_manifest_bytes: NonZeroU64,
    pub build_memory_bytes: NonZeroU64,
    pub max_spill_bytes: NonZeroU64,
    pub max_spill_runs: NonZeroUsize,
    pub max_merge_fan_in: NonZeroUsize,
    pub target_block_bytes: NonZeroU64,
    pub max_block_bytes: NonZeroU64,
    pub max_term_bytes: NonZeroU64,
    pub max_document_tokens: NonZeroUsize,
    pub max_document_source_bytes: NonZeroU64,
    pub max_query_terms: NonZeroUsize,
    pub query_memory_bytes: NonZeroU64,
    pub max_query_score_entries: NonZeroUsize,
    pub mini_delta_bytes: NonZeroU64,
}

impl Default for LexicalProjectionConfig {
    fn default() -> Self {
        Self {
            max_manifest_bytes: NonZeroU64::new(DEFAULT_MAX_MANIFEST_BYTES).unwrap(),
            build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(4_096).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(32).unwrap(),
            target_block_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            max_block_bytes: NonZeroU64::new(2 * 1024 * 1024).unwrap(),
            max_term_bytes: crate::SearchLexicalTermPolicy::default().max_term_bytes(),
            max_document_tokens: NonZeroUsize::new(1_000_000).unwrap(),
            max_document_source_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_query_terms: NonZeroUsize::new(32).unwrap(),
            query_memory_bytes: NonZeroU64::new(128 * 1024 * 1024).unwrap(),
            max_query_score_entries: NonZeroUsize::new(1_000_000).unwrap(),
            mini_delta_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum BlockKind {
    Documents = 1,
    Postings = 2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockDescriptor {
    block_id: u64,
    kind: BlockKind,
    min_key: String,
    max_key: String,
    offset: u64,
    length: u64,
    checksum: u64,
    entry_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TermStatistics {
    term: String,
    document_frequency: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBody {
    format: String,
    generation: u64,
    source_graph_commit_epoch: Option<u64>,
    analyzer_digest: u64,
    documents_digest: u64,
    artifact_file: String,
    artifact_len: u64,
    artifact_checksum: u64,
    document_count: u64,
    total_document_len: u64,
    posting_count: u64,
    term_statistics: Vec<TermStatistics>,
    blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    body: ManifestBody,
    checksum: u64,
}

impl ManifestBody {
    fn required_term_bytes(&self, task: Option<&RuntimeTaskContext>) -> Result<u64> {
        self.term_statistics
            .iter()
            .map(|statistics| statistics.term.len())
            .chain(
                self.blocks
                    .iter()
                    .filter(|block| block.kind == BlockKind::Postings)
                    .flat_map(|block| [block.min_key.len(), block.max_key.len()]),
            )
            .try_fold(0u64, |largest, bytes| {
                task.map_or(Ok(()), checkpoint)?;
                Ok(largest.max(bytes as u64))
            })
    }

    #[cfg(test)]
    fn validate(&self) -> Result<()> {
        self.validate_with_context(None)
    }

    fn validate_with_context(&self, task: Option<&RuntimeTaskContext>) -> Result<()> {
        task.map_or(Ok(()), checkpoint)?;
        if self.format != "SKEIN_LEXICAL_MANIFEST_V1"
            || self.artifact_file != artifact_file(self.generation)
            || Path::new(&self.artifact_file)
                .file_name()
                .and_then(|name| name.to_str())
                != Some(self.artifact_file.as_str())
            || self.artifact_len < ARTIFACT_HEADER.len() as u64 + 8
        {
            return Err(SkeinError::Storage(
                "lexical projection manifest header is invalid".to_string(),
            ));
        }
        let mut previous_end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut previous = None;
        let mut documents = 0u64;
        let mut postings = 0u64;
        for block in &self.blocks {
            task.map_or(Ok(()), checkpoint)?;
            if block.length == 0
                || block.entry_count == 0
                || block.min_key > block.max_key
                || block.offset != previous_end
            {
                return Err(SkeinError::Storage(format!(
                    "lexical projection block {} has invalid bounds",
                    block.block_id
                )));
            }
            let key = (block.kind as u8, block.min_key.as_str(), block.block_id);
            if previous.is_some_and(|previous| previous >= key) {
                return Err(SkeinError::Storage(
                    "lexical projection blocks are not ordered".to_string(),
                ));
            }
            previous = Some(key);
            previous_end = previous_end.checked_add(block.length).ok_or_else(|| {
                SkeinError::Storage("lexical projection block range overflows".to_string())
            })?;
            match block.kind {
                BlockKind::Documents => {
                    documents = documents.saturating_add(u64::from(block.entry_count));
                }
                BlockKind::Postings => {
                    postings = postings.saturating_add(u64::from(block.entry_count));
                }
            }
        }
        let mut previous_term: Option<&str> = None;
        let mut term_postings = 0u64;
        for statistics in &self.term_statistics {
            task.map_or(Ok(()), checkpoint)?;
            if statistics.term.is_empty()
                || statistics.document_frequency == 0
                || previous_term.is_some_and(|previous| previous >= statistics.term.as_str())
            {
                return Err(SkeinError::Storage(
                    "lexical projection term statistics are invalid or unordered".to_string(),
                ));
            }
            term_postings = term_postings
                .checked_add(statistics.document_frequency)
                .ok_or_else(|| {
                    SkeinError::Storage(
                        "lexical projection term document frequency overflows".to_string(),
                    )
                })?;
            previous_term = Some(&statistics.term);
        }
        if previous_end != self.artifact_len
            || documents != self.document_count
            || postings != self.posting_count
            || term_postings != self.posting_count
        {
            return Err(SkeinError::Storage(
                "lexical projection manifest counts are inconsistent".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn encode(&self, max_bytes: u64) -> Result<Vec<u8>> {
        self.validate()?;
        manifest_encoding::encode(self, max_bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_with_context(bytes, None)
    }

    fn decode_with_context(bytes: &[u8], task: Option<&RuntimeTaskContext>) -> Result<Self> {
        task.map_or(Ok(()), checkpoint)?;
        let envelope: ManifestEnvelope = serde_json::from_slice(bytes)
            .map_err(|error| SkeinError::Storage(format!("invalid lexical manifest: {error}")))?;
        task.map_or(Ok(()), checkpoint)?;
        if manifest_encoding::checksum_with_context(&envelope.body, task)? != envelope.checksum {
            return Err(SkeinError::Storage(
                "lexical projection manifest checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate_with_context(task)?;
        Ok(envelope.body)
    }

    fn document_frequency(&self, term: &str) -> u64 {
        self.term_statistics
            .binary_search_by(|statistics| statistics.term.as_str().cmp(term))
            .ok()
            .map_or(0, |index| self.term_statistics[index].document_frequency)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Posting {
    term: Term,
    document_id: String,
    term_frequency: u32,
    document_len: u32,
}

impl Posting {
    fn resident_bytes(term: &str, document_id: &str) -> u64 {
        32u64
            .saturating_add(term.len() as u64)
            .saturating_add(document_id.len() as u64)
    }

    fn encoded_len(&self) -> u64 {
        4u64.saturating_add(self.term.len() as u64)
            .saturating_add(4)
            .saturating_add(self.document_id.len() as u64)
            .saturating_add(8)
    }
}

#[derive(Debug)]
struct DeltaDocument {
    document_len: u32,
    frequencies: BTreeMap<String, u32>,
    resident_bytes: u64,
    base: Option<Arc<BaseDocumentTerms>>,
}

impl DeltaDocument {
    fn total_resident_bytes(&self) -> u64 {
        self.resident_bytes
            .saturating_add(self.base.as_ref().map_or(0, |base| base.resident_bytes))
    }
}

#[derive(Debug)]
struct BaseDocumentTerms {
    document_len: u32,
    terms: BTreeSet<String>,
    resident_bytes: u64,
}

impl BaseDocumentTerms {
    fn from_analyzed(document: DeltaDocument) -> Self {
        Self {
            document_len: document.document_len,
            terms: document.frequencies.into_keys().collect(),
            resident_bytes: document.resident_bytes,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct LexicalMiniDelta {
    // A mutation can share unchanged terms with a retained query snapshot.
    upserts: BTreeMap<String, Arc<DeltaDocument>>,
    deletes: BTreeMap<String, Arc<BaseDocumentTerms>>,
    resident_bytes: u64,
}

impl LexicalMiniDelta {
    pub(super) fn upsert(
        self: &mut Arc<Self>,
        document: &SearchDocument,
        previous_document: Option<&SearchDocument>,
        analyzer: &SearchAnalyzerLexicon,
        config: LexicalProjectionConfig,
    ) -> Result<()> {
        let mut delta = analyze_delta_document(document, analyzer, config)?;
        let base = if let Some(previous) = self.upserts.get(&document.id) {
            previous.base.clone()
        } else if let Some(previous) = self.deletes.get(&document.id) {
            Some(previous.clone())
        } else {
            previous_document
                .map(|document| analyze_delta_document(document, analyzer, config))
                .transpose()?
                .map(BaseDocumentTerms::from_analyzed)
                .map(Arc::new)
        };
        let removed_upsert = self
            .upserts
            .get(&document.id)
            .map_or(0, |document| document.total_resident_bytes());
        let removed_delete = self
            .deletes
            .get(&document.id)
            .map_or(0, |document| document.resident_bytes);
        delta.base = base;
        let required = self
            .resident_bytes
            .saturating_sub(removed_upsert)
            .saturating_sub(removed_delete)
            .saturating_add(delta.total_resident_bytes());
        if required > config.mini_delta_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical mini-delta requires {required} bytes, exceeding {}",
                config.mini_delta_bytes
            )));
        }
        // A retained query snapshot must not force a map clone for rejected work.
        let current = Arc::make_mut(self);
        current.upserts.remove(&document.id);
        current.deletes.remove(&document.id);
        current.resident_bytes = required;
        current.upserts.insert(document.id.clone(), Arc::new(delta));
        Ok(())
    }

    pub(super) fn delete(
        self: &mut Arc<Self>,
        document_id: &str,
        previous_document: Option<&SearchDocument>,
        analyzer: &SearchAnalyzerLexicon,
        config: LexicalProjectionConfig,
    ) -> Result<bool> {
        if self.deletes.contains_key(document_id) {
            return Ok(true);
        }
        let existing_upsert = self.upserts.get(document_id);
        let base = if let Some(previous) = existing_upsert {
            previous.base.clone()
        } else {
            previous_document
                .map(|document| analyze_delta_document(document, analyzer, config))
                .transpose()?
                .map(BaseDocumentTerms::from_analyzed)
                .map(Arc::new)
        };
        let removed = existing_upsert.map_or(0, |document| document.total_resident_bytes());
        let Some(base) = base else {
            if existing_upsert.is_some() {
                let current = Arc::make_mut(self);
                current.upserts.remove(document_id);
                current.resident_bytes = current.resident_bytes.saturating_sub(removed);
            }
            return Ok(true);
        };
        let required = self
            .resident_bytes
            .saturating_sub(removed)
            .saturating_add(base.resident_bytes);
        if required > config.mini_delta_bytes.get() {
            return Ok(false);
        }
        let current = Arc::make_mut(self);
        current.upserts.remove(document_id);
        current.deletes.insert(document_id.to_string(), base);
        current.resident_bytes = required;
        Ok(true)
    }

    fn overrides(&self, document_id: &str) -> bool {
        self.deletes.contains_key(document_id) || self.upserts.contains_key(document_id)
    }

    fn projected_document_frequency(&self, term: &str, base_frequency: u64) -> usize {
        let removed = self
            .upserts
            .values()
            .filter(|document| {
                document
                    .base
                    .as_ref()
                    .is_some_and(|base| base.terms.contains(term))
            })
            .count()
            .saturating_add(
                self.deletes
                    .values()
                    .filter(|document| document.terms.contains(term))
                    .count(),
            );
        let added = self
            .upserts
            .values()
            .filter(|document| document.frequencies.contains_key(term))
            .count();
        usize::try_from(base_frequency)
            .unwrap_or(usize::MAX)
            .saturating_sub(removed)
            .saturating_add(added)
    }

    fn projected_corpus(&self, base_document_count: u64, base_total_len: u64) -> (usize, u64) {
        let mut document_count = base_document_count;
        let mut total_document_len = base_total_len;
        for document in self.upserts.values() {
            if let Some(base) = &document.base {
                document_count = document_count.saturating_sub(1);
                total_document_len =
                    total_document_len.saturating_sub(u64::from(base.document_len));
            }
            document_count = document_count.saturating_add(1);
            total_document_len =
                total_document_len.saturating_add(u64::from(document.document_len));
        }
        for document in self.deletes.values() {
            document_count = document_count.saturating_sub(1);
            total_document_len =
                total_document_len.saturating_sub(u64::from(document.document_len));
        }
        (
            usize::try_from(document_count).unwrap_or(usize::MAX),
            total_document_len,
        )
    }
}

fn analyze_delta_document(
    document: &SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
    config: LexicalProjectionConfig,
) -> Result<DeltaDocument> {
    admit_document_source(document, config)?;
    if crate::analyzer_workspace::document_needs_workspace(document) {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task)?;
        return crate::analyzer_workspace::run(&memory, &task, |workspace| {
            analyze_delta_document_with_workspace(document, analyzer, config, Some(&workspace))
        });
    }
    analyze_delta_document_with_workspace(document, analyzer, config, None)
}

fn analyze_delta_document_with_workspace(
    document: &SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
    config: LexicalProjectionConfig,
    workspace: Option<&crate::analyzer_workspace::Workspace>,
) -> Result<DeltaDocument> {
    let mut accumulator = DocumentAnalysis::new(&document.id, config)?;
    for (field, (text, weight)) in document_token_fields(document).enumerate() {
        let field = u8::try_from(field).expect("document analysis has at most six fields");
        crate::analyzer_stream::visit_token_list_with_workspace(
            text,
            analyzer,
            workspace,
            |term, occurrence| accumulator.push(term, occurrence, field, weight),
        )?;
    }
    accumulator.finish()
}

fn admit_document_source(document: &SearchDocument, config: LexicalProjectionConfig) -> Result<()> {
    let source_bytes = document
        .title
        .len()
        .saturating_add(document.content.len())
        .saturating_add(
            document
                .metadata
                .iter()
                .fold(0usize, |bytes, (key, value)| {
                    bytes.saturating_add(key.len()).saturating_add(value.len())
                }),
        );
    if source_bytes as u64 > config.max_document_source_bytes.get() {
        return Err(SkeinError::Storage(format!(
            "lexical document {} uses {source_bytes} source bytes, exceeding {}",
            document.id, config.max_document_source_bytes
        )));
    }
    Ok(())
}

struct AnalyzedTerm {
    frequency: u32,
    last_field: u8,
}

fn admit_term_bytes(bytes: u64, max_term_bytes: NonZeroU64) -> Result<()> {
    if bytes > max_term_bytes.get() {
        return Err(SkeinError::Storage(format!(
            "lexical term uses {bytes} bytes, exceeding {max_term_bytes}"
        )));
    }
    Ok(())
}

struct DocumentAnalysis<'a> {
    document_id: &'a str,
    config: LexicalProjectionConfig,
    document_len: u32,
    frequencies: BTreeMap<Term, AnalyzedTerm>,
    resident_bytes: u64,
    map_memory: Option<QueryMemoryLease>,
}

impl<'a> DocumentAnalysis<'a> {
    fn new(document_id: &'a str, config: LexicalProjectionConfig) -> Result<Self> {
        Self::new_with_memory(document_id, config, None)
    }

    fn new_with_memory(
        document_id: &'a str,
        config: LexicalProjectionConfig,
        memory: Option<&BuildMemory>,
    ) -> Result<Self> {
        let analysis = Self {
            document_id,
            config,
            document_len: 0,
            frequencies: BTreeMap::new(),
            resident_bytes: document_id.len() as u64 + 64,
            map_memory: memory
                .map(|memory| memory.retained.reserve(0))
                .transpose()?,
        };
        analysis.admit_map_bytes(analysis.resident_bytes, 0)?;
        Ok(analysis)
    }

    fn push(
        &mut self,
        term: String,
        occurrence: TokenOccurrence,
        field: u8,
        weight: usize,
    ) -> Result<()> {
        self.push_term(term.into(), occurrence, field, weight)
    }

    fn push_term(
        &mut self,
        term: Term,
        occurrence: TokenOccurrence,
        field: u8,
        weight: usize,
    ) -> Result<()> {
        admit_term_bytes(term.len() as u64, self.config.max_term_bytes)?;
        let previous = self.frequencies.get(&term);
        if occurrence == TokenOccurrence::UniqueInField
            && previous.is_some_and(|entry| entry.last_field == field)
        {
            return Ok(());
        }
        let required_tokens = u64::from(self.document_len).saturating_add(weight as u64);
        if required_tokens > self.config.max_document_tokens.get() as u64 {
            return Err(SkeinError::Storage(format!(
                "lexical document {} produced at least {required_tokens} tokens, exceeding {}",
                self.document_id, self.config.max_document_tokens,
            )));
        }
        let document_len = u32::try_from(required_tokens)
            .map_err(|_| SkeinError::Storage("lexical document length exceeds u32".to_string()))?;
        if previous.is_none() {
            let required_bytes = self.resident_bytes.saturating_add(term.len() as u64 + 32);
            self.admit_map_bytes(required_bytes, self.frequencies.len() + 1)?;
            if let Some(memory) = self.map_memory.as_mut() {
                memory.grow(MAP_ENTRY_BYTES)?;
            }
            self.resident_bytes = required_bytes;
        }
        // One field marker per distinct term preserves phrase uniqueness without
        // retaining a separate document-wide token sequence or seen-term set.
        let entry = self.frequencies.entry(term).or_insert(AnalyzedTerm {
            frequency: 0,
            last_field: field,
        });
        entry.frequency = entry.frequency.saturating_add(weight as u32);
        entry.last_field = field;
        self.document_len = document_len;
        Ok(())
    }

    fn admit_map_bytes(&self, resident_bytes: u64, terms: usize) -> Result<()> {
        let required_bytes = self.required_map_bytes(resident_bytes, terms);
        if required_bytes > self.config.build_memory_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical document {} requires more than {} analyzer bytes",
                self.document_id, self.config.build_memory_bytes,
            )));
        }
        Ok(())
    }

    fn required_map_bytes(&self, resident_bytes: u64, terms: usize) -> u64 {
        let marker_bytes =
            (std::mem::size_of::<AnalyzedTerm>() - std::mem::size_of::<u32>()) as u64;
        resident_bytes.saturating_add((terms as u64).saturating_mul(marker_bytes))
    }

    fn finish(self) -> Result<DeltaDocument> {
        Ok(DeltaDocument {
            document_len: self.document_len,
            frequencies: self
                .frequencies
                .into_iter()
                .map(|(term, entry)| Ok((term.into_untracked()?, entry.frequency)))
                .collect::<Result<_>>()?,
            resident_bytes: self.resident_bytes,
            base: None,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LexicalQueryReport {
    pub scores: BTreeMap<String, f64>,
    pub matching_document_count: usize,
    pub postings_visited: u64,
    pub bytes_read: u64,
}

#[derive(Debug)]
pub(super) struct LexicalProjectionReader {
    manifest: ManifestBody,
    file: Arc<File>,
    config: LexicalProjectionConfig,
    required_term_bytes: u64,
    // Only build-created readers retain an operation-owned metadata lease.
    _build_memory: Option<QueryMemoryLease>,
}

impl LexicalProjectionReader {
    pub(super) fn load(
        root: &Path,
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
    ) -> Result<Option<Arc<Self>>> {
        Self::load_named(
            root,
            MANIFEST_FILE,
            expected_source_epoch,
            expected_analyzer_digest,
            expected_documents_digest,
            config,
        )
    }

    fn load_named(
        root: &Path,
        manifest_file: &str,
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
    ) -> Result<Option<Arc<Self>>> {
        if Path::new(manifest_file)
            .file_name()
            .and_then(|name| name.to_str())
            != Some(manifest_file)
        {
            return Err(SkeinError::Storage(
                "lexical projection manifest name is invalid".to_string(),
            ));
        }
        let manifest_path = root.join(manifest_file);
        if !manifest_path.exists() {
            return Ok(None);
        }
        let bytes = read_bounded_file(&manifest_path, config.max_manifest_bytes.get())?;
        Self::load_manifest_bytes(
            root,
            &bytes,
            expected_source_epoch,
            expected_analyzer_digest,
            expected_documents_digest,
            config,
        )
    }

    pub(super) fn load_manifest_bytes(
        root: &Path,
        bytes: &[u8],
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
    ) -> Result<Option<Arc<Self>>> {
        if bytes.len() as u64 > config.max_manifest_bytes.get() {
            return Err(SkeinError::Storage(
                "lexical projection manifest exceeds its read budget".to_string(),
            ));
        }
        let manifest = ManifestBody::decode(bytes)?;
        let artifact_path = root.join(&manifest.artifact_file);
        Self::load_decoded_manifest(
            &artifact_path,
            manifest,
            expected_source_epoch,
            expected_analyzer_digest,
            expected_documents_digest,
            config,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn load_decoded_manifest(
        artifact_path: &Path,
        decoded: ManifestBody,
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
        context: Option<(&BuildMemory, &RuntimeTaskContext)>,
        build_memory: Option<QueryMemoryLease>,
    ) -> Result<Option<Arc<Self>>> {
        // Bind after the lease so the decoded payload drops first on every early return.
        let manifest = decoded;
        let task = context.map(|(_, task)| task);
        task.map_or(Ok(()), checkpoint)?;
        if manifest.source_graph_commit_epoch != expected_source_epoch
            || manifest.analyzer_digest != expected_analyzer_digest
            || manifest.documents_digest != expected_documents_digest
        {
            return Ok(None);
        }
        let required_term_bytes = manifest.required_term_bytes(task)?;
        admit_term_bytes(required_term_bytes, config.max_term_bytes)?;
        for block in &manifest.blocks {
            task.map_or(Ok(()), checkpoint)?;
            if block.length > config.max_block_bytes.get() {
                return Err(SkeinError::Storage(
                    "lexical projection contains a block above the read admission limit"
                        .to_string(),
                ));
            }
        }
        let file = File::open(artifact_path)?;
        if file.metadata()?.len() != manifest.artifact_len {
            return Err(SkeinError::Storage(
                "lexical projection artifact length mismatch".to_string(),
            ));
        }
        let (length, digest) = match context {
            Some((memory, task)) => build_manifest::file_digest(&file, memory, task)?,
            None => file_digest(&file)?,
        };
        if length != manifest.artifact_len || digest != manifest.artifact_checksum {
            return Err(SkeinError::Storage(
                "lexical projection artifact checksum mismatch".to_string(),
            ));
        }
        let mut header = [0u8; 24];
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(0))?;
        cloned.read_exact(&mut header)?;
        task.map_or(Ok(()), checkpoint)?;
        if &header[..16] != ARTIFACT_HEADER
            || u64::from_le_bytes(header[16..24].try_into().unwrap()) != manifest.generation
        {
            return Err(SkeinError::Storage(
                "lexical projection artifact header mismatch".to_string(),
            ));
        }
        Ok(Some(Arc::new(Self {
            manifest,
            file: Arc::new(file),
            config,
            required_term_bytes,
            _build_memory: build_memory,
        })))
    }

    pub(super) fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub(super) fn validate_term_limit(&self, max_term_bytes: NonZeroU64) -> Result<()> {
        admit_term_bytes(self.required_term_bytes, max_term_bytes)
    }

    fn posting_blocks<'a>(&'a self, term: &'a str) -> impl Iterator<Item = &'a BlockDescriptor> {
        self.manifest.blocks.iter().filter(move |block| {
            block.kind == BlockKind::Postings
                && block.min_key.as_str() <= term
                && term <= block.max_key.as_str()
        })
    }

    pub(super) fn tokenize_query(
        &self,
        text: &str,
        analyzer: &SearchAnalyzerLexicon,
        max_term_bytes: NonZeroU64,
    ) -> Result<BTreeSet<String>> {
        if text.len() as u64 > self.config.query_memory_bytes.get() {
            return Err(SkeinError::Storage(
                "lexical query source exceeds its memory budget".into(),
            ));
        }
        let mut terms = BTreeSet::new();
        let mut retained_bytes = 0u64;
        visit_token_list(text, analyzer, |term, _| {
            admit_term_bytes(term.len() as u64, max_term_bytes)?;
            if !terms.contains(&term) {
                if terms.len() >= self.config.max_query_terms.get() {
                    return Err(SkeinError::Storage(format!(
                        "lexical query produced more than {} terms",
                        self.config.max_query_terms
                    )));
                }
                retained_bytes = retained_bytes
                    .saturating_add(term.len() as u64)
                    .saturating_add(32);
                if retained_bytes > self.config.query_memory_bytes.get() {
                    return Err(SkeinError::Storage(
                        "lexical query terms exceed their memory budget".into(),
                    ));
                }
                terms.insert(term);
            }
            Ok(())
        })?;
        Ok(terms)
    }

    pub(super) fn score(
        &self,
        query_terms: &BTreeSet<String>,
        delta: &LexicalMiniDelta,
        retained_score_limit: Option<usize>,
        allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<LexicalQueryReport> {
        self.score_with_term_limit(
            query_terms,
            delta,
            self.config.max_term_bytes,
            retained_score_limit,
            allowed,
        )
    }

    pub(super) fn score_with_term_limit(
        &self,
        query_terms: &BTreeSet<String>,
        delta: &LexicalMiniDelta,
        max_term_bytes: NonZeroU64,
        retained_score_limit: Option<usize>,
        mut allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<LexicalQueryReport> {
        self.validate_term_limit(max_term_bytes)?;
        for term in query_terms {
            admit_term_bytes(term.len() as u64, max_term_bytes)?;
        }
        if query_terms.len() > self.config.max_query_terms.get() {
            return Err(SkeinError::Storage(format!(
                "lexical query produced {} terms, exceeding {}",
                query_terms.len(),
                self.config.max_query_terms
            )));
        }
        if query_terms.is_empty() {
            return Ok(LexicalQueryReport::default());
        }
        let admitted_stream_bytes = query_terms.iter().fold(0u64, |bytes, term| {
            let (max_block, max_entries, references) = self.posting_blocks(term).fold(
                (0u64, 0u64, 0u64),
                |(max, entries, count), block| {
                    (
                        max.max(block.length),
                        entries.max(u64::from(block.entry_count)),
                        count.saturating_add(1),
                    )
                },
            );
            // Reserve the actual validated block extent, not the reader's
            // ceiling: otherwise charging keys makes the default 32-term
            // boundary fail even for tiny blocks. Reserve old/new decoded Vec
            // capacity during block transitions, encoded/decoded strings and
            // heap keys, pointer-vector growth, and retained query term copies.
            bytes
                .saturating_add(max_block.saturating_mul(4))
                .saturating_add(
                    max_entries.saturating_mul(4 * std::mem::size_of::<Posting>() as u64),
                )
                .saturating_add(
                    references.saturating_mul(2 * std::mem::size_of::<&BlockDescriptor>() as u64),
                )
                .saturating_add((term.len() as u64).saturating_mul(3))
                .saturating_add(32)
        });
        if admitted_stream_bytes > self.config.query_memory_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical query streams require {admitted_stream_bytes} bytes, exceeding {}",
                self.config.query_memory_bytes
            )));
        }
        let (document_count, total_document_len) = delta.projected_corpus(
            self.manifest.document_count,
            self.manifest.total_document_len,
        );
        let mut bytes_read = 0u64;
        if document_count == 0 {
            return Ok(LexicalQueryReport::default());
        }
        let mut document_frequency = BTreeMap::new();
        let mut postings_visited = 0u64;
        for term in query_terms {
            document_frequency.insert(
                term.clone(),
                delta.projected_document_frequency(term, self.manifest.document_frequency(term)),
            );
        }
        let average_document_len = (total_document_len as f64 / document_count as f64).max(1.0);
        let mut collector = ScoreCollector::new(
            retained_score_limit,
            self.config.max_query_score_entries.get(),
        )?;
        let mut streams = Vec::new();
        let mut stream_idf = Vec::new();
        for term in query_terms {
            let df = document_frequency.get(term).copied().unwrap_or(0);
            if df > 0 {
                streams.push(TermPostingStream::new(self, term, max_term_bytes));
                stream_idf.push(idf(document_count, df));
            }
        }
        let mut heap = BinaryHeap::new();
        for (index, stream) in streams.iter_mut().enumerate() {
            if let Some(posting) = stream.next()? {
                heap.push(Reverse((posting.document_id.clone(), index, posting)));
            }
        }
        while let Some(Reverse((document_id, stream_index, posting))) = heap.pop() {
            let mut score = 0.0;
            if !delta.overrides(&document_id) && allowed(&document_id)? {
                score += bm25_term_score(
                    stream_idf[stream_index],
                    posting.term_frequency,
                    posting.document_len,
                    average_document_len,
                );
            }
            if let Some(next) = streams[stream_index].next()? {
                heap.push(Reverse((next.document_id.clone(), stream_index, next)));
            }
            while heap
                .peek()
                .is_some_and(|Reverse((next_id, _, _))| next_id == &document_id)
            {
                let Reverse((_, next_stream_index, next_posting)) =
                    heap.pop().expect("peeked lexical posting exists");
                if !delta.overrides(&document_id) && allowed(&document_id)? {
                    score += bm25_term_score(
                        stream_idf[next_stream_index],
                        next_posting.term_frequency,
                        next_posting.document_len,
                        average_document_len,
                    );
                }
                if let Some(next) = streams[next_stream_index].next()? {
                    heap.push(Reverse((next.document_id.clone(), next_stream_index, next)));
                }
            }
            if score > 0.0 {
                collector.push(document_id, score)?;
            }
        }
        for stream in &streams {
            postings_visited = postings_visited.saturating_add(stream.postings_visited);
            bytes_read = bytes_read.saturating_add(stream.bytes_read);
        }
        for (id, document) in &delta.upserts {
            if !allowed(id)? {
                continue;
            }
            let mut score = 0.0;
            for term in query_terms {
                let Some(frequency) = document.frequencies.get(term) else {
                    continue;
                };
                let df = document_frequency.get(term).copied().unwrap_or(0);
                if df > 0 {
                    score += bm25_term_score(
                        idf(document_count, df),
                        *frequency,
                        document.document_len,
                        average_document_len,
                    );
                }
            }
            if score > 0.0 {
                collector.push(id.clone(), score)?;
            }
        }
        let matching_document_count = collector.matching_count;
        let scores = collector.finish();
        Ok(LexicalQueryReport {
            scores,
            matching_document_count,
            postings_visited,
            bytes_read,
        })
    }

    fn read_block(&self, block: &BlockDescriptor) -> Result<Vec<u8>> {
        if block.length > self.config.max_block_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical block {} exceeds the read budget",
                block.block_id
            )));
        }
        let length = usize::try_from(block.length).map_err(|_| {
            SkeinError::Storage(format!(
                "lexical block {} length exceeds the platform address space",
                block.block_id
            ))
        })?;
        let mut bytes = vec![0u8; length];
        // File clones can share a cursor. Each read must carry its own offset
        // so concurrent posting streams cannot redirect one another's I/O.
        skein_storage::io::read_exact_at(&self.file, &mut bytes, block.offset)?;
        if checksum(&bytes) != block.checksum {
            return Err(SkeinError::Storage(format!(
                "lexical block {} checksum mismatch",
                block.block_id
            )));
        }
        Ok(bytes)
    }
}

struct TermPostingStream<'a> {
    projection: &'a LexicalProjectionReader,
    term: &'a str,
    blocks: Vec<&'a BlockDescriptor>,
    block_index: usize,
    current: std::vec::IntoIter<Posting>,
    postings_visited: u64,
    bytes_read: u64,
    max_term_bytes: NonZeroU64,
}

impl<'a> TermPostingStream<'a> {
    fn new(
        projection: &'a LexicalProjectionReader,
        term: &'a str,
        max_term_bytes: NonZeroU64,
    ) -> Self {
        let blocks = projection.posting_blocks(term).collect();
        Self {
            projection,
            term,
            blocks,
            block_index: 0,
            current: Vec::new().into_iter(),
            postings_visited: 0,
            bytes_read: 0,
            max_term_bytes,
        }
    }

    fn next(&mut self) -> Result<Option<Posting>> {
        loop {
            if let Some(posting) = self.current.next() {
                return Ok(Some(posting));
            }
            let Some(block) = self.blocks.get(self.block_index).copied() else {
                return Ok(None);
            };
            self.block_index = self.block_index.saturating_add(1);
            let bytes = self.projection.read_block(block)?;
            self.bytes_read = self.bytes_read.saturating_add(bytes.len() as u64);
            let mut postings = Vec::new();
            decode_posting_block(
                &bytes,
                self.projection.manifest.generation,
                block,
                self.max_term_bytes.get(),
                |posting| {
                    self.postings_visited = self.postings_visited.saturating_add(1);
                    if posting.term.as_str() == self.term {
                        postings.push(posting);
                    }
                    Ok(())
                },
            )?;
            self.current = postings.into_iter();
        }
    }
}

#[derive(Debug, Clone)]
struct ScoredDocument {
    id: String,
    score: f64,
}

impl PartialEq for ScoredDocument {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score.to_bits() == other.score.to_bits()
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
            .then_with(|| other.id.cmp(&self.id))
    }
}

enum ScoreStorage {
    Full(BTreeMap<String, f64>),
    TopK {
        limit: usize,
        heap: BinaryHeap<Reverse<ScoredDocument>>,
    },
}

struct ScoreCollector {
    storage: ScoreStorage,
    matching_count: usize,
    max_entries: usize,
}

impl ScoreCollector {
    fn new(retained_limit: Option<usize>, max_entries: usize) -> Result<Self> {
        if retained_limit.is_some_and(|limit| limit > max_entries) {
            return Err(SkeinError::Storage(format!(
                "lexical rank window exceeds the admitted {max_entries} score entries"
            )));
        }
        Ok(Self {
            storage: match retained_limit {
                Some(limit) => ScoreStorage::TopK {
                    limit,
                    heap: BinaryHeap::with_capacity(limit.saturating_add(1)),
                },
                None => ScoreStorage::Full(BTreeMap::new()),
            },
            matching_count: 0,
            max_entries,
        })
    }

    fn push(&mut self, id: String, score: f64) -> Result<()> {
        self.matching_count = self.matching_count.saturating_add(1);
        match &mut self.storage {
            ScoreStorage::Full(scores) => {
                if scores.len() >= self.max_entries {
                    return Err(SkeinError::Storage(format!(
                        "lexical query matched more than {} documents; provide a rank window or narrow the candidate set",
                        self.max_entries
                    )));
                }
                scores.insert(id, score);
            }
            ScoreStorage::TopK { limit, heap } => {
                if *limit == 0 {
                    return Ok(());
                }
                let candidate = ScoredDocument { id, score };
                if heap.len() < *limit {
                    heap.push(Reverse(candidate));
                } else if heap
                    .peek()
                    .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
                {
                    heap.pop();
                    heap.push(Reverse(candidate));
                }
            }
        }
        Ok(())
    }

    fn finish(self) -> BTreeMap<String, f64> {
        match self.storage {
            ScoreStorage::Full(scores) => scores,
            ScoreStorage::TopK { heap, .. } => heap
                .into_iter()
                .map(|Reverse(candidate)| (candidate.id, candidate.score))
                .collect(),
        }
    }
}

fn idf(document_count: usize, document_frequency: usize) -> f64 {
    (1.0 + (document_count as f64 - document_frequency as f64 + 0.5)
        / (document_frequency as f64 + 0.5))
        .ln()
}

fn bm25_term_score(idf: f64, frequency: u32, document_len: u32, average_len: f64) -> f64 {
    let frequency = f64::from(frequency);
    let denominator = frequency
        + BM25_K1 * (1.0 - BM25_B + BM25_B * f64::from(document_len) / average_len.max(1.0));
    idf * (frequency * (BM25_K1 + 1.0)) / denominator
}

pub(super) struct LexicalProjectionWriter {
    config: LexicalProjectionConfig,
    build_context: Option<(BuildMemory, RuntimeTaskContext)>,
    analyzer_workspace: Option<Arc<crate::analyzer_workspace::Workspace>>,
}

impl LexicalProjectionWriter {
    pub(super) const fn new(config: LexicalProjectionConfig) -> Self {
        Self {
            config,
            build_context: None,
            analyzer_workspace: None,
        }
    }

    pub(super) fn with_context(mut self, memory: BuildMemory, task: RuntimeTaskContext) -> Self {
        self.build_context = Some((memory, task));
        self
    }

    pub(super) fn with_analyzer_workspace(
        mut self,
        workspace: Option<Arc<crate::analyzer_workspace::Workspace>>,
    ) -> Self {
        self.analyzer_workspace = workspace;
        self
    }

    fn context(&self) -> Result<(BuildMemory, RuntimeTaskContext)> {
        match &self.build_context {
            Some((memory, task)) => Ok((memory.clone(), task.clone())),
            None => {
                let task = RuntimeTaskContext::default();
                Ok((BuildMemory::new(&task)?, task))
            }
        }
    }

    fn needs_analyzer_workspace<'a>(
        &self,
        documents: impl Iterator<Item = &'a SearchDocument>,
    ) -> Result<bool> {
        for document in documents {
            if let Some((_, task)) = &self.build_context {
                checkpoint(task)?;
            }
            admit_document_source(document, self.config)?;
            if crate::analyzer_workspace::document_needs_workspace(document) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn write<'a>(
        &self,
        root: &Path,
        generation: u64,
        source_graph_commit_epoch: Option<u64>,
        analyzer_digest: u64,
        documents_digest: u64,
        documents: impl Iterator<Item = &'a SearchDocument> + Clone + Send,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Arc<LexicalProjectionReader>> {
        if let Some((_, task)) = &self.build_context {
            checkpoint(task)?;
        }
        if self.analyzer_workspace.is_none() && self.needs_analyzer_workspace(documents.clone())? {
            let (memory, task) = self.context()?;
            return crate::analyzer_workspace::run(&memory, &task, |workspace| {
                Self::new(self.config)
                    .with_context(memory.clone(), task.clone())
                    .with_analyzer_workspace(Some(workspace))
                    .write(
                        root,
                        generation,
                        source_graph_commit_epoch,
                        analyzer_digest,
                        documents_digest,
                        documents,
                        analyzer,
                    )
            });
        }
        self.write_scanned(
            root,
            generation,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            |consumer| {
                for document in documents {
                    consumer(document)?;
                }
                Ok(())
            },
            analyzer,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_scanned(
        &self,
        root: &Path,
        generation: u64,
        source_graph_commit_epoch: Option<u64>,
        analyzer_digest: u64,
        documents_digest: u64,
        scan: impl FnOnce(&mut dyn FnMut(&SearchDocument) -> Result<()>) -> Result<()>,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Arc<LexicalProjectionReader>> {
        let (memory, task) = self.context()?;
        checkpoint(&task)?;
        let mut paths = build_manifest::Paths::new(root, generation, &memory, &task)?;
        let mut artifact_guard = build_manifest::Cleanup::new(&paths.artifact_tmp);
        let mut artifact = ArtifactBuilder::new_with_context(
            &paths.artifact_tmp,
            generation,
            self.config,
            memory.clone(),
            task.clone(),
        )?;
        let mut runs =
            SpillRuns::with_context(root, generation, self.config, memory.clone(), task.clone())?;
        let mut chunk = PendingPostings::new(Some(&memory))?;
        let mut document_count = 0u64;
        let mut total_document_len = 0u64;
        let mut consume = |document: &SearchDocument| -> Result<()> {
            let analyzed = document_frequency::analyze_with_control(
                document,
                analyzer,
                &mut runs,
                &mut chunk,
                crate::analyzer_stream::Control {
                    memory: Some(&memory),
                    task: Some(&task),
                    workspace: self.analyzer_workspace.as_deref(),
                },
            )?;
            let document_len = analyzed.document_len();
            document_count = document_count.saturating_add(1);
            total_document_len = total_document_len.saturating_add(u64::from(document_len));
            artifact.push_document(&document.id, document_len)?;
            if let document_frequency::AnalyzedDocument::Spilled { run, .. } = analyzed {
                chunk.flush(&mut runs)?;
                document_frequency::spill_postings(run, &document.id, document_len, &mut runs)?;
            } else {
                analyzed.visit(self.config, |term, term_frequency, retained| {
                    runs.prepare(term.len(), document.id.len())?;
                    let bytes = Posting::resident_bytes(&term, &document.id);
                    let posting_limit = self
                        .config
                        .build_memory_bytes
                        .get()
                        .saturating_sub(retained);
                    if bytes > posting_limit {
                        return Err(SkeinError::Storage(
                            "one lexical posting exceeds the build memory budget".into(),
                        ));
                    }
                    if !chunk.values.is_empty() && chunk.bytes.saturating_add(bytes) > posting_limit
                    {
                        chunk.flush(&mut runs)?;
                    }
                    chunk.push(term, &document.id, term_frequency, document_len)
                })?;
            }
            Ok(())
        };
        scan(&mut consume)?;
        artifact.finish_documents()?;
        chunk.flush(&mut runs)?;
        runs.compact()?;
        artifact.merge_postings_with_progress(&runs.paths, self.config, runs.progress.as_ref())?;
        let artifact = artifact.finish()?;
        let _format_memory = memory.retained.reserve("SKEIN_LEXICAL_MANIFEST_V1".len())?;
        let manifest = ManifestBody {
            format: "SKEIN_LEXICAL_MANIFEST_V1".to_string(),
            generation,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            artifact_file: std::mem::take(&mut paths.artifact_name),
            artifact_len: artifact.len,
            artifact_checksum: artifact.checksum,
            document_count,
            total_document_len,
            posting_count: artifact.posting_count,
            term_statistics: artifact.term_statistics,
            blocks: artifact.blocks,
        };
        let reader = build_manifest::finish(
            manifest,
            artifact.memory,
            &paths,
            self.config,
            &memory,
            &task,
        )?;
        artifact_guard.disarm();
        Ok(reader)
    }
}

struct RemoveOnDrop {
    path: PathBuf,
    armed: bool,
    _memory: Option<crate::build_memory::reserved::Grant>,
}

impl RemoveOnDrop {
    fn disarm(&mut self) {
        self.armed = false;
    }

    fn remove(&mut self) -> Result<()> {
        if let Some(memory) = &self._memory {
            memory.with_scratch(
                crate::build_memory::reserved::native_path::bytes(&self.path)?,
                || Ok(fs::remove_file(&self.path)?),
            )?;
        } else {
            fs::remove_file(&self.path)?;
        }
        self.disarm();
        Ok(())
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.remove();
        }
    }
}

struct SpillRuns {
    root: PathBuf,
    generation: u64,
    config: LexicalProjectionConfig,
    paths: Vec<RemoveOnDrop>,
    bytes: u64,
    sequence: usize,
    max_posting_bytes: u64,
    progress: Option<crate::build_memory::reserved::ReservedMemory>,
    context: Option<(BuildMemory, RuntimeTaskContext)>,
    max_term_bytes: usize,
    max_id_bytes: usize,
    prepared_paths: Option<usize>,
    path_slots: Option<crate::build_memory::reserved::Grant>,
    _root_memory: Option<QueryMemoryLease>,
}

trait SpillIo {
    type Writer: Write;

    fn create(&mut self, path: &Path) -> Result<Self::Writer>;
    fn remove(&mut self, path: &Path) -> Result<()>;
}

struct FileSpillIo;

impl SpillIo for FileSpillIo {
    type Writer = BufWriter<File>;

    fn create(&mut self, path: &Path) -> Result<Self::Writer> {
        Ok(BufWriter::with_capacity(
            SPILL_IO_BUFFER_BYTES,
            File::create(path)?,
        ))
    }

    fn remove(&mut self, path: &Path) -> Result<()> {
        Ok(fs::remove_file(path)?)
    }
}

fn checked_spill_bytes(current: u64, additional: u64, limit: NonZeroU64) -> Result<u64> {
    let required = current
        .checked_add(additional)
        .ok_or_else(|| SkeinError::Storage("lexical spill bytes overflow".to_string()))?;
    if required > limit.get() {
        return Err(SkeinError::Storage(format!(
            "lexical build requires {required} spill bytes, exceeding {limit}"
        )));
    }
    Ok(required)
}

struct SpillRunWriter<W> {
    writer: W,
    total_bytes: u64,
    limit: NonZeroU64,
    _buffer_memory: Option<crate::build_memory::reserved::Grant>,
    task: Option<RuntimeTaskContext>,
}

impl<W: Write> SpillRunWriter<W> {
    fn create_with_progress(
        path: &Path,
        previous_bytes: u64,
        limit: NonZeroU64,
        io: &mut impl SpillIo<Writer = W>,
        progress: Option<&crate::build_memory::reserved::ReservedMemory>,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        task.map_or(Ok(()), checkpoint)?;
        let total_bytes = checked_spill_bytes(previous_bytes, RUN_HEADER.len() as u64, limit)?;
        let buffer_memory = progress
            .map(|memory| memory.reserve(SPILL_IO_BUFFER_BYTES))
            .transpose()?;
        let mut writer = match progress {
            Some(progress) => progress.with_scratch(
                crate::build_memory::reserved::native_path::bytes(path)?,
                || io.create(path),
            )?,
            None => io.create(path)?,
        };
        writer.write_all(RUN_HEADER)?;
        Ok(Self {
            writer,
            total_bytes,
            limit,
            _buffer_memory: buffer_memory,
            task: task.cloned(),
        })
    }

    fn push(&mut self, posting: &Posting) -> Result<()> {
        self.push_parts(
            &posting.term,
            &posting.document_id,
            posting.term_frequency,
            posting.document_len,
        )
    }

    fn push_parts(&mut self, term: &str, id: &str, frequency: u32, length: u32) -> Result<()> {
        let bytes = (term.len() as u64)
            .saturating_add(id.len() as u64)
            .saturating_add(16);
        let total_bytes = checked_spill_bytes(self.total_bytes, bytes, self.limit)?;
        let mut writer =
            crate::build_control::CheckedWriter::new(&mut self.writer, self.task.as_ref());
        write_string(&mut writer, term)?;
        write_string(&mut writer, id)?;
        writer.write_all(&frequency.to_le_bytes())?;
        writer.write_all(&length.to_le_bytes())?;
        self.total_bytes = total_bytes;
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        crate::build_control::CheckedWriter::new(&mut self.writer, self.task.as_ref()).flush()?;
        Ok(self.total_bytes)
    }
}

impl SpillRuns {
    fn new(root: &Path, generation: u64, config: LexicalProjectionConfig) -> Self {
        Self {
            root: root.to_path_buf(),
            generation,
            config,
            paths: Vec::new(),
            bytes: 0,
            sequence: 0,
            max_posting_bytes: 0,
            progress: None,
            context: None,
            max_term_bytes: 0,
            max_id_bytes: 0,
            prepared_paths: None,
            path_slots: None,
            _root_memory: None,
        }
    }

    fn spill(&mut self, postings: &mut Vec<Posting>) -> Result<()> {
        self.spill_with_io(postings, &mut FileSpillIo)
    }

    fn spill_with_io(&mut self, postings: &mut Vec<Posting>, io: &mut impl SpillIo) -> Result<()> {
        postings.sort_unstable();
        postings.dedup();
        let limit = self.config.max_spill_bytes;
        // Unlike a merge, this run is already materialized: reject the entire
        // output before creating a file or consuming a run sequence number.
        postings.iter().try_fold(
            checked_spill_bytes(self.bytes, RUN_HEADER.len() as u64, limit)?,
            |bytes, posting| checked_spill_bytes(bytes, posting.encoded_len(), limit),
        )?;
        for posting in postings.iter() {
            self.prepare(posting.term.len(), posting.document_id.len())?;
        }
        let guard = self.next_guard()?;
        let mut writer = SpillRunWriter::create_with_progress(
            &guard.path,
            self.bytes,
            limit,
            io,
            self.progress.as_ref(),
            self.task(),
        )?;
        for posting in postings.iter() {
            writer.push(posting)?;
        }
        self.bytes = writer.finish()?;
        self.max_posting_bytes = self.max_posting_bytes.max(
            postings
                .iter()
                .map(|posting| Posting::resident_bytes(&posting.term, &posting.document_id))
                .max()
                .unwrap_or(0),
        );
        self.register(guard)?;
        postings.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        self.compact_with_io(&mut FileSpillIo)
    }

    fn compact_with_io(&mut self, io: &mut impl SpillIo) -> Result<()> {
        let configured_fan_in = self.config.max_merge_fan_in.get();
        if configured_fan_in < 2 {
            return Err(SkeinError::Storage(
                "lexical merge fan-in must be at least two".to_string(),
            ));
        }
        // The configured fan-in is a ceiling, not a mandate to retain that many
        // heads. Reserve a deduplication head too, using actual spilled records
        // rather than the caller's potentially generous term-length policy.
        let memory_fan_in = self
            .config
            .build_memory_bytes
            .get()
            .checked_div(self.max_posting_bytes)
            .unwrap_or(u64::MAX)
            .saturating_sub(1);
        let fan_in =
            configured_fan_in.min(usize::try_from(memory_fan_in).unwrap_or(usize::MAX).max(2));
        while self.paths.len() > fan_in {
            // Keep sources and completed destinations owned until the entire
            // level succeeds, including if a source unlink fails or unwinds.
            let old_len = self.paths.len();
            for start in (0..old_len).step_by(fan_in) {
                let end = start.saturating_add(fan_in).min(old_len);
                let guard = self.next_guard()?;
                self.bytes = merge_runs_with_progress(
                    &self.paths[start..end],
                    &guard.path,
                    self.config,
                    self.bytes,
                    io,
                    self.progress.as_ref(),
                    self.task(),
                )?;
                self.register(guard)?;
                for source in &mut self.paths[start..end] {
                    if let Some(memory) = &source._memory {
                        memory.with_scratch(
                            crate::build_memory::reserved::native_path::bytes(&source.path)?,
                            || io.remove(&source.path),
                        )?;
                    } else {
                        io.remove(&source.path)?;
                    }
                    source.disarm();
                }
            }
            self.paths.drain(..old_len);
        }
        Ok(())
    }

    fn next_path(&mut self) -> Result<PathBuf> {
        let required = self.sequence.checked_add(1).ok_or_else(|| {
            SkeinError::Storage("lexical spill run sequence overflow".to_string())
        })?;
        if required > self.config.max_spill_runs.get() {
            return Err(SkeinError::Storage(format!(
                "lexical build requires {required} spill runs, exceeding {}",
                self.config.max_spill_runs
            )));
        }
        let path = self.root.join(format!(
            ".search-lexical.{}.{}.tmp",
            self.generation, self.sequence
        ));
        self.sequence = required;
        Ok(path)
    }
}

struct RunReader {
    reader: BufReader<File>,
    config: LexicalProjectionConfig,
    progress: Option<crate::build_memory::reserved::ReservedMemory>,
    task: Option<RuntimeTaskContext>,
    _buffer_memory: Option<crate::build_memory::reserved::Grant>,
}

impl RunReader {
    #[cfg(test)]
    fn open(path: &Path, config: LexicalProjectionConfig) -> Result<Self> {
        Self::open_with_progress(path, config, None, None)
    }

    fn open_with_progress(
        path: &Path,
        config: LexicalProjectionConfig,
        progress: Option<&crate::build_memory::reserved::ReservedMemory>,
        task: Option<&RuntimeTaskContext>,
    ) -> Result<Self> {
        task.map_or(Ok(()), checkpoint)?;
        let memory = progress
            .map(|memory| memory.reserve(SPILL_IO_BUFFER_BYTES))
            .transpose()?;
        let file = match progress {
            Some(progress) => progress.with_scratch(
                crate::build_memory::reserved::native_path::bytes(path)?,
                || Ok(File::open(path)?),
            )?,
            None => File::open(path)?,
        };
        let mut reader = BufReader::with_capacity(SPILL_IO_BUFFER_BYTES, file);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(SkeinError::Storage(
                "lexical spill run header mismatch".into(),
            ));
        }
        Ok(Self {
            reader,
            config,
            progress: progress.cloned(),
            task: task.cloned(),
            _buffer_memory: memory,
        })
    }

    fn next(&mut self, available_bytes: u64) -> Result<Option<RunPosting>> {
        self.task.as_ref().map_or(Ok(()), checkpoint)?;
        let available_strings = available_bytes.saturating_sub(32);
        let Some(length) = read_optional_length(
            &mut self.reader,
            self.config.max_term_bytes.get().min(available_strings),
        )?
        else {
            return Ok(None);
        };
        if available_bytes < 32 {
            return Err(SkeinError::Storage(
                "lexical merge head exceeds the build memory budget".into(),
            ));
        }
        let term = if let Some(progress) = &self.progress {
            Term::build_reserved(length, progress, || {
                spill_memory::read_text(&mut self.reader, length, self.task.as_ref())
            })?
        } else {
            spill_memory::read_text(&mut self.reader, length, self.task.as_ref())?.into()
        };
        let id_length = read_optional_length(
            &mut self.reader,
            (1024 * 1024).min(available_strings.saturating_sub(term.len() as u64)),
        )?
        .ok_or_else(|| SkeinError::Storage("lexical spill run is truncated".into()))?;
        let id_memory = self
            .progress
            .as_ref()
            .map(|memory| memory.reserve(id_length))
            .transpose()?;
        let document_id = spill_memory::read_text(&mut self.reader, id_length, self.task.as_ref())?;
        let term_frequency = read_u32(&mut self.reader)?;
        let document_len = read_u32(&mut self.reader)?;
        Ok(Some(RunPosting {
            posting: Posting {
                term,
                document_id,
                term_frequency,
                document_len,
            },
            _id_memory: id_memory,
        }))
    }
}

#[allow(clippy::too_many_arguments)]
fn merge_runs_with_progress(
    paths: &[impl AsRef<Path>],
    destination: &Path,
    config: LexicalProjectionConfig,
    previous_bytes: u64,
    io: &mut impl SpillIo,
    progress: Option<&crate::build_memory::reserved::ReservedMemory>,
    task: Option<&RuntimeTaskContext>,
) -> Result<u64> {
    let mut writer = SpillRunWriter::create_with_progress(
        destination,
        previous_bytes,
        config.max_spill_bytes,
        io,
        progress,
        task,
    )?;
    visit_merged_postings_with_progress(paths, config, progress, task, |posting| {
        writer.push(posting)
    })?;
    writer.finish()
}

#[cfg(test)]
fn visit_merged_postings(
    paths: &[impl AsRef<Path>],
    config: LexicalProjectionConfig,
    consume: impl FnMut(&Posting) -> Result<()>,
) -> Result<()> {
    visit_merged_postings_with_progress(paths, config, None, None, consume)
}

fn visit_merged_postings_with_progress(
    paths: &[impl AsRef<Path>],
    config: LexicalProjectionConfig,
    progress: Option<&crate::build_memory::reserved::ReservedMemory>,
    task: Option<&RuntimeTaskContext>,
    mut consume: impl FnMut(&Posting) -> Result<()>,
) -> Result<()> {
    task.map_or(Ok(()), checkpoint)?;
    // Admit the complete registry before opening readers or growing the heap.
    let _reader_slots = progress
        .map(|memory| {
            memory.reserve(crate::build_memory::checked_mul(
                paths.len(),
                std::mem::size_of::<RunReader>(),
            )?)
        })
        .transpose()?;
    let mut readers = Vec::with_capacity(paths.len());
    for path in paths {
        readers.push(RunReader::open_with_progress(
            path.as_ref(),
            config,
            progress,
            task,
        )?);
    }
    let _heap_slots = progress
        .map(|memory| {
            memory.reserve(crate::build_memory::checked_mul(
                paths.len(),
                std::mem::size_of::<Reverse<(RunPosting, usize)>>(),
            )?)
        })
        .transpose()?;
    let mut heap = BinaryHeap::with_capacity(paths.len());
    let mut resident_bytes = 0u64;
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(posting) = reader.next(
            config
                .build_memory_bytes
                .get()
                .saturating_sub(resident_bytes),
        )? {
            resident_bytes += Posting::resident_bytes(&posting.term, &posting.document_id);
            heap.push(Reverse((posting, index)));
        }
    }
    let mut previous: Option<RunPosting> = None;
    while let Some(Reverse((posting, index))) = heap.pop() {
        task.map_or(Ok(()), checkpoint)?;
        if previous.as_ref() != Some(&posting) {
            consume(&posting)?;
            if let Some(previous) = previous.take() {
                resident_bytes -= Posting::resident_bytes(&previous.term, &previous.document_id);
            }
            previous = Some(posting);
        } else {
            resident_bytes -= Posting::resident_bytes(&posting.term, &posting.document_id);
        }
        if let Some(next) = readers[index].next(
            config
                .build_memory_bytes
                .get()
                .saturating_sub(resident_bytes),
        )? {
            resident_bytes += Posting::resident_bytes(&next.term, &next.document_id);
            heap.push(Reverse((next, index)));
        }
    }
    Ok(())
}

fn encode_block_header(
    output: &mut impl Write,
    generation: u64,
    block_id: u64,
    kind: BlockKind,
    count: usize,
) -> Result<()> {
    let count = u32::try_from(count)
        .map_err(|_| SkeinError::Storage("lexical block count exceeds u32".to_string()))?;
    output.write_all(BLOCK_HEADER)?;
    output.write_all(&generation.to_le_bytes())?;
    output.write_all(&block_id.to_le_bytes())?;
    output.write_all(&[match kind {
        BlockKind::Documents => 1,
        BlockKind::Postings => 2,
    }])?;
    output.write_all(&count.to_le_bytes())?;
    Ok(())
}

fn encode_posting(mut writer: impl Write, posting: &Posting) -> Result<()> {
    write_string(&mut writer, &posting.term)?;
    write_string(&mut writer, &posting.document_id)?;
    writer.write_all(&posting.term_frequency.to_le_bytes())?;
    writer.write_all(&posting.document_len.to_le_bytes())?;
    Ok(())
}

fn decode_posting_block(
    bytes: &[u8],
    generation: u64,
    descriptor: &BlockDescriptor,
    max_term_bytes: u64,
    mut consumer: impl FnMut(Posting) -> Result<()>,
) -> Result<()> {
    let mut cursor = SliceCursor::new(bytes);
    let count = decode_block_header(&mut cursor, generation, descriptor, BlockKind::Postings)?;
    let mut first = None;
    let mut previous = None;
    for _ in 0..count {
        let posting = Posting {
            term: cursor.string(max_term_bytes)?.into(),
            document_id: cursor.string(1024 * 1024)?,
            term_frequency: cursor.u32()?,
            document_len: cursor.u32()?,
        };
        if posting.term_frequency == 0
            || posting.document_len == 0
            || previous
                .as_ref()
                .is_some_and(|previous: &Posting| previous >= &posting)
        {
            return Err(SkeinError::Storage(
                "lexical posting block is invalid or unordered".to_string(),
            ));
        }
        if first.is_none() {
            first = Some(posting.term.as_str().to_owned());
        }
        consumer(posting.clone())?;
        previous = Some(posting);
    }
    let previous_term = previous
        .map(|posting| posting.term.into_untracked())
        .transpose()?;
    validate_block_tail(cursor, descriptor, first, previous_term)
}

fn decode_block_header(
    cursor: &mut SliceCursor<'_>,
    generation: u64,
    descriptor: &BlockDescriptor,
    kind: BlockKind,
) -> Result<u32> {
    if cursor.bytes(8)? != BLOCK_HEADER
        || cursor.u64()? != generation
        || cursor.u64()? != descriptor.block_id
        || cursor.u8()?
            != match kind {
                BlockKind::Documents => 1,
                BlockKind::Postings => 2,
            }
    {
        return Err(SkeinError::Storage(
            "lexical block header does not match its manifest".to_string(),
        ));
    }
    let count = cursor.u32()?;
    if count != descriptor.entry_count {
        return Err(SkeinError::Storage(
            "lexical block count does not match its manifest".to_string(),
        ));
    }
    Ok(count)
}

fn validate_block_tail(
    cursor: SliceCursor<'_>,
    descriptor: &BlockDescriptor,
    first: Option<String>,
    last: Option<String>,
) -> Result<()> {
    if !cursor.is_empty()
        || first.as_deref() != Some(descriptor.min_key.as_str())
        || last.as_deref() != Some(descriptor.max_key.as_str())
    {
        return Err(SkeinError::Storage(
            "lexical block payload bounds do not match its manifest".to_string(),
        ));
    }
    Ok(())
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| SkeinError::Storage("lexical block cursor overflow".to_string()))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| SkeinError::Storage("lexical block is truncated".to_string()))?;
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    fn string(&mut self, max: u64) -> Result<String> {
        let length = self.u32()? as usize;
        if length as u64 > max {
            return Err(SkeinError::Storage(format!(
                "lexical string uses {length} bytes, exceeding {max}"
            )));
        }
        String::from_utf8(self.bytes(length)?.to_vec())
            .map_err(|error| SkeinError::Storage(format!("invalid lexical UTF-8: {error}")))
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn write_string(writer: &mut impl Write, value: &str) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| SkeinError::Storage("lexical string exceeds u32".to_string()))?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_optional_length(reader: &mut impl Read, max: u64) -> Result<Option<usize>> {
    let mut length = [0u8; 4];
    match reader.read(&mut length)? {
        0 => return Ok(None),
        4 => {}
        count => {
            reader.read_exact(&mut length[count..])?;
        }
    }
    let length = u32::from_le_bytes(length) as usize;
    if length as u64 > max {
        return Err(SkeinError::Storage(
            "lexical spill string exceeds its admitted length".to_string(),
        ));
    }
    Ok(Some(length))
}

fn read_optional_string(reader: &mut impl Read, max: u64) -> Result<Option<String>> {
    let Some(length) = read_optional_length(reader, max)? else {
        return Ok(None);
    };
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| SkeinError::Storage(error.to_string()))
}

fn read_string(reader: &mut impl Read, max: u64) -> Result<String> {
    read_optional_string(reader, max)?
        .ok_or_else(|| SkeinError::Storage("lexical spill run is truncated".to_string()))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn file_digest(file: &File) -> Result<(u64, u64)> {
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Digest::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        total = total.saturating_add(read as u64);
    }
    Ok((total, digest.finish()))
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut digest = Digest::new();
    digest.update(bytes);
    digest.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "full-text-search")]
    mod checkpoint;
    mod robustness;

    fn projection_root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "skein-lexical-projection-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn document(id: &str, title: &str, content: &str) -> SearchDocument {
        SearchDocument {
            id: id.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        }
    }

    fn term_posting_bytes(reader: &LexicalProjectionReader, term: &str) -> u64 {
        reader
            .manifest
            .blocks
            .iter()
            .filter(|block| {
                block.kind == BlockKind::Postings
                    && block.min_key.as_str() <= term
                    && term <= block.max_key.as_str()
            })
            .map(|block| block.length)
            .sum()
    }

    #[test]
    fn internal_reopen_preserves_the_manifest_budget() {
        let root = projection_root("manifest-budget-reopen");
        fs::create_dir_all(&root).unwrap();
        let documents = [document("a", "Graph", "storage")];
        let config = LexicalProjectionConfig {
            max_manifest_bytes: NonZeroU64::new(4096).unwrap(),
            ..Default::default()
        };
        let reader = LexicalProjectionWriter::new(config)
            .write(
                &root,
                1,
                None,
                11,
                13,
                documents.iter(),
                &Default::default(),
            )
            .unwrap();
        assert_eq!(reader.config.max_manifest_bytes, config.max_manifest_bytes);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn projection_scores_match_reference_bm25_and_reopens() {
        let root = projection_root("reference");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let analyzer = SearchAnalyzerLexicon::default();
        let documents = BTreeMap::from([
            ("a".to_string(), document("a", "Graph Graph", "storage")),
            ("b".to_string(), document("b", "Graph", "memory retrieval")),
            ("c".to_string(), document("c", "Vector", "embedding")),
        ]);
        let config = LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(256).unwrap(),
            target_block_bytes: NonZeroU64::new(128).unwrap(),
            max_block_bytes: NonZeroU64::new(1024).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            ..LexicalProjectionConfig::default()
        };
        let reader = LexicalProjectionWriter::new(config)
            .write(&root, 1, Some(7), 11, 13, documents.values(), &analyzer)
            .unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);
        let report = reader
            .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
            .unwrap();
        assert_eq!(reader.manifest.document_frequency("graph"), 2);
        assert_eq!(report.bytes_read, term_posting_bytes(&reader, "graph"));
        let corpus = super::super::TextCorpusStats::from_documents(documents.values(), &analyzer);
        for document in documents.values() {
            let expected = super::super::bm25_score(&terms, document, &corpus, &analyzer);
            assert_eq!(
                report.scores.get(&document.id).copied().unwrap_or(0.0),
                expected
            );
        }
        let reopened = LexicalProjectionReader::load(&root, Some(7), 11, 13, config)
            .unwrap()
            .unwrap();
        assert_eq!(reopened.generation(), 1);
        let mut invalid_manifest = reopened.manifest.clone();
        invalid_manifest
            .term_statistics
            .iter_mut()
            .find(|statistics| statistics.term == "graph")
            .unwrap()
            .document_frequency += 1;
        assert!(invalid_manifest.validate().is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn filtered_scores_use_manifest_corpus_without_reading_document_blocks() {
        let root = projection_root("filtered-manifest-corpus");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let analyzer = SearchAnalyzerLexicon::default();
        let documents = BTreeMap::from([
            ("a".to_string(), document("a", "Graph Graph", "storage")),
            (
                "b".to_string(),
                document("b", "Graph", "embedding index with several tokens"),
            ),
        ]);
        let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
            .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
            .unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);

        let allowed_calls = std::cell::Cell::new(0usize);
        let report = reader
            .score(&terms, &LexicalMiniDelta::default(), None, |id| {
                allowed_calls.set(allowed_calls.get().saturating_add(1));
                Ok(id == "a")
            })
            .unwrap();

        let corpus = super::super::TextCorpusStats::from_documents(documents.values(), &analyzer);
        let expected = super::super::bm25_score(&terms, &documents["a"], &corpus, &analyzer);
        assert_eq!(report.scores, BTreeMap::from([("a".to_string(), expected)]));
        assert_eq!(allowed_calls.get(), 2);
        assert_eq!(report.bytes_read, term_posting_bytes(&reader, "graph"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delta_scores_do_not_read_document_blocks() {
        let root = projection_root("delta-postings-only");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let analyzer = SearchAnalyzerLexicon::default();
        let documents = BTreeMap::from([
            ("a".to_string(), document("a", "Graph", "storage")),
            ("b".to_string(), document("b", "Vector", "embedding")),
        ]);
        let config = LexicalProjectionConfig::default();
        let reader = LexicalProjectionWriter::new(config)
            .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
            .unwrap();
        let mut delta = Arc::new(LexicalMiniDelta::default());
        delta
            .upsert(&document("c", "Graph", "query"), None, &analyzer, config)
            .unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);

        let report = reader.score(&terms, &delta, None, |_| Ok(true)).unwrap();

        assert_eq!(report.scores.keys().collect::<Vec<_>>(), vec!["a", "c"]);
        assert_eq!(report.bytes_read, term_posting_bytes(&reader, "graph"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn mini_delta_updates_persisted_term_statistics_without_a_counting_pass() {
        let root = projection_root("delta-term-statistics");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let analyzer = SearchAnalyzerLexicon::default();
        let documents = BTreeMap::from([
            ("a".to_string(), document("a", "Graph Graph", "storage")),
            ("b".to_string(), document("b", "Graph", "memory")),
            ("c".to_string(), document("c", "Vector", "embedding")),
        ]);
        let config = LexicalProjectionConfig::default();
        let reader = LexicalProjectionWriter::new(config)
            .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
            .unwrap();
        let first_update = document("a", "Graph", "updated");
        let final_update = document("a", "Vector", "updated");
        let inserted = document("d", "Graph", "query");
        let mut delta = Arc::new(LexicalMiniDelta::default());
        delta
            .upsert(&first_update, Some(&documents["a"]), &analyzer, config)
            .unwrap();
        delta
            .upsert(&final_update, Some(&first_update), &analyzer, config)
            .unwrap();
        assert!(delta
            .delete("b", Some(&documents["b"]), &analyzer, config)
            .unwrap());
        delta.upsert(&inserted, None, &analyzer, config).unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);

        let report = reader.score(&terms, &delta, None, |_| Ok(true)).unwrap();

        let current_documents = [&final_update, &documents["c"], &inserted];
        let corpus =
            super::super::TextCorpusStats::from_documents(current_documents.into_iter(), &analyzer);
        let expected = super::super::bm25_score(&terms, &inserted, &corpus, &analyzer);
        assert_eq!(report.scores, BTreeMap::from([("d".to_string(), expected)]));
        assert_eq!(report.bytes_read, term_posting_bytes(&reader, "graph"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn empty_projection_uses_delta_normalization_corpus() {
        let root = projection_root("empty-base-delta");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        let analyzer = SearchAnalyzerLexicon::default();
        let documents = BTreeMap::<String, SearchDocument>::new();
        let config = LexicalProjectionConfig::default();
        let reader = LexicalProjectionWriter::new(config)
            .write(&root, 1, None, 11, 13, documents.values(), &analyzer)
            .unwrap();
        let mut delta = Arc::new(LexicalMiniDelta::default());
        delta
            .upsert(&document("a", "Graph", "query"), None, &analyzer, config)
            .unwrap();

        let report = reader
            .score(&BTreeSet::from(["graph".to_string()]), &delta, None, |_| {
                Ok(true)
            })
            .unwrap();

        assert!(report.scores["a"].is_finite());
        assert!(report.scores["a"] > 0.0);
        assert_eq!(report.bytes_read, 0);
        fs::remove_dir_all(root).unwrap();
    }
}
