use super::cjk_tokenizer::ANALYZER_FORMAT_VERSION;
use super::{document_tokens, SearchAnalyzerLexicon, SearchDocument, BM25_B, BM25_K1};
use crate::build_control::checkpoint;
use crate::error::{Result, SkeinError};
use serde::{Deserialize, Serialize};
use skein_core::RuntimeTaskContext;
use skein_integrity::Crc32cHasher as Digest;
use skein_storage::{durable_replace_file, SegmentCache, StoreId};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static CACHE_NAMESPACE: AtomicU64 = AtomicU64::new(1);
pub(super) const DEFAULT_CACHE_BYTES: u64 = 32 * 1024 * 1024;

mod dictionary;
mod dictionary_store;
mod doclist;
mod documents;
mod fst_validation;
mod manifest_io;
mod posting_codec;
mod read_context;
use documents::DocumentLookup;
use read_context::ReadContext;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINLEXICAL0001";
const BLOCK_HEADER: &[u8; 8] = b"SKNLEX01";
const RUN_HEADER: &[u8; 8] = b"SKNLEXR1";
const MAX_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MANIFEST_FILE: &str = "search_lexical.manifest.skein";

pub(super) fn artifact_file(generation: u64) -> String {
    format!("search_lexical.{generation}.skein")
}

pub(super) fn manifest_generation(path: &Path) -> Result<u64> {
    let bytes = manifest_io::read(path, MAX_MANIFEST_BYTES)?;
    Ok(ManifestBody::decode_bounded(&bytes, MAX_MANIFEST_BYTES)?.generation)
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
    pub build_memory_bytes: NonZeroU64,
    pub dictionary_build_memory_bytes: NonZeroU64,
    pub dictionary_validation_bytes: NonZeroU64,
    pub max_directory_bytes: NonZeroU64,
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
            build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            dictionary_build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            dictionary_validation_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_directory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
            max_spill_bytes: NonZeroU64::new(4 * 1024 * 1024 * 1024 * 1024).unwrap(),
            max_spill_runs: NonZeroUsize::new(4_096).unwrap(),
            max_merge_fan_in: NonZeroUsize::new(32).unwrap(),
            target_block_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            max_block_bytes: NonZeroU64::new(2 * 1024 * 1024).unwrap(),
            max_term_bytes: NonZeroU64::new(4 * 1024).unwrap(),
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

/// Disjoint physical byte categories of one immutable lexical artifact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchLexicalArtifactBytes {
    pub header_bytes: u64,
    pub document_mapping_bytes: u64,
    pub posting_frame_bytes: u64,
    pub posting_skip_bytes: u64,
    pub dictionary_bytes: u64,
    /// Exact size of the former full-string posting records for this input.
    /// Excludes old block headers and is not a measured legacy artifact size.
    pub uncompressed_posting_payload_bytes: u64,
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
    ordinal_start: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestBody {
    format: String,
    layout: String,
    generation: u64,
    source_graph_commit_epoch: Option<u64>,
    analyzer_digest: u64,
    documents_digest: u64,
    artifact_file: String,
    artifact_len: u64,
    artifact_checksum: u64,
    byte_counters: SearchLexicalArtifactBytes,
    document_count: u64,
    total_document_len: u64,
    posting_count: u64,
    posting_offset: u64,
    posting_bytes: u64,
    dictionaries: Vec<dictionary_store::Descriptor>,
    blocks: Vec<BlockDescriptor>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestEnvelope {
    body: ManifestBody,
    checksum: u64,
}

impl ManifestBody {
    fn validate(&self) -> Result<()> {
        let invalid =
            || SkeinError::Storage("invalid lexical compact manifest bounds or counts".to_string());
        if self.format != "SKEIN_LEXICAL_MANIFEST_V1"
            || self.layout != "SKEIN_LEXICAL_COMPACT_V1"
            || self.artifact_file != artifact_file(self.generation)
        {
            return Err(invalid());
        }
        let mut end = ARTIFACT_HEADER.len() as u64 + 8;
        let mut documents = 0u64;
        let mut previous_id: Option<&str> = None;
        for (index, block) in self.blocks.iter().enumerate() {
            if block.kind != BlockKind::Documents
                || block.block_id != index as u64
                || block.entry_count == 0
                || block.length == 0
                || block.min_key > block.max_key
                || block.ordinal_start != documents
                || block.offset != end
                || previous_id.is_some_and(|previous| previous >= block.min_key.as_str())
            {
                return Err(invalid());
            }
            end = end.checked_add(block.length).ok_or_else(invalid)?;
            documents = documents
                .checked_add(u64::from(block.entry_count))
                .ok_or_else(invalid)?;
            previous_id = Some(&block.max_key);
        }
        if documents != self.document_count || end != self.posting_offset {
            return Err(invalid());
        }
        end = end.checked_add(self.posting_bytes).ok_or_else(invalid)?;
        let mut posting_end = self.posting_offset;
        let mut postings = 0u64;
        let mut previous_term: Option<&str> = None;
        for dictionary in &self.dictionaries {
            if dictionary.length == 0
                || dictionary.term_count == 0
                || dictionary.posting_count == 0
                || dictionary.min_term.is_empty()
                || dictionary.min_term > dictionary.max_term
                || previous_term.is_some_and(|term| term >= dictionary.min_term.as_str())
                || dictionary.offset != end
                || dictionary.posting_offset != posting_end
                || dictionary.posting_bytes == 0
            {
                return Err(invalid());
            }
            end = end.checked_add(dictionary.length).ok_or_else(invalid)?;
            posting_end = posting_end
                .checked_add(dictionary.posting_bytes)
                .ok_or_else(invalid)?;
            postings = postings
                .checked_add(dictionary.posting_count)
                .ok_or_else(invalid)?;
            previous_term = Some(&dictionary.max_term);
        }
        if end != self.artifact_len
            || postings != self.posting_count
            || posting_end != self.posting_offset + self.posting_bytes
        {
            return Err(invalid());
        }
        let bytes = self.byte_counters;
        if bytes.header_bytes != ARTIFACT_HEADER.len() as u64 + 8
            || bytes.document_mapping_bytes != self.posting_offset - bytes.header_bytes
            || bytes
                .posting_frame_bytes
                .checked_add(bytes.posting_skip_bytes)
                != Some(self.posting_bytes)
            || bytes
                .header_bytes
                .checked_add(bytes.document_mapping_bytes)
                .and_then(|total| total.checked_add(self.posting_bytes))
                .and_then(|total| total.checked_add(bytes.dictionary_bytes))
                != Some(self.artifact_len)
            || (self.posting_count == 0) != (bytes.uncompressed_posting_payload_bytes == 0)
            || self
                .posting_count
                .checked_mul(16)
                .is_none_or(|minimum| minimum > bytes.uncompressed_posting_payload_bytes)
        {
            return Err(invalid());
        }
        Ok(())
    }

    #[cfg(test)]
    fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_bounded(bytes, MAX_MANIFEST_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Posting {
    term: String,
    ordinal: u64,
    term_frequency: u32,
}

impl Posting {
    fn resident_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64).saturating_add(self.term.len() as u64)
    }

    fn encoded_len(&self) -> u64 {
        4u64.saturating_add(self.term.len() as u64)
            .saturating_add(12)
    }
}

#[derive(Debug, Clone)]
struct DeltaDocument {
    document_len: u32,
    frequencies: BTreeMap<String, u32>,
    resident_bytes: u64,
    base: Option<BaseDocumentTerms>,
}

impl DeltaDocument {
    fn total_resident_bytes(&self) -> u64 {
        self.resident_bytes
            .saturating_add(self.base.as_ref().map_or(0, |base| base.resident_bytes))
    }
}

#[derive(Debug, Clone)]
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
    upserts: BTreeMap<String, DeltaDocument>,
    deletes: BTreeMap<String, BaseDocumentTerms>,
    resident_bytes: u64,
}

impl LexicalMiniDelta {
    pub(super) fn upsert(
        &mut self,
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
        };
        let removed_upsert = self
            .upserts
            .get(&document.id)
            .map_or(0, DeltaDocument::total_resident_bytes);
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
        self.upserts.remove(&document.id);
        self.deletes.remove(&document.id);
        self.resident_bytes = required;
        self.upserts.insert(document.id.clone(), delta);
        Ok(())
    }

    pub(super) fn delete(
        &mut self,
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
        };
        let removed = existing_upsert.map_or(0, DeltaDocument::total_resident_bytes);
        let Some(base) = base else {
            self.upserts.remove(document_id);
            self.resident_bytes = self.resident_bytes.saturating_sub(removed);
            return Ok(true);
        };
        let required = self
            .resident_bytes
            .saturating_sub(removed)
            .saturating_add(base.resident_bytes);
        if required > config.mini_delta_bytes.get() {
            return Ok(false);
        }
        self.upserts.remove(document_id);
        self.deletes.insert(document_id.to_string(), base);
        self.resident_bytes = required;
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
    let tokens = document_tokens(document, analyzer);
    if tokens.len() > config.max_document_tokens.get() {
        return Err(SkeinError::Storage(format!(
            "lexical document {} produced {} tokens, exceeding {}",
            document.id,
            tokens.len(),
            config.max_document_tokens
        )));
    }
    let document_len = u32::try_from(tokens.len())
        .map_err(|_| SkeinError::Storage("lexical document length exceeds u32".to_string()))?;
    let mut frequencies = BTreeMap::<String, u32>::new();
    let mut resident_bytes = document.id.len() as u64 + 64;
    for term in tokens {
        if term.len() as u64 > config.max_term_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical term uses {} bytes, exceeding {}",
                term.len(),
                config.max_term_bytes
            )));
        }
        if !frequencies.contains_key(&term) {
            resident_bytes = resident_bytes.saturating_add(term.len() as u64 + 32);
            if resident_bytes > config.build_memory_bytes.get() {
                return Err(SkeinError::Storage(format!(
                    "lexical document {} requires more than {} analyzer bytes",
                    document.id, config.build_memory_bytes
                )));
            }
        }
        let frequency = frequencies.entry(term).or_default();
        *frequency = frequency.saturating_add(1);
    }
    Ok(DeltaDocument {
        document_len,
        frequencies,
        resident_bytes,
        base: None,
    })
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct LexicalQueryReport {
    pub scores: BTreeMap<String, f64>,
    pub matching_document_count: usize,
    pub postings_visited: u64,
    pub bytes_read: u64,
    pub document_bytes_read: u64,
    pub dictionary_bytes_read: u64,
    pub posting_bytes_read: u64,
}

#[derive(Debug)]
pub(super) struct LexicalProjectionReader {
    manifest: ManifestBody,
    file: Arc<File>,
    config: LexicalProjectionConfig,
    cache: Arc<SegmentCache>,
    cache_namespace: StoreId,
}

impl LexicalProjectionReader {
    #[cfg(test)]
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

    #[cfg(test)]
    pub(super) fn load_named(
        root: &Path,
        manifest_file: &str,
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
    ) -> Result<Option<Arc<Self>>> {
        Self::load_named_with_cache(
            root,
            manifest_file,
            expected_source_epoch,
            expected_analyzer_digest,
            expected_documents_digest,
            config,
            Arc::new(SegmentCache::new(DEFAULT_CACHE_BYTES)),
        )
    }

    pub(super) fn load_named_with_cache(
        root: &Path,
        manifest_file: &str,
        expected_source_epoch: Option<u64>,
        expected_analyzer_digest: u64,
        expected_documents_digest: u64,
        config: LexicalProjectionConfig,
        cache: Arc<SegmentCache>,
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
        let manifest_bytes = manifest_io::read(&manifest_path, config.max_directory_bytes.get())?;
        let manifest =
            ManifestBody::decode_bounded(&manifest_bytes, config.max_directory_bytes.get())?;
        drop(manifest_bytes);
        if manifest.source_graph_commit_epoch != expected_source_epoch
            || manifest.analyzer_digest != expected_analyzer_digest
            || manifest.documents_digest != expected_documents_digest
        {
            return Ok(None);
        }
        if manifest
            .blocks
            .iter()
            .any(|block| block.length > config.max_block_bytes.get())
        {
            return Err(SkeinError::Storage(
                "lexical projection contains a block above the read admission limit".to_string(),
            ));
        }
        let artifact_path = root.join(&manifest.artifact_file);
        let file = File::open(&artifact_path)?;
        if file.metadata()?.len() != manifest.artifact_len {
            return Err(SkeinError::Storage(
                "lexical projection artifact length mismatch".to_string(),
            ));
        }
        let (length, digest) = file_digest(&file)?;
        if length != manifest.artifact_len || digest != manifest.artifact_checksum {
            return Err(SkeinError::Storage(
                "lexical projection artifact checksum mismatch".to_string(),
            ));
        }
        let mut header = [0u8; 24];
        let mut cloned = file.try_clone()?;
        cloned.seek(SeekFrom::Start(0))?;
        cloned.read_exact(&mut header)?;
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
            cache,
            cache_namespace: StoreId(u128::from(
                CACHE_NAMESPACE
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                        next.checked_add(1)
                    })
                    .map_err(|_| {
                        SkeinError::Storage("lexical cache namespace exhausted".to_string())
                    })?,
            )),
        })))
    }

    pub(super) fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub(super) fn artifact_bytes(&self) -> SearchLexicalArtifactBytes {
        self.manifest.byte_counters
    }

    #[cfg(test)]
    pub(super) fn score(
        &self,
        query_terms: &BTreeSet<String>,
        delta: &LexicalMiniDelta,
        retained_score_limit: Option<usize>,
        allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<LexicalQueryReport> {
        self.score_with_context(query_terms, delta, retained_score_limit, None, allowed)
    }

    pub(super) fn score_with_context(
        &self,
        query_terms: &BTreeSet<String>,
        delta: &LexicalMiniDelta,
        retained_score_limit: Option<usize>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
        mut allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<LexicalQueryReport> {
        let read = ReadContext {
            projection: self,
            task: task_context,
        };
        read.checkpoint()?;
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
        if query_terms
            .iter()
            .any(|term| term.len() as u64 > self.config.max_term_bytes.get())
        {
            return Err(SkeinError::Storage(
                "lexical query term exceeds its byte budget".to_string(),
            ));
        }
        let per_stream_bytes = (posting_codec::BLOCK_LEN
            * std::mem::size_of::<posting_codec::Posting>()) as u64
            + posting_codec::MAX_BLOCK_BYTES as u64
            + self.config.max_term_bytes.get().saturating_mul(2)
            + 128;
        let admitted_stream_bytes = (query_terms.len() as u64)
            .saturating_mul(per_stream_bytes)
            .saturating_add(self.config.max_block_bytes.get().saturating_mul(2))
            .saturating_add(self.config.dictionary_validation_bytes.get());
        let reservation = task_context.and_then(|context| context.memory_reservation());
        let query_memory_bytes =
            reservation.map_or(self.config.query_memory_bytes.get(), |reservation| {
                self.config
                    .query_memory_bytes
                    .get()
                    .min(reservation.memory_bytes())
            });
        if admitted_stream_bytes > query_memory_bytes {
            return Err(SkeinError::Storage(format!(
                "lexical query streams require {admitted_stream_bytes} bytes, exceeding {}",
                query_memory_bytes
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
        let mut collector = ScoreCollector::new(
            retained_score_limit,
            self.config.max_query_score_entries.get(),
            reservation.map_or(query_memory_bytes - admitted_stream_bytes, |reservation| {
                (query_memory_bytes - admitted_stream_bytes).min(reservation.result_bytes())
            }),
        )?;
        let mut document_frequency = BTreeMap::new();
        let mut base_metadata = BTreeMap::new();
        let mut postings_visited = 0u64;
        for term in query_terms {
            let metadata = read.term_metadata(term, &mut bytes_read)?;
            document_frequency.insert(
                term.clone(),
                delta
                    .projected_document_frequency(term, metadata.map_or(0, |metadata| metadata.df)),
            );
            base_metadata.insert(term, metadata);
        }
        let average_document_len = (total_document_len as f64 / document_count as f64).max(1.0);
        let mut streams = Vec::new();
        let mut stream_idf = Vec::new();
        for term in query_terms {
            let df = document_frequency.get(term).copied().unwrap_or(0);
            if df > 0
                && let Some(metadata) = base_metadata[term]
            {
                streams.push(TermPostingStream::with_context(read, term, metadata)?);
                stream_idf.push(idf(document_count, df));
            }
        }
        let mut heap = BinaryHeap::new();
        let mut documents = DocumentLookup::with_context(read);
        for (index, stream) in streams.iter_mut().enumerate() {
            if let Some(posting) = stream.next()? {
                heap.push(Reverse((posting.ordinal, index, posting)));
            }
        }
        while let Some(Reverse((ordinal, stream_index, posting))) = heap.pop() {
            read.checkpoint()?;
            let (document_id, document_len) = documents.get(ordinal)?;
            let mut score = 0.0;
            validate_posting_length(&posting, document_len)?;
            if !delta.overrides(&document_id) && allowed(&document_id)? {
                score += bm25_term_score(
                    stream_idf[stream_index],
                    posting.term_frequency,
                    document_len,
                    average_document_len,
                );
            }
            if let Some(next) = streams[stream_index].next()? {
                heap.push(Reverse((next.ordinal, stream_index, next)));
            }
            while heap
                .peek()
                .is_some_and(|Reverse((next_ordinal, _, _))| *next_ordinal == ordinal)
            {
                let Reverse((_, next_stream_index, next_posting)) =
                    heap.pop().expect("peeked lexical posting exists");
                validate_posting_length(&next_posting, document_len)?;
                if !delta.overrides(&document_id) && allowed(&document_id)? {
                    score += bm25_term_score(
                        stream_idf[next_stream_index],
                        next_posting.term_frequency,
                        document_len,
                        average_document_len,
                    );
                }
                if let Some(next) = streams[next_stream_index].next()? {
                    heap.push(Reverse((next.ordinal, next_stream_index, next)));
                }
            }
            if score > 0.0 {
                collector.push(document_id, score)?;
            }
        }
        let dictionary_bytes_read = bytes_read;
        let mut posting_bytes_read = 0u64;
        for stream in &streams {
            postings_visited = postings_visited.saturating_add(stream.postings_visited);
            bytes_read = bytes_read.saturating_add(stream.bytes_read);
            posting_bytes_read = posting_bytes_read.saturating_add(stream.bytes_read);
        }
        for (id, document) in &delta.upserts {
            read.checkpoint()?;
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
        read.checkpoint()?;
        Ok(LexicalQueryReport {
            scores,
            matching_document_count,
            postings_visited,
            bytes_read: bytes_read.saturating_add(documents.bytes_read),
            document_bytes_read: documents.bytes_read,
            dictionary_bytes_read,
            posting_bytes_read,
        })
    }

    #[cfg(test)]
    fn read_range(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        ReadContext {
            projection: self,
            task: None,
        }
        .read_range(offset, length)
    }

    #[cfg(test)]
    fn term_metadata(
        &self,
        term: &str,
        bytes_read: &mut u64,
    ) -> Result<Option<dictionary::Metadata>> {
        ReadContext {
            projection: self,
            task: None,
        }
        .term_metadata(term, bytes_read)
    }

    #[cfg(test)]
    fn read_cached_range(
        &self,
        offset: u64,
        length: usize,
        digest: u64,
    ) -> Result<(Arc<[u8]>, u64)> {
        ReadContext {
            projection: self,
            task: None,
        }
        .read_cached_range(offset, length, digest)
    }

    #[cfg(test)]
    fn read_block(&self, block: &BlockDescriptor) -> Result<Vec<u8>> {
        if block.length > self.config.max_block_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical block {} exceeds the read budget",
                block.block_id
            )));
        }
        let length = usize::try_from(block.length).map_err(|_| {
            SkeinError::Storage("lexical block exceeds the address space".to_string())
        })?;
        let mut bytes = vec![0u8; length];
        super::out_of_core::read_exact_at(&self.file, block.offset, &mut bytes)?;
        if checksum(&bytes) != block.checksum {
            return Err(SkeinError::Storage(format!(
                "lexical block {} checksum mismatch",
                block.block_id
            )));
        }
        Ok(bytes)
    }
}

fn validate_posting_length(posting: &Posting, document_len: u32) -> Result<()> {
    if posting.term_frequency > document_len {
        return Err(SkeinError::Storage(
            "lexical term frequency exceeds its document length".to_string(),
        ));
    }
    Ok(())
}

struct TermPostingStream<'a> {
    read: ReadContext<'a>,
    term: &'a str,
    cursor: doclist::Cursor,
    current: std::vec::IntoIter<posting_codec::Posting>,
    postings_visited: u64,
    bytes_read: u64,
}

impl<'a> TermPostingStream<'a> {
    #[cfg(test)]
    fn new(
        projection: &'a LexicalProjectionReader,
        term: &'a str,
        metadata: dictionary::Metadata,
    ) -> Result<Self> {
        Self::with_context(
            ReadContext {
                projection,
                task: None,
            },
            term,
            metadata,
        )
    }

    fn with_context(
        read: ReadContext<'a>,
        term: &'a str,
        metadata: dictionary::Metadata,
    ) -> Result<Self> {
        Ok(Self {
            read,
            term,
            cursor: doclist::Cursor::new(metadata)?,
            current: Vec::new().into_iter(),
            postings_visited: 0,
            bytes_read: 0,
        })
    }

    fn next(&mut self) -> Result<Option<Posting>> {
        self.read.checkpoint()?;
        loop {
            if let Some(posting) = self.current.next() {
                self.postings_visited += 1;
                return Ok(Some(Posting {
                    term: self.term.to_owned(),
                    ordinal: posting.ordinal,
                    term_frequency: posting.tf,
                }));
            }
            let Some(postings) = self.cursor.next_frame(&mut |offset, length, digest| {
                let (bytes, read) = if let Some(digest) = digest {
                    self.read.read_cached_range(offset, length, digest)?
                } else {
                    (
                        Arc::from(self.read.read_range(offset, length)?),
                        length as u64,
                    )
                };
                self.bytes_read = self.bytes_read.saturating_add(read);
                Ok(bytes)
            })?
            else {
                return Ok(None);
            };
            if postings.last().unwrap().ordinal >= self.read.projection.manifest.document_count {
                return Err(SkeinError::Storage(
                    "lexical posting ordinal exceeds its generation".to_string(),
                ));
            }
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
    bytes: ScoreBudget,
}

struct ScoreBudget {
    used: u64,
    limit: u64,
}

impl ScoreBudget {
    fn replace(&mut self, removed: u64, added: u64) -> Result<()> {
        self.used = self
            .used
            .checked_sub(removed)
            .and_then(|bytes| bytes.checked_add(added))
            .filter(|&bytes| bytes <= self.limit)
            .ok_or_else(|| {
                SkeinError::Storage(format!(
                    "lexical score byte budget exceeded: limit {}",
                    self.limit
                ))
            })?;
        Ok(())
    }
}

fn score_entry_bytes(id: &String) -> u64 {
    // Requested capacity for the owned ID, B-tree node slack and the temporary
    // tree produced while consuming a top-k heap. Not allocator/RSS telemetry.
    256u64.saturating_add(id.capacity() as u64)
}

impl ScoreCollector {
    fn new(retained_limit: Option<usize>, max_entries: usize, max_bytes: u64) -> Result<Self> {
        if retained_limit.is_some_and(|limit| limit > max_entries) {
            return Err(SkeinError::Storage(format!(
                "lexical rank window exceeds the admitted {max_entries} score entries"
            )));
        }
        let mut bytes = ScoreBudget {
            used: 0,
            limit: max_bytes,
        };
        let storage = match retained_limit {
            Some(limit) => {
                let allocation = (limit as u64)
                    .checked_mul(std::mem::size_of::<Reverse<ScoredDocument>>() as u64)
                    .ok_or_else(|| {
                        SkeinError::Storage("lexical score allocation overflow".to_string())
                    })?;
                bytes.replace(0, allocation)?;
                let mut heap = BinaryHeap::new();
                heap.try_reserve_exact(limit).map_err(|_| {
                    SkeinError::Storage("lexical score allocation failed".to_string())
                })?;
                bytes.replace(
                    allocation,
                    (heap.capacity() as u64)
                        .saturating_mul(std::mem::size_of::<Reverse<ScoredDocument>>() as u64),
                )?;
                ScoreStorage::TopK { limit, heap }
            }
            None => ScoreStorage::Full(BTreeMap::new()),
        };
        Ok(Self {
            storage,
            matching_count: 0,
            max_entries,
            bytes,
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
                self.bytes.replace(0, score_entry_bytes(&id))?;
                scores.insert(id, score);
            }
            ScoreStorage::TopK { limit, heap } => {
                if *limit == 0 {
                    return Ok(());
                }
                let candidate = ScoredDocument { id, score };
                if heap.len() < *limit {
                    self.bytes.replace(0, score_entry_bytes(&candidate.id))?;
                    heap.push(Reverse(candidate));
                } else if heap
                    .peek()
                    .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
                {
                    self.bytes.replace(
                        score_entry_bytes(&heap.peek().unwrap().0.id),
                        score_entry_bytes(&candidate.id),
                    )?;
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
    cache: Option<Arc<SegmentCache>>,
    task_context: RuntimeTaskContext,
}

impl LexicalProjectionWriter {
    pub(super) fn new(config: LexicalProjectionConfig) -> Self {
        Self {
            config,
            cache: None,
            task_context: RuntimeTaskContext::default(),
        }
    }

    pub(super) fn with_context(mut self, task_context: RuntimeTaskContext) -> Self {
        self.task_context = task_context;
        self
    }

    pub(super) fn with_cache(mut self, cache: Arc<SegmentCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn write<'a>(
        &self,
        root: &Path,
        generation: u64,
        source_graph_commit_epoch: Option<u64>,
        analyzer_digest: u64,
        documents_digest: u64,
        documents: impl Iterator<Item = &'a SearchDocument>,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Arc<LexicalProjectionReader>> {
        self.write_scanned(
            root,
            generation,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            |consumer| {
                for (ordinal, document) in documents.enumerate() {
                    let ordinal = u64::try_from(ordinal).map_err(|_| {
                        SkeinError::Storage("lexical document ordinal exceeds u64".to_string())
                    })?;
                    consumer(ordinal, document)?;
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
        scan: impl FnOnce(&mut dyn FnMut(u64, &SearchDocument) -> Result<()>) -> Result<()>,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Arc<LexicalProjectionReader>> {
        checkpoint(&self.task_context)?;
        let artifact_name = artifact_file(generation);
        let artifact_path = root.join(&artifact_name);
        let tmp_path = artifact_path.with_extension("skein.tmp");
        let mut artifact_guard = RemoveOnDrop::new(tmp_path.clone());
        let mut artifact = ArtifactBuilder::new(&tmp_path, generation, self.config)?;
        artifact.task_context = self.task_context.clone();
        let mut runs = SpillRuns::new(root, generation, self.config);
        runs.task_context = self.task_context.clone();
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut document_count = 0u64;
        let mut total_document_len = 0u64;
        let mut uncompressed_posting_payload_bytes = 0u64;
        let mut previous_document_id: Option<String> = None;
        let mut consume = |ordinal: u64, document: &SearchDocument| -> Result<()> {
            checkpoint(&self.task_context)?;
            if ordinal != document_count {
                return Err(SkeinError::Storage(format!(
                    "lexical document ordinal {ordinal} does not follow {document_count}"
                )));
            }
            let next_document_count = document_count.checked_add(1).ok_or_else(|| {
                SkeinError::Storage("lexical document ordinal overflow".to_string())
            })?;
            if previous_document_id
                .as_ref()
                .is_some_and(|previous| previous >= &document.id)
            {
                return Err(SkeinError::Storage(
                    "lexical documents must have strictly increasing IDs".to_string(),
                ));
            }
            let analyzed = analyze_delta_document(document, analyzer, self.config)?;
            checkpoint(&self.task_context)?;
            document_count = next_document_count;
            previous_document_id = Some(document.id.clone());
            total_document_len =
                total_document_len.saturating_add(u64::from(analyzed.document_len));
            artifact.push_document(document.id.clone(), analyzed.document_len)?;
            for (term, term_frequency) in analyzed.frequencies {
                checkpoint(&self.task_context)?;
                uncompressed_posting_payload_bytes = uncompressed_posting_payload_bytes
                    .checked_add(16)
                    .and_then(|bytes| bytes.checked_add(document.id.len() as u64))
                    .and_then(|bytes| bytes.checked_add(term.len() as u64))
                    .ok_or_else(|| {
                        SkeinError::Storage(
                            "uncompressed posting byte counter overflow".to_string(),
                        )
                    })?;
                let posting = Posting {
                    term,
                    ordinal,
                    term_frequency,
                };
                let bytes = posting.resident_bytes();
                if bytes > self.config.build_memory_bytes.get() {
                    return Err(SkeinError::Storage(
                        "one lexical posting exceeds the build memory budget".to_string(),
                    ));
                }
                if !chunk.is_empty()
                    && chunk_bytes.saturating_add(bytes) > self.config.build_memory_bytes.get()
                {
                    runs.spill(&mut chunk)?;
                    chunk_bytes = 0;
                }
                chunk_bytes = chunk_bytes.saturating_add(bytes);
                chunk.push(posting);
            }
            Ok(())
        };
        scan(&mut consume)?;
        checkpoint(&self.task_context)?;
        artifact.finish_documents()?;
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;
        artifact.merge_postings(&runs.paths, self.config, runs.bytes)?;
        let artifact = artifact.finish()?;
        let header_bytes = ARTIFACT_HEADER.len() as u64 + 8;
        let byte_counters = SearchLexicalArtifactBytes {
            header_bytes,
            document_mapping_bytes: artifact.posting_offset - header_bytes,
            posting_frame_bytes: artifact.posting_bytes - artifact.posting_skip_bytes,
            posting_skip_bytes: artifact.posting_skip_bytes,
            dictionary_bytes: artifact.len - artifact.posting_offset - artifact.posting_bytes,
            uncompressed_posting_payload_bytes,
        };
        let manifest = ManifestBody {
            format: "SKEIN_LEXICAL_MANIFEST_V1".to_string(),
            layout: "SKEIN_LEXICAL_COMPACT_V1".to_string(),
            generation,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            artifact_file: artifact_name,
            artifact_len: artifact.len,
            artifact_checksum: artifact.checksum,
            byte_counters,
            document_count,
            total_document_len,
            posting_count: artifact.posting_count,
            posting_offset: artifact.posting_offset,
            posting_bytes: artifact.posting_bytes,
            dictionaries: artifact.dictionaries,
            blocks: artifact.blocks,
        };
        let manifest_bytes = manifest.encode_bounded(self.config.max_directory_bytes.get())?;
        // No cancellation after entering this manifest-last publication section.
        checkpoint(&self.task_context)?;
        durable_replace_file(&tmp_path, &artifact_path)?;
        artifact_guard.disarm();
        let manifest_path = root.join(MANIFEST_FILE);
        let manifest_tmp = manifest_path.with_extension("skein.tmp");
        let mut manifest_guard = RemoveOnDrop::new(manifest_tmp.clone());
        {
            let mut file = File::create(&manifest_tmp)?;
            file.write_all(&manifest_bytes)?;
            file.sync_all()?;
        }
        durable_replace_file(&manifest_tmp, &manifest_path)?;
        manifest_guard.disarm();
        LexicalProjectionReader::load_named_with_cache(
            root,
            MANIFEST_FILE,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            self.config,
            self.cache
                .clone()
                .unwrap_or_else(|| Arc::new(SegmentCache::new(DEFAULT_CACHE_BYTES))),
        )?
        .ok_or_else(|| SkeinError::Storage("published lexical projection is missing".to_string()))
    }
}

struct ArtifactBuilder {
    writer: BufWriter<File>,
    path: PathBuf,
    generation: u64,
    config: LexicalProjectionConfig,
    offset: u64,
    next_block_id: u64,
    document_pending: Vec<(String, u32)>,
    document_pending_bytes: u64,
    document_count: u64,
    posting_count: u64,
    posting_offset: u64,
    posting_bytes: u64,
    posting_skip_bytes: u64,
    dictionaries: Vec<dictionary_store::Descriptor>,
    blocks: Vec<BlockDescriptor>,
    directory: dictionary_store::DirectoryBudget,
    task_context: RuntimeTaskContext,
}

struct ArtifactSummary {
    len: u64,
    checksum: u64,
    posting_count: u64,
    posting_offset: u64,
    posting_bytes: u64,
    posting_skip_bytes: u64,
    dictionaries: Vec<dictionary_store::Descriptor>,
    blocks: Vec<BlockDescriptor>,
}

struct RemoveOnDrop {
    path: PathBuf,
    armed: bool,
}

impl RemoveOnDrop {
    fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.path);
        }
    }
}

impl ArtifactBuilder {
    fn new(path: &Path, generation: u64, config: LexicalProjectionConfig) -> Result<Self> {
        let mut writer = BufWriter::new(File::create(path)?);
        writer.write_all(ARTIFACT_HEADER)?;
        writer.write_all(&generation.to_le_bytes())?;
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            generation,
            config,
            offset: ARTIFACT_HEADER.len() as u64 + 8,
            next_block_id: 0,
            document_pending: Vec::new(),
            document_pending_bytes: 0,
            document_count: 0,
            posting_count: 0,
            posting_offset: 0,
            posting_bytes: 0,
            posting_skip_bytes: 0,
            dictionaries: Vec::new(),
            blocks: Vec::new(),
            directory: dictionary_store::DirectoryBudget::new(config.max_directory_bytes.get()),
            task_context: RuntimeTaskContext::default(),
        })
    }

    fn push_document(&mut self, id: String, length: u32) -> Result<()> {
        checkpoint(&self.task_context)?;
        let bytes = 4u64.saturating_add(id.len() as u64).saturating_add(4);
        if !self.document_pending.is_empty()
            && self.document_pending_bytes.saturating_add(bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_documents()?;
        }
        self.document_pending_bytes = self.document_pending_bytes.saturating_add(bytes);
        self.document_pending.push((id, length));
        Ok(())
    }

    fn finish_documents(&mut self) -> Result<()> {
        self.flush_documents()
    }

    fn flush_documents(&mut self) -> Result<()> {
        checkpoint(&self.task_context)?;
        if self.document_pending.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(self.document_pending_bytes as usize + 29);
        encode_block_header(
            &mut payload,
            self.generation,
            self.next_block_id,
            BlockKind::Documents,
            self.document_pending.len(),
        )?;
        for (id, length) in &self.document_pending {
            checkpoint(&self.task_context)?;
            write_string(&mut payload, id)?;
            payload.extend_from_slice(&length.to_le_bytes());
        }
        let min_key = self.document_pending.first().unwrap().0.clone();
        let max_key = self.document_pending.last().unwrap().0.clone();
        self.write_block(BlockKind::Documents, min_key, max_key, payload)?;
        self.document_pending.clear();
        self.document_pending_bytes = 0;
        Ok(())
    }

    fn merge_postings(
        &mut self,
        paths: &[PathBuf],
        config: LexicalProjectionConfig,
        spill_bytes: u64,
    ) -> Result<()> {
        checkpoint(&self.task_context)?;
        self.posting_offset = self.offset;
        let budget = dictionary_store::SpillBudget::new(spill_bytes, config.max_spill_bytes.get());
        let mut doclist = doclist::Writer::new(
            &self.path.with_extension("skip.tmp"),
            budget.clone(),
            config.max_block_bytes.get(),
        )?
        .with_context(self.task_context.clone());
        let mut dictionary = dictionary_store::Writer::new(
            &self.path.with_extension("dictionary.tmp"),
            config,
            budget,
            self.directory.clone(),
        )?
        .with_context(self.task_context.clone());
        let mut readers = paths
            .iter()
            .map(|path| RunReader::open(path, config))
            .collect::<Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(posting) = reader.next()? {
                heap.push(Reverse((posting, index)));
            }
        }
        let mut previous: Option<Posting> = None;
        let mut term: Option<String> = None;
        let mut frame = Vec::with_capacity(posting_codec::BLOCK_LEN);
        while let Some(Reverse((posting, index))) = heap.pop() {
            checkpoint(&self.task_context)?;
            if previous.as_ref() != Some(&posting) {
                if term.as_ref().is_some_and(|term| term != &posting.term) {
                    if !frame.is_empty() {
                        doclist.push_frame(&mut self.writer, &mut self.offset, &frame)?;
                        frame.clear();
                    }
                    let metadata = doclist.finish(&mut self.writer, &mut self.offset)?;
                    self.add_skip_bytes(metadata)?;
                    dictionary.push(term.take().unwrap(), metadata)?;
                }
                if term.is_none() {
                    term = Some(posting.term.clone());
                }
                frame.push(posting_codec::Posting {
                    ordinal: posting.ordinal,
                    tf: posting.term_frequency,
                });
                if frame.len() == posting_codec::BLOCK_LEN {
                    doclist.push_frame(&mut self.writer, &mut self.offset, &frame)?;
                    frame.clear();
                }
                self.posting_count = self.posting_count.checked_add(1).ok_or_else(|| {
                    SkeinError::Storage("lexical posting count overflow".to_string())
                })?;
                previous = Some(posting);
            }
            if let Some(next) = readers[index].next()? {
                heap.push(Reverse((next, index)));
            }
        }
        if let Some(term) = term {
            if !frame.is_empty() {
                doclist.push_frame(&mut self.writer, &mut self.offset, &frame)?;
            }
            let metadata = doclist.finish(&mut self.writer, &mut self.offset)?;
            self.add_skip_bytes(metadata)?;
            dictionary.push(term, metadata)?;
        }
        self.posting_bytes = self.offset - self.posting_offset;
        self.dictionaries = dictionary.finish(&mut self.writer, &mut self.offset)?;
        Ok(())
    }

    fn add_skip_bytes(&mut self, metadata: dictionary::Metadata) -> Result<()> {
        if metadata.skip_offset != 0 {
            self.posting_skip_bytes = self
                .posting_skip_bytes
                .checked_add(metadata.posting_bytes - metadata.skip_offset)
                .ok_or_else(|| {
                    SkeinError::Storage("posting skip byte counter overflow".to_string())
                })?;
        }
        Ok(())
    }

    fn write_block(
        &mut self,
        kind: BlockKind,
        min_key: String,
        max_key: String,
        payload: Vec<u8>,
    ) -> Result<()> {
        if payload.len() as u64 > self.config.max_block_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical build produced a {} byte block, exceeding {}",
                payload.len(),
                self.config.max_block_bytes
            )));
        }
        let entry_count = u32::from_le_bytes(payload[25..29].try_into().unwrap());
        let descriptor = BlockDescriptor {
            block_id: self.next_block_id,
            kind,
            min_key,
            max_key,
            offset: self.offset,
            length: payload.len() as u64,
            checksum: checksum(&payload),
            entry_count,
            ordinal_start: if kind == BlockKind::Documents {
                self.document_count
            } else {
                0
            },
        };
        self.directory.admit(
            &mut self.blocks,
            descriptor
                .min_key
                .capacity()
                .saturating_add(descriptor.max_key.capacity()),
        )?;
        self.writer.write_all(&payload)?;
        self.offset = self.offset.saturating_add(payload.len() as u64);
        self.next_block_id = self.next_block_id.saturating_add(1);
        self.blocks.push(descriptor);
        if kind == BlockKind::Documents {
            self.document_count = self
                .document_count
                .checked_add(u64::from(entry_count))
                .ok_or_else(|| {
                    SkeinError::Storage("lexical document ordinal range overflows".to_string())
                })?;
        }
        Ok(())
    }

    fn finish(mut self) -> Result<ArtifactSummary> {
        checkpoint(&self.task_context)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let (length, digest) =
            file_digest_with_context(&File::open(&self.path)?, &self.task_context)?;
        Ok(ArtifactSummary {
            len: length,
            checksum: digest,
            posting_count: self.posting_count,
            posting_offset: self.posting_offset,
            posting_bytes: self.posting_bytes,
            posting_skip_bytes: self.posting_skip_bytes,
            dictionaries: self.dictionaries,
            blocks: self.blocks,
        })
    }
}

struct SpillRuns {
    root: PathBuf,
    generation: u64,
    config: LexicalProjectionConfig,
    paths: Vec<PathBuf>,
    bytes: u64,
    sequence: usize,
    task_context: RuntimeTaskContext,
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
            task_context: RuntimeTaskContext::default(),
        }
    }

    fn spill(&mut self, postings: &mut Vec<Posting>) -> Result<()> {
        checkpoint(&self.task_context)?;
        postings.sort_unstable();
        postings.dedup();
        checkpoint(&self.task_context)?;
        let path = self.next_path()?;
        let mut guard = RemoveOnDrop::new(path.clone());
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        let mut bytes = RUN_HEADER.len() as u64;
        for posting in postings.iter() {
            checkpoint(&self.task_context)?;
            encode_posting(&mut writer, posting)?;
            bytes = bytes.saturating_add(posting.encoded_len());
        }
        writer.flush()?;
        drop(writer);
        checkpoint(&self.task_context)?;
        self.admit_spill(bytes)?;
        self.paths.push(path);
        guard.disarm();
        postings.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        checkpoint(&self.task_context)?;
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(SkeinError::Storage(
                "lexical merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
            checkpoint(&self.task_context)?;
            let old = std::mem::take(&mut self.paths);
            let mut merged = Vec::new();
            for group in old.chunks(fan_in) {
                let path = match self.next_path() {
                    Ok(path) => path,
                    Err(error) => {
                        remove_paths(old.iter().chain(merged.iter()));
                        return Err(error);
                    }
                };
                let bytes = match merge_runs(group, &path, self.config, &self.task_context) {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = fs::remove_file(&path);
                        remove_paths(old.iter().chain(merged.iter()));
                        return Err(error);
                    }
                };
                if let Err(error) = self.admit_spill(bytes) {
                    let _ = fs::remove_file(&path);
                    remove_paths(old.iter().chain(merged.iter()));
                    return Err(error);
                }
                merged.push(path);
                for source in group {
                    fs::remove_file(source)?;
                }
            }
            self.paths = merged;
        }
        Ok(())
    }

    fn admit_spill(&mut self, bytes: u64) -> Result<()> {
        let required = self.bytes.saturating_add(bytes);
        if required > self.config.max_spill_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical build requires {required} spill bytes, exceeding {}",
                self.config.max_spill_bytes
            )));
        }
        self.bytes = required;
        Ok(())
    }

    fn next_path(&mut self) -> Result<PathBuf> {
        let required = self.sequence.saturating_add(1);
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

fn remove_paths<'a>(paths: impl Iterator<Item = &'a PathBuf>) {
    for path in paths {
        let _ = fs::remove_file(path);
    }
}

impl Drop for SpillRuns {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = fs::remove_file(path);
        }
    }
}

struct RunReader {
    reader: BufReader<File>,
    config: LexicalProjectionConfig,
}

impl RunReader {
    fn open(path: &Path, config: LexicalProjectionConfig) -> Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = [0u8; 8];
        reader.read_exact(&mut header)?;
        if &header != RUN_HEADER {
            return Err(SkeinError::Storage(
                "lexical spill run header mismatch".to_string(),
            ));
        }
        Ok(Self { reader, config })
    }

    fn next(&mut self) -> Result<Option<Posting>> {
        let Some(term) = read_optional_string(&mut self.reader, self.config.max_term_bytes.get())?
        else {
            return Ok(None);
        };
        let mut ordinal = [0u8; 8];
        self.reader.read_exact(&mut ordinal)?;
        let ordinal = u64::from_le_bytes(ordinal);
        let term_frequency = read_u32(&mut self.reader)?;
        if term.is_empty() || term_frequency == 0 {
            return Err(SkeinError::Storage(
                "invalid lexical spill posting".to_string(),
            ));
        }
        Ok(Some(Posting {
            term,
            ordinal,
            term_frequency,
        }))
    }
}

fn merge_runs(
    paths: &[PathBuf],
    destination: &Path,
    config: LexicalProjectionConfig,
    task_context: &RuntimeTaskContext,
) -> Result<u64> {
    checkpoint(task_context)?;
    let mut readers = paths
        .iter()
        .map(|path| RunReader::open(path, config))
        .collect::<Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(posting) = reader.next()? {
            heap.push(Reverse((posting, index)));
        }
    }
    let mut writer = BufWriter::new(File::create(destination)?);
    writer.write_all(RUN_HEADER)?;
    let mut bytes = RUN_HEADER.len() as u64;
    let mut previous = None;
    while let Some(Reverse((posting, index))) = heap.pop() {
        checkpoint(task_context)?;
        if previous.as_ref() != Some(&posting) {
            encode_posting(&mut writer, &posting)?;
            bytes = bytes.saturating_add(posting.encoded_len());
            previous = Some(posting);
        }
        if let Some(next) = readers[index].next()? {
            heap.push(Reverse((next, index)));
        }
    }
    writer.flush()?;
    Ok(bytes)
}

fn encode_block_header(
    output: &mut Vec<u8>,
    generation: u64,
    block_id: u64,
    kind: BlockKind,
    count: usize,
) -> Result<()> {
    output.extend_from_slice(BLOCK_HEADER);
    output.extend_from_slice(&generation.to_le_bytes());
    output.extend_from_slice(&block_id.to_le_bytes());
    output.push(match kind {
        BlockKind::Documents => 1,
        BlockKind::Postings => 2,
    });
    output.extend_from_slice(
        &u32::try_from(count)
            .map_err(|_| SkeinError::Storage("lexical block count exceeds u32".to_string()))?
            .to_le_bytes(),
    );
    Ok(())
}

fn encode_posting(mut writer: impl Write, posting: &Posting) -> Result<()> {
    write_string(&mut writer, &posting.term)?;
    writer.write_all(&posting.ordinal.to_le_bytes())?;
    writer.write_all(&posting.term_frequency.to_le_bytes())?;
    Ok(())
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

    fn str(&mut self, max: u64) -> Result<&'a str> {
        let length = self.u32()? as usize;
        if length as u64 > max {
            return Err(SkeinError::Storage(format!(
                "lexical string uses {length} bytes, exceeding {max}"
            )));
        }
        std::str::from_utf8(self.bytes(length)?)
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

fn read_optional_string(reader: &mut impl Read, max: u64) -> Result<Option<String>> {
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
    let mut bytes = vec![0u8; length];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| SkeinError::Storage(error.to_string()))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn file_digest(file: &File) -> Result<(u64, u64)> {
    file_digest_with_context(file, &RuntimeTaskContext::default())
}

fn file_digest_with_context(file: &File, task_context: &RuntimeTaskContext) -> Result<(u64, u64)> {
    checkpoint(task_context)?;
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Digest::new();
    let mut total = 0u64;
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        checkpoint(task_context)?;
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

    mod admission;
    mod artifact_accounting;
    mod build_cancellation;
    mod compact_dictionary;
    mod compact_postings;
    mod fuzz;
    mod robustness;

    pub(super) fn projection_root(name: &str) -> PathBuf {
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
            .term_metadata(term, &mut 0)
            .unwrap()
            .map_or(0, |metadata| metadata.posting_bytes)
    }

    fn term_read_bytes(reader: &LexicalProjectionReader, term: &str) -> u64 {
        let bytes: u64 = reader
            .manifest
            .dictionaries
            .iter()
            .filter(|block| block.min_term.as_str() <= term && term <= block.max_term.as_str())
            .map(|block| block.length)
            .sum();
        let metadata = reader.term_metadata(term, &mut 0).unwrap();
        bytes + metadata.map_or(0, |metadata| metadata.posting_bytes)
    }

    fn document_mapping_bytes(reader: &LexicalProjectionReader) -> u64 {
        reader
            .manifest
            .blocks
            .iter()
            .filter(|block| block.kind == BlockKind::Documents)
            .map(|block| block.length)
            .sum()
    }

    #[test]
    fn scan_rejects_non_dense_document_ordinals_without_publishing_artifacts() {
        for ordinals in [vec![1], vec![0, 2], vec![0, 0]] {
            let root = projection_root(&format!("ordinal-sequence-{ordinals:?}"));
            fs::create_dir_all(&root).unwrap();
            let analyzer = SearchAnalyzerLexicon::default();
            let result = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
                .write_scanned(
                    &root,
                    1,
                    None,
                    11,
                    13,
                    |consume| {
                        for (number, ordinal) in ordinals.iter().enumerate() {
                            consume(
                                *ordinal,
                                &document(&format!("doc-{number}"), "Graph", "storage"),
                            )?;
                        }
                        Ok(())
                    },
                    &analyzer,
                );
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("lexical document ordinal"));
            assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
            fs::remove_dir_all(root).unwrap();
        }
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
        assert_eq!(
            reader.term_metadata("graph", &mut 0).unwrap().unwrap().df,
            2
        );
        assert_eq!(report.document_bytes_read, document_mapping_bytes(&reader));
        assert_eq!(
            report.bytes_read,
            term_read_bytes(&reader, "graph") + report.document_bytes_read
        );
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
        invalid_manifest.dictionaries[0].posting_count += 1;
        assert!(invalid_manifest.validate().is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn filtered_scores_use_manifest_corpus_and_bounded_document_mapping() {
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
        assert_eq!(report.document_bytes_read, document_mapping_bytes(&reader));
        assert_eq!(
            report.bytes_read,
            term_read_bytes(&reader, "graph") + report.document_bytes_read
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delta_scores_only_hydrate_base_document_mapping() {
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
        let mut delta = LexicalMiniDelta::default();
        delta
            .upsert(&document("c", "Graph", "query"), None, &analyzer, config)
            .unwrap();
        let terms = BTreeSet::from(["graph".to_string()]);

        let report = reader.score(&terms, &delta, None, |_| Ok(true)).unwrap();

        assert_eq!(report.scores.keys().collect::<Vec<_>>(), vec!["a", "c"]);
        assert_eq!(report.document_bytes_read, document_mapping_bytes(&reader));
        assert_eq!(
            report.bytes_read,
            term_read_bytes(&reader, "graph") + report.document_bytes_read
        );
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
        let mut delta = LexicalMiniDelta::default();
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
        assert_eq!(report.document_bytes_read, document_mapping_bytes(&reader));
        assert_eq!(
            report.bytes_read,
            term_read_bytes(&reader, "graph") + report.document_bytes_read
        );
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
        let mut delta = LexicalMiniDelta::default();
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
