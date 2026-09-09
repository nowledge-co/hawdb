use super::cjk_tokenizer::ANALYZER_FORMAT_VERSION;
use super::{
    document_token_fields, visit_token_list, SearchAnalyzerLexicon, SearchDocument,
    TokenOccurrence, BM25_B, BM25_K1,
};
use crate::error::{Result, SkeinError};
use serde::{Deserialize, Serialize};
use skein_integrity::Crc32cHasher as Digest;
use skein_storage::durable_replace_file;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(test)]
mod analysis_tests;

const ARTIFACT_HEADER: &[u8; 16] = b"SKEINLEXICAL0001";
const BLOCK_HEADER: &[u8; 8] = b"SKNLEX01";
const RUN_HEADER: &[u8; 8] = b"SKNLEXR1";
const MAX_MANIFEST_BYTES: u64 = 256 * 1024 * 1024;
pub(super) const MANIFEST_FILE: &str = "search_lexical.manifest.skein";

pub(super) fn artifact_file(generation: u64) -> String {
    format!("search_lexical.{generation}.skein")
}

pub(super) fn manifest_generation(path: &Path) -> Result<u64> {
    let length = fs::metadata(path)?.len();
    if length > MAX_MANIFEST_BYTES {
        return Err(SkeinError::Storage(format!(
            "lexical projection manifest requires {length} bytes, exceeding {MAX_MANIFEST_BYTES}"
        )));
    }
    Ok(ManifestBody::decode(&fs::read(path)?)?.generation)
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
    fn validate(&self) -> Result<()> {
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

    fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let body =
            serde_json::to_vec(self).map_err(|error| SkeinError::Storage(error.to_string()))?;
        serde_json::to_vec(&ManifestEnvelope {
            body: self.clone(),
            checksum: checksum(&body),
        })
        .map_err(|error| SkeinError::Storage(error.to_string()))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let envelope: ManifestEnvelope = serde_json::from_slice(bytes)
            .map_err(|error| SkeinError::Storage(format!("invalid lexical manifest: {error}")))?;
        let body = serde_json::to_vec(&envelope.body)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        if checksum(&body) != envelope.checksum {
            return Err(SkeinError::Storage(
                "lexical projection manifest checksum mismatch".to_string(),
            ));
        }
        envelope.body.validate()?;
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
    term: String,
    document_id: String,
    term_frequency: u32,
    document_len: u32,
}

impl Posting {
    fn resident_bytes(&self) -> u64 {
        32u64
            .saturating_add(self.term.len() as u64)
            .saturating_add(self.document_id.len() as u64)
    }

    fn encoded_len(&self) -> u64 {
        4u64.saturating_add(self.term.len() as u64)
            .saturating_add(4)
            .saturating_add(self.document_id.len() as u64)
            .saturating_add(8)
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
    let mut accumulator = DocumentAnalysis::new(&document.id, config)?;
    for (field, (text, weight)) in document_token_fields(document).enumerate() {
        let field = u8::try_from(field).expect("document analysis has at most six fields");
        visit_token_list(text, analyzer, |term, occurrence| {
            accumulator.push(term, occurrence, field, weight)
        })?;
    }
    Ok(accumulator.finish())
}

struct AnalyzedTerm {
    frequency: u32,
    last_field: u8,
}

struct DocumentAnalysis<'a> {
    document_id: &'a str,
    config: LexicalProjectionConfig,
    document_len: u32,
    frequencies: BTreeMap<String, AnalyzedTerm>,
    resident_bytes: u64,
}

impl<'a> DocumentAnalysis<'a> {
    fn new(document_id: &'a str, config: LexicalProjectionConfig) -> Result<Self> {
        let analysis = Self {
            document_id,
            config,
            document_len: 0,
            frequencies: BTreeMap::new(),
            resident_bytes: document_id.len() as u64 + 64,
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
        if term.len() as u64 > self.config.max_term_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical term uses {} bytes, exceeding {}",
                term.len(),
                self.config.max_term_bytes
            )));
        }
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
        let marker_bytes =
            (std::mem::size_of::<AnalyzedTerm>() - std::mem::size_of::<u32>()) as u64;
        let required_bytes =
            resident_bytes.saturating_add((terms as u64).saturating_mul(marker_bytes));
        if required_bytes > self.config.build_memory_bytes.get() {
            return Err(SkeinError::Storage(format!(
                "lexical document {} requires more than {} analyzer bytes",
                self.document_id, self.config.build_memory_bytes,
            )));
        }
        Ok(())
    }

    fn finish(self) -> DeltaDocument {
        DeltaDocument {
            document_len: self.document_len,
            frequencies: self
                .frequencies
                .into_iter()
                .map(|(term, entry)| (term, entry.frequency))
                .collect(),
            resident_bytes: self.resident_bytes,
            base: None,
        }
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

    pub(super) fn load_named(
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
        if fs::metadata(&manifest_path)?.len() > MAX_MANIFEST_BYTES {
            return Err(SkeinError::Storage(
                "lexical projection manifest exceeds its read budget".to_string(),
            ));
        }
        let manifest = ManifestBody::decode(&fs::read(&manifest_path)?)?;
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
        })))
    }

    pub(super) fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub(super) fn score(
        &self,
        query_terms: &BTreeSet<String>,
        delta: &LexicalMiniDelta,
        retained_score_limit: Option<usize>,
        mut allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<LexicalQueryReport> {
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
        let admitted_stream_bytes = (query_terms.len() as u64)
            .saturating_mul(self.config.max_block_bytes.get())
            .saturating_mul(2);
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
                streams.push(TermPostingStream::new(self, term));
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
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(block.offset))?;
        let mut bytes = vec![0u8; block.length as usize];
        file.read_exact(&mut bytes)?;
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
}

impl<'a> TermPostingStream<'a> {
    fn new(projection: &'a LexicalProjectionReader, term: &'a str) -> Self {
        let blocks = projection
            .manifest
            .blocks
            .iter()
            .filter(|block| {
                block.kind == BlockKind::Postings
                    && block.min_key.as_str() <= term
                    && term <= block.max_key.as_str()
            })
            .collect();
        Self {
            projection,
            term,
            blocks,
            block_index: 0,
            current: Vec::new().into_iter(),
            postings_visited: 0,
            bytes_read: 0,
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
                self.projection.config.max_term_bytes.get(),
                |posting| {
                    self.postings_visited = self.postings_visited.saturating_add(1);
                    if posting.term == self.term {
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
}

impl LexicalProjectionWriter {
    pub(super) const fn new(config: LexicalProjectionConfig) -> Self {
        Self { config }
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
        let artifact_name = artifact_file(generation);
        let artifact_path = root.join(&artifact_name);
        let tmp_path = artifact_path.with_extension("skein.tmp");
        let mut artifact_guard = RemoveOnDrop::new(tmp_path.clone());
        let mut artifact = ArtifactBuilder::new(&tmp_path, generation, self.config)?;
        let mut runs = SpillRuns::new(root, generation, self.config);
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0u64;
        let mut document_count = 0u64;
        let mut total_document_len = 0u64;
        let mut consume = |document: &SearchDocument| -> Result<()> {
            let analyzed = analyze_delta_document(document, analyzer, self.config)?;
            document_count = document_count.saturating_add(1);
            total_document_len =
                total_document_len.saturating_add(u64::from(analyzed.document_len));
            artifact.push_document(document.id.clone(), analyzed.document_len)?;
            for (term, term_frequency) in analyzed.frequencies {
                let posting = Posting {
                    term,
                    document_id: document.id.clone(),
                    term_frequency,
                    document_len: analyzed.document_len,
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
        artifact.finish_documents()?;
        if !chunk.is_empty() {
            runs.spill(&mut chunk)?;
        }
        runs.compact()?;
        artifact.merge_postings(&runs.paths, self.config)?;
        let artifact = artifact.finish()?;
        let manifest = ManifestBody {
            format: "SKEIN_LEXICAL_MANIFEST_V1".to_string(),
            generation,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            artifact_file: artifact_name,
            artifact_len: artifact.len,
            artifact_checksum: artifact.checksum,
            document_count,
            total_document_len,
            posting_count: artifact.posting_count,
            term_statistics: artifact.term_statistics,
            blocks: artifact.blocks,
        };
        let manifest_bytes = manifest.encode()?;
        if manifest_bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(SkeinError::Storage(format!(
                "lexical projection manifest requires {} bytes, exceeding {MAX_MANIFEST_BYTES}",
                manifest_bytes.len()
            )));
        }
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
        LexicalProjectionReader::load(
            root,
            source_graph_commit_epoch,
            analyzer_digest,
            documents_digest,
            self.config,
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
    posting_pending: Vec<Posting>,
    posting_pending_bytes: u64,
    posting_count: u64,
    term_statistics: Vec<TermStatistics>,
    blocks: Vec<BlockDescriptor>,
}

struct ArtifactSummary {
    len: u64,
    checksum: u64,
    posting_count: u64,
    term_statistics: Vec<TermStatistics>,
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
            posting_pending: Vec::new(),
            posting_pending_bytes: 0,
            posting_count: 0,
            term_statistics: Vec::new(),
            blocks: Vec::new(),
        })
    }

    fn push_document(&mut self, id: String, length: u32) -> Result<()> {
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

    fn merge_postings(&mut self, paths: &[PathBuf], config: LexicalProjectionConfig) -> Result<()> {
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
        let mut previous = None;
        while let Some(Reverse((posting, index))) = heap.pop() {
            if previous.as_ref() != Some(&posting) {
                self.push_posting(posting.clone())?;
                previous = Some(posting);
            }
            if let Some(next) = readers[index].next()? {
                heap.push(Reverse((next, index)));
            }
        }
        self.flush_postings()
    }

    fn push_posting(&mut self, posting: Posting) -> Result<()> {
        match self.term_statistics.last_mut() {
            Some(statistics) if statistics.term == posting.term => {
                statistics.document_frequency = statistics
                    .document_frequency
                    .checked_add(1)
                    .ok_or_else(|| {
                        SkeinError::Storage(
                            "lexical term document frequency exceeds u64".to_string(),
                        )
                    })?;
            }
            Some(statistics) if statistics.term > posting.term => {
                return Err(SkeinError::Storage(
                    "lexical merge produced unordered term statistics".to_string(),
                ));
            }
            _ => self.term_statistics.push(TermStatistics {
                term: posting.term.clone(),
                document_frequency: 1,
            }),
        }
        let bytes = posting.encoded_len();
        if !self.posting_pending.is_empty()
            && self.posting_pending_bytes.saturating_add(bytes)
                > self.config.target_block_bytes.get()
        {
            self.flush_postings()?;
        }
        self.posting_pending_bytes = self.posting_pending_bytes.saturating_add(bytes);
        self.posting_pending.push(posting);
        Ok(())
    }

    fn flush_postings(&mut self) -> Result<()> {
        if self.posting_pending.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(self.posting_pending_bytes as usize + 29);
        encode_block_header(
            &mut payload,
            self.generation,
            self.next_block_id,
            BlockKind::Postings,
            self.posting_pending.len(),
        )?;
        for posting in &self.posting_pending {
            encode_posting(&mut payload, posting)?;
        }
        let min_key = self.posting_pending.first().unwrap().term.clone();
        let max_key = self.posting_pending.last().unwrap().term.clone();
        self.posting_count = self
            .posting_count
            .saturating_add(self.posting_pending.len() as u64);
        self.write_block(BlockKind::Postings, min_key, max_key, payload)?;
        self.posting_pending.clear();
        self.posting_pending_bytes = 0;
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
        };
        self.writer.write_all(&payload)?;
        self.offset = self.offset.saturating_add(payload.len() as u64);
        self.next_block_id = self.next_block_id.saturating_add(1);
        self.blocks.push(descriptor);
        Ok(())
    }

    fn finish(mut self) -> Result<ArtifactSummary> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        let (length, digest) = file_digest(&File::open(&self.path)?)?;
        Ok(ArtifactSummary {
            len: length,
            checksum: digest,
            posting_count: self.posting_count,
            term_statistics: self.term_statistics,
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
        }
    }

    fn spill(&mut self, postings: &mut Vec<Posting>) -> Result<()> {
        postings.sort_unstable();
        postings.dedup();
        let path = self.next_path()?;
        let mut writer = BufWriter::new(File::create(&path)?);
        writer.write_all(RUN_HEADER)?;
        let mut bytes = RUN_HEADER.len() as u64;
        for posting in postings.iter() {
            encode_posting(&mut writer, posting)?;
            bytes = bytes.saturating_add(posting.encoded_len());
        }
        writer.flush()?;
        if let Err(error) = self.admit_spill(bytes) {
            let _ = fs::remove_file(&path);
            return Err(error);
        }
        self.paths.push(path);
        postings.clear();
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        let fan_in = self.config.max_merge_fan_in.get();
        if fan_in < 2 {
            return Err(SkeinError::Storage(
                "lexical merge fan-in must be at least two".to_string(),
            ));
        }
        while self.paths.len() > fan_in {
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
                let bytes = match merge_runs(group, &path, self.config) {
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
        let document_id = read_string(&mut self.reader, 1024 * 1024)?;
        let term_frequency = read_u32(&mut self.reader)?;
        let document_len = read_u32(&mut self.reader)?;
        Ok(Some(Posting {
            term,
            document_id,
            term_frequency,
            document_len,
        }))
    }
}

fn merge_runs(
    paths: &[PathBuf],
    destination: &Path,
    config: LexicalProjectionConfig,
) -> Result<u64> {
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
            term: cursor.string(max_term_bytes)?,
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
            first = Some(posting.term.clone());
        }
        consumer(posting.clone())?;
        previous = Some(posting);
    }
    let previous_term = previous.map(|posting| posting.term);
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
        let mut delta = LexicalMiniDelta::default();
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
