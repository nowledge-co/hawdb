// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Candidate filtering owns native spill files and query-funded caches.

use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul, read::io_error, reserve_capacity,
};
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

mod spill_file;
use spill_file::SpillFile;

#[cfg(test)]
mod tests;

impl SearchOutOfCoreReader {
    pub(super) fn build_candidate_set<'a>(
        &self,
        predicates: &hawdb_optimizer_predicate::SearchPredicateSet,
        report: &mut SearchPredicatePushdownReport,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'a>>,
        scratch: Option<&'a ScratchDirectory>,
        output: super::query_output::Memory<'_>,
    ) -> Result<CandidateSet<'a>> {
        report.segment_count = self
            .segments
            .iter()
            .map(|segment| segment.descriptor.segments.len())
            .sum();
        report.segment_pruning_candidate_document_count = self.manifest.document_count;
        report.persisted_segment_descriptor_used = true;
        let mut field_pruning = super::query_output::report::Fields::new(predicates, context)?;

        if predicates.is_empty() && self.visibility.is_empty() {
            report.field_summaries = field_pruning.into_reports(output)?;
            return Ok(CandidateSet::All(self.manifest.document_count));
        }

        let mut block_memory = context
            .map(|context| context.memory.reserve(0))
            .transpose()?;
        let block_count = self.segments.iter().try_fold(0usize, |count, artifact| {
            add(count, artifact.descriptor.segments.len())
        })?;
        let mut blocks = Vec::new();
        if let Some(memory) = block_memory.as_mut() {
            reserve_capacity(&mut blocks, block_count, memory)?;
            let names = self
                .segments
                .iter()
                .flat_map(|artifact| &artifact.descriptor.segments)
                .try_fold(0usize, |total, segment| {
                    add(
                        total,
                        add(
                            segment.first_document_id.len(),
                            segment.last_document_id.len(),
                        )?,
                    )
                })?;
            memory.grow(names)?;
        } else {
            blocks = Vec::with_capacity(block_count);
        }
        let mut file = SpillFile::create(&self.config, context, scratch)?;
        file.write_all(CANDIDATE_FILE_HEADER)?;
        let mut offset = CANDIDATE_FILE_HEADER.len() as u64;
        let mut cardinality = 0usize;

        for (layer, artifact) in self.segments.iter().enumerate() {
            for segment in &artifact.descriptor.segments {
                if let Some(context) = context {
                    context.checkpoint()?;
                }
                let visible_count = self
                    .visibility
                    .visible_count(artifact.content_segment_id, segment)?;
                field_pruning.observe(segment, predicates);
                if !segment.may_match_predicates(predicates) {
                    report.pruned_segment_count = report.pruned_segment_count.saturating_add(1);
                    report.segment_pruned_document_count = report
                        .segment_pruned_document_count
                        .saturating_add(visible_count);
                    blocks.push(CandidateBlock::empty(layer, segment)?);
                    continue;
                }
                report.scanned_segment_count = report.scanned_segment_count.saturating_add(1);
                report.segment_scanned_document_count = report
                    .segment_scanned_document_count
                    .saturating_add(visible_count);
                let mut encoded_memory = context
                    .map(|context| context.memory.reserve(0))
                    .transpose()?;
                let mut encoded = Vec::new();
                let mut block_cardinality = 0usize;
                let mut append = |document: &SearchMetadataDocument| {
                    if !self
                        .visibility
                        .is_visible(artifact.content_segment_id, &document.id)
                    {
                        return Ok(());
                    }
                    if !crate::search_metadata_matches_predicates(
                        &document.id,
                        &document.metadata,
                        predicates,
                    ) {
                        return Ok(());
                    }
                    let id_len = u32::try_from(document.id.len()).map_err(|_| {
                        HawDBError::Storage(format!(
                            "search candidate id {} exceeds the supported length",
                            document.id
                        ))
                    })?;
                    if let Some(memory) = &mut encoded_memory {
                        let required = crate::build_memory::checked_add(
                            encoded.len(),
                            crate::build_memory::checked_add(document.id.len(), 12)?,
                        )?;
                        let limit = usize::try_from(self.config.max_candidate_block_bytes.get())
                            .unwrap_or(usize::MAX);
                        let capacity =
                            required.max(encoded.capacity().saturating_mul(2).min(limit));
                        crate::build_memory::reserve_capacity(&mut encoded, capacity, memory)?;
                    }
                    encoded.extend_from_slice(&id_len.to_le_bytes());
                    encoded.extend_from_slice(document.id.as_bytes());
                    encoded.extend_from_slice(
                        &document.vector_ordinal.unwrap_or(u64::MAX).to_le_bytes(),
                    );
                    block_cardinality = block_cardinality.saturating_add(1);
                    Ok(())
                };
                if let Some(context) = context {
                    self.visit_query_metadata_segment(artifact, segment, metrics, context, append)?;
                } else {
                    for document in self.read_metadata_segment(artifact, segment, metrics)? {
                        append(&document)?;
                    }
                }
                if encoded.len() as u64 > self.config.max_candidate_block_bytes.get() {
                    return Err(ResourceBudgetError {
                        resource: ResourceBudgetKind::MemoryBytes,
                        scope: ResourceBudgetScope::Operator,
                        owner: "search candidate block".to_string(),
                        memory_class: None,
                        requested: encoded.len() as u64,
                        used: 0,
                        budget: self.config.max_candidate_block_bytes.get(),
                        budget_option: "SearchOutOfCoreConfig.max_candidate_block_bytes",
                        message: format!(
                            "search candidate block for layer {layer} segment {} requires {} bytes, exceeding {}",
                            segment.segment_id,
                            encoded.len(),
                            self.config.max_candidate_block_bytes
                        ),
                    }
                    .into());
                }
                let next_spill_bytes = offset
                    .saturating_add(encoded.len() as u64)
                    .saturating_sub(CANDIDATE_FILE_HEADER.len() as u64);
                if next_spill_bytes > self.config.max_candidate_spill_bytes.get() {
                    return Err(ResourceBudgetError {
                        resource: ResourceBudgetKind::SpillBytes,
                        scope: ResourceBudgetScope::Operator,
                        owner: "search candidate spill".to_string(),
                        memory_class: None,
                        requested: encoded.len() as u64,
                        used: offset.saturating_sub(CANDIDATE_FILE_HEADER.len() as u64),
                        budget: self.config.max_candidate_spill_bytes.get(),
                        budget_option: "SearchOutOfCoreConfig.max_candidate_spill_bytes",
                        message: format!(
                            "search candidate spill requires {next_spill_bytes} bytes, exceeding {}",
                            self.config.max_candidate_spill_bytes
                        ),
                    }
                    .into());
                }
                file.write_all(&encoded)?;
                let mut block = CandidateBlock::empty(layer, segment)?;
                block.offset = offset;
                block.length = encoded.len() as u64;
                block.cardinality = block_cardinality;
                blocks.push(block);
                offset = offset.saturating_add(encoded.len() as u64);
                cardinality = cardinality.saturating_add(block_cardinality);
            }
        }
        file.sync_all()?;
        metrics.candidate_spill_bytes = offset.saturating_sub(CANDIDATE_FILE_HEADER.len() as u64);
        report.physical_range_read_count =
            usize::try_from(metrics.segment_range_reads).unwrap_or(usize::MAX);
        report.physical_bytes_read = metrics.segment_bytes_read;
        report.field_summaries = field_pruning.into_reports(output)?;
        let box_memory = context
            .map(|context| context.memory.reserve(size_of::<SpilledCandidateSet<'_>>()))
            .transpose()?;
        Ok(CandidateSet::Spilled(SpilledCandidateOwner {
            data: Box::new(SpilledCandidateSet {
                file,
                blocks,
                cardinality,
                max_block_bytes: self.config.max_candidate_block_bytes.get(),
                cache: Mutex::new(None),
                _memory: block_memory,
            }),
            _memory: box_memory,
        }))
    }
}

pub(super) enum CandidateSet<'a> {
    All(usize),
    Spilled(SpilledCandidateOwner<'a>),
}

pub(super) struct SpilledCandidateOwner<'a> {
    // Deallocate the Box before refunding its allocation, independently of
    // the data's payload leases which die after their vectors and strings.
    data: Box<SpilledCandidateSet<'a>>,
    _memory: Option<QueryMemoryLease>,
}
impl<'a> std::ops::Deref for SpilledCandidateOwner<'a> {
    type Target = SpilledCandidateSet<'a>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl CandidateSet<'_> {
    #[cfg(feature = "vector-search")]
    pub(super) fn vector_allowlist_working_bytes(&self, layer: usize) -> Result<u64> {
        match self {
            Self::All(_) => Ok(0),
            Self::Spilled(set) => set.vector_allowlist_layout(layer).map(|(_, bytes)| bytes),
        }
    }

    pub(super) fn cardinality(&self) -> usize {
        match self {
            Self::All(cardinality) => *cardinality,
            Self::Spilled(set) => set.cardinality,
        }
    }

    pub(super) fn representation(&self) -> &'static str {
        match self {
            Self::All(_) => "all_documents",
            Self::Spilled(_) => "spilled_sorted_document_ids",
        }
    }

    pub(super) fn contains(&self, id: &str, metrics: &mut SearchOutOfCoreMetrics) -> Result<bool> {
        match self {
            Self::All(_) => Ok(true),
            Self::Spilled(set) => set.contains(id, metrics),
        }
    }

    pub(super) fn segment_cardinality(&self, layer: usize, segment_id: u64) -> usize {
        match self {
            Self::All(_) => usize::MAX,
            Self::Spilled(set) => set
                .blocks
                .iter()
                .find(|block| block.layer == layer && block.segment_id == segment_id)
                .map(|block| block.cardinality)
                .unwrap_or(0),
        }
    }

    #[cfg(feature = "vector-search")]
    pub(super) fn vector_ordinals_for_layer(
        &self,
        layer: usize,
        max_bytes: u64,
        task_context: Option<&crate::RuntimeTaskContext>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Option<Ordinals>> {
        match self {
            Self::All(_) => Ok(None),
            Self::Spilled(set) => set
                .vector_ordinals_for_layer(layer, max_bytes, task_context, metrics)
                .map(Some),
        }
    }
}

#[derive(Debug)]
struct CandidateBlock {
    layer: usize,
    segment_id: u64,
    first_document_id: String,
    last_document_id: String,
    offset: u64,
    length: u64,
    cardinality: usize,
}

impl CandidateBlock {
    fn empty(layer: usize, segment: &SearchSegmentDescriptorEntry) -> Result<Self> {
        let first_document_id = segment.first_document_id.clone();
        let last_document_id = segment.last_document_id.clone();
        if first_document_id.capacity() > segment.first_document_id.len()
            || last_document_id.capacity() > segment.last_document_id.len()
        {
            return Err(HawDBError::Execution(
                "candidate block bounds exceeded admission".into(),
            ));
        }
        Ok(Self {
            layer,
            segment_id: segment.segment_id,
            first_document_id,
            last_document_id,
            offset: 0,
            length: 0,
            cardinality: 0,
        })
    }

    fn contains_range(&self, id: &str) -> bool {
        self.first_document_id.as_str() <= id && id <= self.last_document_id.as_str()
    }
}

#[derive(Debug)]
struct CandidateCache {
    layer: usize,
    segment_id: u64,
    entries: Vec<CandidateEntry>,
    _memory: Option<QueryMemoryLease>,
}

#[derive(Debug)]
struct CandidateEntry {
    id: String,
}

pub(super) struct SpilledCandidateSet<'a> {
    file: SpillFile<'a>,
    blocks: Vec<CandidateBlock>,
    cardinality: usize,
    max_block_bytes: u64,
    cache: Mutex<Option<CandidateCache>>,
    _memory: Option<QueryMemoryLease>,
}

impl SpilledCandidateSet<'_> {
    fn contains(&self, id: &str, metrics: &mut SearchOutOfCoreMetrics) -> Result<bool> {
        if let Some(context) = self.file.context() {
            context.checkpoint()?;
        }
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for block in self
            .blocks
            .iter()
            .filter(|block| block.cardinality > 0 && block.contains_range(id))
        {
            if cache.as_ref().is_none_or(|cached| {
                cached.layer != block.layer || cached.segment_id != block.segment_id
            }) {
                let bytes = self.read_block_bytes(block, metrics)?;
                let memory = self
                    .file
                    .context()
                    .map(|context| {
                        context.memory.reserve(add(
                            bytes.len(),
                            mul(block.cardinality, size_of::<CandidateEntry>())?,
                        )?)
                    })
                    .transpose()?;
                *cache = Some(CandidateCache {
                    layer: block.layer,
                    segment_id: block.segment_id,
                    entries: decode_candidate_entries(&bytes, block.cardinality)?,
                    _memory: memory,
                });
            }
            if cache.as_ref().is_some_and(|cached| {
                cached
                    .entries
                    .binary_search_by(|value| value.id.as_str().cmp(id))
                    .is_ok()
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(feature = "vector-search")]
    fn vector_allowlist_layout(&self, layer: usize) -> Result<(usize, u64)> {
        let cardinality = self
            .blocks
            .iter()
            .filter(|block| block.layer == layer)
            .try_fold(0usize, |total, block| total.checked_add(block.cardinality))
            .ok_or_else(|| {
                HawDBError::Storage("search vector candidate count overflow".to_string())
            })?;
        let ordinal_capacity_bytes = (cardinality as u64)
            .checked_mul(std::mem::size_of::<u64>() as u64)
            .ok_or_else(|| {
                HawDBError::Storage("search vector allowlist size overflow".to_string())
            })?;
        let max_block_bytes = self
            .blocks
            .iter()
            .filter(|block| block.layer == layer)
            .map(|block| block.length)
            .max()
            .unwrap_or_default();
        let required_working_bytes = ordinal_capacity_bytes
            .checked_add(max_block_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("search vector allowlist working set overflow".to_string())
            })?;
        Ok((cardinality, required_working_bytes))
    }

    #[cfg(feature = "vector-search")]
    fn vector_ordinals_for_layer(
        &self,
        layer: usize,
        max_bytes: u64,
        task_context: Option<&crate::RuntimeTaskContext>,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<Ordinals> {
        let (cardinality, required_working_bytes) = self.vector_allowlist_layout(layer)?;
        if required_working_bytes > max_bytes {
            return Err(HawDBError::Storage(format!(
                "search vector candidate allowlist and block require {required_working_bytes} bytes, exceeding {max_bytes}"
            )));
        }
        let mut ordinals = Ordinals {
            values: Vec::new(),
            _memory: self
                .file
                .context()
                .map(|context| context.memory.reserve(0))
                .transpose()?,
        };
        if let Some(memory) = &mut ordinals._memory {
            reserve_capacity(&mut ordinals.values, cardinality, memory)?;
        } else {
            ordinals.values = Vec::with_capacity(cardinality);
        }
        for block in self.blocks.iter().filter(|block| block.layer == layer) {
            if let Some(task_context) = task_context {
                task_context.checkpoint().map_err(|reason| {
                    HawDBError::Execution(format!("search vector task {reason}"))
                })?;
            }
            if block.cardinality == 0 {
                continue;
            }
            let bytes = self.read_block_bytes(block, metrics)?;
            decode_candidate_ordinals_into(&bytes, block.cardinality, &mut ordinals.values)?;
        }
        if ordinals.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(HawDBError::Storage(
                "search vector candidate ordinals are not strictly ordered".to_string(),
            ));
        }
        Ok(ordinals)
    }

    fn read_block_bytes(
        &self,
        block: &CandidateBlock,
        metrics: &mut SearchOutOfCoreMetrics,
    ) -> Result<BlockBytes> {
        if block.length > self.max_block_bytes {
            return Err(HawDBError::Storage(format!(
                "search candidate block {} exceeds its read budget",
                block.segment_id
            )));
        }
        let length = usize::try_from(block.length).map_err(|_| {
            HawDBError::Storage("search candidate block length exceeds usize".to_string())
        })?;
        let memory = self
            .file
            .context()
            .map(|context| context.memory.reserve(length))
            .transpose()?;
        let mut bytes = BlockBytes {
            values: vec![0u8; length],
            _memory: memory,
        };
        self.file.read_exact_at(block.offset, &mut bytes.values)?;
        metrics.candidate_block_reads = metrics.candidate_block_reads.saturating_add(1);
        metrics.candidate_bytes_read = metrics.candidate_bytes_read.saturating_add(block.length);
        Ok(bytes)
    }
}

struct BlockBytes {
    values: Vec<u8>,
    _memory: Option<QueryMemoryLease>,
}
impl std::ops::Deref for BlockBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

#[cfg(feature = "vector-search")]
pub(super) struct Ordinals {
    values: Vec<u64>,
    _memory: Option<QueryMemoryLease>,
}
#[cfg(feature = "vector-search")]
impl std::ops::Deref for Ordinals {
    type Target = Vec<u64>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

fn decode_candidate_entries(bytes: &[u8], expected: usize) -> Result<Vec<CandidateEntry>> {
    let mut offset = 0usize;
    let mut entries = Vec::with_capacity(expected);
    while offset < bytes.len() {
        if entries.len() == expected {
            return Err(HawDBError::Storage(
                "search candidate block count exceeded".into(),
            ));
        }
        let end = offset.saturating_add(4);
        let length_bytes = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated length".to_string())
        })?;
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
        offset = end;
        let end = offset.checked_add(length).ok_or_else(|| {
            HawDBError::Storage("search candidate id length overflows".to_string())
        })?;
        let raw = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated id".to_string())
        })?;
        let id = String::from_utf8(raw.to_vec()).map_err(|error| {
            HawDBError::Storage(format!("search candidate id is not UTF-8: {error}"))
        })?;
        offset = end;
        let end = offset.saturating_add(std::mem::size_of::<u64>());
        let raw_ordinal = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated vector ordinal".to_string())
        })?;
        let _vector_ordinal = u64::from_le_bytes(raw_ordinal.try_into().unwrap());
        entries.push(CandidateEntry { id });
        offset = end;
    }
    if entries.len() != expected || entries.windows(2).any(|pair| pair[0].id >= pair[1].id) {
        return Err(HawDBError::Storage(
            "search candidate block count or ordering mismatch".to_string(),
        ));
    }
    Ok(entries)
}

#[cfg(feature = "vector-search")]
fn decode_candidate_ordinals_into(
    bytes: &[u8],
    expected: usize,
    ordinals: &mut Vec<u64>,
) -> Result<()> {
    let mut offset = 0usize;
    let mut count = 0usize;
    let mut previous_id = None;
    while offset < bytes.len() {
        if count == expected {
            return Err(HawDBError::Storage(
                "search candidate block count exceeded".into(),
            ));
        }
        let end = offset.saturating_add(std::mem::size_of::<u32>());
        let length_bytes = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated length".to_string())
        })?;
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap()) as usize;
        offset = end;
        let end = offset.checked_add(length).ok_or_else(|| {
            HawDBError::Storage("search candidate id length overflows".to_string())
        })?;
        let id = std::str::from_utf8(bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated id".to_string())
        })?)
        .map_err(|error| {
            HawDBError::Storage(format!("search candidate id is not UTF-8: {error}"))
        })?;
        if previous_id.is_some_and(|previous| previous >= id) {
            return Err(HawDBError::Storage(
                "search candidate block ids are not strictly ordered".to_string(),
            ));
        }
        previous_id = Some(id);
        offset = end;
        let end = offset.saturating_add(std::mem::size_of::<u64>());
        let raw_ordinal = bytes.get(offset..end).ok_or_else(|| {
            HawDBError::Storage("search candidate block has a truncated vector ordinal".to_string())
        })?;
        let vector_ordinal = u64::from_le_bytes(raw_ordinal.try_into().unwrap());
        if vector_ordinal != u64::MAX {
            ordinals.push(vector_ordinal);
        }
        offset = end;
        count = count.saturating_add(1);
    }
    if count != expected {
        return Err(HawDBError::Storage(
            "search candidate block count mismatch".to_string(),
        ));
    }
    Ok(())
}

fn unique_candidate_path(directory: &Path) -> PathBuf {
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    directory.join(format!(
        ".hawdb-search-candidates.{}.{}",
        std::process::id(),
        sequence
    ))
}
