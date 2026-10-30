// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Immutable resident graph candidates with an exact changed-record overlay.

use crate::{CosineQuery, DocumentView, HawDBError, Result, SearchDocument, SearchIndex};
use hawdb_core::{ResourceBudgetError, ResourceBudgetKind, ResourceBudgetScope, VectorDistance};
use hawdb_executor::{
    VectorCandidate, VectorCandidateBatch, VectorCandidateScanMetrics, VectorScoreSource,
};
use hawdb_vector_projection::diskann::{
    search_diskann, DiskAnnBuildConfig, DiskAnnIndex, DiskAnnRead, DiskAnnSearchOptions,
    DiskAnnSearchOutput, FileDiskAnn,
};
use hawdb_vector_projection::{CandidateSet, ProjectionError};
#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

const CACHE_BYTES: usize = 64 << 20;

#[derive(Debug, Default)]
pub(super) struct ResidentDiskAnn {
    projection: Mutex<Option<Arc<Projection>>>,
}

#[derive(Debug)]
struct Projection {
    graph: Arc<Graph>,
    ids: Arc<Vec<String>>,
    // Mutation never edits a graph already captured by a query. These bytes
    // retract original ordinals; new IDs are recognized by the immutable map.
    changed: Vec<bool>,
    dirty: bool,
    memory_bytes: usize,
    _memory: Mutex<Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>>,
}

impl Clone for Projection {
    fn clone(&self) -> Self {
        Self {
            graph: Arc::clone(&self.graph),
            ids: Arc::clone(&self.ids),
            changed: self.changed.clone(),
            dirty: self.dirty,
            memory_bytes: self.memory_bytes,
            // A successor query funds its own conservative bound; the old
            // generation retains its original allocation permit independently.
            _memory: Mutex::new(None),
        }
    }
}

#[derive(Debug)]
enum Graph {
    Memory(DiskAnnIndex),
    File(FileDiskAnn<()>),
}

impl Graph {
    fn len(&self) -> usize {
        match self {
            Self::Memory(graph) => graph.len(),
            Self::File(graph) => graph.len(),
        }
    }

    fn dimension(&self) -> usize {
        match self {
            Self::Memory(graph) => graph.dimension(),
            Self::File(graph) => graph.dimension(),
        }
    }

    fn search(
        &self,
        query: &[f32],
        limit: usize,
        candidates: CandidateSet<'_>,
        options: DiskAnnSearchOptions<'_>,
    ) -> hawdb_vector_projection::Result<DiskAnnSearchOutput> {
        match self {
            Self::Memory(graph) => search_diskann(graph, query, limit, Some(candidates), options),
            Self::File(graph) => search_diskann(graph, query, limit, Some(candidates), options),
        }
    }

    fn write_artifact(
        &self,
        output: &mut impl std::io::Write,
        identity: hawdb_vector_projection::diskann::DiskAnnArtifactIdentity,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> hawdb_vector_projection::Result<()> {
        match self {
            Self::Memory(graph) => graph.write_artifact(output, identity, task),
            Self::File(graph) => graph.write_artifact(output, identity, task),
        }?;
        Ok(())
    }
}

pub(super) struct Output {
    pub(super) batch: VectorCandidateBatch,
    pub(super) metrics: VectorCandidateScanMetrics,
    pub(super) candidate_vector_bytes: u64,
    pub(super) memory: Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>,
}

impl ResidentDiskAnn {
    pub(super) fn capture(&self) -> Self {
        Self {
            projection: Mutex::new(self.lock(None).expect("unpoisoned cache").clone()),
        }
    }

    pub(super) fn retained_capacity_bound(&self, index: &SearchIndex) -> usize {
        if let Some(projection) = self.lock(None).expect("unpoisoned cache").as_ref() {
            let mapped = match projection.graph.as_ref() {
                Graph::File(graph) => {
                    usize::try_from(graph.artifact_fingerprint().0).unwrap_or(usize::MAX)
                }
                Graph::Memory(_) => 0,
            };
            return projection.memory_bytes.saturating_add(mapped);
        }
        index.embedding_dimension.map_or(0, |dimension| {
            Projection::memory_bound(&index.documents, dimension, None)
                .map_or(usize::MAX, |(_, _, total)| total)
        })
    }

    pub(super) fn qualification_identity(
        &self,
        index: &SearchIndex,
    ) -> Option<crate::VectorProjectionQualificationIdentity> {
        let current = self.lock(None).ok()?;
        let projection = current.as_ref()?;
        if projection.dirty {
            return None;
        }
        let Graph::File(graph) = projection.graph.as_ref() else {
            return None;
        };
        let metadata = graph.metadata();
        if Some(metadata.identity.source_epoch) != index.source_graph_commit_epoch {
            return None;
        }
        let (payload_bytes, payload_checksum) = graph.artifact_fingerprint();
        Some(crate::VectorProjectionQualificationIdentity {
            projection_generation: metadata.identity.generation,
            source_graph_commit_epoch: Some(metadata.identity.source_epoch),
            document_count: metadata.node_count,
            source_digest: u64::from_le_bytes(
                metadata.identity.source_digest[..8]
                    .try_into()
                    .expect("digest prefix"),
            ),
            payload_bytes,
            payload_checksum,
            format_version: 2,
            algorithm: "diskann".into(),
            bit_width: 0,
            dimension: metadata.dimension,
            transform_seed: 0,
            embedding_model: index
                .embedding_manifest
                .as_ref()
                .map(|manifest| manifest.model.clone()),
            embedding_version: index
                .embedding_manifest
                .as_ref()
                .and_then(|manifest| manifest.version.clone()),
            file_backed: true,
        })
    }

    fn restore(&self, index: &SearchIndex, graph: FileDiskAnn<()>) -> Result<()> {
        let count = graph.len();
        let mut memory_bytes = size_of::<Projection>().saturating_add(count);
        for document in index
            .documents
            .values()
            .filter(|row| row.embedding.is_some())
        {
            memory_bytes = memory_bytes.saturating_add(size_of::<String>() + document.id.len() + 1);
        }
        if memory_bytes > CACHE_BYTES {
            return Err(memory_error(memory_bytes, CACHE_BYTES));
        }
        // This facade's private ordinal map uses contiguous vector-only IDs.
        for ordinal in 0..count as u32 {
            if graph.id(ordinal).map_err(projection_error)? != u64::from(ordinal) {
                return Err(HawDBError::StorageIntegrity(
                    "resident graph IDs differ from the canonical ordinal map".into(),
                ));
            }
        }
        let mut ids = Vec::with_capacity(count);
        ids.extend(
            index
                .documents
                .values()
                .filter(|row| row.embedding.is_some())
                .map(|row| row.id.clone()),
        );
        *self.lock(None)? = Some(Arc::new(Projection {
            graph: Arc::new(Graph::File(graph)),
            ids: Arc::new(ids),
            changed: vec![false; count],
            dirty: false,
            memory_bytes,
            _memory: Mutex::new(None),
        }));
        Ok(())
    }

    pub(super) fn reset(&self) {
        *self
            .projection
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = None;
    }

    pub(super) fn record_change(&mut self, id: &str) {
        if let Some(projection) = self
            .projection
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
        {
            // Captured read generations retain the immutable graph and their
            // original retractions. Copy only overlay state for this successor.
            let projection = Arc::make_mut(projection);
            projection.dirty = true;
            if let Ok(ordinal) = projection
                .ids
                .binary_search_by(|existing| existing.as_str().cmp(id))
            {
                projection.changed[ordinal] = true;
            }
        }
    }

    fn snapshot(
        &self,
        index: &SearchIndex,
        dimension: usize,
        options: crate::VectorSearchExecutionOptions<'_>,
    ) -> Result<Arc<Projection>> {
        let mut current = self.lock(options.task_context)?;
        if current.is_none() {
            *current = Some(Arc::new(Projection::build(
                &index.documents,
                dimension,
                options,
            )?));
        }
        Ok(Arc::clone(
            current
                .as_ref()
                .expect("graph initialized under the cache lock"),
        ))
    }

    /// Cache residency is optional for legacy, unbound SearchIndex callers.
    /// Caller-selected limits below the ordinary envelope remain hard limits.
    pub(super) fn admits_default_search(
        &self,
        index: &SearchIndex,
        documents: &[&SearchDocument],
        dimension: usize,
        limit: usize,
        options: crate::VectorSearchExecutionOptions<'_>,
    ) -> Result<bool> {
        checkpoint(options.task_context)?;
        let current = self.lock(options.task_context)?;
        let (count, memory) = match current.as_ref() {
            Some(projection) => (projection.graph.len(), projection.memory_bytes),
            None => {
                let (count, _, memory) =
                    Projection::memory_bound(&index.documents, dimension, options.task_context)?;
                (count, memory)
            }
        };
        if memory > CACHE_BYTES {
            return Ok(false);
        }
        let (query_bytes, _) = query_memory(documents, count, limit.min(documents.len()), options)?;
        Ok(memory.saturating_add(query_bytes) <= options.max_working_bytes.max(CACHE_BYTES))
    }

    fn admits_complete_cache(
        &self,
        index: &SearchIndex,
        dimension: usize,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<bool> {
        let current = self.lock(task)?;
        if current.as_ref().is_some_and(|projection| !projection.dirty) {
            return Ok(true);
        }
        let (_, _, required) = Projection::memory_bound(&index.documents, dimension, task)?;
        let retained = current
            .as_ref()
            .filter(|projection| Arc::strong_count(projection) > 1)
            .map_or(0, |projection| projection.memory_bytes);
        Ok(required.saturating_add(retained) <= CACHE_BYTES)
    }

    fn complete_snapshot(
        &self,
        index: &SearchIndex,
        dimension: usize,
        mut options: crate::VectorSearchExecutionOptions<'_>,
    ) -> Result<Arc<Projection>> {
        let mut current = self.lock(options.task_context)?;
        if current.as_ref().is_some_and(|projection| projection.dirty) {
            let previous = current.as_ref().expect("checked dirty projection");
            if Arc::strong_count(previous) == 1 {
                current.take();
            } else {
                options.max_working_bytes = options
                    .max_working_bytes
                    .checked_sub(previous.memory_bytes)
                    .ok_or_else(|| {
                        memory_error(previous.memory_bytes, options.max_working_bytes)
                    })?;
                let replacement = Projection::build(&index.documents, dimension, options)?;
                *current = Some(Arc::new(replacement));
            }
        }
        if current.is_none() {
            *current = Some(Arc::new(Projection::build(
                &index.documents,
                dimension,
                options,
            )?));
        }
        Ok(Arc::clone(
            current
                .as_ref()
                .expect("complete graph initialized under cache lock"),
        ))
    }

    fn lock(
        &self,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<MutexGuard<'_, Option<Arc<Projection>>>> {
        if task.is_none() {
            return Ok(self
                .projection
                .lock()
                .unwrap_or_else(|error| error.into_inner()));
        }
        loop {
            checkpoint(task)?;
            match self.projection.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
                Err(TryLockError::WouldBlock) => {
                    std::thread::sleep(std::time::Duration::from_millis(1))
                }
            }
        }
    }

    pub(super) fn search(
        &self,
        index: &SearchIndex,
        documents: &[&SearchDocument],
        query: &[f32],
        limit: usize,
        options: crate::VectorSearchExecutionOptions<'_>,
    ) -> Result<Output> {
        let checkpoint = || checkpoint(options.task_context);
        checkpoint()?;
        let projection = self.snapshot(index, query.len(), options)?;
        let limit = limit.min(documents.len());
        let (query_bytes, graph_options) =
            query_memory(documents, projection.graph.len(), limit, options)?;
        let combined = query_bytes.saturating_add(projection.memory_bytes);
        if combined > options.max_working_bytes {
            return Err(memory_error(combined, options.max_working_bytes));
        }
        {
            let mut funding = projection
                ._memory
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if funding.is_none() {
                *funding = reserve(options.task_context, projection.memory_bytes)?;
            }
        }
        let memory = reserve(options.task_context, query_bytes)?;
        let mut allowed = Vec::with_capacity(documents.len());
        for document in documents {
            checkpoint()?;
            if document.embedding.is_some()
                && let Ok(ordinal) = projection.ids.binary_search(&document.id)
                && !projection.changed[ordinal]
            {
                allowed.push(ordinal as u64);
            }
        }
        // Callers normally supply canonical ID order. Keep this private boundary
        // independent of payload-access order without allocating another set.
        allowed.sort_unstable();
        allowed.dedup();
        let output = projection
            .graph
            .search(
                query,
                limit,
                CandidateSet {
                    ids: &allowed,
                    exact: true,
                },
                graph_options,
            )
            .map_err(projection_error)?;
        let query_scorer = CosineQuery::with_work(query, checkpoint)?;
        let mut candidates = Vec::with_capacity(limit);
        let mut candidate_vector_bytes = 0u64;
        for hit in &output.hits {
            checkpoint()?;
            let id = &projection.ids[hit.id as usize];
            let embedding = index
                .documents
                .get(id)
                .and_then(|row| row.embedding.as_deref())
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "resident graph ordinal was not retracted before mutation".into(),
                    )
                })?;
            candidate_vector_bytes =
                candidate_vector_bytes.saturating_add((embedding.len() as u64).saturating_mul(4));
            if let Some(score) = query_scorer.score(embedding) {
                retain_candidate(&mut candidates, limit, id, score);
            }
        }
        let mut overlay_count = 0usize;
        let mut overlay_vector_bytes = 0u64;
        for document in documents {
            checkpoint()?;
            if projection
                .ids
                .binary_search(&document.id)
                .is_ok_and(|ordinal| !projection.changed[ordinal])
            {
                continue;
            }
            let Some(embedding) = document.embedding.as_deref() else {
                continue;
            };
            if output.report.visited_nodes.saturating_add(overlay_count)
                >= graph_options.max_visited
            {
                return Err(ResourceBudgetError {
                    resource: ResourceBudgetKind::WorkUnits,
                    scope: ResourceBudgetScope::Operator,
                    owner: "resident DiskANN changed candidates".into(),
                    memory_class: None,
                    requested: 1,
                    used: output.report.visited_nodes.saturating_add(overlay_count) as u64,
                    budget: graph_options.max_visited as u64,
                    budget_option: "DiskAnnSearchOptions.max_visited",
                    message: "graph navigation and changed candidates exceed their work budget"
                        .into(),
                }
                .into());
            }
            if let Some(task) = options.task_context {
                task.charge_work(1)?;
            }
            overlay_count += 1;
            overlay_vector_bytes =
                overlay_vector_bytes.saturating_add((embedding.len() as u64).saturating_mul(4));
            if let Some(score) = query_scorer.score(embedding) {
                if !score.is_finite() {
                    return Err(non_finite_source());
                }
                retain_candidate(&mut candidates, limit, &document.id, score);
            }
        }
        let report = output.report;
        Ok(Output {
            batch: VectorCandidateBatch {
                score_source: VectorScoreSource::AnnApproximate,
                candidates,
            },
            metrics: VectorCandidateScanMetrics {
                kernel: hawdb_core::vector::distance_kernel_name(
                    hawdb_core::VectorDistance::Cosine,
                    query.len(),
                )
                .into(),
                worker_count: 1,
                segment_count: 1 + usize::from(overlay_count != 0),
                scanned_segment_count: usize::from(report.visited_nodes != 0)
                    + usize::from(overlay_count != 0),
                scored_document_count: report.distance_comparisons.saturating_add(overlay_count),
                filtered_document_count: report.bridge_nodes,
                scanned_block_count: report.expanded_nodes,
                skipped_block_count: 0,
                adaptive_exact_scan_count: usize::from(report.adaptive_exact_scan),
                payload_bytes_read: (report.distance_comparisons as u64)
                    .saturating_mul((projection.graph.dimension() as u64).saturating_mul(4))
                    .saturating_add((report.neighbor_reads as u64).saturating_mul(4))
                    .saturating_add(overlay_vector_bytes),
                admitted_working_bytes: options.max_working_bytes,
            },
            candidate_vector_bytes: candidate_vector_bytes.saturating_add(overlay_vector_bytes),
            memory,
        })
    }
}

fn query_memory<'a>(
    documents: &[&SearchDocument],
    count: usize,
    limit: usize,
    options: crate::VectorSearchExecutionOptions<'a>,
) -> Result<(usize, DiskAnnSearchOptions<'a>)> {
    let max_id_bytes = documents
        .iter()
        .map(|document| document.id.len())
        .max()
        .unwrap_or(0);
    let retained_bytes = documents
        .len()
        .saturating_mul(size_of::<u64>())
        .saturating_add(
            limit
                .saturating_mul(size_of::<VectorCandidate>() + max_id_bytes)
                .saturating_mul(2),
        );
    let graph_options = DiskAnnSearchOptions {
        beam_width: limit.max(crate::vector_execution::DISKANN_NAVIGATION_BEAM),
        max_working_bytes: options.max_working_bytes.saturating_sub(retained_bytes),
        task: options.task_context,
        ..Default::default()
    };
    let bytes = graph_options
        .working_memory_bound(count, limit)
        .map_err(projection_error)?
        .saturating_add(retained_bytes);
    Ok((bytes, graph_options))
}

impl Projection {
    fn memory_bound(
        documents: &impl DocumentView,
        dimension: usize,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<(usize, usize, usize)> {
        let mut count = 0usize;
        let mut id_bytes = 0usize;
        for document in documents.values() {
            checkpoint(task)?;
            if document
                .embedding
                .as_ref()
                .is_some_and(|vector| vector.iter().any(|value| !value.is_finite()))
            {
                return Err(non_finite_source());
            }
            if valid_embedding(document, dimension) {
                count = count.saturating_add(1);
                id_bytes = id_bytes.saturating_add(size_of::<String>() + document.id.len() + 1);
            }
        }
        let config = DiskAnnBuildConfig {
            max_working_bytes: usize::MAX,
            ..Default::default()
        };
        let required =
            DiskAnnIndex::build_memory_bound(count, dimension, config).map_err(projection_error)?;
        let total = required
            .saturating_add(id_bytes)
            .saturating_add(size_of::<Projection>() + size_of::<Graph>() + 64);
        Ok((count, id_bytes, total))
    }

    fn build(
        documents: &impl DocumentView,
        dimension: usize,
        options: crate::VectorSearchExecutionOptions<'_>,
    ) -> Result<Self> {
        let checkpoint = || checkpoint(options.task_context);
        let (count, id_bytes, total) =
            Self::memory_bound(documents, dimension, options.task_context)?;
        let available = options.max_working_bytes.min(CACHE_BYTES);
        if total > available {
            return Err(memory_error(total, available));
        }
        let config = DiskAnnBuildConfig {
            max_working_bytes: available - id_bytes,
            ..Default::default()
        };
        let memory = reserve(options.task_context, total)?;
        let mut ids = Vec::with_capacity(count);
        let mut entries = Vec::with_capacity(count);
        for document in documents.values() {
            checkpoint()?;
            if valid_embedding(document, dimension) {
                entries.push((
                    ids.len() as u64,
                    document
                        .embedding
                        .as_ref()
                        .expect("validated embedding")
                        .clone(),
                ));
                ids.push(document.id.clone());
            }
        }
        let graph = DiskAnnIndex::build(
            dimension,
            VectorDistance::Cosine,
            entries,
            config,
            options.task_context,
        )
        .map_err(projection_error)?;
        Ok(Self {
            graph: Arc::new(Graph::Memory(graph)),
            ids: Arc::new(ids),
            changed: vec![false; count],
            dirty: false,
            memory_bytes: total,
            _memory: Mutex::new(memory),
        })
    }
}

fn valid_embedding(document: &SearchDocument, dimension: usize) -> bool {
    document.embedding.as_ref().is_some_and(|vector| {
        vector.len() == dimension
            && vector.iter().all(|value| value.is_finite())
            && vector.iter().any(|value| *value != 0.0)
    })
}

fn retain_candidate(candidates: &mut Vec<VectorCandidate>, limit: usize, id: &str, score: f64) {
    if limit == 0 || !score.is_finite() || score <= 0.0 {
        return;
    }
    let position = candidates.partition_point(|candidate| {
        candidate
            .score
            .total_cmp(&score)
            .reverse()
            .then_with(|| candidate.id.as_str().cmp(id))
            .is_lt()
    });
    if position >= limit {
        return;
    }
    if candidates.len() == limit {
        candidates.pop();
    }
    candidates.insert(
        position,
        VectorCandidate {
            id: id.to_owned(),
            score,
        },
    );
}

fn checkpoint(task: Option<&hawdb_core::RuntimeTaskContext>) -> Result<()> {
    task.map_or(Ok(()), crate::build_control::checkpoint)
}

fn reserve(
    task: Option<&hawdb_core::RuntimeTaskContext>,
    bytes: usize,
) -> Result<Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>> {
    task.map_or(Ok(None), |task| {
        task.reserve_working_memory(bytes as u64)
            .map_err(|error| match error {
                hawdb_core::RuntimeMemoryError::ReservationExceeded {
                    requested_bytes,
                    available_bytes,
                }
                | hawdb_core::RuntimeMemoryError::AdmissionDenied {
                    requested_bytes,
                    available_bytes,
                    ..
                } => memory_error(requested_bytes as usize, available_bytes as usize),
                error => {
                    HawDBError::Execution(format!("resident DiskANN allocation refused: {error}"))
                }
            })
    })
}

fn memory_error(required: usize, available: usize) -> HawDBError {
    ResourceBudgetError {
        resource: ResourceBudgetKind::MemoryBytes,
        scope: ResourceBudgetScope::Operator,
        owner: "resident DiskANN".into(),
        memory_class: None,
        requested: required as u64,
        used: 0,
        budget: available as u64,
        budget_option: "VectorSearchExecutionOptions.max_working_bytes",
        message: "resident graph candidates exceed the admitted working memory".into(),
    }
    .into()
}

fn non_finite_source() -> HawDBError {
    HawDBError::Execution("resident DiskANN source contains a non-finite vector".into())
}

fn projection_error(error: ProjectionError) -> HawDBError {
    match error {
        ProjectionError::ResourceBudgetExceeded {
            required,
            available,
        } => memory_error(required, available),
        error => crate::rabitq_projection::map_projection_error(error, "resident DiskANN"),
    }
}

#[cfg(test)]
mod tests;

pub(super) mod artifact;
