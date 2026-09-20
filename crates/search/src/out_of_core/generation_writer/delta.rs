// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{
    publication::ActiveManifestUpdate, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::error::{HawDBError, Result};
use crate::{
    SearchDocument, SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreMetrics,
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionDeltaReport,
};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;

mod input;

pub(super) mod hydration;

#[derive(Debug, Clone, Copy)]
struct LocalMutationTarget {
    start: usize,
    end: usize,
    first_segment_id: u64,
    last_segment_id: u64,
    document_count: usize,
    documents_digest: u64,
}

impl LocalMutationTarget {
    fn active_manifest_update(self, expected_generation: u64) -> ActiveManifestUpdate {
        if self.end - self.start == 1 {
            return ActiveManifestUpdate::Replace {
                expected_generation,
                segment_id: self.first_segment_id,
                expected_document_count: self.document_count,
                expected_documents_digest: self.documents_digest,
            };
        }
        ActiveManifestUpdate::ReplaceRange {
            expected_generation,
            first_segment_id: self.first_segment_id,
            last_segment_id: self.last_segment_id,
            segment_count: self.end - self.start,
            expected_document_count: self.document_count,
            expected_documents_digest: self.documents_digest,
        }
    }

    fn action(self) -> &'static str {
        if self.end - self.start == 1 {
            "incremental_segment_replace"
        } else {
            "incremental_segment_range_replace"
        }
    }
}

#[derive(Debug)]
pub struct SearchOutOfCoreGenerationUpdate {
    writer: SearchOutOfCoreGenerationWriter,
    delta_report: SearchProjectionDeltaReport,
    source_read_metrics: SearchOutOfCoreMetrics,
    // The owned public report is handed to its caller only after finish returns.
    _report_memory: QueryMemoryLease,
}

impl SearchOutOfCoreGenerationUpdate {
    pub(super) fn prepare(
        reader: &SearchOutOfCoreReader,
        delta: SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
        task: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task)?;
        let memory = BuildMemory::new(&task)?;
        let operation_count = delta.operation_count();
        if let Some(limit) = delta.max_operations
            && operation_count > limit
        {
            return Err(HawDBError::Storage(format!(
                "incremental projection update operation count {operation_count} exceeded configured limit {limit}"
            )));
        }
        if operation_count > options.max_delta_operations.get() {
            return Err(HawDBError::Storage(format!(
                "incremental projection update operation count {operation_count} exceeded the generation admission {}",
                options.max_delta_operations
            )));
        }
        let delta_working_bytes = delta_working_bytes(&delta, &task)?;
        if delta_working_bytes > options.max_delta_working_bytes.get() {
            return Err(HawDBError::Storage(format!(
                "incremental projection update requires {delta_working_bytes} bytes, exceeding the generation admission {}",
                options.max_delta_working_bytes
            )));
        }

        let source_graph_commit_epoch_before = reader.source_graph_commit_epoch();
        let source_graph_commit_epoch_after = delta
            .source_graph_commit_epoch
            .or(source_graph_commit_epoch_before);
        let epoch_updated = delta.source_graph_commit_epoch.is_some();
        // Admit the retained input before any identity clone or conversion.
        let input = input::Pending::new(delta, &memory, &task)?;
        let mut options = super::context_memory::Options::new(options, &memory, &task)?;
        options.bind_delta_identity(reader, source_graph_commit_epoch_after)?;
        let report_memory = memory
            .retained
            .reserve("search_projection".len() * 2 + "bounded_generation_update".len())?;
        let mut input = input.convert(&task)?;
        validate_delta_ids(
            input.upserts.make_contiguous(),
            input.deletes.make_contiguous(),
            &task,
        )?;
        let before_document_count = reader.document_count();
        let upserted_documents = input.upserts.len();
        // Preserve the reader's identity, limits and policy snapshot for finish.
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            &reader.root,
            options,
            task.clone(),
            memory.clone(),
        )?;
        writer.set_lexical_term_policy(reader.lexical_term_policy());
        writer.set_lexical_source_policy(reader.lexical_source_policy());
        writer.set_max_lexical_manifest_bytes(reader.config().max_lexical_manifest_bytes)?;
        writer.expected_active_generation = Some(reader.generation());
        let can_append = input
            .upserts
            .front()
            .map_or(Ok(false), |upsert| reader.can_append_after(&upsert.id))?;
        if input.deletes.is_empty() && can_append {
            let after_document_count = before_document_count
                .checked_add(upserted_documents)
                .ok_or_else(|| HawDBError::Storage("search document count overflow".into()))?;
            while !input.upserts.is_empty() {
                writer.push_inner(input.pop_upsert())?;
            }
            writer.active_manifest_update = Some(ActiveManifestUpdate::Append {
                expected_generation: reader.generation(),
            });
            return Ok(Self {
                delta_report: SearchProjectionDeltaReport {
                    artifact_type: "search_projection".to_string(),
                    name: "search_projection".to_string(),
                    action: "incremental_segment_append".to_string(),
                    before_document_count,
                    after_document_count,
                    upserted_documents,
                    deleted_documents: 0,
                    operation_count,
                    source_graph_commit_epoch_before,
                    source_graph_commit_epoch_after,
                    source_graph_commit_epoch_updated: epoch_updated,
                },
                writer,
                source_read_metrics: SearchOutOfCoreMetrics::default(),
                _report_memory: report_memory,
            });
        }
        if let Some(target) = local_mutation_target(reader, &input)? {
            // An empty artifact has no valid descriptor range. Retain the
            // established full-generation path until removal is an explicit
            // manifest operation.
            if target.document_count > input.deletes.len() {
                writer.active_manifest_update =
                    Some(target.active_manifest_update(reader.generation()));
                let mut deleted_documents = 0usize;
                let source_read_metrics = hydration::visit_range(
                    reader,
                    target.start,
                    target.end,
                    &memory,
                    &task,
                    &mut |document| {
                        if input
                            .upserts
                            .front()
                            .is_some_and(|upsert| upsert.id == document.id)
                        {
                            writer.push_inner(input.pop_upsert())?;
                            return Ok(());
                        }
                        if input
                            .deletes
                            .pop_front_if(|deleted| deleted == &document.id)
                            .is_some()
                        {
                            deleted_documents = deleted_documents.saturating_add(1);
                            return Ok(());
                        }
                        writer.push_inner(document)
                    },
                )?;
                if !input.upserts.is_empty() || !input.deletes.is_empty() {
                    return Err(HawDBError::Storage(
                        "search mutation target did not contain its requested document".into(),
                    ));
                }
                let after_document_count = before_document_count
                    .checked_sub(target.document_count)
                    .and_then(|count| count.checked_add(writer.document_count))
                    .ok_or_else(|| {
                        HawDBError::Storage(
                            "search document count overflow during local segment update".into(),
                        )
                    })?;
                return Ok(Self {
                    delta_report: SearchProjectionDeltaReport {
                        artifact_type: "search_projection".to_string(),
                        name: "search_projection".to_string(),
                        action: target.action().to_string(),
                        before_document_count,
                        after_document_count,
                        upserted_documents,
                        deleted_documents,
                        operation_count,
                        source_graph_commit_epoch_before,
                        source_graph_commit_epoch_after,
                        source_graph_commit_epoch_updated: epoch_updated,
                    },
                    writer,
                    source_read_metrics,
                    _report_memory: report_memory,
                });
            }
        }
        let mut deleted_documents = 0usize;
        let source_read_metrics = hydration::visit(reader, &memory, &task, &mut |document| {
            while input
                .upserts
                .front()
                .is_some_and(|upsert| upsert.id < document.id)
            {
                writer.push_inner(input.pop_upsert())?;
            }
            while input
                .deletes
                .front()
                .is_some_and(|deleted| deleted < &document.id)
            {
                input.deletes.pop_front();
            }
            if input
                .upserts
                .front()
                .is_some_and(|upsert| upsert.id == document.id)
            {
                writer.push_inner(input.pop_upsert())?;
                return Ok(());
            }
            if input
                .deletes
                .pop_front_if(|deleted| deleted == &document.id)
                .is_some()
            {
                deleted_documents = deleted_documents.saturating_add(1);
                return Ok(());
            }
            writer.push_inner(document)
        })?;
        while !input.upserts.is_empty() {
            writer.push_inner(input.pop_upsert())?;
        }
        drop(input);
        checkpoint(&task)?;

        Ok(Self {
            delta_report: SearchProjectionDeltaReport {
                artifact_type: "search_projection".to_string(),
                name: "search_projection".to_string(),
                action: "bounded_generation_update".to_string(),
                before_document_count,
                after_document_count: writer.document_count,
                upserted_documents,
                deleted_documents,
                operation_count,
                source_graph_commit_epoch_before,
                source_graph_commit_epoch_after,
                source_graph_commit_epoch_updated: epoch_updated,
            },
            writer,
            source_read_metrics,
            _report_memory: report_memory,
        })
    }

    pub fn delta_report(&self) -> &SearchProjectionDeltaReport {
        &self.delta_report
    }

    /// Metrics for base artifacts consumed while preparing this update.
    ///
    /// A strictly appended delta does not read base payloads, so all source
    /// read counters are zero for that path.
    pub fn source_read_metrics(&self) -> &SearchOutOfCoreMetrics {
        &self.source_read_metrics
    }

    pub fn finish(
        self,
    ) -> Result<(
        SearchProjectionDeltaReport,
        SearchOutOfCoreGenerationBuildReport,
        SearchOutOfCoreMetrics,
    )> {
        let build_report = self.writer.finish()?;
        Ok((self.delta_report, build_report, self.source_read_metrics))
    }
}

fn local_mutation_target(
    reader: &SearchOutOfCoreReader,
    input: &input::Input,
) -> Result<Option<LocalMutationTarget>> {
    let mut target_indices = Vec::new();
    for document_id in input
        .upserts
        .iter()
        .map(|document| document.id.as_str())
        .chain(input.deletes.iter().map(String::as_str))
    {
        let Some(segment_id) = reader.resolve_mutation_segment(document_id)? else {
            return Ok(None);
        };
        let index = reader
            .manifest
            .segments
            .iter()
            .position(|segment| segment.segment_id == segment_id)
            .ok_or_else(|| {
                HawDBError::Storage(
                    "search mutation target segment is not in the active manifest".into(),
                )
            })?;
        target_indices.push(index);
    }
    target_indices.sort_unstable();
    target_indices.dedup();
    let Some(&start) = target_indices.first() else {
        return Ok(None);
    };
    let end = target_indices
        .last()
        .and_then(|last| last.checked_add(1))
        .ok_or_else(|| HawDBError::Storage("search mutation target range overflows".into()))?;
    if end - start != target_indices.len() {
        return Ok(None);
    }
    let segments = &reader.manifest.segments[start..end];
    let level = segments[0].level;
    if segments.iter().any(|segment| segment.level != level) {
        return Ok(None);
    }
    let document_count = segments.iter().try_fold(0usize, |total, segment| {
        total.checked_add(segment.document_count).ok_or_else(|| {
            HawDBError::Storage("search mutation target document count overflows".into())
        })
    })?;
    let documents_digest = segments.iter().fold(0u64, |digest, segment| {
        crate::lexical_projection::DocumentsDigest::combine(digest, segment.documents_digest)
    });
    Ok(Some(LocalMutationTarget {
        start,
        end,
        first_segment_id: segments[0].segment_id,
        last_segment_id: segments[segments.len() - 1].segment_id,
        document_count,
        documents_digest,
    }))
}

fn validate_delta_ids(
    upserts: &[SearchDocument],
    deletes: &[String],
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    for id in upserts.iter().map(|document| &document.id).chain(deletes) {
        checkpoint(task)?;
        if id.is_empty() {
            return Err(HawDBError::Storage(
                "search generation delta document ids must not be empty".into(),
            ));
        }
    }
    for pair in upserts.windows(2) {
        checkpoint(task)?;
        if pair[0].id == pair[1].id {
            return Err(HawDBError::Storage(
                "search generation delta contains duplicate upsert ids".into(),
            ));
        }
    }
    for pair in deletes.windows(2) {
        checkpoint(task)?;
        if pair[0] == pair[1] {
            return Err(HawDBError::Storage(
                "search generation delta contains duplicate delete ids".into(),
            ));
        }
    }
    let mut upsert_index = 0usize;
    let mut delete_index = 0usize;
    while upsert_index < upserts.len() && delete_index < deletes.len() {
        checkpoint(task)?;
        match upserts[upsert_index].id.cmp(&deletes[delete_index]) {
            std::cmp::Ordering::Less => upsert_index = upsert_index.saturating_add(1),
            std::cmp::Ordering::Greater => delete_index = delete_index.saturating_add(1),
            std::cmp::Ordering::Equal => {
                return Err(HawDBError::Storage(format!(
                    "search generation delta contains both upsert and delete for {}",
                    deletes[delete_index]
                )));
            }
        }
    }
    Ok(())
}

fn delta_working_bytes(delta: &SearchProjectionDelta, task: &RuntimeTaskContext) -> Result<u64> {
    let mut total = 0u64;
    for row in &delta.upserts {
        checkpoint(task)?;
        let embedding_bytes = row.embedding.as_ref().map_or(0u64, |embedding| {
            (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
        });
        let mut metadata_bytes = 0u64;
        for (name, value) in &row.metadata {
            checkpoint(task)?;
            metadata_bytes = metadata_bytes
                .saturating_add(name.len() as u64)
                .saturating_add(value.len() as u64);
        }
        total = total
            .saturating_add(std::mem::size_of::<crate::SearchProjectionRow>() as u64)
            .saturating_add(row.external_id.len() as u64)
            .saturating_add(row.title.len() as u64)
            .saturating_add(row.body.len() as u64)
            .saturating_add(row.source_id.as_ref().map_or(0, |value| value.len() as u64))
            .saturating_add(embedding_bytes)
            .saturating_add(metadata_bytes);
    }
    for id in &delta.deletes {
        checkpoint(task)?;
        total = total
            .saturating_add(std::mem::size_of::<String>() as u64)
            .saturating_add(id.len() as u64);
    }
    Ok(total)
}

#[cfg(test)]
mod tests;
