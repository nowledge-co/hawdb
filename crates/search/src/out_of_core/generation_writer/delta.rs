use super::{SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter};
use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::error::{Result, SkeinError};
use crate::{
    SearchDocument, SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreMetrics,
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionDeltaReport,
};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;

mod input;

mod hydration;

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
            return Err(SkeinError::Storage(format!(
                "incremental projection update operation count {operation_count} exceeded configured limit {limit}"
            )));
        }
        if operation_count > options.max_delta_operations.get() {
            return Err(SkeinError::Storage(format!(
                "incremental projection update operation count {operation_count} exceeded the generation admission {}",
                options.max_delta_operations
            )));
        }
        let delta_working_bytes = delta_working_bytes(&delta, &task)?;
        if delta_working_bytes > options.max_delta_working_bytes.get() {
            return Err(SkeinError::Storage(format!(
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
        writer.set_max_lexical_manifest_bytes(reader.config().max_lexical_manifest_bytes)?;
        writer.expected_active_generation = Some(reader.generation());
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

fn validate_delta_ids(
    upserts: &[SearchDocument],
    deletes: &[String],
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    for id in upserts.iter().map(|document| &document.id).chain(deletes) {
        checkpoint(task)?;
        if id.is_empty() {
            return Err(SkeinError::Storage(
                "search generation delta document ids must not be empty".into(),
            ));
        }
    }
    for pair in upserts.windows(2) {
        checkpoint(task)?;
        if pair[0].id == pair[1].id {
            return Err(SkeinError::Storage(
                "search generation delta contains duplicate upsert ids".into(),
            ));
        }
    }
    for pair in deletes.windows(2) {
        checkpoint(task)?;
        if pair[0] == pair[1] {
            return Err(SkeinError::Storage(
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
                return Err(SkeinError::Storage(format!(
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
