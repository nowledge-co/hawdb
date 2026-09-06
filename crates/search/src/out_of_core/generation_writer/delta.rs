use super::{SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter};
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, BuildMemory};
use crate::error::{Result, SkeinError};
use crate::{
    SearchDocument, SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreMetrics,
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionDeltaReport,
};
use skein_core::RuntimeTaskContext;

#[derive(Debug)]
pub struct SearchOutOfCoreGenerationUpdate {
    writer: SearchOutOfCoreGenerationWriter,
    delta_report: SearchProjectionDeltaReport,
    source_read_metrics: SearchOutOfCoreMetrics,
}

impl SearchOutOfCoreGenerationUpdate {
    pub(super) fn prepare(
        reader: &SearchOutOfCoreReader,
        delta: SearchProjectionDelta,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<Self> {
        Self::prepare_with_context(reader, delta, options, RuntimeTaskContext::default())
    }

    pub(super) fn prepare_with_context(
        reader: &SearchOutOfCoreReader,
        delta: SearchProjectionDelta,
        mut options: SearchOutOfCoreGenerationBuildOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(&task_context)?;
        let operation_count = checked_add(delta.upserts.len(), delta.deletes.len())?;
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
        let source_graph_commit_epoch_before = reader.source_graph_commit_epoch();
        let source_graph_commit_epoch = delta.source_graph_commit_epoch;
        let source_graph_commit_epoch_after =
            source_graph_commit_epoch.or(source_graph_commit_epoch_before);
        let upserted_documents = delta.upserts.len();
        let memory = BuildMemory::new(&task_context)?;
        let mut input = super::delta_memory::Input::new(
            delta,
            &memory,
            options.max_delta_working_bytes.get(),
            &task_context,
        )?;
        bind_identity(reader, &mut options, source_graph_commit_epoch_after)?;
        let before_document_count = reader.document_count();
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            &reader.root,
            options,
            task_context.clone(),
            memory,
        )?;
        writer.expected_active_generation = Some(reader.generation());
        let mut deleted_documents = 0usize;
        let source_memory = writer.memory.retained.clone();
        let source_read_metrics =
            reader.visit_documents_in_order(&task_context, &source_memory, &mut |document| {
                while input
                    .upsert_id()
                    .is_some_and(|id| id < document.id.as_str())
                {
                    input.consume_upsert(|document| writer.push(document))?;
                }
                while input
                    .delete_id()
                    .is_some_and(|id| id < document.id.as_str())
                {
                    input.discard_delete();
                }
                if input.upsert_id() == Some(document.id.as_str()) {
                    input.consume_upsert(|document| writer.push(document))?;
                    return Ok(());
                }
                if input.delete_id() == Some(document.id.as_str()) {
                    input.discard_delete();
                    deleted_documents = deleted_documents.saturating_add(1);
                    return Ok(());
                }
                writer.push(document)
            })?;
        while input.upsert_id().is_some() {
            checkpoint(&task_context)?;
            input.consume_upsert(|document| writer.push(document))?;
        }
        drop(input);
        checkpoint(&task_context)?;

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
                source_graph_commit_epoch_updated: source_graph_commit_epoch.is_some(),
            },
            writer,
            source_read_metrics,
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

fn bind_identity(
    reader: &SearchOutOfCoreReader,
    options: &mut SearchOutOfCoreGenerationBuildOptions,
    source_graph_commit_epoch: Option<u64>,
) -> Result<()> {
    if options.source_graph_commit_epoch.is_some()
        && options.source_graph_commit_epoch != source_graph_commit_epoch
    {
        return Err(SkeinError::Storage(
            "search generation update source graph epoch does not match the delta".to_string(),
        ));
    }
    if options.import_source_graph_commit_epoch.is_some()
        && options.import_source_graph_commit_epoch != reader.import_source_graph_commit_epoch()
    {
        return Err(SkeinError::Storage(
            "search generation update import provenance does not match the active generation"
                .to_string(),
        ));
    }
    if options.embedding_manifest.is_some()
        && options.embedding_manifest != reader.embedding_manifest()
    {
        return Err(SkeinError::Storage(
            "search generation update embedding identity does not match the active generation"
                .to_string(),
        ));
    }
    if options.analyzer_lexicon != *reader.analyzer_lexicon() {
        return Err(SkeinError::Storage(
            "search generation update analyzer does not match the active generation".to_string(),
        ));
    }
    options.source_graph_commit_epoch = source_graph_commit_epoch;
    options.import_source_graph_commit_epoch = reader.import_source_graph_commit_epoch();
    options.embedding_manifest = reader.embedding_manifest();
    Ok(())
}

pub(super) fn validate_delta_ids(upserts: &[SearchDocument], deletes: &[String]) -> Result<()> {
    if upserts.iter().any(|document| document.id.is_empty()) || deletes.iter().any(String::is_empty)
    {
        return Err(SkeinError::Storage(
            "search generation delta document ids must not be empty".to_string(),
        ));
    }
    if upserts.windows(2).any(|pair| pair[0].id == pair[1].id) {
        return Err(SkeinError::Storage(
            "search generation delta contains duplicate upsert ids".to_string(),
        ));
    }
    if deletes.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(SkeinError::Storage(
            "search generation delta contains duplicate delete ids".to_string(),
        ));
    }
    let mut upsert_index = 0usize;
    let mut delete_index = 0usize;
    while upsert_index < upserts.len() && delete_index < deletes.len() {
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
