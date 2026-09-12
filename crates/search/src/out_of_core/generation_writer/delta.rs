use super::{SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter};
use crate::error::{Result, SkeinError};
use crate::{
    SearchDocument, SearchOutOfCoreGenerationBuildReport, SearchOutOfCoreMetrics,
    SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionDeltaReport,
};
use std::collections::VecDeque;

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
        mut options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<Self> {
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
        let delta_working_bytes = delta_working_bytes(&delta);
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
        bind_identity(reader, &mut options, source_graph_commit_epoch_after)?;

        let SearchProjectionDelta {
            upserts,
            mut deletes,
            max_operations: _,
            source_graph_commit_epoch,
        } = delta;
        let mut upserts = upserts
            .into_iter()
            .map(|row| row.into_document())
            .collect::<Vec<_>>();
        upserts.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        deletes.sort_unstable();
        validate_delta_ids(&upserts, &deletes)?;

        let before_document_count = reader.document_count();
        let upserted_documents = upserts.len();
        let mut upserts = VecDeque::from(upserts);
        let mut deletes = VecDeque::from(deletes);
        // A prepared update retains this snapshot even if the reader is later
        // reconfigured before the staged generation is finalized.
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_term_policy(
            &reader.root,
            options,
            reader.lexical_term_policy(),
        )?;
        writer.set_max_lexical_manifest_bytes(reader.config().max_lexical_manifest_bytes)?;
        writer.expected_active_generation = Some(reader.generation());
        let mut deleted_documents = 0usize;
        let source_read_metrics = reader.visit_documents_in_order(&mut |document| {
            while upserts
                .front()
                .is_some_and(|upsert| upsert.id < document.id)
            {
                writer.push(upserts.pop_front().expect("front was present"))?;
            }
            while deletes
                .front()
                .is_some_and(|deleted| deleted < &document.id)
            {
                deletes.pop_front();
            }
            if let Some(upsert) = upserts.pop_front_if(|upsert| upsert.id == document.id) {
                writer.push(upsert)?;
                return Ok(());
            }
            if deletes
                .pop_front_if(|deleted| deleted == &document.id)
                .is_some()
            {
                deleted_documents = deleted_documents.saturating_add(1);
                return Ok(());
            }
            writer.push(document)
        })?;
        for document in upserts {
            writer.push(document)?;
        }

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

fn validate_delta_ids(upserts: &[SearchDocument], deletes: &[String]) -> Result<()> {
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

fn delta_working_bytes(delta: &SearchProjectionDelta) -> u64 {
    let upsert_bytes = delta.upserts.iter().fold(0u64, |total, row| {
        let embedding_bytes = row.embedding.as_ref().map_or(0u64, |embedding| {
            (embedding.len() as u64).saturating_mul(std::mem::size_of::<f32>() as u64)
        });
        let metadata_bytes = row.metadata.iter().fold(0u64, |bytes, (name, value)| {
            bytes
                .saturating_add(name.len() as u64)
                .saturating_add(value.len() as u64)
        });
        total
            .saturating_add(std::mem::size_of::<crate::SearchProjectionRow>() as u64)
            .saturating_add(row.external_id.len() as u64)
            .saturating_add(row.title.len() as u64)
            .saturating_add(row.body.len() as u64)
            .saturating_add(row.source_id.as_ref().map_or(0, |value| value.len() as u64))
            .saturating_add(embedding_bytes)
            .saturating_add(metadata_bytes)
    });
    delta.deletes.iter().fold(upsert_bytes, |total, id| {
        total
            .saturating_add(std::mem::size_of::<String>() as u64)
            .saturating_add(id.len() as u64)
    })
}
