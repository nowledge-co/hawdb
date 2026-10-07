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

//! Ordered one-shot mutations share the existing target-bound publication path.

use super::*;
use crate::build_term::Term;
use crate::out_of_core::mutation_run::SearchMutationOperation;
use crate::{
    SearchDocumentBody, SearchDocumentHeader, SearchOutOfCoreMetrics, SearchOutOfCoreReader,
    SearchProjectionDeltaReport,
};
use std::io::Read;

/// An ordered mutation batch pinned to one reader generation.
///
/// Upserts and deletes share strictly increasing document IDs. A body is read
/// once, and any failed operation poisons the batch. Publication keeps the
/// existing expected-generation check and never overwrites a newer head.
pub struct SearchOutOfCoreMutationWriter<'reader> {
    reader: &'reader SearchOutOfCoreReader,
    writer: SearchOutOfCoreGenerationWriter,
    last_id: Option<Term>,
    operations: usize,
    deleted: usize,
    epoch_updated: bool,
    metrics: SearchOutOfCoreMetrics,
    _report_memory: QueryMemoryLease,
}

impl SearchOutOfCoreGenerationWriter {
    pub fn prepare_streamed_delta(
        reader: &SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
    ) -> Result<SearchOutOfCoreMutationWriter<'_>> {
        Self::prepare_streamed_delta_with_context(reader, options, RuntimeTaskContext::default())
    }

    /// Captures provenance from `options.source_graph_commit_epoch`, retaining
    /// the base reader's epoch when the caller does not supply a newer snapshot.
    pub fn prepare_streamed_delta_with_context(
        reader: &SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        task: RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreMutationWriter<'_>> {
        let memory = BuildMemory::new(&task)?;
        SearchOutOfCoreMutationWriter::create(reader, options, task, memory)
    }
}

impl<'reader> SearchOutOfCoreMutationWriter<'reader> {
    pub(super) fn create(
        reader: &'reader SearchOutOfCoreReader,
        options: SearchOutOfCoreGenerationBuildOptions,
        task: RuntimeTaskContext,
        memory: BuildMemory,
    ) -> Result<Self> {
        checkpoint(&task)?;
        let epoch_updated = options.source_graph_commit_epoch.is_some();
        let epoch = options
            .source_graph_commit_epoch
            .or(reader.source_graph_commit_epoch());
        let mut options = context_memory::Options::new(options, &memory, &task)?;
        options.bind_delta_identity(reader, epoch)?;
        let report_memory = memory
            .retained
            .reserve("search_projection".len() * 2 + "incremental_mutation_publish".len())?;
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            &reader.root,
            options,
            task,
            memory,
        )?;
        writer.set_lexical_term_policy(reader.lexical_term_policy());
        writer.set_lexical_source_policy(reader.lexical_source_policy());
        writer.set_max_lexical_manifest_bytes(reader.config.max_lexical_manifest_bytes)?;
        writer.embedding_dimension = reader.manifest.embedding_dimension;
        writer.expected_active_generation = Some(reader.generation());
        writer
            .stage
            .reserve_additional_disk(delta::mutation::Prepared::disk_reservation(reader)?)?;
        writer.mutations = Some(delta::mutation::Prepared::new(
            reader,
            &writer.stage.path,
            &writer.memory,
            &writer.task_context,
            true,
        )?);
        writer.active_manifest_update = Some(ActiveManifestUpdate::Mutate {
            expected_generation: reader.generation(),
        });
        Ok(Self {
            reader,
            writer,
            last_id: None,
            operations: 0,
            deleted: 0,
            epoch_updated,
            metrics: Default::default(),
            _report_memory: report_memory,
        })
    }

    pub fn upsert_reader(
        &mut self,
        header: SearchDocumentHeader,
        body: impl Read,
        source: SearchDocumentBody,
    ) -> Result<()> {
        let result = (|| {
            let header = self.writer.memory.admit_header(header)?;
            self.target(&header.id, SearchMutationOperation::Replace)?;
            self.writer.push_admitted_reader(header, body, source)
        })();
        if result.is_err() {
            self.writer.poisoned = true;
        }
        result
    }

    pub fn delete(&mut self, document_id: &str) -> Result<()> {
        let result = self.target(document_id, SearchMutationOperation::Delete);
        if result.is_err() {
            self.writer.poisoned = true;
        }
        result
    }

    fn target(&mut self, id: &str, operation: SearchMutationOperation) -> Result<()> {
        if self.writer.poisoned {
            return Err(HawDBError::Storage(
                "streamed mutation batch is poisoned".into(),
            ));
        }
        checkpoint(&self.writer.task_context)?;
        if id.is_empty()
            || self
                .last_id
                .as_ref()
                .is_some_and(|previous| &**previous >= id)
        {
            return Err(HawDBError::Storage(
                "streamed mutation IDs must be nonempty and strictly increasing".into(),
            ));
        }
        if id.len() > self.reader.config.max_document_header_bytes.get() {
            return Err(HawDBError::Storage(
                "mutation ID exceeds header admission".into(),
            ));
        }
        if self.operations >= self.writer.options.max_delta_operations.get() {
            return Err(HawDBError::Storage(
                "streamed mutation operation count exceeds admission".into(),
            ));
        }
        let id = Term::copy(id, Some(&self.writer.memory))?;
        let evidence = self
            .writer
            .mutations
            .as_mut()
            .expect("mutation writer owns a run")
            .push_target(
                self.reader,
                &id,
                operation,
                &self.writer.stage.path,
                &self.writer.memory,
                &self.writer.task_context,
            )?;
        if operation == SearchMutationOperation::Delete {
            self.deleted += evidence.streamed_documents;
        }
        self.metrics.streamed_documents += evidence.streamed_documents;
        self.metrics.streamed_body_bytes = self
            .metrics
            .streamed_body_bytes
            .checked_add(evidence.streamed_body_bytes)
            .ok_or_else(|| HawDBError::Storage("mutation source bytes overflow".into()))?;
        self.metrics.segment_range_reads += evidence.segment_range_reads;
        self.metrics.segment_bytes_read += evidence.segment_bytes_read;
        self.metrics.hydration_segment_bytes_read += evidence.hydration_segment_bytes_read;
        self.metrics.lexical_document_bytes_read += evidence.lexical_document_bytes_read;
        self.operations += 1;
        self.last_id = Some(id);
        Ok(())
    }

    pub fn finish(
        self,
    ) -> Result<(
        SearchProjectionDeltaReport,
        SearchOutOfCoreGenerationBuildReport,
        SearchOutOfCoreMetrics,
    )> {
        let retracted = self
            .writer
            .mutations
            .as_ref()
            .expect("mutation writer owns a run")
            .entries
            .len();
        let before = self.reader.document_count();
        let upserts = self.writer.document_count;
        let after = before
            .checked_sub(retracted)
            .and_then(|count| count.checked_add(upserts))
            .ok_or_else(|| HawDBError::Storage("mutation logical count overflows".into()))?;
        let epoch = self.writer.options.source_graph_commit_epoch;
        let report = SearchProjectionDeltaReport {
            artifact_type: "search_projection".into(),
            name: "search_projection".into(),
            action: "incremental_mutation_publish".into(),
            before_document_count: before,
            after_document_count: after,
            upserted_documents: upserts,
            deleted_documents: self.deleted,
            operation_count: self.operations,
            source_graph_commit_epoch_before: self.reader.source_graph_commit_epoch(),
            source_graph_commit_epoch_after: epoch,
            source_graph_commit_epoch_updated: self.epoch_updated,
        };
        let build = self.writer.finish()?;
        Ok((report, build, self.metrics))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::RuntimeMemoryReservation;

    #[test]
    fn streamed_upsert_charges_one_header_with_bounded_remaining_memory() {
        let root = super::super::tests::test_dir("single_mutation_header");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial
            .push(SearchDocument {
                id: "a".into(),
                title: String::new(),
                content: "small".into(),
                embedding: None,
                metadata: Default::default(),
            })
            .unwrap();
        initial.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(16 * 1024 * 1024, 0));
        let mut update = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
            &reader,
            Default::default(),
            task,
        )
        .unwrap();
        let memory = update.writer.memory.clone();
        let header = SearchDocumentHeader {
            id: "b".into(),
            title: " ".repeat(512 * 1024),
            embedding: None,
            metadata: Default::default(),
        };
        let admitted = memory.admit_header(header).unwrap();
        let header_bytes = admitted._memory.bytes();
        let header = admitted.header;
        drop(admitted._memory);
        let used = memory.ledger.snapshot().used_bytes;
        // One header plus bounded encoding/ID scratch fits; a second header
        // does not. The body is empty so the cap isolates header ownership.
        let held = memory
            .input
            .reserve(16 * 1024 * 1024 - used - header_bytes - 64 * 1024)
            .unwrap();
        update
            .upsert_reader(
                header,
                std::io::empty(),
                SearchDocumentBody {
                    bytes: 0,
                    expected_checksum: None,
                },
            )
            .unwrap();
        drop(held);
        update.finish().unwrap();
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().document_count(),
            2
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}
