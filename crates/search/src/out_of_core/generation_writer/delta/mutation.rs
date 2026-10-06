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

//! Target-bound retractions owned by one unpublished delta operation.

use super::{hydration, input};
use crate::build_control::checkpoint;
use crate::build_memory::{reserve_capacity, shared::Shared, BuildMemory};
use crate::lexical_projection::analyzer_digest;
use crate::out_of_core::mutation_run::terms;
use crate::out_of_core::mutation_run::{
    SearchMutationOperation, SearchMutationRetraction, SearchMutationRunEntry,
};
use crate::{HawDBError, Result, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;

pub(in crate::out_of_core::generation_writer) struct Prepared {
    pub(in crate::out_of_core::generation_writer) entries: Vec<SearchMutationRunEntry>,
    pub(in crate::out_of_core::generation_writer) analyzer_digest: u64,
    pub(in crate::out_of_core::generation_writer) max_run_bytes: u64,
    pub(in crate::out_of_core::generation_writer) reopen_budget:
        crate::out_of_core::mutation_run::MutationRunBudget,
    term_file: Shared<terms::TermFile>,
    _memory: QueryMemoryLease,
}

impl Prepared {
    pub(in crate::out_of_core::generation_writer) fn disk_reservation(
        reader: &SearchOutOfCoreReader,
    ) -> Result<u64> {
        // One reused old-body file, one bounded term file, and the reanalysis
        // spill overlap remain owned independently of the new-document spool.
        reader
            .config
            .max_uncompressed_segment_bytes
            .get()
            .checked_add(reader.config.max_mutation_run_bytes.get())
            .and_then(|bytes| bytes.checked_add(reader.config.max_reanalysis_spill_bytes.get()))
            .ok_or_else(|| HawDBError::Storage("mutation stage disk reservation overflow".into()))
    }

    pub(super) fn prepare(
        reader: &SearchOutOfCoreReader,
        input: &input::Input,
        stage: &std::path::Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<(Self, usize, SearchOutOfCoreMetrics)> {
        let mut prepared = Self::new(reader, stage, memory, task, !input.upserts.is_empty())?;
        let mut upserts = input.upserts.iter().peekable();
        let mut deletes = input.deletes.iter().peekable();
        let mut deleted = 0usize;
        let mut metrics = SearchOutOfCoreMetrics::default();
        loop {
            checkpoint(task)?;
            let (id, operation) = match (upserts.peek(), deletes.peek()) {
                (Some(upsert), Some(delete)) if upsert.id.as_str() < delete.as_str() => (
                    upserts.next().unwrap().id.as_str(),
                    SearchMutationOperation::Replace,
                ),
                (_, Some(_)) => (
                    deletes.next().unwrap().as_str(),
                    SearchMutationOperation::Delete,
                ),
                (Some(_), None) => (
                    upserts.next().unwrap().id.as_str(),
                    SearchMutationOperation::Replace,
                ),
                (None, None) => break,
            };
            let evidence = prepared.push_target(reader, id, operation, stage, memory, task)?;
            if operation == SearchMutationOperation::Delete {
                deleted += evidence.streamed_documents;
            }
            metrics.streamed_documents += evidence.streamed_documents;
            metrics.streamed_body_bytes = metrics
                .streamed_body_bytes
                .checked_add(evidence.streamed_body_bytes)
                .ok_or_else(|| HawDBError::Storage("mutation source bytes overflow".into()))?;
            metrics.segment_range_reads = metrics
                .segment_range_reads
                .saturating_add(evidence.segment_range_reads);
            metrics.segment_bytes_read = metrics
                .segment_bytes_read
                .saturating_add(evidence.segment_bytes_read);
            metrics.hydration_segment_bytes_read = metrics
                .hydration_segment_bytes_read
                .saturating_add(evidence.hydration_segment_bytes_read);
            metrics.lexical_document_bytes_read = metrics
                .lexical_document_bytes_read
                .saturating_add(evidence.lexical_document_bytes_read);
            metrics.peak_segment_document_bytes = metrics
                .peak_segment_document_bytes
                .max(evidence.peak_segment_document_bytes);
            metrics.hydrated_documents = metrics
                .hydrated_documents
                .saturating_add(evidence.hydrated_documents);
        }
        Ok((prepared, deleted, metrics))
    }

    pub(in crate::out_of_core::generation_writer) fn new(
        reader: &SearchOutOfCoreReader,
        stage: &std::path::Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
        has_upserts: bool,
    ) -> Result<Self> {
        let segments = reader
            .segments
            .len()
            .checked_add(usize::from(has_upserts))
            .ok_or_else(|| HawDBError::Storage("mutation content count overflows".into()))?;
        let reopen_budget = reader.visibility.publication_budget(
            reader.config.max_mutation_working_bytes.get(),
            segments,
            true,
        )?;
        let path = crate::build_memory::path::OwnedPath::join(
            stage,
            std::path::Path::new("mutation-terms.json"),
            memory,
            task,
        )?;
        let term_file = terms::TermFile::new(
            hawdb_storage::file_io::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&*path)?,
            Some(memory),
        )?;
        Ok(Self {
            entries: Vec::new(),
            analyzer_digest: analyzer_digest(reader.analyzer_lexicon()),
            max_run_bytes: reader.config.max_mutation_run_bytes.get(),
            reopen_budget,
            term_file,
            _memory: memory.retained.reserve(0)?,
        })
    }

    pub(in crate::out_of_core::generation_writer) fn push_target(
        &mut self,
        reader: &SearchOutOfCoreReader,
        id: &str,
        operation: SearchMutationOperation,
        stage: &std::path::Path,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<SearchOutOfCoreMetrics> {
        hydration::visit_target(
            reader,
            id,
            stage,
            memory,
            task,
            &mut |target_segment_id, lexical, document| {
                let mut term_writer = terms::Writer::new(
                    self.term_file.clone(),
                    memory,
                    reader.config.max_mutation_run_bytes.get(),
                )?;
                let lexical_document_len = lexical.source_retraction(
                    &document,
                    reader.analyzer_lexicon(),
                    crate::lexical_projection::retraction::RetractionContext {
                        root: stage,
                        memory,
                        task,
                        needs_chinese: document.needs_chinese,
                        source_policy: reader.lexical_source_policy(),
                    },
                    |term| term_writer.push(&term),
                )?;
                let unique_terms = term_writer.finish()?;
                let count = self
                    .entries
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| HawDBError::Storage("mutation entries overflow".into()))?;
                reserve_capacity(&mut self.entries, count, &mut self._memory)?;
                self._memory.grow(id.len())?;
                let document_id = id.to_string();
                if document_id.capacity() > id.len() {
                    return Err(HawDBError::Execution(
                        "mutation ID exceeds admission".into(),
                    ));
                }
                let documents_digest = document.documents_digest()?;
                self.entries.push(SearchMutationRunEntry {
                    document_id,
                    target_segment_id,
                    operation,
                    retraction: SearchMutationRetraction {
                        documents_digest,
                        lexical_document_len,
                        unique_terms,
                    },
                });
                Ok(())
            },
        )
    }
}
