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
use crate::build_memory::{checked_mul, BuildMemory};
use crate::document_encoding::DocumentEncoding;
use crate::lexical_projection::{analyzer_digest, DocumentsDigest};
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
    _memory: QueryMemoryLease,
}

impl Prepared {
    pub(super) fn prepare(
        reader: &SearchOutOfCoreReader,
        input: &input::Input,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<(Self, usize, SearchOutOfCoreMetrics)> {
        let count = input
            .upserts
            .len()
            .checked_add(input.deletes.len())
            .ok_or_else(|| HawDBError::Storage("mutation operation count overflows".into()))?;
        let mut retained = memory.retained.reserve(checked_mul(
            count,
            std::mem::size_of::<SearchMutationRunEntry>(),
        )?)?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(count).map_err(|error| {
            HawDBError::Execution(format!("cannot allocate mutation entries: {error}"))
        })?;
        if entries.capacity() > count {
            return Err(HawDBError::Execution(
                "mutation entries exceed admission".into(),
            ));
        }
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
            let evidence = hydration::visit_target(
                reader,
                id,
                memory,
                task,
                &mut |target_segment_id, lexical, document| {
                    let (lexical_document_len, unique_terms) = lexical
                        .document_retraction_with_context(
                            &document,
                            reader.analyzer_lexicon(),
                            memory,
                            task,
                            &mut retained,
                        )?;
                    retained.grow(id.len())?;
                    let document_id = id.to_string();
                    if document_id.capacity() > id.len() {
                        return Err(HawDBError::Execution(
                            "mutation ID exceeds admission".into(),
                        ));
                    }
                    let encoding = DocumentEncoding::new_with_context(&document, Some(task))?;
                    let mut digest = DocumentsDigest::default();
                    super::super::spool::write_frame_with_context(
                        &mut std::io::sink(),
                        &encoding,
                        &mut digest,
                        memory,
                        task,
                    )?;
                    entries.push(SearchMutationRunEntry {
                        document_id,
                        target_segment_id,
                        operation,
                        retraction: SearchMutationRetraction {
                            documents_digest: digest.finish(),
                            lexical_document_len,
                            unique_terms,
                        },
                    });
                    if operation == SearchMutationOperation::Delete {
                        deleted += 1;
                    }
                    Ok(())
                },
            )?;
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
        let segments = reader
            .segments
            .len()
            .checked_add(usize::from(!input.upserts.is_empty()))
            .ok_or_else(|| HawDBError::Storage("mutation content count overflows".into()))?;
        let reopen_budget = reader.visibility.publication_budget(
            reader.config.max_mutation_working_bytes.get(),
            segments,
            !entries.is_empty(),
        )?;
        Ok((
            Self {
                entries,
                analyzer_digest: analyzer_digest(reader.analyzer_lexicon()),
                max_run_bytes: reader.config.max_mutation_run_bytes.get(),
                reopen_budget,
                _memory: retained,
            },
            deleted,
            metrics,
        ))
    }
}
