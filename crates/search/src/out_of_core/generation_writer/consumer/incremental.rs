// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Only changed vectors enter construction. The completed native base retains
//! source hydration authority; a fresh private producer owns every new effect.

use super::*;
use crate::consumer::ConsumerProjection;
use crate::out_of_core::mutation_run::SearchMutationOperation;
use crate::vector_index::VectorDocumentSummary;

impl SearchOutOfCoreGenerationWriter {
    pub(in crate::out_of_core) fn write_consumer_incremental(
        reader: &crate::SearchOutOfCoreReader,
        index: &SearchIndex,
        previous: &ConsumerProjection,
        publication: &super::super::super::SearchProjectionPublishLease,
        task: RuntimeTaskContext,
        generation_bytes: NonZeroU64,
        permit: std::sync::Arc<hawdb_qos::RuntimePermit>,
    ) -> Result<crate::snapshot_writer::SearchSnapshotWriteReport> {
        let root = index
            .path
            .as_deref()
            .ok_or_else(|| crate::vector_index::invalid("incremental producer has no root"))?;
        let delta = index
            .canonical_vector_delta
            .as_ref()
            .ok_or_else(|| crate::vector_index::invalid("incremental producer has no delta"))?;
        let baseline = previous.canonical_vector_summary()?;
        if reader.source_graph_commit_epoch() != Some(delta.base_epoch)
            || index.canonical_vector_source_epoch != index.source_graph_commit_epoch
        {
            return Err(crate::vector_index::invalid(
                "incremental producer source epochs differ",
            ));
        }
        let mut memory = BuildMemory::new(&task)?;
        memory.host_admission = Some(permit);
        // Nested writer phases borrow this synchronous wave instead of waiting
        // for a second slot while the producer already holds its only slot.
        let scope = io_phase::Scope::acquire(&task, &memory)?;
        let task = scope.task();
        if !publication.matches_source_directory(root)? {
            return Err(crate::vector_index::invalid(
                "incremental producer changed native root",
            ));
        }
        let generation = reader
            .generation()
            .checked_add(1)
            .ok_or_else(|| crate::vector_index::invalid("vector generation overflow"))?;
        if discovery::active_identity(root, &memory, &task)?.is_some() {
            return Err(crate::vector_index::invalid(
                "incremental private producer is not empty",
            ));
        }
        let mut options = consumer_options(index, generation_bytes, &memory, &task)?;
        options.bind_delta_identity(reader, index.source_graph_commit_epoch)?;
        let operations = index
            .documents
            .len()
            .checked_add(delta.deletes.len())
            .ok_or_else(|| crate::vector_index::invalid("vector delta operation count overflow"))?;
        if operations > options.max_delta_operations.get() {
            return Err(hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MetadataEntries,
                scope: hawdb_core::ResourceBudgetScope::Operator,
                owner: "incremental vector producer".into(),
                memory_class: None,
                requested: operations as u64,
                used: 0,
                budget: options.max_delta_operations.get() as u64,
                budget_option: "SearchOutOfCoreGenerationBuildOptions.max_delta_operations",
                message: "vector delta exceeds operation admission".into(),
            }
            .into());
        }
        previous.seed_vector_producer(publication, &memory, &task, generation_bytes.get())?;
        let mut writer = Self::create_with_memory(root, options, task, memory)?;
        scope.install(&mut writer);
        writer.embedding_dimension = reader.manifest.embedding_dimension;
        writer.expected_active_generation = Some(reader.generation());
        writer.active_manifest_update = Some(ActiveManifestUpdate::Mutate {
            expected_generation: reader.generation(),
        });
        writer
            .stage
            .reserve_additional_disk(delta::mutation::Prepared::disk_reservation(reader)?)?;
        let mut mutations = delta::mutation::Prepared::new(
            reader,
            &writer.stage.path,
            &writer.memory,
            &writer.task_context,
            !index.documents.is_empty(),
        )?;
        let mut upserts = crate::DocumentView::keys(&index.documents).peekable();
        let mut deletes = delta.deletes.iter().peekable();
        while upserts.peek().is_some() || deletes.peek().is_some() {
            checkpoint(&writer.task_context)?;
            let (id, operation) = match (upserts.peek(), deletes.peek()) {
                (Some(upsert), Some(delete)) if upsert < delete => (
                    upserts.next().expect("peeked upsert"),
                    SearchMutationOperation::Replace,
                ),
                (_, Some(_)) => (
                    deletes.next().expect("peeked delete"),
                    SearchMutationOperation::Delete,
                ),
                (Some(_), None) => (
                    upserts.next().expect("peeked upsert"),
                    SearchMutationOperation::Replace,
                ),
                (None, None) => unreachable!("one mutation input remains"),
            };
            mutations.push_target(
                reader,
                id,
                operation,
                &writer.stage.path,
                &writer.memory,
                &writer.task_context,
            )?;
        }
        for document in index.documents.values() {
            checkpoint(&writer.task_context)?;
            let lease = writer
                .memory
                .input
                .reserve(crate::build_memory::document_bytes(document)?)?;
            writer.push_inner(crate::build_memory::AdmittedDocument::from_admitted_parts(
                document.clone(),
                lease,
            ))?;
        }
        let document_count = baseline
            .document_count
            .checked_sub(mutations.entries.len())
            .and_then(|count| count.checked_add(writer.document_count))
            .ok_or_else(|| crate::vector_index::invalid("incremental logical count overflow"))?;
        let mut digest = baseline.documents_digest;
        for entry in &mutations.entries {
            digest = DocumentsDigest::replace(digest, entry.retraction.documents_digest, 0);
        }
        let summary = VectorDocumentSummary {
            document_count,
            vector_document_count: document_count,
            documents_digest: DocumentsDigest::combine(digest, writer.documents_digest.finish()),
        };
        writer.mutations = Some(mutations);
        writer.seal_input_spool()?;
        let source = writer.input_source()?;
        let artifacts = writer.build_artifacts(&source, generation)?;
        drop(source);
        writer.held_spool.take();
        let snapshot = crate::snapshot_writer::write_search_snapshot_with_context(
            &root.join(crate::SEARCH_SNAPSHOT_FILE),
            crate::snapshot_writer::SearchSnapshotMetadata {
                source_graph_commit_epoch: index.source_graph_commit_epoch,
                import_source_graph_commit_epoch: index.import_source_graph_commit_epoch,
                embedding_manifest: index.embedding_manifest.as_ref(),
                embedding_dimension: index.embedding_dimension,
                consumer_binding: index.consumer_binding.as_ref(),
                vector_index_descriptor: index.vector_index_descriptor.as_ref(),
                vector_scoring: index.vector_scoring,
                vector_document_summary: Some(summary),
                canonical_vector_source_epoch: index.canonical_vector_source_epoch,
                rabitq_checkpoint: None,
            },
            std::iter::empty(),
            &writer.memory,
            &writer.task_context,
        )?;
        writer.resident_snapshot = Some(super::super::super::ResidentSnapshot {
            object: super::super::super::ArtifactObject::from_reference(snapshot.encoded_object)?,
            vector_document_count: summary.vector_document_count,
            metadata_only: true,
        });
        let published = writer.publish_artifacts(generation, true, artifacts, publication)?;
        if published.document_count != summary.document_count
            || published.documents_digest != summary.documents_digest
        {
            return Err(crate::vector_index::invalid(
                "incremental summary differs from sealed publication",
            ));
        }
        Ok(snapshot)
    }
}
