// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One admitted private Vamana build, sealed by the existing publication owner.

use super::{
    artifact_name::Name, context_memory::OwnedPath, io::GenerationIo,
    SearchOutOfCoreGenerationWriter, VectorGenerationArtifact,
};
use crate::build_control::checkpoint;
use crate::build_memory::{BuildMemory, SPOOL_BUFFER_BYTES};
use crate::{HawDBError, Result};
use hawdb_core::{RuntimeTaskContext, VectorDistance};
use hawdb_executor::QueryMemoryLease;
use hawdb_integrity::IntegrityHasher;
use hawdb_storage::file_io::OpenOptions;
use hawdb_vector_projection::diskann::{DiskAnnArtifactIdentity, DiskAnnBuildConfig, DiskAnnIndex};
use std::io::{BufWriter, Write};

pub(super) struct DiskAnnArtifactBuilder {
    entries: Vec<(u64, Vec<f32>)>,
    digest: IntegrityHasher,
    file_name: Name,
    path: OwnedPath,
    config: DiskAnnBuildConfig,
    dimension: usize,
    binding_digest: [u8; 32],
    metric: VectorDistance,
    expected_documents: usize,
    generation: u64,
    source_epoch: u64,
    task: RuntimeTaskContext,
    memory: BuildMemory,
    peak_bytes: usize,
    // Keep the complete source and construction envelope until all data drops.
    _memory: QueryMemoryLease,
}

impl DiskAnnArtifactBuilder {
    pub(super) fn new(input: &SearchOutOfCoreGenerationWriter, generation: u64) -> Result<Self> {
        checkpoint(&input.task_context)?;
        let definition = input.options.vector_index_descriptor.as_ref();
        let mut config = match definition {
            Some(definition) => crate::vector_index::diskann_build_config(
                definition.backend(),
                input.options.rabitq_build_memory_bytes.get(),
            ),
            None => input.options.vector_backend.and_then(|backend| {
                crate::vector_index::diskann_build_config(
                    backend,
                    input.options.rabitq_build_memory_bytes.get(),
                )
            }),
        }
        .ok_or_else(|| HawDBError::Semantic("DiskANN builder has a different backend".into()))?;
        config.max_working_bytes = config
            .max_working_bytes
            .min(input.options.rabitq_build_memory_bytes.get());
        let dimension = input.embedding_dimension.ok_or_else(|| {
            HawDBError::StorageIntegrity("DiskANN source has no embedding dimension".into())
        })?;
        let metric = input.options.vector_scoring.metric(definition);
        let peak_bytes =
            DiskAnnIndex::build_memory_bound(input.vector_document_count, dimension, config)
                .map_err(error)?;
        let lease = input.memory.retained.reserve(peak_bytes)?;
        let file_name = Name::rabitq(generation, &input.memory, &input.task_context)?;
        let path = OwnedPath::join(
            &input.stage.path,
            file_name.as_ref(),
            &input.memory,
            &input.task_context,
        )?;
        let _definition_memory = input
            .memory
            .spool
            .reserve(hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES)?;
        let (digest, binding_digest) = crate::diskann_identity::source(
            input.options.embedding_manifest.as_ref(),
            definition,
            input.options.vector_scoring,
            dimension,
            config,
            Some(&input.task_context),
        )?;
        Ok(Self {
            entries: Vec::with_capacity(input.vector_document_count),
            digest,
            file_name,
            path,
            config,
            dimension,
            binding_digest,
            metric,
            expected_documents: input.vector_document_count,
            generation,
            source_epoch: input.options.source_graph_commit_epoch.unwrap_or(0),
            task: input.task_context.clone(),
            memory: input.memory.clone(),
            peak_bytes,
            _memory: lease,
        })
    }

    pub(super) fn push_embedding(&mut self, embedding: Option<&[f32]>) -> Result<()> {
        checkpoint(&self.task)?;
        let Some(embedding) = embedding else {
            return Ok(());
        };
        if self.entries.len() == self.expected_documents || embedding.len() != self.dimension {
            return Err(HawDBError::StorageIntegrity(
                "DiskANN source count or dimension differs from preflight".into(),
            ));
        }
        let ordinal = self.entries.len() as u64;
        crate::diskann_identity::push(&mut self.digest, ordinal, embedding, Some(&self.task))?;
        self.entries.push((ordinal, embedding.to_vec()));
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<VectorGenerationArtifact>> {
        checkpoint(&self.task)?;
        if self.entries.len() != self.expected_documents {
            return Err(HawDBError::StorageIntegrity(
                "DiskANN source ended before its admitted count".into(),
            ));
        }
        if self.expected_documents == 0 {
            return Ok(None);
        }
        let source_digest = *self.digest.finish().sha256.as_bytes();
        let index = DiskAnnIndex::build(
            self.dimension,
            self.metric,
            self.entries,
            self.config,
            Some(&self.task),
        )
        .map_err(error)?;
        let io = GenerationIo::new(&self.memory, &self.task);
        let mut file = io.native(&[&self.path], || {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.path)
        })??;
        let _buffer_memory = self.memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        {
            let mut output = BufWriter::with_capacity(SPOOL_BUFFER_BYTES, &mut file);
            index
                .write_artifact(
                    &mut output,
                    DiskAnnArtifactIdentity {
                        source_digest,
                        binding_digest: self.binding_digest,
                        source_epoch: self.source_epoch,
                        generation: self.generation,
                    },
                    Some(&self.task),
                )
                .map_err(error)?;
            output.flush()?;
        }
        checkpoint(&self.task)?;
        file.sync_all()?;
        drop(file);
        let (artifact_bytes, artifact_checksum) =
            super::publication::file_len_checksum_with_context(
                &self.path,
                &self.memory,
                &self.task,
            )?;
        Ok(Some(VectorGenerationArtifact {
            file_name: self.file_name,
            artifact_bytes,
            artifact_checksum,
            source_digest: u64::from_le_bytes(
                source_digest[..8].try_into().expect("source digest prefix"),
            ),
            document_count: self.expected_documents,
            payload_checksum: artifact_checksum as u32,
            peak_build_working_bytes: self.peak_bytes,
            diskann_source_digest: Some(source_digest),
        }))
    }
}

fn error(error: hawdb_vector_projection::ProjectionError) -> HawDBError {
    match error {
        hawdb_vector_projection::ProjectionError::ResourceBudgetExceeded {
            required,
            available,
        } => hawdb_core::ResourceBudgetError {
            resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
            scope: hawdb_core::ResourceBudgetScope::Operator,
            owner: "DiskANN construction".into(),
            memory_class: None,
            requested: required as u64,
            used: 0,
            budget: available as u64,
            budget_option: "SearchOutOfCoreGenerationBuildOptions.rabitq_build_memory_bytes",
            message: "DiskANN construction exceeds its admitted vector working memory".into(),
        }
        .into(),
        error => crate::rabitq_projection::map_projection_error(error, "search DiskANN graph"),
    }
}
