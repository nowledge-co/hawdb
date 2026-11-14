// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Resident checkpoints use the same immutable candidate slot and identity recipe.

use super::*;
use hawdb_storage::file_io::File;
use hawdb_vector_projection::diskann::{DiskAnnArtifactIdentity, FileDiskAnn};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

fn configuration(index: &SearchIndex) -> Option<(DiskAnnBuildConfig, VectorDistance)> {
    if let Some(definition) = &index.vector_index_descriptor {
        return crate::vector_index::diskann_build_config(definition.backend(), CACHE_BYTES)
            .map(|config| (config, definition.distance()));
    }
    index
        .rabitq_checkpoint
        .is_none()
        .then_some((DiskAnnBuildConfig::default(), index.vector_metric()))
}

fn shape(
    index: &SearchIndex,
    metric: VectorDistance,
    task: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<Option<(usize, usize)>> {
    let Some(dimension) = index.embedding_dimension else {
        return Ok(None);
    };
    let mut count = 0usize;
    for row in index.documents.values() {
        checkpoint(task)?;
        if let Some(vector) = &row.embedding {
            if vector.len() != dimension {
                return Err(HawDBError::Execution(
                    "invalid resident graph source vector".into(),
                ));
            }
            let mut nonzero = false;
            for chunk in vector.chunks(4096) {
                checkpoint(task)?;
                if chunk.iter().any(|value| !value.is_finite()) {
                    return Err(non_finite_source());
                }
                nonzero |= chunk.iter().any(|value| *value != 0.0);
            }
            if metric == VectorDistance::Cosine && !nonzero {
                // Legacy unbound zero cosine content keeps its compatible codec.
                return Ok(None);
            }
            count += 1;
        }
    }
    Ok((count != 0).then_some((dimension, count)))
}

fn identity(
    index: &SearchIndex,
    config: DiskAnnBuildConfig,
    dimension: usize,
    generation: u64,
    task: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<DiskAnnArtifactIdentity> {
    let _definition_memory = index
        .vector_index_descriptor
        .as_ref()
        .map(|_| {
            reserve(
                task,
                hawdb_core::vector_index::MAX_VECTOR_INDEX_DESCRIPTOR_BYTES,
            )
        })
        .transpose()?;
    let (mut digest, binding_digest) = crate::diskann_identity::source(
        index.embedding_manifest.as_ref(),
        index.vector_index_descriptor.as_ref(),
        index.vector_scoring,
        dimension,
        config,
        task,
    )?;
    for (ordinal, vector) in index
        .documents
        .values()
        .filter_map(|row| row.embedding.as_deref())
        .enumerate()
    {
        crate::diskann_identity::push(&mut digest, ordinal as u64, vector, task)?;
    }
    Ok(DiskAnnArtifactIdentity {
        source_digest: *digest.finish().sha256.as_bytes(),
        binding_digest,
        source_epoch: index.source_graph_commit_epoch.unwrap_or(0),
        generation,
    })
}

pub(crate) fn write(
    index: &SearchIndex,
    path: &Path,
    generation: u64,
    task: &hawdb_core::RuntimeTaskContext,
    publication: &crate::out_of_core::SearchProjectionPublishLease,
) -> Result<Option<crate::out_of_core::ResidentRaBitQArtifact>> {
    let Some((config, metric)) = configuration(index) else {
        return Ok(None);
    };
    let Some((dimension, count)) = shape(index, metric, Some(task))? else {
        return Ok(None);
    };
    // An unbound cache is optional. Decide before creating the candidate file:
    // the existing streaming codec must own that same slot on cache refusal.
    // Bound descriptors still require their declared graph and typed admission.
    if index.vector_index_descriptor.is_none()
        && !index
            .resident_diskann
            .admits_complete_cache(index, dimension, Some(task))?
    {
        return Ok(None);
    }
    let identity = identity(index, config, dimension, generation, Some(task))?;
    let options = crate::VectorSearchExecutionOptions::admitted(CACHE_BYTES, task);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            HawDBError::StorageIntegrity("invalid resident graph artifact name".into())
        })?;
    let mut output = publication.with_captured_directory(|directory| {
        directory.create_child_new(&directory.operation_context(), name)
    })?;
    let peak;
    if index.vector_index_descriptor.is_none() {
        let projection = index
            .resident_diskann
            .complete_snapshot(index, dimension, options)?;
        projection
            .graph
            .write_artifact(&mut output, identity, Some(task))
            .map_err(projection_error)?;
        peak = projection.memory_bytes;
    } else {
        peak =
            DiskAnnIndex::build_memory_bound(count, dimension, config).map_err(projection_error)?;
        let _memory = reserve(Some(task), peak)?;
        let mut entries = Vec::with_capacity(count);
        for row in index.documents.values() {
            checkpoint(Some(task))?;
            if let Some(vector) = &row.embedding {
                let mut copied = Vec::with_capacity(dimension);
                for chunk in vector.chunks(4096) {
                    checkpoint(Some(task))?;
                    copied.extend_from_slice(chunk);
                }
                entries.push((entries.len() as u64, copied));
            }
        }
        let graph = DiskAnnIndex::build(dimension, metric, entries, config, Some(task))
            .map_err(projection_error)?;
        graph
            .write_artifact(&mut output, identity, Some(task))
            .map_err(projection_error)?;
    }
    output.flush()?;
    output.sync_all()?;
    let (reference, checksum) = crate::out_of_core::seal_resident_candidate(&mut output, task)?;
    Ok(Some(crate::out_of_core::ResidentRaBitQArtifact {
        reference,
        checksum,
        source_digest: u64::from_le_bytes(
            identity.source_digest[..8]
                .try_into()
                .expect("digest prefix"),
        ),
        vector_document_count: count,
        payload_checksum: checksum as u32,
        peak_build_working_bytes: peak,
        diskann_source_digest: Some(identity.source_digest),
    }))
}

pub(crate) fn validate_file(index: &SearchIndex, path: &Path, generation: u64) -> Result<Restore> {
    validate_open_file(index, &mut File::open(path)?, generation)
}

pub(crate) fn validate_open_file(
    index: &SearchIndex,
    file: &mut File,
    generation: u64,
) -> Result<Restore> {
    let mut magic = [0; 8];
    file.seek(SeekFrom::Start(0))?;
    if file.metadata()?.len() < magic.len() as u64 {
        return Ok(Restore::Other);
    }
    file.read_exact(&mut magic)?;
    file.seek(SeekFrom::Start(0))?;
    if &magic != b"HDANN002" {
        return Ok(Restore::Other);
    }
    let Some((config, metric)) = configuration(index) else {
        return Ok(Restore::Skip);
    };
    let Some((dimension, count)) = shape(index, metric, None)? else {
        return Ok(Restore::Skip);
    };
    let expected = identity(index, config, dimension, generation, None)?;
    let declared = FileDiskAnn::inspect_file(file).map_err(restore_error)?;
    if declared.identity != expected {
        return Ok(Restore::Skip);
    }
    let graph = FileDiskAnn::open_file(file.try_clone()?, expected).map_err(restore_error)?;
    let metadata = graph.metadata();
    if graph.len() != count
        || graph.dimension() != dimension
        || graph.metric() != metric
        || metadata.max_degree != config.max_degree
        || metadata.build_beam != config.build_beam
        || metadata.alpha != config.alpha
        || metadata.lsg != config.lsg
    {
        return Err(HawDBError::StorageIntegrity(
            "graph artifact parameters differ from the resident source".into(),
        ));
    }
    if index.vector_index_descriptor.is_none() {
        index.resident_diskann.restore(index, graph)?;
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(Restore::Ready)
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum Restore {
    Other,
    Skip,
    Ready,
}

fn restore_error(error: ProjectionError) -> HawDBError {
    match error {
        ProjectionError::CorruptArtifact(message)
        | ProjectionError::InvalidConfiguration(message)
        | ProjectionError::InvalidVector(message) => {
            HawDBError::StorageIntegrity(format!("resident graph: {message}"))
        }
        ProjectionError::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData
            ) =>
        {
            HawDBError::StorageIntegrity(format!("resident graph: {error}"))
        }
        error => projection_error(error),
    }
}
