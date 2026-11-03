// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One source-identity recipe for streaming and resident graph artifacts.

use crate::{Result, SearchEmbeddingManifest};
use hawdb_core::{RuntimeTaskContext, VectorIndexDescriptor};
use hawdb_integrity::IntegrityHasher;
use hawdb_vector_projection::diskann::DiskAnnBuildConfig;

pub(crate) fn embedding(
    model: Option<&str>,
    version: Option<&str>,
    dimension: Option<usize>,
    task: Option<&RuntimeTaskContext>,
) -> Result<[u8; 32]> {
    let mut digest = IntegrityHasher::new();
    digest.update(b"hawdb-diskann-embedding-v1");
    for value in [model, version] {
        digest.update(&[u8::from(value.is_some())]);
        if let Some(value) = value {
            digest.update(&(value.len() as u64).to_le_bytes());
            for chunk in value.as_bytes().chunks(4096) {
                checkpoint(task)?;
                digest.update(chunk);
            }
        }
    }
    digest.update(&(dimension.unwrap_or(0) as u64).to_le_bytes());
    Ok(*digest.finish().sha256.as_bytes())
}

pub(crate) fn source(
    manifest: Option<&SearchEmbeddingManifest>,
    definition: Option<&VectorIndexDescriptor>,
    scoring: crate::SearchVectorScoring,
    dimension: usize,
    config: DiskAnnBuildConfig,
    task: Option<&RuntimeTaskContext>,
) -> Result<(IntegrityHasher, [u8; 32])> {
    let binding = embedding(
        manifest.map(|value| value.model.as_str()),
        manifest.and_then(|value| value.version.as_deref()),
        Some(dimension),
        task,
    )?;
    let mut digest = IntegrityHasher::new();
    digest.update(b"hawdb-diskann-source-v1");
    digest.update(&binding);
    if let Some(definition) = definition {
        digest.update(&definition.encode());
    } else {
        digest.update(if scoring.is_default() {
            b"unbound-cosine"
        } else {
            b"unbound-linear-squared-l2"
        });
        digest.update(&(dimension as u64).to_le_bytes());
        digest.update(&(config.max_degree as u64).to_le_bytes());
        digest.update(&(config.build_beam as u64).to_le_bytes());
        digest.update(&config.alpha.to_le_bytes());
        digest.update(&[u8::from(config.lsg.is_some())]);
        if let Some(lsg) = config.lsg {
            digest.update(&(lsg.sample_size as u64).to_le_bytes());
            digest.update(&(lsg.neighbors as u64).to_le_bytes());
            digest.update(&lsg.exponent.to_le_bytes());
        }
    }
    Ok((digest, binding))
}

pub(crate) fn push(
    digest: &mut IntegrityHasher,
    ordinal: u64,
    vector: &[f32],
    task: Option<&RuntimeTaskContext>,
) -> Result<()> {
    digest.update(&ordinal.to_le_bytes());
    for chunk in vector.chunks(4096) {
        checkpoint(task)?;
        for coordinate in chunk {
            digest.update(&coordinate.to_le_bytes());
        }
    }
    Ok(())
}

fn checkpoint(task: Option<&RuntimeTaskContext>) -> Result<()> {
    task.map_or(Ok(()), crate::build_control::checkpoint)
}
