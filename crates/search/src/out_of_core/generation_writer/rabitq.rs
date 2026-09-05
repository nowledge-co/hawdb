//! Incremental RaBitQ sink for the shared generation scan.

use super::{RaBitQGenerationArtifact, SearchOutOfCoreGenerationWriter};
use crate::error::Result;
use crate::SearchDocument;

#[cfg(feature = "vector-search")]
use super::{file_len_checksum, PathBuf, SkeinError};

#[cfg(feature = "vector-search")]
pub(super) struct RaBitQArtifactBuilder {
    writer: Option<skein_vector_projection::ProjectionWriter>,
    file_name: String,
    path: PathBuf,
    expected_documents: usize,
    vector_ordinal: u64,
}

#[cfg(feature = "vector-search")]
impl RaBitQArtifactBuilder {
    pub(super) fn new(input: &SearchOutOfCoreGenerationWriter, generation: u64) -> Result<Self> {
        let file_name = crate::rabitq_artifact_file(generation);
        let path = input.stage.path.join(&file_name);
        let writer = if input.vector_document_count == 0 {
            None
        } else {
            let dimension = input.embedding_dimension.ok_or_else(|| {
                SkeinError::Storage(
                    "search generation has vector documents without an embedding dimension"
                        .to_string(),
                )
            })?;
            let identity = skein_vector_projection::ProjectionIdentity {
                generation,
                source_epoch: input.options.source_graph_commit_epoch,
                embedding_model: input
                    .options
                    .embedding_manifest
                    .as_ref()
                    .map(|manifest| manifest.model.clone()),
                embedding_version: input
                    .options
                    .embedding_manifest
                    .as_ref()
                    .and_then(|manifest| manifest.version.clone()),
            };
            let config = skein_vector_projection::ProjectionBuildConfig::new(dimension, identity)
                .with_bit_width(input.options.rabitq_bit_width)
                .with_segment_rows(input.options.rabitq_segment_rows.get())
                .with_max_working_bytes(input.options.rabitq_build_memory_bytes.get())
                .with_transform_seed(input.options.rabitq_transform_seed);
            Some(
                skein_vector_projection::ProjectionWriter::create(&path, config)
                    .map_err(rabitq_error)?,
            )
        };
        Ok(Self {
            writer,
            file_name,
            path,
            expected_documents: input.vector_document_count,
            vector_ordinal: 0,
        })
    }

    pub(super) fn push(&mut self, document: &SearchDocument) -> Result<()> {
        let Some(embedding) = document.embedding.as_deref() else {
            return Ok(());
        };
        let writer = self.writer.as_mut().ok_or_else(|| {
            SkeinError::Storage(
                "search generation contains unexpected vector documents".to_string(),
            )
        })?;
        writer
            .push(self.vector_ordinal, embedding)
            .map_err(rabitq_error)?;
        self.vector_ordinal = self
            .vector_ordinal
            .checked_add(1)
            .ok_or_else(|| SkeinError::Storage("search vector ordinal overflow".to_string()))?;
        Ok(())
    }

    pub(super) fn finish(self) -> Result<Option<RaBitQGenerationArtifact>> {
        if self.vector_ordinal != self.expected_documents as u64 {
            return Err(SkeinError::Storage(
                "search RaBitQ build did not consume the expected vector document count"
                    .to_string(),
            ));
        }
        let Some(writer) = self.writer else {
            return Ok(None);
        };
        let projection = writer.finish().map_err(rabitq_error)?;
        let manifest = projection.manifest();
        let (artifact_bytes, artifact_checksum) = file_len_checksum(&self.path)?;
        Ok(Some(RaBitQGenerationArtifact {
            file_name: self.file_name,
            artifact_bytes,
            artifact_checksum,
            source_digest: manifest.source_digest,
            document_count: manifest.document_count,
            payload_checksum: manifest.payload_checksum,
            peak_build_working_bytes: manifest.peak_build_working_bytes,
        }))
    }
}

#[cfg(feature = "vector-search")]
fn rabitq_error(error: skein_vector_projection::ProjectionError) -> SkeinError {
    SkeinError::Storage(format!("search RaBitQ projection: {error}"))
}

#[cfg(not(feature = "vector-search"))]
pub(super) struct RaBitQArtifactBuilder;

#[cfg(not(feature = "vector-search"))]
impl RaBitQArtifactBuilder {
    pub(super) fn new(_: &SearchOutOfCoreGenerationWriter, _: u64) -> Result<Self> {
        Ok(Self)
    }
    pub(super) fn push(&mut self, _: &SearchDocument) -> Result<()> {
        Ok(())
    }
    pub(super) fn finish(self) -> Result<Option<RaBitQGenerationArtifact>> {
        Ok(None)
    }
}
