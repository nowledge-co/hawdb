use super::super::{SearchOutOfCoreLayoutBody, SearchOutOfCoreManifestBody, OUT_OF_CORE_FORMAT};
use super::RaBitQGenerationArtifact;
use crate::build_control::checkpoint;
use crate::build_io;
use crate::build_memory::{checked_add, BuildMemory};
use crate::error::{Result, SkeinError};
use crate::lexical_projection::MANIFEST_FILE as LEXICAL_MANIFEST_FILE;
use crate::{checksum_bytes, SearchEmbeddingManifest};
use skein_core::RuntimeTaskContext;
use skein_integrity::Crc32cHasher;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

mod paths;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) fn before_encoding_for_test(hook: impl FnOnce(&BuildMemory) + 'static) {
    tests::PREPARED.with_borrow_mut(|slot| {
        assert!(slot.is_none());
        *slot = Some(Box::new(move |memory, _| hook(memory)));
    });
}

pub(super) struct PublishGenerationInput<'a> {
    pub(super) task_context: &'a RuntimeTaskContext,
    pub(super) memory: &'a BuildMemory,
    pub(super) root: &'a Path,
    pub(super) stage: &'a Path,
    pub(super) generation: u64,
    pub(super) document_count: usize,
    pub(super) documents_digest: u64,
    pub(super) source_graph_commit_epoch: Option<u64>,
    pub(super) import_source_graph_commit_epoch: Option<u64>,
    pub(super) embedding_manifest: Option<&'a SearchEmbeddingManifest>,
    pub(super) embedding_dimension: Option<usize>,
    pub(super) layout: &'a SearchOutOfCoreLayoutBody,
    pub(super) lexical_artifact_name: &'a str,
    pub(super) rabitq: Option<&'a RaBitQGenerationArtifact>,
    pub(super) payload_bytes: u64,
    pub(super) metadata_payload_bytes: u64,
    pub(super) vector_payload_bytes: u64,
    pub(super) max_generation_bytes: u64,
}

pub(super) struct PublishedGeneration {
    pub(super) manifest_bytes: u64,
    pub(super) generation_bytes: u64,
}

pub(super) fn publish_generation(input: PublishGenerationInput<'_>) -> Result<PublishedGeneration> {
    checkpoint(input.task_context)?;
    #[cfg(test)]
    tests::run(&tests::BEFORE, &input);
    let generation = input.generation;
    let names = paths::Names::new(generation, input.memory, input.task_context)?;
    let paths = paths::Paths::new(&input, &names)?;
    #[cfg(test)]
    tests::run(&tests::PREPARED, &input);
    let (descriptor_len, descriptor_checksum) =
        file_len_checksum_with_context(&paths.descriptor.source, input.task_context)?;
    let payload_len = fs::metadata(&paths.payload.source)?.len();
    let metadata_payload_len = fs::metadata(&paths.metadata.source)?.len();
    let vector_payload_len = fs::metadata(&paths.vector.source)?.len();
    for (name, actual, expected) in [
        ("document", payload_len, input.payload_bytes),
        (
            "metadata",
            metadata_payload_len,
            input.metadata_payload_bytes,
        ),
        ("vector", vector_payload_len, input.vector_payload_bytes),
    ] {
        if actual != expected {
            return Err(SkeinError::Storage(format!(
                "staged search generation {name} payload length {actual} does not match {expected}"
            )));
        }
    }
    let (lexical_manifest_len, lexical_manifest_checksum) =
        file_len_checksum_with_context(&paths.lexical_manifest.source, input.task_context)?;
    let lexical_artifact_len = fs::metadata(&paths.lexical.source)?.len();
    let layout_bytes = build_io::json_envelope(
        input.layout,
        input.max_generation_bytes,
        input.memory,
        input.task_context,
    )
    .map_err(publication_encoding_error)?;
    let mut manifest_string_bytes = names
        .all()
        .iter()
        .try_fold(OUT_OF_CORE_FORMAT.len(), |bytes, name| {
            checked_add(bytes, name.len())
        })?;
    for text in [
        input.rabitq.map(|artifact| artifact.file_name.as_str()),
        input
            .embedding_manifest
            .map(|manifest| manifest.model.as_str()),
        input
            .embedding_manifest
            .and_then(|manifest| manifest.version.as_deref()),
    ]
    .into_iter()
    .flatten()
    {
        manifest_string_bytes = checked_add(manifest_string_bytes, text.len())?;
    }
    let _manifest_strings = input.memory.retained.reserve(manifest_string_bytes)?;
    let manifest = SearchOutOfCoreManifestBody {
        format: OUT_OF_CORE_FORMAT.to_string(),
        generation,
        descriptor_file: names.descriptor.clone(),
        descriptor_len,
        descriptor_checksum,
        payload_file: names.payload.clone(),
        payload_len,
        metadata_payload_file: names.metadata.clone(),
        metadata_payload_len,
        vector_payload_file: names.vector.clone(),
        vector_payload_len,
        layout_file: names.layout.clone(),
        layout_len: layout_bytes.len() as u64,
        layout_checksum: checksum_bytes(layout_bytes.as_ref()),
        lexical_manifest_file: names.lexical_manifest.clone(),
        lexical_manifest_len,
        lexical_manifest_checksum,
        rabitq_artifact_file: input.rabitq.map(|artifact| artifact.file_name.clone()),
        rabitq_artifact_len: input.rabitq.map(|artifact| artifact.artifact_bytes),
        rabitq_artifact_checksum: input.rabitq.map(|artifact| artifact.artifact_checksum),
        rabitq_source_digest: input.rabitq.map(|artifact| artifact.source_digest),
        rabitq_vector_document_count: input.rabitq.map(|artifact| artifact.document_count),
        rabitq_payload_checksum: input.rabitq.map(|artifact| artifact.payload_checksum),
        rabitq_peak_build_working_bytes: input
            .rabitq
            .map(|artifact| artifact.peak_build_working_bytes),
        document_count: input.document_count,
        documents_digest: input.documents_digest,
        source_graph_commit_epoch: input.source_graph_commit_epoch,
        import_source_graph_commit_epoch: input.import_source_graph_commit_epoch,
        embedding_model: input
            .embedding_manifest
            .map(|manifest| manifest.model.clone()),
        embedding_version: input
            .embedding_manifest
            .and_then(|manifest| manifest.version.clone()),
        embedding_dimension: input
            .embedding_manifest
            .map(|manifest| manifest.dimension)
            .or(input.embedding_dimension),
    };
    manifest.validate_names()?;
    let manifest_bytes = build_io::json_envelope(
        &manifest,
        input.max_generation_bytes,
        input.memory,
        input.task_context,
    )
    .map_err(publication_encoding_error)?;
    let generation_bytes = [
        descriptor_len,
        payload_len,
        metadata_payload_len,
        vector_payload_len,
        layout_bytes.len() as u64,
        lexical_manifest_len,
        lexical_artifact_len,
        manifest_bytes.len() as u64,
        input.rabitq.map_or(0, |artifact| artifact.artifact_bytes),
    ]
    .into_iter()
    .try_fold(0u64, |total, bytes| total.checked_add(bytes))
    .ok_or_else(|| SkeinError::Storage("search generation size overflow".to_string()))?;
    if generation_bytes > input.max_generation_bytes {
        return Err(SkeinError::Storage(format!(
            "search generation requires {generation_bytes} published bytes, exceeding {}",
            input.max_generation_bytes
        )));
    }

    // Cancellation before this point leaves the active manifest untouched. Once
    // publication begins, finish the manifest-last commit rather than reporting
    // a late cancellation for a generation that may already be authoritative.
    checkpoint(input.task_context)?;
    #[cfg(test)]
    late_cancellation::trigger();
    #[cfg(test)]
    tests::run(&tests::COMMITTED, &input);
    paths.descriptor.publish()?;
    paths.payload.publish()?;
    paths.metadata.publish()?;
    paths.vector.publish()?;
    paths.lexical.publish()?;
    if let Some(rabitq) = &paths.rabitq {
        rabitq.publish()?;
    }
    paths.lexical_manifest.publish()?;
    paths.layout.write(layout_bytes.as_ref())?;

    verify_published_artifact(
        &paths.descriptor.target.path,
        descriptor_len,
        Some(descriptor_checksum),
        "descriptor",
    )?;
    if let Some(rabitq) = input.rabitq {
        verify_published_artifact(
            &paths
                .rabitq
                .as_ref()
                .expect("prepared RaBitQ paths")
                .target
                .path,
            rabitq.artifact_bytes,
            Some(rabitq.artifact_checksum),
            "RaBitQ artifact",
        )?;
    }
    verify_published_artifact(
        &paths.payload.target.path,
        payload_len,
        None,
        "document payload",
    )?;
    verify_published_artifact(
        &paths.metadata.target.path,
        metadata_payload_len,
        None,
        "metadata payload",
    )?;
    verify_published_artifact(
        &paths.vector.target.path,
        vector_payload_len,
        None,
        "vector payload",
    )?;
    verify_published_artifact(
        &paths.lexical_manifest.target.path,
        lexical_manifest_len,
        Some(lexical_manifest_checksum),
        "lexical manifest",
    )?;
    verify_published_artifact(
        &paths.lexical.target.path,
        lexical_artifact_len,
        None,
        "lexical artifact",
    )?;

    paths.manifest.write(manifest_bytes.as_ref())?;
    Ok(PublishedGeneration {
        manifest_bytes: manifest_bytes.len() as u64,
        generation_bytes,
    })
}

fn publication_encoding_error(error: SkeinError) -> SkeinError {
    match error {
        SkeinError::Storage(message) => SkeinError::Storage(format!(
            "search generation published bytes encoding failed: {message}"
        )),
        // Preserve root-admission and cancellation errors as terminal failures.
        other => other,
    }
}

fn verify_published_artifact(
    path: &Path,
    expected_len: u64,
    expected_checksum: Option<u64>,
    name: &str,
) -> Result<()> {
    let actual_len = fs::metadata(path)?.len();
    let checksum_matches = match expected_checksum {
        Some(expected) => file_len_checksum(path)?.1 == expected,
        None => true,
    };
    if actual_len != expected_len || !checksum_matches {
        return Err(SkeinError::Storage(format!(
            "published search generation {name} does not match its staged artifact"
        )));
    }
    Ok(())
}

pub(super) fn file_len_checksum(path: &Path) -> Result<(u64, u64)> {
    file_len_checksum_with_context(path, &RuntimeTaskContext::default())
}

pub(super) fn file_len_checksum_with_context(
    path: &Path,
    task_context: &RuntimeTaskContext,
) -> Result<(u64, u64)> {
    checkpoint(task_context)?;
    let mut file = File::open(path)?;
    let expected_len = file.metadata()?.len();
    let mut checksum = Crc32cHasher::new();
    let mut actual_len = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        checkpoint(task_context)?;
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        checksum.update(&buffer[..read]);
        actual_len = actual_len.saturating_add(read as u64);
    }
    if actual_len != expected_len {
        return Err(SkeinError::Storage(format!(
            "search generation artifact {} changed while checksumming",
            path.display()
        )));
    }
    Ok((actual_len, checksum.finish()))
}

#[cfg(test)]
pub(super) mod late_cancellation {
    use crate::RuntimeCancellationToken;
    use std::cell::RefCell;
    thread_local! {
        static TOKEN: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
    }
    pub(in super::super) struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            TOKEN.with(|slot| {
                slot.borrow_mut().take();
            });
        }
    }
    pub(in super::super) fn arm(token: RuntimeCancellationToken) -> Guard {
        TOKEN.with(|slot| *slot.borrow_mut() = Some(token));
        Guard
    }
    pub(super) fn trigger() {
        TOKEN.with(|slot| {
            if let Some(token) = slot.borrow_mut().take() {
                token.cancel();
            }
        });
    }
}
