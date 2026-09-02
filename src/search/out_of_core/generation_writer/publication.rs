use super::super::{
    publish_generation_link, write_generation_artifact, SearchOutOfCoreLayoutBody,
    SearchOutOfCoreManifestBody, OUT_OF_CORE_FORMAT, OUT_OF_CORE_MANIFEST_FILE,
};
use super::{RaBitQGenerationArtifact, STAGE_METADATA_FILE, STAGE_VECTOR_FILE};
use crate::error::{Result, SkeinError};
use crate::search::lexical_projection::MANIFEST_FILE as LEXICAL_MANIFEST_FILE;
use crate::search::{
    checksum_bytes, SearchEmbeddingManifest, SEARCH_SEGMENT_DESCRIPTOR_FILE,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use skein_integrity::Crc32cHasher;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

pub(super) struct PublishGenerationInput<'a> {
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
    let generation = input.generation;
    let descriptor_file = format!("search_projection_segments.{generation}.skein");
    let payload_file = format!("search_projection_segment_payloads.{generation}.skein");
    let metadata_payload_file = format!("search_projection_metadata_payloads.{generation}.skein");
    let vector_payload_file = format!("search_projection_vector_payloads.{generation}.skein");
    let layout_file = format!("search_projection_out_of_core_layout.{generation}.skein");
    let lexical_manifest_file = format!("search_lexical.manifest.{generation}.skein");

    let descriptor_source = input.stage.join(SEARCH_SEGMENT_DESCRIPTOR_FILE);
    let payload_source = input.stage.join(SEARCH_SEGMENT_PAYLOAD_FILE);
    let metadata_source = input.stage.join(STAGE_METADATA_FILE);
    let vector_source = input.stage.join(STAGE_VECTOR_FILE);
    let lexical_artifact_source = input.stage.join(input.lexical_artifact_name);
    let lexical_manifest_source = input.stage.join(LEXICAL_MANIFEST_FILE);
    let (descriptor_len, descriptor_checksum) = file_len_checksum(&descriptor_source)?;
    let payload_len = fs::metadata(&payload_source)?.len();
    let metadata_payload_len = fs::metadata(&metadata_source)?.len();
    let vector_payload_len = fs::metadata(&vector_source)?.len();
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
        file_len_checksum(&lexical_manifest_source)?;
    let lexical_artifact_len = fs::metadata(&lexical_artifact_source)?.len();
    let layout_bytes = input.layout.encode()?;
    let manifest = SearchOutOfCoreManifestBody {
        format: OUT_OF_CORE_FORMAT.to_string(),
        generation,
        descriptor_file: descriptor_file.clone(),
        descriptor_len,
        descriptor_checksum,
        payload_file: payload_file.clone(),
        payload_len,
        metadata_payload_file: metadata_payload_file.clone(),
        metadata_payload_len,
        vector_payload_file: vector_payload_file.clone(),
        vector_payload_len,
        layout_file: layout_file.clone(),
        layout_len: layout_bytes.len() as u64,
        layout_checksum: checksum_bytes(&layout_bytes),
        lexical_manifest_file: lexical_manifest_file.clone(),
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
    let manifest_bytes = manifest.encode()?;
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

    publish_generation_link(&descriptor_source, &input.root.join(&descriptor_file))?;
    publish_generation_link(&payload_source, &input.root.join(&payload_file))?;
    publish_generation_link(&metadata_source, &input.root.join(&metadata_payload_file))?;
    publish_generation_link(&vector_source, &input.root.join(&vector_payload_file))?;
    publish_generation_link(
        &lexical_artifact_source,
        &input.root.join(input.lexical_artifact_name),
    )?;
    if let Some(rabitq) = input.rabitq {
        publish_generation_link(
            &input.stage.join(&rabitq.file_name),
            &input.root.join(&rabitq.file_name),
        )?;
    }
    publish_generation_link(
        &lexical_manifest_source,
        &input.root.join(&lexical_manifest_file),
    )?;
    write_generation_artifact(&input.root.join(&layout_file), &layout_bytes)?;

    verify_published_artifact(
        &input.root.join(&descriptor_file),
        descriptor_len,
        Some(descriptor_checksum),
        "descriptor",
    )?;
    if let Some(rabitq) = input.rabitq {
        verify_published_artifact(
            &input.root.join(&rabitq.file_name),
            rabitq.artifact_bytes,
            Some(rabitq.artifact_checksum),
            "RaBitQ artifact",
        )?;
    }
    verify_published_artifact(
        &input.root.join(&payload_file),
        payload_len,
        None,
        "document payload",
    )?;
    verify_published_artifact(
        &input.root.join(&metadata_payload_file),
        metadata_payload_len,
        None,
        "metadata payload",
    )?;
    verify_published_artifact(
        &input.root.join(&vector_payload_file),
        vector_payload_len,
        None,
        "vector payload",
    )?;
    verify_published_artifact(
        &input.root.join(&lexical_manifest_file),
        lexical_manifest_len,
        Some(lexical_manifest_checksum),
        "lexical manifest",
    )?;
    verify_published_artifact(
        &input.root.join(input.lexical_artifact_name),
        lexical_artifact_len,
        None,
        "lexical artifact",
    )?;

    write_generation_artifact(&input.root.join(OUT_OF_CORE_MANIFEST_FILE), &manifest_bytes)?;
    Ok(PublishedGeneration {
        manifest_bytes: manifest_bytes.len() as u64,
        generation_bytes,
    })
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
    let mut file = File::open(path)?;
    let expected_len = file.metadata()?.len();
    let mut checksum = Crc32cHasher::new();
    let mut actual_len = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
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
