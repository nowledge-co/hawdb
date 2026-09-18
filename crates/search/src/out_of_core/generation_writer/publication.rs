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

use super::super::{
    SearchOutOfCoreLayoutBody, SearchOutOfCoreManifestBody, OUT_OF_CORE_FORMAT,
    OUT_OF_CORE_MANIFEST_FILE,
};
use super::{
    artifact_name::Name, RaBitQGenerationArtifact, STAGE_METADATA_FILE, STAGE_VECTOR_FILE,
};
use crate::build_control::json;
use crate::build_memory::BuildMemory;
use crate::error::{HawDBError, Result};
use crate::lexical_projection::MANIFEST_FILE as LEXICAL_MANIFEST_FILE;
use crate::{SearchEmbeddingManifest, SEARCH_SEGMENT_DESCRIPTOR_FILE, SEARCH_SEGMENT_PAYLOAD_FILE};
use hawdb_core::RuntimeTaskContext;
use std::path::Path;

use super::io::GenerationIo;

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

pub(super) fn publish_generation(
    input: PublishGenerationInput<'_>,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<PublishedGeneration> {
    let io = GenerationIo::new(memory, task);
    let generation = input.generation;
    let descriptor_file = Name::generated("search_projection_segments.", generation, memory, task)?;
    let payload_file = Name::generated(
        "search_projection_segment_payloads.",
        generation,
        memory,
        task,
    )?;
    let metadata_payload_file = Name::generated(
        "search_projection_metadata_payloads.",
        generation,
        memory,
        task,
    )?;
    let vector_payload_file = Name::generated(
        "search_projection_vector_payloads.",
        generation,
        memory,
        task,
    )?;
    let layout_file = Name::generated(
        "search_projection_out_of_core_layout.",
        generation,
        memory,
        task,
    )?;
    let lexical_manifest_file =
        Name::generated("search_lexical.manifest.", generation, memory, task)?;

    let descriptor_source = io.path(input.stage, Path::new(SEARCH_SEGMENT_DESCRIPTOR_FILE))?;
    let payload_source = io.path(input.stage, Path::new(SEARCH_SEGMENT_PAYLOAD_FILE))?;
    let metadata_source = io.path(input.stage, Path::new(STAGE_METADATA_FILE))?;
    let vector_source = io.path(input.stage, Path::new(STAGE_VECTOR_FILE))?;
    let lexical_artifact_source = io.path(input.stage, Path::new(input.lexical_artifact_name))?;
    let lexical_manifest_source = io.path(input.stage, Path::new(LEXICAL_MANIFEST_FILE))?;
    let (descriptor_len, descriptor_checksum) = io.checksum(&descriptor_source)?;
    let payload_len = io.length(&payload_source)?;
    let metadata_payload_len = io.length(&metadata_source)?;
    let vector_payload_len = io.length(&vector_source)?;
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
            return Err(HawDBError::Storage(format!(
                "staged search generation {name} payload length {actual} does not match {expected}"
            )));
        }
    }
    let (lexical_manifest_len, lexical_manifest_checksum) =
        io.checksum(&lexical_manifest_source)?;
    let lexical_artifact_len = io.length(&lexical_artifact_source)?;
    let layout = json::prepare(
        input.layout,
        u64::MAX,
        Some(task),
        "search out-of-core layout",
    )?;
    let manifest = SearchOutOfCoreManifestBody {
        format: OUT_OF_CORE_FORMAT,
        generation,
        descriptor_file: descriptor_file.as_str(),
        descriptor_len,
        descriptor_checksum,
        payload_file: payload_file.as_str(),
        payload_len,
        metadata_payload_file: metadata_payload_file.as_str(),
        metadata_payload_len,
        vector_payload_file: vector_payload_file.as_str(),
        vector_payload_len,
        layout_file: layout_file.as_str(),
        layout_len: layout.len() as u64,
        layout_checksum: layout.checksum(),
        lexical_manifest_file: lexical_manifest_file.as_str(),
        lexical_manifest_len,
        lexical_manifest_checksum,
        rabitq_artifact_file: input.rabitq.map(|artifact| artifact.file_name.as_str()),
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
            .map(|manifest| manifest.model.as_str()),
        embedding_version: input
            .embedding_manifest
            .and_then(|manifest| manifest.version.as_deref()),
        embedding_dimension: input
            .embedding_manifest
            .map(|manifest| manifest.dimension)
            .or(input.embedding_dimension),
    };
    let _validation = memory.spool.reserve(3 * 128)?;
    manifest.validate_names()?;
    let manifest = json::prepare(
        &manifest,
        u64::MAX,
        Some(task),
        "search out-of-core manifest",
    )?;
    drop(_validation);
    let generation_bytes = [
        descriptor_len,
        payload_len,
        metadata_payload_len,
        vector_payload_len,
        layout.len() as u64,
        lexical_manifest_len,
        lexical_artifact_len,
        manifest.len() as u64,
        input.rabitq.map_or(0, |artifact| artifact.artifact_bytes),
    ]
    .into_iter()
    .try_fold(0u64, |total, bytes| total.checked_add(bytes))
    .ok_or_else(|| HawDBError::Storage("search generation size overflow".to_string()))?;
    if generation_bytes > input.max_generation_bytes {
        return Err(HawDBError::Storage(format!(
            "search generation requires {generation_bytes} published bytes, exceeding {}",
            input.max_generation_bytes
        )));
    }

    let layout_bytes = layout.encode(memory, task)?;
    let manifest_bytes = manifest.encode(memory, task)?;

    io.link(
        &descriptor_source,
        &io.path(input.root, descriptor_file.as_ref())?,
    )?;
    io.link(
        &payload_source,
        &io.path(input.root, payload_file.as_ref())?,
    )?;
    io.link(
        &metadata_source,
        &io.path(input.root, metadata_payload_file.as_ref())?,
    )?;
    io.link(
        &vector_source,
        &io.path(input.root, vector_payload_file.as_ref())?,
    )?;
    io.link(
        &lexical_artifact_source,
        &io.path(input.root, Path::new(input.lexical_artifact_name))?,
    )?;
    if let Some(rabitq) = input.rabitq {
        io.link(
            &io.path(input.stage, rabitq.file_name.as_ref())?,
            &io.path(input.root, rabitq.file_name.as_ref())?,
        )?;
    }
    io.link(
        &lexical_manifest_source,
        &io.path(input.root, lexical_manifest_file.as_ref())?,
    )?;
    io.write(
        &io.path(input.root, layout_file.as_ref())?,
        &layout_bytes.bytes,
    )?;

    io.verify(
        &io.path(input.root, descriptor_file.as_ref())?,
        descriptor_len,
        Some(descriptor_checksum),
        "descriptor",
    )?;
    if let Some(rabitq) = input.rabitq {
        io.verify(
            &io.path(input.root, rabitq.file_name.as_ref())?,
            rabitq.artifact_bytes,
            Some(rabitq.artifact_checksum),
            "RaBitQ artifact",
        )?;
    }
    io.verify(
        &io.path(input.root, payload_file.as_ref())?,
        payload_len,
        None,
        "document payload",
    )?;
    io.verify(
        &io.path(input.root, metadata_payload_file.as_ref())?,
        metadata_payload_len,
        None,
        "metadata payload",
    )?;
    io.verify(
        &io.path(input.root, vector_payload_file.as_ref())?,
        vector_payload_len,
        None,
        "vector payload",
    )?;
    io.verify(
        &io.path(input.root, lexical_manifest_file.as_ref())?,
        lexical_manifest_len,
        Some(lexical_manifest_checksum),
        "lexical manifest",
    )?;
    io.verify(
        &io.path(input.root, Path::new(input.lexical_artifact_name))?,
        lexical_artifact_len,
        None,
        "lexical artifact",
    )?;

    io.write(
        &io.path(input.root, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
        &manifest_bytes.bytes,
    )?;
    Ok(PublishedGeneration {
        manifest_bytes: manifest_bytes.bytes.len() as u64,
        generation_bytes,
    })
}

#[cfg(test)]
pub(super) fn file_len_checksum(path: &Path) -> Result<(u64, u64)> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task)?;
    GenerationIo::new(&memory, &task).checksum(path)
}

#[cfg(feature = "vector-search")]
pub(super) fn file_len_checksum_with_context(
    path: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<(u64, u64)> {
    GenerationIo::new(memory, task).checksum(path)
}
