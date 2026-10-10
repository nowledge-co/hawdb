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

//! Preflight the complete closure, install new dependencies, then select it.

use super::*;
use crate::out_of_core::SearchOutOfCoreSegmentManifest;

pub(super) fn publish(
    writer: &SearchOutOfCoreGenerationWriter,
    private: &Path,
    active: Option<u64>,
    report: &mut SearchOutOfCoreGenerationBuildReport,
) -> Result<(SearchProjectionGenerations, u64, QueryMemoryLease)> {
    let memory = &writer.memory;
    let task = &writer.task_context;
    let io = io::GenerationIo::new(memory, task);
    let head = io.read(
        &io.path(private, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
    )?;
    let _decode = memory.spool.reserve(checked_add(
        json::decode_capacity(&head.bytes, 0, 0, task)?,
        3 * 128,
    )?)?;
    let mut envelope: SearchOutOfCoreManifestEnvelope = serde_json::from_slice(&head.bytes)
        .map_err(|error| {
            HawDBError::Storage(format!("invalid staged initial manifest: {error}"))
        })?;
    if json::checksum_with_context(&envelope.body, Some(task))? != envelope.checksum {
        return Err(HawDBError::Storage(
            "staged initial manifest checksum mismatch".into(),
        ));
    }
    let manifest = &mut envelope.body;
    manifest.validate_names()?;
    if manifest.generation != report.generation
        || manifest.document_count != writer.document_count
        || manifest.documents_digest != writer.documents_digest.finish()
        || !manifest.mutation_runs.is_empty()
        || manifest.segments.len() != report.published_content_segments
    {
        return Err(HawDBError::Storage(
            "staged initial manifest identity mismatch".into(),
        ));
    }
    let update = update::prepare(writer, manifest)?;
    manifest.validate_names()?;
    let final_head = json::encode_with_context(
        manifest,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
        memory,
        task,
        "partitioned search manifest",
    )?;
    let mutation_bytes = update
        .mutation
        .as_ref()
        .map_or(0, |(_, encoded)| encoded.bytes.len() as u64);
    let published_bytes = report
        .generation_bytes
        .checked_add(mutation_bytes)
        .ok_or_else(|| HawDBError::Storage("partitioned published size overflows".into()))?;
    let total = published_bytes
        .checked_add(final_head.bytes.len() as u64)
        .ok_or_else(|| HawDBError::Storage("initial published size overflows".into()))?;
    if total > writer.options.max_generation_bytes.get() {
        return Err(HawDBError::Storage(
            "complete initial generation exceeds publication admission".into(),
        ));
    }
    let count = checked_add(
        checked_mul(3, manifest.segments.len())?,
        manifest.mutation_runs.len(),
    )?;
    let retained = memory
        .retained
        .reserve(checked_mul(count, SET_ENTRY_BYTES)?)?;
    let mut generations = SearchProjectionGenerations {
        lexical: Some(report.lexical_generation),
        out_of_core: Some(report.generation),
        rabitq_remove_all: true,
        ..Default::default()
    };
    let mut complete_content_bytes = 0_u64;
    for (offset, segment) in manifest.segments.iter().enumerate() {
        checkpoint(task)?;
        if offset >= update.old_segments {
            let bytes = dependencies(writer, private, segment, |source, _, length, checksum| {
                io.verify(source, length, checksum, "staged initial dependency")
            })?;
            if segment.document_count > writer.options.max_content_documents.get()
                || (segment.document_count > 1
                    && bytes > writer.options.max_content_artifact_bytes.get())
            {
                return Err(HawDBError::Storage(
                    "initial content owner exceeds admission".into(),
                ));
            }
            complete_content_bytes = complete_content_bytes
                .checked_add(bytes)
                .ok_or_else(|| HawDBError::Storage("initial content size overflows".into()))?;
        }
        generations.retained_lexical.insert(segment.generation);
        generations.retained_out_of_core.insert(segment.generation);
        if segment.rabitq_artifact_file.is_some() {
            generations.retained_rabitq.insert(segment.generation);
            generations.rabitq = Some(segment.generation);
            generations.rabitq_remove_all = false;
        }
    }
    generations
        .retained_out_of_core
        .extend(manifest.mutation_runs.iter().map(|run| run.generation));
    if complete_content_bytes != report.generation_bytes {
        return Err(HawDBError::Storage(
            "initial dependency lengths differ from the complete publication report".into(),
        ));
    }
    check_active(writer, active)?;
    // All ownership, aggregate and manifest admission checks precede real-root
    // dependency links. Only the new owners/run are installed; old bodies are
    // neither copied nor scanned during incremental publication.
    for segment in &manifest.segments[update.old_segments..] {
        checkpoint(task)?;
        dependencies(
            writer,
            private,
            segment,
            |source, target, length, checksum| {
                io.link(source, target)?;
                io.verify(target, length, checksum, "initial dependency")
            },
        )?;
    }
    if let Some((name, encoded)) = &update.mutation {
        io.write(&io.path(&writer.root, name.as_ref())?, &encoded.bytes)?;
    }
    check_active(writer, active)?;
    io.write(
        &io.path(&writer.root, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
        &final_head.bytes,
    )?;
    report.generation_bytes = published_bytes;
    report.document_count = manifest.document_count;
    report.documents_digest = manifest.documents_digest;
    Ok((generations, final_head.bytes.len() as u64, retained))
}

fn check_active(writer: &SearchOutOfCoreGenerationWriter, expected: Option<u64>) -> Result<()> {
    let actual = discovery::active(&writer.root, &writer.memory, &writer.task_context)?;
    if actual != expected {
        return Err(HawDBError::TransactionConflict {
            read_epoch: expected.unwrap_or_default(),
            committed_epoch: actual.unwrap_or_default(),
            key: "initial search publication changed".into(),
        });
    }
    Ok(())
}

fn dependencies(
    writer: &SearchOutOfCoreGenerationWriter,
    private: &Path,
    segment: &SearchOutOfCoreSegmentManifest,
    mut visit: impl FnMut(&Path, &Path, u64, Option<u64>) -> Result<()>,
) -> Result<u64> {
    let io = io::GenerationIo::new(&writer.memory, &writer.task_context);
    let lexical = artifact_name::Name::generated(
        "search_lexical.",
        segment.generation,
        &writer.memory,
        &writer.task_context,
    )?;
    let lexical_len = io.length(&io.path(private, lexical.as_ref())?)?;
    let mut content_bytes = 0_u64;
    for (file, length, checksum) in [
        (
            segment.descriptor_file.as_str(),
            segment.descriptor_len,
            Some(segment.descriptor_checksum),
        ),
        (segment.payload_file.as_str(), segment.payload_len, None),
        (
            segment.metadata_payload_file.as_str(),
            segment.metadata_payload_len,
            None,
        ),
        (
            segment.vector_payload_file.as_str(),
            segment.vector_payload_len,
            None,
        ),
        (
            segment.layout_file.as_str(),
            segment.layout_len,
            Some(segment.layout_checksum),
        ),
        (
            segment.lexical_manifest_file.as_str(),
            segment.lexical_manifest_len,
            Some(segment.lexical_manifest_checksum),
        ),
        (lexical.as_str(), lexical_len, None),
    ]
    .into_iter()
    .chain(
        segment
            .rabitq_artifact_file
            .as_deref()
            .zip(segment.rabitq_artifact_len)
            .map(|(file, length)| (file, length, segment.rabitq_artifact_checksum)),
    ) {
        content_bytes = content_bytes
            .checked_add(length)
            .ok_or_else(|| HawDBError::Storage("initial content size overflows".into()))?;
        visit(
            &io.path(private, Path::new(file))?,
            &io.path(&writer.root, Path::new(file))?,
            length,
            checksum,
        )?;
    }
    Ok(content_bytes)
}
