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

//! Partitioned initial and incremental content, installed by one complete selector.

use super::*;
use crate::build_control::json;
use crate::out_of_core::{
    SearchOutOfCoreManifestEnvelope, MAX_OUT_OF_CORE_MANIFEST_BYTES, OUT_OF_CORE_MANIFEST_FILE,
};

mod install;
mod update;

#[derive(Clone, Copy)]
struct Range {
    start: u64,
    end: u64,
    documents: usize,
}

struct Totals {
    report: Option<SearchOutOfCoreGenerationBuildReport>,
    rabitq_artifacts: usize,
    next_generation: u64,
}

impl Totals {
    fn add(&mut self, mut next: SearchOutOfCoreGenerationBuildReport) -> Result<()> {
        next.generation_bytes -= next.manifest_bytes;
        self.next_generation = next
            .generation
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("initial generation overflow".into()))?;
        self.rabitq_artifacts += usize::from(next.rabitq_artifact_bytes != 0);
        if let Some(total) = self.report.as_mut() {
            macro_rules! sum {
                ($($field:ident),+ $(,)?) => { $(
                    total.$field = total.$field.checked_add(next.$field)
                        .ok_or_else(|| HawDBError::Storage("initial generation report overflows".into()))?;
                )+ };
            }
            sum!(
                vector_document_count,
                logical_document_bytes,
                descriptor_bytes,
                document_payload_bytes,
                metadata_payload_bytes,
                vector_payload_bytes,
                lexical_artifact_bytes,
                lexical_manifest_bytes,
                rabitq_artifact_bytes,
                generation_bytes,
                published_content_segments,
                cleanup_pending_stages
            );
            total.generation = next.generation;
            total.cleanup_retry_required |= next.cleanup_retry_required;
            total.lexical_generation = next.lexical_generation;
            total.document_count = next.document_count;
            total.documents_digest = next.documents_digest;
            total.peak_segment_document_count = total
                .peak_segment_document_count
                .max(next.peak_segment_document_count);
            total.peak_segment_encoded_bytes = total
                .peak_segment_encoded_bytes
                .max(next.peak_segment_encoded_bytes);
            total.descriptor_working_bytes = total
                .descriptor_working_bytes
                .max(next.descriptor_working_bytes);
            total.rabitq_peak_build_working_bytes = total
                .rabitq_peak_build_working_bytes
                .max(next.rabitq_peak_build_working_bytes);
            if self.rabitq_artifacts == 1 && next.rabitq_source_digest.is_some() {
                total.rabitq_source_digest = next.rabitq_source_digest;
            } else if self.rabitq_artifacts > 1 {
                total.rabitq_source_digest = None;
            }
        } else {
            self.report = Some(next);
        }
        Ok(())
    }
}

pub(super) fn finish(
    mut writer: SearchOutOfCoreGenerationWriter,
    generation: u64,
) -> Result<SearchOutOfCoreGenerationBuildReport> {
    // The caller has flushed the one captured source and owns the real root's
    // publication lease. Prefix selectors below live only in this private stage.
    let active = discovery::active(&writer.root, &writer.memory, &writer.task_context)?;
    // Every stage has flat contents. Partition stages are siblings in the real
    // project, so its cleanup API can also retry a deferred partition removal.
    let private =
        context_memory::OwnedPath::copy(&writer.stage.path, &writer.memory, &writer.task_context)?;
    let source = SpoolSource {
        path: &writer.spool_path,
        document_count: writer.document_count,
        max_record_bytes: writer.options.max_record_bytes.get(),
        max_metadata_fields: writer.options.max_metadata_fields.get(),
        memory: writer.memory.clone(),
    };
    let mut cursor = source.cursor(&writer.task_context)?;
    let mut totals = Totals {
        report: None,
        rabitq_artifacts: 0,
        next_generation: generation,
    };
    loop {
        let start = cursor.position();
        let mut documents = 0;
        while documents < writer.options.max_content_documents.get() {
            let Some(record) = cursor.next()? else { break };
            drop(record);
            documents += 1;
        }
        if documents == 0 {
            break;
        }
        build_range(
            &writer,
            &private,
            &source,
            Range {
                start,
                end: cursor.position(),
                documents,
            },
            &mut totals,
        )?;
    }
    drop(cursor);
    drop(source);
    let mut report = totals
        .report
        .ok_or_else(|| HawDBError::Storage("initial import has no content".into()))?;
    if report.document_count != writer.document_count
        || report.documents_digest != writer.documents_digest.finish()
        || report.vector_document_count != writer.vector_document_count
        || report.logical_document_bytes != writer.logical_document_bytes
    {
        return Err(HawDBError::Storage(
            "initial partition identity differs from captured source".into(),
        ));
    }
    let cleanup = PreparedCleanup::prepare(&writer.root, &writer.memory, &writer.task_context)?;
    let (generations, manifest_bytes, retention_memory) =
        install::publish(&writer, &private, active, &mut report)?;
    report.manifest_bytes = manifest_bytes;
    report.generation_bytes = report
        .generation_bytes
        .checked_add(manifest_bytes)
        .ok_or_else(|| HawDBError::Storage("initial generation size overflows".into()))?;
    report.spool_bytes = writer.spool_bytes;
    report.peak_record_bytes = writer.peak_record_bytes;
    #[cfg(test)]
    crate::generation_cleanup::once::evidence::run(
        crate::generation_cleanup::once::evidence::Point::AfterCommit,
        &writer.memory,
    );
    let cleanup = cleanup.run(
        &writer.root,
        generations,
        writer.options.cleanup_options,
        &writer.task_context,
    );
    drop(retention_memory);
    report.cleanup_deleted_files = cleanup.deleted_files;
    report.cleanup_pending_files = cleanup.pending_files;
    report.cleanup_pending_stages = report
        .cleanup_pending_stages
        .checked_add(usize::from(writer.stage.cleanup()))
        .ok_or_else(|| HawDBError::Storage("initial cleanup report overflows".into()))?;
    report.cleanup_retry_required |= cleanup.retry_required || report.cleanup_pending_stages != 0;
    Ok(report)
}

fn build_range(
    parent: &SearchOutOfCoreGenerationWriter,
    root: &Path,
    source: &SpoolSource,
    range: Range,
    totals: &mut Totals,
) -> Result<()> {
    checkpoint(&parent.task_context)?;
    let options = parent
        .options
        .try_clone(&parent.memory, &parent.task_context)?;
    let mut part = SearchOutOfCoreGenerationWriter::create_with_staging_root(
        root,
        Some(&parent.root),
        options,
        parent.task_context.clone(),
        parent.memory.clone(),
    )?;
    part.lexical_term_policy = parent.lexical_term_policy;
    part.max_lexical_manifest_bytes = parent.max_lexical_manifest_bytes;
    part.embedding_dimension = parent.embedding_dimension;
    part.needs_chinese_analyzer = parent.needs_chinese_analyzer;
    let metadata_bytes = parent.metadata_fields.iter().try_fold(0, |bytes, field| {
        checked_add(bytes, checked_add(SET_ENTRY_BYTES, field.capacity())?)
    })?;
    let metadata_memory = parent.memory.retained.reserve(metadata_bytes)?;
    part.metadata_fields = parent.metadata_fields.clone();
    part.metadata_memory = metadata_memory;
    part.metadata_field_bytes = parent.metadata_field_bytes;
    if let Some(expected_generation) =
        discovery::active(root, &parent.memory, &parent.task_context)?
    {
        part.expected_active_generation = Some(expected_generation);
        part.active_manifest_update = Some(ActiveManifestUpdate::Append {
            expected_generation,
        });
    }
    let mut cursor = source.range_cursor(
        range.start,
        range.end,
        range.documents,
        &parent.task_context,
    )?;
    while let Some(record) = cursor.next()? {
        let bytes = record.encoded_bytes;
        let prepared = part.prepare_record(record.header.header(), bytes)?;
        record.add_digest(&mut part.documents_digest);
        let (header, body, _) = record.into_body()?;
        drop(body);
        part.commit_record(header.header, bytes, prepared);
    }
    drop(cursor);
    // Artifact scans use the immutable outer source range. The private writer's
    // own spool contains only its header; no body is recaptured or copied.
    part.spool_bytes = SPOOL_HEADER.len() as u64;
    let mut exceeds_content = false;
    let result = part.finish_publication(
        |part, _, generation| {
            let artifacts = if part.needs_chinese_analyzer {
                crate::analyzer_workspace::run(&part.memory, &part.task_context, |workspace| {
                    artifacts(part, source, range, generation, Some(workspace))
                })?
            } else {
                artifacts(part, source, range, generation, None)?
            };
            let bytes = artifacts.content_bytes(&part.task_context)?;
            if range.documents > 1 && bytes > part.options.max_content_artifact_bytes.get() {
                exceeds_content = true;
                return Err(HawDBError::Storage(format!(
                    "initial content candidate with {} documents requires {bytes} artifact bytes, exceeding {}",
                    range.documents,
                    part.options.max_content_artifact_bytes,
                )));
            }
            Ok(artifacts)
        },
        Some(totals.next_generation),
        true,
    );
    match result {
        Ok(report) => totals.add(report),
        Err(_) if exceeds_content && range.documents > 1 => {
            let left_documents = range.documents / 2;
            let mut cursor = source.range_cursor(
                range.start,
                range.end,
                range.documents,
                &parent.task_context,
            )?;
            for _ in 0..left_documents {
                cursor.next()?.ok_or_else(|| {
                    HawDBError::Storage("initial split source is incomplete".into())
                })?;
            }
            let middle = cursor.position();
            drop(cursor);
            build_range(
                parent,
                root,
                source,
                Range {
                    start: range.start,
                    end: middle,
                    documents: left_documents,
                },
                totals,
            )?;
            build_range(
                parent,
                root,
                source,
                Range {
                    start: middle,
                    end: range.end,
                    documents: range.documents - left_documents,
                },
                totals,
            )
        }
        Err(error) => Err(error),
    }
}

fn artifacts(
    writer: &SearchOutOfCoreGenerationWriter,
    source: &SpoolSource,
    range: Range,
    generation: u64,
    workspace: Option<&crate::analyzer_workspace::Workspace>,
) -> Result<GenerationArtifacts> {
    writer.build_artifacts_with_scan(range.documents, generation, workspace, |consume| {
        let mut cursor = source.range_cursor(
            range.start,
            range.end,
            range.documents,
            &writer.task_context,
        )?;
        let mut ordinal = 0;
        while let Some(record) = cursor.next()? {
            consume(ordinal, record)?;
            ordinal += 1;
        }
        Ok(())
    })
}
