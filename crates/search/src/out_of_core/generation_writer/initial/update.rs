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

//! Join new private content with the active immutable closure exactly once.

use super::*;
use crate::out_of_core::{SearchOutOfCoreManifestBody, SearchOutOfCoreMutationRunManifest};

pub(super) struct Prepared {
    pub(super) old_segments: usize,
    pub(super) mutation: Option<(artifact_name::Name, json::EncodedManifest)>,
    _decode: Option<QueryMemoryLease>,
    _growth: Option<QueryMemoryLease>,
    _reference: Option<QueryMemoryLease>,
}

pub(super) fn prepare(
    writer: &SearchOutOfCoreGenerationWriter,
    manifest: &mut SearchOutOfCoreManifestBody,
) -> Result<Prepared> {
    let expected = match writer.active_manifest_update {
        None => {
            return Ok(Prepared {
                old_segments: 0,
                mutation: None,
                _decode: None,
                _growth: None,
                _reference: None,
            })
        }
        Some(ActiveManifestUpdate::Append {
            expected_generation,
        })
        | Some(ActiveManifestUpdate::Mutate {
            expected_generation,
        }) => expected_generation,
        Some(ActiveManifestUpdate::Compact { .. }) => {
            return Err(HawDBError::Storage(
                "compaction cannot publish partitioned input content".into(),
            ))
        }
    };
    let memory = &writer.memory;
    let task = &writer.task_context;
    let io = io::GenerationIo::new(memory, task);
    let bytes = io.read(
        &io.path(&writer.root, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
    )?;
    let decode = memory.spool.reserve(checked_add(
        json::decode_capacity(&bytes.bytes, 0, 0, task)?,
        3 * 128,
    )?)?;
    let mut active = SearchOutOfCoreManifestBody::decode(&bytes.bytes)?;
    if active.generation != expected {
        return Err(generation_changed(expected, active.generation));
    }
    let old_segments = active.segments.len();
    let count = checked_add(old_segments, manifest.segments.len())?.max(active.segments.capacity());
    // The old/new serde allocations remain admitted by their decode leases.
    // Admit vector growth before moving either closure into the final manifest.
    let growth = memory.retained.reserve(checked_add(
        checked_mul(
            count,
            std::mem::size_of::<crate::out_of_core::SearchOutOfCoreSegmentManifest>(),
        )?,
        checked_add(
            std::mem::size_of::<SearchOutOfCoreMutationRunManifest>(),
            128,
        )?,
    )?)?;
    active
        .segments
        .try_reserve_exact(manifest.segments.len())
        .map_err(|error| {
            HawDBError::Execution(format!("cannot grow partitioned content manifest: {error}"))
        })?;
    if active.segments.capacity() > count {
        return Err(HawDBError::Execution(
            "partitioned content manifest exceeds admission".into(),
        ));
    }
    let first_id = active
        .segments
        .iter()
        .map(|segment| segment.segment_id)
        .max()
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| HawDBError::Storage("partitioned content segment id overflows".into()))?;
    for (offset, segment) in manifest.segments.iter_mut().enumerate() {
        checkpoint(task)?;
        segment.segment_id = first_id.checked_add(offset as u64).ok_or_else(|| {
            HawDBError::Storage("partitioned content segment id overflows".into())
        })?;
    }
    let mut digest = active.documents_digest;
    let retractions = writer
        .mutations
        .as_ref()
        .map_or(0, |mutation| mutation.entries.len());
    if let Some(mutation) = &writer.mutations {
        mutation
            .reopen_budget
            .admit_entries_with_additional_segments(
                &mutation.entries,
                manifest.segments.len().saturating_sub(1),
            )?;
        for entry in &mutation.entries {
            checkpoint(task)?;
            digest = DocumentsDigest::replace(digest, entry.retraction.documents_digest, 0);
        }
    } else if matches!(
        writer.active_manifest_update,
        Some(ActiveManifestUpdate::Mutate { .. })
    ) {
        return Err(HawDBError::Storage(
            "partitioned mutation has no prepared retractions".into(),
        ));
    }
    manifest.document_count = active
        .document_count
        .checked_sub(retractions)
        .and_then(|count| count.checked_add(writer.document_count))
        .ok_or_else(|| HawDBError::Storage("partitioned logical count overflows".into()))?;
    manifest.documents_digest = DocumentsDigest::combine(digest, writer.documents_digest.finish());
    active.segments.append(&mut manifest.segments);
    manifest.segments = active.segments;
    manifest.mutation_runs = active.mutation_runs;
    let mutation = writer
        .mutations
        .as_ref()
        .filter(|mutation| !mutation.entries.is_empty())
        .map(|mutation| {
            #[derive(serde::Serialize)]
            struct Body<'a> {
                format: &'static str,
                generation: u64,
                analyzer_digest: u64,
                entries: &'a [crate::out_of_core::mutation_run::SearchMutationRunEntry],
            }
            let name = artifact_name::Name::generated(
                "search_projection_mutation_run.",
                manifest.generation,
                memory,
                task,
            )?;
            let body = Body {
                format: crate::out_of_core::mutation_run::MUTATION_RUN_FORMAT,
                generation: manifest.generation,
                analyzer_digest: mutation.analyzer_digest,
                entries: &mutation.entries,
            };
            let prepared = json::prepare(
                &body,
                mutation.max_run_bytes,
                Some(task),
                "search mutation run",
            )?;
            let checksum = prepared.checksum();
            let encoded = prepared.encode(memory, task)?;
            let capacity = checked_add(manifest.mutation_runs.len(), 1)?
                .max(manifest.mutation_runs.capacity());
            let reference = memory.retained.reserve(checked_mul(
                capacity,
                std::mem::size_of::<SearchOutOfCoreMutationRunManifest>(),
            )?)?;
            manifest
                .mutation_runs
                .try_reserve_exact(1)
                .map_err(|error| {
                    HawDBError::Execution(format!(
                        "cannot grow partitioned mutation manifest: {error}"
                    ))
                })?;
            if manifest.mutation_runs.capacity() > capacity {
                return Err(HawDBError::Execution(
                    "partitioned mutation manifest exceeds admission".into(),
                ));
            }
            manifest
                .mutation_runs
                .push(SearchOutOfCoreMutationRunManifest {
                    generation: manifest.generation,
                    file: name.as_str().into(),
                    len: encoded.bytes.len() as u64,
                    checksum,
                    entry_count: mutation.entries.len(),
                    analyzer_digest: mutation.analyzer_digest,
                });
            Ok::<_, HawDBError>(((name, encoded), reference))
        })
        .transpose()?;
    let (mutation, reference) = match mutation {
        Some((mutation, reference)) => (Some(mutation), Some(reference)),
        None => (None, None),
    };
    Ok(Prepared {
        old_segments,
        mutation,
        _decode: Some(decode),
        _growth: Some(growth),
        _reference: reference,
    })
}
