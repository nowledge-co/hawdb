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

//! Ordered update hydration, retaining one admitted encoded/decoded document.

use crate::build_control::checkpoint;
use crate::build_memory::BuildMemory;
use crate::{HawDBError, Result, SearchOutOfCoreMetrics, SearchOutOfCoreReader};
use hawdb_core::RuntimeTaskContext;

pub(super) fn visit_target(
    reader: &SearchOutOfCoreReader,
    id: &str,
    stage: &std::path::Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    consumer: &mut dyn FnMut(
        u64,
        &crate::lexical_projection::LexicalProjectionReader,
        crate::out_of_core::hydration::source::Source,
    ) -> Result<()>,
) -> Result<SearchOutOfCoreMetrics> {
    checkpoint(task)?;
    let Some(route) = reader.segment_for_document(id)? else {
        return Ok(SearchOutOfCoreMetrics::default());
    };
    let artifact = &reader.segments[route.artifact_index];
    let range = route.segment.payload_range.ok_or_else(|| {
        HawDBError::Storage("search hydration segment has no payload range".into())
    })?;
    let path = crate::build_memory::path::OwnedPath::join(
        stage,
        std::path::Path::new("mutation-source.body"),
        memory,
        task,
    )?;
    let file = hawdb_storage::file_io::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&*path)?;
    let source = crate::out_of_core::hydration::source::Source::capture(
        artifact,
        route.segment,
        id,
        file,
        (
            reader.config.max_uncompressed_segment_bytes.get(),
            reader.config.max_document_header_bytes.get(),
        ),
        memory,
        task,
    )?;
    let body_bytes = source.bytes;
    consumer(
        artifact.content_segment_id,
        &artifact.lexical_projection,
        source,
    )?;
    Ok(SearchOutOfCoreMetrics {
        streamed_documents: 1,
        streamed_body_bytes: body_bytes,
        segment_range_reads: 1,
        segment_bytes_read: range.length,
        hydration_segment_bytes_read: range.length,
        lexical_document_bytes_read: route.lexical_document_bytes_read,
        ..Default::default()
    })
}

#[cfg(test)]
mod legacy;
