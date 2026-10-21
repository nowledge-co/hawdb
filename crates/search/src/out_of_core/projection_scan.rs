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

//! Complete reads use the same held artifact closure and visibility as serving.

use super::hydration::read_record;
use super::*;
use crate::build_memory::{reserve_capacity, BuildMemory};
use crate::projection_scan::{add, checkpoint, payload_bytes};
use crate::{SearchProjectionScanBudget, SearchProjectionScanReport};
use hawdb_core::RuntimeTaskContext;

type ProjectionVisitor<'a> = &'a mut dyn FnMut(&SearchDocument) -> Result<()>;

impl SearchOutOfCoreReader {
    /// Exhaustively visits live documents under this reader's immutable pin.
    /// Order is unspecified. A prefix is not completion evidence. A validation
    /// and full logical-budget pass completes before any visitor callback.
    /// Both passes read all physical payloads; only preflight probes visible ID
    /// routes. Runtime depends on physical bytes, visible IDs and mapping-block
    /// reads. A host holding a store read guard blocks publication for that time.
    /// A callback can precede the emit pass's final checksum validation; an error
    /// after callbacks still refuses completion evidence.
    pub fn visit_projection_documents(
        &self,
        budget: SearchProjectionScanBudget,
        task: &RuntimeTaskContext,
        visitor: &mut dyn FnMut(&SearchDocument) -> Result<()>,
    ) -> Result<SearchProjectionScanReport> {
        budget.validate()?;
        checkpoint(task)?;
        budget.check(self.document_count(), 0)?;
        let memory = BuildMemory::new(task)?;
        let probe_bytes = self.segments.iter().try_fold(0, |maximum, artifact| {
            artifact
                .lexical_projection
                .document_id_probe_memory_bytes()
                .map(|bytes| maximum.max(bytes))
        })?;
        // Route validation owns one mapping block and its transient IDs. Admit
        // that capacity before any probe, not merely the payload decoder below.
        let preflight = {
            let _probe_memory = memory.input.reserve(probe_bytes)?;
            self.scan_projection_pass(budget, task, &memory, None)?
        };
        let emitted = self.scan_projection_pass(budget, task, &memory, Some(visitor))?;
        if emitted != preflight {
            return Err(invalid("held projection changed between scan passes"));
        }
        checkpoint(task)?;
        Ok(SearchProjectionScanReport {
            document_count: emitted.0,
            payload_bytes: emitted.1,
            artifact_generation: Some(self.generation()),
        })
    }

    fn scan_projection_pass(
        &self,
        budget: SearchProjectionScanBudget,
        task: &RuntimeTaskContext,
        memory: &BuildMemory,
        mut visitor: Option<ProjectionVisitor<'_>>,
    ) -> Result<(usize, usize)> {
        let mut rows = 0;
        let mut bytes = 0;
        for artifact in &self.segments {
            for segment in &artifact.descriptor.segments {
                checkpoint(task)?;
                let range = segment
                    .payload_range
                    .ok_or_else(|| invalid("missing payload range"))?;
                let admission = hydration::ReadAdmission {
                    memory,
                    task,
                    max_header_bytes: self.config.max_document_header_bytes.get(),
                };
                hydration::read_validated(
                    hydration::RangeReader {
                        file: &artifact.payload,
                        offset: range.offset,
                        remaining: range.length,
                    },
                    range.length,
                    range.checksum,
                    self.config.max_uncompressed_segment_bytes.get(),
                    Some(admission),
                    |text| {
                        let mut line_memory = memory.spool.reserve(0)?;
                        let mut previous_memory = memory.spool.reserve(0)?;
                        let mut line = Vec::new();
                        let mut previous = Vec::new();
                        let mut physical_rows = 0;
                        while read_record(
                            text,
                            &mut line,
                            budget.max_encoded_record_bytes,
                            task,
                            &mut line_memory,
                        )? {
                            let record = std::str::from_utf8(&line)
                                .map_err(|_| invalid("non-ASCII encoded record"))?
                                .trim_end_matches(['\r', '\n']);
                            if record.is_empty() || record == "HAWDB_SEARCH_SEGMENT_V1" {
                                continue;
                            }
                            let (raw_id, logical_bytes, metadata_count) =
                                encoded_record_size(record)?;
                            physical_rows = add(physical_rows, 1)?;
                            if physical_rows > segment.document_count {
                                return Err(invalid("segment count exceeded"));
                            }
                            // Admit the decoded record and conservative map/capacity overhead
                            // before the canonical decoder allocates any field.
                            let decode_bytes = add(
                                add(
                                    logical_bytes
                                        .checked_mul(4)
                                        .ok_or_else(|| invalid("decode size overflow"))?,
                                    size_of::<SearchDocument>(),
                                )?,
                                metadata_count
                                    .checked_mul(crate::build_memory::MAP_ENTRY_BYTES)
                                    .ok_or_else(|| invalid("metadata size overflow"))?,
                            )?;
                            let id_bytes = add(raw_id.len() / 2, size_of::<String>())?;
                            let _id_memory = memory.input.reserve(id_bytes)?;
                            let id = crate::decode_string(raw_id)?;
                            if physical_rows > 1 && previous.as_slice() >= id.as_bytes() {
                                return Err(invalid("segment IDs are not strictly ordered"));
                            }
                            previous.clear();
                            reserve_capacity(&mut previous, id.len(), &mut previous_memory)?;
                            previous.extend_from_slice(id.as_bytes());
                            if physical_rows == 1 && id != segment.first_document_id {
                                return Err(invalid("first ID differs from descriptor"));
                            }
                            let visible =
                                self.visibility.is_visible(artifact.content_segment_id, &id);
                            if visible {
                                rows = add(rows, 1)?;
                                bytes = add(bytes, logical_bytes)?;
                                budget.check(rows, bytes)?;
                                // The immutable pin and preflight establish routes for
                                // both passes. Avoid repeating mapping-block I/O during emit.
                                if visitor.is_none() {
                                    let route =
                                        self.segment_for_document(&id)?.ok_or_else(|| {
                                            invalid("visible document has no indexed route")
                                        })?;
                                    if self.segments[route.artifact_index].content_segment_id
                                        != artifact.content_segment_id
                                    {
                                        return Err(invalid("visible document route differs"));
                                    }
                                }
                            }
                            let _decode_memory = memory.input.reserve(decode_bytes)?;
                            let document = crate::decode_search_document_line(record)?;
                            if payload_bytes(&document)? != logical_bytes {
                                return Err(invalid("decoded record size differs"));
                            }
                            if visible && let Some(visitor) = visitor.as_mut() {
                                visitor(&document)?;
                            }
                        }
                        if physical_rows != segment.document_count
                            || previous.as_slice() != segment.last_document_id.as_bytes()
                        {
                            return Err(invalid("segment count or final ID differs"));
                        }
                        Ok(())
                    },
                )?;
            }
        }
        if rows != self.document_count() {
            return Err(invalid("live inventory differs from manifest"));
        }
        checkpoint(task)?;
        Ok((rows, bytes))
    }
}

fn invalid(reason: &str) -> HawDBError {
    HawDBError::Storage(format!("projection scan {reason}"))
}

fn encoded_record_size(record: &str) -> Result<(&str, usize, usize)> {
    let mut fields = record.split('\t');
    if fields.next() != Some("doc") {
        return Err(invalid("invalid record tag"));
    }
    let id = fields.next().ok_or_else(|| invalid("missing ID"))?;
    let title = fields.next().ok_or_else(|| invalid("missing title"))?;
    let content = fields.next().ok_or_else(|| invalid("missing content"))?;
    let embedding = fields.next().ok_or_else(|| invalid("missing embedding"))?;
    let metadata = fields.next().ok_or_else(|| invalid("missing metadata"))?;
    if fields.next().is_some() {
        return Err(invalid("extra record fields"));
    }
    let mut bytes = add(add(hex_size(id)?, hex_size(title)?)?, hex_size(content)?)?;
    if !embedding.is_empty() {
        bytes = add(
            bytes,
            embedding
                .split(',')
                .count()
                .checked_mul(size_of::<f32>())
                .ok_or_else(|| invalid("embedding size overflow"))?,
        )?;
    }
    let mut count = 0;
    let mut previous = None;
    if !metadata.is_empty() {
        for pair in metadata.split(';') {
            let (key, value) = pair
                .split_once('=')
                .ok_or_else(|| invalid("invalid metadata pair"))?;
            if previous.is_some_and(|previous| previous >= key) {
                return Err(invalid("duplicate or unordered metadata keys"));
            }
            previous = Some(key);
            bytes = add(bytes, add(hex_size(key)?, hex_size(value)?)?)?;
            count = add(count, 1)?;
        }
    }
    Ok((id, bytes, count))
}

fn hex_size(value: &str) -> Result<usize> {
    if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("invalid hex field"));
    }
    Ok(value.len() / 2)
}

#[cfg(test)]
mod tests;
