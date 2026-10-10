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

//! Merge overlapping content ranges without retaining document bodies in RAM.

use super::*;
use crate::analyzer_stream::Control;
use crate::build_memory::reserve_capacity;
use crate::document_encoding::{streamed, HeaderSource};
use crate::out_of_core::generation_writer::spool::{
    SpoolCursor, SpoolRecord, SpoolSource, SPOOL_HEADER,
};
use crate::SearchDocumentBody;

struct Capture<'a> {
    body: File,
    output: File,
    reader: &'a SearchOutOfCoreReader,
    writer: &'a mut SearchOutOfCoreGenerationWriter,
    memory: &'a BuildMemory,
    task: &'a RuntimeTaskContext,
    content_segment_id: u64,
    documents: usize,
    spool_bytes: &'a mut u64,
    body_bytes: u64,
}

impl selected_body::Consumer for Capture<'_> {
    fn start(&mut self, id: &str) -> Result<bool> {
        let selected = self
            .reader
            .visibility
            .is_visible(self.content_segment_id, id);
        if selected {
            self.body.set_len(0)?;
            self.body.seek(SeekFrom::Start(0))?;
        }
        Ok(selected)
    }

    fn body(&mut self, bytes: &[u8]) -> Result<()> {
        self.body.write_all(bytes)?;
        Ok(())
    }

    fn finish(&mut self, receipt: selected_body::Receipt) -> Result<()> {
        checkpoint(self.task)?;
        let bytes = streamed::record_len(receipt.header.header(), receipt.body_bytes)?
            .checked_add(16)
            .ok_or_else(|| HawDBError::Storage("compaction spool size overflow".into()))?;
        let total = self
            .spool_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.writer.options.max_spool_bytes.get())
            .ok_or_else(|| {
                HawDBError::Storage("compaction input spool exceeds admission".into())
            })?;
        self.writer.stage.reserve_additional_disk(bytes)?;
        streamed::write_frame(
            &mut self.output,
            receipt.header.header(),
            &mut RangeReader {
                file: &self.body,
                offset: 0,
                remaining: receipt.body_bytes,
            },
            SearchDocumentBody {
                bytes: receipt.body_bytes,
                expected_checksum: Some(receipt.body_checksum),
            },
            self.writer.options.max_record_bytes.get(),
            Control {
                memory: Some(crate::analyzer_memory::Memory::Build(self.memory)),
                task: Some(self.task),
                ..Control::default()
            },
        )?;
        *self.spool_bytes = total;
        self.documents = self
            .documents
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("compaction document count overflow".into()))?;
        self.body_bytes = self
            .body_bytes
            .checked_add(receipt.body_bytes)
            .ok_or_else(|| HawDBError::Storage("compaction body bytes overflow".into()))?;
        Ok(())
    }
}

struct Source {
    cursor: SpoolCursor,
    head: Option<SpoolRecord>,
}

pub(super) fn copy(
    reader: &SearchOutOfCoreReader,
    selection: &Selection,
    writer: &mut SearchOutOfCoreGenerationWriter,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<SearchOutOfCoreMetrics> {
    let body_path = crate::build_memory::path::OwnedPath::join(
        &writer.stage.path,
        Path::new("compaction-source.body"),
        memory,
        task,
    )?;
    let fan_in = selection.end - selection.start;
    let mut sources = Vec::<Source>::new();
    let mut source_memory = memory.retained.reserve(0)?;
    reserve_capacity(&mut sources, fan_in, &mut source_memory)?;
    let mut metrics = SearchOutOfCoreMetrics::default();
    let mut spool_bytes = 0u64;
    for (ordinal, artifact) in reader.segments[selection.start..selection.end]
        .iter()
        .enumerate()
    {
        checkpoint(task)?;
        let name_memory = memory.retained.reserve(64)?;
        let name = format!("compaction-source-{ordinal}.spool");
        let path = crate::build_memory::path::OwnedPath::join(
            &writer.stage.path,
            Path::new(&name),
            memory,
            task,
        )?;
        drop(name);
        drop(name_memory);
        let mut output = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&*path)?;
        spool_bytes = spool_bytes
            .checked_add(SPOOL_HEADER.len() as u64)
            .filter(|bytes| *bytes <= writer.options.max_spool_bytes.get())
            .ok_or_else(|| {
                HawDBError::Storage("compaction input spool exceeds admission".into())
            })?;
        writer
            .stage
            .reserve_additional_disk(SPOOL_HEADER.len() as u64)?;
        output.write_all(SPOOL_HEADER)?;
        let body = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&*body_path)?;
        let mut capture = Capture {
            body,
            output,
            reader,
            writer,
            memory,
            task,
            content_segment_id: artifact.content_segment_id,
            documents: 0,
            spool_bytes: &mut spool_bytes,
            body_bytes: 0,
        };
        for segment in &artifact.descriptor.segments {
            checkpoint(task)?;
            let range = segment.payload_range.ok_or_else(|| {
                HawDBError::Storage("compaction source has no payload range".into())
            })?;
            let admission = ReadAdmission {
                memory,
                task,
                max_header_bytes: reader.config.max_document_header_bytes.get(),
            };
            hydration::read_validated(
                RangeReader {
                    file: &artifact.payload,
                    offset: range.offset,
                    remaining: range.length,
                },
                range.length,
                range.checksum,
                reader.config.max_uncompressed_segment_bytes.get(),
                Some(admission),
                |text| {
                    selected_body::visit(
                        text,
                        segment,
                        &mut capture,
                        reader
                            .lexical_source_policy()
                            .max_document_source_bytes()
                            .get(),
                        admission,
                    )
                },
            )?;
            metrics.segment_range_reads += 1;
            metrics.segment_bytes_read = metrics
                .segment_bytes_read
                .checked_add(range.length)
                .ok_or_else(|| HawDBError::Storage("compaction read bytes overflow".into()))?;
        }
        metrics.streamed_documents += capture.documents;
        metrics.streamed_body_bytes = metrics
            .streamed_body_bytes
            .checked_add(capture.body_bytes)
            .ok_or_else(|| HawDBError::Storage("compaction body bytes overflow".into()))?;
        let document_count = capture.documents;
        capture.output.flush()?;
        drop(capture);
        let mut cursor = SpoolSource {
            path: &path,
            document_count,
            max_record_bytes: writer.options.max_record_bytes.get(),
            max_metadata_fields: writer.options.max_metadata_fields.get(),
            memory: memory.clone(),
        }
        .cursor(task)?;
        let head = cursor.next()?;
        sources.push(Source { cursor, head });
    }
    loop {
        checkpoint(task)?;
        let Some(index) = sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| {
                source
                    .head
                    .as_ref()
                    .map(|head| (index, head.header.id.as_str()))
            })
            .min_by(|left, right| left.1.cmp(right.1))
            .map(|(index, _)| index)
        else {
            break;
        };
        let record = sources[index].head.take().expect("selected merge head");
        if sources.iter().any(|source| {
            source
                .head
                .as_ref()
                .is_some_and(|head| head.header.id == record.header.id)
        }) {
            return Err(HawDBError::Storage(
                "duplicate visible document during compaction".into(),
            ));
        }
        let (header, body, bytes) = record.into_body()?;
        writer.push_admitted_reader(
            header,
            body,
            SearchDocumentBody {
                bytes,
                expected_checksum: None,
            },
        )?;
        sources[index].head = sources[index].cursor.next()?;
    }
    metrics.hydration_segment_bytes_read = metrics.segment_bytes_read;
    Ok(metrics)
}
