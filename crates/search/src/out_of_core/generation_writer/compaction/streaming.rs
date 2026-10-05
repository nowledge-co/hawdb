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

//! Private compaction output is discarded if any later source check fails.

use super::*;
use crate::out_of_core::hydration::{self, selected_body, RangeReader, ReadAdmission};
use hawdb_storage::file_io::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

struct Consumer<'a> {
    file: File,
    writer: &'a mut SearchOutOfCoreGenerationWriter,
    reader: &'a SearchOutOfCoreReader,
    content_segment_id: u64,
    documents: usize,
    bytes: u64,
}
impl selected_body::Consumer for Consumer<'_> {
    fn start(&mut self, id: &str) -> Result<bool> {
        let selected = self
            .reader
            .visibility
            .is_visible(self.content_segment_id, id);
        if selected {
            self.file.set_len(0)?;
            self.file.seek(SeekFrom::Start(0))?;
        }
        Ok(selected)
    }
    fn body(&mut self, bytes: &[u8]) -> Result<()> {
        self.file.write_all(bytes)?;
        Ok(())
    }
    fn finish(&mut self, receipt: selected_body::Receipt) -> Result<()> {
        self.writer.push_reader(
            receipt.header.header,
            RangeReader {
                file: &self.file,
                offset: 0,
                remaining: receipt.body_bytes,
            },
            crate::SearchDocumentBody {
                bytes: receipt.body_bytes,
                expected_checksum: Some(receipt.body_checksum),
            },
        )?;
        self.documents = self
            .documents
            .checked_add(1)
            .ok_or_else(|| HawDBError::Storage("compaction document count overflow".into()))?;
        self.bytes = self
            .bytes
            .checked_add(receipt.body_bytes)
            .ok_or_else(|| HawDBError::Storage("compaction body bytes overflow".into()))?;
        Ok(())
    }
}

pub(super) fn copy(
    reader: &SearchOutOfCoreReader,
    selection: &Selection,
    writer: &mut SearchOutOfCoreGenerationWriter,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<SearchOutOfCoreMetrics> {
    let path = crate::build_memory::path::OwnedPath::join(
        &writer.stage.path,
        Path::new("compaction-source.body"),
        memory,
        task,
    )?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&*path)?;
    let mut consumer = Consumer {
        file,
        writer,
        reader,
        content_segment_id: 0,
        documents: 0,
        bytes: 0,
    };
    let mut metrics = SearchOutOfCoreMetrics::default();
    let mut previous_last = None;
    for artifact in &reader.segments[selection.start..selection.end] {
        consumer.content_segment_id = artifact.content_segment_id;
        for segment in &artifact.descriptor.segments {
            checkpoint(task)?;
            if previous_last.is_some_and(|previous| previous >= segment.first_document_id.as_str())
            {
                return Err(HawDBError::Storage(
                    "compaction source document ranges overlap".into(),
                ));
            }
            previous_last = Some(segment.last_document_id.as_str());
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
                        &mut consumer,
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
    }
    metrics.hydration_segment_bytes_read = metrics.segment_bytes_read;
    metrics.streamed_documents = consumer.documents;
    metrics.streamed_body_bytes = consumer.bytes;
    Ok(metrics)
}
