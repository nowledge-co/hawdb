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

//! A verified body can feed mutations and compaction without materialization.

use super::*;
use crate::analyzer_stream::{Control, TokenOccurrence};
use crate::build_memory::{AdmittedHeader, BuildMemory};
use crate::build_term::Term;
use crate::document_encoding::{streamed, Header, HeaderSource, RecordSource};
use crate::lexical_projection::{DocumentSource, LexicalProjectionConfig};
use hawdb_core::RuntimeTaskContext;
use hawdb_executor::QueryMemoryLease;

pub(in crate::out_of_core) struct Source<'execution> {
    pub(in crate::out_of_core) header: AdmittedHeader,
    pub(in crate::out_of_core) body: File,
    pub(in crate::out_of_core) bytes: u64,
    pub(in crate::out_of_core) checksum: u64,
    pub(in crate::out_of_core) needs_chinese: bool,
    query: Option<crate::lexical_projection::QueryContext<'execution>>,
    memory: BuildMemory,
    task: RuntimeTaskContext,
    _memory: QueryMemoryLease,
}

struct CaptureContext<'a, 'execution> {
    segment: &'a SearchSegmentDescriptorEntry,
    id: &'a str,
    output: File,
    limits: (u64, usize),
    memory: &'a BuildMemory,
    task: &'a RuntimeTaskContext,
    query: Option<crate::lexical_projection::QueryContext<'execution>>,
}

impl Source<'static> {
    pub(in crate::out_of_core) fn capture(
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        id: &str,
        output: File,
        limits: (u64, usize),
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        Self::capture_reader(
            CaptureContext {
                segment,
                id,
                output,
                limits,
                memory,
                task,
                query: None,
            },
            RangeReader {
                file: &artifact.payload,
                offset: range.offset,
                remaining: range.length,
            },
        )
    }
}

impl<'execution> Source<'execution> {
    pub(in crate::out_of_core) fn capture_with_read_admission(
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        id: &str,
        output: File,
        limits: (u64, usize),
        admission: &'execution crate::build_memory::read::ReadAdmission<'_>,
        maximum_artifact_bytes: u64,
    ) -> Result<Self> {
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        if range
            .offset
            .checked_add(range.length)
            .is_none_or(|end| end > artifact.payload_length)
        {
            return Err(invalid("segment range exceeds payload artifact"));
        }
        admission.with_read_scope(
            &artifact.payload,
            artifact.payload_length,
            maximum_artifact_bytes,
            |reader| {
                let input = crate::build_memory::read::ScopedRange {
                    reader,
                    offset: range.offset,
                    remaining: range.length,
                };
                #[cfg(test)]
                let input = crate::build_control::read_observation::track_reads(input);
                Self::capture_reader(
                    CaptureContext {
                        segment,
                        id,
                        output,
                        limits,
                        memory: admission.memory,
                        task: admission.task,
                        query: Some(
                            crate::lexical_projection::QueryContext::from_read_admission(admission),
                        ),
                    },
                    input,
                )
            },
        )
    }

    fn capture_reader(context: CaptureContext<'_, 'execution>, input: impl Read) -> Result<Self> {
        let CaptureContext {
            segment,
            id,
            mut output,
            limits,
            memory,
            task,
            query,
        } = context;
        let retained = memory.retained.reserve(std::mem::size_of::<Self>())?;
        let range = segment
            .payload_range
            .ok_or_else(|| invalid("segment has no payload range"))?;
        let admission = ReadAdmission {
            memory,
            task,
            max_header_bytes: limits.1,
        };
        let receipt = read_validated(
            input,
            range.length,
            range.checksum,
            limits.0,
            Some(admission),
            |text| selected_body::select(text, segment, id, &mut output, limits.0, admission),
        )?;
        let needs_chinese = receipt.needs_chinese
            || std::iter::once((receipt.header.title.as_str(), 1))
                .chain(crate::analyzer_stream::metadata_token_fields(
                    &receipt.header.metadata,
                ))
                .any(|(text, _)| text.chars().any(crate::cjk_tokenizer::is_han_search_char));
        Ok(Self {
            header: receipt.header,
            body: output,
            bytes: receipt.body_bytes,
            checksum: receipt.body_checksum,
            needs_chinese,
            query,
            memory: memory.clone(),
            task: task.clone(),
            _memory: retained,
        })
    }

    pub(in crate::out_of_core) fn documents_digest(&self) -> Result<u64> {
        let receipt = streamed::write_record(
            &mut std::io::sink(),
            self.header(),
            &mut self.reader(),
            crate::SearchDocumentBody {
                bytes: self.bytes,
                expected_checksum: Some(self.checksum),
            },
            Control {
                memory: Some(crate::analyzer_memory::Memory::Build(&self.memory)),
                task: Some(&self.task),
                ..Control::default()
            },
        )?;
        let mut digest = crate::lexical_projection::DocumentsDigest::default();
        digest.add_record(receipt.checksum, receipt.bytes);
        Ok(digest.finish())
    }

    fn reader(&self) -> SourceReader<'_> {
        match self.query {
            Some(context) => SourceReader::Admitted(crate::build_memory::read::QueryFileRange {
                file: &self.body,
                expected_length: Some(self.bytes),
                offset: 0,
                remaining: self.bytes,
                context,
                require_execution: true,
            }),
            None => SourceReader::Legacy(RangeReader {
                file: &self.body,
                offset: 0,
                remaining: self.bytes,
            }),
        }
    }
}

enum SourceReader<'a> {
    Legacy(RangeReader<'a>),
    Admitted(crate::build_memory::read::QueryFileRange<'a>),
}
impl Read for SourceReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Legacy(reader) => reader.read(output),
            Self::Admitted(reader) => reader.read(output),
        }
    }
}

impl HeaderSource for Source<'_> {
    fn header(&self) -> Header<'_> {
        self.header.header()
    }
}
impl RecordSource for Source<'_> {
    fn encoded_len(&self, _: Option<&RuntimeTaskContext>) -> Result<usize> {
        usize::try_from(streamed::record_len(self.header(), self.bytes)?)
            .map_err(|_| invalid("encoded body size exceeds usize"))
    }
    fn write_encoded(&self, output: &mut impl std::io::Write) -> io::Result<()> {
        streamed::write_record(
            output,
            self.header(),
            &mut self.reader(),
            crate::SearchDocumentBody {
                bytes: self.bytes,
                expected_checksum: Some(self.checksum),
            },
            Control {
                memory: Some(crate::analyzer_memory::Memory::Build(&self.memory)),
                task: Some(&self.task),
                ..Control::default()
            },
        )
        .map(|_| ())
        .map_err(io::Error::other)
    }
}
impl DocumentSource for Source<'_> {
    fn id(&self) -> &str {
        &self.header.id
    }
    fn admit_source(&self, config: LexicalProjectionConfig) -> Result<()> {
        crate::lexical_projection::source::admit_streamed_source(self.header(), self.bytes, config)
    }
    fn visit_tokens(
        &self,
        analyzer: &SearchAnalyzerLexicon,
        control: Control<'_>,
        emit: &mut dyn FnMut(u8, usize, Term, TokenOccurrence) -> Result<()>,
    ) -> Result<()> {
        crate::lexical_projection::source::visit_streamed_source(
            self.header(),
            &mut self.reader(),
            self.bytes,
            analyzer,
            control,
            emit,
        )
    }
}
