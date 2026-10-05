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

use super::*;

#[derive(Clone, Copy)]
pub(crate) enum SegmentKind {
    Documents,
    Metadata { vector_ordinal_base: u64 },
    Vectors { vector_ordinal_base: u64 },
}

impl SegmentKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Documents => "document",
            Self::Metadata { .. } => "metadata",
            Self::Vectors { .. } => "vector",
        }
    }
}

pub(crate) struct SegmentEncoding<'a, T> {
    documents: &'a [T],
    kind: SegmentKind,
    bytes: usize,
}

impl<'a, T: RecordSource> SegmentEncoding<'a, T> {
    #[cfg(test)]
    pub(crate) fn new(documents: &'a [T], kind: SegmentKind) -> Result<Self> {
        Self::new_with_context(documents, kind, None)
    }

    pub(crate) fn new_with_context(
        documents: &'a [T],
        kind: SegmentKind,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Self> {
        if matches!(kind, SegmentKind::Documents) {
            let bytes = documents.iter().try_fold(
                "HAWDB_SEARCH_SEGMENT_V1\n".len(),
                |bytes, document| {
                    bytes
                        .checked_add(document.encoded_len(task)?)
                        .ok_or_else(|| HawDBError::Storage("search segment length overflow".into()))
                },
            )?;
            return Ok(Self {
                documents,
                kind,
                bytes,
            });
        }
        let mut length = EncodedLength::default();
        write_segment(
            &mut CheckedSink {
                sink: &mut length,
                task,
            },
            documents,
            kind,
        )
        .map_err(|_| {
            if let Some(error) = task.and_then(|task| crate::build_control::checkpoint(task).err())
            {
                return error;
            }
            HawDBError::Storage(format!(
                "search {} segment encoded size overflow",
                kind.name()
            ))
        })?;
        Ok(Self {
            documents,
            kind,
            bytes: length.0,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes
    }

    pub(crate) fn write_to(&self, writer: &mut impl io::Write) -> io::Result<()> {
        if matches!(self.kind, SegmentKind::Documents) {
            writer.write_all(b"HAWDB_SEARCH_SEGMENT_V1\n")?;
            for document in self.documents {
                document.write_encoded(writer)?;
            }
            return Ok(());
        }
        IoSink {
            writer,
            error: None,
            remaining: self.bytes,
        }
        .write_checked(|sink| write_segment(sink, self.documents, self.kind))
    }
}

fn write_segment<T: RecordSource>(
    sink: &mut impl DocumentSink,
    documents: &[T],
    kind: SegmentKind,
) -> fmt::Result {
    let mut ordinal = match kind {
        SegmentKind::Documents => {
            sink.write_str("HAWDB_SEARCH_SEGMENT_V1\n")?;
            0
        }
        SegmentKind::Metadata {
            vector_ordinal_base,
        } => {
            sink.write_str("HAWDB_SEARCH_METADATA_SEGMENT_V1\n")?;
            vector_ordinal_base
        }
        SegmentKind::Vectors {
            vector_ordinal_base,
        } => {
            sink.write_str("HAWDB_SEARCH_VECTOR_SEGMENT_V1\n")?;
            vector_ordinal_base
        }
    };
    for document in documents {
        let document = document.header();
        match kind {
            SegmentKind::Documents => {
                unreachable!("document records use their admitted encoded ranges")
            }
            SegmentKind::Metadata { .. } => {
                sink.write_str("meta\t")?;
                sink.write_hex(document.id)?;
                sink.write_char('\t')?;
                if document.embedding.is_some() {
                    write!(sink, "{ordinal}")?;
                } else {
                    sink.write_char('-')?;
                }
                sink.write_char('\t')?;
                write_metadata(sink, document.metadata)?;
                sink.write_char('\n')?;
            }
            SegmentKind::Vectors { .. } => {
                if let Some(embedding) = document.embedding {
                    write!(sink, "vector\t{ordinal}\t")?;
                    sink.write_hex(document.id)?;
                    sink.write_char('\t')?;
                    write_embedding(sink, Some(embedding))?;
                    sink.write_char('\n')?;
                }
            }
        }
        if document.embedding.is_some() {
            ordinal = ordinal.saturating_add(1);
        }
    }
    Ok(())
}
