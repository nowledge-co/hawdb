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

//! Metadata filtering borrows the query account and original file/I/O custody.

use super::*;
use crate::build_memory::{checked_add as add, reserve_capacity, MAP_ENTRY_BYTES};
use crate::lexical_projection::QueryContext;
use std::io::BufRead;

impl SearchOutOfCoreReader {
    pub(super) fn visit_query_metadata_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        context: QueryContext<'_>,
        visit: impl FnMut(&SearchMetadataDocument) -> Result<()>,
    ) -> Result<()> {
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let range = layout.metadata;
        let input = crate::build_memory::read::QueryFileRange {
            file: &artifact.metadata_payload,
            expected_length: Some(artifact.metadata_payload_length),
            offset: range.offset,
            remaining: range.length,
            context,
            require_execution: true,
        };
        let mut text_bytes = 0;
        hydration::read_validated_with_account(
            input,
            range.length,
            range.checksum,
            self.config.max_uncompressed_segment_bytes.get(),
            Some(hydration::DecodeAdmission {
                memory: context.memory,
                task: context.task,
                max_header_bytes: self.config.max_document_header_bytes.get(),
            }),
            |text| {
                text_bytes = visit_records(
                    text,
                    segment,
                    layout,
                    self.config.max_uncompressed_segment_bytes,
                    context,
                    visit,
                )?;
                Ok(())
            },
        )?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.metadata_segment_bytes_read = metrics
            .metadata_segment_bytes_read
            .saturating_add(range.length);
        metrics.peak_metadata_segment_bytes = metrics.peak_metadata_segment_bytes.max(text_bytes);
        Ok(())
    }
}

fn invalid(reason: &str) -> HawDBError {
    HawDBError::Storage(format!("search metadata {reason}"))
}

fn visit_records(
    text: &mut dyn BufRead,
    segment: &SearchSegmentDescriptorEntry,
    layout: &SearchOutOfCoreSegmentLayout,
    max_record_bytes: NonZeroU64,
    context: QueryContext<'_>,
    mut visit: impl FnMut(&SearchMetadataDocument) -> Result<()>,
) -> Result<u64> {
    let limit = usize::try_from(max_record_bytes.get()).unwrap_or(usize::MAX);
    // Scratch and decoded fields are admitted before allocation and released
    // only after their payloads. No segment-wide String or document Vec exists.
    let mut line_memory = context.memory.reserve(0)?;
    let mut previous_memory = context.memory.reserve(0)?;
    let mut line = Vec::new();
    let mut previous = Vec::new();
    let mut count = 0usize;
    let mut vector_count = 0usize;
    let mut text_bytes = 0u64;
    let mut header = false;
    while hydration::read_record(text, &mut line, limit, context.task, &mut line_memory)? {
        text_bytes = text_bytes.saturating_add(line.len() as u64);
        let record = std::str::from_utf8(&line).map_err(|_| invalid("non-ASCII record"))?;
        // Match str::lines() exactly, including a final record without LF.
        let record = match record.strip_suffix('\n') {
            Some(record) => record.strip_suffix('\r').unwrap_or(record),
            None => record,
        };
        if !header {
            if record != "HAWDB_SEARCH_METADATA_SEGMENT_V1" {
                return Err(invalid("invalid sidecar header"));
            }
            header = true;
            continue;
        }
        let mut fields = record.split('\t');
        let (Some("meta"), Some(raw_id), Some(raw_vector), Some(raw_metadata), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return Err(invalid("invalid sidecar line"));
        };
        count = add(count, 1)?;
        if count > layout.metadata.entry_count {
            return Err(invalid("sidecar count exceeded"));
        }
        let mut decode_bytes = add(size_of::<SearchMetadataDocument>(), hex_size(raw_id)?)?;
        if !raw_metadata.is_empty() {
            for pair in raw_metadata.split(';') {
                let (key, value) = pair
                    .split_once('=')
                    .ok_or_else(|| invalid("invalid metadata pair"))?;
                decode_bytes = add(
                    decode_bytes,
                    add(add(hex_size(key)?, hex_size(value)?)?, MAP_ENTRY_BYTES)?,
                )?;
            }
        }
        let _decode_memory = context.memory.reserve(decode_bytes)?;
        let document = SearchMetadataDocument {
            id: decode_string(raw_id)?,
            vector_ordinal: decode_optional_vector_ordinal(raw_vector)?,
            metadata: decode_metadata(raw_metadata)?,
        };
        if (count == 1 && document.id != segment.first_document_id)
            || (count > 1 && previous.as_slice() >= document.id.as_bytes())
        {
            return Err(invalid("sidecar bounds or ordering mismatch"));
        }
        if let Some(ordinal) = document.vector_ordinal {
            if Some(ordinal) != layout.vector_ordinal_base.checked_add(vector_count as u64) {
                return Err(invalid("vector ordinals differ from layout"));
            }
            vector_count = add(vector_count, 1)?;
        }
        previous.clear();
        reserve_capacity(&mut previous, document.id.len(), &mut previous_memory)?;
        previous.extend_from_slice(document.id.as_bytes());
        visit(&document)?;
    }
    if !header
        || count != layout.metadata.entry_count
        || count != segment.document_count
        || previous.as_slice() != segment.last_document_id.as_bytes()
        || vector_count != layout.vectors.entry_count
    {
        return Err(invalid("sidecar count, bounds or vector layout mismatch"));
    }
    Ok(text_bytes)
}

fn hex_size(raw: &str) -> Result<usize> {
    if !raw.len().is_multiple_of(2) || !raw.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("invalid hex field"));
    }
    Ok(raw.len() / 2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
    use std::io::Cursor;

    #[test]
    fn query_metadata_preserves_wire_line_endings() {
        let document = SearchDocument {
            id: "memory:1".into(),
            title: String::new(),
            content: String::new(),
            embedding: None,
            metadata: BTreeMap::new(),
        };
        let descriptor =
            SearchSegmentDescriptorEntry::from_documents(0, &[&document], &BTreeSet::new());
        let range = SearchOutOfCoreRange {
            offset: 0,
            length: 0,
            checksum: 0,
            entry_count: 1,
        };
        let layout = SearchOutOfCoreSegmentLayout {
            segment_id: 0,
            vector_ordinal_base: 0,
            metadata: range,
            vectors: SearchOutOfCoreRange {
                entry_count: 0,
                ..range
            },
        };
        let budget = NonZeroUsize::new(1 << 20).unwrap();
        let ledger = QueryMemoryLedger::new(budget);
        let account = ledger.account(QueryMemoryClass::ExternalRead, "metadata grammar", budget);
        let task = crate::RuntimeTaskContext::default();
        for ending in ["\n", "\r\n", "", "\r\r\n", "\r"] {
            let text = format!(
                "HAWDB_SEARCH_METADATA_SEGMENT_V1\nmeta\t{}\t-\t{ending}",
                crate::encode_string(&document.id),
            );
            let expected = decode_metadata_segment(&text, &descriptor, 1);
            let result = visit_records(
                &mut Cursor::new(text.as_bytes()),
                &descriptor,
                &layout,
                NonZeroU64::new(4096).unwrap(),
                QueryContext {
                    memory: &account,
                    task: &task,
                    execution: None,
                    io_scope: None,
                },
                |actual| {
                    assert_eq!(actual.id, document.id);
                    assert_eq!(actual.metadata, document.metadata);
                    Ok(())
                },
            );
            assert_eq!(result.is_ok(), expected.is_ok(), "{ending:?}: {result:?}");
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }

    #[test]
    fn query_metadata_streams_large_segment_and_refunds_refused_decode() {
        let documents = (0..512)
            .map(|number| SearchDocument {
                id: format!("memory:{number:04}"),
                title: String::new(),
                content: String::new(),
                embedding: None,
                metadata: BTreeMap::from([("body".into(), "x".repeat(4096))]),
            })
            .collect::<Vec<_>>();
        let descriptor = SearchSegmentDescriptorEntry::from_documents(
            0,
            &documents.iter().collect::<Vec<_>>(),
            &BTreeSet::new(),
        );
        let mut text = String::from("HAWDB_SEARCH_METADATA_SEGMENT_V1\n");
        for document in &documents {
            text.push_str(&format!(
                "meta\t{}\t-\t{}\n",
                crate::encode_string(&document.id),
                crate::encode_metadata(&document.metadata),
            ));
        }
        assert!(text.len() > 3 << 20);
        let bytes = crate::encode_search_snapshot_text(&text).unwrap();
        let range = SearchOutOfCoreRange {
            offset: 0,
            length: bytes.len() as u64,
            checksum: checksum_bytes(&bytes),
            entry_count: documents.len(),
        };
        let layout = SearchOutOfCoreSegmentLayout {
            segment_id: 0,
            vector_ordinal_base: 0,
            metadata: range,
            vectors: SearchOutOfCoreRange {
                entry_count: 0,
                ..range
            },
        };
        // The zstd frame advertises its own window: admit it even when the
        // selected records are tiny. The successful budget is below plaintext.
        for (budget, succeeds) in [(64 << 10, false), (1 << 20, false), (3 << 20, true)] {
            let budget = NonZeroUsize::new(budget).unwrap();
            let ledger = QueryMemoryLedger::new(budget);
            let account = ledger.account(QueryMemoryClass::ExternalRead, "query metadata", budget);
            let task = crate::RuntimeTaskContext::default();
            let context = QueryContext {
                memory: &account,
                task: &task,
                execution: None,
                io_scope: None,
            };
            let mut count = 0;
            let result = hydration::read_validated_with_account(
                Cursor::new(&bytes),
                range.length,
                range.checksum,
                text.len() as u64,
                Some(hydration::DecodeAdmission {
                    memory: &account,
                    task: &task,
                    max_header_bytes: 4096,
                }),
                |text| {
                    visit_records(
                        text,
                        &descriptor,
                        &layout,
                        NonZeroU64::new(16 << 10).unwrap(),
                        context,
                        |document| {
                            assert_eq!(document.id, documents[count].id);
                            assert_eq!(document.metadata, documents[count].metadata);
                            count += 1;
                            Ok(())
                        },
                    )
                },
            );
            if succeeds {
                assert_eq!(result.unwrap(), text.len() as u64);
                assert_eq!(count, documents.len());
            } else {
                assert!(
                    matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))),
                    "{result:?}"
                );
                assert_eq!(count, 0);
            }
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}
