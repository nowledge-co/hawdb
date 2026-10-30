// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! One vector record is decoded under the original query account and I/O.

use super::*;
use crate::build_memory::{checked_add as add, checked_mul as mul, reserve_capacity};
use crate::lexical_projection::QueryContext;
use std::io::BufRead;

#[cfg(test)]
mod layout_probe;
#[cfg(test)]
mod tests;

impl SearchOutOfCoreReader {
    pub(super) fn visit_query_vector_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        context: QueryContext<'_>,
        visit: impl FnMut(&SearchVectorDocument) -> Result<()>,
    ) -> Result<()> {
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let range = layout.vectors;
        let input = crate::build_memory::read::QueryFileRange {
            file: &artifact.vector_payload,
            expected_length: Some(artifact.vector_payload_length),
            offset: range.offset,
            remaining: range.length,
            context,
            require_execution: true,
        };
        let bytes = hydration::read_validated_with_account(
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
                visit_records(
                    text,
                    RecordContext {
                        segment,
                        layout,
                        dimension: self.manifest.embedding_dimension,
                        limit: self.config.max_uncompressed_segment_bytes,
                        query: context,
                    },
                    visit,
                )
            },
        )?;
        metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(1);
        metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(range.length);
        metrics.vector_segment_bytes_read = metrics
            .vector_segment_bytes_read
            .saturating_add(range.length);
        metrics.peak_vector_segment_bytes = metrics.peak_vector_segment_bytes.max(bytes);
        Ok(())
    }
}

struct RecordContext<'a> {
    segment: &'a SearchSegmentDescriptorEntry,
    layout: &'a SearchOutOfCoreSegmentLayout,
    dimension: Option<usize>,
    limit: NonZeroU64,
    query: QueryContext<'a>,
}
fn invalid(reason: &str) -> HawDBError {
    HawDBError::Storage(format!("search vector sidecar {reason}"))
}

fn visit_records(
    text: &mut dyn BufRead,
    context: RecordContext<'_>,
    mut visit: impl FnMut(&SearchVectorDocument) -> Result<()>,
) -> Result<u64> {
    let query = context.query;
    let mut line_memory = query.memory.reserve(0)?;
    let mut previous_memory = query.memory.reserve(0)?;
    let mut line = Vec::new();
    let mut previous = Vec::new();
    let mut header = false;
    let mut count = 0usize;
    let mut bytes = 0u64;
    let mut invalid_layout = false;
    let limit = usize::try_from(context.limit.get()).unwrap_or(usize::MAX);
    while hydration::read_record(text, &mut line, limit, query.task, &mut line_memory)? {
        bytes = bytes.saturating_add(line.len() as u64);
        let record = std::str::from_utf8(&line).map_err(|_| invalid("is not ASCII"))?;
        let record = match record.strip_suffix('\n') {
            Some(record) => record.strip_suffix('\r').unwrap_or(record),
            None => record,
        };
        if !header {
            if record != "HAWDB_SEARCH_VECTOR_SEGMENT_V1" {
                return Err(invalid("has an invalid header"));
            }
            header = true;
            continue;
        }
        let mut fields = record.split('\t');
        let (Some("vector"), Some(raw_ordinal), Some(raw_id), Some(raw_embedding), None) = (
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
            fields.next(),
        ) else {
            return Err(invalid("has an invalid line"));
        };
        let ordinal = raw_ordinal
            .parse::<u64>()
            .map_err(|_| invalid("has an invalid ordinal"))?;
        if count == context.layout.vectors.entry_count {
            return Err(invalid("count exceeded"));
        }
        let dimension = if raw_embedding.is_empty() {
            0
        } else {
            raw_embedding.split(',').count()
        };
        if !raw_id.len().is_multiple_of(2) {
            return Err(invalid("has an invalid ID"));
        }
        let decode_bytes = add(
            size_of::<SearchVectorDocument>(),
            add(raw_id.len() / 2, mul(dimension, size_of::<f32>())?)?,
        )?;
        let _decode_memory = query.memory.reserve(decode_bytes)?;
        let mut document = SearchVectorDocument {
            id: String::new(),
            embedding: Vec::with_capacity(dimension),
        };
        if document.embedding.capacity() != dimension {
            return Err(HawDBError::Execution(
                "vector row exceeded decode admission".into(),
            ));
        }
        if dimension == 0 {
            return Err(invalid("contains an empty embedding"));
        }
        for (index, raw) in raw_embedding.split(',').enumerate() {
            if index.is_multiple_of(1024) {
                query.checkpoint()?;
            }
            document.embedding.push(
                raw.parse::<f32>()
                    .map_err(|_| invalid("has an invalid embedding value"))?,
            );
        }
        if document.embedding.iter().any(|value| !value.is_finite())
            || context.dimension != Some(dimension)
        {
            return Err(invalid("has an invalid embedding"));
        }
        document.id = decode_string(raw_id)?;
        invalid_layout |= Some(ordinal)
            != context.layout.vector_ordinal_base.checked_add(count as u64)
            || (count != 0 && previous.as_slice() >= document.id.as_bytes())
            || document.id < context.segment.first_document_id
            || document.id > context.segment.last_document_id;
        count = add(count, 1)?;
        previous.clear();
        reserve_capacity(&mut previous, document.id.len(), &mut previous_memory)?;
        previous.extend_from_slice(document.id.as_bytes());
        visit(&document)?;
    }
    if !header || count != context.layout.vectors.entry_count || invalid_layout {
        return Err(invalid("count, bounds or ordering mismatch"));
    }
    Ok(bytes)
}
