// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual selected-materialization prototype; production decoding is unchanged.
use super::*;

pub(super) fn visit_records_selected(
    text: &mut dyn BufRead,
    context: RecordContext<'_>,
    mut selected: impl FnMut(u64) -> bool,
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
        let retain = selected(ordinal);
        let retained_dimension = if retain { dimension } else { 0 };
        let decode_bytes = add(
            size_of::<SearchVectorDocument>(),
            add(raw_id.len() / 2, mul(retained_dimension, size_of::<f32>())?)?,
        )?;
        let _decode_memory = query.memory.reserve(decode_bytes)?;
        let mut document = SearchVectorDocument {
            id: String::new(),
            embedding: Vec::with_capacity(retained_dimension),
        };
        if document.embedding.capacity() != retained_dimension {
            return Err(HawDBError::Execution(
                "vector row exceeded decode admission".into(),
            ));
        }
        if dimension == 0 {
            return Err(invalid("contains an empty embedding"));
        }
        let finite = if retain {
            // Keep the original parser and finite-value pass for retained rows.
            for (index, raw) in raw_embedding.split(',').enumerate() {
                if index.is_multiple_of(1024) {
                    query.checkpoint()?;
                }
                document.embedding.push(
                    raw.parse::<f32>()
                        .map_err(|_| invalid("has an invalid embedding value"))?,
                );
            }
            document.embedding.iter().all(|value| value.is_finite())
        } else {
            let mut finite = true;
            for (index, raw) in raw_embedding.split(',').enumerate() {
                if index.is_multiple_of(1024) {
                    query.checkpoint()?;
                }
                // Preserve validation and parser-error precedence for excluded
                // rows, reusing the existing conservative decimal guard.
                if !is_common_finite_decimal(raw) {
                    let value = raw
                        .parse::<f32>()
                        .map_err(|_| invalid("has an invalid embedding value"))?;
                    finite &= value.is_finite();
                }
            }
            finite
        };
        if !finite || context.dimension != Some(dimension) {
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
        if retain {
            visit(&document)?;
        }
    }
    if !header || count != context.layout.vectors.entry_count || invalid_layout {
        return Err(invalid("count, bounds or ordering mismatch"));
    }
    Ok(bytes)
}
