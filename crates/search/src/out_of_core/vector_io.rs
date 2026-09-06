//! Allocation preflight for the raw vector sidecar shared by exact and rerank scans.

use super::{query_io, SearchVectorDocument};
use crate::{Result, RuntimeTaskContext, SkeinError};
use std::mem::size_of;

pub(super) fn fields(line: &str) -> Result<(&str, &str, &str)> {
    let mut fields = line.split('\t');
    match (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) {
        (Some("vector"), Some(ordinal), Some(id), Some(embedding), None) => {
            Ok((ordinal, id, embedding))
        }
        _ => Err(invalid("fields")),
    }
}

pub(super) fn preflight(
    text: &str,
    expected: usize,
    dimension: Option<usize>,
    ordinal_base: u64,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    query_io::checkpoint(task)?;
    let mut lines = text.lines();
    if lines.next() != Some("SKEIN_SEARCH_VECTOR_SEGMENT_V1") {
        return Err(invalid("header"));
    }
    let mut bytes = query_io::mul(expected, size_of::<SearchVectorDocument>())?;
    let mut count = 0;
    for line in lines {
        query_io::checkpoint(task)?;
        if count == expected {
            return Err(invalid("count"));
        }
        let (ordinal, id, embedding) = fields(line)?;
        let expected_ordinal = ordinal_base
            .checked_add(count as u64)
            .ok_or_else(|| invalid("ordinal overflow"))?;
        if ordinal.parse::<u64>().ok() != Some(expected_ordinal) {
            return Err(invalid("ordinal"));
        }
        if !id.len().is_multiple_of(2) || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid("id encoding"));
        }
        let dimension = dimension
            .filter(|&value| value > 0)
            .ok_or_else(|| invalid("dimension"))?;
        let mut coordinates = 0;
        for raw in embedding.split(',') {
            query_io::checkpoint(task)?;
            if coordinates == dimension || !raw.parse::<f32>().is_ok_and(f32::is_finite) {
                return Err(invalid("embedding"));
            }
            coordinates += 1;
        }
        if coordinates != dimension {
            return Err(invalid("dimension"));
        }
        bytes = query_io::add(
            bytes,
            query_io::add(id.len() / 2, query_io::mul(dimension, size_of::<f32>())?)?,
        )?;
        count += 1;
    }
    if count != expected {
        return Err(invalid("count"));
    }
    Ok(bytes)
}

fn invalid(field: &str) -> SkeinError {
    SkeinError::Storage(format!("search vector sidecar has invalid {field}"))
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static DECODES: Cell<usize> = const { Cell::new(0) }; }
    pub(in super::super) fn decode() {
        DECODES.with(|value| value.set(value.get() + 1));
    }
    pub(in super::super) fn take() -> usize {
        DECODES.with(|value| value.replace(0))
    }
}
