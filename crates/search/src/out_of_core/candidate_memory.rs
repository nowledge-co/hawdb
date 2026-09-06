use super::{query_io, CandidateBlock, SearchMetadataDocument, SearchSegmentDescriptorEntry};
use crate::query_memory::Admitted;
use crate::{Result, RuntimeTaskContext, SearchPredicateOp, SearchPredicateSet, SkeinError};
use skein_executor::QueryMemoryAccount;
use std::borrow::Cow;
use std::mem::size_of;

pub(super) fn directory_bytes(segments: &[SearchSegmentDescriptorEntry]) -> Result<usize> {
    let mut bytes = query_io::mul(segments.len(), size_of::<CandidateBlock>())?;
    for segment in segments {
        bytes = query_io::add(
            bytes,
            query_io::add(
                segment.first_document_id.len(),
                segment.last_document_id.len(),
            )?,
        )?;
    }
    Ok(bytes)
}

fn predicate_scratch(
    document: &crate::SearchDocument,
    predicates: &SearchPredicateSet,
) -> Result<usize> {
    let raw = document
        .metadata
        .values()
        .map(String::len)
        .chain([document.id.len(), 16])
        .max()
        .unwrap();
    let commas = document
        .metadata
        .values()
        .map(|value| value.bytes().filter(|&ch| ch == b',').count())
        .max()
        .unwrap_or(0);
    let expected = predicates
        .predicates()
        .iter()
        .map(|predicate| match predicate.op() {
            SearchPredicateOp::Eq(value)
            | SearchPredicateOp::Gt(value)
            | SearchPredicateOp::Gte(value)
            | SearchPredicateOp::Lt(value)
            | SearchPredicateOp::Lte(value) => value.as_str().len(),
            SearchPredicateOp::In(values) | SearchPredicateOp::NotIn(values) => values
                .iter()
                .map(|value| value.as_str().len())
                .max()
                .unwrap_or(0),
            SearchPredicateOp::Exists | SearchPredicateOp::IsMissing => 0,
        })
        .max()
        .unwrap_or(0)
        .max(16);
    // One field's JSON/CSV values and two simultaneous normalized comparisons.
    // Match the build-side list/lowercase envelopes; no predicate semantics change.
    query_io::add(
        query_io::add(
            query_io::mul(raw, 8)?,
            query_io::mul(query_io::add(commas, 1)?, 6 * size_of::<Cow<'_, str>>())?,
        )?,
        query_io::add(query_io::mul(query_io::add(raw, expected)?, 12)?, 512)?,
    )
}

pub(super) fn encode(
    documents: &[SearchMetadataDocument],
    predicates: &SearchPredicateSet,
    block_limit: u64,
    spill_remaining: u64,
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
) -> Result<(Admitted<Vec<u8>>, usize)> {
    query_io::checkpoint(task)?;
    let _selection_memory = memory.reserve(query_io::mul(documents.len(), size_of::<usize>())?)?;
    let mut selected = Vec::with_capacity(documents.len());
    let mut size = 0usize;
    for (index, entry) in documents.iter().enumerate() {
        query_io::checkpoint(task)?;
        let _scratch = memory.reserve(predicate_scratch(&entry.document, predicates)?)?;
        if crate::search_document_matches_predicates(&entry.document, predicates) {
            u32::try_from(entry.document.id.len())
                .map_err(|_| SkeinError::Storage("search candidate id exceeds u32".to_owned()))?;
            size = query_io::add(size, query_io::add(12, entry.document.id.len())?)?;
            if size as u64 > block_limit {
                return Err(SkeinError::Storage(
                    "search candidate block exceeds its byte budget".to_owned(),
                ));
            }
            if size as u64 > spill_remaining {
                return Err(SkeinError::Storage(
                    format!("search candidate spill requires {size} bytes, exceeding remaining {spill_remaining}"),
                ));
            }
            selected.push(index);
        }
    }
    let lease = memory.reserve(size)?;
    #[cfg(test)]
    evidence::encode();
    let mut bytes = Vec::with_capacity(size);
    for &index in &selected {
        query_io::checkpoint(task)?;
        let entry = &documents[index];
        let id = &entry.document.id;
        bytes.extend_from_slice(&(id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(id.as_bytes());
        bytes.extend_from_slice(&entry.vector_ordinal.unwrap_or(u64::MAX).to_le_bytes());
    }
    Ok((Admitted::new(bytes, lease), selected.len()))
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static ENCODES: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn encode() {
        ENCODES.with(|v| v.set(v.get() + 1));
    }
    pub(in super::super) fn take() -> usize {
        ENCODES.with(|v| v.replace(0))
    }
}
