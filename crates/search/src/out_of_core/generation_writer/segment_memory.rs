use crate::build_control::checkpoint;
use crate::build_io::{self, Buffer};
use crate::build_memory::{
    checked_add, checked_mul, AdmittedDocument, BuildMemory, SET_ENTRY_BYTES,
};
use crate::document_codec::write_hex;
use crate::error::{Result, SkeinError};
use crate::{
    search_document_field_value, search_document_field_values, search_segment_summary_value,
    SearchSegmentDescriptor, SearchSegmentDescriptorEntry, SearchSegmentFieldSummary,
    SearchSegmentPayloadRange,
};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

pub(super) fn grow_slots<T>(entries: &mut Vec<T>, lease: &mut QueryMemoryLease) -> Result<()> {
    if entries.len() < entries.capacity() {
        return Ok(());
    }
    let old = checked_mul(entries.capacity(), size_of::<T>())?;
    let capacity = checked_mul(entries.capacity().max(2), 2)?;
    let replacement = checked_mul(capacity, size_of::<T>())?;
    lease.grow(replacement)?;
    if let Err(error) = entries.try_reserve_exact(capacity - entries.len()) {
        lease.shrink(replacement);
        return Err(SkeinError::Execution(format!(
            "search segment directory allocation failed: {error}"
        )));
    }
    lease.shrink(old);
    Ok(())
}

// The caller retains this lease with its descriptor vector, including while
// the newly built entry is local and on every early-return path.
pub(super) fn descriptor(
    segment_id: u64,
    documents: &[AdmittedDocument],
    fields: &BTreeSet<String>,
    memory: &BuildMemory,
    retained: &mut QueryMemoryLease,
    task: &RuntimeTaskContext,
) -> Result<SearchSegmentDescriptorEntry> {
    checkpoint(task)?;
    let first = documents
        .first()
        .map_or("", |document| document.id.as_str());
    let last = documents.last().map_or("", |document| document.id.as_str());
    retained.grow(checked_add(first.len(), last.len())?)?;
    let first_document_id = first.to_owned();
    let last_document_id = last.to_owned();
    let mut metadata = BTreeMap::new();
    for field in fields {
        checkpoint(task)?;
        // The map value includes a set plus two range summaries. Cover B-tree
        // node occupancy and split overlap, not just the key's logical length.
        retained.grow(checked_add(2048, field.len())?)?;
        metadata.insert(field.clone(), SearchSegmentFieldSummary::default());
    }
    for document in documents {
        for field in fields {
            checkpoint(task)?;
            let raw = search_document_field_value(document, field);
            let list = field == "labels" || field.starts_with("metadata.");
            let scratch = if let Some(raw) = raw {
                if list {
                    let count = checked_add(raw.bytes().filter(|&byte| byte == b',').count(), 1)?;
                    // serde_json 1.x parses Vec<String>, then the existing label
                    // adapter trims into Cow strings. JSON has <= comma_count+1
                    // elements and decoded UTF-8 is no longer than its source.
                    // Include old/new vector growth, parser escape scratch,
                    // original and trimmed strings, including failed JSON fallback.
                    checked_add(
                        checked_mul(raw.len().max(16), 8)?,
                        checked_mul(count, 6 * size_of::<Cow<'_, str>>())?,
                    )?
                } else {
                    4 * size_of::<Cow<'_, str>>()
                }
            } else {
                0
            };
            let values_memory = memory.retained.reserve(scratch)?;
            #[cfg(test)]
            evidence::values();
            let values = search_document_field_values(document, field);
            let summary = metadata
                .get_mut(field)
                .expect("all requested fields were initialized");
            if !values.is_empty() {
                summary.present_count += 1;
            }
            for value in &values {
                checkpoint(task)?;
                // Includes std Unicode lowercase growth, ASCII enum copies and
                // the longest built-in kind alias (source_chunk).
                let normalization = checked_mul(value.len().max(16), 12)?;
                retained.grow(checked_add(normalization, SET_ENTRY_BYTES)?)?;
                let key = search_segment_summary_value(field, value);
                if key.capacity() > normalization {
                    return Err(SkeinError::Execution(
                        "search summary normalization exceeded preflight".to_owned(),
                    ));
                }
                retained.shrink(normalization - key.capacity());
                if summary.values.contains(&key) {
                    let release = checked_add(key.capacity(), SET_ENTRY_BYTES)?;
                    drop(key);
                    retained.shrink(release);
                } else {
                    summary.values.insert(key);
                }
                summary.update_range_summaries(value);
            }
            drop(values);
            drop(values_memory);
        }
    }
    Ok(SearchSegmentDescriptorEntry {
        segment_id,
        first_document_id,
        last_document_id,
        document_count: documents.len(),
        payload_range: None,
        metadata,
    })
}

pub(super) fn encode_descriptor(
    descriptor: &SearchSegmentDescriptor,
    limit: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Buffer> {
    build_io::formatted_with_checksum(limit, memory, task, |output| {
        writeln!(output, "SKEIN_SEARCH_SEGMENTS_V3")?;
        writeln!(output, "target_documents\t{}", descriptor.target_documents)?;
        writeln!(output, "document_count\t{}", descriptor.document_count)?;
        for segment in &descriptor.segments {
            let range = segment.payload_range.unwrap_or(SearchSegmentPayloadRange {
                artifact_id: 0,
                offset: 0,
                length: 0,
                checksum: 0,
            });
            write!(output, "segment\t{}\t", segment.segment_id)?;
            write_hex(output, &segment.first_document_id)?;
            output.write_char('\t')?;
            write_hex(output, &segment.last_document_id)?;
            writeln!(
                output,
                "\t{}\t{}\t{}\t{}\t{}",
                segment.document_count,
                range.artifact_id,
                range.offset,
                range.length,
                range.checksum
            )?;
            for (field, summary) in &segment.metadata {
                output.write_str("field\t")?;
                write_hex(output, field)?;
                write!(output, "\t{}\t", summary.present_count)?;
                for (index, value) in summary.values.iter().enumerate() {
                    if index != 0 {
                        output.write_char(',')?;
                    }
                    write_hex(output, value)?;
                }
                output.write_char('\t')?;
                if let Some(range) = summary.numeric_range {
                    write!(output, "{}\t{}", range.min, range.max)?;
                } else {
                    output.write_char('\t')?;
                }
                output.write_char('\t')?;
                if let Some(range) = summary.timestamp_range {
                    write!(
                        output,
                        "{}\t{}",
                        range.min_epoch_millis, range.max_epoch_millis
                    )?;
                } else {
                    output.write_char('\t')?;
                }
                output.write_char('\n')?;
            }
        }
        Ok(())
    })
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static VALUES: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn values() {
        VALUES.with(|count| count.set(count.get() + 1));
    }
    pub(in super::super) fn take() -> usize {
        VALUES.with(|count| count.replace(0))
    }
}
