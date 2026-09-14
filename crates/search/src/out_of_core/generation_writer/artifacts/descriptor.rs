use super::*;
use crate::{
    normalized_projection_kind, search_document_field_value, search_field_is_enum_like,
    SearchSegmentFieldSummary,
};
use std::borrow::Cow;
use std::collections::BTreeMap;

mod values;

pub(super) fn build(
    segment_id: u64,
    documents: &[SearchDocument],
    fields: &BTreeSet<String>,
    retained_bytes: u64,
    max_bytes: u64,
) -> Result<(SearchSegmentDescriptorEntry, u64)> {
    let mut budget = DescriptorBudget {
        bytes: retained_bytes,
        max_bytes,
    };
    let first = documents
        .first()
        .map_or("", |document| document.id.as_str());
    let last = documents.last().map_or("", |document| document.id.as_str());
    // Retain the existing descriptor and layout estimates. Charge each owned
    // component before inserting it instead of checking after segment I/O.
    budget.reserve(256 + 96)?;
    budget.reserve(first.len() as u64)?;
    budget.reserve(last.len() as u64)?;
    let mut descriptor = SearchSegmentDescriptorEntry {
        segment_id,
        first_document_id: first.to_owned(),
        last_document_id: last.to_owned(),
        document_count: documents.len(),
        payload_range: None,
        metadata: BTreeMap::new(),
    };
    for field in fields {
        budget.reserve(192u64.saturating_add(field.len() as u64))?;
        descriptor
            .metadata
            .insert(field.clone(), SearchSegmentFieldSummary::default());
    }
    for document in documents {
        for (field, summary) in &mut descriptor.metadata {
            let mut present = false;
            values::visit(document, field, &mut |value| {
                present = true;
                let normalized = summary_value(field, value);
                if !summary.values.contains(normalized.as_ref()) {
                    budget.reserve(32u64.saturating_add(normalized.len() as u64))?;
                    summary.values.insert(normalized.into_owned());
                }
                summary.update_range_summaries(value);
                Ok(())
            })?;
            summary.present_count += usize::from(present);
        }
    }
    Ok((descriptor, budget.bytes))
}

struct DescriptorBudget {
    bytes: u64,
    max_bytes: u64,
}

impl DescriptorBudget {
    fn reserve(&mut self, bytes: u64) -> Result<()> {
        let projected = self.bytes.saturating_add(bytes);
        if projected > self.max_bytes {
            return Err(SkeinError::Storage(format!(
                "search generation descriptor working set requires {projected} bytes, exceeding {}",
                self.max_bytes
            )));
        }
        self.bytes = projected;
        Ok(())
    }
}

fn summary_value<'a>(field: &str, value: &'a str) -> Cow<'a, str> {
    if field == "kind"
        && let Some(kind) = normalized_projection_kind(value)
    {
        return Cow::Borrowed(kind);
    }
    let trimmed = value.trim();
    if search_field_is_enum_like(field) {
        if trimmed.bytes().any(|byte| byte.is_ascii_uppercase()) {
            Cow::Owned(trimmed.to_ascii_lowercase())
        } else {
            Cow::Borrowed(trimmed)
        }
    } else if trimmed.chars().any(|character| {
        let mut lowercase = character.to_lowercase();
        lowercase.next() != Some(character) || lowercase.next().is_some()
    }) {
        // Keep the standard whole-value mapping, including contextual final
        // sigma. This one-value normalization allocation remains a resident
        // unit; it is not a second retained dictionary or an altered analyzer.
        Cow::Owned(trimmed.to_lowercase())
    } else {
        Cow::Borrowed(trimmed)
    }
}

#[cfg(test)]
mod tests;
