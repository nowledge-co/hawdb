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
use crate::build_control::checkpoint;
use crate::build_memory::{checked_add, checked_mul, MAP_ENTRY_BYTES, SET_ENTRY_BYTES};
use crate::{
    normalized_projection_kind, search_document_field_value, search_field_is_enum_like,
    SearchSegmentFieldSummary,
};
use hawdb_core::RuntimeTaskContext;
use std::borrow::{Borrow, Cow};
use std::collections::BTreeMap;

mod values;

pub(super) struct Admission<'a> {
    pub(super) memory: &'a BuildMemory,
    pub(super) task: &'a RuntimeTaskContext,
    pub(super) retained: &'a mut QueryMemoryLease,
}

#[cfg(test)]
pub(super) fn build<T: Borrow<SearchDocument>>(
    segment_id: u64,
    documents: &[T],
    fields: &BTreeSet<String>,
    retained_bytes: u64,
    max_bytes: u64,
) -> Result<(SearchSegmentDescriptorEntry, u64)> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task)?;
    let mut retained = memory.retained.reserve(0)?;
    build_with_context(
        segment_id,
        documents,
        fields,
        retained_bytes,
        max_bytes,
        Admission {
            memory: &memory,
            task: &task,
            retained: &mut retained,
        },
    )
}

pub(super) fn build_with_context<T: Borrow<SearchDocument>>(
    segment_id: u64,
    documents: &[T],
    fields: &BTreeSet<String>,
    retained_bytes: u64,
    max_bytes: u64,
    admission: Admission<'_>,
) -> Result<(SearchSegmentDescriptorEntry, u64)> {
    let Admission {
        memory,
        task,
        retained,
    } = admission;
    checkpoint(task)?;
    let mut budget = DescriptorBudget {
        bytes: retained_bytes,
        max_bytes,
    };
    let first = documents
        .first()
        .map_or("", |document| document.borrow().id.as_str());
    let last = documents
        .last()
        .map_or("", |document| document.borrow().id.as_str());
    // Retain the existing descriptor and layout estimates. Charge each owned
    // component before inserting it instead of checking after segment I/O.
    budget.reserve(256 + 96)?;
    budget.reserve(first.len() as u64)?;
    budget.reserve(last.len() as u64)?;
    retained.grow(checked_add(first.len(), last.len())?)?;
    let mut descriptor = SearchSegmentDescriptorEntry {
        segment_id,
        first_document_id: first.to_owned(),
        last_document_id: last.to_owned(),
        document_count: documents.len(),
        payload_range: None,
        metadata: BTreeMap::new(),
    };
    for field in fields {
        checkpoint(task)?;
        retained.grow(checked_add(MAP_ENTRY_BYTES, field.len())?)?;
        budget.reserve(192u64.saturating_add(field.len() as u64))?;
        descriptor
            .metadata
            .insert(field.clone(), SearchSegmentFieldSummary::default());
    }
    for document in documents {
        let document = document.borrow();
        for (field, summary) in &mut descriptor.metadata {
            let mut present = false;
            values::visit_with_context(document, field, memory, task, &mut |value| {
                present = true;
                let (normalized, normalization_bytes) =
                    admitted_summary_value(field, value, retained, task)?;
                if summary.values.contains(normalized.as_ref()) {
                    drop(normalized);
                    if normalization_bytes != 0 {
                        retained.shrink(normalization_bytes);
                    }
                } else {
                    budget.reserve(32u64.saturating_add(normalized.len() as u64))?;
                    let capacity = match &normalized {
                        Cow::Borrowed(value) => {
                            retained.grow(value.len())?;
                            0
                        }
                        Cow::Owned(value) => value.capacity(),
                    };
                    retained.grow(SET_ENTRY_BYTES)?;
                    summary.values.insert(normalized.into_owned());
                    if normalization_bytes != capacity {
                        retained.shrink(normalization_bytes - capacity);
                    }
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
            return Err(HawDBError::Storage(format!(
                "search generation descriptor working set requires {projected} bytes, exceeding {}",
                self.max_bytes
            )));
        }
        self.bytes = projected;
        Ok(())
    }
}

#[cfg(test)]
fn summary_value<'a>(field: &str, value: &'a str) -> Cow<'a, str> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let mut retained = memory.retained.reserve(0).unwrap();
    admitted_summary_value(field, value, &mut retained, &task)
        .unwrap()
        .0
}

fn admitted_summary_value<'a>(
    field: &str,
    value: &'a str,
    retained: &mut QueryMemoryLease,
    task: &RuntimeTaskContext,
) -> Result<(Cow<'a, str>, usize)> {
    checkpoint(task)?;
    if field == "kind"
        && let Some(kind) = normalized_projection_kind(value)
    {
        return Ok((Cow::Borrowed(kind), 0));
    }
    let trimmed = value.trim();
    let ascii = search_field_is_enum_like(field);
    let mut changes = false;
    for (index, character) in trimmed.chars().enumerate() {
        if index % 2048 == 0 {
            checkpoint(task)?;
        }
        changes = if ascii {
            character.is_ascii_uppercase()
        } else {
            let mut lowercase = character.to_lowercase();
            lowercase.next() != Some(character) || lowercase.next().is_some()
        };
        if changes {
            break;
        }
    }
    if !changes {
        return Ok((Cow::Borrowed(trimmed), 0));
    }
    // Cover Unicode expansion and old/new String growth. Keep the standard
    // whole-value mapping, including contextual final sigma.
    let bytes = checked_mul(trimmed.len().max(8), 12)?;
    retained.grow(bytes)?;
    let normalized = if ascii {
        trimmed.to_ascii_lowercase()
    } else {
        trimmed.to_lowercase()
    };
    if normalized.capacity() > bytes {
        return Err(HawDBError::Execution(
            "search summary normalization exceeded admission".into(),
        ));
    }
    checkpoint(task)?;
    Ok((Cow::Owned(normalized), bytes))
}

#[cfg(test)]
mod tests;
