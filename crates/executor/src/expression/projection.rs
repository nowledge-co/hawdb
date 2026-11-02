// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Shared borrowed projection preparation before output-row admission.

use super::{insert_projected_value, ProjectedValue};
use crate::binding::{binding_memory_bytes_with_values, Binding};
use crate::pipeline::{AccountedBindingBatch, BatchControl, BindingBatch};
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{Result, Value};
use hawdb_plan_cypher::{Projection, ProjectionExpression};
use std::collections::BTreeMap;

fn projection_preparation_bytes(items: &[Projection], annotations: Option<&Binding>) -> usize {
    let count = items.len().saturating_add(2);
    let suffix_bytes = count.ilog10() as usize + 2;
    let key_bytes = items.iter().fold(0usize, |total, item| {
        total
            .saturating_add(item.name.len())
            .saturating_add(suffix_bytes)
    });
    let annotation_bytes = annotations.map_or(0, |binding| {
        crate::scoring::borrowed_seed_annotations(binding)
            .map(|(name, _)| name.len())
            .sum::<usize>()
    });
    count
        .saturating_mul(std::mem::size_of::<(String, ProjectedValue<'_>)>() * 3)
        .saturating_add(key_bytes)
        .saturating_add(annotation_bytes)
        .saturating_add(items.iter().map(|item| item.name.len()).max().unwrap_or(0))
}

pub(crate) fn prepare_borrowed_projection<'a>(
    items: &'a [Projection],
    working: &QueryMemoryAccount,
    annotations: Option<&'a Binding>,
    mut evaluate: impl FnMut(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
) -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)> {
    let layout = working.reserve(projection_preparation_bytes(items, annotations))?;
    let mut values = BTreeMap::new();
    for item in items {
        insert_projected_value(&mut values, &item.name, evaluate(&item.expression)?);
    }
    if let Some(binding) = annotations {
        for (name, value) in crate::scoring::borrowed_seed_annotations(binding) {
            values.insert(name.into(), ProjectedValue::borrowed(value));
        }
    }
    Ok((values, layout))
}

pub(crate) fn own_projection_values(
    values: BTreeMap<String, ProjectedValue<'_>>,
    layout: QueryMemoryLease,
) -> BTreeMap<String, Value> {
    let output = values
        .into_iter()
        .map(|(name, value)| (name, value.into_owned()))
        .collect();
    drop(layout);
    output
}

/// Optimized producers project values only. Release preparation for the next
/// row before a byte-boundary callback, and recreate it only after Continue.
pub(crate) fn push_borrowed_projection<'a>(
    output: &mut AccountedBindingBatch,
    prepare: impl FnMut() -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)>,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let empty = Binding::values(BTreeMap::new());
    push_prepared_projection(
        output,
        prepare,
        |values| {
            binding_memory_bytes_with_values(
                &empty,
                values
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_ref())),
            )
        },
        |values, layout| Binding::values(own_projection_values(values, layout)),
        task_context,
        emit,
    )
}

const SMALL_PROJECTION_CAPACITY: usize = 8;
type SmallProjectionValues<'a> = Vec<ProjectedValue<'a>>;

/// Validated small, simple projections with unique output names.
/// Duplicate aliases retain the general projection's suffix allocation rules.
#[derive(Clone, Copy)]
pub(crate) struct SmallBorrowedProjection<'plan> {
    items: &'plan [Projection],
    preparation_bytes: usize,
}

impl<'plan> SmallBorrowedProjection<'plan> {
    pub(crate) fn new(items: &'plan [Projection]) -> Option<Self> {
        if items.is_empty() || items.len() > SMALL_PROJECTION_CAPACITY {
            return None;
        }
        if items.iter().any(|item| {
            !matches!(
                item.expression,
                ProjectionExpression::Property { .. }
                    | ProjectionExpression::Id { .. }
                    | ProjectionExpression::Literal(_)
            )
        }) {
            return None;
        }
        if items.iter().enumerate().any(|(index, item)| {
            items[..index]
                .iter()
                .any(|previous| previous.name == item.name)
        }) {
            return None;
        }
        Some(Self {
            items,
            // Preserve the existing conservative workspace allowance, including
            // its refusal boundary, while avoiding its temporary owned map.
            preparation_bytes: projection_preparation_bytes(items, None).max(
                std::mem::size_of::<SmallProjectionValues<'_>>().saturating_add(
                    items
                        .len()
                        .saturating_mul(std::mem::size_of::<ProjectedValue<'_>>()),
                ),
            ),
        })
    }

    pub(crate) fn push<'a>(
        &'a self,
        output: &mut AccountedBindingBatch,
        working: &QueryMemoryAccount,
        mut evaluate: impl FnMut(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let empty = Binding::values(BTreeMap::new());
        push_prepared_projection(
            output,
            || {
                let layout = working.reserve(self.preparation_bytes)?;
                let mut values: SmallProjectionValues<'a> = Vec::with_capacity(self.items.len());
                for item in self.items {
                    values.push(evaluate(&item.expression)?);
                }
                Ok((values, layout))
            },
            |values| {
                binding_memory_bytes_with_values(
                    &empty,
                    self.items
                        .iter()
                        .zip(values)
                        .map(|(item, value)| (item.name.as_str(), value.as_ref())),
                )
            },
            |values, layout| {
                let owned = values
                    .into_iter()
                    .zip(self.items)
                    .map(|(value, item)| (item.name.clone(), value.into_owned()))
                    .collect();
                drop(layout);
                Binding::values(owned)
            },
            task_context,
            emit,
        )
    }
}

fn push_prepared_projection<T>(
    output: &mut AccountedBindingBatch,
    mut prepare: impl FnMut() -> Result<(T, QueryMemoryLease)>,
    estimate: impl Fn(&T) -> usize,
    own: impl FnOnce(T, QueryMemoryLease) -> Binding,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    crate::pipeline::runtime_checkpoint(task_context)?;
    let (mut values, mut layout) = prepare()?;
    let mut bytes = estimate(&values);
    output.check_row_size(bytes)?;
    if !output.is_empty() && output.would_exceed_payload(bytes) {
        drop(values);
        drop(layout);
        if output.emit(emit)? == BatchControl::Stop {
            return Ok(BatchControl::Stop);
        }
        crate::pipeline::runtime_checkpoint(task_context)?;
        (values, layout) = prepare()?;
        bytes = estimate(&values);
    }
    crate::pipeline::runtime_checkpoint(task_context)?;
    let control = output.push_generated(bytes, || own(values, layout), emit)?;
    if control == BatchControl::Continue {
        crate::pipeline::runtime_checkpoint(task_context)?;
    }
    Ok(control)
}

#[cfg(test)]
mod tests;
