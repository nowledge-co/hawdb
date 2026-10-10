// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Shared borrowed projection preparation before output-row admission.

use super::{insert_projected_value, ProjectedValue};
use crate::binding::{binding_memory_bytes_with_values, Binding};
use crate::pipeline::{AccountedBindingBatch, BatchControl, BindingBatch};
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::{Result, Value};
use hawdb_plan_cypher::{Projection, ProjectionExpression};
use std::collections::BTreeMap;

pub(crate) fn prepare_borrowed_projection<'a>(
    items: &'a [Projection],
    working: &QueryMemoryAccount,
    annotations: Option<&'a Binding>,
    mut evaluate: impl FnMut(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
) -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)> {
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
    let map_bytes = count
        .saturating_mul(std::mem::size_of::<(String, ProjectedValue<'_>)>() * 3)
        .saturating_add(key_bytes)
        .saturating_add(annotation_bytes)
        .saturating_add(items.iter().map(|item| item.name.len()).max().unwrap_or(0));
    let layout = working.reserve(map_bytes)?;
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
    mut prepare: impl FnMut() -> Result<(BTreeMap<String, ProjectedValue<'a>>, QueryMemoryLease)>,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    crate::pipeline::runtime_checkpoint(task_context)?;
    let empty = Binding::values(BTreeMap::new());
    let (mut values, mut layout) = prepare()?;
    let estimate = |values: &BTreeMap<String, ProjectedValue<'_>>| {
        binding_memory_bytes_with_values(
            &empty,
            values
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_ref())),
        )
    };
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
    let control = output.push_generated(
        bytes,
        || Binding::values(own_projection_values(values, layout)),
        emit,
    )?;
    if control == BatchControl::Continue {
        crate::pipeline::runtime_checkpoint(task_context)?;
    }
    Ok(control)
}
