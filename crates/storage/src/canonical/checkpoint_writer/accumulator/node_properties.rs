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

//! Retain the ordered label/property cursor and unfinished key-vector append.
//! Individual wide value hashing still uses the shared streaming codec.
use super::*;
use crate::background::{CheckpointText, CheckpointVectorPush};
use std::ops::Bound::{Excluded, Unbounded};

pub(super) struct Progress {
    id: NodeId,
    labels: usize,
    properties: usize,
    label: Option<LabelId>,
    previous_label: Option<LabelId>,
    previous_property: Option<CheckpointText>,
    property: Option<CheckpointText>,
    key: Option<CheckpointVectorPush<u64>>,
    phase: usize,
    label_recorded: bool,
}

pub(super) fn add(
    output: &mut Accumulator,
    node: &NodeRecord,
    work: &CheckpointWorkContext,
) -> Result<(), CanonicalSegmentError> {
    work.classify(|scoped| {
        let context = CheckpointDecodeContext {
            work: scoped.clone(),
            memory: RefCell::new(std::mem::take(&mut output.memory)),
        };
        let result = resume(output, node, &context);
        output.memory = context.memory.into_inner();
        result
    })
    .map_err(|error| match error {
        CheckpointOperationError::Work(error) => CanonicalSegmentError::Work(error),
        CheckpointOperationError::Operation(error) => error,
    })
}

// The caller retains the immutable captured node across attempts.
fn resume(
    output: &mut Accumulator,
    node: &NodeRecord,
    context: &CheckpointDecodeContext,
) -> Result<(), CanonicalSegmentError> {
    if let Some(state) = &output.pending_properties
        && (state.id != node.id
            || state.labels != node.labels.len()
            || state.properties != node.properties.len())
    {
        return Err(CanonicalSegmentError::Corrupt(
            "retained property source identity changed".into(),
        ));
    }
    if node.labels.is_empty() {
        context.checkpoint()?;
        return Ok(());
    }
    let state = output.pending_properties.get_or_insert_with(|| Progress {
        id: node.id,
        labels: node.labels.len(),
        properties: node.properties.len(),
        label: None,
        previous_label: None,
        previous_property: None,
        property: None,
        key: None,
        phase: 0,
        label_recorded: false,
    });
    if state.id != node.id
        || state.labels != node.labels.len()
        || state.properties != node.properties.len()
    {
        return Err(CanonicalSegmentError::Corrupt(
            "retained property source identity changed".into(),
        ));
    }
    loop {
        if state.phase == 0 {
            let unit = context.start_unit()?;
            if state.label.is_none() {
                state.label = match state.previous_label {
                    Some(previous) => node
                        .labels
                        .range((Excluded(previous), Unbounded))
                        .next()
                        .copied(),
                    None => node.labels.first().copied(),
                };
            }
            if state.label.is_none() {
                state.phase = 5;
            } else if !state.label_recorded {
                state.key = Some(CheckpointVectorPush::new(u64::from(
                    state.label.expect("selected label").0,
                )));
                state.phase = 6;
            } else if next_property(node, state.previous_property.as_deref()).is_some() {
                state.property = Some(CheckpointText::new());
                state.phase = 1;
            } else {
                state.previous_label = state.label.take();
                state.previous_property = None;
                state.label_recorded = false;
            }
            unit.finish();
        }
        if state.phase == 6 {
            context
                .resume_push(&mut output.segment.node_label_keys, &mut state.key)
                .map_err(source)?;
            state.label_recorded = true;
            state.phase = 0;
            continue;
        }
        if state.phase == 1 {
            let unit = context.start_unit()?;
            let (key, _) =
                next_property(node, state.previous_property.as_deref()).ok_or_else(|| {
                    CanonicalSegmentError::Corrupt("captured property cursor disappeared".into())
                })?;
            unit.finish();
            let property = state.property.as_mut().expect("selected property");
            let start = property.len();
            property.append(&key[start..], context).map_err(source)?;
            state.phase = 2;
        }
        if state.phase == 2 {
            let unit = context.start_unit()?;
            let property = state.property.as_ref().expect("copied property");
            let value = node.properties.get(&**property).ok_or_else(|| {
                CanonicalSegmentError::Corrupt("captured property disappeared".into())
            })?;
            unit.finish();
            let key = checkpoint_bloom::key(
                state.label.expect("selected label"),
                property,
                value,
                Some(context),
            )?;
            state.key = Some(CheckpointVectorPush::new(key));
            state.phase = 3;
        }
        if state.phase == 3 {
            context
                .resume_push(&mut output.segment.node_property_keys, &mut state.key)
                .map_err(source)?;
            state.phase = 4;
        }
        if state.phase == 4 {
            state.previous_property = state.property.take();
            state.phase = 0;
        }
        if state.phase == 5 {
            context.checkpoint()?;
            output.pending_properties = None;
            return Ok(());
        }
    }
}

fn next_property<'a>(
    node: &'a NodeRecord,
    previous: Option<&str>,
) -> Option<(&'a String, &'a Value)> {
    match previous {
        Some(previous) => node
            .properties
            .range::<str, _>((Excluded(previous), Unbounded))
            .next(),
        None => node.properties.first_key_value(),
    }
}
