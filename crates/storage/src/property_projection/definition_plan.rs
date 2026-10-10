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

//! Subject grouping and composite-name decoding admitted before allocation.
//! The inventory outlives every grouped vector/string, including error cleanup.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointOperationError,
};

pub(super) struct DefinitionPlan {
    by_subject: BTreeMap<ProjectionSubject, Vec<PreparedProjectionDefinition>>,
    // Field order keeps all data alive only while its leases are retained.
    _memory: CheckpointAllocationOwner,
}

impl std::ops::Deref for DefinitionPlan {
    type Target = BTreeMap<ProjectionSubject, Vec<PreparedProjectionDefinition>>;

    fn deref(&self) -> &Self::Target {
        &self.by_subject
    }
}

pub(super) fn prepare(
    definitions: &[PersistentPropertyProjectionDefinition],
    work: &CheckpointWorkContext,
) -> Result<DefinitionPlan, PersistentPropertyProjectionError> {
    // Shared vector helpers preserve typed work errors through classify once
    // for this plan; string diagnostics are never interpreted as error types.
    work.classify(|work| {
        let memory = CheckpointDecodeContext {
            work: work.clone(),
            memory: std::cell::RefCell::default(),
        };
        let mut by_subject: BTreeMap<ProjectionSubject, Vec<PreparedProjectionDefinition>> =
            BTreeMap::new();
        // The pinned-toolchain bound includes a String key, which conservatively
        // covers the smaller, fixed-size ProjectionSubject key. Each vector and
        // composite string has a separate exact-capacity lease.
        let mut tree_memory = crate::projection::predicate_checkpoint::decode::MapMemory::<
            Vec<PreparedProjectionDefinition>,
        >::default();
        for (index, definition) in definitions.iter().enumerate() {
            let value_source =
                if definition.kind == PersistentPropertyProjectionKind::CompositeEquality {
                    ProjectionValueSource::Composite(decode_composite_property_identity_inner(
                        &definition.property,
                        Some(&memory),
                    )?)
                } else {
                    ProjectionValueSource::Scalar
                };
            let unit = work.start_unit()?;
            let subject = definition_subject(definition);
            if !by_subject.contains_key(&subject) {
                tree_memory
                    .before_insert(by_subject.len(), &memory)
                    .map_err(|error| {
                        PersistentPropertyProjectionError::Source(error.to_string())
                    })?;
            }
            let bucket = by_subject.entry(subject).or_default();
            unit.finish();
            memory
                .push(
                    bucket,
                    PreparedProjectionDefinition {
                        definition_index: index,
                        value_source,
                    },
                )
                .map_err(|error| PersistentPropertyProjectionError::Source(error.to_string()))?;
        }
        work.checkpoint()?;
        Ok(DefinitionPlan {
            by_subject,
            _memory: memory.memory.into_inner(),
        })
    })
    .map_err(|error| match error {
        CheckpointOperationError::Work(error) => PersistentPropertyProjectionError::Work(error),
        CheckpointOperationError::Operation(error) => error,
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
