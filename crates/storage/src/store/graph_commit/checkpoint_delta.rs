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

//! Cooperative private replay delta estimation. The ordinary estimate remains
//! an independent reference. Touched-ID tree capacity is admitted before insert
//! and retained through scratch destruction. Mounted-record hydration, allocator
//! latency and cleanup latency still need separate qualification.

use super::*;
use crate::background::{CheckpointAllocationOwner, CheckpointWorkContext};

impl GraphStore {
    pub(in crate::store) fn ensure_checkpoint_out_of_core_delta_replay_admission(
        &self,
        ops: &[WalOp],
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        if !self.canonical_base_out_of_core {
            return Ok(());
        }
        let Some(limit) = self.max_out_of_core_delta_bytes else {
            return Ok(());
        };
        let mut nodes = TouchedIds::default();
        let mut relationships = TouchedIds::default();
        let additional = self.estimated_checkpoint_mutation_delta_bytes(
            ops,
            &mut nodes,
            &mut relationships,
            work,
        )?;
        let projected = self
            .estimated_delta_resident_bytes()
            .saturating_add(additional);
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        if projected > limit {
            return Err(HawDBError::Storage(format!(
                "out-of-core mutation delta admission rejected {projected} estimated bytes under the {limit} byte limit; checkpoint the database or raise max_out_of_core_delta_bytes"
            )));
        }
        Ok(())
    }

    fn estimated_checkpoint_mutation_delta_bytes(
        &self,
        ops: &[WalOp],
        touched_nodes: &mut TouchedIds<NodeId>,
        touched_relationships: &mut TouchedIds<RelId>,
        work: &CheckpointWorkContext,
    ) -> Result<u64> {
        let mut bytes = 0u64;
        for op in ops {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            match op {
                WalOp::CreateNode { id, properties, .. } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_nodes.insert(*id, work)? && !self.nodes.contains_key(id) {
                        bytes = bytes.saturating_add(64).saturating_add(
                            estimate_properties(properties, work)?.saturating_mul(2),
                        );
                    }
                }
                WalOp::SetNodeProperty {
                    id,
                    property,
                    value,
                } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_nodes.insert(*id, work)?
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes.saturating_add(estimate_node(&node, work)?);
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimate_value(value, work)?.saturating_mul(2));
                }
                WalOp::DeleteNode { id } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_nodes.insert(*id, work)?
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimate_node(&node, work)?)
                            .saturating_add(32);
                    }
                }
                WalOp::CreateRelationship { id, properties, .. } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_relationships.insert(*id, work)?
                        && !self.relationships.contains_key(id)
                    {
                        bytes = bytes.saturating_add(160).saturating_add(
                            estimate_properties(properties, work)?.saturating_mul(2),
                        );
                    }
                }
                WalOp::SetRelationshipProperty {
                    id,
                    property,
                    value,
                } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_relationships.insert(*id, work)?
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimate_relationship(&relationship, work)?)
                            .saturating_add(96);
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimate_value(value, work)?.saturating_mul(2));
                }
                WalOp::DeleteRelationship { id } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    if touched_relationships.insert(*id, work)?
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimate_relationship(&relationship, work)?)
                            .saturating_add(128);
                    }
                }
                WalOp::Batch(batch) => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                    bytes = bytes.saturating_add(self.estimated_checkpoint_mutation_delta_bytes(
                        batch,
                        touched_nodes,
                        touched_relationships,
                        work,
                    )?);
                }
                WalOp::CreateNodeLabel { .. }
                | WalOp::CreateRelationshipType { .. }
                | WalOp::CreateNodeTable { .. }
                | WalOp::CreateRelationshipTable { .. }
                | WalOp::CreateProperty { .. }
                | WalOp::AlterTableState { .. }
                | WalOp::AlterPropertyState { .. }
                | WalOp::GcTableDescriptor { .. }
                | WalOp::GcPropertyDescriptor { .. }
                | WalOp::CreateIndex { .. }
                | WalOp::CreateCompositeIndex { .. }
                | WalOp::CreateRangeIndex { .. }
                | WalOp::CreateFullTextIndex { .. }
                | WalOp::CreateUniqueConstraint { .. }
                | WalOp::CreateNodePropertyExistsConstraint { .. }
                | WalOp::CreateRelationshipUniqueConstraint { .. }
                | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
                | WalOp::ProjectGraph { .. }
                | WalOp::MarkInitialImportSource { .. }
                | WalOp::Relational { .. }
                | WalOp::RelationalSnapshot { .. }
                | WalOp::Append { .. } => {
                    unit.finish();
                    work.checkpoint().map_err(HawDBError::from_storage_error)?;
                }
            }
        }
        Ok(bytes)
    }
}

struct TouchedIds<T> {
    // Tree nodes are destroyed before their retained capacity leases.
    ids: BTreeSet<T>,
    memory: CheckpointAllocationOwner,
    admitted_nodes: usize,
}

impl<T> Default for TouchedIds<T> {
    fn default() -> Self {
        Self {
            ids: BTreeSet::new(),
            memory: CheckpointAllocationOwner::default(),
            admitted_nodes: 0,
        }
    }
}

impl<T: Ord> TouchedIds<T> {
    fn insert(&mut self, id: T, work: &CheckpointWorkContext) -> Result<bool> {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        if self.ids.contains(&id) {
            unit.finish();
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            return Ok(false);
        }
        // Pinned Rust 1.97.1 uses B=6. Cover an internal node's 11 keys,
        // parent metadata, 12 child pointers and eight alignment gaps. The
        // insertion-only tree has >=4 keys per transient non-root node; a
        // root and one new split node are covered separately.
        let alignment = std::mem::align_of::<T>().max(std::mem::align_of::<usize>());
        let node_bytes = std::mem::size_of::<Option<std::ptr::NonNull<u8>>>()
            + 2 * std::mem::size_of::<u16>()
            + 11 * std::mem::size_of::<T>()
            + 12 * std::mem::size_of::<std::ptr::NonNull<u8>>()
            + 8 * (alignment - 1);
        let nodes = self.ids.len().saturating_add(1) / 4 + 2;
        if nodes > self.admitted_nodes {
            let bytes = (nodes - self.admitted_nodes)
                .checked_mul(node_bytes)
                .ok_or_else(|| {
                    HawDBError::from_storage_error(work.record_failure(
                        crate::background::CheckpointWorkError::Allocation {
                            bytes: u64::MAX,
                            reason: "checkpoint touched-ID capacity overflows usize".into(),
                        },
                    ))
                })?;
            self.memory
                .reserve(bytes, work)
                .map_err(HawDBError::from_storage_error)?;
            self.admitted_nodes = nodes;
        }
        let inserted = self.ids.insert(id);
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        Ok(inserted)
    }
}

fn estimate_properties(
    properties: &BTreeMap<String, Value>,
    work: &CheckpointWorkContext,
) -> Result<u64> {
    let mut bytes = 0u64;
    for (key, value) in properties {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        bytes = bytes.saturating_add(key.len() as u64).saturating_add(16);
        unit.finish();
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        bytes = bytes.saturating_add(estimate_value(value, work)?);
    }
    Ok(bytes)
}

fn estimate_value(value: &Value, work: &CheckpointWorkContext) -> Result<u64> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let bytes = match value {
        Value::Null | Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len() as u64,
        Value::Binary(value) => value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(_) => 16,
        Value::Map(_) => 0,
    };
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    match value {
        Value::List(values) => {
            let mut bytes = bytes;
            for value in values {
                bytes = bytes.saturating_add(estimate_value(value, work)?);
            }
            Ok(bytes)
        }
        Value::Map(values) => estimate_properties(values, work),
        _ => Ok(bytes),
    }
}

fn estimate_node(node: &NodeRecord, work: &CheckpointWorkContext) -> Result<u64> {
    let bytes = 32u64.saturating_add((node.labels.len() as u64).saturating_mul(4));
    Ok(bytes.saturating_add(estimate_properties(&node.properties, work)?))
}

fn estimate_relationship(relationship: &RelRecord, work: &CheckpointWorkContext) -> Result<u64> {
    Ok(40u64.saturating_add(estimate_properties(&relationship.properties, work)?))
}
