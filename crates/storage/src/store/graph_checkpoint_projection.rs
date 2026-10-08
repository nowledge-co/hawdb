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

//! Controlled projection arrays retain admission with their data. Selector and
//! adjacency tree coverage uses the existing conservative pinned map bound.
//! Legacy owned scan inputs and allocator/destruction costs remain separate
//! ledger gaps; this owner does not qualify the entire producer or candidate.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointWorkContext,
};
use crate::projection::predicate_checkpoint::decode::MapMemory;
use crate::projection::ProjectedRelationshipPredicate;
use std::borrow::Borrow;
use std::cell::RefCell;

#[derive(Debug)]
pub(super) struct CheckpointProjectedGraphData {
    data: ProjectedGraphArtifactData,
    _memory: CheckpointAllocationOwner,
}
impl std::ops::Deref for CheckpointProjectedGraphData {
    type Target = ProjectedGraphArtifactData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl Borrow<ProjectedGraphArtifactData> for CheckpointProjectedGraphData {
    fn borrow(&self) -> &ProjectedGraphArtifactData {
        &self.data
    }
}
impl PartialEq<ProjectedGraphArtifactData> for CheckpointProjectedGraphData {
    fn eq(&self, other: &ProjectedGraphArtifactData) -> bool {
        self.data == *other
    }
}
#[derive(Default)]
struct Neighbors {
    entries: BTreeSet<usize>,
    memory: MapMemory<()>,
}

pub(super) fn checkpoint_projected_graph_from_definition(
    catalog: &Catalog,
    store: &GraphStore,
    definition: &ProjectedGraphDefinition,
    work: &CheckpointWorkContext,
) -> Result<CheckpointProjectedGraphData> {
    let allocation = CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    };
    // All temporary containers die before their conservative admission owner.
    // Move the completed arrays with that owner; borrowed encoding never detaches it.
    let data = build(catalog, store, definition, &allocation)?;
    Ok(CheckpointProjectedGraphData {
        data,
        _memory: allocation.memory.into_inner(),
    })
}

fn build(
    catalog: &Catalog,
    store: &GraphStore,
    definition: &ProjectedGraphDefinition,
    allocation: &CheckpointDecodeContext,
) -> Result<ProjectedGraphArtifactData> {
    let work = &allocation.work;
    let scratch = CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    };
    let mut labels = BTreeSet::new();
    let mut label_memory = MapMemory::<()>::default();
    for name in &definition.node_labels {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        if let Some(id) = catalog.label_id(name)
            && !labels.contains(&id)
        {
            label_memory.before_insert(labels.len(), &scratch)?;
            labels.insert(id);
        }
        unit.finish();
    }
    let mut rel_types = BTreeSet::new();
    let mut rel_memory = MapMemory::<()>::default();
    for name in &definition.rel_types {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        if let Some(id) = catalog.rel_type_id(name)
            && !rel_types.contains(&id)
        {
            rel_memory.before_insert(rel_types.len(), &scratch)?;
            rel_types.insert(id);
        }
        unit.finish();
    }
    let mut relationship_predicates = BTreeMap::new();
    let mut predicate_memory = MapMemory::<&ProjectedRelationshipPredicate>::default();
    // Match the independent ordinary all-label/all-type fast path.
    if !definition.node_labels.is_empty() || !definition.rel_types.is_empty() {
        for (name, predicate) in &definition.relationship_predicates {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if let Some(id) = catalog.rel_type_id(name) {
                if !relationship_predicates.contains_key(&id) {
                    predicate_memory.before_insert(relationship_predicates.len(), &scratch)?;
                }
                relationship_predicates.insert(id, predicate);
            }
            unit.finish();
        }
    }
    let mut nodes = Vec::new();
    if !definition.node_labels.is_empty() && labels.is_empty() {
        let mut csr_offsets = Vec::new();
        let mut csc_offsets = Vec::new();
        allocation.push(&mut csr_offsets, 0)?;
        allocation.push(&mut csc_offsets, 0)?;
        return ProjectedGraphArtifactData::new_with_work_context(
            nodes,
            csr_offsets,
            Vec::new(),
            csc_offsets,
            Vec::new(),
            work,
        );
    }
    let mut source = store
        .checkpoint_node_records_owned(work)?
        .checkpoint_steps();
    loop {
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        let next = source.next();
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let Some(record) = next else {
            unit.finish();
            break;
        };
        store.poison_on_storage_error(&record);
        let Some(node) = record? else {
            unit.finish();
            continue;
        };
        let selected = labels.is_empty() || node.labels.iter().any(|label| labels.contains(label));
        unit.finish();
        if selected {
            allocation.push(&mut nodes, node.id)?;
        }
    }
    drop(source);
    let mut outgoing = Vec::new();
    let mut incoming = Vec::new();
    for _ in &nodes {
        scratch.push(&mut outgoing, Neighbors::default())?;
        scratch.push(&mut incoming, Neighbors::default())?;
    }
    if definition.rel_types.is_empty() || !rel_types.is_empty() {
        let mut source = store
            .checkpoint_relationship_records_owned(work)?
            .checkpoint_steps();
        loop {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            let next = source.next();
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let Some(record) = next else {
                unit.finish();
                break;
            };
            store.poison_on_storage_error(&record);
            let Some(relationship) = record? else {
                unit.finish();
                continue;
            };
            let matches_type = rel_types.is_empty() || rel_types.contains(&relationship.rel_type);
            let predicate = relationship_predicates.get(&relationship.rel_type);
            unit.finish();
            if !matches_type {
                continue;
            }
            if let Some(predicate) = predicate
                && !predicate.matches_with_work_context(&relationship.properties, work)?
            {
                continue;
            }
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if let Ok(source) = nodes.binary_search(&relationship.source)
                && let Ok(target) = nodes.binary_search(&relationship.target)
            {
                let out = &mut outgoing[source];
                if !out.entries.contains(&target) {
                    out.memory.before_insert(out.entries.len(), &scratch)?;
                    out.entries.insert(target);
                }
                let back = &mut incoming[target];
                if !back.entries.contains(&source) {
                    back.memory.before_insert(back.entries.len(), &scratch)?;
                    back.entries.insert(source);
                }
            }
            unit.finish();
        }
    }
    let (csr_offsets, csr_targets) = flatten(outgoing, allocation)?;
    let (csc_offsets, csc_sources) = flatten(incoming, allocation)?;
    ProjectedGraphArtifactData::new_with_work_context(
        nodes,
        csr_offsets,
        csr_targets,
        csc_offsets,
        csc_sources,
        work,
    )
}

fn flatten(
    adjacency: Vec<Neighbors>,
    allocation: &CheckpointDecodeContext,
) -> Result<(Vec<usize>, Vec<usize>)> {
    let mut offsets = Vec::new();
    let mut targets = Vec::new();
    allocation.push(&mut offsets, 0)?;
    for neighbors in adjacency {
        for target in neighbors.entries {
            allocation.push(&mut targets, target)?;
        }
        allocation.push(&mut offsets, targets.len())?;
    }
    allocation
        .checkpoint()
        .map_err(HawDBError::from_storage_error)?;
    Ok((offsets, targets))
}
