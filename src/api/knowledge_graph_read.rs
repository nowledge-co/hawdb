// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Retrieval-owned canonical point reads; callers keep the before-copy permit.

use super::*;
use hawdb_executor::graph_seed::property_text;
use hawdb_executor::store::{admit_graph_read, AdjacencyReadMemory, ScanControl};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_storage::read_view::{AdmittedNodeRead, GraphReadAllocation};

/// Retains ownership admission for graph output until final result accounting.
/// Temporary graph collections use a separate instance with lexical lifetime.
pub(super) struct KnowledgeOwnedState {
    allocation: std::cell::RefCell<QueryMemoryLease>,
    sources: std::cell::RefCell<Vec<Box<dyn GraphReadAllocation>>>,
}

impl KnowledgeOwnedState {
    pub(super) fn new(account: &QueryMemoryAccount) -> Result<Self> {
        Ok(Self {
            allocation: std::cell::RefCell::new(account.reserve(0)?),
            sources: std::cell::RefCell::new(Vec::new()),
        })
    }

    pub(super) fn reserve(&self, bytes: usize) -> Result<()> {
        self.allocation.borrow_mut().grow(bytes)
    }

    pub(super) fn reserve_vec<T>(&self, values: &mut Vec<T>) -> Result<()> {
        if values.len() == values.capacity() {
            let capacity = values
                .capacity()
                .checked_mul(2)
                .map(|n| n.max(4))
                .ok_or_else(|| {
                    HawDBError::Execution("knowledge output capacity overflow".into())
                })?;
            let bytes = (capacity - values.capacity())
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| {
                    HawDBError::Execution("knowledge output capacity overflow".into())
                })?;
            self.reserve(bytes)?;
            values.reserve_exact(capacity - values.len());
        }
        Ok(())
    }

    pub(super) fn retain_source(&self, source: Box<dyn GraphReadAllocation>) -> Result<()> {
        let mut sources = self.sources.borrow_mut();
        self.reserve_vec(&mut sources)?;
        sources.push(source);
        Ok(())
    }

    pub(super) fn release_for_result(&self, account: &QueryMemoryAccount) -> Result<()> {
        *self.sources.borrow_mut() = Vec::new();
        *self.allocation.borrow_mut() = account.reserve(0)?;
        Ok(())
    }
}

pub(super) struct KnowledgeReadNode {
    pub(super) node: NodeRecord,
    _allocation: Box<dyn GraphReadAllocation>,
}

pub(super) fn node(
    store: &impl crate::executor::ExecutionStore,
    id: NodeId,
    labels: Option<&[crate::schema::LabelId]>,
    account: &QueryMemoryAccount,
) -> Result<Option<KnowledgeReadNode>> {
    match store.node_with_allocation(
        id,
        labels,
        &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
            &mut |bytes| admit_graph_read(account, None, bytes).map(Some),
            None,
        ),
    )? {
        AdmittedNodeRead::Node(node) => {
            let (node, allocation) = node.into_parts();
            Ok(Some(KnowledgeReadNode {
                node,
                _allocation: allocation,
            }))
        }
        AdmittedNodeRead::Missing => Ok(None),
        AdmittedNodeRead::Stopped => Err(HawDBError::Execution(
            "knowledge point read stopped before source admission".into(),
        )),
    }
}

fn identity_matches(
    node: &NodeRecord,
    external_id: &str,
    account: &QueryMemoryAccount,
) -> Result<bool> {
    if let Some(value) = node.properties.get("id") {
        let (text, _allocation) = property_text(value, account, None)?;
        if !text.is_empty() {
            return Ok(text == external_id);
        }
    }
    // Decimal identity fallback is at most twenty bytes.
    let _allocation = account.reserve(20)?;
    Ok(node.id.0.to_string() == external_id)
}

pub(super) fn seed_by_external_id(
    catalog: &Catalog,
    store: &impl crate::executor::ExecutionStore,
    label: &str,
    external_id: &str,
    account: &QueryMemoryAccount,
) -> Result<Option<KnowledgeReadNode>> {
    let Some(label_id) = catalog.label_id(label) else {
        return Ok(None);
    };
    let _lookup_allocation = account.reserve(
        external_id
            .len()
            .checked_add(128 + 11 * std::mem::size_of::<String>() + 2)
            .ok_or_else(|| {
                HawDBError::Execution("knowledge identity lookup size overflow".into())
            })?,
    )?;
    let numeric = external_id.parse::<i64>().ok();
    let values = [
        Value::String(external_id.to_string()),
        Value::Int(numeric.unwrap_or_default()),
    ];
    let values = &values[..if numeric.is_some() { 2 } else { 1 }];
    let properties = BTreeSet::from(["id".to_string()]);
    let mut found = None;
    store.visit_projected_nodes_by_property_admitted(
        label_id,
        "id",
        values,
        &properties,
        &mut hawdb_storage::read_view::GraphReadAllocator::new(
            &mut |bytes| admit_graph_read(account, None, bytes),
            None,
        ),
        &mut |projected| {
            // Keep the projected read's permit while admitting full ownership.
            if let Some(candidate) = node(store, projected.id, Some(&[label_id]), account)?
                && identity_matches(&candidate.node, external_id, account)?
            {
                found = Some(candidate);
                return Ok(ScanControl::Stop);
            }
            Ok(ScanControl::Continue)
        },
    )?;
    if found.is_some() {
        return Ok(found);
    }
    if let Ok(id) = external_id.parse::<u64>()
        && let Some(candidate) = node(store, NodeId(id), Some(&[label_id]), account)?
        && identity_matches(&candidate.node, external_id, account)?
    {
        return Ok(Some(candidate));
    }
    Ok(None)
}

pub(super) struct KnowledgeReadRelationship {
    pub(super) relationship: RelRecord,
    pub(super) _allocation: Box<dyn GraphReadAllocation>,
}

pub(super) fn relationship(
    store: &impl crate::executor::ExecutionStore,
    id: RelId,
    account: &QueryMemoryAccount,
) -> Result<Option<KnowledgeReadRelationship>> {
    match store.relationship_with_allocation(
        id,
        &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
            &mut |bytes| admit_graph_read(account, None, bytes).map(Some),
            None,
        ),
    )? {
        hawdb_storage::read_view::AdmittedRelationshipRead::Relationship(relationship) => {
            let (relationship, allocation) = relationship.into_parts();
            Ok(Some(KnowledgeReadRelationship {
                relationship,
                _allocation: allocation,
            }))
        }
        hawdb_storage::read_view::AdmittedRelationshipRead::Missing => Ok(None),
        hawdb_storage::read_view::AdmittedRelationshipRead::Stopped => Err(HawDBError::Execution(
            "knowledge relationship point read stopped before admission".into(),
        )),
    }
}

pub(super) struct KnowledgeReadEdge {
    pub(super) direction: KnowledgeGraphPathDirection,
    pub(super) next_node: NodeId,
    pub(super) relationship: RelRecord,
    _allocation: Box<dyn GraphReadAllocation>,
}

pub(super) fn visit_expansion_edges(
    store: &impl crate::executor::ExecutionStore,
    node_id: NodeId,
    budget_bytes: usize,
    account: &QueryMemoryAccount,
    mut visit: impl FnMut(KnowledgeReadEdge) -> Result<ScanControl>,
) -> Result<ScanControl> {
    for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
        let mut stopped = false;
        store.visit_ordered_adjacent_relationships_with_allocation(
            node_id,
            None,
            direction,
            AdjacencyReadMemory {
                budget_bytes,
                account: Some(account),
            },
            &mut hawdb_storage::read_view::ControlledGraphReadAllocator::new(
                &mut |bytes| admit_graph_read(account, None, bytes).map(Some),
                None,
            ),
            &mut |relationship| {
                let (relationship, allocation) = relationship.into_parts();
                let next_node = match direction {
                    AdjacencyDirection::Outgoing => relationship.target,
                    AdjacencyDirection::Incoming => relationship.source,
                };
                // The caller applies scope and deduplication before its output limit.
                let control = visit(KnowledgeReadEdge {
                    direction: knowledge_path_direction_for_adjacency(direction),
                    next_node,
                    relationship,
                    _allocation: allocation,
                })?;
                stopped = control == ScanControl::Stop;
                Ok(control)
            },
        )?;
        if stopped {
            return Ok(ScanControl::Stop);
        }
    }
    Ok(ScanControl::Continue)
}
