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

//! Shortest-path and relationship traversal operators.

use crate::binding::{
    binding_memory_bytes, relationship_memory_bytes, value_memory_bytes, value_payload_bytes,
    Binding,
};
use crate::blocking::in_memory_report;
use crate::kernel::{push_bounded_operator_binding, OperatorMemoryTracker};
use crate::observer::ExecutionObserver;
use crate::pipeline::{runtime_checkpoint, AccountedBindingSet};
use crate::predicate::{
    combine_property_filters, label_ids_for_pattern, node_matches_label_pattern,
    node_matches_property_filter, property_filter_from_properties, property_filter_matches_values,
    relationship_properties_match,
};
use crate::store::{AdjacencyReadMemory, GraphExecutionRead, ScanControl};
use crate::{
    ExecutionLimit, ExecutionMemoryConfig, QueryMemoryAccount, QueryMemoryClass, QueryMemoryLedger,
};
use hawdb_core::{
    Catalog, HawDBError, LabelId, RelTypeId, RelationshipDirection, Result, RuntimeTaskContext,
    Value,
};
use hawdb_plan_cypher::{
    RelationshipCountFilter, RelationshipCountLeg, ShortestPathProjection,
    ShortestPathProjectionExpression,
};
use hawdb_storage::read_view::{AdmittedNodeRead, AdmittedNodeRecord};
use hawdb_storage::{
    adjacency::AdjacencyDirection, mutation::PropertyFilter, NodeId, NodeRecord, RelRecord,
};
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

mod shortest_path;
use shortest_path::{search_shortest_paths, ShortestPathSearchResult};

pub struct ShortestPathExecInput<'a> {
    pub source_label: &'a str,
    pub source_id: &'a Value,
    pub source_visibility_filter: Option<&'a PropertyFilter>,
    pub path_node_visibility_filter: Option<&'a PropertyFilter>,
    pub rel_type: &'a str,
    pub direction: RelationshipDirection,
    pub target_label: &'a str,
    pub target_id: &'a Value,
    pub target_visibility_filter: Option<&'a PropertyFilter>,
    pub min_hops: usize,
    pub max_hops: usize,
    pub returns: &'a [ShortestPathProjection],
}

#[derive(Clone, Copy)]
pub struct TraversalExecutionContext<'a> {
    pub memory: &'a ExecutionMemoryConfig,
    pub memory_ledger: &'a QueryMemoryLedger,
    pub task_context: Option<&'a RuntimeTaskContext>,
    pub observer: &'a dyn ExecutionObserver,
}

pub fn execute_shortest_path(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    input: ShortestPathExecInput<'_>,
    execution_limit: ExecutionLimit,
    context: TraversalExecutionContext<'_>,
) -> Result<AccountedBindingSet> {
    let TraversalExecutionContext {
        memory,
        memory_ledger,
        task_context,
        observer,
    } = context;
    runtime_checkpoint(task_context)?;
    let blocking_account = memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "ShortestPathExec",
        memory.blocking_operator_bytes,
    );
    let source_account = memory_ledger.source_account_retaining(
        "ShortestPathExec source",
        memory.query_memory_bytes,
        blocking_account.clone(),
    );
    let Some(source) = find_node_by_id_property(
        catalog,
        store,
        input.source_label,
        input.source_id,
        &source_account,
        task_context,
    )?
    else {
        return Ok(empty_accounted_binding_set(memory, memory_ledger));
    };
    let (source, source_allocation) = source;
    let Some(target) = find_node_by_id_property(
        catalog,
        store,
        input.target_label,
        input.target_id,
        &source_account,
        task_context,
    )?
    else {
        return Ok(empty_accounted_binding_set(memory, memory_ledger));
    };
    let (target, target_allocation) = target;
    if input
        .source_visibility_filter
        .map(|filter| !node_matches_property_filter(&source, filter))
        .unwrap_or(false)
        || input
            .target_visibility_filter
            .map(|filter| !node_matches_property_filter(&target, filter))
            .unwrap_or(false)
    {
        return Ok(empty_accounted_binding_set(memory, memory_ledger));
    }
    let rel_type_id = if input.rel_type.is_empty() {
        None
    } else {
        let Some(rel_type_id) = catalog.rel_type_id(input.rel_type) else {
            return Ok(empty_accounted_binding_set(memory, memory_ledger));
        };
        Some(rel_type_id)
    };
    let source_id = source.id;
    let target_id = target.id;
    drop((source, source_allocation, target, target_allocation));
    let ShortestPathSearchResult {
        paths,
        tracker: mut search_tracker,
        visited_paths,
    } = search_shortest_paths(
        store,
        ShortestPathSearch {
            source: source_id,
            target: target_id,
            rel_type_id,
            direction: input.direction,
            min_hops: input.min_hops,
            max_hops: input.max_hops,
            path_node_visibility_filter: input.path_node_visibility_filter,
        },
        memory.blocking_operator_bytes,
        execution_limit.output_rows.unwrap_or(usize::MAX),
        blocking_account.clone(),
        task_context,
        observer,
    )?;
    let mut output_tracker = OperatorMemoryTracker::with_account(
        memory.query_memory_bytes,
        memory_ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "ShortestPathExec results",
            memory.query_memory_bytes,
        ),
    );
    output_tracker.try_charge(paths.len().saturating_mul(std::mem::size_of::<Binding>()))?;
    let mut output = Vec::with_capacity(paths.len());
    for path in paths {
        runtime_checkpoint(task_context)?;
        let binding = shortest_path_binding(
            store,
            &path,
            input.returns,
            &mut output_tracker,
            &source_account,
            task_context,
        )?;
        let path_bytes = path
            .capacity()
            .saturating_mul(std::mem::size_of::<NodeId>());
        drop(path);
        search_tracker.release(path_bytes);
        output.push(binding);
    }
    // The outer path vector stays allocated until its owning iterator is dropped.
    search_tracker.reset();
    observer.record_blocking_memory_report(in_memory_report(
        "ShortestPathExec",
        &search_tracker,
        blocking_account.peak_bytes(),
        visited_paths,
        memory,
    ));
    Ok(AccountedBindingSet::new(output, output_tracker))
}

fn empty_accounted_binding_set(
    memory: &ExecutionMemoryConfig,
    memory_ledger: &QueryMemoryLedger,
) -> AccountedBindingSet {
    AccountedBindingSet::new(
        Vec::new(),
        OperatorMemoryTracker::with_account(
            memory.blocking_operator_bytes,
            memory_ledger.account(
                QueryMemoryClass::BlockingState,
                "ShortestPathExec",
                memory.blocking_operator_bytes,
            ),
        ),
    )
}

fn find_node_by_id_property(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    label: &str,
    id: &Value,
    account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<
    Option<(
        NodeRecord,
        Box<dyn hawdb_storage::read_view::GraphReadAllocation>,
    )>,
> {
    let label_id = if label.is_empty() {
        None
    } else {
        let Some(label_id) = catalog.label_id(label) else {
            return Ok(None);
        };
        Some(label_id)
    };
    let mut matched = None;
    let mut admit = |bytes| crate::store::admit_graph_read(account, task_context, bytes).map(Some);
    let mut visit = |input: AdmittedNodeRecord| {
        runtime_checkpoint(task_context)?;
        let (node, allocation) = input.into_parts();
        if node.properties.get("id") == Some(id) {
            matched = Some((node, allocation));
            Ok(ScanControl::Stop)
        } else {
            Ok(ScanControl::Continue)
        }
    };
    store.visit_nodes_with_allocation(label_id, &mut admit, &mut visit)?;
    Ok(matched)
}

pub struct ShortestPathSearch<'a> {
    pub source: NodeId,
    pub target: NodeId,
    pub rel_type_id: Option<RelTypeId>,
    pub direction: RelationshipDirection,
    pub min_hops: usize,
    pub max_hops: usize,
    pub path_node_visibility_filter: Option<&'a PropertyFilter>,
}

pub fn all_shortest_paths(
    store: &dyn GraphExecutionRead,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    memory_account: QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
) -> Result<(Vec<Vec<NodeId>>, usize, usize)> {
    let result = search_shortest_paths(
        store,
        search,
        memory_budget,
        result_limit,
        memory_account,
        task_context,
        observer,
    )?;
    Ok((
        result.paths,
        result.tracker.peak_bytes,
        result.visited_paths,
    ))
}

fn shortest_path_binding(
    store: &dyn GraphExecutionRead,
    path: &[NodeId],
    returns: &[ShortestPathProjection],
    tracker: &mut OperatorMemoryTracker,
    account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<Binding> {
    let mut values = BTreeMap::new();
    for projection in returns {
        runtime_checkpoint(task_context)?;
        tracker.try_charge(
            projection
                .name
                .len()
                .saturating_add(std::mem::size_of::<usize>() * 6),
        )?;
        let value = match &projection.expression {
            ShortestPathProjectionExpression::NodePropertyList { property } => {
                tracker.try_charge(
                    std::mem::size_of::<Vec<Value>>()
                        .saturating_add(path.len().saturating_mul(std::mem::size_of::<Value>())),
                )?;
                let mut result = Vec::with_capacity(path.len());
                let mut admit =
                    |bytes| crate::store::admit_graph_read(account, task_context, bytes).map(Some);
                for node_id in path {
                    runtime_checkpoint(task_context)?;
                    match store.node_with_allocation(*node_id, None, &mut admit)? {
                        AdmittedNodeRead::Node(input) => {
                            let (node, _allocation) = input.into_parts();
                            let value = node.properties.get(property).unwrap_or(&Value::Null);
                            tracker.try_charge(
                                value_memory_bytes(value)
                                    .saturating_sub(std::mem::size_of::<Value>()),
                            )?;
                            result.push(value.clone());
                        }
                        AdmittedNodeRead::Missing => result.push(Value::Null),
                        AdmittedNodeRead::Stopped => {
                            return Err(HawDBError::Execution(
                                "ShortestPathExec property scan is incomplete".into(),
                            ))
                        }
                    }
                }
                Value::List(result)
            }
            ShortestPathProjectionExpression::Length => {
                let value = Value::Int(path.len() as i64 - 1);
                tracker.try_charge(value_payload_bytes(&value))?;
                value
            }
        };
        values.insert(projection.name.clone(), value);
    }
    Ok(Binding {
        values,
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    })
}

#[derive(Clone, Copy)]
pub struct OneHopRelationshipSpec<'a> {
    pub source: NodeId,
    pub rel_type_id: Option<RelTypeId>,
    pub target_label_ids: Option<&'a [LabelId]>,
    pub rel_properties: &'a BTreeMap<String, Value>,
    pub relationship_scan_filter: Option<&'a PropertyFilter>,
    pub direction: RelationshipDirection,
}

pub fn visit_one_hop_relationships_with_budget(
    store: &dyn GraphExecutionRead,
    spec: OneHopRelationshipSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    observer: &dyn ExecutionObserver,
    consumer: &mut dyn FnMut(RelRecord, NodeRecord) -> Result<ScanControl>,
) -> Result<ScanControl> {
    visit_one_hop_relationships_with_context(store, spec, memory, observer, None, consumer)
}

pub(crate) fn visit_one_hop_relationships_with_context(
    store: &dyn GraphExecutionRead,
    spec: OneHopRelationshipSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    observer: &dyn ExecutionObserver,
    task_context: Option<&hawdb_core::RuntimeTaskContext>,
    consumer: &mut dyn FnMut(RelRecord, NodeRecord) -> Result<ScanControl>,
) -> Result<ScanControl> {
    visit_one_hop_relationships_selected(
        store,
        spec,
        memory,
        observer,
        task_context,
        false,
        consumer,
    )
}

fn visit_one_hop_relationships_selected(
    store: &dyn GraphExecutionRead,
    spec: OneHopRelationshipSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    observer: &dyn ExecutionObserver,
    task_context: Option<&RuntimeTaskContext>,
    metadata_only: bool,
    consumer: &mut dyn FnMut(RelRecord, NodeRecord) -> Result<ScanControl>,
) -> Result<ScanControl> {
    let relationship_filter = combine_property_filters(
        property_filter_from_properties(spec.rel_properties),
        spec.relationship_scan_filter.cloned(),
    );
    let mut admit_relationship = |relationship: RelRecord| -> Result<ScanControl> {
        let Some(target_id) =
            relationship_target_for_source_direction(&relationship, spec.source, spec.direction)
        else {
            return Ok(ScanControl::Continue);
        };
        if !relationship_properties_match(&relationship, spec.rel_properties)
            || relationship_filter.as_ref().is_some_and(|filter| {
                !property_filter_matches_values(filter, relationship.id.0, &relationship.properties)
            })
        {
            return Ok(ScanControl::Continue);
        }
        if metadata_only {
            let mut allocation = None;
            let target =
                store.projected_node_owned_admitted(target_id, &BTreeSet::new(), &mut |bytes| {
                    allocation = Some(memory.admit_node(
                        bytes,
                        relationship_memory_bytes(&relationship),
                        task_context,
                    )?);
                    Ok(())
                })?;
            let Some(target) = target else {
                return Ok(ScanControl::Continue);
            };
            let target = NodeRecord {
                id: target.id,
                labels: target.labels,
                properties: target.properties,
            };
            return if node_matches_label_pattern(&target, spec.target_label_ids) {
                consumer(relationship, target)
            } else {
                Ok(ScanControl::Continue)
            };
        }
        let mut admit_node = |bytes| {
            memory
                .admit_node(
                    bytes,
                    relationship_memory_bytes(&relationship),
                    task_context,
                )
                .map(Some)
        };
        match store.node_with_allocation(target_id, spec.target_label_ids, &mut admit_node)? {
            AdmittedNodeRead::Node(target) => {
                let (target, _allocation) = target.into_parts();
                consumer(relationship, target)
            }
            AdmittedNodeRead::Missing => Ok(ScanControl::Continue),
            AdmittedNodeRead::Stopped => Ok(ScanControl::Stop),
        }
    };
    let mut visit_direction = |adjacency_direction: AdjacencyDirection,
                               skip_undirected_self_loops: bool|
     -> Result<ScanControl> {
        let mut visit = |input: hawdb_storage::read_view::AdmittedRelationshipRecord| {
            let (relationship, _allocation) = input.into_parts();
            crate::pipeline::runtime_checkpoint(task_context)?;
            if skip_undirected_self_loops
                && relationship.source == spec.source
                && relationship.target == spec.source
            {
                return Ok(ScanControl::Continue);
            }
            admit_relationship(relationship)
        };
        if let Some(filter) = relationship_filter.as_ref() {
            let (control, report) = store.visit_filtered_ordered_relationships_with_allocation(
                (spec.source, spec.rel_type_id, adjacency_direction),
                filter,
                memory,
                &mut |bytes| memory.admit_node(bytes, 0, task_context).map(Some),
                &mut visit,
            )?;
            if let Some(report) = report {
                observer.record_scan_pruning_report(report);
            }
            Ok(control)
        } else {
            store.visit_ordered_adjacent_relationships_with_allocation(
                spec.source,
                spec.rel_type_id,
                adjacency_direction,
                memory,
                &mut |bytes| memory.admit_node(bytes, 0, task_context).map(Some),
                &mut visit,
            )
        }
    };
    match spec.direction {
        RelationshipDirection::Outgoing => visit_direction(AdjacencyDirection::Outgoing, false),
        RelationshipDirection::Incoming => visit_direction(AdjacencyDirection::Incoming, false),
        RelationshipDirection::Undirected => {
            if visit_direction(AdjacencyDirection::Outgoing, false)? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
            visit_direction(AdjacencyDirection::Incoming, true)
        }
    }
}

const MAX_STREAMING_EXPAND_RECURSION_DEPTH: usize = 256;

#[derive(Clone, Copy)]
pub struct BoundedExpandSpec<'a> {
    pub source: NodeId,
    pub rel_type_id: RelTypeId,
    pub target_label_ids: Option<&'a [LabelId]>,
    pub min_hops: usize,
    pub max_hops: usize,
}

pub fn visit_bounded_expand_targets(
    store: &dyn GraphExecutionRead,
    spec: BoundedExpandSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    task_context: Option<&RuntimeTaskContext>,
    consumer: &mut dyn FnMut(NodeRecord, usize) -> Result<ScanControl>,
) -> Result<ScanControl> {
    visit_bounded_expand_targets_inner(
        store,
        ResolvedBoundedExpandSpec {
            source: spec.source,
            rel_type_id: Some(spec.rel_type_id),
            target_label_ids: spec.target_label_ids,
            min_hops: spec.min_hops,
            max_hops: spec.max_hops,
        },
        memory,
        task_context,
        consumer,
    )
}

#[derive(Clone, Copy)]
struct ResolvedBoundedExpandSpec<'a> {
    source: NodeId,
    // None means this typed relationship does not exist, never a wildcard.
    rel_type_id: Option<RelTypeId>,
    target_label_ids: Option<&'a [LabelId]>,
    min_hops: usize,
    max_hops: usize,
}

pub(crate) fn visit_zero_hop_expand_target(
    store: &dyn GraphExecutionRead,
    source: NodeId,
    target_label_ids: Option<&[LabelId]>,
    max_hops: usize,
    memory: AdjacencyReadMemory<'_>,
    task_context: Option<&RuntimeTaskContext>,
    consumer: &mut dyn FnMut(NodeRecord, usize) -> Result<ScanControl>,
) -> Result<ScanControl> {
    visit_bounded_expand_targets_inner(
        store,
        ResolvedBoundedExpandSpec {
            source,
            rel_type_id: None,
            target_label_ids,
            min_hops: 0,
            max_hops,
        },
        memory,
        task_context,
        consumer,
    )
}

fn visit_bounded_expand_targets_inner(
    store: &dyn GraphExecutionRead,
    spec: ResolvedBoundedExpandSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    task_context: Option<&RuntimeTaskContext>,
    consumer: &mut dyn FnMut(NodeRecord, usize) -> Result<ScanControl>,
) -> Result<ScanControl> {
    if spec.max_hops > MAX_STREAMING_EXPAND_RECURSION_DEPTH {
        return Err(HawDBError::Execution(format!(
            "AdjacencyExpandExec max_hops {} exceeds streaming recursion limit {MAX_STREAMING_EXPAND_RECURSION_DEPTH}",
            spec.max_hops
        )));
    }
    let traversal_frame_bytes = std::mem::size_of::<(NodeId, usize)>();
    let traversal_state_bytes = spec
        .max_hops
        .saturating_add(1)
        .saturating_mul(traversal_frame_bytes);
    if traversal_state_bytes > memory.budget_bytes {
        return Err(HawDBError::Execution(format!(
            "AdjacencyExpandExec traversal frames use {traversal_state_bytes} bytes, exceeding blocking_operator_bytes {}",
            memory.budget_bytes
        )));
    }

    fn visit_depth(
        store: &dyn GraphExecutionRead,
        spec: ResolvedBoundedExpandSpec<'_>,
        current: NodeId,
        depth: usize,
        memory: AdjacencyReadMemory<'_>,
        task_context: Option<&RuntimeTaskContext>,
        consumer: &mut dyn FnMut(NodeRecord, usize) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(task_context)?;
        if depth >= spec.min_hops {
            let mut admit_node = |bytes| {
                memory
                    .admit_node(bytes, std::mem::size_of::<usize>(), task_context)
                    .map(Some)
            };
            match store.node_with_allocation(current, spec.target_label_ids, &mut admit_node)? {
                AdmittedNodeRead::Node(node) => {
                    let (node, _allocation) = node.into_parts();
                    if consumer(node, depth)? == ScanControl::Stop {
                        return Ok(ScanControl::Stop);
                    }
                }
                AdmittedNodeRead::Missing => {}
                AdmittedNodeRead::Stopped => return Ok(ScanControl::Stop),
            }
        }
        if depth == spec.max_hops {
            return Ok(ScanControl::Continue);
        }
        let Some(rel_type_id) = spec.rel_type_id else {
            return Ok(ScanControl::Continue);
        };
        let mut visit = |input: hawdb_storage::read_view::AdmittedRelationshipRecord| {
            let (relationship, _allocation) = input.into_parts();
            visit_depth(
                store,
                spec,
                relationship.target,
                depth + 1,
                memory,
                task_context,
                consumer,
            )
        };
        store.visit_ordered_adjacent_relationships_with_allocation(
            current,
            Some(rel_type_id),
            AdjacencyDirection::Outgoing,
            memory,
            &mut |bytes| memory.admit_node(bytes, 0, task_context).map(Some),
            &mut visit,
        )
    }

    visit_depth(store, spec, spec.source, 0, memory, task_context, consumer)
}

fn relationship_target_for_source_direction(
    relationship: &RelRecord,
    source: NodeId,
    direction: RelationshipDirection,
) -> Option<NodeId> {
    match direction {
        RelationshipDirection::Outgoing => {
            (relationship.source == source).then_some(relationship.target)
        }
        RelationshipDirection::Incoming => {
            (relationship.target == source).then_some(relationship.source)
        }
        RelationshipDirection::Undirected => {
            if relationship.source == source {
                Some(relationship.target)
            } else if relationship.target == source {
                Some(relationship.source)
            } else {
                None
            }
        }
    }
}

pub fn relationship_count_sum_leg(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    source: NodeId,
    leg: &RelationshipCountLeg,
    memory: AdjacencyReadMemory<'_>,
    observer: &dyn ExecutionObserver,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<usize> {
    let rel_type_id = if leg.rel_type.is_empty() {
        None
    } else {
        catalog.rel_type_id(&leg.rel_type)
    };
    if !leg.rel_type.is_empty() && rel_type_id.is_none() {
        return Ok(0);
    }
    count_one_hop_relationships(
        store,
        OneHopRelationshipSpec {
            source,
            rel_type_id,
            target_label_ids: None,
            rel_properties: &BTreeMap::new(),
            relationship_scan_filter: None,
            direction: leg.direction,
        },
        memory,
        observer,
        leg.filter.as_ref(),
        task_context,
    )
}

fn count_one_hop_relationships(
    store: &dyn GraphExecutionRead,
    spec: OneHopRelationshipSpec<'_>,
    memory: AdjacencyReadMemory<'_>,
    observer: &dyn ExecutionObserver,
    count_filter: Option<&RelationshipCountFilter>,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<usize> {
    runtime_checkpoint(task_context)?;
    let mut count = 0usize;
    visit_one_hop_relationships_selected(
        store,
        spec,
        memory,
        observer,
        task_context,
        true,
        &mut |relationship, _| {
            runtime_checkpoint(task_context)?;
            if relationship_count_filter_matches(&relationship, count_filter) {
                count = count.saturating_add(1);
            }
            Ok(ScanControl::Continue)
        },
    )?;
    Ok(count)
}

#[derive(Debug)]
struct ThreadRepairThread {
    node_id: NodeId,
    id: Value,
    identity_key: Value,
    thread_id: Value,
    space_id: Value,
    message_count: Value,
}

impl ThreadRepairThread {
    fn from_node(node: NodeRecord, thread_id_property: &str) -> Self {
        let space_id = match node.properties.get("space_id") {
            Some(Value::String(value)) if !value.is_empty() => Value::String(value.clone()),
            _ => Value::String("default".to_string()),
        };
        let message_count = match node.properties.get("message_count") {
            Some(Value::Null) | None => Value::Int(0),
            Some(value) => value.clone(),
        };
        Self {
            node_id: node.id,
            id: node.properties.get("id").cloned().unwrap_or(Value::Null),
            identity_key: node
                .properties
                .get(thread_id_property)
                .cloned()
                .unwrap_or(Value::Null),
            thread_id: node
                .properties
                .get("thread_id")
                .cloned()
                .unwrap_or(Value::Null),
            space_id,
            message_count,
        }
    }

    fn borrowed_memory_bytes(node: &NodeRecord, thread_id_property: &str) -> usize {
        let null = Value::Null;
        let bytes = |key: &str| value_memory_bytes(node.properties.get(key).unwrap_or(&null));
        let space_bytes = match node.properties.get("space_id") {
            Some(Value::String(value)) if !value.is_empty() => {
                value_memory_bytes(&Value::Null).saturating_add(value.len())
            }
            _ => std::mem::size_of::<Value>().saturating_add("default".len()),
        };
        std::mem::size_of::<Self>()
            .saturating_add(bytes("id"))
            .saturating_add(bytes(thread_id_property))
            .saturating_add(bytes("thread_id"))
            .saturating_add(space_bytes)
            .saturating_add(bytes("message_count"))
    }

    fn memory_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(value_memory_bytes(&self.id))
            .saturating_add(value_memory_bytes(&self.identity_key))
            .saturating_add(value_memory_bytes(&self.thread_id))
            .saturating_add(value_memory_bytes(&self.space_id))
            .saturating_add(value_memory_bytes(&self.message_count))
    }
}

fn thread_repair_identity_entry_bytes(identity_ref: &Value) -> usize {
    std::mem::size_of::<(Value, usize)>()
        .saturating_mul(3)
        .saturating_add(value_memory_bytes(identity_ref))
}

fn relationship_count_filter_matches(
    relationship: &RelRecord,
    filter: Option<&RelationshipCountFilter>,
) -> bool {
    match filter {
        None => true,
        Some(RelationshipCountFilter::PropertyNotEqOrEmpty { property, value }) => {
            match relationship.properties.get(property) {
                None | Some(Value::Null) => true,
                Some(Value::String(text)) if text.is_empty() => true,
                Some(current) => current != value,
            }
        }
    }
}

fn thread_repair_properties(
    names: &[&str],
    account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<(
    BTreeSet<String>,
    Box<dyn hawdb_storage::read_view::GraphReadAllocation>,
)> {
    // Admit field-selector scratch separately from retained summary state;
    // both classes still share the same query root before any key is cloned.
    let bytes = 392usize
        .saturating_add(
            names
                .len()
                .saturating_mul(2)
                .max(4)
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(
            names
                .iter()
                .fold(0usize, |bytes, name| bytes.saturating_add(name.len())),
        );
    let allocation = crate::store::admit_graph_read(account, task_context, bytes)?;
    Ok((
        names.iter().map(|name| (*name).to_string()).collect(),
        allocation,
    ))
}

fn visit_thread_repair_nodes(
    store: &dyn GraphExecutionRead,
    label_ids: Option<&[LabelId]>,
    properties: &BTreeSet<String>,
    account: &QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
    consumer: &mut dyn FnMut(NodeRecord) -> Result<ScanControl>,
) -> Result<()> {
    let mut scan = |label_id| {
        let mut admit =
            |bytes| crate::store::admit_graph_read(account, task_context, bytes).map(Some);
        store.visit_projected_nodes_with_allocation(
            label_id,
            properties,
            &mut admit,
            &mut |input| {
                let (node, _allocation) = input.into_parts();
                runtime_checkpoint(task_context)?;
                // Multi-label patterns count a record once, without allocating a
                // second retained set of node ids.
                if let Some(label_ids) = label_ids {
                    let first = label_ids.iter().find(|id| node.labels.contains(id));
                    if first.copied() != label_id {
                        return Ok(ScanControl::Continue);
                    }
                }
                consumer(NodeRecord {
                    id: node.id,
                    labels: node.labels,
                    properties: node.properties,
                })
            },
        )?;
        Ok::<_, HawDBError>(())
    };
    match label_ids {
        Some(ids) => {
            for (index, id) in ids.iter().enumerate() {
                if ids[..index].contains(id) {
                    continue;
                }
                scan(Some(*id))?;
            }
        }
        None => scan(None)?,
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn thread_repair_stats_rows(
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    label: &str,
    identity_label: &str,
    identity_ref_property: &str,
    thread_id_property: &str,
    message_rel_type: &str,
    message_label: &str,
    memory_rel_type: &str,
    memory_label: &str,
    memory_budget: NonZeroUsize,
    memory_ledger: &QueryMemoryLedger,
    observer: &dyn ExecutionObserver,
    task_context: Option<&RuntimeTaskContext>,
) -> Result<AccountedBindingSet> {
    runtime_checkpoint(task_context)?;
    let thread_label_ids = label_ids_for_pattern(catalog, label);
    let identity_label_ids = label_ids_for_pattern(catalog, identity_label);
    let message_label_ids = label_ids_for_pattern(catalog, message_label);
    let memory_label_ids = label_ids_for_pattern(catalog, memory_label);
    let message_rel_type_id = catalog.rel_type_id(message_rel_type);
    let memory_rel_type_id = catalog.rel_type_id(memory_rel_type);
    let mut identity_counts = BTreeMap::<Value, usize>::new();
    let mut threads = Vec::new();
    let blocking_account = memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "ThreadRepairStatsExec",
        memory_budget,
    );
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, blocking_account.clone());
    let property_account = memory_ledger.source_account_retaining(
        "ThreadRepairStatsExec properties",
        memory_ledger.budget_bytes(),
        blocking_account.clone(),
    );
    let mut identity_bytes = 0usize;
    // Identity counts and thread state have different property requirements.
    // Select labels before ownership, then project only each phase's fields.
    let (identity_properties, identity_property_allocation) =
        thread_repair_properties(&[identity_ref_property], &property_account, task_context)?;
    visit_thread_repair_nodes(
        store,
        identity_label_ids.as_deref(),
        &identity_properties,
        &property_account,
        task_context,
        &mut |node| {
            if let Some(identity_ref) = node.properties.get(identity_ref_property) {
                if let Some(count) = identity_counts.get_mut(identity_ref) {
                    *count = count.saturating_add(1);
                } else {
                    let bytes = thread_repair_identity_entry_bytes(identity_ref);
                    if tracker.would_exceed(bytes) {
                        return Err(HawDBError::Execution(format!(
                            "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                            tracker.budget_bytes
                        )));
                    }
                    tracker.try_charge(bytes)?;
                    identity_bytes = identity_bytes.saturating_add(bytes);
                    identity_counts.insert(identity_ref.clone(), 1);
                }
            }
            Ok(ScanControl::Continue)
        },
    )?;
    drop(identity_properties);
    drop(identity_property_allocation);
    let (thread_properties, _thread_property_allocation) = thread_repair_properties(
        &[
            "id",
            thread_id_property,
            "thread_id",
            "space_id",
            "message_count",
        ],
        &property_account,
        task_context,
    )?;
    visit_thread_repair_nodes(
        store,
        thread_label_ids.as_deref(),
        &thread_properties,
        &property_account,
        task_context,
        &mut |node| {
            let bytes = ThreadRepairThread::borrowed_memory_bytes(&node, thread_id_property);
            if tracker.would_exceed(bytes) {
                return Err(HawDBError::Execution(format!(
                    "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.try_charge(bytes)?;
            threads.try_reserve_exact(1).map_err(|_| {
                HawDBError::Execution(
                    "ThreadRepairStatsExec cannot reserve thread state".to_string(),
                )
            })?;
            threads.push(ThreadRepairThread::from_node(node, thread_id_property));
            Ok(ScanControl::Continue)
        },
    )?;
    threads.sort_by(|left, right| left.id.cmp(&right.id));
    let mut rows = Vec::new();
    for thread in threads {
        runtime_checkpoint(task_context)?;
        let thread_bytes = thread.memory_bytes();
        let identity_refs = identity_counts
            .get(&thread.identity_key)
            .copied()
            .unwrap_or_default();
        let legacy_messages = match message_rel_type_id {
            Some(rel_type_id) => count_one_hop_relationships(
                store,
                OneHopRelationshipSpec {
                    source: thread.node_id,
                    rel_type_id: Some(rel_type_id),
                    target_label_ids: message_label_ids.as_deref(),
                    rel_properties: &BTreeMap::new(),
                    relationship_scan_filter: None,
                    direction: RelationshipDirection::Outgoing,
                },
                AdjacencyReadMemory {
                    budget_bytes: property_account.budget_bytes().get(),
                    account: Some(&property_account),
                },
                observer,
                None,
                task_context,
            )?,
            None => 0,
        };
        let compacted_memories = match memory_rel_type_id {
            Some(rel_type_id) => count_one_hop_relationships(
                store,
                OneHopRelationshipSpec {
                    source: thread.node_id,
                    rel_type_id: Some(rel_type_id),
                    target_label_ids: memory_label_ids.as_deref(),
                    rel_properties: &BTreeMap::new(),
                    relationship_scan_filter: None,
                    direction: RelationshipDirection::Outgoing,
                },
                AdjacencyReadMemory {
                    budget_bytes: property_account.budget_bytes().get(),
                    account: Some(&property_account),
                },
                observer,
                None,
                task_context,
            )?,
            None => 0,
        };
        let binding = Binding {
            values: BTreeMap::from([
                    ("t.id".to_string(), thread.id),
                    ("t.thread_id".to_string(), thread.thread_id),
                    (
                        "CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END"
                            .to_string(),
                        thread.space_id,
                    ),
                    ("COALESCE(t.message_count, 0)".to_string(), thread.message_count),
                    ("identity_refs".to_string(), Value::Int(identity_refs as i64)),
                    (
                        "legacy_messages".to_string(),
                        Value::Int(legacy_messages as i64),
                    ),
                    ("COUNT(m)".to_string(), Value::Int(compacted_memories as i64)),
                ]),
            nodes: BTreeMap::new(),
            relationships: BTreeMap::new(),
        };
        push_bounded_operator_binding("ThreadRepairStatsExec", &mut rows, binding, &mut tracker)?;
        tracker.release(thread_bytes);
    }
    tracker.release(identity_bytes);
    debug_assert_eq!(
        tracker.used_bytes,
        rows.iter().fold(0usize, |bytes, binding| {
            bytes.saturating_add(binding_memory_bytes(binding))
        })
    );
    Ok(AccountedBindingSet::new(rows, tracker))
}
