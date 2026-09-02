//! Shortest-path and relationship traversal operators.

use crate::binding::{
    binding_memory_bytes, node_memory_bytes, relationship_memory_bytes, value_memory_bytes, Binding,
};
use crate::blocking::in_memory_report;
use crate::kernel::{
    ensure_operator_item_fits, push_bounded_operator_binding, OperatorMemoryTracker,
};
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
use skein_core::{
    Catalog, LabelId, RelTypeId, RelationshipDirection, Result, RuntimeTaskContext, SkeinError,
    Value,
};
use skein_plan::{
    RelationshipCountFilter, RelationshipCountLeg, ShortestPathProjection,
    ShortestPathProjectionExpression,
};
use skein_storage::{AdjacencyDirection, NodeId, NodeRecord, PropertyFilter, RelRecord};
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;

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
    let Some(source) =
        find_node_by_id_property(catalog, store, input.source_label, input.source_id)?
    else {
        return Ok(empty_accounted_binding_set(memory, memory_ledger));
    };
    let Some(target) =
        find_node_by_id_property(catalog, store, input.target_label, input.target_id)?
    else {
        return Ok(empty_accounted_binding_set(memory, memory_ledger));
    };
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
    let blocking_account = memory_ledger.account(
        QueryMemoryClass::BlockingState,
        "ShortestPathExec",
        memory.blocking_operator_bytes,
    );
    let ShortestPathSearchResult {
        paths,
        tracker: mut search_tracker,
        visited_paths,
    } = search_shortest_paths(
        store,
        ShortestPathSearch {
            source: source.id,
            target: target.id,
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
    let mut output = Vec::with_capacity(paths.len());
    let mut output_tracker =
        OperatorMemoryTracker::with_account(memory.blocking_operator_bytes, blocking_account);
    for path in paths {
        runtime_checkpoint(task_context)?;
        let binding = shortest_path_binding(store, &path, input.returns)?;
        let bytes = binding_memory_bytes(&binding);
        ensure_operator_item_fits("ShortestPathExec result", bytes, &output_tracker)?;
        if output_tracker.would_exceed(bytes) {
            return Err(SkeinError::Execution(format!(
                "ShortestPathExec result state exceeds blocking_operator_bytes {}",
                output_tracker.budget_bytes
            )));
        }
        output_tracker.try_charge(bytes)?;
        search_tracker.release(path_memory_bytes(&path));
        output.push(binding);
    }
    observer.record_blocking_memory_report(in_memory_report(
        "ShortestPathExec",
        &output_tracker,
        search_tracker.peak_bytes.max(output_tracker.peak_bytes),
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
) -> Result<Option<NodeRecord>> {
    let label_id = if label.is_empty() {
        None
    } else {
        let Some(label_id) = catalog.label_id(label) else {
            return Ok(None);
        };
        Some(label_id)
    };
    let mut matched = None;
    let mut visit = |node: NodeRecord| {
        if node.properties.get("id") == Some(id) {
            matched = Some(node);
            Ok(ScanControl::Stop)
        } else {
            Ok(ScanControl::Continue)
        }
    };
    store.visit_nodes_owned(label_id, &mut visit)?;
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

struct ShortestPathSearchResult {
    paths: Vec<Vec<NodeId>>,
    tracker: OperatorMemoryTracker,
    visited_paths: usize,
}

#[allow(clippy::too_many_arguments)]
fn search_shortest_paths(
    store: &dyn GraphExecutionRead,
    search: ShortestPathSearch<'_>,
    memory_budget: NonZeroUsize,
    result_limit: usize,
    memory_account: QueryMemoryAccount,
    task_context: Option<&RuntimeTaskContext>,
    observer: &dyn ExecutionObserver,
) -> Result<ShortestPathSearchResult> {
    let initial_path = vec![search.source];
    let mut tracker = OperatorMemoryTracker::with_account(memory_budget, memory_account.clone());
    tracker.try_charge(path_memory_bytes(&initial_path))?;
    let mut queue = VecDeque::from([initial_path]);
    let mut results = Vec::new();
    let mut found_depth = None;
    let mut visited_paths = 0usize;
    while let Some(path) = queue.pop_front() {
        runtime_checkpoint(task_context)?;
        visited_paths = visited_paths.saturating_add(1);
        let path_bytes = path_memory_bytes(&path);
        let depth = path.len() - 1;
        if found_depth.is_some_and(|found| depth >= found) || depth == search.max_hops {
            tracker.release(path_bytes);
            continue;
        }
        let current = *path.last().expect("path is never empty");
        let adjacency_memory = AdjacencyReadMemory {
            budget_bytes: memory_budget.get(),
            account: Some(&memory_account),
        };
        visit_one_hop_relationships_with_budget(
            store,
            OneHopRelationshipSpec {
                source: current,
                rel_type_id: search.rel_type_id,
                target_label_ids: None,
                rel_properties: &BTreeMap::new(),
                relationship_scan_filter: None,
                direction: search.direction,
            },
            adjacency_memory,
            observer,
            &mut |_, next| {
                runtime_checkpoint(task_context)?;
                if search
                    .path_node_visibility_filter
                    .map(|filter| !node_matches_property_filter(&next, filter))
                    .unwrap_or(false)
                {
                    return Ok(ScanControl::Continue);
                }
                if path.contains(&next.id) {
                    return Ok(ScanControl::Continue);
                }
                let next_depth = depth + 1;
                let mut next_path = path.clone();
                next_path.push(next.id);
                let next_path_bytes = path_memory_bytes(&next_path);
                ensure_operator_item_fits("ShortestPathExec", next_path_bytes, &tracker)?;
                if tracker.would_exceed(next_path_bytes) {
                    return Err(SkeinError::Execution(format!(
                        "ShortestPathExec frontier exceeds blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                }
                tracker.try_charge(next_path_bytes)?;
                if next.id == search.target && next_depth >= search.min_hops {
                    found_depth = Some(next_depth);
                    results.push(next_path);
                    if results.len() >= result_limit {
                        return Ok(ScanControl::Stop);
                    }
                } else if found_depth.is_none() && next_depth < search.max_hops {
                    queue.push_back(next_path);
                } else {
                    tracker.release(next_path_bytes);
                }
                Ok(ScanControl::Continue)
            },
        )?;
        tracker.release(path_bytes);
        if results.len() >= result_limit {
            break;
        }
    }
    for path in queue {
        tracker.release(path_memory_bytes(&path));
    }
    debug_assert_eq!(
        tracker.used_bytes,
        results.iter().fold(0usize, |bytes, path| {
            bytes.saturating_add(path_memory_bytes(path))
        })
    );
    Ok(ShortestPathSearchResult {
        paths: results,
        tracker,
        visited_paths,
    })
}

fn path_memory_bytes(path: &[NodeId]) -> usize {
    std::mem::size_of::<Vec<NodeId>>()
        .saturating_add(path.len().saturating_mul(std::mem::size_of::<NodeId>()))
}

fn shortest_path_binding(
    store: &dyn GraphExecutionRead,
    path: &[NodeId],
    returns: &[ShortestPathProjection],
) -> Result<Binding> {
    let mut values = BTreeMap::new();
    for projection in returns {
        let value = match &projection.expression {
            ShortestPathProjectionExpression::NodePropertyList { property } => Value::List(
                path.iter()
                    .map(|node_id| {
                        Ok(store
                            .node_owned(*node_id)?
                            .and_then(|node| node.properties.get(property).cloned())
                            .unwrap_or(Value::Null))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            ShortestPathProjectionExpression::Length => Value::Int(path.len() as i64 - 1),
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
        if let Some(target) = store.node_owned(target_id)?
            && node_matches_label_pattern(&target, spec.target_label_ids)
        {
            let match_bytes =
                relationship_memory_bytes(&relationship).saturating_add(node_memory_bytes(&target));
            if match_bytes > memory.budget_bytes {
                return Err(SkeinError::Execution(format!(
                    "adjacency result uses {match_bytes} bytes, exceeding blocking_operator_bytes {}",
                    memory.budget_bytes
                )));
            }
            return consumer(relationship, target);
        }
        Ok(ScanControl::Continue)
    };
    let mut visit_direction = |adjacency_direction: AdjacencyDirection,
                               skip_undirected_self_loops: bool|
     -> Result<ScanControl> {
        let mut visit = |relationship: RelRecord| {
            if skip_undirected_self_loops
                && relationship.source == spec.source
                && relationship.target == spec.source
            {
                return Ok(ScanControl::Continue);
            }
            admit_relationship(relationship)
        };
        if let Some(filter) = relationship_filter.as_ref() {
            let (control, report) = store.visit_ordered_adjacent_relationships_with_filter_owned(
                spec.source,
                spec.rel_type_id,
                adjacency_direction,
                filter,
                memory,
                &mut visit,
            )?;
            if let Some(report) = report {
                observer.record_scan_pruning_report(report);
            }
            Ok(control)
        } else {
            store.visit_ordered_adjacent_relationships_owned(
                spec.source,
                spec.rel_type_id,
                adjacency_direction,
                memory,
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
    if spec.max_hops > MAX_STREAMING_EXPAND_RECURSION_DEPTH {
        return Err(SkeinError::Execution(format!(
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
        return Err(SkeinError::Execution(format!(
            "AdjacencyExpandExec traversal frames use {traversal_state_bytes} bytes, exceeding blocking_operator_bytes {}",
            memory.budget_bytes
        )));
    }

    fn visit_depth(
        store: &dyn GraphExecutionRead,
        spec: BoundedExpandSpec<'_>,
        current: NodeId,
        depth: usize,
        memory: AdjacencyReadMemory<'_>,
        task_context: Option<&RuntimeTaskContext>,
        consumer: &mut dyn FnMut(NodeRecord, usize) -> Result<ScanControl>,
    ) -> Result<ScanControl> {
        runtime_checkpoint(task_context)?;
        if depth >= spec.min_hops
            && let Some(node) = store.node_owned(current)?
            && node_matches_label_pattern(&node, spec.target_label_ids)
        {
            let item_bytes = node_memory_bytes(&node).saturating_add(std::mem::size_of::<usize>());
            if item_bytes > memory.budget_bytes {
                return Err(SkeinError::Execution(format!(
                    "AdjacencyExpandExec result uses {item_bytes} bytes, exceeding blocking_operator_bytes {}",
                    memory.budget_bytes
                )));
            }
            if consumer(node, depth)? == ScanControl::Stop {
                return Ok(ScanControl::Stop);
            }
        }
        if depth == spec.max_hops {
            return Ok(ScanControl::Continue);
        }
        let mut visit = |relationship: RelRecord| {
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
        store.visit_ordered_adjacent_relationships_owned(
            current,
            Some(spec.rel_type_id),
            AdjacencyDirection::Outgoing,
            memory,
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
    visit_one_hop_relationships_with_budget(
        store,
        spec,
        memory,
        observer,
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
    let mut identity_bytes = 0usize;
    let mut visit = |node: NodeRecord| {
        runtime_checkpoint(task_context)?;
        if node_matches_label_pattern(&node, identity_label_ids.as_deref())
            && let Some(identity_ref) = node.properties.get(identity_ref_property).cloned()
        {
            if let Some(count) = identity_counts.get_mut(&identity_ref) {
                *count = count.saturating_add(1);
            } else {
                let bytes = thread_repair_identity_entry_bytes(&identity_ref);
                if tracker.would_exceed(bytes) {
                    return Err(SkeinError::Execution(format!(
                        "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                        tracker.budget_bytes
                    )));
                }
                tracker.try_charge(bytes)?;
                identity_bytes = identity_bytes.saturating_add(bytes);
                identity_counts.insert(identity_ref, 1);
            }
        }
        if node_matches_label_pattern(&node, thread_label_ids.as_deref()) {
            let thread = ThreadRepairThread::from_node(node, thread_id_property);
            let bytes = thread.memory_bytes();
            if tracker.would_exceed(bytes) {
                return Err(SkeinError::Execution(format!(
                    "ThreadRepairStatsExec state exceeds blocking_operator_bytes {}",
                    tracker.budget_bytes
                )));
            }
            tracker.try_charge(bytes)?;
            threads.try_reserve(1).map_err(|_| {
                SkeinError::Execution(
                    "ThreadRepairStatsExec cannot reserve thread state".to_string(),
                )
            })?;
            threads.push(thread);
        }
        Ok(ScanControl::Continue)
    };
    store.visit_nodes_owned(None, &mut visit)?;
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
                    budget_bytes: memory_budget.get(),
                    account: Some(&blocking_account),
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
                    budget_bytes: memory_budget.get(),
                    account: Some(&blocking_account),
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
