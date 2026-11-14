// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::binding::value_memory_bytes;
use crate::store::admit_graph_read;
use crate::{ExecutionMemoryConfig, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use hawdb_storage::mutation::{MatchedRelationshipMerge, PropertyFilter};
use hawdb_storage::read_view::GraphReadAllocator;
use std::collections::BTreeSet;
use std::mem::size_of;

type PatternKey = (String, Vec<String>);
type Matches = BTreeMap<Vec<Value>, Vec<NodeId>>;

fn key(label: &str, filter: &Option<PropertyFilter>) -> Option<(PatternKey, Vec<Value>)> {
    let filters = match filter {
        None => &[][..],
        Some(PropertyFilter::Eq { .. }) => std::slice::from_ref(filter.as_ref()?),
        Some(PropertyFilter::And(filters)) => filters,
        Some(PropertyFilter::Or(filters)) if filters.is_empty() => return None,
        _ => unreachable!("materialized relationship endpoint equalities"),
    };
    let mut names = Vec::with_capacity(filters.len());
    let mut values = Vec::with_capacity(filters.len());
    for filter in filters {
        let PropertyFilter::Eq { property, value } = filter else {
            unreachable!("materialized endpoint equality")
        };
        names.push(property.clone());
        values.push(value.clone());
    }
    Some(((label.to_owned(), names), values))
}

fn request(mutation: &GraphMutation) -> &MatchedRelationshipMerge {
    let GraphMutation::MergeRelationshipsBetweenMatches(request) = mutation else {
        unreachable!("uniform relationship MERGE batch")
    };
    request
}

/// Resolve against the same statement snapshot that staging/commit will use.
/// Keep the returned lease alive until the rewritten batch is consumed.
pub fn resolve_relationship_merge_endpoints(
    mutations: &mut [GraphMutation],
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    memory: &ExecutionMemoryConfig,
    task: Option<&RuntimeTaskContext>,
) -> Result<Option<QueryMemoryLease>> {
    if !matches!(
        mutations.first(),
        Some(GraphMutation::MergeRelationshipsBetweenMatches(_))
    ) {
        return Ok(None);
    }
    let ledger = QueryMemoryLedger::for_query(memory, task)?;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "relationship endpoint keys",
        memory.blocking_operator_bytes,
    );
    let mut charge = account.reserve(0)?;
    let mut patterns: BTreeMap<PatternKey, Matches> = BTreeMap::new();
    for mutation in mutations.iter() {
        runtime_checkpoint(task)?;
        let request = request(mutation);
        for (label, filter) in [
            (&request.source_label, &request.source_filter),
            (&request.target_label, &request.target_filter),
        ] {
            if let Some((pattern, values)) = key(label, filter) {
                if !patterns.contains_key(&pattern) {
                    charge.grow(
                        size_of::<(PatternKey, Matches)>() * 3
                            + pattern.0.len()
                            + pattern
                                .1
                                .iter()
                                .map(|name| size_of::<String>() + name.len())
                                .sum::<usize>(),
                    )?;
                }
                let matches = patterns.entry(pattern).or_default();
                if let std::collections::btree_map::Entry::Vacant(entry) = matches.entry(values) {
                    charge.grow(
                        size_of::<(Vec<Value>, Vec<NodeId>)>() * 3
                            + entry.key().iter().map(value_memory_bytes).sum::<usize>(),
                    )?;
                    entry.insert(Vec::new());
                }
            }
        }
    }
    for ((label, names), matches) in &mut patterns {
        runtime_checkpoint(task)?;
        let label_id = if label.is_empty() {
            None
        } else {
            catalog.label_id(label)
        };
        if !label.is_empty() && label_id.is_none() {
            continue;
        }
        let required = names.iter().cloned().collect::<BTreeSet<_>>();
        let read_account = ledger.account(
            QueryMemoryClass::ExternalRead,
            "relationship endpoint scan",
            memory.query_memory_bytes,
        );
        let indexed = label_id.is_some_and(|label| {
            !names.is_empty()
                && !store.is_out_of_core()
                && catalog.has_scalar_property_index(label, &names[0])
        });
        let mut leading = Vec::new();
        let mut leading_charge = read_account.reserve(0)?;
        if indexed {
            for values in matches.keys() {
                if leading.last() != Some(&values[0]) {
                    leading_charge.grow(value_memory_bytes(&values[0]))?;
                    leading.push(values[0].clone());
                }
            }
        }
        let mut admit = |bytes| admit_graph_read(&read_account, task, bytes);
        let mut allocator = GraphReadAllocator::new(&mut admit, task);
        let mut visited = 0usize;
        let mut consumer = |mut node: hawdb_storage::ProjectedNodeRecord| {
            visited += 1;
            if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
                runtime_checkpoint(task)?;
            }
            let Some(values) = names
                .iter()
                .map(|name| node.properties.remove(name))
                .collect::<Option<Vec<_>>>()
            else {
                return Ok(ScanControl::Continue);
            };
            if let Some(ids) = matches.get_mut(&values) {
                if ids.len() == limits.max_affected_rows.get() {
                    return Err(HawDBError::Execution(
                        "relationship MERGE exceeds max_mutation_affected_rows".into(),
                    ));
                }
                // Account for Vec's next capacity before retaining another id.
                if ids.len() == ids.capacity() {
                    charge.grow(ids.capacity().max(4) * size_of::<NodeId>())?;
                }
                ids.push(node.id);
            }
            Ok(ScanControl::Continue)
        };
        let used_index = indexed
            && store
                .visit_indexed_projected_nodes_by_property_admitted(
                    label_id.expect("indexed label"),
                    &names[0],
                    &leading,
                    &required,
                    &mut allocator,
                    &mut consumer,
                )?
                .is_some();
        if !used_index {
            store.visit_projected_nodes_admitted(
                label_id,
                &required,
                &mut allocator,
                &mut consumer,
            )?;
        }
        for ids in matches.values_mut() {
            ids.sort_unstable();
            ids.dedup();
        }
    }
    let ids = |label: &str, filter: &Option<PropertyFilter>| -> &[NodeId] {
        key(label, filter)
            .and_then(|(pattern, values)| patterns.get(&pattern)?.get(&values))
            .map_or(&[], Vec::as_slice)
    };
    // Check the complete Cartesian expansion before making any per-row copies.
    let mut pairs = 0usize;
    for mutation in mutations.iter() {
        runtime_checkpoint(task)?;
        let request = request(mutation);
        let count = ids(&request.source_label, &request.source_filter)
            .len()
            .saturating_mul(ids(&request.target_label, &request.target_filter).len());
        pairs = pairs.saturating_add(count);
        if pairs > limits.max_affected_rows.get() {
            return Err(HawDBError::Execution(
                "relationship MERGE exceeds max_mutation_affected_rows".into(),
            ));
        }
    }
    for mutation in mutations {
        runtime_checkpoint(task)?;
        let GraphMutation::MergeRelationshipsBetweenMatches(request) = mutation else {
            unreachable!()
        };
        let sources = ids(&request.source_label, &request.source_filter);
        let targets = ids(&request.target_label, &request.target_filter);
        let empty = sources.is_empty() || targets.is_empty();
        for (matched, filter) in [
            (sources, &mut request.source_filter),
            (targets, &mut request.target_filter),
        ] {
            let matched = if empty { &[][..] } else { matched };
            charge.grow(matched.len() * size_of::<Value>() + size_of::<PropertyFilter>() * 3)?;
            let internal = match matched {
                [id] => PropertyFilter::IdEq {
                    value: Value::Int(id.0 as i64),
                },
                ids => PropertyFilter::IdIn {
                    values: ids.iter().map(|id| Value::Int(id.0 as i64)).collect(),
                },
            };
            *filter = Some(match filter.take() {
                Some(original) => PropertyFilter::And(vec![internal, original]),
                None => internal,
            });
        }
    }
    Ok(Some(charge))
}

pub(super) fn resolved_ids(filter: &Option<PropertyFilter>) -> &[Value] {
    let filter = match filter.as_ref().expect("resolved endpoint") {
        PropertyFilter::And(filters) => &filters[0],
        filter => filter,
    };
    match filter {
        PropertyFilter::IdEq { value } => std::slice::from_ref(value),
        PropertyFilter::IdIn { values } => values,
        _ => unreachable!("resolved endpoint ids"),
    }
}
