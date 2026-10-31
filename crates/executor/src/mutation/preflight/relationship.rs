// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_plan_cypher::BatchRelationshipMerge;
use hawdb_storage::mutation::{MatchedRelationshipMerge, PropertyFilter};

fn matched_ids(
    label: &str,
    filter: Option<&PropertyFilter>,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    task: Option<&RuntimeTaskContext>,
) -> Result<Vec<NodeId>> {
    let labels = label_ids_for_pattern(catalog, label);
    if labels.as_ref().is_some_and(Vec::is_empty)
        || matches!(filter, Some(PropertyFilter::Or(filters)) if filters.is_empty())
    {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    let mut visited = 0usize;
    let mut visit = |node: hawdb_storage::NodeRecord| {
        visited += 1;
        if visited.is_multiple_of(DEFAULT_EXECUTION_BATCH_ROWS) {
            runtime_checkpoint(task)?;
        }
        if node_matches_label_pattern(&node, labels.as_deref())
            && filter
                .is_none_or(|filter| crate::predicate::node_matches_property_filter(&node, filter))
        {
            if ids.len() == limits.max_affected_rows.get() {
                return Err(HawDBError::Execution(
                    "relationship MERGE exceeds max_mutation_affected_rows".into(),
                ));
            }
            ids.push(node.id);
        }
        Ok(ScanControl::Continue)
    };
    fn equality(filter: &PropertyFilter) -> Option<(&str, &Value)> {
        match filter {
            PropertyFilter::Eq { property, value } => Some((property, value)),
            PropertyFilter::And(filters) => filters.iter().find_map(equality),
            _ => None,
        }
    }
    if let Some([label]) = labels.as_deref() {
        if let Some((property, value)) = filter.and_then(equality) {
            store.visit_nodes_by_property_owned(
                *label,
                property,
                std::slice::from_ref(value),
                &mut visit,
            )?;
        } else {
            store.visit_nodes_owned(Some(*label), &mut visit)?;
        }
    } else {
        store.visit_nodes_owned(None, &mut visit)?;
    }
    Ok(ids)
}

fn project_pair(
    merge: &BatchRelationshipMerge,
    source: NodeId,
    target: NodeId,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
) -> Result<Row> {
    let mut nodes = BTreeMap::new();
    for (variable, id) in [
        (&merge.source_variable, source),
        (&merge.target_variable, target),
    ] {
        let node = store.node_owned(id)?.ok_or_else(|| {
            HawDBError::Execution("matched relationship RETURN lost its endpoint".into())
        })?;
        nodes.insert(variable.clone(), node);
    }
    let binding = Binding {
        values: BTreeMap::new(),
        nodes: nodes.into(),
        relationships: BTreeMap::new().into(),
    };
    merge
        .returns
        .as_ref()
        .expect("explicit RETURN")
        .iter()
        .map(|item| project_value(item, catalog, &binding).map(|value| (item.name.clone(), value)))
        .collect()
}

fn push_row(
    rows: &mut Vec<Row>,
    bytes: &mut usize,
    row: Row,
    limits: MutationLimits,
    output: OutputLimits,
) -> Result<()> {
    let next = bytes.saturating_add(map_payload_bytes(&row));
    if rows.len() >= limits.max_result_rows.get() || next > limits.max_result_payload_bytes.get() {
        return Err(HawDBError::Execution(
            "relationship MERGE RETURN exceeds mutation result limits".into(),
        ));
    }
    output.validate(rows.len() + 1, next)?;
    *bytes = next;
    rows.push(row);
    Ok(())
}

/// Node-only projections do not depend on the new relationship. Validate them
/// against the same immutable endpoint set before the atomic storage batch.
pub(super) fn project_before_commit(
    merge: &BatchRelationshipMerge,
    mutations: &[GraphMutation],
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    limits: MutationLimits,
    output: OutputLimits,
    task: Option<&RuntimeTaskContext>,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut bytes = 0;
    for mutation in mutations {
        runtime_checkpoint(task)?;
        let GraphMutation::MergeRelationshipsBetweenMatches(MatchedRelationshipMerge {
            source_label,
            source_filter,
            target_label,
            target_filter,
            ..
        }) = mutation
        else {
            unreachable!("bound relationship batch")
        };
        let sources = matched_ids(
            source_label,
            source_filter.as_ref(),
            catalog,
            store,
            limits,
            task,
        )?;
        if sources.is_empty() {
            continue;
        }
        let targets = matched_ids(
            target_label,
            target_filter.as_ref(),
            catalog,
            store,
            limits,
            task,
        )?;
        for source in sources {
            for target in &targets {
                runtime_checkpoint(task)?;
                let row = project_pair(merge, source, *target, catalog, store)?;
                push_row(&mut rows, &mut bytes, row, limits, output)?;
            }
        }
    }
    Ok(rows)
}

pub(super) fn project_staged(
    merge: &BatchRelationshipMerge,
    catalog: &Catalog,
    store: &dyn GraphExecutionRead,
    mutation_rows: &[Row],
    limits: MutationLimits,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut bytes = 0;
    for row in mutation_rows {
        let id = |name| match row.get(name) {
            Some(Value::Int(id)) => u64::try_from(*id)
                .map(NodeId)
                .map_err(|_| HawDBError::Execution("negative relationship endpoint".into())),
            _ => Err(HawDBError::Execution(
                "relationship MERGE RETURN requires endpoint ids".into(),
            )),
        };
        let projected = project_pair(
            merge,
            id("source_node_id")?,
            id("target_node_id")?,
            catalog,
            store,
        )?;
        push_row(
            &mut rows,
            &mut bytes,
            projected,
            limits,
            OutputLimits::default(),
        )?;
    }
    Ok(rows)
}
