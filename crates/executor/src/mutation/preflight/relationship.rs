// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::endpoints::resolved_ids;
use super::*;
use hawdb_plan_cypher::BatchRelationshipMerge;
use hawdb_storage::mutation::MatchedRelationshipMerge;

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
            source_filter,
            target_filter,
            ..
        }) = mutation
        else {
            unreachable!("bound relationship batch")
        };
        let sources = resolved_ids(source_filter);
        let targets = resolved_ids(target_filter);
        for source in sources {
            for target in targets {
                runtime_checkpoint(task)?;
                let node_id = |value: &Value| match value {
                    Value::Int(id) => NodeId(*id as u64),
                    _ => unreachable!("resolved endpoint id"),
                };
                let row = project_pair(merge, node_id(source), node_id(target), catalog, store)?;
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
