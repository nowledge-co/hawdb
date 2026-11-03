// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::QueryMemoryAccount;
use hawdb_storage::read_view::{AdmittedKeySet, GraphReadAdmission, GraphReadAllocator};
use hawdb_storage::ProjectedNodeRecord;

pub(super) fn visit(
    label: &str,
    properties: &BTreeMap<String, Value>,
    context: BatchReadContext<'_>,
    account: &QueryMemoryAccount,
    consume: &mut dyn FnMut(NodeId) -> Result<()>,
) -> Result<()> {
    runtime_checkpoint(context.task_context)?;
    let labels = label_ids_for_pattern(context.catalog, label);
    if labels.as_ref().is_some_and(Vec::is_empty) {
        return Ok(());
    }
    let exact_label = (!label.is_empty() && !label.contains(':'))
        .then(|| context.catalog.label_id(label))
        .flatten();
    // String payloads and B-tree capacity have separate before-copy permits.
    let _names = crate::scan::owned::admit_access_copy(
        account,
        context.task_context,
        properties.keys().map(String::as_str),
        std::iter::empty(),
    )?;
    let mut admit = |bytes| crate::store::admit_graph_read(account, context.task_context, bytes);
    let admission = GraphReadAdmission::new(&mut admit);
    let mut required = AdmittedKeySet::new(&admission)?;
    for property in properties.keys() {
        required.try_insert(property.clone())?;
    }
    let lookup = exact_label.and_then(|label| {
        properties.iter().find_map(|(property, value)| {
            (value != &Value::Null && context.catalog.has_scalar_property_index(label, property))
                .then_some((label, property, value))
        })
    });
    let mut visited = 0usize;
    let mut matched = 0usize;
    let mut visit = |node: ProjectedNodeRecord| {
        runtime_checkpoint(context.task_context)?;
        visited = visited.saturating_add(1);
        let node = NodeRecord {
            id: node.id,
            labels: node.labels,
            properties: node.properties,
        };
        if node_matches_label_pattern(&node, labels.as_deref())
            && node_properties_match(&node, properties)
        {
            consume(node.id)?;
            matched = matched.saturating_add(1);
        }
        Ok(ScanControl::Continue)
    };
    let mut allocate = |bytes| crate::store::admit_graph_read(account, context.task_context, bytes);
    let mut allocator = GraphReadAllocator::new(&mut allocate, context.task_context);
    let control = if let Some((label, property, value)) = lookup {
        let _key = crate::scan::owned::admit_access_copy(
            account,
            context.task_context,
            std::iter::once(property.as_str()),
            std::iter::once(value),
        )?;
        let access = NodeProjectionAccess::PropertyValues {
            property: property.clone(),
            values: vec![value.clone()],
        };
        let NodeProjectionAccess::PropertyValues { values, .. } = &access else {
            unreachable!()
        };
        if !context.store.is_out_of_core()
            && let Some(control) = context
                .store
                .visit_indexed_projected_nodes_by_property_admitted(
                    label,
                    property,
                    values,
                    &required,
                    &mut allocator,
                    &mut visit,
                )?
        {
            control
        } else {
            context.store.visit_projected_nodes_by_access_admitted(
                label,
                &access,
                &required,
                &mut allocator,
                &mut visit,
            )?
        }
    } else {
        context.store.visit_projected_nodes_admitted(
            exact_label,
            &required,
            &mut allocator,
            &mut visit,
        )?
    };
    if control == ScanControl::Stop {
        return Err(HawDBError::Execution(
            "relationship count source scan stopped before completion".into(),
        ));
    }
    let before = context.store.node_count_for_label(exact_label);
    context
        .observer
        .record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Node,
            label_id: exact_label,
            rel_type_id: None,
            strategy: lookup.map_or(ScanPruningStrategy::FullLabelScan, |(_, property, _)| {
                ScanPruningStrategy::PropertyEq {
                    property: property.clone(),
                }
            }),
            pruned: lookup.is_some(),
            exact_empty: visited == 0,
            candidate_count_before_pruning: before,
            pruned_candidate_count: before.saturating_sub(visited),
            candidate_count_before_filter: visited,
            output_count: matched,
            filtered_out_count: visited.saturating_sub(matched),
        });
    Ok(())
}
