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

//! Private batch scan and count handlers.

use super::scan::stream_node_access_batches;
use super::*;
use hawdb_plan_cypher::NodeProjectionAccess;

fn admit_access_copy<'a>(
    context: BatchReadContext<'_>,
    properties: impl Iterator<Item = &'a str>,
    values: impl Iterator<Item = &'a Value>,
) -> Result<Box<dyn hawdb_storage::read_view::GraphReadAllocation>> {
    let account = context
        .kernel_context()
        .source_account("IndexNodeAccess keys");
    crate::scan::owned::admit_access_copy(&account, context.task_context, properties, values)
}

pub(super) fn stream_composite_node_seek_batches(
    variable: &str,
    label: &str,
    predicates: &[(String, Value)],
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let _access_copy = admit_access_copy(
        context,
        predicates.iter().map(|(key, _)| key.as_str()),
        predicates.iter().map(|(_, value)| value),
    )?;
    stream_node_access_batches(
        variable,
        label,
        &NodeProjectionAccess::CompositeEquality {
            predicates: predicates.to_vec(),
        },
        context,
        execution_limit,
        emit,
    )
}

pub(super) fn stream_composite_node_range_seek_batches(
    variable: &str,
    label: &str,
    seek: &hawdb_plan_cypher::CompositeRangeSeek,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let _access_copy = admit_access_copy(
        context,
        seek.index_properties
            .iter()
            .map(String::as_str)
            .chain(seek.equality_prefix.iter().map(|(key, _)| key.as_str()))
            .chain(std::iter::once(seek.range_property.as_str())),
        seek.equality_prefix
            .iter()
            .map(|(_, value)| value)
            .chain(seek.lower.iter().map(|(value, _)| value))
            .chain(seek.upper.iter().map(|(value, _)| value)),
    )?;
    stream_node_access_batches(
        variable,
        label,
        &NodeProjectionAccess::CompositeRange { seek: seek.clone() },
        context,
        execution_limit,
        emit,
    )
}

pub(super) struct NodeRangeSeekSpec<'a> {
    pub(super) variable: &'a str,
    pub(super) label: &'a str,
    pub(super) property: &'a str,
    pub(super) lower: &'a Option<(Value, bool)>,
    pub(super) upper: &'a Option<(Value, bool)>,
}

impl NodeRangeSeekSpec<'_> {
    pub(super) fn stream(
        self,
        context: BatchReadContext<'_>,
        execution_limit: ExecutionLimit,
        emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
    ) -> Result<BatchControl> {
        let Self {
            variable,
            label,
            property,
            lower,
            upper,
        } = self;
        let _access_copy = admit_access_copy(
            context,
            std::iter::once(property),
            lower
                .iter()
                .map(|(value, _)| value)
                .chain(upper.iter().map(|(value, _)| value)),
        )?;
        stream_node_access_batches(
            variable,
            label,
            &NodeProjectionAccess::PropertyRange {
                property: property.to_owned(),
                lower: lower.clone(),
                upper: upper.clone(),
            },
            context,
            execution_limit,
            emit,
        )
    }
}

pub(super) fn stream_node_text_seek_batches(
    variable: &str,
    label: &str,
    property: &str,
    query: &str,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let _access_copy =
        admit_access_copy(context, [property, query].into_iter(), std::iter::empty())?;
    stream_node_access_batches(
        variable,
        label,
        &NodeProjectionAccess::FullText {
            property: property.to_owned(),
            query: query.to_owned(),
        },
        context,
        execution_limit,
        emit,
    )
}

pub(super) fn stream_optional_relationship_count_sum_batches(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let label_ids = label_ids_for_pattern(context.catalog, label);
    let count_account = context
        .kernel_context()
        .source_account("OptionalRelationshipCountSumExec");
    let mut total = 0usize;
    let mut nodes_since_checkpoint = 0usize;
    let mut admit = |bytes| {
        crate::store::admit_graph_read(&count_account, context.task_context, bytes).map(Some)
    };
    context
        .store
        .visit_nodes_with_allocation(None, &mut admit, &mut |input| {
            let (node, _allocation) = input.into_parts();
            nodes_since_checkpoint += 1;
            if nodes_since_checkpoint == context.memory.batch_rows.get() {
                nodes_since_checkpoint = 0;
                runtime_checkpoint(context.task_context)?;
            }
            if !node_matches_label_pattern(&node, label_ids.as_deref())
                || !node_properties_match(&node, properties)
            {
                return Ok(ScanControl::Continue);
            }
            for leg in legs {
                let count = relationship_count_sum_leg(
                    context.catalog,
                    context.store,
                    node.id,
                    leg,
                    crate::store::AdjacencyReadMemory {
                        budget_bytes: context.memory.query_memory_bytes.get(),
                        account: Some(&count_account),
                    },
                    context.observer,
                    context.task_context,
                )?;
                total = total.saturating_add(count);
            }
            Ok(ScanControl::Continue)
        })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(total as i64))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_node_count_batches(
    label: &str,
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let label_id = (!label.is_empty())
        .then(|| context.catalog.label_id(label))
        .flatten();
    let count = if label.is_empty() {
        context.store.node_count_for_label(None)
    } else if let Some(label_id) = label_id {
        context.store.node_count_for_label(Some(label_id))
    } else {
        0
    };
    context
        .observer
        .record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Node,
            label_id,
            rel_type_id: None,
            strategy: ScanPruningStrategy::ExactCount,
            pruned: true,
            exact_empty: count == 0,
            candidate_count_before_pruning: count,
            pruned_candidate_count: count,
            candidate_count_before_filter: 0,
            output_count: 1,
            filtered_out_count: 0,
        });
    let count = i64::try_from(count).map_err(|_| {
        HawDBError::Execution(format!(
            "node count for label '{label}' exceeds the supported i64 result range"
        ))
    })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(count))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_relationship_count_batches(
    rel_type: &str,
    output: &str,
    context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    let rel_type_id = (!rel_type.is_empty())
        .then(|| context.catalog.rel_type_id(rel_type))
        .flatten();
    let count = if rel_type.is_empty() {
        context.store.relationship_count_for_type(None)
    } else if let Some(rel_type_id) = rel_type_id {
        context.store.relationship_count_for_type(Some(rel_type_id))
    } else {
        0
    };
    context
        .observer
        .record_scan_pruning_report(ScanPruningReport {
            target_kind: ScanPruningTargetKind::Relationship,
            label_id: None,
            rel_type_id,
            strategy: ScanPruningStrategy::ExactCount,
            pruned: true,
            exact_empty: count == 0,
            candidate_count_before_pruning: count,
            pruned_candidate_count: count,
            candidate_count_before_filter: 0,
            output_count: 1,
            filtered_out_count: 0,
        });
    let count = i64::try_from(count).map_err(|_| {
        HawDBError::Execution(format!(
            "relationship count for type '{rel_type}' exceeds the supported i64 result range"
        ))
    })?;
    emit(vec![Binding {
        values: BTreeMap::from([(output.to_owned(), Value::Int(count))]),
        nodes: BTreeMap::new(),
        relationships: BTreeMap::new(),
    }])
}

pub(super) fn stream_node_projection_batches(
    spec: NodeProjectionScanSpec<'_>,
    context: BatchReadContext<'_>,
    execution_limit: ExecutionLimit,
    emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    if !spec.items.is_empty()
        && spec.access.is_label_scan()
        && let Some(result) = try_stream_columnar_node_projection_batches(
            spec.variable,
            spec.label,
            spec.predicate,
            spec.items,
            context,
            execution_limit,
            emit,
        )
    {
        return result;
    }
    stream_node_projection_scan_batches(spec, context, execution_limit, emit)
}

pub(super) fn stream_empty_batches(
    _context: BatchReadContext<'_>,
    _execution_limit: ExecutionLimit,
    _emit: &mut dyn FnMut(BindingBatch) -> Result<BatchControl>,
) -> Result<BatchControl> {
    Ok(BatchControl::Continue)
}
