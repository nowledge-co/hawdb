use super::{PhysicalPlan, PlanChildren};
use crate::{SetAssignment, SetNodePropertiesReturnMode, SetValue};
use skein_cypher::RelationshipDirection;

#[cfg(test)]
mod allocation_tests;

#[cfg(test)]
mod tests;

impl PhysicalPlan {
    pub fn explain(&self, indent: usize) -> String {
        let mut output = String::new();
        let mut pending = vec![(self, indent)];
        // Render each header once: copying complete child strings at every
        // ancestor multiplies the already depth-sensitive indentation cost.
        while let Some((plan, indent)) = pending.pop() {
            if !output.is_empty() {
                output.push('\n');
            }
            output.push_str(&plan.explain_operator(indent));
            match plan.children() {
                PlanChildren::None => {}
                PlanChildren::Unary(input) => pending.push((input, indent + 2)),
                PlanChildren::Binary(left, right) => {
                    pending.push((right, indent + 2));
                    pending.push((left, indent + 2));
                }
            }
        }
        output
    }

    fn explain_operator(&self, indent: usize) -> String {
        let pad = " ".repeat(indent);
        match self {
            PhysicalPlan::CreateNodeLabel { label } => {
                format!("{pad}CreateNodeLabel label={label}")
            }
            PhysicalPlan::CreateRelationshipType { rel_type } => {
                format!("{pad}CreateRelationshipType rel_type={rel_type}")
            }
            PhysicalPlan::CreateNodeTable { name } => {
                format!("{pad}CreateNodeTable name={name}")
            }
            PhysicalPlan::CreateRelationshipTable { name } => {
                format!("{pad}CreateRelationshipTable name={name}")
            }
            PhysicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                format!(
                    "{pad}CreateProperty table_kind={table_kind:?} table={table} property={property} value_type={value_type:?} nullable={nullable}"
                )
            }
            PhysicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                format!(
                    "{pad}AlterTableState table_kind={table_kind:?} table={table} state={state:?}"
                )
            }
            PhysicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                format!(
                    "{pad}AlterPropertyState table_kind={table_kind:?} table={table} property={property} state={state:?}"
                )
            }
            PhysicalPlan::CreateIndex { label, property } => {
                format!("{pad}CreateIndex label={label} property={property}")
            }
            PhysicalPlan::CreateCompositeIndex { label, properties } => {
                format!("{pad}CreateCompositeIndex label={label} properties={properties:?}")
            }
            PhysicalPlan::CreateRangeIndex { label, property } => {
                format!("{pad}CreateRangeIndex label={label} property={property}")
            }
            PhysicalPlan::CreateFullTextIndex { label, property } => {
                format!("{pad}CreateFullTextIndex label={label} property={property}")
            }
            PhysicalPlan::CreateUniqueConstraint { label, property } => {
                format!("{pad}CreateUniqueConstraint label={label} property={property}")
            }
            PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                format!("{pad}CreateNodePropertyExistsConstraint label={label} property={property}")
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                format!(
                    "{pad}CreateRelationshipUniqueConstraint rel_type={rel_type} property={property}"
                )
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                format!(
                    "{pad}CreateRelationshipPropertyExistsConstraint rel_type={rel_type} property={property}"
                )
            }
            PhysicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                format!(
                    "{pad}ProjectGraph name={name} node_labels={node_labels:?} rel_types={rel_types:?}"
                )
            }
            PhysicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
                ..
            } => {
                format!(
                    "{pad}GraphAlgorithm algorithm={algorithm:?} graph={graph_name} options={options:?} score_column={score_column}"
                )
            }
            PhysicalPlan::VectorSeedScan {
                embedding_parameter,
                output_external_id,
                metadata_filters,
                vector_plan,
                resource_profile,
            } => format!(
                "{pad}VectorSeedScan embedding=${embedding_parameter} output_external_id={output_external_id} metadata_filter_fields={:?} priority={} max_parallelism={} max_working_memory_bytes={:?} {}",
                metadata_filters.keys().collect::<Vec<_>>(),
                resource_profile.priority,
                resource_profile.max_parallelism,
                resource_profile.max_working_memory_bytes,
                vector_plan.explain_summary()
            ),
            PhysicalPlan::CreateNode { label, .. } => {
                format!("{pad}CreateNode label={label}")
            }
            PhysicalPlan::MergeNode { label, .. } => {
                format!("{pad}MergeNode label={label}")
            }
            PhysicalPlan::MergeRelationship {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}MergeRelationship source_label={source_label} rel_type={rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::MergeMatchedRelationship {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}MergeMatchedRelationship source_label={source_label} rel_type={rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label,
                old_rel_type,
                new_rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}MergeRelationshipFromMatchedRelationship source_label={source_label} old_rel_type={old_rel_type} new_rel_type={new_rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::MergeRelationshipToMatchedTarget {
                source_label,
                old_rel_type,
                old_target_label,
                new_rel_type,
                new_target_label,
                ..
            } => {
                format!(
                    "{pad}MergeRelationshipToMatchedTarget source_label={source_label} old_rel_type={old_rel_type} old_target={old_target_label} new_rel_type={new_rel_type} new_target={new_target_label}"
                )
            }
            PhysicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label,
                old_rel_type,
                old_target_label,
                new_source_label,
                new_rel_type,
                ..
            } => {
                format!(
                    "{pad}MergeRelationshipFromMatchedTarget old_source={old_source_label} old_rel_type={old_rel_type} old_target={old_target_label} new_rel_type={new_rel_type} new_source={new_source_label}"
                )
            }
            PhysicalPlan::CreateMatchedRelationship {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}CreateMatchedRelationship source_label={source_label} rel_type={rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::SetNodeProperty {
                variable,
                label,
                property,
                value,
                ..
            } => {
                format!(
                    "{pad}SetNodeProperty variable={variable} label={label} property={property} value={}",
                    set_value_summary(value)
                )
            }
            PhysicalPlan::SetNodeProperties {
                variable,
                label,
                assignments,
                ..
            } => {
                format!(
                    "{pad}SetNodeProperties variable={variable} label={label} assignments={}",
                    set_assignments_summary(assignments)
                )
            }
            PhysicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                assignments,
                returns,
                ..
            } => {
                format!(
                    "{pad}SetNodePropertiesReturn variable={variable} label={label} assignments={} returns={}",
                    set_assignments_summary(assignments),
                    set_return_mode_summary(returns)
                )
            }
            PhysicalPlan::SetRelationshipProperty {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                property,
                value,
                ..
            } => {
                format!(
                    "{pad}SetRelationshipProperty source={source_variable}:{source_label} rel={rel_variable}:{rel_type} properties={rel_properties:?} rel_predicate={rel_predicate:?} target={target_variable}:{target_label} property={property} value={value:?}"
                )
            }
            PhysicalPlan::SetRelationshipProperties {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                assignments,
                ..
            } => {
                format!(
                    "{pad}SetRelationshipProperties source={source_variable}:{source_label} rel={rel_variable}:{rel_type} properties={rel_properties:?} rel_predicate={rel_predicate:?} target={target_variable}:{target_label} assignments={}",
                    assignments.len()
                )
            }
            PhysicalPlan::DeleteNode {
                variable,
                label,
                detach,
                ..
            } => {
                format!("{pad}DeleteNode variable={variable} label={label} detach={detach}")
            }
            PhysicalPlan::DeleteRelationship {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                ..
            } => {
                format!(
                    "{pad}DeleteRelationship source={source_variable}:{source_label} rel={rel_variable}:{rel_type} properties={rel_properties:?} rel_predicate={rel_predicate:?} target={target_variable}:{target_label} target_properties={target_properties:?}"
                )
            }
            PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => {
                format!(
                    "{pad}DeleteRelationshipTargetNodes source={source_variable}:{source_label} source_predicate={source_predicate:?} rel_type={rel_type} properties={rel_properties:?} target={target_variable}:{target_label} target_properties={target_properties:?} detach={detach}"
                )
            }
            PhysicalPlan::CreateRelationship {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                format!(
                    "{pad}CreateRelationship source_label={source_label} rel_type={rel_type} target_label={target_label}"
                )
            }
            PhysicalPlan::EmptyExec => format!("{pad}EmptyExec"),
            PhysicalPlan::SeqNodeScan { variable, label } => {
                format!("{pad}SeqNodeScan variable={variable} label={label}")
            }
            PhysicalPlan::NodeProjectionScanExec {
                variable,
                label,
                access,
                required_properties,
                predicate,
                items,
            } => {
                let columns = items
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let output = if items.is_empty() {
                    "node_binding"
                } else {
                    "projected_values"
                };
                format!(
                    "{pad}NodeProjectionScanExec variable={variable} label={label} access={} access_detail={access:?} properties={required_properties:?} predicate={predicate:?} output={output} columns=[{columns}]",
                    access.physical_operator_name()
                )
            }
            PhysicalPlan::SourceSegmentScan { variable, predicate } => {
                format!("{pad}SourceSegmentScan variable={variable} predicate={predicate:?}")
            }
            PhysicalPlan::HashJoinExec { left_key, right_key, .. } => {
                format!("{pad}HashJoinExec left={}.{} right={}.{}", left_key.variable,
                    left_key.property, right_key.variable, right_key.property)
            }
            PhysicalPlan::NodeCartesianProductExec { .. } => {
                format!("{pad}NodeCartesianProductExec")
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                ..
            } => {
                format!(
                    "{pad}NodeColumnLookupExec variable={variable} label={label} property={property} column={column} optional={optional}"
                )
            }
            PhysicalPlan::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => {
                format!(
                    "{pad}IndexNodeSeek variable={variable} label={label} property={property} value={value:?}"
                )
            }
            PhysicalPlan::IndexNodeMultiSeek {
                variable,
                label,
                property,
                values,
            } => {
                format!(
                    "{pad}IndexNodeMultiSeek variable={variable} label={label} property={property} values={values:?}"
                )
            }
            PhysicalPlan::IndexNodeUnionSeek {
                variable,
                label,
                branches,
            } => {
                format!(
                    "{pad}IndexNodeUnionSeek variable={variable} label={label} branches={branches:?}"
                )
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => {
                format!(
                    "{pad}IndexNodeCompositeSeek variable={variable} label={label} predicates={predicates:?}"
                )
            }
            PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable,
                label,
                seek,
            } => {
                format!(
                    "{pad}IndexNodeCompositeRangeSeek variable={variable} label={label} seek={seek:?}"
                )
            }
            PhysicalPlan::IndexNodeRangeSeek {
                variable,
                label,
                property,
                lower,
                upper,
            } => {
                format!(
                    "{pad}IndexNodeRangeSeek variable={variable} label={label} property={property} lower={lower:?} upper={upper:?}"
                )
            }
            PhysicalPlan::IndexNodeTextSeek {
                variable,
                label,
                property,
                query,
            } => {
                format!(
                    "{pad}IndexNodeTextSeek variable={variable} label={label} property={property} query={query:?}"
                )
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                rel_properties,
                optional,
                graph_budget,
                ..
            } => {
                let arrow = match direction {
                    RelationshipDirection::Outgoing => "->",
                    RelationshipDirection::Incoming => "<-",
                    RelationshipDirection::Undirected => "-",
                };
                let rel = rel_variable
                    .as_ref()
                    .map(|variable| format!(" rel={variable}:{rel_type}"))
                    .unwrap_or_else(|| format!(" rel_type={rel_type}"));
                let budget = graph_budget
                    .map(|budget| {
                        format!(
                            " graph_candidate_limit={} graph_payload_byte_limit={}",
                            budget.candidate_limit, budget.payload_byte_limit
                        )
                    })
                    .unwrap_or_default();
                format!(
                    "{pad}AdjacencyExpandExec source={source_variable}:{source_label}{rel} direction={arrow} properties={rel_properties:?} hops={min_hops}..{max_hops} optional={optional}{budget} target={target_variable}:{target_label}"
                )
            }
            PhysicalPlan::AdjacencyExistsExec {
                source_variable,
                rel_type,
                direction,
                target_variable,
                ..
            } => {
                let arrow = match direction {
                    RelationshipDirection::Outgoing => "->",
                    RelationshipDirection::Incoming => "<-",
                    RelationshipDirection::Undirected => "-",
                };
                format!(
                    "{pad}AdjacencyExistsExec source={source_variable} direction={arrow} rel_type={rel_type} target={target_variable}"
                )
            }
            PhysicalPlan::OptionalDegreeExec {
                source_variable,
                rel_type,
                direction,
                target_label,
                alias,
                ..
            } => {
                let arrow = match direction {
                    RelationshipDirection::Outgoing => "->",
                    RelationshipDirection::Incoming => "<-",
                    RelationshipDirection::Undirected => "-",
                };
                format!(
                    "{pad}OptionalDegreeExec source={source_variable} rel_type={rel_type} direction={arrow} target={target_label} alias={alias}"
                )
            }
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable,
                label,
                legs,
                output,
                ..
            } => {
                format!(
                    "{pad}OptionalRelationshipCountSumExec source={variable}:{label} legs={} output={output}",
                    legs.len()
                )
            }
            PhysicalPlan::NodeCountExec { label, output } => {
                format!("{pad}NodeCountExec label={label} output={output}")
            }
            PhysicalPlan::RelationshipCountExec { rel_type, output } => {
                format!("{pad}RelationshipCountExec rel_type={rel_type} output={output}")
            }
            PhysicalPlan::ThreadRepairStatsExec { label, .. } => {
                format!("{pad}ThreadRepairStatsExec label={label}")
            }
            PhysicalPlan::ShortestPathExec {
                source_variable,
                source_label,
                rel_type,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                returns,
                ..
            } => {
                let arrow = match direction {
                    RelationshipDirection::Outgoing => "->",
                    RelationshipDirection::Incoming => "<-",
                    RelationshipDirection::Undirected => "-",
                };
                let columns = returns
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{pad}ShortestPathExec source={source_variable}:{source_label} rel_type={rel_type} direction={arrow} hops={min_hops}..{max_hops} target={target_variable}:{target_label} columns=[{columns}]"
                )
            }
            PhysicalPlan::FilterExec { predicate, .. } => {
                format!("{pad}FilterExec predicate={predicate:?}")
            }
            PhysicalPlan::ProjectExec { items, .. } => {
                let columns = items
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{pad}ProjectExec columns=[{columns}]")
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                ..
            } => {
                let mut columns = group_keys
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>();
                columns.extend(items.iter().map(|item| item.name.as_str()));
                let columns = columns.join(", ");
                format!("{pad}AggregateExec columns=[{columns}]")
            }
            PhysicalPlan::DistinctExec { .. } => {
                format!("{pad}DistinctExec")
            }
            PhysicalPlan::SortExec { items, .. } => {
                format!("{pad}SortExec keys={items:?}")
            }
            PhysicalPlan::TopNExec {
                items,
                offset,
                limit,
                ..
            } => {
                format!("{pad}TopNExec keys={items:?} offset={offset} limit={limit}")
            }
            PhysicalPlan::LimitExec {
                offset,
                limit,
                ..
            } => {
                format!("{pad}LimitExec offset={offset} limit={limit:?}")
            }
        }
    }
}

fn set_value_summary(value: &SetValue) -> String {
    match value {
        SetValue::Value(value) => format!("{value:?}"),
        SetValue::Coalesce { property, default } => format!("coalesce({property},{default:?})"),
        SetValue::AddInt { property, amount } => format!("{property}+{amount}"),
        SetValue::DecrementFloorZero { property } => format!("max({property}-1,0)"),
        SetValue::PreserveNewerExisting {
            property,
            incoming,
            preserve,
        } => format!("preserve_newer({property},{incoming:?},{preserve})"),
    }
}

fn set_assignments_summary(assignments: &[SetAssignment]) -> String {
    assignments
        .iter()
        .map(|assignment| {
            format!(
                "{}={}",
                assignment.property,
                set_value_summary(&assignment.value)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn set_return_mode_summary(returns: &SetNodePropertiesReturnMode) -> String {
    match returns {
        SetNodePropertiesReturnMode::Project(items) => items.len().to_string(),
        SetNodePropertiesReturnMode::Count { name } => format!("count:{name}"),
    }
}
