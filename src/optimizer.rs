use crate::cypher::RelationshipDirection;
use crate::planner::{
    AggregateFunction, AggregateTarget, Aggregation, ComparisonOp, GraphAlgorithmKind,
    GraphAlgorithmOptions, LogicalPlan, Predicate, Projection, ProjectionExpression,
    RelationshipCountFilter, RelationshipCountLeg, RelationshipOnCreateValue,
    RelationshipSetAssignment, SchemaObjectState, SchemaPropertyType, SchemaTableKind,
    SetAssignment, SetNodePropertiesReturnMode, SetValue, ShortestPathProjection, SortDirection,
    SortItem, SortKey,
};
use crate::value::Value;
pub use skein_optimizer::{
    apply_rule_batch, plan_class_counts, plan_operator_counts, Distribution, GroupId, Memo,
    OptimizationSearchReport, OptimizerConfig, OptimizerRule, OptimizerTrace, PhysicalPlanClass,
    PhysicalPlanKind, PhysicalProperties, PlanCost, PlanCostBreakdown, RuleApplication, RuleEvent,
    RuleId, RuleKind, RuleOutcome, RulePromise, SearchMode, SelectedPlanTrace,
};
use std::collections::{BTreeMap, BTreeSet};

type ValueRangeBound = (Value, bool);
type ValueRangeBounds = (Option<ValueRangeBound>, Option<ValueRangeBound>);

pub const AST_FAST_PATH_RULE_NAME: &str = "fast_path";

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalPlan {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: SchemaTableKind,
        table: String,
        property: String,
        value_type: SchemaPropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: SchemaTableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: SchemaTableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
    GraphAlgorithm {
        algorithm: GraphAlgorithmKind,
        graph_name: String,
        options: GraphAlgorithmOptions,
        score_column: String,
    },
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<SetAssignment>,
        post_merge_assignments: Vec<SetAssignment>,
    },
    MergeRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    MergeMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, RelationshipOnCreateValue>,
    },
    MergeRelationshipToMatchedTarget {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_target_label: String,
        new_target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedTarget {
        old_source_label: String,
        old_source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_source_label: String,
        new_source_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    CreateMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
    },
    SetNodeProperty {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        property: String,
        value: SetValue,
    },
    SetNodeProperties {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
    },
    SetNodePropertiesReturn {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
        returns: SetNodePropertiesReturnMode,
    },
    SetRelationshipProperty {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        detach: bool,
    },
    DeleteRelationship {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    DeleteRelationshipTargetNodes {
        source_variable: String,
        source_label: String,
        source_predicate: Option<Predicate>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        detach: bool,
    },
    CreateRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    SeqNodeScan {
        variable: String,
        label: String,
    },
    NodeCartesianProductExec {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
    },
    NodeColumnLookupExec {
        variable: String,
        label: String,
        property: String,
        column: String,
        optional: bool,
        input: Box<PhysicalPlan>,
    },
    IndexNodeSeek {
        variable: String,
        label: String,
        property: String,
        value: Value,
    },
    IndexNodeMultiSeek {
        variable: String,
        label: String,
        property: String,
        values: Vec<Value>,
    },
    IndexNodeCompositeSeek {
        variable: String,
        label: String,
        predicates: Vec<(String, Value)>,
    },
    IndexNodeRangeSeek {
        variable: String,
        label: String,
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    IndexNodeTextSeek {
        variable: String,
        label: String,
        property: String,
        query: String,
    },
    AdjacencyExpandExec {
        source_variable: String,
        source_label: String,
        rel_variable: Option<String>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        input: Box<PhysicalPlan>,
    },
    OptionalDegreeExec {
        source_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        alias: String,
        input: Box<PhysicalPlan>,
    },
    OptionalRelationshipCountSumExec {
        variable: String,
        label: String,
        properties: BTreeMap<String, Value>,
        legs: Vec<RelationshipCountLeg>,
        output: String,
    },
    ThreadRepairStatsExec {
        label: String,
        identity_label: String,
        identity_ref_property: String,
        thread_id_property: String,
        message_rel_type: String,
        message_label: String,
        memory_rel_type: String,
        memory_label: String,
    },
    ShortestPathExec {
        source_variable: String,
        source_label: String,
        source_id: Value,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        target_id: Value,
        min_hops: usize,
        max_hops: usize,
        returns: Vec<ShortestPathProjection>,
    },
    FilterExec {
        predicate: Predicate,
        input: Box<PhysicalPlan>,
    },
    ProjectExec {
        items: Vec<Projection>,
        input: Box<PhysicalPlan>,
    },
    AggregateExec {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
        input: Box<PhysicalPlan>,
    },
    DistinctExec {
        input: Box<PhysicalPlan>,
    },
    SortExec {
        items: Vec<SortItem>,
        input: Box<PhysicalPlan>,
    },
    LimitExec {
        offset: usize,
        limit: Option<usize>,
        input: Box<PhysicalPlan>,
    },
}

mod physical_plan;
pub use physical_plan::PhysicalPlanChildren;

impl PhysicalPlan {
    pub fn fingerprint(&self) -> String {
        let mut output = String::new();
        self.write_fingerprint(&mut output);
        output
    }

    pub fn explain(&self, indent: usize) -> String {
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
            } => {
                format!(
                    "{pad}GraphAlgorithm algorithm={algorithm:?} graph={graph_name} options={options:?} score_column={score_column}"
                )
            }
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
            PhysicalPlan::SeqNodeScan { variable, label } => {
                format!("{pad}SeqNodeScan variable={variable} label={label}")
            }
            PhysicalPlan::NodeCartesianProductExec { left, right } => {
                format!(
                    "{pad}NodeCartesianProductExec\n{}\n{}",
                    left.explain(indent + 2),
                    right.explain(indent + 2)
                )
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                input,
            } => {
                format!(
                    "{pad}NodeColumnLookupExec variable={variable} label={label} property={property} column={column} optional={optional}\n{}",
                    input.explain(indent + 2)
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
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => {
                format!(
                    "{pad}IndexNodeCompositeSeek variable={variable} label={label} predicates={predicates:?}"
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
                input,
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
                format!(
                    "{pad}AdjacencyExpandExec source={source_variable}:{source_label}{rel} direction={arrow} properties={rel_properties:?} hops={min_hops}..{max_hops} optional={optional} target={target_variable}:{target_label}\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::OptionalDegreeExec {
                source_variable,
                rel_type,
                direction,
                target_label,
                alias,
                input,
                ..
            } => {
                let arrow = match direction {
                    RelationshipDirection::Outgoing => "->",
                    RelationshipDirection::Incoming => "<-",
                    RelationshipDirection::Undirected => "-",
                };
                format!(
                    "{pad}OptionalDegreeExec source={source_variable} rel_type={rel_type} direction={arrow} target={target_label} alias={alias}\n{}",
                    input.explain(indent + 2)
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
            PhysicalPlan::FilterExec { predicate, input } => {
                format!(
                    "{pad}FilterExec predicate={predicate:?}\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::ProjectExec { items, input } => {
                let columns = items
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{pad}ProjectExec columns=[{columns}]\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                input,
            } => {
                let mut columns = group_keys
                    .iter()
                    .map(|item| item.name.as_str())
                    .collect::<Vec<_>>();
                columns.extend(items.iter().map(|item| item.name.as_str()));
                let columns = columns.join(", ");
                format!(
                    "{pad}AggregateExec columns=[{columns}]\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::DistinctExec { input } => {
                format!("{pad}DistinctExec\n{}", input.explain(indent + 2))
            }
            PhysicalPlan::SortExec { items, input } => {
                format!(
                    "{pad}SortExec keys={items:?}\n{}",
                    input.explain(indent + 2)
                )
            }
            PhysicalPlan::LimitExec {
                offset,
                limit,
                input,
            } => {
                format!(
                    "{pad}LimitExec offset={offset} limit={limit:?}\n{}",
                    input.explain(indent + 2)
                )
            }
        }
    }

    fn write_fingerprint(&self, output: &mut String) {
        match self {
            PhysicalPlan::CreateNodeLabel { label } => {
                output.push_str("CreateNodeLabel(");
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipType { rel_type } => {
                output.push_str("CreateRelationshipType(");
                write_identifier(output, rel_type);
                output.push(')');
            }
            PhysicalPlan::CreateNodeTable { name } => {
                output.push_str("CreateNodeTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipTable { name } => {
                output.push_str("CreateRelationshipTable(");
                write_identifier(output, name);
                output.push(')');
            }
            PhysicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => {
                output.push_str("CreateProperty(");
                output.push_str(match table_kind {
                    SchemaTableKind::Node => "node",
                    SchemaTableKind::Relationship => "relationship",
                });
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push_str(match value_type {
                    SchemaPropertyType::Any => ":any",
                    SchemaPropertyType::Bool => ":bool",
                    SchemaPropertyType::Int => ":int",
                    SchemaPropertyType::Float => ":float",
                    SchemaPropertyType::String => ":string",
                    SchemaPropertyType::List => ":list",
                });
                output.push_str(if *nullable { ":nullable" } else { ":not_null" });
                output.push(')');
            }
            PhysicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => {
                output.push_str("AlterTableState(");
                output.push_str(match table_kind {
                    SchemaTableKind::Node => "node",
                    SchemaTableKind::Relationship => "relationship",
                });
                output.push(':');
                write_identifier(output, table);
                output.push(':');
                output.push_str(schema_state_fingerprint(*state));
                output.push(')');
            }
            PhysicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => {
                output.push_str("AlterPropertyState(");
                output.push_str(match table_kind {
                    SchemaTableKind::Node => "node",
                    SchemaTableKind::Relationship => "relationship",
                });
                output.push(':');
                write_identifier(output, table);
                output.push('.');
                write_identifier(output, property);
                output.push(':');
                output.push_str(schema_state_fingerprint(*state));
                output.push(')');
            }
            PhysicalPlan::CreateIndex { label, property } => {
                output.push_str("CreateIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateCompositeIndex { label, properties } => {
                output.push_str("CreateCompositeIndex(");
                write_identifier(output, label);
                output.push('(');
                write_identifier_list(output, properties);
                output.push(')');
            }
            PhysicalPlan::CreateRangeIndex { label, property } => {
                output.push_str("CreateRangeIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateFullTextIndex { label, property } => {
                output.push_str("CreateFullTextIndex(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateUniqueConstraint { label, property } => {
                output.push_str("CreateUniqueConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                output.push_str("CreateNodePropertyExistsConstraint(");
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipUniqueConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                output.push_str("CreateRelationshipPropertyExistsConstraint(");
                write_identifier(output, rel_type);
                output.push('.');
                write_identifier(output, property);
                output.push(')');
            }
            PhysicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => {
                output.push_str("ProjectGraph(");
                write_identifier(output, name);
                output.push_str(":labels=");
                write_identifier_list(output, node_labels);
                output.push_str(":rels=");
                write_identifier_list(output, rel_types);
                output.push(')');
            }
            PhysicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
            } => {
                output.push_str("GraphAlgorithm(");
                output.push_str(match algorithm {
                    GraphAlgorithmKind::PageRank => "page_rank",
                    GraphAlgorithmKind::Louvain => "louvain",
                });
                output.push(':');
                write_identifier(output, graph_name);
                output.push_str(":damping=");
                if let Some(damping) = options.damping {
                    output.push_str(&damping.to_bits().to_string());
                }
                output.push_str(":iterations=");
                if let Some(iterations) = options.max_iterations {
                    output.push_str(&iterations.to_string());
                }
                output.push_str(":score=");
                write_identifier(output, score_column);
                output.push(')');
            }
            PhysicalPlan::CreateNode { label, properties } => {
                output.push_str("CreateNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push(')');
            }
            PhysicalPlan::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => {
                output.push_str("MergeNode(");
                write_identifier(output, label);
                output.push(',');
                write_properties(output, match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(',');
                write_set_assignments(output, on_match_assignments);
                output.push(',');
                write_set_assignments(output, post_merge_assignments);
                output.push(')');
            }
            PhysicalPlan::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("MergeRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::MergeMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_match_properties,
                on_create_properties,
            } => {
                output.push_str("MergeMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label,
                source_properties,
                old_rel_type,
                old_rel_properties,
                target_label,
                target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => {
                output.push_str("MergeRelationshipFromMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(")=>[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_relationship_on_create_properties(output, on_create_properties);
                output.push(']');
            }
            PhysicalPlan::MergeRelationshipToMatchedTarget {
                source_label,
                source_properties,
                old_rel_type,
                old_rel_properties,
                old_target_label,
                old_target_properties,
                new_target_label,
                new_target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => {
                output.push_str("MergeRelationshipToMatchedTarget(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_target=(");
                write_identifier(output, new_target_label);
                output.push(',');
                write_properties(output, new_target_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
            }
            PhysicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label,
                old_source_properties,
                old_rel_type,
                old_rel_properties,
                old_target_label,
                old_target_properties,
                new_source_label,
                new_source_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => {
                output.push_str("MergeRelationshipFromMatchedTarget(old_source=");
                write_identifier(output, old_source_label);
                output.push(',');
                write_properties(output, old_source_properties);
                output.push_str(")-[");
                write_identifier(output, old_rel_type);
                output.push(',');
                write_properties(output, old_rel_properties);
                output.push_str("]->(");
                write_identifier(output, old_target_label);
                output.push(',');
                write_properties(output, old_target_properties);
                output.push_str("),new_source=(");
                write_identifier(output, new_source_label);
                output.push(',');
                write_properties(output, new_source_properties);
                output.push_str("),new_rel=[");
                write_identifier(output, new_rel_type);
                output.push(',');
                write_properties(output, new_rel_match_properties);
                output.push(',');
                write_properties(output, on_create_properties);
                output.push(']');
            }
            PhysicalPlan::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => {
                output.push_str("SetNodeProperty(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_set_value(output, value);
                output.push(')');
            }
            PhysicalPlan::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => {
                output.push_str("CreateMatchedRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => {
                output.push_str("SetNodeProperties(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push(')');
            }
            PhysicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => {
                output.push_str("SetNodePropertiesReturn(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_set_assignments(output, assignments);
                output.push_str(",returns=");
                write_set_return_mode(output, returns);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperty {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                property,
                value,
            } => {
                output.push_str("SetRelationshipProperty(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(',');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::SetRelationshipProperties {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                assignments,
            } => {
                output.push_str("SetRelationshipProperties(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push_str(",assignments=");
                for assignment in assignments {
                    write_identifier(output, &assignment.property);
                    output.push('=');
                    write_value(output, &assignment.value);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => {
                output.push_str("DeleteNode(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::DeleteRelationship {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
            } => {
                output.push_str("DeleteRelationship(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str("-[");
                write_identifier(output, rel_variable);
                output.push(':');
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(',');
                write_optional_predicate(output, predicate.as_ref());
                output.push(',');
                write_optional_predicate(output, rel_predicate.as_ref());
                output.push(')');
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
                output.push_str("DeleteRelationshipTargetNodes(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push(',');
                write_optional_predicate(output, source_predicate.as_ref());
                output.push_str("-[:");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->");
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",detach=");
                output.push_str(if *detach { "true" } else { "false" });
                output.push(')');
            }
            PhysicalPlan::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => {
                output.push_str("CreateRelationship(");
                write_identifier(output, source_label);
                output.push(',');
                write_properties(output, source_properties);
                output.push_str(")-[");
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push_str("]->(");
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push(')');
            }
            PhysicalPlan::SeqNodeScan { variable, label } => {
                output.push_str("SeqNodeScan(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(')');
            }
            PhysicalPlan::NodeCartesianProductExec { left, right } => {
                output.push_str("NodeCartesianProductExec(");
                left.write_fingerprint(output);
                output.push(',');
                right.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                label,
                property,
                column,
                optional,
                input,
            } => {
                output.push_str("NodeColumnLookupExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_identifier(output, column);
                output.push(',');
                output.push_str(if *optional { "optional" } else { "required" });
                output.push(',');
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::IndexNodeSeek {
                variable,
                label,
                property,
                value,
            } => {
                output.push_str("IndexNodeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push('=');
                write_value(output, value);
                output.push(')');
            }
            PhysicalPlan::IndexNodeMultiSeek {
                variable,
                label,
                property,
                values,
            } => {
                output.push_str("IndexNodeMultiSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" IN [");
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_value(output, value);
                }
                output.push_str("])");
            }
            PhysicalPlan::IndexNodeCompositeSeek {
                variable,
                label,
                predicates,
            } => {
                output.push_str("IndexNodeCompositeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('(');
                for (index, (property, value)) in predicates.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_identifier(output, property);
                    output.push('=');
                    write_value(output, value);
                }
                output.push(')');
            }
            PhysicalPlan::IndexNodeRangeSeek {
                variable,
                label,
                property,
                lower,
                upper,
            } => {
                output.push_str("IndexNodeRangeSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(",lower=");
                write_optional_range_bound(output, lower.as_ref());
                output.push_str(",upper=");
                write_optional_range_bound(output, upper.as_ref());
                output.push(')');
            }
            PhysicalPlan::IndexNodeTextSeek {
                variable,
                label,
                property,
                query,
            } => {
                output.push_str("IndexNodeTextSeek(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push('.');
                write_identifier(output, property);
                output.push_str(" contains ");
                write_identifier(output, query);
                output.push(')');
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                optional,
                input,
            } => {
                output.push_str("AdjacencyExpandExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                if let Some(rel_variable) = rel_variable {
                    write_identifier(output, rel_variable);
                    output.push(':');
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",optional=");
                output.push_str(if *optional { "true" } else { "false" });
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::OptionalDegreeExec {
                source_variable,
                rel_type,
                rel_properties,
                direction,
                target_label,
                target_properties,
                alias,
                input,
            } => {
                output.push_str("OptionalDegreeExec(");
                write_identifier(output, source_variable);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push(',');
                write_properties(output, rel_properties);
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_label);
                output.push(',');
                write_properties(output, target_properties);
                output.push_str(",alias=");
                write_identifier(output, alias);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable,
                label,
                properties,
                legs,
                output: projection,
            } => {
                output.push_str("OptionalRelationshipCountSumExec(");
                write_identifier(output, variable);
                output.push(':');
                write_identifier(output, label);
                output.push(',');
                write_properties(output, properties);
                output.push_str(",legs=[");
                for (index, leg) in legs.iter().enumerate() {
                    if index > 0 {
                        output.push(',');
                    }
                    write_relationship_count_leg(output, leg);
                }
                output.push_str("],output=");
                write_identifier(output, projection);
                output.push(')');
            }
            PhysicalPlan::ThreadRepairStatsExec {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            } => {
                output.push_str("ThreadRepairStatsExec(");
                write_identifier(output, label);
                output.push_str(",identity=");
                write_identifier(output, identity_label);
                output.push('.');
                write_identifier(output, identity_ref_property);
                output.push('=');
                write_identifier(output, thread_id_property);
                output.push_str(",messages=");
                write_identifier(output, message_rel_type);
                output.push(':');
                write_identifier(output, message_label);
                output.push_str(",memories=");
                write_identifier(output, memory_rel_type);
                output.push(':');
                write_identifier(output, memory_label);
                output.push(')');
            }
            PhysicalPlan::ShortestPathExec {
                source_variable,
                source_label,
                source_id,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_id,
                min_hops,
                max_hops,
                returns,
            } => {
                output.push_str("ShortestPathExec(");
                write_identifier(output, source_variable);
                output.push(':');
                write_identifier(output, source_label);
                output.push_str(",source_id=");
                write_value(output, source_id);
                match direction {
                    RelationshipDirection::Incoming => output.push_str("<-[:"),
                    RelationshipDirection::Outgoing | RelationshipDirection::Undirected => {
                        output.push_str("-[:");
                    }
                }
                write_identifier(output, rel_type);
                output.push('*');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                match direction {
                    RelationshipDirection::Outgoing => output.push_str("]->"),
                    RelationshipDirection::Incoming => output.push_str("]-"),
                    RelationshipDirection::Undirected => output.push_str("]-"),
                }
                write_identifier(output, target_variable);
                output.push(':');
                write_identifier(output, target_label);
                output.push_str(",target_id=");
                write_value(output, target_id);
                output.push_str(",returns=");
                for item in returns {
                    write_identifier(output, &item.name);
                    output.push(',');
                }
                output.push(')');
            }
            PhysicalPlan::FilterExec { predicate, input } => {
                output.push_str("FilterExec(");
                write_predicate(output, predicate);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::ProjectExec { items, input } => {
                output.push_str("ProjectExec(");
                write_projection_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::AggregateExec {
                group_keys,
                items,
                input,
            } => {
                output.push_str("AggregateExec(groups=");
                write_projection_list(output, group_keys);
                output.push_str(",aggs=");
                write_aggregation_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::DistinctExec { input } => {
                output.push_str("DistinctExec(input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::SortExec { items, input } => {
                output.push_str("SortExec(");
                write_sort_list(output, items);
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
            PhysicalPlan::LimitExec {
                offset,
                limit,
                input,
            } => {
                output.push_str("LimitExec(offset=");
                output.push_str(&offset.to_string());
                output.push_str(",limit=");
                match limit {
                    Some(limit) => output.push_str(&limit.to_string()),
                    None => output.push_str("none"),
                }
                output.push_str(",input=");
                input.write_fingerprint(output);
                output.push(')');
            }
        }
    }
}

type GraphMemo = Memo<GroupExpr>;

#[derive(Debug, Clone, PartialEq)]
enum GraphRuleExpr {
    Filter {
        predicate: Box<Predicate>,
        input: Box<LogicalPlan>,
    },
    Physical(Box<PhysicalPlan>),
}

struct NodeEqualitySeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeInSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeRangeSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeTextSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeCompositeSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

struct NodeConjunctionSeekRule<'a> {
    catalog: &'a OptimizerCatalog,
}

#[derive(Debug)]
struct GroupExpr {
    logical: LogicalPlan,
    children: Vec<GroupId>,
}

#[derive(Debug, Default, Clone)]
pub struct CascadesOptimizer {
    config: OptimizerConfig,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalog {
    assume_all_indexes: bool,
    equality_property_indexes: BTreeSet<(String, String)>,
    composite_property_indexes: BTreeSet<(String, Vec<String>)>,
    range_property_indexes: BTreeSet<(String, String)>,
    full_text_property_indexes: BTreeSet<(String, String)>,
    label_counts: BTreeMap<String, u64>,
    rel_type_counts: BTreeMap<String, u64>,
    rel_type_source_counts: BTreeMap<String, u64>,
    rel_type_target_counts: BTreeMap<String, u64>,
    path_counts: BTreeMap<(String, String, String), u64>,
    path_source_distinct_counts: BTreeMap<(String, String, String), u64>,
    path_target_distinct_counts: BTreeMap<(String, String, String), u64>,
    bounded_path_counts: BTreeMap<(String, String, String, usize), u64>,
    bounded_path_source_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    bounded_path_target_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    property_distinct_counts: BTreeMap<(String, String), u64>,
    rel_property_distinct_counts: BTreeMap<(String, String), u64>,
    property_histograms: BTreeMap<(String, String), Vec<Value>>,
    rel_property_histograms: BTreeMap<(String, String), Vec<Value>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalogIndexes {
    equality_property_indexes: BTreeSet<(String, String)>,
    composite_property_indexes: BTreeSet<(String, Vec<String>)>,
    range_property_indexes: BTreeSet<(String, String)>,
    full_text_property_indexes: BTreeSet<(String, String)>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct OptimizerCatalogStatistics {
    label_counts: BTreeMap<String, u64>,
    rel_type_counts: BTreeMap<String, u64>,
    rel_type_source_counts: BTreeMap<String, u64>,
    rel_type_target_counts: BTreeMap<String, u64>,
    path_counts: BTreeMap<(String, String, String), u64>,
    path_source_distinct_counts: BTreeMap<(String, String, String), u64>,
    path_target_distinct_counts: BTreeMap<(String, String, String), u64>,
    bounded_path_counts: BTreeMap<(String, String, String, usize), u64>,
    bounded_path_source_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    bounded_path_target_distinct_counts: BTreeMap<(String, String, String, usize), u64>,
    property_distinct_counts: BTreeMap<(String, String), u64>,
    rel_property_distinct_counts: BTreeMap<(String, String), u64>,
    property_histograms: BTreeMap<(String, String), Vec<Value>>,
    rel_property_histograms: BTreeMap<(String, String), Vec<Value>>,
}

impl CascadesOptimizer {
    pub fn new(config: OptimizerConfig) -> Self {
        Self { config }
    }

    pub fn optimize(&self, logical: &LogicalPlan) -> PhysicalPlan {
        self.optimize_with_trace(logical).0
    }

    pub fn optimize_with_trace(&self, logical: &LogicalPlan) -> (PhysicalPlan, OptimizerTrace) {
        self.optimize_with_catalog(logical, &OptimizerCatalog::optimistic())
    }

    pub fn optimize_with_catalog(
        &self,
        logical: &LogicalPlan,
        catalog: &OptimizerCatalog,
    ) -> (PhysicalPlan, OptimizerTrace) {
        let required_groups = logical_group_count(logical);
        if required_groups > self.config.max_groups {
            let mut decisions = Vec::new();
            let plan = logical_to_physical_direct(logical, catalog, &mut decisions);
            let mut report =
                OptimizationSearchReport::direct_fallback(required_groups, self.config.max_groups);
            report.extend_decisions(decisions);
            let selected = selected_plan_trace(&plan, catalog);
            report.record_selected_plan_cost(selected.cost);
            return (plan, report.into_trace(selected));
        }
        let mut memo = GraphMemo::default();
        let root = insert_logical_group(&mut memo, logical);
        let mut decisions = Vec::new();
        let plan = best_physical(&memo, root, catalog, &mut decisions);
        let mut report = OptimizationSearchReport::memo(memo.group_count());
        report.extend_decisions(decisions);
        let selected = selected_plan_trace(&plan, catalog);
        report.record_selected_plan_cost(selected.cost);
        (plan, report.into_trace(selected))
    }

    pub fn optimize_fast_path_with_catalog(
        &self,
        logical: &LogicalPlan,
        catalog: &OptimizerCatalog,
        reason: &str,
    ) -> (PhysicalPlan, OptimizerTrace) {
        let mut decisions = Vec::new();
        let plan = logical_to_physical_direct(logical, catalog, &mut decisions);
        let mut report = OptimizationSearchReport::fast_path(logical_group_count(logical));
        report.push_rule_event(RuleEvent::selected(AST_FAST_PATH_RULE_NAME, reason));
        report.extend_decisions(decisions);
        let selected = selected_plan_trace(&plan, catalog);
        report.record_selected_plan_cost(selected.cost);
        (plan, report.into_trace(selected))
    }
}

impl OptimizerCatalog {
    pub fn new(indexes: OptimizerCatalogIndexes, statistics: OptimizerCatalogStatistics) -> Self {
        Self {
            assume_all_indexes: false,
            equality_property_indexes: indexes.equality_property_indexes,
            composite_property_indexes: indexes.composite_property_indexes,
            range_property_indexes: indexes.range_property_indexes,
            full_text_property_indexes: indexes.full_text_property_indexes,
            label_counts: statistics.label_counts,
            rel_type_counts: statistics.rel_type_counts,
            rel_type_source_counts: statistics.rel_type_source_counts,
            rel_type_target_counts: statistics.rel_type_target_counts,
            path_counts: statistics.path_counts,
            path_source_distinct_counts: statistics.path_source_distinct_counts,
            path_target_distinct_counts: statistics.path_target_distinct_counts,
            bounded_path_counts: statistics.bounded_path_counts,
            bounded_path_source_distinct_counts: statistics.bounded_path_source_distinct_counts,
            bounded_path_target_distinct_counts: statistics.bounded_path_target_distinct_counts,
            property_distinct_counts: statistics.property_distinct_counts,
            rel_property_distinct_counts: statistics.rel_property_distinct_counts,
            property_histograms: statistics.property_histograms,
            rel_property_histograms: statistics.rel_property_histograms,
        }
    }

    fn optimistic() -> Self {
        Self {
            assume_all_indexes: true,
            ..Self::default()
        }
    }

    fn has_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .equality_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    fn has_range_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .range_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    fn has_full_text_property_index(&self, label: &str, property: &str) -> bool {
        self.assume_all_indexes
            || self
                .full_text_property_indexes
                .contains(&(label.to_string(), property.to_string()))
    }

    fn has_composite_property_index(&self, label: &str, properties: &[String]) -> bool {
        self.assume_all_indexes
            || self
                .composite_property_indexes
                .contains(&(label.to_string(), properties.to_vec()))
    }

    fn composite_property_indexes_for_label(&self, label: &str) -> Vec<Vec<String>> {
        if self.assume_all_indexes {
            return Vec::new();
        }
        self.composite_property_indexes
            .iter()
            .filter_map(|(candidate_label, properties)| {
                if candidate_label == label {
                    Some(properties.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn label_count(&self, label: &str) -> u64 {
        self.label_counts.get(label).copied().unwrap_or(1)
    }

    fn relationship_count(&self, rel_type: &str) -> u64 {
        self.rel_type_counts.get(rel_type).copied().unwrap_or(1)
    }

    fn path_source_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
    ) -> Option<u64> {
        self.path_source_distinct_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied()
    }

    fn path_target_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
    ) -> Option<u64> {
        self.path_target_distinct_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied()
    }

    fn bounded_path_source_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        self.bounded_path_distinct_count(
            &self.bounded_path_source_distinct_counts,
            source_label,
            rel_type,
            target_label,
            min_hops,
            max_hops,
        )
    }

    fn bounded_path_target_distinct_count(
        &self,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        self.bounded_path_distinct_count(
            &self.bounded_path_target_distinct_counts,
            source_label,
            rel_type,
            target_label,
            min_hops,
            max_hops,
        )
    }

    fn bounded_path_distinct_count(
        &self,
        counts: &BTreeMap<(String, String, String, usize), u64>,
        source_label: &str,
        rel_type: &str,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> Option<u64> {
        let mut total = 0_u64;
        let mut found = false;
        for hop in min_hops..=max_hops.max(min_hops) {
            if let Some(count) = counts.get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
                hop,
            )) {
                found = true;
                total = total.saturating_add(*count);
            }
        }
        found.then_some(total.max(1))
    }

    fn distinct_count(&self, label: &str, property: &str) -> u64 {
        self.property_distinct_counts
            .get(&(label.to_string(), property.to_string()))
            .copied()
            .unwrap_or_else(|| self.label_count(label).max(1))
    }

    fn known_distinct_count(&self, label: &str, property: &str) -> Option<u64> {
        self.property_distinct_counts
            .get(&(label.to_string(), property.to_string()))
            .copied()
    }

    fn known_rel_property_distinct_count(&self, rel_type: &str, property: &str) -> Option<u64> {
        self.rel_property_distinct_counts
            .get(&(rel_type.to_string(), property.to_string()))
            .copied()
    }

    fn rel_property_distinct_count(&self, rel_type: &str, property: &str) -> u64 {
        self.rel_property_distinct_counts
            .get(&(rel_type.to_string(), property.to_string()))
            .copied()
            .unwrap_or_else(|| {
                self.rel_type_counts
                    .get(rel_type)
                    .copied()
                    .unwrap_or(1)
                    .max(1)
            })
    }

    fn estimate_rel_property_eq_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .div_ceil(self.rel_property_distinct_count(rel_type, property).max(1))
            .max(1)
    }

    fn estimate_rel_property_not_eq_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .saturating_sub(self.estimate_rel_property_eq_rows(rel_type, property, input_rows))
    }

    fn estimate_rel_property_in_rows(
        &self,
        rel_type: &str,
        property: &str,
        values: &[Value],
        input_rows: u64,
    ) -> u64 {
        if values.is_empty() {
            return 0;
        }
        let distinct_count = self.rel_property_distinct_count(rel_type, property).max(1);
        let value_count = values
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            .min(distinct_count as usize) as u64;
        input_rows
            .saturating_mul(value_count)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_rel_property_string_match_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
        max_divisor: u64,
    ) -> u64 {
        let divisor = self
            .rel_property_distinct_count(rel_type, property)
            .max(1)
            .min(max_divisor)
            .max(1);
        input_rows.div_ceil(divisor).max(1)
    }

    fn estimate_rel_property_null_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        let distinct_count = self.rel_property_distinct_count(rel_type, property).max(1);
        input_rows.div_ceil(distinct_count.min(10)).max(1)
    }

    fn estimate_rel_property_not_null_rows(
        &self,
        rel_type: &str,
        property: &str,
        input_rows: u64,
    ) -> u64 {
        input_rows
            .saturating_sub(self.estimate_rel_property_null_rows(rel_type, property, input_rows))
            .max(1)
    }

    fn estimate_rel_property_range_rows(
        &self,
        rel_type: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        let Some(histogram) = self
            .rel_property_histograms
            .get(&(rel_type.to_string(), property.to_string()))
            .filter(|values| !values.is_empty())
        else {
            return input_rows.div_ceil(2).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        let distinct_count = histogram.len() as u64;
        input_rows
            .saturating_mul(matching_values)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_range_rows(
        &self,
        label: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
    ) -> u64 {
        let label_count = self.label_count(label);
        let Some(histogram) = self
            .property_histograms
            .get(&(label.to_string(), property.to_string()))
            .filter(|values| !values.is_empty())
        else {
            return label_count.div_ceil(2).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        let distinct_count = histogram.len() as u64;
        label_count
            .saturating_mul(matching_values)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_property_eq_rows(&self, label: &str, property: &str, input_rows: u64) -> u64 {
        input_rows
            .div_ceil(self.distinct_count(label, property).max(1))
            .max(1)
    }

    fn estimate_property_not_eq_rows(&self, label: &str, property: &str, input_rows: u64) -> u64 {
        input_rows.saturating_sub(self.estimate_property_eq_rows(label, property, input_rows))
    }

    fn estimate_property_in_rows(
        &self,
        label: &str,
        property: &str,
        values: &[Value],
        input_rows: u64,
    ) -> u64 {
        if values.is_empty() {
            return 0;
        }
        let distinct_count = self.distinct_count(label, property).max(1);
        let value_count = values
            .iter()
            .collect::<BTreeSet<_>>()
            .len()
            .min(distinct_count as usize) as u64;
        input_rows
            .saturating_mul(value_count)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_property_string_match_rows(
        &self,
        label: &str,
        property: &str,
        input_rows: u64,
        max_divisor: u64,
    ) -> u64 {
        let divisor = self
            .distinct_count(label, property)
            .max(1)
            .min(max_divisor)
            .max(1);
        input_rows.div_ceil(divisor).max(1)
    }

    fn estimate_property_null_rows(&self, label: &str, property: &str, input_rows: u64) -> u64 {
        let distinct_count = self.distinct_count(label, property).max(1);
        input_rows.div_ceil(distinct_count.min(10)).max(1)
    }

    fn estimate_property_not_null_rows(&self, label: &str, property: &str, input_rows: u64) -> u64 {
        input_rows
            .saturating_sub(self.estimate_property_null_rows(label, property, input_rows))
            .max(1)
    }

    fn estimate_property_range_rows(
        &self,
        label: &str,
        property: &str,
        op: ComparisonOp,
        value: &Value,
        input_rows: u64,
    ) -> u64 {
        let Some(histogram) = self
            .property_histograms
            .get(&(label.to_string(), property.to_string()))
            .filter(|values| !values.is_empty())
        else {
            return input_rows.div_ceil(2).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| compare_histogram_value(candidate, op, value))
            .count() as u64;
        let distinct_count = histogram.len() as u64;
        input_rows
            .saturating_mul(matching_values)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_range_bounds_rows(
        &self,
        label: &str,
        property: &str,
        lower: Option<&ValueRangeBound>,
        upper: Option<&ValueRangeBound>,
    ) -> u64 {
        let label_count = self.label_count(label);
        let Some(histogram) = self
            .property_histograms
            .get(&(label.to_string(), property.to_string()))
            .filter(|values| !values.is_empty())
        else {
            return label_count.div_ceil(2).max(1);
        };
        let matching_values = histogram
            .iter()
            .filter(|candidate| range_bound_matches(candidate, lower, upper))
            .count() as u64;
        let distinct_count = histogram.len() as u64;
        label_count
            .saturating_mul(matching_values)
            .div_ceil(distinct_count)
            .max(1)
    }

    fn estimate_expand_rows(
        &self,
        source_label: &str,
        rel_type: &str,
        rel_properties: &BTreeMap<String, Value>,
        target_label: &str,
        min_hops: usize,
        max_hops: usize,
    ) -> ExpandEstimate {
        let path_count = self
            .path_counts
            .get(&(
                source_label.to_string(),
                rel_type.to_string(),
                target_label.to_string(),
            ))
            .copied();
        let rel_count = self.rel_type_counts.get(rel_type).copied().unwrap_or(1);
        let source_count = self
            .rel_type_source_counts
            .get(rel_type)
            .copied()
            .unwrap_or_else(|| self.label_count(source_label).max(1));
        let average_fanout = rel_count.div_ceil(source_count.max(1)).max(1);
        let one_hop = path_count.unwrap_or_else(|| {
            self.label_count(source_label)
                .min(self.label_count(target_label))
                .max(1)
        });
        let property_distinct_product = rel_properties
            .keys()
            .map(|property| self.rel_property_distinct_count(rel_type, property).max(1))
            .fold(1_u64, |acc, value| acc.saturating_mul(value))
            .max(1);
        let mut estimated_rows = 0_u64;
        let mut hop_rows = one_hop.max(1);
        let mut hop_estimates = Vec::new();
        for hop in 1..=max_hops.max(1) {
            let exact_hop_rows = self
                .bounded_path_counts
                .get(&(
                    source_label.to_string(),
                    rel_type.to_string(),
                    target_label.to_string(),
                    hop,
                ))
                .copied();
            let current_hop_rows = exact_hop_rows
                .unwrap_or(hop_rows)
                .div_ceil(property_distinct_product)
                .max(1);
            hop_estimates.push(HopEstimate {
                hop,
                rows: current_hop_rows,
                exact: exact_hop_rows.is_some(),
            });
            if hop >= min_hops {
                estimated_rows = estimated_rows.saturating_add(current_hop_rows);
            }
            hop_rows = current_hop_rows.max(1).saturating_mul(average_fanout);
        }
        ExpandEstimate {
            path_count,
            rel_count,
            source_count,
            average_fanout,
            property_distinct_product,
            estimated_rows: estimated_rows.max(1),
            hop_estimates,
        }
    }
}

impl OptimizerCatalogIndexes {
    pub fn new(
        equality_property_indexes: impl IntoIterator<Item = (String, String)>,
        composite_property_indexes: impl IntoIterator<Item = (String, Vec<String>)>,
        range_property_indexes: impl IntoIterator<Item = (String, String)>,
        full_text_property_indexes: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        Self {
            equality_property_indexes: equality_property_indexes.into_iter().collect(),
            composite_property_indexes: composite_property_indexes.into_iter().collect(),
            range_property_indexes: range_property_indexes.into_iter().collect(),
            full_text_property_indexes: full_text_property_indexes.into_iter().collect(),
        }
    }
}

impl OptimizerCatalogStatistics {
    pub fn new(
        label_counts: impl IntoIterator<Item = (String, u64)>,
        rel_type_counts: impl IntoIterator<Item = (String, u64)>,
        rel_type_source_counts: impl IntoIterator<Item = (String, u64)>,
        path_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
        bounded_path_counts: impl IntoIterator<Item = ((String, String, String, usize), u64)>,
        property_distinct_counts: impl IntoIterator<Item = ((String, String), u64)>,
        property_histograms: impl IntoIterator<Item = ((String, String), Vec<Value>)>,
    ) -> Self {
        Self {
            label_counts: label_counts.into_iter().collect(),
            rel_type_counts: rel_type_counts.into_iter().collect(),
            rel_type_source_counts: rel_type_source_counts.into_iter().collect(),
            rel_type_target_counts: BTreeMap::new(),
            path_counts: path_counts.into_iter().collect(),
            path_source_distinct_counts: BTreeMap::new(),
            path_target_distinct_counts: BTreeMap::new(),
            bounded_path_counts: bounded_path_counts.into_iter().collect(),
            bounded_path_source_distinct_counts: BTreeMap::new(),
            bounded_path_target_distinct_counts: BTreeMap::new(),
            property_distinct_counts: property_distinct_counts.into_iter().collect(),
            rel_property_distinct_counts: BTreeMap::new(),
            property_histograms: property_histograms.into_iter().collect(),
            rel_property_histograms: BTreeMap::new(),
        }
    }

    pub fn with_relationship_property_distinct_counts(
        mut self,
        rel_property_distinct_counts: impl IntoIterator<Item = ((String, String), u64)>,
    ) -> Self {
        self.rel_property_distinct_counts = rel_property_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_relationship_type_target_counts(
        mut self,
        rel_type_target_counts: impl IntoIterator<Item = (String, u64)>,
    ) -> Self {
        self.rel_type_target_counts = rel_type_target_counts.into_iter().collect();
        self
    }

    pub fn with_path_source_distinct_counts(
        mut self,
        path_source_distinct_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        self.path_source_distinct_counts = path_source_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_path_target_distinct_counts(
        mut self,
        path_target_distinct_counts: impl IntoIterator<Item = ((String, String, String), u64)>,
    ) -> Self {
        self.path_target_distinct_counts = path_target_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_bounded_path_source_distinct_counts(
        mut self,
        bounded_path_source_distinct_counts: impl IntoIterator<
            Item = ((String, String, String, usize), u64),
        >,
    ) -> Self {
        self.bounded_path_source_distinct_counts =
            bounded_path_source_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_bounded_path_target_distinct_counts(
        mut self,
        bounded_path_target_distinct_counts: impl IntoIterator<
            Item = ((String, String, String, usize), u64),
        >,
    ) -> Self {
        self.bounded_path_target_distinct_counts =
            bounded_path_target_distinct_counts.into_iter().collect();
        self
    }

    pub fn with_relationship_property_histograms(
        mut self,
        rel_property_histograms: impl IntoIterator<Item = ((String, String), Vec<Value>)>,
    ) -> Self {
        self.rel_property_histograms = rel_property_histograms.into_iter().collect();
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpandEstimate {
    path_count: Option<u64>,
    rel_count: u64,
    source_count: u64,
    average_fanout: u64,
    property_distinct_product: u64,
    estimated_rows: u64,
    hop_estimates: Vec<HopEstimate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HopEstimate {
    hop: usize,
    rows: u64,
    exact: bool,
}

fn insert_logical_group(memo: &mut GraphMemo, logical: &LogicalPlan) -> GroupId {
    let expr = GroupExpr::from_logical(logical, memo);
    memo.insert_group(expr)
}

fn best_physical(
    memo: &GraphMemo,
    root: GroupId,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> PhysicalPlan {
    let group = memo.group(root).expect("memo group id should exist");
    group
        .first_expression()
        .expect("memo group should contain at least one expression")
        .to_physical(memo, catalog, decisions)
}

impl GroupExpr {
    fn from_logical(logical: &LogicalPlan, memo: &mut GraphMemo) -> Self {
        match logical {
            LogicalPlan::CreateNodeLabel { .. }
            | LogicalPlan::CreateRelationshipType { .. }
            | LogicalPlan::CreateNodeTable { .. }
            | LogicalPlan::CreateRelationshipTable { .. }
            | LogicalPlan::CreateProperty { .. }
            | LogicalPlan::AlterTableState { .. }
            | LogicalPlan::AlterPropertyState { .. }
            | LogicalPlan::CreateIndex { .. }
            | LogicalPlan::CreateCompositeIndex { .. }
            | LogicalPlan::CreateRangeIndex { .. }
            | LogicalPlan::CreateFullTextIndex { .. }
            | LogicalPlan::CreateUniqueConstraint { .. }
            | LogicalPlan::CreateNodePropertyExistsConstraint { .. }
            | LogicalPlan::CreateRelationshipUniqueConstraint { .. }
            | LogicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
            | LogicalPlan::ProjectGraph { .. }
            | LogicalPlan::GraphAlgorithm { .. }
            | LogicalPlan::CreateNode { .. }
            | LogicalPlan::MergeNode { .. }
            | LogicalPlan::MergeRelationship { .. }
            | LogicalPlan::MergeMatchedRelationship { .. }
            | LogicalPlan::MergeRelationshipFromMatchedRelationship { .. }
            | LogicalPlan::MergeRelationshipToMatchedTarget { .. }
            | LogicalPlan::MergeRelationshipFromMatchedTarget { .. }
            | LogicalPlan::CreateMatchedRelationship { .. }
            | LogicalPlan::SetNodeProperty { .. }
            | LogicalPlan::SetNodeProperties { .. }
            | LogicalPlan::SetNodePropertiesReturn { .. }
            | LogicalPlan::SetRelationshipProperty { .. }
            | LogicalPlan::SetRelationshipProperties { .. }
            | LogicalPlan::DeleteNode { .. }
            | LogicalPlan::DeleteRelationship { .. }
            | LogicalPlan::DeleteRelationshipTargetNodes { .. }
            | LogicalPlan::CreateRelationship { .. }
            | LogicalPlan::NodeScan { .. }
            | LogicalPlan::OptionalRelationshipCountSum { .. }
            | LogicalPlan::ThreadRepairStats { .. }
            | LogicalPlan::ShortestPath { .. } => Self {
                logical: logical.clone(),
                children: Vec::new(),
            },
            LogicalPlan::NodeCartesianProduct { left, right } => Self {
                logical: logical.clone(),
                children: vec![
                    insert_logical_group(memo, left),
                    insert_logical_group(memo, right),
                ],
            },
            LogicalPlan::Expand { input, .. }
            | LogicalPlan::NodeColumnLookup { input, .. }
            | LogicalPlan::OptionalDegree { input, .. }
            | LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => Self {
                logical: logical.clone(),
                children: vec![insert_logical_group(memo, input)],
            },
        }
    }

    fn to_physical(
        &self,
        memo: &GraphMemo,
        catalog: &OptimizerCatalog,
        decisions: &mut Vec<String>,
    ) -> PhysicalPlan {
        match &self.logical {
            LogicalPlan::CreateNodeLabel { label } => PhysicalPlan::CreateNodeLabel {
                label: label.clone(),
            },
            LogicalPlan::CreateRelationshipType { rel_type } => {
                PhysicalPlan::CreateRelationshipType {
                    rel_type: rel_type.clone(),
                }
            }
            LogicalPlan::CreateNodeTable { name } => {
                PhysicalPlan::CreateNodeTable { name: name.clone() }
            }
            LogicalPlan::CreateRelationshipTable { name } => {
                PhysicalPlan::CreateRelationshipTable { name: name.clone() }
            }
            LogicalPlan::CreateProperty {
                table_kind,
                table,
                property,
                value_type,
                nullable,
            } => PhysicalPlan::CreateProperty {
                table_kind: *table_kind,
                table: table.clone(),
                property: property.clone(),
                value_type: *value_type,
                nullable: *nullable,
            },
            LogicalPlan::AlterTableState {
                table_kind,
                table,
                state,
            } => PhysicalPlan::AlterTableState {
                table_kind: *table_kind,
                table: table.clone(),
                state: *state,
            },
            LogicalPlan::AlterPropertyState {
                table_kind,
                table,
                property,
                state,
            } => PhysicalPlan::AlterPropertyState {
                table_kind: *table_kind,
                table: table.clone(),
                property: property.clone(),
                state: *state,
            },
            LogicalPlan::CreateIndex { label, property } => PhysicalPlan::CreateIndex {
                label: label.clone(),
                property: property.clone(),
            },
            LogicalPlan::CreateCompositeIndex { label, properties } => {
                PhysicalPlan::CreateCompositeIndex {
                    label: label.clone(),
                    properties: properties.clone(),
                }
            }
            LogicalPlan::CreateRangeIndex { label, property } => PhysicalPlan::CreateRangeIndex {
                label: label.clone(),
                property: property.clone(),
            },
            LogicalPlan::CreateFullTextIndex { label, property } => {
                PhysicalPlan::CreateFullTextIndex {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateUniqueConstraint { label, property } => {
                PhysicalPlan::CreateUniqueConstraint {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
                PhysicalPlan::CreateNodePropertyExistsConstraint {
                    label: label.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
                PhysicalPlan::CreateRelationshipUniqueConstraint {
                    rel_type: rel_type.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
                PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                    rel_type: rel_type.clone(),
                    property: property.clone(),
                }
            }
            LogicalPlan::ProjectGraph {
                name,
                node_labels,
                rel_types,
            } => PhysicalPlan::ProjectGraph {
                name: name.clone(),
                node_labels: node_labels.clone(),
                rel_types: rel_types.clone(),
            },
            LogicalPlan::GraphAlgorithm {
                algorithm,
                graph_name,
                options,
                score_column,
            } => PhysicalPlan::GraphAlgorithm {
                algorithm: *algorithm,
                graph_name: graph_name.clone(),
                options: *options,
                score_column: score_column.clone(),
            },
            LogicalPlan::CreateNode { label, properties } => PhysicalPlan::CreateNode {
                label: label.clone(),
                properties: properties.clone(),
            },
            LogicalPlan::MergeNode {
                label,
                match_properties,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            } => PhysicalPlan::MergeNode {
                label: label.clone(),
                match_properties: match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
                on_match_assignments: on_match_assignments.clone(),
                post_merge_assignments: post_merge_assignments.clone(),
            },
            LogicalPlan::MergeRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => PhysicalPlan::MergeRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::MergeMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                rel_type: rel_type.clone(),
                rel_match_properties: rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label,
                source_properties,
                old_rel_variable: _,
                old_rel_type,
                old_rel_properties,
                target_label,
                target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipToMatchedTarget {
                source_label,
                source_properties,
                old_rel_type,
                old_rel_properties,
                old_target_label,
                old_target_properties,
                new_target_label,
                new_target_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeRelationshipToMatchedTarget {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_properties: old_target_properties.clone(),
                new_target_label: new_target_label.clone(),
                new_target_properties: new_target_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label,
                old_source_properties,
                old_rel_type,
                old_rel_properties,
                old_target_label,
                old_target_properties,
                new_source_label,
                new_source_properties,
                new_rel_type,
                new_rel_match_properties,
                on_create_properties,
            } => PhysicalPlan::MergeRelationshipFromMatchedTarget {
                old_source_label: old_source_label.clone(),
                old_source_properties: old_source_properties.clone(),
                old_rel_type: old_rel_type.clone(),
                old_rel_properties: old_rel_properties.clone(),
                old_target_label: old_target_label.clone(),
                old_target_properties: old_target_properties.clone(),
                new_source_label: new_source_label.clone(),
                new_source_properties: new_source_properties.clone(),
                new_rel_type: new_rel_type.clone(),
                new_rel_match_properties: new_rel_match_properties.clone(),
                on_create_properties: on_create_properties.clone(),
            },
            LogicalPlan::CreateMatchedRelationship {
                source_label,
                source_properties,
                target_label,
                target_properties,
                rel_type,
                rel_properties,
            } => PhysicalPlan::CreateMatchedRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
            },
            LogicalPlan::SetNodeProperty {
                variable,
                label,
                predicate,
                property,
                value,
            } => PhysicalPlan::SetNodeProperty {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                property: property.clone(),
                value: value.clone(),
            },
            LogicalPlan::SetNodeProperties {
                variable,
                label,
                predicate,
                assignments,
            } => PhysicalPlan::SetNodeProperties {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                assignments: assignments.clone(),
            },
            LogicalPlan::SetNodePropertiesReturn {
                variable,
                label,
                predicate,
                assignments,
                returns,
            } => PhysicalPlan::SetNodePropertiesReturn {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                assignments: assignments.clone(),
                returns: returns.clone(),
            },
            LogicalPlan::SetRelationshipProperty {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                property,
                value,
            } => PhysicalPlan::SetRelationshipProperty {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                property: property.clone(),
                value: value.clone(),
            },
            LogicalPlan::SetRelationshipProperties {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
                assignments,
            } => PhysicalPlan::SetRelationshipProperties {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                assignments: assignments.clone(),
            },
            LogicalPlan::DeleteNode {
                variable,
                label,
                predicate,
                detach,
            } => PhysicalPlan::DeleteNode {
                variable: variable.clone(),
                label: label.clone(),
                predicate: predicate.clone(),
                detach: *detach,
            },
            LogicalPlan::DeleteRelationship {
                source_variable,
                source_label,
                predicate,
                rel_variable,
                rel_type,
                rel_properties,
                rel_predicate,
                target_variable,
                target_label,
                target_properties,
            } => PhysicalPlan::DeleteRelationship {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                predicate: predicate.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                rel_predicate: rel_predicate.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::DeleteRelationshipTargetNodes {
                source_variable,
                source_label,
                source_predicate,
                rel_type,
                rel_properties,
                target_variable,
                target_label,
                target_properties,
                detach,
            } => PhysicalPlan::DeleteRelationshipTargetNodes {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                source_predicate: source_predicate.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                detach: *detach,
            },
            LogicalPlan::CreateRelationship {
                source_label,
                source_properties,
                rel_type,
                rel_properties,
                target_label,
                target_properties,
            } => PhysicalPlan::CreateRelationship {
                source_label: source_label.clone(),
                source_properties: source_properties.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
            },
            LogicalPlan::NodeScan { variable, label } => PhysicalPlan::SeqNodeScan {
                variable: variable.clone(),
                label: label.clone(),
            },
            LogicalPlan::NodeCartesianProduct { .. } => {
                let left = best_physical(memo, self.children[0], catalog, decisions);
                let right = best_physical(memo, self.children[1], catalog, decisions);
                let (left, right) =
                    order_single_row_cartesian_product_children(catalog, decisions, left, right);
                push_cartesian_product_cost_decision(catalog, decisions, &left, &right);
                PhysicalPlan::NodeCartesianProductExec {
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            LogicalPlan::NodeColumnLookup {
                variable,
                label,
                property,
                column,
                optional,
                ..
            } => PhysicalPlan::NodeColumnLookupExec {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                column: column.clone(),
                optional: *optional,
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::Expand {
                source_variable,
                source_label,
                rel_variable,
                rel_type,
                rel_properties,
                direction,
                target_variable,
                target_label,
                min_hops,
                max_hops,
                optional,
                ..
            } => {
                push_expand_estimate_decision(
                    catalog,
                    decisions,
                    ExpandEstimateRequest {
                        source_label,
                        rel_type,
                        rel_properties,
                        target_label,
                        min_hops: *min_hops,
                        max_hops: *max_hops,
                    },
                );
                PhysicalPlan::AdjacencyExpandExec {
                    source_variable: source_variable.clone(),
                    source_label: source_label.clone(),
                    rel_variable: rel_variable.clone(),
                    rel_type: rel_type.clone(),
                    rel_properties: rel_properties.clone(),
                    direction: *direction,
                    target_variable: target_variable.clone(),
                    target_label: target_label.clone(),
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                    optional: *optional,
                    input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
                }
            }
            LogicalPlan::OptionalDegree {
                source_variable,
                rel_type,
                rel_properties,
                direction,
                target_label,
                target_properties,
                alias,
                ..
            } => PhysicalPlan::OptionalDegreeExec {
                source_variable: source_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                direction: *direction,
                target_label: target_label.clone(),
                target_properties: target_properties.clone(),
                alias: alias.clone(),
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::OptionalRelationshipCountSum {
                variable,
                label,
                properties,
                legs,
                output,
            } => {
                push_optional_relationship_count_sum_cost_decision(
                    catalog, decisions, label, properties, legs,
                );
                PhysicalPlan::OptionalRelationshipCountSumExec {
                    variable: variable.clone(),
                    label: label.clone(),
                    properties: properties.clone(),
                    legs: legs.clone(),
                    output: output.clone(),
                }
            }
            LogicalPlan::ThreadRepairStats {
                label,
                identity_label,
                identity_ref_property,
                thread_id_property,
                message_rel_type,
                message_label,
                memory_rel_type,
                memory_label,
            } => PhysicalPlan::ThreadRepairStatsExec {
                label: label.clone(),
                identity_label: identity_label.clone(),
                identity_ref_property: identity_ref_property.clone(),
                thread_id_property: thread_id_property.clone(),
                message_rel_type: message_rel_type.clone(),
                message_label: message_label.clone(),
                memory_rel_type: memory_rel_type.clone(),
                memory_label: memory_label.clone(),
            },
            LogicalPlan::ShortestPath {
                source_variable,
                source_label,
                source_id,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_id,
                min_hops,
                max_hops,
                returns,
            } => PhysicalPlan::ShortestPathExec {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                source_id: source_id.clone(),
                rel_type: rel_type.clone(),
                direction: *direction,
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                target_id: target_id.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                returns: returns.clone(),
            },
            LogicalPlan::Filter { predicate, input } => {
                if let Some(plan) = index_seek_from_filter(predicate, input, catalog, decisions) {
                    plan
                } else {
                    PhysicalPlan::FilterExec {
                        predicate: predicate.clone(),
                        input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
                    }
                }
            }
            LogicalPlan::Project { items, .. } => PhysicalPlan::ProjectExec {
                items: items.clone(),
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::Aggregate {
                group_keys, items, ..
            } => PhysicalPlan::AggregateExec {
                group_keys: group_keys.clone(),
                items: items.clone(),
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::Distinct { .. } => PhysicalPlan::DistinctExec {
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::Sort { items, .. } => PhysicalPlan::SortExec {
                items: items.clone(),
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
            LogicalPlan::Limit { offset, limit, .. } => PhysicalPlan::LimitExec {
                offset: *offset,
                limit: *limit,
                input: Box::new(best_physical(memo, self.children[0], catalog, decisions)),
            },
        }
    }
}

fn logical_group_count(logical: &LogicalPlan) -> usize {
    match logical {
        LogicalPlan::Expand { input, .. }
        | LogicalPlan::NodeColumnLookup { input, .. }
        | LogicalPlan::OptionalDegree { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. } => 1 + logical_group_count(input),
        LogicalPlan::OptionalRelationshipCountSum { .. }
        | LogicalPlan::ThreadRepairStats { .. }
        | LogicalPlan::ShortestPath { .. } => 1,
        LogicalPlan::CreateNodeLabel { .. }
        | LogicalPlan::CreateRelationshipType { .. }
        | LogicalPlan::CreateNodeTable { .. }
        | LogicalPlan::CreateRelationshipTable { .. }
        | LogicalPlan::CreateProperty { .. }
        | LogicalPlan::AlterTableState { .. }
        | LogicalPlan::AlterPropertyState { .. }
        | LogicalPlan::CreateIndex { .. }
        | LogicalPlan::CreateCompositeIndex { .. }
        | LogicalPlan::CreateRangeIndex { .. }
        | LogicalPlan::CreateFullTextIndex { .. }
        | LogicalPlan::CreateUniqueConstraint { .. }
        | LogicalPlan::CreateNodePropertyExistsConstraint { .. }
        | LogicalPlan::CreateRelationshipUniqueConstraint { .. }
        | LogicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | LogicalPlan::ProjectGraph { .. }
        | LogicalPlan::GraphAlgorithm { .. }
        | LogicalPlan::CreateNode { .. }
        | LogicalPlan::MergeNode { .. }
        | LogicalPlan::MergeRelationship { .. }
        | LogicalPlan::MergeMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | LogicalPlan::MergeRelationshipToMatchedTarget { .. }
        | LogicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | LogicalPlan::CreateMatchedRelationship { .. }
        | LogicalPlan::SetNodeProperty { .. }
        | LogicalPlan::SetNodeProperties { .. }
        | LogicalPlan::SetNodePropertiesReturn { .. }
        | LogicalPlan::SetRelationshipProperty { .. }
        | LogicalPlan::SetRelationshipProperties { .. }
        | LogicalPlan::DeleteNode { .. }
        | LogicalPlan::DeleteRelationship { .. }
        | LogicalPlan::DeleteRelationshipTargetNodes { .. }
        | LogicalPlan::CreateRelationship { .. }
        | LogicalPlan::NodeScan { .. } => 1,
        LogicalPlan::NodeCartesianProduct { left, right } => {
            1 + logical_group_count(left) + logical_group_count(right)
        }
    }
}

fn logical_to_physical_direct(
    logical: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> PhysicalPlan {
    match logical {
        LogicalPlan::CreateNodeLabel { label } => PhysicalPlan::CreateNodeLabel {
            label: label.clone(),
        },
        LogicalPlan::CreateRelationshipType { rel_type } => PhysicalPlan::CreateRelationshipType {
            rel_type: rel_type.clone(),
        },
        LogicalPlan::CreateNodeTable { name } => {
            PhysicalPlan::CreateNodeTable { name: name.clone() }
        }
        LogicalPlan::CreateRelationshipTable { name } => {
            PhysicalPlan::CreateRelationshipTable { name: name.clone() }
        }
        LogicalPlan::CreateProperty {
            table_kind,
            table,
            property,
            value_type,
            nullable,
        } => PhysicalPlan::CreateProperty {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            value_type: *value_type,
            nullable: *nullable,
        },
        LogicalPlan::AlterTableState {
            table_kind,
            table,
            state,
        } => PhysicalPlan::AlterTableState {
            table_kind: *table_kind,
            table: table.clone(),
            state: *state,
        },
        LogicalPlan::AlterPropertyState {
            table_kind,
            table,
            property,
            state,
        } => PhysicalPlan::AlterPropertyState {
            table_kind: *table_kind,
            table: table.clone(),
            property: property.clone(),
            state: *state,
        },
        LogicalPlan::CreateIndex { label, property } => PhysicalPlan::CreateIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateCompositeIndex { label, properties } => {
            PhysicalPlan::CreateCompositeIndex {
                label: label.clone(),
                properties: properties.clone(),
            }
        }
        LogicalPlan::CreateRangeIndex { label, property } => PhysicalPlan::CreateRangeIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateFullTextIndex { label, property } => PhysicalPlan::CreateFullTextIndex {
            label: label.clone(),
            property: property.clone(),
        },
        LogicalPlan::CreateUniqueConstraint { label, property } => {
            PhysicalPlan::CreateUniqueConstraint {
                label: label.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateNodePropertyExistsConstraint { label, property } => {
            PhysicalPlan::CreateNodePropertyExistsConstraint {
                label: label.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateRelationshipUniqueConstraint { rel_type, property } => {
            PhysicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::CreateRelationshipPropertyExistsConstraint { rel_type, property } => {
            PhysicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: rel_type.clone(),
                property: property.clone(),
            }
        }
        LogicalPlan::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => PhysicalPlan::ProjectGraph {
            name: name.clone(),
            node_labels: node_labels.clone(),
            rel_types: rel_types.clone(),
        },
        LogicalPlan::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
            score_column,
        } => PhysicalPlan::GraphAlgorithm {
            algorithm: *algorithm,
            graph_name: graph_name.clone(),
            options: *options,
            score_column: score_column.clone(),
        },
        LogicalPlan::CreateNode { label, properties } => PhysicalPlan::CreateNode {
            label: label.clone(),
            properties: properties.clone(),
        },
        LogicalPlan::MergeNode {
            label,
            match_properties,
            on_create_properties,
            on_match_assignments,
            post_merge_assignments,
        } => PhysicalPlan::MergeNode {
            label: label.clone(),
            match_properties: match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
            on_match_assignments: on_match_assignments.clone(),
            post_merge_assignments: post_merge_assignments.clone(),
        },
        LogicalPlan::MergeRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => PhysicalPlan::MergeRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        },
        LogicalPlan::MergeMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_match_properties: rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label,
            source_properties,
            old_rel_variable: _,
            old_rel_type,
            old_rel_properties,
            target_label,
            target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeRelationshipFromMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipToMatchedTarget {
            source_label,
            source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_target_label,
            new_target_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeRelationshipToMatchedTarget {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_target_label: new_target_label.clone(),
            new_target_properties: new_target_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label,
            old_source_properties,
            old_rel_type,
            old_rel_properties,
            old_target_label,
            old_target_properties,
            new_source_label,
            new_source_properties,
            new_rel_type,
            new_rel_match_properties,
            on_create_properties,
        } => PhysicalPlan::MergeRelationshipFromMatchedTarget {
            old_source_label: old_source_label.clone(),
            old_source_properties: old_source_properties.clone(),
            old_rel_type: old_rel_type.clone(),
            old_rel_properties: old_rel_properties.clone(),
            old_target_label: old_target_label.clone(),
            old_target_properties: old_target_properties.clone(),
            new_source_label: new_source_label.clone(),
            new_source_properties: new_source_properties.clone(),
            new_rel_type: new_rel_type.clone(),
            new_rel_match_properties: new_rel_match_properties.clone(),
            on_create_properties: on_create_properties.clone(),
        },
        LogicalPlan::CreateMatchedRelationship {
            source_label,
            source_properties,
            target_label,
            target_properties,
            rel_type,
            rel_properties,
        } => PhysicalPlan::CreateMatchedRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
        },
        LogicalPlan::SetNodeProperty {
            variable,
            label,
            predicate,
            property,
            value,
        } => PhysicalPlan::SetNodeProperty {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            property: property.clone(),
            value: value.clone(),
        },
        LogicalPlan::SetNodeProperties {
            variable,
            label,
            predicate,
            assignments,
        } => PhysicalPlan::SetNodeProperties {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
        },
        LogicalPlan::SetNodePropertiesReturn {
            variable,
            label,
            predicate,
            assignments,
            returns,
        } => PhysicalPlan::SetNodePropertiesReturn {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            assignments: assignments.clone(),
            returns: returns.clone(),
        },
        LogicalPlan::SetRelationshipProperty {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            property,
            value,
        } => PhysicalPlan::SetRelationshipProperty {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            property: property.clone(),
            value: value.clone(),
        },
        LogicalPlan::SetRelationshipProperties {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
            assignments,
        } => PhysicalPlan::SetRelationshipProperties {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            assignments: assignments.clone(),
        },
        LogicalPlan::DeleteNode {
            variable,
            label,
            predicate,
            detach,
        } => PhysicalPlan::DeleteNode {
            variable: variable.clone(),
            label: label.clone(),
            predicate: predicate.clone(),
            detach: *detach,
        },
        LogicalPlan::DeleteRelationship {
            source_variable,
            source_label,
            predicate,
            rel_variable,
            rel_type,
            rel_properties,
            rel_predicate,
            target_variable,
            target_label,
            target_properties,
        } => PhysicalPlan::DeleteRelationship {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            predicate: predicate.clone(),
            rel_variable: rel_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            rel_predicate: rel_predicate.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        },
        LogicalPlan::DeleteRelationshipTargetNodes {
            source_variable,
            source_label,
            source_predicate,
            rel_type,
            rel_properties,
            target_variable,
            target_label,
            target_properties,
            detach,
        } => PhysicalPlan::DeleteRelationshipTargetNodes {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_predicate: source_predicate.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            detach: *detach,
        },
        LogicalPlan::CreateRelationship {
            source_label,
            source_properties,
            rel_type,
            rel_properties,
            target_label,
            target_properties,
        } => PhysicalPlan::CreateRelationship {
            source_label: source_label.clone(),
            source_properties: source_properties.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
        },
        LogicalPlan::NodeScan { variable, label } => PhysicalPlan::SeqNodeScan {
            variable: variable.clone(),
            label: label.clone(),
        },
        LogicalPlan::NodeCartesianProduct { left, right } => {
            let left = logical_to_physical_direct(left, catalog, decisions);
            let right = logical_to_physical_direct(right, catalog, decisions);
            let (left, right) =
                order_single_row_cartesian_product_children(catalog, decisions, left, right);
            push_cartesian_product_cost_decision(catalog, decisions, &left, &right);
            PhysicalPlan::NodeCartesianProductExec {
                left: Box::new(left),
                right: Box::new(right),
            }
        }
        LogicalPlan::NodeColumnLookup {
            variable,
            label,
            property,
            column,
            optional,
            input,
        } => PhysicalPlan::NodeColumnLookupExec {
            variable: variable.clone(),
            label: label.clone(),
            property: property.clone(),
            column: column.clone(),
            optional: *optional,
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::Expand {
            source_variable,
            source_label,
            rel_variable,
            rel_type,
            rel_properties,
            direction,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            optional,
            input,
        } => {
            push_expand_estimate_decision(
                catalog,
                decisions,
                ExpandEstimateRequest {
                    source_label,
                    rel_type,
                    rel_properties,
                    target_label,
                    min_hops: *min_hops,
                    max_hops: *max_hops,
                },
            );
            PhysicalPlan::AdjacencyExpandExec {
                source_variable: source_variable.clone(),
                source_label: source_label.clone(),
                rel_variable: rel_variable.clone(),
                rel_type: rel_type.clone(),
                rel_properties: rel_properties.clone(),
                direction: *direction,
                target_variable: target_variable.clone(),
                target_label: target_label.clone(),
                min_hops: *min_hops,
                max_hops: *max_hops,
                optional: *optional,
                input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
            }
        }
        LogicalPlan::OptionalDegree {
            source_variable,
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            alias,
            input,
        } => PhysicalPlan::OptionalDegreeExec {
            source_variable: source_variable.clone(),
            rel_type: rel_type.clone(),
            rel_properties: rel_properties.clone(),
            direction: *direction,
            target_label: target_label.clone(),
            target_properties: target_properties.clone(),
            alias: alias.clone(),
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::OptionalRelationshipCountSum {
            variable,
            label,
            properties,
            legs,
            output,
        } => {
            push_optional_relationship_count_sum_cost_decision(
                catalog, decisions, label, properties, legs,
            );
            PhysicalPlan::OptionalRelationshipCountSumExec {
                variable: variable.clone(),
                label: label.clone(),
                properties: properties.clone(),
                legs: legs.clone(),
                output: output.clone(),
            }
        }
        LogicalPlan::ThreadRepairStats {
            label,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type,
            message_label,
            memory_rel_type,
            memory_label,
        } => PhysicalPlan::ThreadRepairStatsExec {
            label: label.clone(),
            identity_label: identity_label.clone(),
            identity_ref_property: identity_ref_property.clone(),
            thread_id_property: thread_id_property.clone(),
            message_rel_type: message_rel_type.clone(),
            message_label: message_label.clone(),
            memory_rel_type: memory_rel_type.clone(),
            memory_label: memory_label.clone(),
        },
        LogicalPlan::ShortestPath {
            source_variable,
            source_label,
            source_id,
            rel_type,
            direction,
            target_variable,
            target_label,
            target_id,
            min_hops,
            max_hops,
            returns,
        } => PhysicalPlan::ShortestPathExec {
            source_variable: source_variable.clone(),
            source_label: source_label.clone(),
            source_id: source_id.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
            target_label: target_label.clone(),
            target_id: target_id.clone(),
            min_hops: *min_hops,
            max_hops: *max_hops,
            returns: returns.clone(),
        },
        LogicalPlan::Filter { predicate, input } => {
            if let Some(plan) = index_seek_from_filter(predicate, input, catalog, decisions) {
                plan
            } else {
                PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
                }
            }
        }
        LogicalPlan::Project { items, input } => PhysicalPlan::ProjectExec {
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::Aggregate {
            group_keys,
            items,
            input,
        } => PhysicalPlan::AggregateExec {
            group_keys: group_keys.clone(),
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::Distinct { input } => PhysicalPlan::DistinctExec {
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::Sort { items, input } => PhysicalPlan::SortExec {
            items: items.clone(),
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
        LogicalPlan::Limit {
            offset,
            limit,
            input,
        } => PhysicalPlan::LimitExec {
            offset: *offset,
            limit: *limit,
            input: Box::new(logical_to_physical_direct(input, catalog, decisions)),
        },
    }
}

struct ExpandEstimateRequest<'a> {
    source_label: &'a str,
    rel_type: &'a str,
    rel_properties: &'a BTreeMap<String, Value>,
    target_label: &'a str,
    min_hops: usize,
    max_hops: usize,
}

fn push_expand_estimate_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    request: ExpandEstimateRequest<'_>,
) {
    let estimate = catalog.estimate_expand_rows(
        request.source_label,
        request.rel_type,
        request.rel_properties,
        request.target_label,
        request.min_hops,
        request.max_hops,
    );
    let path_count = estimate
        .path_count
        .map(|count| count.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let hop_rows = estimate
        .hop_estimates
        .iter()
        .map(|estimate| {
            format!(
                "{}:{}:{}",
                estimate.hop,
                if estimate.exact { "exact" } else { "fallback" },
                estimate.rows
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    decisions.push(format!(
        "estimate AdjacencyExpand for {}-[:{}*{}..{}]->{}: path_count={path_count} rel_count={} rel_type_sources={} average_fanout={} rel_property_distinct_product={} hop_rows=[{}] estimated_rows={}",
        request.source_label,
        request.rel_type,
        request.min_hops,
        request.max_hops,
        request.target_label,
        estimate.rel_count,
        estimate.source_count,
        estimate.average_fanout,
        estimate.property_distinct_product,
        hop_rows,
        estimate.estimated_rows
    ));
}

fn selected_plan_trace(plan: &PhysicalPlan, catalog: &OptimizerCatalog) -> SelectedPlanTrace {
    let selected_plan_cost = estimate_physical_plan_cost(plan, catalog);
    SelectedPlanTrace {
        explain: plan.explain(0),
        fingerprint: plan.fingerprint(),
        cost: selected_plan_cost,
        cost_breakdown: estimate_physical_plan_cost_breakdown(plan, catalog),
        properties: selected_plan_properties(plan),
        operator_counts: plan_operator_counts(plan),
        class_counts: plan_class_counts(plan),
    }
}

fn selected_plan_properties(plan: &PhysicalPlan) -> PhysicalProperties {
    let ordering = match plan {
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => selected_plan_properties(input).ordering,
        PhysicalPlan::SortExec { items, .. } => sort_ordering_keys(items),
        _ => Vec::new(),
    };
    PhysicalProperties {
        distribution: Distribution::Single,
        ordering,
    }
}

fn sort_ordering_keys(items: &[SortItem]) -> Vec<String> {
    items.iter().map(sort_ordering_key).collect()
}

fn sort_ordering_key(item: &SortItem) -> String {
    let mut output = String::new();
    match &item.key {
        SortKey::Property { variable, property } => {
            output.push_str(variable);
            output.push('.');
            output.push_str(property);
        }
        SortKey::Id { variable } => {
            output.push_str("id(");
            output.push_str(variable);
            output.push(')');
        }
        SortKey::Expression(expression) => write_projection_expression(&mut output, expression),
        SortKey::Column(column) => output.push_str(column),
    }
    output.push(' ');
    match item.direction {
        SortDirection::Asc => output.push_str("asc"),
        SortDirection::Desc => output.push_str("desc"),
    }
    output
}

fn order_single_row_cartesian_product_children(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    left: PhysicalPlan,
    right: PhysicalPlan,
) -> (PhysicalPlan, PhysicalPlan) {
    let left_cost = estimate_physical_plan_cost(&left, catalog);
    let right_cost = estimate_physical_plan_cost(&right, catalog);
    if !cartesian_product_leaves_are_single_row(catalog, &left)
        || !cartesian_product_leaves_are_single_row(catalog, &right)
    {
        decisions.push(format!(
            "keep NodeCartesianProduct input order: left_rows={} right_rows={} reason=non_single_row_input",
            left_cost.estimated_rows, right_cost.estimated_rows
        ));
        return (left, right);
    }

    let mut leaves = Vec::new();
    collect_cartesian_product_leaves(left, &mut leaves);
    collect_cartesian_product_leaves(right, &mut leaves);

    let mut keyed_leaves = leaves
        .into_iter()
        .map(|plan| {
            let cost = estimate_physical_plan_cost(&plan, catalog);
            let fingerprint = plan.fingerprint();
            (cost, fingerprint, plan)
        })
        .collect::<Vec<_>>();

    let original_order = keyed_leaves
        .iter()
        .map(|(cost, fingerprint, _)| format!("{}:{}", cost.cost, fingerprint))
        .collect::<Vec<_>>()
        .join("|");
    keyed_leaves.sort_by(
        |(left_cost, left_fingerprint, _), (right_cost, right_fingerprint, _)| {
            (left_cost.cost, left_fingerprint).cmp(&(right_cost.cost, right_fingerprint))
        },
    );
    let ordered = keyed_leaves
        .iter()
        .map(|(cost, fingerprint, _)| format!("{}:{}", cost.cost, fingerprint))
        .collect::<Vec<_>>()
        .join("|");
    if original_order == ordered {
        decisions.push(format!(
            "keep NodeCartesianProduct single-row input order: inputs={} order={ordered}",
            keyed_leaves.len()
        ));
    } else {
        decisions.push(format!(
            "order NodeCartesianProduct single-row inputs: inputs={} original={original_order} ordered={ordered}",
            keyed_leaves.len()
        ));
    }

    let mut ordered_plans = keyed_leaves.into_iter().map(|(_, _, plan)| plan);
    let left = ordered_plans
        .next()
        .expect("cartesian product has left input");
    let right = rebuild_cartesian_product(ordered_plans);
    (left, right)
}

fn cartesian_product_leaves_are_single_row(
    catalog: &OptimizerCatalog,
    plan: &PhysicalPlan,
) -> bool {
    match plan {
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            cartesian_product_leaves_are_single_row(catalog, left)
                && cartesian_product_leaves_are_single_row(catalog, right)
        }
        plan => estimate_physical_plan_cost(plan, catalog).estimated_rows == 1,
    }
}

fn collect_cartesian_product_leaves(plan: PhysicalPlan, leaves: &mut Vec<PhysicalPlan>) {
    match plan {
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            collect_cartesian_product_leaves(*left, leaves);
            collect_cartesian_product_leaves(*right, leaves);
        }
        plan => leaves.push(plan),
    }
}

fn rebuild_cartesian_product(plans: impl IntoIterator<Item = PhysicalPlan>) -> PhysicalPlan {
    let mut plans = plans.into_iter();
    let first = plans
        .next()
        .expect("cartesian product rebuild requires at least one input");
    plans.fold(first, |left, right| {
        PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(left),
            right: Box::new(right),
        }
    })
}

fn push_cartesian_product_cost_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    left: &PhysicalPlan,
    right: &PhysicalPlan,
) {
    let left_cost = estimate_physical_plan_cost(left, catalog);
    let right_cost = estimate_physical_plan_cost(right, catalog);
    let cost = estimate_node_cartesian_product_cost(left_cost, right_cost);
    decisions.push(format!(
        "estimate NodeCartesianProduct: left_rows={} right_rows={} output_rows={} left_cost={} right_cost={} cost={}",
        left_cost.estimated_rows,
        right_cost.estimated_rows,
        cost.estimated_rows,
        left_cost.cost,
        right_cost.cost,
        cost.cost
    ));
}

fn push_optional_relationship_count_sum_cost_decision(
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
) {
    let estimate = estimate_optional_relationship_count_sum(label, properties, legs, catalog);
    let leg_rows = estimate
        .leg_rows
        .iter()
        .map(|leg| {
            format!(
                "{}:{}:{}",
                leg.rel_type,
                format_relationship_direction(leg.direction),
                leg.rows
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    decisions.push(format!(
        "estimate OptionalRelationshipCountSum for {label}: seed_rows={} leg_rows=[{}] estimated_rows={} cost={}",
        estimate.seed_rows, leg_rows, estimate.cost.estimated_rows, estimate.cost.cost
    ));
}

fn estimate_node_cartesian_product_cost(left_cost: PlanCost, right_cost: PlanCost) -> PlanCost {
    let rows = left_cost
        .estimated_rows
        .saturating_mul(right_cost.estimated_rows)
        .max(1);
    PlanCost {
        estimated_rows: rows,
        cost: left_cost
            .cost
            .saturating_add(right_cost.cost)
            .saturating_add(rows),
    }
}

fn estimate_physical_plan_cost(plan: &PhysicalPlan, catalog: &OptimizerCatalog) -> PlanCost {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. } => {
            let rows = catalog.label_count(label);
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_add(4),
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left_cost = estimate_physical_plan_cost(left, catalog);
            let right_cost = estimate_physical_plan_cost(right, catalog);
            estimate_node_cartesian_product_cost(left_cost, right_cost)
        }
        PhysicalPlan::NodeColumnLookupExec { label, input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let label_rows = catalog.label_count(label).max(1);
            PlanCost {
                estimated_rows: input_cost.estimated_rows.max(1),
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(label_rows)),
            }
        }
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        } => {
            let rows = catalog
                .label_count(label)
                .div_ceil(catalog.distinct_count(label, property).max(1))
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_mul(2).saturating_add(1),
            }
        }
        PhysicalPlan::IndexNodeMultiSeek {
            label,
            property,
            values,
            ..
        } => {
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = catalog.label_count(label).div_ceil(distinct_count).max(1);
            let rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(catalog.label_count(label))
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_mul(2).saturating_add(values.len() as u64),
            }
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => {
            let distinct_product = predicates
                .iter()
                .map(|(property, _)| catalog.distinct_count(label, property).max(1))
                .fold(1_u64, |acc, value| acc.saturating_mul(value))
                .max(1);
            let rows = catalog.label_count(label).div_ceil(distinct_product).max(1);
            PlanCost {
                estimated_rows: rows,
                cost: rows
                    .saturating_mul(2)
                    .saturating_add(predicates.len() as u64),
            }
        }
        PhysicalPlan::IndexNodeRangeSeek {
            label,
            property,
            lower,
            upper,
            ..
        } => {
            let rows =
                catalog.estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref());
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_mul(2).saturating_add(2),
            }
        }
        PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            let rows = catalog.label_count(label).div_ceil(4).max(1);
            PlanCost {
                estimated_rows: rows,
                cost: rows.saturating_mul(2).saturating_add(3),
            }
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            rel_properties,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let expand_estimate = catalog.estimate_expand_rows(
                source_label,
                rel_type,
                rel_properties,
                target_label,
                *min_hops,
                *max_hops,
            );
            let source_rows = catalog.label_count(source_label).max(1);
            let scaled_rows = expand_estimate
                .estimated_rows
                .saturating_mul(input_cost.estimated_rows.max(1))
                .div_ceil(source_rows)
                .max(1);
            PlanCost {
                estimated_rows: scaled_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows)
                    .saturating_add(scaled_rows),
            }
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let rows = estimate_filter_rows(predicate, input, input_cost.estimated_rows, catalog);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::ProjectExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let degree_work = estimate_optional_degree_work(
                rel_type,
                rel_properties,
                *direction,
                target_label,
                target_properties,
                catalog,
            );
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(degree_work)),
            }
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            ..
        } => estimate_optional_relationship_count_sum_cost(label, properties, legs, catalog),
        PhysicalPlan::ThreadRepairStatsExec { .. } => PlanCost {
            estimated_rows: 1,
            cost: 32,
        },
        PhysicalPlan::ShortestPathExec { max_hops, .. } => PlanCost {
            estimated_rows: 1,
            cost: (*max_hops as u64).saturating_mul(8).saturating_add(4),
        },
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let rows =
                estimate_aggregate_rows(group_keys, input, input_cost.estimated_rows, catalog);
            let work_rows =
                estimate_aggregate_work_rows(items, input, input_cost.estimated_rows, catalog);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(work_rows),
            }
        }
        PhysicalPlan::DistinctExec { input } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost.cost.saturating_add(input_cost.estimated_rows),
            }
        }
        PhysicalPlan::SortExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            PlanCost {
                estimated_rows: input_cost.estimated_rows,
                cost: input_cost
                    .cost
                    .saturating_add(input_cost.estimated_rows.saturating_mul(2)),
            }
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost(input, catalog);
            let remaining_rows = input_cost.estimated_rows.saturating_sub(*offset as u64);
            let rows = limit
                .map(|limit| remaining_rows.min(limit as u64))
                .unwrap_or(remaining_rows)
                .max(1);
            PlanCost {
                estimated_rows: rows,
                cost: input_cost.cost.saturating_add(rows),
            }
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => PlanCost {
            estimated_rows: 1,
            cost: 1,
        },
    }
}

fn estimate_physical_plan_cost_breakdown(
    plan: &PhysicalPlan,
    catalog: &OptimizerCatalog,
) -> PlanCostBreakdown {
    match plan {
        PhysicalPlan::SeqNodeScan { label, .. } => {
            let rows = catalog.label_count(label);
            PlanCostBreakdown::new(rows, 0, 0, rows.saturating_add(4), 0)
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            let left_cost = estimate_physical_plan_cost_breakdown(left, catalog);
            let right_cost = estimate_physical_plan_cost_breakdown(right, catalog);
            let rows = left_cost
                .estimated_rows
                .saturating_mul(right_cost.estimated_rows)
                .max(1);
            PlanCostBreakdown::combine_with_cpu(left_cost, right_cost, rows, rows, 0)
        }
        PhysicalPlan::NodeColumnLookupExec { label, input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let label_rows = catalog.label_count(label).max(1);
            input_cost.with_random_io(
                input_cost.estimated_rows.max(1),
                input_cost.estimated_rows.saturating_mul(label_rows),
                0,
            )
        }
        PhysicalPlan::IndexNodeSeek {
            label, property, ..
        } => {
            let rows = catalog
                .label_count(label)
                .div_ceil(catalog.distinct_count(label, property).max(1))
                .max(1);
            PlanCostBreakdown::new(rows, 0, rows.saturating_mul(2).saturating_add(1), 0, 0)
        }
        PhysicalPlan::IndexNodeMultiSeek {
            label,
            property,
            values,
            ..
        } => {
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = catalog.label_count(label).div_ceil(distinct_count).max(1);
            let rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(catalog.label_count(label))
                .max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                rows.saturating_mul(2).saturating_add(values.len() as u64),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeCompositeSeek {
            label, predicates, ..
        } => {
            let distinct_product = predicates
                .iter()
                .map(|(property, _)| catalog.distinct_count(label, property).max(1))
                .fold(1_u64, |acc, value| acc.saturating_mul(value))
                .max(1);
            let rows = catalog.label_count(label).div_ceil(distinct_product).max(1);
            PlanCostBreakdown::new(
                rows,
                0,
                rows.saturating_mul(2)
                    .saturating_add(predicates.len() as u64),
                0,
                0,
            )
        }
        PhysicalPlan::IndexNodeRangeSeek {
            label,
            property,
            lower,
            upper,
            ..
        } => {
            let rows =
                catalog.estimate_range_bounds_rows(label, property, lower.as_ref(), upper.as_ref());
            PlanCostBreakdown::new(rows, 0, rows.saturating_mul(2).saturating_add(2), 0, 0)
        }
        PhysicalPlan::IndexNodeTextSeek { label, .. } => {
            let rows = catalog.label_count(label).div_ceil(4).max(1);
            PlanCostBreakdown::new(rows, 0, rows.saturating_mul(2).saturating_add(3), 0, 0)
        }
        PhysicalPlan::AdjacencyExpandExec {
            source_label,
            rel_type,
            rel_properties,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let expand_estimate = catalog.estimate_expand_rows(
                source_label,
                rel_type,
                rel_properties,
                target_label,
                *min_hops,
                *max_hops,
            );
            let source_rows = catalog.label_count(source_label).max(1);
            let scaled_rows = expand_estimate
                .estimated_rows
                .saturating_mul(input_cost.estimated_rows.max(1))
                .div_ceil(source_rows)
                .max(1);
            input_cost.with_random_io(
                scaled_rows,
                input_cost.estimated_rows.saturating_add(scaled_rows),
                0,
            )
        }
        PhysicalPlan::FilterExec { predicate, input } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let rows = estimate_filter_rows(predicate, input, input_cost.estimated_rows, catalog);
            input_cost.with_cpu(rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::ProjectExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::OptionalDegreeExec {
            rel_type,
            rel_properties,
            direction,
            target_label,
            target_properties,
            input,
            ..
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let degree_work = estimate_optional_degree_work(
                rel_type,
                rel_properties,
                *direction,
                target_label,
                target_properties,
                catalog,
            );
            input_cost.with_random_io(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(degree_work),
                0,
            )
        }
        PhysicalPlan::OptionalRelationshipCountSumExec {
            label,
            properties,
            legs,
            ..
        } => {
            let scalar =
                estimate_optional_relationship_count_sum_cost(label, properties, legs, catalog);
            PlanCostBreakdown::from_scalar(scalar)
        }
        PhysicalPlan::ThreadRepairStatsExec { .. } => PlanCostBreakdown::new(1, 32, 0, 0, 0),
        PhysicalPlan::ShortestPathExec { max_hops, .. } => PlanCostBreakdown::new(
            1,
            0,
            (*max_hops as u64).saturating_mul(8).saturating_add(4),
            0,
            0,
        ),
        PhysicalPlan::AggregateExec {
            group_keys,
            items,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let rows =
                estimate_aggregate_rows(group_keys, input, input_cost.estimated_rows, catalog);
            let work_rows =
                estimate_aggregate_work_rows(items, input, input_cost.estimated_rows, catalog);
            input_cost.with_cpu(rows, work_rows, 0)
        }
        PhysicalPlan::DistinctExec { input } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(input_cost.estimated_rows, input_cost.estimated_rows, 0)
        }
        PhysicalPlan::SortExec { input, .. } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            input_cost.with_cpu(
                input_cost.estimated_rows,
                input_cost.estimated_rows.saturating_mul(2),
                0,
            )
        }
        PhysicalPlan::LimitExec {
            offset,
            limit,
            input,
        } => {
            let input_cost = estimate_physical_plan_cost_breakdown(input, catalog);
            let remaining_rows = input_cost.estimated_rows.saturating_sub(*offset as u64);
            let rows = limit
                .map(|limit| remaining_rows.min(limit as u64))
                .unwrap_or(remaining_rows)
                .max(1);
            input_cost.with_cpu(rows, rows, 0)
        }
        PhysicalPlan::CreateNodeLabel { .. }
        | PhysicalPlan::CreateRelationshipType { .. }
        | PhysicalPlan::CreateNodeTable { .. }
        | PhysicalPlan::CreateRelationshipTable { .. }
        | PhysicalPlan::CreateProperty { .. }
        | PhysicalPlan::AlterTableState { .. }
        | PhysicalPlan::AlterPropertyState { .. }
        | PhysicalPlan::CreateIndex { .. }
        | PhysicalPlan::CreateCompositeIndex { .. }
        | PhysicalPlan::CreateRangeIndex { .. }
        | PhysicalPlan::CreateFullTextIndex { .. }
        | PhysicalPlan::CreateUniqueConstraint { .. }
        | PhysicalPlan::CreateNodePropertyExistsConstraint { .. }
        | PhysicalPlan::CreateRelationshipUniqueConstraint { .. }
        | PhysicalPlan::CreateRelationshipPropertyExistsConstraint { .. }
        | PhysicalPlan::ProjectGraph { .. }
        | PhysicalPlan::GraphAlgorithm { .. }
        | PhysicalPlan::CreateNode { .. }
        | PhysicalPlan::MergeNode { .. }
        | PhysicalPlan::MergeRelationship { .. }
        | PhysicalPlan::MergeMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedRelationship { .. }
        | PhysicalPlan::MergeRelationshipToMatchedTarget { .. }
        | PhysicalPlan::MergeRelationshipFromMatchedTarget { .. }
        | PhysicalPlan::CreateMatchedRelationship { .. }
        | PhysicalPlan::SetNodeProperty { .. }
        | PhysicalPlan::SetNodeProperties { .. }
        | PhysicalPlan::SetNodePropertiesReturn { .. }
        | PhysicalPlan::SetRelationshipProperty { .. }
        | PhysicalPlan::SetRelationshipProperties { .. }
        | PhysicalPlan::DeleteNode { .. }
        | PhysicalPlan::DeleteRelationship { .. }
        | PhysicalPlan::DeleteRelationshipTargetNodes { .. }
        | PhysicalPlan::CreateRelationship { .. } => PlanCostBreakdown::new(1, 1, 0, 0, 0),
    }
}

fn estimate_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    estimate_relationship_filter_rows(predicate, input, input_rows, catalog)
        .or_else(|| estimate_node_property_filter_rows(predicate, input, input_rows, catalog))
        .unwrap_or_else(|| input_rows.div_ceil(2).max(1))
}

fn estimate_aggregate_rows(
    group_keys: &[Projection],
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    if group_keys.is_empty() {
        return 1;
    }
    let mut distinct_product = 1_u64;
    for group_key in group_keys {
        let ProjectionExpression::Property { variable, property } = &group_key.expression else {
            return input_rows.div_ceil(4).max(1);
        };
        let distinct_count = if let Some(label) = physical_plan_node_label(input, variable) {
            catalog.known_distinct_count(label, property)
        } else if let Some(rel_type) = physical_plan_relationship_type(input, variable) {
            catalog.known_rel_property_distinct_count(rel_type, property)
        } else {
            None
        };
        let Some(distinct_count) = distinct_count else {
            return input_rows.div_ceil(4).max(1);
        };
        distinct_product = distinct_product.saturating_mul(distinct_count.max(1));
    }
    input_rows.min(distinct_product).max(1)
}

fn estimate_aggregate_work_rows(
    items: &[Aggregation],
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> u64 {
    let distinct_property_work = items
        .iter()
        .filter_map(|item| {
            if !item.distinct {
                return None;
            }
            match &item.target {
                AggregateTarget::Variable(variable) => {
                    if let Some(distinct_count) =
                        physical_plan_node_variable_distinct_count(input, variable, catalog)
                    {
                        Some(distinct_count)
                    } else {
                        physical_plan_relationship_type(input, variable)
                            .map(|rel_type| catalog.relationship_count(rel_type))
                    }
                }
                AggregateTarget::Property { variable, property } => {
                    if let Some(label) = physical_plan_node_label(input, variable) {
                        catalog.known_distinct_count(label, property)
                    } else if let Some(rel_type) = physical_plan_relationship_type(input, variable)
                    {
                        catalog.known_rel_property_distinct_count(rel_type, property)
                    } else {
                        None
                    }
                }
                AggregateTarget::All => None,
            }
        })
        .fold(0_u64, |sum, distinct_count| {
            sum.saturating_add(input_rows.min(distinct_count.max(1)))
        });

    input_rows.saturating_add(distinct_property_work)
}

fn physical_plan_node_variable_distinct_count(
    plan: &PhysicalPlan,
    variable: &str,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match plan {
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            source_label,
            rel_type,
            target_variable,
            target_label,
            min_hops,
            max_hops,
            input,
            ..
        } => {
            if source_variable == variable {
                catalog
                    .bounded_path_source_distinct_count(
                        source_label,
                        rel_type,
                        target_label,
                        *min_hops,
                        *max_hops,
                    )
                    .or_else(|| {
                        catalog.path_source_distinct_count(source_label, rel_type, target_label)
                    })
                    .or_else(|| Some(catalog.label_count(source_label)))
            } else if target_variable == variable {
                catalog
                    .bounded_path_target_distinct_count(
                        source_label,
                        rel_type,
                        target_label,
                        *min_hops,
                        *max_hops,
                    )
                    .or_else(|| {
                        catalog.path_target_distinct_count(source_label, rel_type, target_label)
                    })
                    .or_else(|| Some(catalog.label_count(target_label)))
            } else {
                physical_plan_node_variable_distinct_count(input, variable, catalog)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            physical_plan_node_variable_distinct_count(left, variable, catalog)
                .or_else(|| physical_plan_node_variable_distinct_count(right, variable, catalog))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            physical_plan_node_variable_distinct_count(input, variable, catalog)
        }
        _ => physical_plan_node_label(plan, variable).map(|label| catalog.label_count(label)),
    }
}

fn estimate_id_in_rows(values: &[Value], input_rows: u64) -> u64 {
    values
        .iter()
        .collect::<BTreeSet<_>>()
        .len()
        .min(input_rows as usize) as u64
}

fn estimate_node_property_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match predicate {
        Predicate::And(predicates) => {
            let mut rows = input_rows;
            let mut matched = false;
            for predicate in predicates {
                if let Some(estimated) =
                    estimate_node_property_filter_rows(predicate, input, rows, catalog)
                {
                    rows = estimated;
                    matched = true;
                    if rows == 0 {
                        break;
                    }
                }
            }
            matched.then_some(rows)
        }
        Predicate::Or(predicates) => {
            let mut rows = 0_u64;
            for predicate in predicates {
                let estimated =
                    estimate_node_property_filter_rows(predicate, input, input_rows, catalog)?;
                rows = rows.saturating_add(estimated);
                if rows >= input_rows {
                    return Some(input_rows);
                }
            }
            Some(rows)
        }
        Predicate::ConstantBool(value) => Some(if *value { input_rows } else { 0 }),
        Predicate::IdEq { variable, .. } => {
            physical_plan_node_label(input, variable).map(|_| input_rows.min(1))
        }
        Predicate::IdNotEq { variable, .. } => {
            physical_plan_node_label(input, variable).map(|_| input_rows.saturating_sub(1))
        }
        Predicate::IdIn {
            variable, values, ..
        } => physical_plan_node_label(input, variable)
            .map(|_| estimate_id_in_rows(values, input_rows)),
        Predicate::PropertyEq {
            variable, property, ..
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                None
            } else {
                physical_plan_node_label(input, variable)
                    .map(|label| catalog.estimate_property_eq_rows(label, property, input_rows))
            }
        }
        Predicate::PropertyNotEq {
            variable, property, ..
        } => physical_plan_node_label(input, variable)
            .map(|label| catalog.estimate_property_not_eq_rows(label, property, input_rows)),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                None
            } else {
                physical_plan_node_label(input, variable).map(|label| {
                    catalog.estimate_property_range_rows(label, property, *op, value, input_rows)
                })
            }
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => physical_plan_node_label(input, variable)
            .map(|label| catalog.estimate_property_in_rows(label, property, values, input_rows)),
        Predicate::PropertyContains {
            variable, property, ..
        } => {
            if physical_plan_access_path_covers_property(input, variable, property) {
                physical_plan_node_label(input, variable).map(|_| input_rows)
            } else {
                physical_plan_node_label(input, variable).map(|label| {
                    catalog.estimate_property_string_match_rows(label, property, input_rows, 4)
                })
            }
        }
        Predicate::PropertyStartsWith {
            variable, property, ..
        } => physical_plan_node_label(input, variable).map(|label| {
            catalog.estimate_property_string_match_rows(label, property, input_rows, 8)
        }),
        Predicate::PropertyEndsWith {
            variable, property, ..
        } => physical_plan_node_label(input, variable).map(|label| {
            catalog.estimate_property_string_match_rows(label, property, input_rows, 6)
        }),
        Predicate::PropertyIsNull { variable, property } => {
            physical_plan_node_label(input, variable)
                .map(|label| catalog.estimate_property_null_rows(label, property, input_rows))
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            physical_plan_node_label(input, variable)
                .map(|label| catalog.estimate_property_not_null_rows(label, property, input_rows))
        }
        _ => None,
    }
}

fn estimate_optional_relationship_count_sum_cost(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    catalog: &OptimizerCatalog,
) -> PlanCost {
    estimate_optional_relationship_count_sum(label, properties, legs, catalog).cost
}

struct OptionalRelationshipCountSumEstimate {
    seed_rows: u64,
    leg_rows: Vec<OptionalRelationshipCountLegEstimate>,
    cost: PlanCost,
}

struct OptionalRelationshipCountLegEstimate {
    rel_type: String,
    direction: RelationshipDirection,
    rows: u64,
}

fn estimate_optional_relationship_count_sum(
    label: &str,
    properties: &BTreeMap<String, Value>,
    legs: &[RelationshipCountLeg],
    catalog: &OptimizerCatalog,
) -> OptionalRelationshipCountSumEstimate {
    let seed_rows = estimate_seed_rows_from_properties(label, properties, catalog);
    let leg_rows = legs
        .iter()
        .map(|leg| OptionalRelationshipCountLegEstimate {
            rel_type: leg.rel_type.clone(),
            direction: leg.direction,
            rows: estimate_relationship_count_leg_rows(seed_rows, leg, catalog),
        })
        .collect::<Vec<_>>();
    let relationship_rows = leg_rows
        .iter()
        .map(|leg| leg.rows)
        .fold(0_u64, |acc, rows| acc.saturating_add(rows));
    let cost = PlanCost {
        estimated_rows: 1,
        cost: seed_rows
            .saturating_add(relationship_rows)
            .saturating_add(legs.len() as u64)
            .saturating_add(4),
    };
    OptionalRelationshipCountSumEstimate {
        seed_rows,
        leg_rows,
        cost,
    }
}

fn format_relationship_direction(direction: RelationshipDirection) -> &'static str {
    match direction {
        RelationshipDirection::Outgoing => "out",
        RelationshipDirection::Incoming => "in",
        RelationshipDirection::Undirected => "both",
    }
}

fn estimate_seed_rows_from_properties(
    label: &str,
    properties: &BTreeMap<String, Value>,
    catalog: &OptimizerCatalog,
) -> u64 {
    let label_rows = catalog.label_count(label).max(1);
    let distinct_product = properties
        .keys()
        .map(|property| catalog.distinct_count(label, property).max(1))
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    label_rows.div_ceil(distinct_product).max(1)
}

fn estimate_relationship_count_leg_rows(
    seed_rows: u64,
    leg: &RelationshipCountLeg,
    catalog: &OptimizerCatalog,
) -> u64 {
    let rel_count = catalog
        .rel_type_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let source_count = catalog
        .rel_type_source_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(seed_rows)
        .max(1);
    let target_count = catalog
        .rel_type_target_counts
        .get(&leg.rel_type)
        .copied()
        .unwrap_or(seed_rows)
        .max(1);
    let fanout = match leg.direction {
        RelationshipDirection::Outgoing => rel_count.div_ceil(source_count).max(1),
        RelationshipDirection::Incoming => rel_count.div_ceil(target_count).max(1),
        RelationshipDirection::Undirected => rel_count
            .div_ceil(source_count)
            .saturating_add(rel_count.div_ceil(target_count))
            .max(1),
    };
    let mut rows = seed_rows.saturating_mul(fanout).max(1);
    if let Some(RelationshipCountFilter::PropertyNotEqOrEmpty { property, .. }) = &leg.filter {
        let distinct = catalog
            .rel_property_distinct_count(&leg.rel_type, property)
            .max(1);
        rows = rows.saturating_sub(rows.div_ceil(distinct)).max(1);
    }
    rows
}

fn estimate_optional_degree_work(
    rel_type: &str,
    rel_properties: &BTreeMap<String, Value>,
    direction: RelationshipDirection,
    target_label: &str,
    target_properties: &BTreeMap<String, Value>,
    catalog: &OptimizerCatalog,
) -> u64 {
    let rel_count = catalog
        .rel_type_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let source_count = catalog
        .rel_type_source_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let target_count = catalog
        .rel_type_target_counts
        .get(rel_type)
        .copied()
        .unwrap_or(1)
        .max(1);
    let fanout = match direction {
        RelationshipDirection::Outgoing => rel_count.div_ceil(source_count).max(1),
        RelationshipDirection::Incoming => rel_count.div_ceil(target_count).max(1),
        RelationshipDirection::Undirected => rel_count
            .div_ceil(source_count)
            .saturating_add(rel_count.div_ceil(target_count))
            .max(1),
    };
    let rel_property_distinct_product = rel_properties
        .keys()
        .map(|property| {
            catalog
                .rel_property_distinct_count(rel_type, property)
                .max(1)
        })
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    let target_property_distinct_product = target_properties
        .keys()
        .map(|property| catalog.distinct_count(target_label, property).max(1))
        .fold(1_u64, |acc, value| acc.saturating_mul(value))
        .max(1);
    fanout
        .div_ceil(rel_property_distinct_product.saturating_mul(target_property_distinct_product))
        .max(1)
        .saturating_add(1)
}

fn physical_plan_access_path_covers_property(
    plan: &PhysicalPlan,
    variable: &str,
    property: &str,
) -> bool {
    match plan {
        PhysicalPlan::IndexNodeSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            variable: plan_variable,
            property: plan_property,
            ..
        } => plan_variable == variable && plan_property == property,
        PhysicalPlan::IndexNodeCompositeSeek {
            variable: plan_variable,
            predicates,
            ..
        } => {
            plan_variable == variable
                && predicates
                    .iter()
                    .any(|(plan_property, _)| plan_property == property)
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            physical_plan_access_path_covers_property(left, variable, property)
                || physical_plan_access_path_covers_property(right, variable, property)
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::NodeColumnLookupExec { input, .. }
        | PhysicalPlan::AdjacencyExpandExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => {
            physical_plan_access_path_covers_property(input, variable, property)
        }
        _ => false,
    }
}

fn physical_plan_node_label<'a>(plan: &'a PhysicalPlan, variable: &str) -> Option<&'a str> {
    match plan {
        PhysicalPlan::SeqNodeScan {
            variable: plan_variable,
            label,
        }
        | PhysicalPlan::IndexNodeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeMultiSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeCompositeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeRangeSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::IndexNodeTextSeek {
            variable: plan_variable,
            label,
            ..
        }
        | PhysicalPlan::NodeColumnLookupExec {
            variable: plan_variable,
            label,
            ..
        } if plan_variable == variable => Some(label.as_str()),
        PhysicalPlan::AdjacencyExpandExec {
            source_variable,
            source_label,
            target_variable,
            target_label,
            input,
            ..
        } => {
            if source_variable == variable {
                Some(source_label.as_str())
            } else if target_variable == variable {
                Some(target_label.as_str())
            } else {
                physical_plan_node_label(input, variable)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            physical_plan_node_label(left, variable)
                .or_else(|| physical_plan_node_label(right, variable))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => physical_plan_node_label(input, variable),
        _ => None,
    }
}

fn physical_plan_relationship_type<'a>(plan: &'a PhysicalPlan, variable: &str) -> Option<&'a str> {
    match plan {
        PhysicalPlan::AdjacencyExpandExec {
            rel_variable: Some(rel_variable),
            rel_type,
            input,
            ..
        } => {
            if rel_variable == variable {
                Some(rel_type.as_str())
            } else {
                physical_plan_relationship_type(input, variable)
            }
        }
        PhysicalPlan::NodeCartesianProductExec { left, right } => {
            physical_plan_relationship_type(left, variable)
                .or_else(|| physical_plan_relationship_type(right, variable))
        }
        PhysicalPlan::FilterExec { input, .. }
        | PhysicalPlan::ProjectExec { input, .. }
        | PhysicalPlan::OptionalDegreeExec { input, .. }
        | PhysicalPlan::AggregateExec { input, .. }
        | PhysicalPlan::DistinctExec { input }
        | PhysicalPlan::SortExec { input, .. }
        | PhysicalPlan::LimitExec { input, .. } => physical_plan_relationship_type(input, variable),
        _ => None,
    }
}

fn estimate_relationship_filter_rows(
    predicate: &Predicate,
    input: &PhysicalPlan,
    input_rows: u64,
    catalog: &OptimizerCatalog,
) -> Option<u64> {
    match predicate {
        Predicate::And(predicates) => {
            let mut rows = input_rows;
            let mut matched = false;
            for predicate in predicates {
                if let Some(estimated) =
                    estimate_relationship_filter_rows(predicate, input, rows, catalog)
                {
                    rows = estimated;
                    matched = true;
                    if rows == 0 {
                        break;
                    }
                }
            }
            matched.then_some(rows)
        }
        Predicate::Or(predicates) => {
            let mut rows = 0_u64;
            for predicate in predicates {
                let estimated =
                    estimate_relationship_filter_rows(predicate, input, input_rows, catalog)?;
                rows = rows.saturating_add(estimated);
                if rows >= input_rows {
                    return Some(input_rows);
                }
            }
            Some(rows)
        }
        Predicate::ConstantBool(value) => Some(if *value { input_rows } else { 0 }),
        Predicate::IdEq { variable, .. } => {
            physical_plan_relationship_type(input, variable).map(|_| input_rows.min(1))
        }
        Predicate::IdNotEq { variable, .. } => {
            physical_plan_relationship_type(input, variable).map(|_| input_rows.saturating_sub(1))
        }
        Predicate::IdIn {
            variable, values, ..
        } => physical_plan_relationship_type(input, variable)
            .map(|_| estimate_id_in_rows(values, input_rows)),
        Predicate::PropertyEq {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_eq_rows(rel_type, property, rows)
            },
        ),
        Predicate::PropertyNotEq {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_not_eq_rows(rel_type, property, rows)
            },
        ),
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_range_rows(rel_type, property, *op, value, rows)
            },
        ),
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_in_rows(rel_type, property, values, rows)
            },
        ),
        Predicate::PropertyContains {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 4)
            },
        ),
        Predicate::PropertyStartsWith {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 8)
            },
        ),
        Predicate::PropertyEndsWith {
            variable, property, ..
        } => estimate_relationship_property_filter_rows(
            input,
            variable,
            property,
            input_rows,
            catalog,
            |catalog, rel_type, property, rows| {
                catalog.estimate_rel_property_string_match_rows(rel_type, property, rows, 6)
            },
        ),
        Predicate::PropertyIsNull { variable, property } => {
            estimate_relationship_property_filter_rows(
                input,
                variable,
                property,
                input_rows,
                catalog,
                |catalog, rel_type, property, rows| {
                    catalog.estimate_rel_property_null_rows(rel_type, property, rows)
                },
            )
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            estimate_relationship_property_filter_rows(
                input,
                variable,
                property,
                input_rows,
                catalog,
                |catalog, rel_type, property, rows| {
                    catalog.estimate_rel_property_not_null_rows(rel_type, property, rows)
                },
            )
        }
        _ => None,
    }
}

fn estimate_relationship_property_filter_rows(
    input: &PhysicalPlan,
    variable: &str,
    property: &str,
    input_rows: u64,
    catalog: &OptimizerCatalog,
    estimate: impl Fn(&OptimizerCatalog, &str, &str, u64) -> u64,
) -> Option<u64> {
    match input {
        PhysicalPlan::AdjacencyExpandExec {
            rel_variable: Some(rel_variable),
            rel_type,
            ..
        } if rel_variable == variable => Some(estimate(catalog, rel_type, property, input_rows)),
        _ => None,
    }
}

fn index_seek_from_filter(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    match (predicate, input) {
        (
            Predicate::And(predicates),
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) => index_seek_from_conjunction(
            predicates,
            predicate,
            scan_variable,
            label,
            catalog,
            decisions,
        ),
        (
            Predicate::PropertyEq {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) = equality_index_seek_from_rule(predicate, input, catalog, decisions)
            {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let estimated_rows = label_count.div_ceil(distinct_count).max(1);
            let scan_cost = label_count.saturating_add(4);
            let seek_cost = estimated_rows.saturating_mul(2).saturating_add(1);
            if seek_cost <= scan_cost {
                decisions.push(format!(
                    "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                Some(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    value: value.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
                ));
                None
            }
        }
        (
            Predicate::PropertyIn {
                variable,
                property,
                values,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) = in_index_seek_from_rule(predicate, input, catalog, decisions) {
                return Some(plan);
            }
            if !catalog.has_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no equality index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let distinct_count = catalog.distinct_count(label, property).max(1);
            let rows_per_value = label_count.div_ceil(distinct_count).max(1);
            let estimated_rows = rows_per_value
                .saturating_mul(values.len() as u64)
                .min(label_count)
                .max(1);
            let scan_cost = label_count.saturating_add(4);
            let seek_cost = estimated_rows
                .saturating_mul(2)
                .saturating_add(values.len() as u64);
            if seek_cost <= scan_cost {
                decisions.push(format!(
                    "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                Some(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    values: values.clone(),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                    values.len()
                ));
                None
            }
        }
        (
            Predicate::PropertyCompare {
                variable,
                property,
                op,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) = range_index_seek_from_rule(predicate, input, catalog, decisions) {
                return Some(plan);
            }
            if !catalog.has_range_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no range index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = catalog.estimate_range_rows(label, property, *op, value);
            let scan_cost = label_count.saturating_add(4);
            let seek_cost = estimated_rows.saturating_mul(2).saturating_add(2);
            if seek_cost <= scan_cost {
                let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
                decisions.push(format!(
                    "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::IndexNodeRangeSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    lower,
                    upper,
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        (
            Predicate::PropertyContains {
                variable,
                property,
                value,
            },
            LogicalPlan::NodeScan {
                variable: scan_variable,
                label,
            },
        ) if variable == scan_variable => {
            if let Some(plan) = text_index_seek_from_rule(predicate, input, catalog, decisions) {
                return Some(plan);
            }
            if !catalog.has_full_text_property_index(label, property) {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: no fulltext index descriptor"
                ));
                return None;
            }
            let label_count = catalog.label_count(label);
            let estimated_rows = label_count.div_ceil(4).max(1);
            let scan_cost = label_count.saturating_add(4);
            let seek_cost = estimated_rows.saturating_mul(2).saturating_add(3);
            if seek_cost <= scan_cost {
                decisions.push(format!(
                    "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                Some(PhysicalPlan::FilterExec {
                    predicate: predicate.clone(),
                    input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                        variable: variable.clone(),
                        label: label.clone(),
                        property: property.clone(),
                        query: value.clone(),
                    }),
                })
            } else {
                decisions.push(format!(
                    "choose SeqNodeScan for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
                ));
                None
            }
        }
        _ => None,
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeTextSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_text_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyContains {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = label_count.div_ceil(4).max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(3);
        if seek_cost <= scan_cost {
            RulePromise::new(85)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyContains {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_full_text_property_index(label, property)
        {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = label_count.div_ceil(4).max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(3);
        if seek_cost > scan_cost {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::FilterExec {
                predicate: predicate.as_ref().clone(),
                input: Box::new(PhysicalPlan::IndexNodeTextSeek {
                    variable: variable.clone(),
                    label: label.clone(),
                    property: property.clone(),
                    query: value.clone(),
                }),
            })),
            format!(
                "choose IndexNodeTextSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeCompositeSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_composite_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if composite_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .is_some()
        {
            RulePromise::new(105)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        composite_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .map(|(plan, decision)| {
                RuleApplication::new(GraphRuleExpr::Physical(Box::new(plan)), decision)
            })
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeConjunctionSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_conjunction_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if equality_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .is_some()
        {
            RulePromise::new(80)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::And(predicates) = predicate.as_ref() else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        equality_index_seek_candidate(predicates, predicate, scan_variable, label, self.catalog)
            .map(|(_, plan, decision)| {
                RuleApplication::new(GraphRuleExpr::Physical(Box::new(plan)), decision)
            })
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeRangeSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_range_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(2);
        if seek_cost <= scan_cost {
            RulePromise::new(90)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_range_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let estimated_rows = self
            .catalog
            .estimate_range_rows(label, property, *op, value);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(2);
        if seek_cost > scan_cost {
            return None;
        }
        let (lower, upper) = range_bounds_for_comparison(*op, value.clone());
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeRangeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                lower,
                upper,
            })),
            format!(
                "choose IndexNodeRangeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeInSeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_in_index_multi_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows
            .saturating_mul(2)
            .saturating_add(values.len() as u64);
        if seek_cost <= scan_cost {
            RulePromise::new(95)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows
            .saturating_mul(2)
            .saturating_add(values.len() as u64);
        if seek_cost > scan_cost {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeMultiSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                values: values.clone(),
            })),
            format!(
                "choose IndexNodeMultiSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            ),
        ))
    }
}

impl OptimizerRule<GraphRuleExpr> for NodeEqualitySeekRule<'_> {
    fn id(&self) -> RuleId {
        RuleId::new("node_equality_index_seek", RuleKind::Implementation)
    }

    fn promise(&self, expression: &GraphRuleExpr) -> RulePromise {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return RulePromise::NEVER;
        };
        let Predicate::PropertyEq {
            variable, property, ..
        } = predicate.as_ref()
        else {
            return RulePromise::NEVER;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return RulePromise::NEVER;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return RulePromise::NEVER;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(1);
        if seek_cost <= scan_cost {
            RulePromise::new(100)
        } else {
            RulePromise::NEVER
        }
    }

    fn apply(&self, expression: &GraphRuleExpr) -> Option<RuleApplication<GraphRuleExpr>> {
        let GraphRuleExpr::Filter { predicate, input } = expression else {
            return None;
        };
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate.as_ref()
        else {
            return None;
        };
        let LogicalPlan::NodeScan {
            variable: scan_variable,
            label,
        } = input.as_ref()
        else {
            return None;
        };
        if variable != scan_variable || !self.catalog.has_property_index(label, property) {
            return None;
        }
        let label_count = self.catalog.label_count(label);
        let distinct_count = self.catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(1);
        if seek_cost > scan_cost {
            return None;
        }
        Some(RuleApplication::new(
            GraphRuleExpr::Physical(Box::new(PhysicalPlan::IndexNodeSeek {
                variable: variable.clone(),
                label: label.clone(),
                property: property.clone(),
                value: value.clone(),
            })),
            format!(
                "choose IndexNodeSeek for {label}.{property}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            ),
        ))
    }
}

fn text_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeTextSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn composite_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeCompositeSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn conjunction_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeConjunctionSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn range_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeRangeSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn in_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeInSeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn equality_index_seek_from_rule(
    predicate: &Predicate,
    input: &LogicalPlan,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let expression = GraphRuleExpr::Filter {
        predicate: Box::new(predicate.clone()),
        input: Box::new(input.clone()),
    };
    let rule = NodeEqualitySeekRule { catalog };
    physical_plan_from_rule_batch(&expression, &[&rule], decisions)
}

fn physical_plan_from_rule_batch(
    expression: &GraphRuleExpr,
    rules: &[&dyn OptimizerRule<GraphRuleExpr>],
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let batch = apply_rule_batch(expression, rules);
    decisions.extend(
        batch
            .events()
            .iter()
            .cloned()
            .map(|event| event.into_decision()),
    );
    batch.into_parts().0.into_iter().find_map(|applied| {
        let application = applied.into_application();
        decisions.push(application.detail().to_string());
        match application.into_expression() {
            GraphRuleExpr::Physical(plan) => Some(*plan),
            GraphRuleExpr::Filter { .. } => None,
        }
    })
}

fn index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    if let Some(plan) = equality_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
    ) {
        return Some(plan);
    }
    range_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
    )
}

fn equality_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    if let Some(plan) = composite_index_seek_from_conjunction(
        predicates,
        full_predicate,
        scan_variable,
        label,
        catalog,
        decisions,
    ) {
        return Some(plan);
    }
    let logical_scan = LogicalPlan::NodeScan {
        variable: scan_variable.to_string(),
        label: label.to_string(),
    };
    if let Some(plan) =
        conjunction_index_seek_from_rule(full_predicate, &logical_scan, catalog, decisions)
    {
        return Some(plan);
    }
    if let Some((_, plan, decision)) =
        equality_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog)
    {
        decisions.push(decision);
        return Some(plan);
    }
    None
}

fn equality_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<(u64, PhysicalPlan, String)> {
    let label_count = catalog.label_count(label);
    let scan_cost = label_count.saturating_add(4);
    let mut best_candidate: Option<(u64, PhysicalPlan, String)> = None;
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let estimated_rows = label_count.div_ceil(distinct_count).max(1);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(1);
        if seek_cost <= scan_cost {
            let decision = format!(
                "choose IndexNodeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count}"
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    value: value.clone(),
                }),
            };
            if best_candidate
                .as_ref()
                .is_none_or(|(best_cost, _, _)| seek_cost < *best_cost)
            {
                best_candidate = Some((seek_cost, plan, decision));
            }
        }
    }
    for predicate in predicates {
        let Predicate::PropertyIn {
            variable,
            property,
            values,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_property_index(label, property) {
            continue;
        }
        let distinct_count = catalog.distinct_count(label, property).max(1);
        let rows_per_value = label_count.div_ceil(distinct_count).max(1);
        let estimated_rows = rows_per_value
            .saturating_mul(values.len() as u64)
            .min(label_count)
            .max(1);
        let seek_cost = estimated_rows
            .saturating_mul(2)
            .saturating_add(values.len() as u64);
        if seek_cost <= scan_cost {
            let decision = format!(
                "choose IndexNodeMultiSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_count={distinct_count} value_count={}",
                values.len()
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeMultiSeek {
                    variable: variable.clone(),
                    label: label.to_string(),
                    property: property.clone(),
                    values: values.clone(),
                }),
            };
            if best_candidate
                .as_ref()
                .is_none_or(|(best_cost, _, _)| seek_cost < *best_cost)
            {
                best_candidate = Some((seek_cost, plan, decision));
            }
        }
    }
    best_candidate
}

fn composite_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let logical_scan = LogicalPlan::NodeScan {
        variable: scan_variable.to_string(),
        label: label.to_string(),
    };
    if let Some(plan) =
        composite_index_seek_from_rule(full_predicate, &logical_scan, catalog, decisions)
    {
        return Some(plan);
    }
    if let Some((plan, decision)) =
        composite_index_seek_candidate(predicates, full_predicate, scan_variable, label, catalog)
    {
        decisions.push(decision);
        return Some(plan);
    }
    None
}

fn composite_index_seek_candidate(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> Option<(PhysicalPlan, String)> {
    let mut equality_values = BTreeMap::<String, Value>::new();
    for predicate in predicates {
        let Predicate::PropertyEq {
            variable,
            property,
            value,
        } = predicate
        else {
            continue;
        };
        if variable == scan_variable {
            equality_values.insert(property.clone(), value.clone());
        }
    }
    for properties in catalog.composite_property_indexes_for_label(label) {
        if properties.len() < 2 || !catalog.has_composite_property_index(label, &properties) {
            continue;
        }
        let mut seek_predicates = Vec::with_capacity(properties.len());
        for property in &properties {
            let Some(value) = equality_values.get(property) else {
                seek_predicates.clear();
                break;
            };
            seek_predicates.push((property.clone(), value.clone()));
        }
        if seek_predicates.is_empty() {
            continue;
        }
        let label_count = catalog.label_count(label);
        let distinct_product = properties
            .iter()
            .map(|property| catalog.distinct_count(label, property).max(1))
            .fold(1_u64, |acc, value| acc.saturating_mul(value))
            .max(1);
        let estimated_rows = label_count.div_ceil(distinct_product).max(1);
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows
            .saturating_mul(2)
            .saturating_add(properties.len() as u64);
        if seek_cost <= scan_cost {
            let decision = format!(
                "choose IndexNodeCompositeSeek for {label}.{:?}: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} distinct_product={distinct_product}",
                properties
            );
            let plan = PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeCompositeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    predicates: seek_predicates,
                }),
            };
            return Some((plan, decision));
        }
    }
    None
}

fn range_index_seek_from_conjunction(
    predicates: &[Predicate],
    full_predicate: &Predicate,
    scan_variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
    decisions: &mut Vec<String>,
) -> Option<PhysicalPlan> {
    let mut ranges = BTreeMap::<String, ValueRangeBounds>::new();
    for predicate in predicates {
        let Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } = predicate
        else {
            continue;
        };
        if variable != scan_variable || !catalog.has_range_property_index(label, property) {
            continue;
        }
        let (lower, upper) = ranges.entry(property.clone()).or_default();
        let (candidate_lower, candidate_upper) = range_bounds_for_comparison(*op, value.clone());
        merge_lower_bound(lower, candidate_lower);
        merge_upper_bound(upper, candidate_upper);
    }

    let mut best_plan = None;
    let mut best_seek_cost = u64::MAX;
    for (property, (lower, upper)) in ranges {
        let label_count = catalog.label_count(label);
        let estimated_rows =
            catalog.estimate_range_bounds_rows(label, &property, lower.as_ref(), upper.as_ref());
        let scan_cost = label_count.saturating_add(4);
        let seek_cost = estimated_rows.saturating_mul(2).saturating_add(2);
        if seek_cost <= scan_cost && seek_cost < best_seek_cost {
            best_seek_cost = seek_cost;
            decisions.push(format!(
                "choose IndexNodeRangeSeek for {label}.{property} in conjunction: seek_cost={seek_cost} scan_cost={scan_cost} label_count={label_count} estimated_rows={estimated_rows}"
            ));
            best_plan = Some(PhysicalPlan::FilterExec {
                predicate: full_predicate.clone(),
                input: Box::new(PhysicalPlan::IndexNodeRangeSeek {
                    variable: scan_variable.to_string(),
                    label: label.to_string(),
                    property,
                    lower,
                    upper,
                }),
            });
        }
    }
    best_plan
}

fn merge_lower_bound(current: &mut Option<ValueRangeBound>, candidate: Option<ValueRangeBound>) {
    let Some(candidate) = candidate else {
        return;
    };
    let Some(existing) = current else {
        *current = Some(candidate);
        return;
    };
    match comparable_value_ordering(&candidate.0, &existing.0) {
        Some(std::cmp::Ordering::Greater) => *existing = candidate,
        Some(std::cmp::Ordering::Equal) => existing.1 = existing.1 && candidate.1,
        Some(std::cmp::Ordering::Less) | None => {}
    }
}

fn merge_upper_bound(current: &mut Option<ValueRangeBound>, candidate: Option<ValueRangeBound>) {
    let Some(candidate) = candidate else {
        return;
    };
    let Some(existing) = current else {
        *current = Some(candidate);
        return;
    };
    match comparable_value_ordering(&candidate.0, &existing.0) {
        Some(std::cmp::Ordering::Less) => *existing = candidate,
        Some(std::cmp::Ordering::Equal) => existing.1 = existing.1 && candidate.1,
        Some(std::cmp::Ordering::Greater) | None => {}
    }
}

fn range_bounds_for_comparison(op: ComparisonOp, value: Value) -> ValueRangeBounds {
    match op {
        ComparisonOp::Lt => (None, Some((value, false))),
        ComparisonOp::Lte => (None, Some((value, true))),
        ComparisonOp::Gt => (Some((value, false)), None),
        ComparisonOp::Gte => (Some((value, true)), None),
    }
}

fn compare_histogram_value(candidate: &Value, op: ComparisonOp, value: &Value) -> bool {
    let Some(ordering) = comparable_value_ordering(candidate, value) else {
        return false;
    };
    match op {
        ComparisonOp::Lt => ordering == std::cmp::Ordering::Less,
        ComparisonOp::Lte => ordering != std::cmp::Ordering::Greater,
        ComparisonOp::Gt => ordering == std::cmp::Ordering::Greater,
        ComparisonOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn range_bound_matches(
    value: &Value,
    lower: Option<&ValueRangeBound>,
    upper: Option<&ValueRangeBound>,
) -> bool {
    if let Some((bound, inclusive)) = lower {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Less
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    if let Some((bound, inclusive)) = upper {
        let Some(ordering) = comparable_value_ordering(value, bound) else {
            return false;
        };
        if ordering == std::cmp::Ordering::Greater
            || (ordering == std::cmp::Ordering::Equal && !inclusive)
        {
            return false;
        }
    }
    true
}

fn comparable_value_ordering(left: &Value, right: &Value) -> Option<std::cmp::Ordering> {
    match (left, right) {
        (Value::Int(left), Value::Int(right)) => Some(left.cmp(right)),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right)),
        (Value::Int(left), Value::Float(right)) => Some((*left as f64).total_cmp(right)),
        (Value::Float(left), Value::Int(right)) => Some(left.total_cmp(&(*right as f64))),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn write_identifier(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}

fn schema_state_fingerprint(state: SchemaObjectState) -> &'static str {
    match state {
        SchemaObjectState::DeleteOnly => "delete_only",
        SchemaObjectState::WriteOnly => "write_only",
        SchemaObjectState::Backfill => "backfill",
        SchemaObjectState::Validating => "validating",
        SchemaObjectState::Public => "public",
        SchemaObjectState::Gc => "gc",
    }
}

fn write_identifier_list(output: &mut String, values: &[String]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, value);
    }
    output.push(']');
}

fn write_properties(output: &mut String, properties: &BTreeMap<String, Value>) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        write_value(output, value);
    }
    output.push('}');
}

fn write_relationship_on_create_properties(
    output: &mut String,
    properties: &BTreeMap<String, RelationshipOnCreateValue>,
) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        match value {
            RelationshipOnCreateValue::Value(value) => write_value(output, value),
            RelationshipOnCreateValue::MatchedRelationshipProperty { property } => {
                output.push_str("matched_rel.");
                write_identifier(output, property);
            }
        }
    }
    output.push('}');
}

fn write_value(output: &mut String, value: &Value) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "bool:true" } else { "bool:false" }),
        Value::Int(value) => {
            output.push_str("int:");
            output.push_str(&value.to_string());
        }
        Value::Float(value) => {
            output.push_str("float:");
            output.push_str(&value.to_bits().to_string());
        }
        Value::String(value) => {
            output.push_str("string:");
            write_identifier(output, value);
        }
        Value::List(values) => {
            output.push_str("list:[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push(']');
        }
        Value::Map(values) => {
            output.push_str("map:{");
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_identifier(output, key);
                output.push('=');
                write_value(output, value);
            }
            output.push('}');
        }
    }
}

fn write_set_value(output: &mut String, value: &SetValue) {
    match value {
        SetValue::Value(value) => write_value(output, value),
        SetValue::Coalesce { property, default } => {
            output.push_str("coalesce(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        SetValue::AddInt { property, amount } => {
            output.push_str("add_int(");
            write_identifier(output, property);
            output.push(',');
            output.push_str(&amount.to_string());
            output.push(')');
        }
        SetValue::DecrementFloorZero { property } => {
            output.push_str("dec_floor_zero(");
            write_identifier(output, property);
            output.push(')');
        }
        SetValue::PreserveNewerExisting {
            property,
            incoming,
            preserve,
        } => {
            output.push_str("preserve_newer(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, incoming);
            output.push(',');
            output.push_str(&preserve.to_string());
            output.push(')');
        }
    }
}

fn write_set_assignments(output: &mut String, assignments: &[SetAssignment]) {
    output.push('[');
    for (index, assignment) in assignments.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, &assignment.property);
        output.push('=');
        write_set_value(output, &assignment.value);
    }
    output.push(']');
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

fn write_optional_predicate(output: &mut String, predicate: Option<&Predicate>) {
    match predicate {
        Some(predicate) => write_predicate(output, predicate),
        None => output.push_str("none"),
    }
}

fn write_optional_range_bound(output: &mut String, bound: Option<&(Value, bool)>) {
    match bound {
        Some((value, inclusive)) => {
            output.push_str(if *inclusive {
                "inclusive:"
            } else {
                "exclusive:"
            });
            write_value(output, value);
        }
        None => output.push_str("none"),
    }
}

fn write_predicate(output: &mut String, predicate: &Predicate) {
    match predicate {
        Predicate::And(predicates) => {
            output.push_str("And(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Or(predicates) => {
            output.push_str("Or(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Not(predicate) => {
            output.push_str("Not(");
            write_predicate(output, predicate);
            output.push(')');
        }
        Predicate::ConstantBool(value) => {
            output.push_str(if *value { "True" } else { "False" });
        }
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => {
            output.push_str("RelationshipExists(");
            write_identifier(output, variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                crate::cypher::RelationshipDirection::Outgoing => "out",
                crate::cypher::RelationshipDirection::Incoming => "in",
                crate::cypher::RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_label);
            output.push(')');
        }
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => {
            output.push_str("BoundRelationshipExists(");
            write_identifier(output, source_variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                crate::cypher::RelationshipDirection::Outgoing => "out",
                crate::cypher::RelationshipDirection::Incoming => "in",
                crate::cypher::RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_variable);
            output.push(')');
        }
        Predicate::IdEq { variable, value } => {
            output.push_str("IdEq(id(");
            write_identifier(output, variable);
            output.push_str(")=");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdNotEq { variable, value } => {
            output.push_str("IdNotEq(id(");
            write_identifier(output, variable);
            output.push_str(")<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            output.push_str("IdCompare(id(");
            write_identifier(output, variable);
            output.push(')');
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdIn { variable, values } => {
            output.push_str("IdIn(id(");
            write_identifier(output, variable);
            output.push_str(") in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push('=');
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyNotEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str("<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            output.push_str("PropertyCompare(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::ExpressionEq { expression, value } => {
            output.push_str("ExpressionEq(");
            write_projection_expression(output, expression);
            output.push('=');
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionNotEq { expression, value } => {
            output.push_str("ExpressionNotEq(");
            write_projection_expression(output, expression);
            output.push_str("<>");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            output.push_str("ExpressionCompare(");
            write_projection_expression(output, expression);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionContains { expression, value } => {
            output.push_str("ExpressionContains(");
            write_projection_expression(output, expression);
            output.push_str(" contains ");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyListContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyStartsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" starts_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEndsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" ends_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => {
            output.push_str("PropertyRegexMatch(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" =~ ");
            write_identifier(output, pattern);
            output.push(')');
        }
        Predicate::PropertyIsNull { variable, property } => {
            output.push_str("PropertyIsNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            output.push_str("PropertyIsNotNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            output.push_str("PropertyIn(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
    }
}

fn write_projection_list(output: &mut String, items: &[Projection]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_projection(output, item);
    }
    output.push(']');
}

fn write_set_return_mode(output: &mut String, returns: &SetNodePropertiesReturnMode) {
    match returns {
        SetNodePropertiesReturnMode::Project(items) => write_projection_list(output, items),
        SetNodePropertiesReturnMode::Count { name } => {
            output.push_str("Count(");
            write_identifier(output, name);
            output.push(')');
        }
    }
}

fn write_relationship_count_leg(output: &mut String, leg: &RelationshipCountLeg) {
    output.push_str("RelationshipCountLeg(");
    match leg.direction {
        RelationshipDirection::Incoming => output.push_str("in,"),
        RelationshipDirection::Outgoing => output.push_str("out,"),
        RelationshipDirection::Undirected => output.push_str("both,"),
    }
    write_identifier(output, &leg.rel_type);
    output.push_str(",distinct=");
    output.push_str(if leg.distinct { "true" } else { "false" });
    if let Some(filter) = &leg.filter {
        output.push_str(",filter=");
        match filter {
            RelationshipCountFilter::PropertyNotEqOrEmpty { property, value } => {
                output.push_str("not_eq_or_empty(");
                write_identifier(output, property);
                output.push(',');
                write_value(output, value);
                output.push(')');
            }
        }
    }
    output.push(')');
}

fn write_projection(output: &mut String, item: &Projection) {
    output.push_str("Projection(");
    write_projection_expression(output, &item.expression);
    output.push_str(" as ");
    write_identifier(output, &item.name);
    output.push(')');
}

fn write_projection_expression(output: &mut String, expression: &ProjectionExpression) {
    match expression {
        ProjectionExpression::Variable { variable } => {
            write_identifier(output, variable);
        }
        ProjectionExpression::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
        ProjectionExpression::Id { variable } => {
            output.push_str("id(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::RelationshipType { variable } => {
            output.push_str("label(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::Literal(value) => {
            write_value(output, value);
        }
        ProjectionExpression::Coalesce(expressions) => {
            output.push_str("coalesce(");
            for (index, expression) in expressions.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_projection_expression(output, expression);
            }
            output.push(')');
        }
        ProjectionExpression::Left { expression, length } => {
            output.push_str("left(");
            write_projection_expression(output, expression);
            output.push(',');
            output.push_str(&length.to_string());
            output.push(')');
        }
        ProjectionExpression::Lower(expression) => {
            output.push_str("lower(");
            write_projection_expression(output, expression);
            output.push(')');
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            output.push_str("date_part(");
            output.push_str(match part {
                crate::planner::DatePart::Year => "year",
                crate::planner::DatePart::Month => "month",
            });
            output.push(',');
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            output.push_str("default_if_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            output.push_str("default_if_null(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("case_property_not_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            output.push_str("case_property_equals_rank(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            for (candidate, rank) in branches {
                output.push(',');
                write_value(output, candidate);
                output.push_str("=>");
                write_value(output, rank);
            }
            output.push_str(",default=>");
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            output.push_str("case_lower_property_default(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            output.push_str("case_coalesce_difference_floor_zero(");
            for (index, term) in terms.iter().enumerate() {
                if index > 0 {
                    output.push('-');
                }
                output.push_str("coalesce(");
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, &term.property);
                output.push(',');
                write_value(output, &term.default);
                output.push(')');
            }
            output.push(')');
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            output.push_str("case_entity_search_rank(");
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.name_property);
            output.push(',');
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.aliases_property);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.raw_input);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.alias_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            output.push_str("case_column_search_rank(");
            write_identifier(output, &expression.column);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.contains_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            output.push_str("column_default_if_null_or_eq(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            output.push_str("column_value_default_if_null(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("column_value_case_property_not_null_or_eq(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::Column(name) => {
            output.push_str("column(");
            write_identifier(output, name);
            output.push(')');
        }
        ProjectionExpression::ColumnProperty { column, property } => {
            output.push_str("column_property(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
    }
}

fn write_aggregation_list(output: &mut String, items: &[Aggregation]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_aggregation(output, item);
    }
    output.push(']');
}

fn write_aggregation(output: &mut String, item: &Aggregation) {
    output.push_str("Aggregation(");
    match item.function {
        AggregateFunction::Count => output.push_str("count"),
        AggregateFunction::Min => output.push_str("min"),
        AggregateFunction::Max => output.push_str("max"),
        AggregateFunction::Avg => output.push_str("avg"),
        AggregateFunction::Collect => output.push_str("collect"),
    }
    output.push('(');
    if item.distinct {
        output.push_str("distinct ");
    }
    match &item.target {
        AggregateTarget::All => output.push('*'),
        AggregateTarget::Variable(variable) => write_identifier(output, variable),
        AggregateTarget::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
    }
    output.push_str(") as ");
    write_identifier(output, &item.name);
    output.push(')');
}

fn write_sort_list(output: &mut String, items: &[SortItem]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str("Sort(");
        match &item.key {
            SortKey::Property { variable, property } => {
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, property);
            }
            SortKey::Id { variable } => {
                output.push_str("id(");
                write_identifier(output, variable);
                output.push(')');
            }
            SortKey::Expression(expression) => write_projection_expression(output, expression),
            SortKey::Column(column) => write_identifier(output, column),
        }
        output.push(' ');
        match item.direction {
            SortDirection::Asc => output.push_str("asc"),
            SortDirection::Desc => output.push_str("desc"),
        }
        output.push(')');
    }
    output.push(']');
}

#[cfg(test)]
mod tests {
    use super::{
        CascadesOptimizer, Distribution, OptimizerCatalog, OptimizerCatalogIndexes,
        OptimizerCatalogStatistics, OptimizerConfig, PhysicalPlan, PhysicalPlanChildren,
        PhysicalPlanClass, PhysicalPlanKind, PlanCost, RuleOutcome,
    };
    use crate::cypher::RelationshipDirection;
    use crate::planner::{
        AggregateFunction, AggregateTarget, Aggregation, LogicalPlan, Predicate, Projection,
        ProjectionExpression, RelationshipCountLeg, SortDirection, SortItem, SortKey,
    };
    use crate::value::Value;
    use std::collections::BTreeMap;

    #[test]
    fn physical_plan_metadata_describes_kind_class_and_children() {
        let plan = PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(1),
                },
                input: Box::new(PhysicalPlan::IndexNodeSeek {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(1),
                }),
            }),
        };

        assert_eq!(plan.kind(), PhysicalPlanKind::ProjectExec);
        assert_eq!(plan.kind().as_str(), "ProjectExec");
        assert_eq!(plan.class(), PhysicalPlanClass::Relational);
        assert_eq!(plan.class().as_str(), "relational");
        assert_eq!(plan.children().len(), 1);

        let PhysicalPlanChildren::Unary(filter) = plan.children() else {
            panic!("project should expose a unary child");
        };
        assert_eq!(filter.kind(), PhysicalPlanKind::FilterExec);
        assert_eq!(filter.class(), PhysicalPlanClass::Relational);

        let PhysicalPlanChildren::Unary(seek) = filter.children() else {
            panic!("filter should expose a unary child");
        };
        assert_eq!(seek.kind(), PhysicalPlanKind::IndexNodeSeek);
        assert_eq!(seek.class(), PhysicalPlanClass::Access);
        assert!(seek.children().is_empty());
    }

    #[test]
    fn physical_plan_metadata_describes_binary_children() {
        let plan = PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "s".to_string(),
                label: "Source".to_string(),
            }),
        };

        assert_eq!(plan.kind(), PhysicalPlanKind::NodeCartesianProductExec);
        assert_eq!(plan.class(), PhysicalPlanClass::Relational);

        let PhysicalPlanChildren::Binary(left, right) = plan.children() else {
            panic!("cartesian product should expose binary children");
        };
        assert_eq!(left.kind(), PhysicalPlanKind::SeqNodeScan);
        assert_eq!(right.kind(), PhysicalPlanKind::SeqNodeScan);
        assert_eq!(left.class(), PhysicalPlanClass::Access);
        assert_eq!(right.class(), PhysicalPlanClass::Access);
    }

    #[test]
    fn optimizer_trace_reports_physical_plan_operator_and_class_counts() {
        let logical = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            }],
            input: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::Int(1),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 100)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "id".to_string()), 100)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);
        assert_eq!(
            trace.selected_plan_operator_counts.get("ProjectExec"),
            Some(&1)
        );
        assert_eq!(
            trace.selected_plan_operator_counts.get("IndexNodeSeek"),
            Some(&1)
        );
        assert_eq!(trace.selected_plan_operator_counts.get("FilterExec"), None);
        assert_eq!(trace.selected_plan_class_counts.get("relational"), Some(&1));
        assert_eq!(trace.selected_plan_class_counts.get("access"), Some(&1));
        assert_eq!(
            trace.selected_plan_cost_breakdown.as_plan_cost(),
            trace.selected_plan_cost
        );
        assert_eq!(trace.selected_plan_cost_breakdown.cpu, 1);
        assert_eq!(trace.selected_plan_cost_breakdown.random_io, 3);
        assert_eq!(trace.selected_plan_cost_breakdown.sequential_io, 0);

        let (_, fallback_trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 1 })
            .optimize_with_catalog(&logical, &catalog);
        assert_eq!(
            fallback_trace
                .selected_plan_operator_counts
                .get("ProjectExec"),
            Some(&1)
        );
        assert_eq!(
            fallback_trace
                .selected_plan_operator_counts
                .get("IndexNodeSeek"),
            Some(&1)
        );
        assert_eq!(
            fallback_trace.selected_plan_class_counts.get("relational"),
            Some(&1)
        );
        assert_eq!(
            fallback_trace.selected_plan_class_counts.get("access"),
            Some(&1)
        );
        assert_eq!(
            fallback_trace.selected_plan_cost_breakdown.as_plan_cost(),
            fallback_trace.selected_plan_cost
        );
    }

    #[test]
    fn selected_plan_properties_report_distribution_and_sort_ordering() {
        let logical = LogicalPlan::Limit {
            offset: 0,
            limit: Some(10),
            input: Box::new(LogicalPlan::Project {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "title".to_string(),
                    },
                    name: "title".to_string(),
                }],
                input: Box::new(LogicalPlan::Sort {
                    items: vec![SortItem {
                        key: SortKey::Column("title".to_string()),
                        direction: SortDirection::Asc,
                    }],
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
            }),
        };

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &OptimizerCatalog::default());

        assert_eq!(
            trace.selected_plan_properties.distribution,
            Distribution::Single
        );
        assert_eq!(
            trace.selected_plan_properties.ordering,
            vec!["title asc".to_string()]
        );
    }

    #[test]
    fn optimizer_budget_uses_direct_fallback_with_trace_warning() {
        let logical = LogicalPlan::Limit {
            offset: 0,
            limit: Some(10),
            input: Box::new(LogicalPlan::Project {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "m".to_string(),
                        property: "title".to_string(),
                    },
                    name: "title".to_string(),
                }],
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::Int(1),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 100)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "id".to_string()), 100)],
                [],
            ),
        );

        let budgeted = CascadesOptimizer::new(OptimizerConfig { max_groups: 2 });
        let (_, budgeted_trace) = budgeted.optimize_with_catalog(&logical, &catalog);
        assert_eq!(budgeted_trace.groups, 4);
        assert!(budgeted_trace
            .warnings
            .iter()
            .any(|warning| warning.contains("optimizer memo budget exceeded")));
        assert!(budgeted_trace.selected_plan.contains("IndexNodeSeek"));
        assert!(budgeted_trace
            .decisions
            .iter()
            .any(|decision| decision.contains("choose IndexNodeSeek")));

        let full = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 });
        let (_, full_trace) = full.optimize_with_catalog(&logical, &catalog);
        assert!(full_trace.warnings.is_empty());
        assert_eq!(
            budgeted_trace.selected_plan_fingerprint,
            full_trace.selected_plan_fingerprint
        );
    }

    #[test]
    fn cartesian_product_trace_reports_input_rows_and_costs() {
        let logical = LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "m".to_string(),
                    property: "id".to_string(),
                    value: Value::String("memory-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new(
                [
                    ("Memory".to_string(), "id".to_string()),
                    ("Source".to_string(), "id".to_string()),
                ],
                [],
                [],
                [],
            ),
            OptimizerCatalogStatistics::new(
                [
                    ("Memory".to_string(), 10_000),
                    ("Source".to_string(), 1_000),
                ],
                [],
                [],
                [],
                [],
                [
                    (("Memory".to_string(), "id".to_string()), 10_000),
                    (("Source".to_string(), "id".to_string()), 1_000),
                ],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision.starts_with("keep NodeCartesianProduct single-row input order: inputs=2")
        }));
        assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=3 cost=7"
        }));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 7,
            }
        );
    }

    #[test]
    fn cartesian_product_orders_single_row_inputs_by_cost() {
        let logical = LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::Filter {
                predicate: Predicate::And(vec![
                    Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::String("memory-42".to_string()),
                    },
                    Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "kind".to_string(),
                        value: Value::String("note".to_string()),
                    },
                ]),
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "s".to_string(),
                    property: "id".to_string(),
                    value: Value::String("source-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "s".to_string(),
                    label: "Source".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new(
                [("Source".to_string(), "id".to_string())],
                [(
                    "Memory".to_string(),
                    vec!["id".to_string(), "kind".to_string()],
                )],
                [],
                [],
            ),
            OptimizerCatalogStatistics::new(
                [
                    ("Memory".to_string(), 10_000),
                    ("Source".to_string(), 1_000),
                ],
                [],
                [],
                [],
                [],
                [
                    (("Memory".to_string(), "id".to_string()), 10_000),
                    (("Memory".to_string(), "kind".to_string()), 2),
                    (("Source".to_string(), "id".to_string()), 1_000),
                ],
                [],
            ),
        );

        let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=2")
        }));
        assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=5 cost=9"
        }));
        assert!(plan
            .fingerprint()
            .contains("NodeCartesianProductExec(IndexNodeSeek(1:s:6:Source"));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 9,
            }
        );
    }

    #[test]
    fn cartesian_product_orders_nested_single_row_inputs() {
        let logical = LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeCartesianProduct {
                left: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::And(vec![
                        Predicate::PropertyEq {
                            variable: "m".to_string(),
                            property: "id".to_string(),
                            value: Value::String("memory-42".to_string()),
                        },
                        Predicate::PropertyEq {
                            variable: "m".to_string(),
                            property: "kind".to_string(),
                            value: Value::String("note".to_string()),
                        },
                    ]),
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
                right: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "s".to_string(),
                        property: "id".to_string(),
                        value: Value::String("source-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "s".to_string(),
                        label: "Source".to_string(),
                    }),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "e".to_string(),
                    property: "id".to_string(),
                    value: Value::String("entity-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "e".to_string(),
                    label: "Entity".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new(
                [
                    ("Entity".to_string(), "id".to_string()),
                    ("Source".to_string(), "id".to_string()),
                ],
                [(
                    "Memory".to_string(),
                    vec!["id".to_string(), "kind".to_string()],
                )],
                [],
                [],
            ),
            OptimizerCatalogStatistics::new(
                [
                    ("Entity".to_string(), 50_000),
                    ("Memory".to_string(), 10_000),
                    ("Source".to_string(), 1_000),
                ],
                [],
                [],
                [],
                [],
                [
                    (("Entity".to_string(), "id".to_string()), 50_000),
                    (("Memory".to_string(), "id".to_string()), 10_000),
                    (("Memory".to_string(), "kind".to_string()), 2),
                    (("Source".to_string(), "id".to_string()), 1_000),
                ],
                [],
            ),
        );

        let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision.starts_with("order NodeCartesianProduct single-row inputs: inputs=3")
        }));
        assert!(trace.decisions.iter().any(|decision| {
            decision == "estimate NodeCartesianProduct: left_rows=1 right_rows=1 output_rows=1 left_cost=3 right_cost=9 cost=13"
        }));
        let fingerprint = plan.fingerprint();
        assert!(fingerprint.contains("NodeCartesianProductExec(IndexNodeSeek(1:e:6:Entity"));
        assert!(fingerprint.contains("IndexNodeSeek(1:s:6:Source"));
        assert!(fingerprint.contains("FilterExec(And(PropertyEq(1:m.2:id=string:9:memory-42)"));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 13,
            }
        );
    }

    #[test]
    fn cartesian_product_keeps_nested_multi_row_inputs() {
        let logical = LogicalPlan::NodeCartesianProduct {
            left: Box::new(LogicalPlan::NodeCartesianProduct {
                left: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
                right: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "s".to_string(),
                        property: "id".to_string(),
                        value: Value::String("source-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "s".to_string(),
                        label: "Source".to_string(),
                    }),
                }),
            }),
            right: Box::new(LogicalPlan::Filter {
                predicate: Predicate::PropertyEq {
                    variable: "e".to_string(),
                    property: "id".to_string(),
                    value: Value::String("entity-42".to_string()),
                },
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "e".to_string(),
                    label: "Entity".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new(
                [
                    ("Entity".to_string(), "id".to_string()),
                    ("Source".to_string(), "id".to_string()),
                ],
                [],
                [],
                [],
            ),
            OptimizerCatalogStatistics::new(
                [
                    ("Entity".to_string(), 50_000),
                    ("Memory".to_string(), 10),
                    ("Source".to_string(), 1_000),
                ],
                [],
                [],
                [],
                [],
                [
                    (("Entity".to_string(), "id".to_string()), 50_000),
                    (("Source".to_string(), "id".to_string()), 1_000),
                ],
                [],
            ),
        );

        let (plan, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision == "keep NodeCartesianProduct input order: left_rows=10 right_rows=1 reason=non_single_row_input"
        }));
        assert!(plan.fingerprint().starts_with(
            "NodeCartesianProductExec(NodeCartesianProductExec(SeqNodeScan(1:m:6:Memory)"
        ));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 10,
                cost: 40,
            }
        );
    }

    #[test]
    fn post_product_node_property_filter_uses_node_statistics() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyEq {
                variable: "m".to_string(),
                property: "kind".to_string(),
                value: Value::String("note".to_string()),
            },
            input: Box::new(LogicalPlan::NodeCartesianProduct {
                left: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
                right: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "s".to_string(),
                        property: "id".to_string(),
                        value: Value::String("source-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "s".to_string(),
                        label: "Source".to_string(),
                    }),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([("Source".to_string(), "id".to_string())], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Source".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [
                    (("Memory".to_string(), "kind".to_string()), 10),
                    (("Source".to_string(), "id".to_string()), 1_000),
                ],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 100,
                cost: 3_007,
            }
        );
        assert!(trace.decisions.iter().any(
            |decision| decision == "selected physical plan cost: estimated_rows=100 cost=3007"
        ));
    }

    #[test]
    fn residual_node_property_in_uses_distinct_value_count() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyIn {
                variable: "m".to_string(),
                property: "id".to_string(),
                values: vec![Value::Int(1), Value::Int(2), Value::Int(2), Value::Int(3)],
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "id".to_string()), 100)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 30,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_node_property_in_empty_list_estimates_zero_rows() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyIn {
                variable: "m".to_string(),
                property: "id".to_string(),
                values: Vec::new(),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "id".to_string()), 100)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 0,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_node_string_predicate_uses_distinct_count_cap() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyContains {
                variable: "m".to_string(),
                property: "body".to_string(),
                value: "graph".to_string(),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "body".to_string()), 100)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 250,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_node_or_filter_uses_branch_selectivity() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::Or(vec![
                Predicate::ConstantBool(false),
                Predicate::PropertyEq {
                    variable: "t".to_string(),
                    property: "source".to_string(),
                    value: Value::String("slack".to_string()),
                },
            ]),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "t".to_string(),
                label: "Thread".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Thread".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Thread".to_string(), "source".to_string()), 10)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 100,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_node_null_predicate_uses_conservative_selectivity() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyIsNotNull {
                variable: "c".to_string(),
                property: "ai_summary".to_string(),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "c".to_string(),
                label: "Community".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Community".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Community".to_string(), "ai_summary".to_string()), 100)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 900,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_node_not_eq_filter_uses_distinct_counts() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyNotEq {
                variable: "m".to_string(),
                property: "space_id".to_string(),
                value: Value::String("default".to_string()),
            },
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "space_id".to_string()), 10)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 900,
                cost: 2_004,
            }
        );
    }

    #[test]
    fn residual_relationship_id_in_uses_literal_list_width() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::IdIn {
                variable: "r".to_string(),
                values: vec![Value::Int(1), Value::Int(2), Value::Int(2)],
            },
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
                [("MENTIONS".to_string(), 4_000)],
                [("MENTIONS".to_string(), 1_000)],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    4_000,
                )],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    4_000,
                )],
                [],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 2,
                cost: 10_004,
            }
        );
    }

    #[test]
    fn residual_relationship_property_in_uses_relationship_distinct_counts() {
        let logical = LogicalPlan::Filter {
            predicate: Predicate::PropertyIn {
                variable: "r".to_string(),
                property: "kind".to_string(),
                values: vec![
                    Value::String("mentioned".to_string()),
                    Value::String("quoted".to_string()),
                    Value::String("quoted".to_string()),
                    Value::String("linked".to_string()),
                ],
            },
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
                [("MENTIONS".to_string(), 4_000)],
                [("MENTIONS".to_string(), 1_000)],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    4_000,
                )],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    4_000,
                )],
                [],
                [],
            )
            .with_relationship_property_distinct_counts([(
                ("MENTIONS".to_string(), "kind".to_string()),
                10,
            )]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1_200,
                cost: 10_004,
            }
        );
    }

    #[test]
    fn aggregate_group_keys_use_node_property_distinct_counts() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![
                Projection {
                    expression: ProjectionExpression::Property {
                        variable: "e".to_string(),
                        property: "community_id".to_string(),
                    },
                    name: "community_id".to_string(),
                },
                Projection {
                    expression: ProjectionExpression::Property {
                        variable: "l".to_string(),
                        property: "name".to_string(),
                    },
                    name: "label_name".to_string(),
                },
            ],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("m".to_string()),
                distinct: true,
                name: "memory_count".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
                rel_type: "HAS_LABEL".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "l".to_string(),
                target_label: "Label".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Expand {
                    source_variable: "m".to_string(),
                    source_label: "Memory".to_string(),
                    rel_variable: None,
                    rel_type: "MENTIONS".to_string(),
                    rel_properties: BTreeMap::new(),
                    direction: RelationshipDirection::Outgoing,
                    target_variable: "e".to_string(),
                    target_label: "Entity".to_string(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: false,
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [
                    ("Memory".to_string(), 1_000),
                    ("Entity".to_string(), 500),
                    ("Label".to_string(), 100),
                ],
                [
                    ("MENTIONS".to_string(), 2_000),
                    ("HAS_LABEL".to_string(), 1_000),
                ],
                [
                    ("MENTIONS".to_string(), 1_000),
                    ("HAS_LABEL".to_string(), 1_000),
                ],
                [
                    (
                        (
                            "Memory".to_string(),
                            "MENTIONS".to_string(),
                            "Entity".to_string(),
                        ),
                        1_000,
                    ),
                    (
                        (
                            "Memory".to_string(),
                            "HAS_LABEL".to_string(),
                            "Label".to_string(),
                        ),
                        1_000,
                    ),
                ],
                [
                    (
                        (
                            "Memory".to_string(),
                            "MENTIONS".to_string(),
                            "Entity".to_string(),
                            1,
                        ),
                        1_000,
                    ),
                    (
                        (
                            "Memory".to_string(),
                            "HAS_LABEL".to_string(),
                            "Label".to_string(),
                            1,
                        ),
                        1_000,
                    ),
                ],
                [
                    (("Entity".to_string(), "community_id".to_string()), 4),
                    (("Label".to_string(), "name".to_string()), 5),
                ],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 32 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 20,
                cost: 7_004,
            }
        );
    }

    #[test]
    fn aggregate_group_keys_use_relationship_property_distinct_counts() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "weight".to_string(),
                },
                name: "weight".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("e".to_string()),
                distinct: true,
                name: "entity_count".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Entity".to_string(), 1_000)],
                [("MENTIONS".to_string(), 1_000)],
                [("MENTIONS".to_string(), 1_000)],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    1_000,
                )],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    1_000,
                )],
                [],
                [],
            )
            .with_relationship_property_distinct_counts([(
                ("MENTIONS".to_string(), "weight".to_string()),
                10,
            )]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 10,
                cost: 5_004,
            }
        );
    }

    #[test]
    fn aggregate_distinct_property_targets_add_bounded_work_cost() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "unit_type".to_string(),
                },
                name: "unit_type".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property {
                    variable: "m".to_string(),
                    property: "community_id".to_string(),
                },
                distinct: true,
                name: "community_span".to_string(),
            }],
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [
                    (("Memory".to_string(), "unit_type".to_string()), 5),
                    (("Memory".to_string(), "community_id".to_string()), 10),
                ],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 5,
                cost: 2_014,
            }
        );
    }

    #[test]
    fn aggregate_distinct_variable_targets_add_bounded_work_cost() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "unit_type".to_string(),
                },
                name: "unit_type".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("m".to_string()),
                distinct: true,
                name: "memory_count".to_string(),
            }],
            input: Box::new(LogicalPlan::NodeScan {
                variable: "m".to_string(),
                label: "Memory".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000)],
                [],
                [],
                [],
                [],
                [(("Memory".to_string(), "unit_type".to_string()), 5)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 8 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 5,
                cost: 3_004,
            }
        );
    }

    #[test]
    fn aggregate_distinct_variable_targets_use_path_target_coverage() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "unit_type".to_string(),
                },
                name: "unit_type".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("e".to_string()),
                distinct: true,
                name: "entity_count".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
                [("MENTIONS".to_string(), 1_000)],
                [("MENTIONS".to_string(), 1_000)],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    1_000,
                )],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                        1,
                    ),
                    1_000,
                )],
                [(("Memory".to_string(), "unit_type".to_string()), 5)],
                [],
            )
            .with_path_target_distinct_counts([(
                (
                    "Memory".to_string(),
                    "MENTIONS".to_string(),
                    "Entity".to_string(),
                ),
                12,
            )]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 5,
                cost: 4_016,
            }
        );
    }

    #[test]
    fn aggregate_distinct_variable_targets_use_bounded_path_target_coverage() {
        let logical = LogicalPlan::Aggregate {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "unit_type".to_string(),
                },
                name: "unit_type".to_string(),
            }],
            items: vec![Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Variable("e".to_string()),
                distinct: true,
                name: "two_hop_entities".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
                rel_type: "RELATES_TO".to_string(),
                rel_properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 2,
                max_hops: 2,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1_000), ("Entity".to_string(), 5_000)],
                [("RELATES_TO".to_string(), 10_000)],
                [("RELATES_TO".to_string(), 1_000)],
                [(
                    (
                        "Memory".to_string(),
                        "RELATES_TO".to_string(),
                        "Entity".to_string(),
                    ),
                    2_000,
                )],
                [(
                    (
                        "Memory".to_string(),
                        "RELATES_TO".to_string(),
                        "Entity".to_string(),
                        2,
                    ),
                    1_500,
                )],
                [(("Memory".to_string(), "unit_type".to_string()), 5)],
                [],
            )
            .with_bounded_path_target_distinct_counts([(
                (
                    "Memory".to_string(),
                    "RELATES_TO".to_string(),
                    "Entity".to_string(),
                    2,
                ),
                40,
            )]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 5,
                cost: 5_044,
            }
        );
    }

    #[test]
    fn optional_relationship_count_sum_uses_seed_and_relationship_statistics() {
        let logical = LogicalPlan::OptionalRelationshipCountSum {
            variable: "t".to_string(),
            label: "Thread".to_string(),
            properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("thread-42".to_string()),
            )]),
            legs: vec![RelationshipCountLeg {
                rel_type: "CONTAINS".to_string(),
                direction: RelationshipDirection::Outgoing,
                distinct: false,
                filter: None,
            }],
            output: "message_count".to_string(),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [
                    ("Thread".to_string(), 1_000),
                    ("Message".to_string(), 50_000),
                ],
                [("CONTAINS".to_string(), 5_000)],
                [("CONTAINS".to_string(), 1_000)],
                [],
                [],
                [(("Thread".to_string(), "id".to_string()), 1_000)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 11,
            }
        );
        assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Thread: seed_rows=1 leg_rows=[CONTAINS:out:5] estimated_rows=1 cost=11",
            )
        }));
    }

    #[test]
    fn incoming_optional_relationship_count_sum_uses_target_statistics() {
        let logical = LogicalPlan::OptionalRelationshipCountSum {
            variable: "e".to_string(),
            label: "Entity".to_string(),
            properties: BTreeMap::from([(
                "id".to_string(),
                Value::String("entity-42".to_string()),
            )]),
            legs: vec![RelationshipCountLeg {
                rel_type: "MENTIONS".to_string(),
                direction: RelationshipDirection::Incoming,
                distinct: false,
                filter: None,
            }],
            output: "mention_count".to_string(),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [
                    ("Memory".to_string(), 10_000),
                    ("Entity".to_string(), 1_000),
                ],
                [("MENTIONS".to_string(), 5_000)],
                [("MENTIONS".to_string(), 5_000)],
                [],
                [],
                [(("Entity".to_string(), "id".to_string()), 1_000)],
                [],
            )
            .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 16,
            }
        );
        assert!(trace.decisions.iter().any(|decision| {
            decision.contains(
                "estimate OptionalRelationshipCountSum for Entity: seed_rows=1 leg_rows=[MENTIONS:in:10] estimated_rows=1 cost=16",
            )
        }));
    }

    #[test]
    fn incoming_optional_degree_cost_uses_target_statistics() {
        let logical = LogicalPlan::OptionalDegree {
            source_variable: "e".to_string(),
            rel_type: "MENTIONS".to_string(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Incoming,
            target_label: "Memory".to_string(),
            target_properties: BTreeMap::new(),
            alias: "mention_count".to_string(),
            input: Box::new(LogicalPlan::NodeScan {
                variable: "e".to_string(),
                label: "Entity".to_string(),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [
                    ("Memory".to_string(), 10_000),
                    ("Entity".to_string(), 1_000),
                ],
                [("MENTIONS".to_string(), 5_000)],
                [("MENTIONS".to_string(), 5_000)],
                [],
                [],
                [],
                [],
            )
            .with_relationship_type_target_counts([("MENTIONS".to_string(), 500)]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1_000,
                cost: 12_004,
            }
        );
    }

    #[test]
    fn expand_trace_marks_fallback_hop_estimates() {
        let logical = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "name".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
                rel_type: "LINKS".to_string(),
                rel_properties: Default::default(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 3,
                optional: false,
                input: Box::new(LogicalPlan::NodeScan {
                    variable: "m".to_string(),
                    label: "Memory".to_string(),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 10), ("Entity".to_string(), 100)],
                [("LINKS".to_string(), 20)],
                [("LINKS".to_string(), 10)],
                [(
                    (
                        "Memory".to_string(),
                        "LINKS".to_string(),
                        "Entity".to_string(),
                    ),
                    4,
                )],
                [],
                [],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision.contains("estimate AdjacencyExpand")
                && decision.contains("hop_rows=[1:fallback:4,2:fallback:8,3:fallback:16]")
                && decision.contains("estimated_rows=28")
        }));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 28,
                cost: 80,
            }
        );
        assert!(trace
            .decisions
            .iter()
            .any(|decision| decision == "selected physical plan cost: estimated_rows=28 cost=80"));
    }

    #[test]
    fn expand_cost_scales_with_selective_input_rows() {
        let logical = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "e".to_string(),
                    property: "name".to_string(),
                },
                name: "name".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: None,
                rel_type: "LINKS".to_string(),
                rel_properties: Default::default(),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::Int(7),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
                [("LINKS".to_string(), 1000)],
                [("LINKS".to_string(), 1000)],
                [(
                    (
                        "Memory".to_string(),
                        "LINKS".to_string(),
                        "Entity".to_string(),
                    ),
                    1000,
                )],
                [],
                [(("Memory".to_string(), "id".to_string()), 1000)],
                [],
            ),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 6,
            }
        );
        assert!(trace
            .decisions
            .iter()
            .any(|decision| decision.contains("choose IndexNodeSeek for Memory.id")));
        assert!(trace.decisions.iter().any(|decision| {
            decision.starts_with("apply implementation:node_equality_index_seek:")
        }));
        assert!(trace.rule_events.iter().any(|event| {
            event.rule() == "implementation:node_equality_index_seek"
                && event.outcome() == RuleOutcome::Applied
        }));
        assert!(trace
            .decisions
            .iter()
            .any(|decision| decision == "selected physical plan cost: estimated_rows=1 cost=6"));
    }

    #[test]
    fn expand_cost_uses_relationship_property_distinct_counts() {
        let logical = LogicalPlan::Project {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "r".to_string(),
                    property: "weight".to_string(),
                },
                name: "weight".to_string(),
            }],
            input: Box::new(LogicalPlan::Expand {
                source_variable: "m".to_string(),
                source_label: "Memory".to_string(),
                rel_variable: Some("r".to_string()),
                rel_type: "MENTIONS".to_string(),
                rel_properties: BTreeMap::from([("weight".to_string(), Value::Int(4))]),
                direction: RelationshipDirection::Outgoing,
                target_variable: "e".to_string(),
                target_label: "Entity".to_string(),
                min_hops: 1,
                max_hops: 1,
                optional: false,
                input: Box::new(LogicalPlan::Filter {
                    predicate: Predicate::PropertyEq {
                        variable: "m".to_string(),
                        property: "id".to_string(),
                        value: Value::String("memory-42".to_string()),
                    },
                    input: Box::new(LogicalPlan::NodeScan {
                        variable: "m".to_string(),
                        label: "Memory".to_string(),
                    }),
                }),
            }),
        };
        let catalog = OptimizerCatalog::new(
            OptimizerCatalogIndexes::new([("Memory".to_string(), "id".to_string())], [], [], []),
            OptimizerCatalogStatistics::new(
                [("Memory".to_string(), 1000), ("Entity".to_string(), 1000)],
                [("MENTIONS".to_string(), 1000)],
                [("MENTIONS".to_string(), 1000)],
                [(
                    (
                        "Memory".to_string(),
                        "MENTIONS".to_string(),
                        "Entity".to_string(),
                    ),
                    1000,
                )],
                [],
                [(("Memory".to_string(), "id".to_string()), 1000)],
                [],
            )
            .with_relationship_property_distinct_counts([(
                ("MENTIONS".to_string(), "weight".to_string()),
                10,
            )]),
        );

        let (_, trace) = CascadesOptimizer::new(OptimizerConfig { max_groups: 16 })
            .optimize_with_catalog(&logical, &catalog);

        assert!(trace.decisions.iter().any(|decision| {
            decision.contains("estimate AdjacencyExpand")
                && decision.contains("rel_property_distinct_product=10")
                && decision.contains("estimated_rows=100")
        }));
        assert_eq!(
            trace.selected_plan_cost,
            PlanCost {
                estimated_rows: 1,
                cost: 6,
            }
        );
    }
}
