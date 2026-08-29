use crate::{
    Aggregation, GraphAlgorithmKind, GraphAlgorithmOptions, Predicate, Projection,
    RelationshipCountLeg, RelationshipOnCreateValue, RelationshipSetAssignment, SchemaObjectState,
    SchemaPropertyType, SchemaTableKind, SetAssignment, SetNodePropertiesReturnMode, SetValue,
    ShortestPathProjection, SortItem,
};
use skein_core::Value;
use skein_cypher::RelationshipDirection;
use std::collections::BTreeMap;

mod domain;
mod explain;
mod fingerprint;
mod metadata;
mod plan_node;

pub use fingerprint::write_projection_expression;
pub use metadata::{
    plan_class_counts, plan_operator_counts, visit_plan, visit_plan_with_ids, PhysicalOperatorId,
    PhysicalPlanClass, PhysicalPlanKind, PhysicalPlanNode, PlanChildren,
};
pub use plan_node::PhysicalPlanChildren;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphExpansionBudget {
    pub candidate_limit: usize,
    pub payload_byte_limit: usize,
}

/// One equality-index branch in a bounded exact-property union seek.
#[derive(Debug, Clone, PartialEq)]
pub struct ExactPropertySeekBranch {
    pub property: String,
    pub values: Vec<Value>,
}

/// Leading equalities followed by one bounded range on an ordered composite index.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeRangeSeek {
    pub index_properties: Vec<String>,
    pub equality_prefix: Vec<(String, Value)>,
    pub range_property: String,
    pub lower: Option<(Value, bool)>,
    pub upper: Option<(Value, bool)>,
}

/// Candidate source used by a fused scalar node projection.
///
/// Keeping the access path beside the required-property set lets storage
/// decode only fields needed by the residual predicate and output projection,
/// without giving up an index seek selected earlier by the optimizer.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeProjectionAccess {
    LabelScan,
    PropertyValues {
        property: String,
        values: Vec<Value>,
    },
    PropertyUnion {
        branches: Vec<ExactPropertySeekBranch>,
    },
    CompositeEquality {
        predicates: Vec<(String, Value)>,
    },
    CompositeRange {
        seek: CompositeRangeSeek,
    },
    PropertyRange {
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    FullText {
        property: String,
        query: String,
    },
}

impl NodeProjectionAccess {
    pub fn is_label_scan(&self) -> bool {
        matches!(self, Self::LabelScan)
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::LabelScan => "label_scan",
            Self::PropertyValues { .. } => "property_values",
            Self::PropertyUnion { .. } => "property_union",
            Self::CompositeEquality { .. } => "composite_equality",
            Self::CompositeRange { .. } => "composite_range",
            Self::PropertyRange { .. } => "property_range",
            Self::FullText { .. } => "full_text",
        }
    }

    /// Returns the access operator represented inside the fused projection.
    pub fn physical_operator_name(&self) -> &'static str {
        match self {
            Self::LabelScan => "SeqNodeScan",
            Self::PropertyValues { values, .. } if values.len() == 1 => "IndexNodeSeek",
            Self::PropertyValues { .. } => "IndexNodeMultiSeek",
            Self::PropertyUnion { .. } => "IndexNodeUnionSeek",
            Self::CompositeEquality { .. } => "IndexNodeCompositeSeek",
            Self::CompositeRange { .. } => "IndexNodeCompositeRangeSeek",
            Self::PropertyRange { .. } => "IndexNodeRangeSeek",
            Self::FullText { .. } => "IndexNodeTextSeek",
        }
    }
}

/// Compatibility plan exchanged by the public planner and executor facades.
///
/// New storage-independent analysis should target the decomposed internal plan
/// representation instead of adding more cross-domain behavior to this enum.
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
        node_visibility_predicate: Option<Predicate>,
    },
    VectorSeedScan {
        embedding_parameter: String,
        output_external_id: bool,
        metadata_filters: BTreeMap<String, String>,
        vector_plan: crate::VectorPhysicalPlan,
        resource_profile: crate::VectorExecutionResourceProfile,
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
    EmptyExec,
    SeqNodeScan {
        variable: String,
        label: String,
    },
    /// Fused required-property node scan, local predicate, and optional scalar projection.
    ///
    /// Storage decodes only `required_properties`. A non-empty `items` list
    /// emits scalar values directly; an empty list preserves the projected
    /// node binding for a downstream aggregate that was proven to require no
    /// other properties.
    NodeProjectionScanExec {
        variable: String,
        label: String,
        access: NodeProjectionAccess,
        required_properties: Vec<String>,
        predicate: Option<Predicate>,
        items: Vec<Projection>,
    },
    /// A checkpoint-published Source sidecar candidate scan. The enclosing
    /// FilterExec retains the original predicate as the semantic authority.
    SourceSegmentScan {
        variable: String,
        predicate: Predicate,
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
    IndexNodeUnionSeek {
        variable: String,
        label: String,
        branches: Vec<ExactPropertySeekBranch>,
    },
    IndexNodeCompositeSeek {
        variable: String,
        label: String,
        predicates: Vec<(String, Value)>,
    },
    IndexNodeCompositeRangeSeek {
        variable: String,
        label: String,
        seek: CompositeRangeSeek,
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
        graph_budget: Option<GraphExpansionBudget>,
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
    NodeCountExec {
        label: String,
        output: String,
    },
    RelationshipCountExec {
        rel_type: String,
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
        source_visibility_predicate: Option<Predicate>,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        target_id: Value,
        target_visibility_predicate: Option<Predicate>,
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
    TopNExec {
        items: Vec<SortItem>,
        offset: usize,
        limit: usize,
        input: Box<PhysicalPlan>,
    },
    LimitExec {
        offset: usize,
        limit: Option<usize>,
        input: Box<PhysicalPlan>,
    },
}

pub use domain::{
    AccessPhysicalPlanRef, MutationPhysicalPlanRef, PhysicalOperatorDomain, PhysicalPlanDomainRef,
    ProcedurePhysicalPlanRef, RelationalPhysicalPlanRef, SchemaPhysicalPlanRef,
    TraversalPhysicalPlanRef,
};
