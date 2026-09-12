use skein_core::{PropertyType, SchemaObjectState, TableKind, ValidatedRegex, Value};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

mod compact;
#[doc(hidden)]
pub mod evaluate;

#[doc(hidden)]
pub use compact::compact_transaction_graph_ops;

pub const DEFAULT_MAX_MUTATION_AFFECTED_ROWS: usize = 100_000;
pub const DEFAULT_MAX_MUTATION_OPERATIONS: usize = 100_000;
pub const DEFAULT_MAX_MUTATION_RESULT_ROWS: usize = 100_000;
pub const DEFAULT_MAX_MUTATION_RESULT_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Hard limits applied before a mutation is appended to the WAL.
///
/// Mutation execution is atomic: exceeding a limit must reject the complete
/// mutation before durable or in-memory state changes become visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationLimits {
    pub max_affected_rows: NonZeroUsize,
    pub max_operations: NonZeroUsize,
    pub max_result_rows: NonZeroUsize,
    pub max_result_payload_bytes: NonZeroUsize,
}

/// Storage-neutral outcome of an atomically committed graph mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationSummary {
    pub rows: Vec<BTreeMap<String, Value>>,
    pub relational_mutation_outcomes: Vec<crate::RelationalMutationOutcome>,
    pub append_mutation_outcomes: Vec<crate::AppendMutationOutcome>,
}

impl Default for MutationLimits {
    fn default() -> Self {
        Self {
            max_affected_rows: NonZeroUsize::new(DEFAULT_MAX_MUTATION_AFFECTED_ROWS)
                .expect("default mutation affected-row limit is non-zero"),
            max_operations: NonZeroUsize::new(DEFAULT_MAX_MUTATION_OPERATIONS)
                .expect("default mutation operation limit is non-zero"),
            max_result_rows: NonZeroUsize::new(DEFAULT_MAX_MUTATION_RESULT_ROWS)
                .expect("default mutation result-row limit is non-zero"),
            max_result_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_MUTATION_RESULT_PAYLOAD_BYTES)
                .expect("default mutation result payload limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedNodesCreate {
    pub source_label: String,
    pub source_properties: BTreeMap<String, Value>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCreate {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipOnCreatePropertyValue {
    Value(Value),
    MatchedRelationshipProperty { property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipCopyMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, RelationshipOnCreatePropertyValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipRetargetMerge {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_target_label: String,
    pub new_target_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchedRelationshipSourceRetargetMerge {
    pub old_source_label: String,
    pub old_source_filter: Option<PropertyFilter>,
    pub old_rel_type: String,
    pub old_rel_filter: BTreeMap<String, Value>,
    pub old_target_label: String,
    pub old_target_filter: Option<PropertyFilter>,
    pub new_source_label: String,
    pub new_source_filter: Option<PropertyFilter>,
    pub new_rel_type: String,
    pub new_rel_match_properties: BTreeMap<String, Value>,
    pub on_create_properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertyUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipSetAssignment {
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPropertiesUpdate {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
    pub assignments: Vec<RelationshipSetAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDeleteRequest {
    pub source_label: String,
    pub filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub rel_filter: Option<PropertyFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipTargetNodeDelete {
    pub source_label: String,
    pub source_filter: Option<PropertyFilter>,
    pub rel_type: String,
    pub rel_filter: Option<PropertyFilter>,
    pub target_label: String,
    pub target_filter: Option<PropertyFilter>,
    pub detach: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutation {
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
        table_kind: TableKind,
        table: String,
        property: String,
        value_type: PropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: TableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: TableKind,
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
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<NodeSetAssignment>,
        post_merge_assignments: Vec<NodeSetAssignment>,
    },
    MergeConnectedNodes(ConnectedNodesCreate),
    SetNodeProperty {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetNodePropertyAddInt {
        label: String,
        filter: Option<PropertyFilter>,
        property: String,
        amount: i64,
    },
    SetNodeProperties {
        label: String,
        filter: Option<PropertyFilter>,
        assignments: Vec<NodeSetAssignment>,
    },
    SetRelationshipProperty {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        label: String,
        filter: Option<PropertyFilter>,
        detach: bool,
    },
    DeleteRelationship {
        source_label: String,
        filter: Option<PropertyFilter>,
        rel_type: String,
        target_label: String,
        target_filter: Option<PropertyFilter>,
        rel_filter: Option<PropertyFilter>,
    },
    DeleteRelationshipTargetNodes(RelationshipTargetNodeDelete),
    CreateRelationshipsBetweenMatches(MatchedRelationshipCreate),
    MergeRelationshipsBetweenMatches(MatchedRelationshipMerge),
    MergeRelationshipsFromMatchedRelationships(MatchedRelationshipCopyMerge),
    MergeRelationshipsToMatchedTarget(MatchedRelationshipRetargetMerge),
    MergeRelationshipsFromMatchedTarget(MatchedRelationshipSourceRetargetMerge),
    CreateConnectedNodes(ConnectedNodesCreate),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSetAssignment {
    pub property: String,
    pub value: NodeSetValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSetValue {
    Value(Value),
    Coalesce { default: Value },
    AddInt { amount: i64 },
    DecrementFloorZero,
    PreserveNewerExisting { incoming: Value, preserve: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilter {
    And(Vec<PropertyFilter>),
    Or(Vec<PropertyFilter>),
    Not(Box<PropertyFilter>),
    IdEq {
        value: Value,
    },
    IdNotEq {
        value: Value,
    },
    IdRange {
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    IdIn {
        values: Vec<Value>,
    },
    Eq {
        property: String,
        value: Value,
    },
    NotEq {
        property: String,
        value: Value,
    },
    IsNull {
        property: String,
    },
    IsNotNull {
        property: String,
    },
    In {
        property: String,
        values: Vec<Value>,
    },
    ListContains {
        property: String,
        value: Value,
    },
    ListContainsLower {
        property: String,
        value: String,
    },
    Contains {
        property: String,
        value: String,
    },
    StartsWith {
        property: String,
        value: String,
    },
    EndsWith {
        property: String,
        value: String,
    },
    RegexMatch {
        property: String,
        pattern: ValidatedRegex,
    },
    DefaultIfNullOrEq {
        property: String,
        empty: Value,
        default: Value,
        value: Value,
        negated: bool,
    },
    Range {
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_contract_keeps_property_filters_structured() {
        let mutation = GraphMutation::DeleteNode {
            label: "Memory".to_string(),
            filter: Some(PropertyFilter::Eq {
                property: "status".to_string(),
                value: Value::String("deleted".to_string()),
            }),
            detach: false,
        };
        assert!(matches!(
            mutation,
            GraphMutation::DeleteNode {
                filter: Some(PropertyFilter::Eq { .. }),
                ..
            }
        ));
    }
}
