use crate::cypher::{
    ComparisonOp as CypherComparisonOp, GraphAlgorithmKind as CypherGraphAlgorithmKind,
    GraphAlgorithmOptions as CypherGraphAlgorithmOptions, MatchReturn,
    OrderDirection as CypherOrderDirection, OrderExpression, OrderItem, PostWithNodeLookup,
    PropertyPredicate, RelationshipDirection, RelationshipExpand as CypherRelationshipExpand,
    ReturnExpression, ReturnItem, ReturnValueExpression,
    SchemaObjectState as CypherSchemaObjectState, SchemaPropertyType as CypherSchemaPropertyType,
    SchemaTableKind as CypherSchemaTableKind, SetProperty, SetValueExpression, ShortestPathReturn,
    ShortestPathReturnExpression, Statement, ValueExpression, WithAggregateProjection,
    WithAliasFilter, WithAliasFilterExpression, WithAliasFilterOp, WithCollect,
    WithDistinctProjection, WithProjection,
};
use crate::error::{Result, SkeinError};
use crate::value::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
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
        old_rel_variable: Option<String>,
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
    CreateMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
    },
    NodeScan {
        variable: String,
        label: String,
    },
    NodeCartesianProduct {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    NodeColumnLookup {
        variable: String,
        label: String,
        property: String,
        column: String,
        optional: bool,
        input: Box<LogicalPlan>,
    },
    Expand {
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
        input: Box<LogicalPlan>,
    },
    OptionalDegree {
        source_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        alias: String,
        input: Box<LogicalPlan>,
    },
    OptionalRelationshipCountSum {
        variable: String,
        label: String,
        properties: BTreeMap<String, Value>,
        legs: Vec<RelationshipCountLeg>,
        output: String,
    },
    ThreadRepairStats {
        label: String,
        identity_label: String,
        identity_ref_property: String,
        thread_id_property: String,
        message_rel_type: String,
        message_label: String,
        memory_rel_type: String,
        memory_label: String,
    },
    ShortestPath {
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
    Filter {
        predicate: Predicate,
        input: Box<LogicalPlan>,
    },
    Project {
        items: Vec<Projection>,
        input: Box<LogicalPlan>,
    },
    Aggregate {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
        input: Box<LogicalPlan>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    Sort {
        items: Vec<SortItem>,
        input: Box<LogicalPlan>,
    },
    Limit {
        offset: usize,
        limit: Option<usize>,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    ConstantBool(bool),
    RelationshipExists {
        variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_label: String,
    },
    BoundRelationshipExists {
        source_variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
    },
    IdEq {
        variable: String,
        value: Value,
    },
    IdNotEq {
        variable: String,
        value: Value,
    },
    IdCompare {
        variable: String,
        op: ComparisonOp,
        value: Value,
    },
    IdIn {
        variable: String,
        values: Vec<Value>,
    },
    PropertyEq {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyNotEq {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyCompare {
        variable: String,
        property: String,
        op: ComparisonOp,
        value: Value,
    },
    ExpressionEq {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    ExpressionNotEq {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    ExpressionCompare {
        expression: ProjectionExpression,
        op: ComparisonOp,
        value: ProjectionExpression,
    },
    ExpressionContains {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    PropertyListContains {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyContains {
        variable: String,
        property: String,
        value: String,
    },
    PropertyStartsWith {
        variable: String,
        property: String,
        value: String,
    },
    PropertyEndsWith {
        variable: String,
        property: String,
        value: String,
    },
    PropertyRegexMatch {
        variable: String,
        property: String,
        pattern: String,
    },
    PropertyIsNull {
        variable: String,
        property: String,
    },
    PropertyIsNotNull {
        variable: String,
        property: String,
    },
    PropertyIn {
        variable: String,
        property: String,
        values: Vec<Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaTableKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaPropertyType {
    Any,
    Bool,
    Int,
    Float,
    String,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaObjectState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphAlgorithmKind {
    PageRank,
    Louvain,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphAlgorithmOptions {
    pub damping: Option<f64>,
    pub max_iterations: Option<usize>,
    pub max_levels: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub expression: ProjectionExpression,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetNodePropertiesReturnMode {
    Project(Vec<Projection>),
    Count { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortestPathProjection {
    pub expression: ShortestPathProjectionExpression,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShortestPathProjectionExpression {
    NodePropertyList { property: String },
    Length,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    Value(Value),
    Coalesce {
        property: String,
        default: Value,
    },
    AddInt {
        property: String,
        amount: i64,
    },
    DecrementFloorZero {
        property: String,
    },
    PreserveNewerExisting {
        property: String,
        incoming: Value,
        preserve: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAssignment {
    pub property: String,
    pub value: SetValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipSetAssignment {
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipCountLeg {
    pub rel_type: String,
    pub direction: RelationshipDirection,
    pub distinct: bool,
    pub filter: Option<RelationshipCountFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipCountFilter {
    PropertyNotEqOrEmpty { property: String, value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipOnCreateValue {
    Value(Value),
    MatchedRelationshipProperty { property: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionExpression {
    Variable {
        variable: String,
    },
    Property {
        variable: String,
        property: String,
    },
    Id {
        variable: String,
    },
    RelationshipType {
        variable: String,
    },
    Literal(Value),
    Coalesce(Vec<ProjectionExpression>),
    Left {
        expression: Box<ProjectionExpression>,
        length: usize,
    },
    Lower(Box<ProjectionExpression>),
    DatePart {
        part: DatePart,
        variable: String,
        property: String,
    },
    DefaultIfNullOrEq {
        variable: String,
        property: String,
        empty: Value,
        default: Value,
    },
    DefaultIfNull {
        variable: String,
        property: String,
        default: Value,
    },
    CasePropertyNotNullOrEq {
        variable: String,
        property: String,
        empty: Value,
        non_empty: Value,
        null_or_empty: Value,
    },
    CasePropertyEqualsRank {
        variable: String,
        property: String,
        branches: Vec<(Value, Value)>,
        default: Value,
    },
    CaseLowerPropertyDefault {
        variable: String,
        property: String,
        default: Value,
    },
    CaseCoalesceDifferenceFloorZero {
        variable: String,
        terms: Vec<CoalesceDifferenceProjectionTerm>,
    },
    CaseEntitySearchRank(Box<CaseEntitySearchRankProjection>),
    CaseColumnSearchRank(Box<CaseColumnSearchRankProjection>),
    ColumnDefaultIfNullOrEq {
        column: String,
        property: String,
        empty: Value,
        default: Value,
    },
    ColumnValueDefaultIfNull {
        column: String,
        default: Value,
    },
    ColumnValueCasePropertyNotNullOrEq {
        column: String,
        empty: Value,
        non_empty: Value,
        null_or_empty: Value,
    },
    Column(String),
    ColumnProperty {
        column: String,
        property: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseEntitySearchRankProjection {
    pub variable: String,
    pub name_property: String,
    pub aliases_property: String,
    pub raw_query: Value,
    pub normalized_query: Value,
    pub raw_input: Value,
    pub exact_rank: Value,
    pub alias_rank: Value,
    pub fallback_rank: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseColumnSearchRankProjection {
    pub column: String,
    pub raw_query: Value,
    pub normalized_query: Value,
    pub exact_rank: Value,
    pub contains_rank: Value,
    pub fallback_rank: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceDifferenceProjectionTerm {
    pub property: String,
    pub default: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregation {
    pub function: AggregateFunction,
    pub target: AggregateTarget,
    pub distinct: bool,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Min,
    Max,
    Avg,
    Collect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTarget {
    All,
    Variable(String),
    Property { variable: String, property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortItem {
    pub key: SortKey,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKey {
    Property { variable: String, property: String },
    Id { variable: String },
    Expression(ProjectionExpression),
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

pub fn plan(statement: &Statement) -> Result<LogicalPlan> {
    plan_with_params(statement, &BTreeMap::new())
}

pub fn plan_with_params(
    statement: &Statement,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    match statement {
        Statement::BeginTransaction | Statement::Commit | Statement::Rollback => {
            Err(SkeinError::Semantic(
                "transaction control is executed by a database session".to_string(),
            ))
        }
        Statement::Checkpoint => Err(SkeinError::Semantic(
            "CHECKPOINT is executed by the database session".to_string(),
        )),
        Statement::CypherQuery(_) => Err(SkeinError::Semantic(
            "CYPHER system hints are applied before planning".to_string(),
        )),
        Statement::Explain(_) => Err(SkeinError::Semantic(
            "EXPLAIN is executed by the database query runtime".to_string(),
        )),
        Statement::SetSystemVariable(_) => Err(SkeinError::Semantic(
            "SET system variable is executed by the database session".to_string(),
        )),
        Statement::CreateNodeLabel(label) => Ok(LogicalPlan::CreateNodeLabel {
            label: label.clone(),
        }),
        Statement::CreateRelationshipType(rel_type) => Ok(LogicalPlan::CreateRelationshipType {
            rel_type: rel_type.clone(),
        }),
        Statement::CreateNodeTable(name) => Ok(LogicalPlan::CreateNodeTable { name: name.clone() }),
        Statement::CreateRelationshipTable(name) => {
            Ok(LogicalPlan::CreateRelationshipTable { name: name.clone() })
        }
        Statement::CreateProperty(property) => Ok(LogicalPlan::CreateProperty {
            table_kind: plan_schema_table_kind(property.table_kind),
            table: property.table.clone(),
            property: property.property.clone(),
            value_type: plan_schema_property_type(property.value_type),
            nullable: property.nullable,
        }),
        Statement::AlterTableState(alter) => Ok(LogicalPlan::AlterTableState {
            table_kind: plan_schema_table_kind(alter.table_kind),
            table: alter.table.clone(),
            state: plan_schema_object_state(alter.state),
        }),
        Statement::AlterPropertyState(alter) => Ok(LogicalPlan::AlterPropertyState {
            table_kind: plan_schema_table_kind(alter.table_kind),
            table: alter.table.clone(),
            property: alter.property.clone(),
            state: plan_schema_object_state(alter.state),
        }),
        Statement::CreateIndex(index) => Ok(LogicalPlan::CreateIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateCompositeIndex(index) => Ok(LogicalPlan::CreateCompositeIndex {
            label: index.label.clone(),
            properties: index.properties.clone(),
        }),
        Statement::CreateRangeIndex(index) => Ok(LogicalPlan::CreateRangeIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateFullTextIndex(index) => Ok(LogicalPlan::CreateFullTextIndex {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateUniqueConstraint(index) => Ok(LogicalPlan::CreateUniqueConstraint {
            label: index.label.clone(),
            property: index.property.clone(),
        }),
        Statement::CreateNodePropertyExistsConstraint(index) => {
            Ok(LogicalPlan::CreateNodePropertyExistsConstraint {
                label: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::CreateRelationshipUniqueConstraint(index) => {
            Ok(LogicalPlan::CreateRelationshipUniqueConstraint {
                rel_type: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::CreateRelationshipPropertyExistsConstraint(index) => {
            Ok(LogicalPlan::CreateRelationshipPropertyExistsConstraint {
                rel_type: index.label.clone(),
                property: index.property.clone(),
            })
        }
        Statement::ProjectGraph(project) => Ok(LogicalPlan::ProjectGraph {
            name: project.name.clone(),
            node_labels: project.node_labels.clone(),
            rel_types: project.rel_types.clone(),
        }),
        Statement::GraphAlgorithm(algorithm) => Ok(LogicalPlan::GraphAlgorithm {
            algorithm: plan_graph_algorithm_kind(algorithm.algorithm),
            graph_name: algorithm.graph_name.clone(),
            options: bind_graph_algorithm_options(&algorithm.options, parameters)?,
            score_column: algorithm.score_column.clone(),
        }),
        Statement::CreateNode(node) => Ok(LogicalPlan::CreateNode {
            label: node.label.clone(),
            properties: bind_properties(&node.properties, parameters)?,
        }),
        Statement::MergeNode(node) => {
            let on_create_properties = bind_on_create_set_properties(
                node.variable.as_deref(),
                &node.on_create_sets,
                parameters,
            )?;
            let on_match_assignments = bind_on_match_set_assignments(
                node.variable.as_deref(),
                &node.on_match_sets,
                parameters,
            )?;
            let post_merge_assignments = bind_post_merge_set_assignments(
                node.variable.as_deref(),
                &node.post_merge_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeNode {
                label: node.label.clone(),
                match_properties: bind_properties(&node.properties, parameters)?,
                on_create_properties,
                on_match_assignments,
                post_merge_assignments,
            })
        }
        Statement::MergeRelationship(relationship) => Ok(LogicalPlan::MergeRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
        Statement::MatchCreateRelationship(create) => {
            if create.create_source_variable != create.source_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship CREATE source variable '{}' does not match bound variable '{}'",
                    create.create_source_variable, create.source_variable
                )));
            }
            if create.create_target_variable != create.target_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship CREATE target variable '{}' does not match bound variable '{}'",
                    create.create_target_variable, create.target_variable
                )));
            }
            let (source_properties, target_properties) = bind_two_node_relationship_create_filters(
                &create.source_variable,
                &create.source_properties,
                &create.target_variable,
                &create.target_properties,
                create.predicate.as_ref(),
                parameters,
            )?;
            Ok(LogicalPlan::CreateMatchedRelationship {
                source_label: create.source_label.clone(),
                source_properties,
                target_label: create.target_label.clone(),
                target_properties,
                rel_type: create.rel_type.clone(),
                rel_properties: bind_properties(&create.rel_properties, parameters)?,
            })
        }
        Statement::MatchMergeRelationship(merge) => {
            if merge.merge_source_variable != merge.source_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship MERGE source variable '{}' does not match bound variable '{}'",
                    merge.merge_source_variable, merge.source_variable
                )));
            }
            if merge.merge_target_variable != merge.target_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship MERGE target variable '{}' does not match bound variable '{}'",
                    merge.merge_target_variable, merge.target_variable
                )));
            }
            let (source_properties, target_properties) = bind_two_node_relationship_create_filters(
                &merge.source_variable,
                &merge.source_properties,
                &merge.target_variable,
                &merge.target_properties,
                merge.predicate.as_ref(),
                parameters,
            )?;
            let on_create_properties = bind_relationship_on_create_set_properties(
                merge.rel_variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeMatchedRelationship {
                source_label: merge.source_label.clone(),
                source_properties,
                target_label: merge.target_label.clone(),
                target_properties,
                rel_type: merge.rel_type.clone(),
                rel_match_properties: bind_properties(&merge.rel_properties, parameters)?,
                on_create_properties,
            })
        }
        Statement::MatchExpandMergeRelationship(merge) => {
            if merge.expand.direction != RelationshipDirection::Outgoing
                || merge.expand.min_hops != 1
                || merge.expand.max_hops != 1
            {
                return Err(SkeinError::Semantic(
                    "relationship-copy MERGE supports only one-hop outgoing MATCH patterns"
                        .to_string(),
                ));
            }
            if merge.merge_source_variable != merge.source_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship-copy MERGE source variable '{}' does not match bound variable '{}'",
                    merge.merge_source_variable, merge.source_variable
                )));
            }
            if merge.merge_target_variable != merge.expand.target_variable {
                return Err(SkeinError::Semantic(format!(
                    "relationship-copy MERGE target variable '{}' does not match bound variable '{}'",
                    merge.merge_target_variable, merge.expand.target_variable
                )));
            }
            if merge.predicate.is_some() {
                return Err(SkeinError::Semantic(
                    "relationship-copy MERGE does not support WHERE predicates".to_string(),
                ));
            }
            let on_create_properties = bind_relationship_copy_on_create_set_properties(
                merge.rel_variable.as_deref(),
                merge.expand.variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            Ok(LogicalPlan::MergeRelationshipFromMatchedRelationship {
                source_label: merge.source_label.clone(),
                source_properties: bind_properties(&merge.source_properties, parameters)?,
                old_rel_variable: merge.expand.variable.clone(),
                old_rel_type: merge.expand.rel_type.clone(),
                old_rel_properties: bind_properties(&merge.expand.properties, parameters)?,
                target_label: merge.expand.target_label.clone(),
                target_properties: bind_properties(&merge.expand.target_properties, parameters)?,
                new_rel_type: merge.rel_type.clone(),
                new_rel_match_properties: bind_properties(&merge.rel_properties, parameters)?,
                on_create_properties,
            })
        }
        Statement::MatchExpandMatchMergeRelationship(merge) => {
            if merge.expand.direction != RelationshipDirection::Outgoing
                || merge.expand.min_hops != 1
                || merge.expand.max_hops != 1
            {
                return Err(SkeinError::Semantic(
                    "relationship retarget MERGE supports only one-hop outgoing MATCH patterns"
                        .to_string(),
                ));
            }
            let (source_properties, new_target_properties) =
                bind_two_node_relationship_create_filters(
                    &merge.source_variable,
                    &merge.source_properties,
                    &merge.matched_target_variable,
                    &merge.matched_target_properties,
                    merge.predicate.as_ref(),
                    parameters,
                )?;
            let on_create_properties = bind_relationship_on_create_set_properties(
                merge.rel_variable.as_deref(),
                &merge.on_create_sets,
                parameters,
            )?;
            let old_rel_properties = bind_properties(&merge.expand.properties, parameters)?;
            let old_target_properties =
                bind_properties(&merge.expand.target_properties, parameters)?;
            let new_rel_match_properties = bind_properties(&merge.rel_properties, parameters)?;
            if merge.merge_source_variable == merge.source_variable
                && merge.merge_target_variable == merge.matched_target_variable
            {
                return Ok(LogicalPlan::MergeRelationshipToMatchedTarget {
                    source_label: merge.source_label.clone(),
                    source_properties,
                    old_rel_type: merge.expand.rel_type.clone(),
                    old_rel_properties,
                    old_target_label: merge.expand.target_label.clone(),
                    old_target_properties,
                    new_target_label: merge.matched_target_label.clone(),
                    new_target_properties,
                    new_rel_type: merge.rel_type.clone(),
                    new_rel_match_properties,
                    on_create_properties,
                });
            }
            if merge.merge_source_variable == merge.matched_target_variable
                && merge.merge_target_variable == merge.expand.target_variable
            {
                return Ok(LogicalPlan::MergeRelationshipFromMatchedTarget {
                    old_source_label: merge.source_label.clone(),
                    old_source_properties: source_properties,
                    old_rel_type: merge.expand.rel_type.clone(),
                    old_rel_properties,
                    old_target_label: merge.expand.target_label.clone(),
                    old_target_properties,
                    new_source_label: merge.matched_target_label.clone(),
                    new_source_properties: new_target_properties,
                    new_rel_type: merge.rel_type.clone(),
                    new_rel_match_properties,
                    on_create_properties,
                });
            }
            Err(SkeinError::Semantic(format!(
                "relationship retarget MERGE variables '{}'-'{}' do not match supported bound pairs '{}'-'{}' or '{}'-'{}'",
                merge.merge_source_variable,
                merge.merge_target_variable,
                merge.source_variable,
                merge.matched_target_variable,
                merge.matched_target_variable,
                merge.expand.target_variable
            )))
        }
        Statement::MatchSet(update) => {
            if update.sets.is_empty() {
                return Err(SkeinError::Semantic(
                    "SET requires at least one assignment".to_string(),
                ));
            }
            if let Some(expand) = &update.expand {
                if expand.min_hops != 1 || expand.max_hops != 1 {
                    return Err(SkeinError::Semantic(
                        "relationship SET supports only one-hop relationship patterns".to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing {
                    return Err(SkeinError::Semantic(
                        "relationship SET supports only outgoing relationship patterns".to_string(),
                    ));
                }
                if expand.rel_type.is_empty() {
                    return Err(SkeinError::Semantic(
                        "relationship SET requires a relationship type".to_string(),
                    ));
                }
                let Some(rel_variable) = &expand.variable else {
                    return Err(SkeinError::Semantic(
                        "relationship SET requires a relationship variable".to_string(),
                    ));
                };
                for set in &update.sets {
                    if set.variable != *rel_variable {
                        return Err(SkeinError::Semantic(format!(
                            "relationship SET can only update relationship variable '{}', got '{}'",
                            rel_variable, set.variable
                        )));
                    }
                }
                let mutation_predicate = plan_relationship_mutation_predicate(
                    update.predicate.as_ref(),
                    &update.variable,
                    rel_variable,
                    &expand.target_variable,
                    &expand.target_properties,
                    parameters,
                )?;
                let predicate = combine_pattern_and_optional_predicate(
                    &update.variable,
                    &update.properties,
                    mutation_predicate.source_predicate,
                    parameters,
                )?;
                let assignments = update
                    .sets
                    .iter()
                    .map(|set| {
                        Ok(RelationshipSetAssignment {
                            property: set.property.clone(),
                            value: bind_relationship_set_value(&set.value, parameters)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                if assignments.len() == 1 {
                    let assignment = assignments
                        .into_iter()
                        .next()
                        .expect("relationship SET assignment is present");
                    return Ok(LogicalPlan::SetRelationshipProperty {
                        source_variable: update.variable.clone(),
                        source_label: update.label.clone(),
                        predicate,
                        rel_variable: rel_variable.clone(),
                        rel_type: expand.rel_type.clone(),
                        rel_properties: bind_properties(&expand.properties, parameters)?,
                        rel_predicate: mutation_predicate.rel_predicate,
                        target_variable: expand.target_variable.clone(),
                        target_label: expand.target_label.clone(),
                        target_properties: mutation_predicate.target_properties,
                        property: assignment.property,
                        value: assignment.value,
                    });
                }
                return Ok(LogicalPlan::SetRelationshipProperties {
                    source_variable: update.variable.clone(),
                    source_label: update.label.clone(),
                    predicate,
                    rel_variable: rel_variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    rel_predicate: mutation_predicate.rel_predicate,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    target_properties: mutation_predicate.target_properties,
                    assignments,
                });
            }
            let scope = BTreeSet::from([update.variable.clone()]);
            if let Some(predicate) = &update.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut assignments = Vec::with_capacity(update.sets.len());
            for set in &update.sets {
                if set.variable != update.variable {
                    return Err(SkeinError::Semantic(format!(
                        "unknown variable '{}' in set item",
                        set.variable
                    )));
                }
                assignments.push(SetAssignment {
                    property: set.property.clone(),
                    value: plan_set_value(set, parameters)?,
                });
            }
            Ok(LogicalPlan::SetNodeProperties {
                variable: update.variable.clone(),
                label: update.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &update.variable,
                    &update.properties,
                    update.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                assignments,
            })
        }
        Statement::MatchSetReturn(update_return) => {
            let update = &update_return.update;
            if update.expand.is_some() {
                return Err(SkeinError::Semantic(
                    "SET RETURN supports only single-node MATCH updates".to_string(),
                ));
            }
            if update.sets.is_empty() {
                return Err(SkeinError::Semantic(
                    "SET requires at least one assignment".to_string(),
                ));
            }
            let scope = BTreeSet::from([update.variable.clone()]);
            if let Some(predicate) = &update.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut assignments = Vec::with_capacity(update.sets.len());
            for set in &update.sets {
                if set.variable != update.variable {
                    return Err(SkeinError::Semantic(format!(
                        "unknown variable '{}' in set item",
                        set.variable
                    )));
                }
                assignments.push(SetAssignment {
                    property: set.property.clone(),
                    value: plan_set_value(set, parameters)?,
                });
            }
            let returns =
                plan_set_node_properties_return_mode(update, &update_return.returns, parameters)?;
            Ok(LogicalPlan::SetNodePropertiesReturn {
                variable: update.variable.clone(),
                label: update.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &update.variable,
                    &update.properties,
                    update.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                assignments,
                returns,
            })
        }
        Statement::MatchOptionalRelationshipCountSum(query) => {
            if query.legs.is_empty() {
                return Err(SkeinError::Semantic(
                    "optional relationship count sum requires at least one count leg".to_string(),
                ));
            }
            Ok(LogicalPlan::OptionalRelationshipCountSum {
                variable: query.variable.clone(),
                label: query.label.clone(),
                properties: bind_properties(&query.properties, parameters)?,
                legs: bind_relationship_count_legs(&query.legs, parameters)?,
                output: query.output.clone(),
            })
        }
        Statement::MatchThreadRepairStats(query) => Ok(LogicalPlan::ThreadRepairStats {
            label: query.label.clone(),
            identity_label: query.identity_label.clone(),
            identity_ref_property: query.identity_ref_property.clone(),
            thread_id_property: query.thread_id_property.clone(),
            message_rel_type: query.message_rel_type.clone(),
            message_label: query.message_label.clone(),
            memory_rel_type: query.memory_rel_type.clone(),
            memory_label: query.memory_label.clone(),
        }),
        Statement::MatchDelete(delete) => {
            if let Some(expand) = &delete.expand {
                if expand.min_hops != 1 || expand.max_hops != 1 {
                    return Err(SkeinError::Semantic(
                        "relationship DELETE supports only one-hop relationship patterns"
                            .to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing {
                    return Err(SkeinError::Semantic(
                        "relationship DELETE supports only outgoing relationship patterns"
                            .to_string(),
                    ));
                }
                if expand.rel_type.is_empty() {
                    return Err(SkeinError::Semantic(
                        "relationship DELETE requires a relationship type".to_string(),
                    ));
                }
                if delete.detach {
                    if delete.delete_variable != expand.target_variable {
                        return Err(SkeinError::Semantic(format!(
                            "DETACH DELETE after relationship MATCH can only delete target variable '{}', got '{}'",
                            expand.target_variable, delete.delete_variable
                        )));
                    }
                    if expand.variable.is_some() || !expand.properties.is_empty() {
                        return Err(SkeinError::Semantic(
                            "DETACH DELETE after relationship MATCH does not support relationship variables or properties yet".to_string(),
                        ));
                    }
                    let scope = BTreeSet::from([delete.variable.clone()]);
                    let source_predicate = combine_pattern_and_optional_cypher_predicate(
                        &delete.variable,
                        &delete.properties,
                        delete.predicate.as_ref(),
                        &scope,
                        parameters,
                    )?;
                    return Ok(LogicalPlan::DeleteRelationshipTargetNodes {
                        source_variable: delete.variable.clone(),
                        source_label: delete.label.clone(),
                        source_predicate,
                        rel_type: expand.rel_type.clone(),
                        rel_properties: BTreeMap::new(),
                        target_variable: expand.target_variable.clone(),
                        target_label: expand.target_label.clone(),
                        target_properties: bind_properties(&expand.target_properties, parameters)?,
                        detach: true,
                    });
                }
                let Some(rel_variable) = &expand.variable else {
                    return Err(SkeinError::Semantic(
                        "relationship DELETE requires a relationship variable".to_string(),
                    ));
                };
                if delete.delete_variable != *rel_variable {
                    return Err(SkeinError::Semantic(format!(
                        "relationship DELETE can only delete relationship variable '{}', got '{}'",
                        rel_variable, delete.delete_variable
                    )));
                }
                let mutation_predicate = plan_relationship_mutation_predicate(
                    delete.predicate.as_ref(),
                    &delete.variable,
                    rel_variable,
                    &expand.target_variable,
                    &expand.target_properties,
                    parameters,
                )?;
                let predicate = combine_pattern_and_optional_predicate(
                    &delete.variable,
                    &delete.properties,
                    mutation_predicate.source_predicate,
                    parameters,
                )?;
                return Ok(LogicalPlan::DeleteRelationship {
                    source_variable: delete.variable.clone(),
                    source_label: delete.label.clone(),
                    predicate,
                    rel_variable: rel_variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    rel_predicate: mutation_predicate.rel_predicate,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    target_properties: mutation_predicate.target_properties,
                });
            }
            let scope = BTreeSet::from([delete.variable.clone()]);
            if let Some(predicate) = &delete.predicate {
                validate_predicate(&scope, predicate)?;
            }
            if delete.delete_variable != delete.variable {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{}' in delete item",
                    delete.delete_variable
                )));
            }
            Ok(LogicalPlan::DeleteNode {
                variable: delete.variable.clone(),
                label: delete.label.clone(),
                predicate: combine_pattern_and_optional_cypher_predicate(
                    &delete.variable,
                    &delete.properties,
                    delete.predicate.as_ref(),
                    &scope,
                    parameters,
                )?,
                detach: delete.detach,
            })
        }
        Statement::CreateRelationship(relationship) => Ok(LogicalPlan::CreateRelationship {
            source_label: relationship.source.label.clone(),
            source_properties: bind_properties(&relationship.source.properties, parameters)?,
            rel_type: relationship.rel_type.clone(),
            rel_properties: bind_properties(&relationship.properties, parameters)?,
            target_label: relationship.target.label.clone(),
            target_properties: bind_properties(&relationship.target.properties, parameters)?,
        }),
        Statement::MatchNodesReturn(query) => {
            if query.left_variable == query.right_variable {
                return Err(SkeinError::Semantic(format!(
                    "duplicate node variable '{}' in match pattern",
                    query.left_variable
                )));
            }
            let scope = BTreeSet::from([query.left_variable.clone(), query.right_variable.clone()]);
            let mut left = LogicalPlan::NodeScan {
                variable: query.left_variable.clone(),
                label: query.left_label.clone(),
            };
            if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                &query.left_variable,
                &query.left_properties,
                parameters,
            )?) {
                left = LogicalPlan::Filter {
                    predicate,
                    input: Box::new(left),
                };
            }
            let mut right = LogicalPlan::NodeScan {
                variable: query.right_variable.clone(),
                label: query.right_label.clone(),
            };
            if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                &query.right_variable,
                &query.right_properties,
                parameters,
            )?) {
                right = LogicalPlan::Filter {
                    predicate,
                    input: Box::new(right),
                };
            }
            let input = LogicalPlan::NodeCartesianProduct {
                left: Box::new(left),
                right: Box::new(right),
            };
            let mut input = if let Some(predicate) = query
                .predicate
                .as_ref()
                .map(|predicate| plan_predicate(predicate, &scope, parameters))
                .transpose()?
            {
                LogicalPlan::Filter {
                    predicate,
                    input: Box::new(input),
                }
            } else {
                input
            };
            let planned_returns = plan_return_items(&scope, &query.returns, parameters)?;
            input = planned_returns.into_logical(input);
            let limit = query
                .limit
                .as_ref()
                .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                .transpose()?;
            if let Some(limit) = limit {
                input = LogicalPlan::Limit {
                    offset: 0,
                    limit: Some(limit),
                    input: Box::new(input),
                };
            }
            Ok(input)
        }
        Statement::ShortestPathReturn(query) => plan_shortest_path_return(query, parameters),
        Statement::MatchReturn(query) => {
            let mut scope = BTreeSet::from([query.variable.clone()]);
            if let Some(expand) = &query.expand {
                scope.insert(expand.target_variable.clone());
                if !expand.properties.is_empty() && (expand.min_hops != 1 || expand.max_hops != 1) {
                    return Err(SkeinError::Semantic(
                        "relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if expand.rel_type.is_empty() && (expand.min_hops != 1 || expand.max_hops != 1) {
                    return Err(SkeinError::Semantic(
                        "untyped relationship patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if expand.direction != RelationshipDirection::Outgoing
                    && (expand.min_hops != 1 || expand.max_hops != 1)
                {
                    return Err(SkeinError::Semantic(
                        "non-outgoing relationship patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if !expand.target_properties.is_empty()
                    && (expand.min_hops != 1 || expand.max_hops != 1)
                {
                    return Err(SkeinError::Semantic(
                        "target node property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if let Some(rel_variable) = &expand.variable {
                    if expand.min_hops != 1 || expand.max_hops != 1 {
                        return Err(SkeinError::Semantic(
                            "relationship variables are supported only for one-hop patterns"
                                .to_string(),
                        ));
                    }
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(post_expand) = &query.post_match_expand {
                if !scope.contains(&post_expand.source_variable) {
                    return Err(SkeinError::Semantic(format!(
                        "post-MATCH source variable '{}' is not bound",
                        post_expand.source_variable
                    )));
                }
                if !post_expand.source_properties.is_empty() {
                    return Err(SkeinError::Semantic(
                        "post-MATCH relationship reads do not support source property patterns"
                            .to_string(),
                    ));
                }
                if !post_expand.expand.properties.is_empty()
                    && (post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1)
                {
                    return Err(SkeinError::Semantic(
                        "post-MATCH relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                if !post_expand.expand.target_properties.is_empty()
                    && (post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1)
                {
                    return Err(SkeinError::Semantic(
                        "post-MATCH target node property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                scope.insert(post_expand.expand.target_variable.clone());
                if let Some(rel_variable) = &post_expand.expand.variable {
                    if post_expand.expand.min_hops != 1 || post_expand.expand.max_hops != 1 {
                        return Err(SkeinError::Semantic(
                            "post-MATCH relationship variables are supported only for one-hop patterns"
                                .to_string(),
                        ));
                    }
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(optional) = &query.optional_expand {
                if !scope.contains(&optional.source_variable) {
                    return Err(SkeinError::Semantic(format!(
                        "OPTIONAL MATCH source variable '{}' is not bound",
                        optional.source_variable
                    )));
                }
                if query.optional_with.is_none()
                    && !returns_are_count_only(&query.returns)
                    && optional_direct_count_alias(query, optional)?.is_none()
                    && optional_direct_collect_alias(query, optional)?.is_none()
                    && !optional_direct_row_projection(query, optional)
                {
                    return Err(SkeinError::Semantic(
                        "OPTIONAL MATCH is currently supported only for COUNT returns, source projections plus one COUNT, source projections plus one COLLECT, or non-aggregate row projections".to_string(),
                    ));
                }
                if !optional.expand.properties.is_empty()
                    && (optional.expand.min_hops != 1 || optional.expand.max_hops != 1)
                {
                    return Err(SkeinError::Semantic(
                        "OPTIONAL MATCH relationship property patterns are supported only for one-hop patterns"
                            .to_string(),
                    ));
                }
                scope.insert(optional.expand.target_variable.clone());
                if let Some(rel_variable) = &optional.expand.variable {
                    scope.insert(rel_variable.clone());
                }
            }
            if let Some(optional_with) = &query.optional_with {
                if let Some(optional) = &query.optional_expand {
                    if optional_with.group_variable != optional.source_variable {
                        return Err(SkeinError::Semantic(
                            "OPTIONAL MATCH WITH must group by the optional source variable"
                                .to_string(),
                        ));
                    }
                    let count_variable = optional_with.count_variable.as_str();
                    let count_matches_relationship =
                        optional.expand.variable.as_deref() == Some(count_variable);
                    let count_matches_target = optional.expand.target_variable == count_variable;
                    if !count_matches_relationship && !count_matches_target {
                        return Err(SkeinError::Semantic(
                            "OPTIONAL MATCH WITH COUNT must reference the optional relationship or target variable"
                                .to_string(),
                        ));
                    }
                    if query.returns.iter().any(|item| {
                        matches!(
                            item.expression,
                            ReturnExpression::CountAll
                                | ReturnExpression::CountVariable { .. }
                                | ReturnExpression::CountProperty { .. }
                                | ReturnExpression::CollectVariable { .. }
                                | ReturnExpression::CollectProperty { .. }
                                | ReturnExpression::MinProperty { .. }
                                | ReturnExpression::MaxProperty { .. }
                                | ReturnExpression::AvgProperty { .. }
                        )
                    }) {
                        return Err(SkeinError::Semantic(
                            "OPTIONAL MATCH WITH supports only projection returns".to_string(),
                        ));
                    }
                    validate_with_alias_filter(
                        query.aggregate_with_filter.as_ref(),
                        &scope,
                        &BTreeSet::from([optional_with.alias.clone()]),
                    )?;
                } else {
                    let aggregate_with = optional_with_as_aggregate(optional_with);
                    validate_aggregate_with_match_return(query, &aggregate_with)?;
                }
            }
            if let Some(collect_with) = &query.collect_with {
                validate_collect_with_match_return(query, collect_with)?;
            }
            if let Some(distinct_with) = &query.distinct_with {
                validate_distinct_with_match_return(query, distinct_with)?;
            }
            if let Some(with_projection) = &query.with_projection {
                validate_with_projection_match_return(query, &scope, with_projection)?;
            }
            if let Some(aggregate_with) = &query.aggregate_with {
                validate_aggregate_with_match_return(query, aggregate_with)?;
            }
            if let Some(predicate) = &query.predicate {
                validate_predicate(&scope, predicate)?;
            }
            let mut input = LogicalPlan::NodeScan {
                variable: query.variable.clone(),
                label: query.label.clone(),
            };
            if let Some(expand) = &query.expand {
                input = LogicalPlan::Expand {
                    source_variable: query.variable.clone(),
                    source_label: query.label.clone(),
                    rel_variable: expand.variable.clone(),
                    rel_type: expand.rel_type.clone(),
                    rel_properties: bind_properties(&expand.properties, parameters)?,
                    direction: expand.direction,
                    target_variable: expand.target_variable.clone(),
                    target_label: expand.target_label.clone(),
                    min_hops: expand.min_hops,
                    max_hops: expand.max_hops,
                    optional: false,
                    input: Box::new(input),
                };
            }
            if let Some(post_expand) = &query.post_match_expand {
                input = LogicalPlan::Expand {
                    source_variable: post_expand.source_variable.clone(),
                    source_label: post_expand.source_label.clone(),
                    rel_variable: post_expand.expand.variable.clone(),
                    rel_type: post_expand.expand.rel_type.clone(),
                    rel_properties: bind_properties(&post_expand.expand.properties, parameters)?,
                    direction: post_expand.expand.direction,
                    target_variable: post_expand.expand.target_variable.clone(),
                    target_label: post_expand.expand.target_label.clone(),
                    min_hops: post_expand.expand.min_hops,
                    max_hops: post_expand.expand.max_hops,
                    optional: false,
                    input: Box::new(input),
                };
            }
            let pattern_predicate = plan_match_pattern_predicate(
                &query.variable,
                &query.properties,
                query.expand.as_ref(),
                query.post_match_expand.as_ref(),
                parameters,
            )?;
            let predicate = combine_pattern_and_optional_cypher_predicate_parts(
                pattern_predicate,
                query.predicate.as_ref(),
                &scope,
                parameters,
            )?;
            if let Some(predicate) = predicate {
                if let Some(predicate) =
                    pushdown_relationship_property_eq_predicates(&mut input, predicate)
                {
                    input = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(input),
                    };
                }
            }
            if let Some(collect_with) = &query.collect_with {
                return plan_collect_with_match_return(input, query, collect_with);
            }
            if let Some(distinct_with) = &query.distinct_with {
                return plan_distinct_with_match_return(
                    input,
                    &scope,
                    query,
                    distinct_with,
                    parameters,
                );
            }
            if let Some(aggregate_with) = &query.aggregate_with {
                return plan_aggregate_with_match_return(
                    input,
                    &scope,
                    query,
                    aggregate_with,
                    parameters,
                );
            }
            if let Some(with_projection) = &query.with_projection {
                input = plan_with_projection(input, &scope, with_projection, parameters)?;
                let column_names = with_projection_column_names(with_projection);
                if let Some(filter) = &query.aggregate_with_filter {
                    input = LogicalPlan::Filter {
                        predicate: plan_with_alias_filter(filter, parameters)?,
                        input: Box::new(input),
                    };
                }
                if !query.with_order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &column_names,
                            &query.with_order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let with_offset = query
                    .with_offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let with_limit = query
                    .with_limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if with_offset > 0 || with_limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset: with_offset,
                        limit: with_limit,
                        input: Box::new(input),
                    };
                }
                let projections = query
                    .returns
                    .iter()
                    .map(|item| {
                        plan_projection_with_columns(&scope, &column_names, item, parameters)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let projection_names = projections
                    .iter()
                    .map(|projection| projection.name.clone())
                    .collect::<BTreeSet<_>>();
                input = LogicalPlan::Project {
                    items: projections,
                    input: Box::new(input),
                };
                if query.distinct {
                    input = LogicalPlan::Distinct {
                        input: Box::new(input),
                    };
                }
                if !query.order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &projection_names,
                            &query.order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let offset = query
                    .offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let limit = query
                    .limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if offset > 0 || limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset,
                        limit,
                        input: Box::new(input),
                    };
                }
                return Ok(input);
            }
            if let Some(optional_with) = &query.optional_with {
                if query.optional_expand.is_none() {
                    let aggregate_with = optional_with_as_aggregate(optional_with);
                    return plan_aggregate_with_match_return(
                        input,
                        &scope,
                        query,
                        &aggregate_with,
                        parameters,
                    );
                }
                let optional = query
                    .optional_expand
                    .as_ref()
                    .expect("optional WITH validated above");
                input = LogicalPlan::OptionalDegree {
                    source_variable: optional.source_variable.clone(),
                    rel_type: optional.expand.rel_type.clone(),
                    rel_properties: bind_properties(&optional.expand.properties, parameters)?,
                    direction: optional.expand.direction,
                    target_label: optional.expand.target_label.clone(),
                    target_properties: bind_properties(
                        &optional.expand.target_properties,
                        parameters,
                    )?,
                    alias: optional_with.alias.clone(),
                    input: Box::new(input),
                };
                if let Some(filter) = &query.aggregate_with_filter {
                    input = LogicalPlan::Filter {
                        predicate: plan_with_alias_filter(filter, parameters)?,
                        input: Box::new(input),
                    };
                }
                let projections = query
                    .returns
                    .iter()
                    .map(|item| {
                        plan_projection_with_columns(
                            &scope,
                            &BTreeSet::from([optional_with.alias.clone()]),
                            item,
                            parameters,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let projection_names = projections
                    .iter()
                    .map(|projection| projection.name.clone())
                    .collect::<BTreeSet<_>>();
                input = LogicalPlan::Project {
                    items: projections,
                    input: Box::new(input),
                };
                if query.distinct {
                    input = LogicalPlan::Distinct {
                        input: Box::new(input),
                    };
                }
                if !query.order_by.is_empty() {
                    input = LogicalPlan::Sort {
                        items: plan_sort_items(
                            &scope,
                            &projection_names,
                            &query.order_by,
                            parameters,
                        )?,
                        input: Box::new(input),
                    };
                }
                let offset = query
                    .offset
                    .as_ref()
                    .map(|offset| bind_pagination_value(offset, parameters, "offset"))
                    .transpose()?
                    .unwrap_or(0);
                let limit = query
                    .limit
                    .as_ref()
                    .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                    .transpose()?;
                if offset > 0 || limit.is_some() {
                    input = LogicalPlan::Limit {
                        offset,
                        limit,
                        input: Box::new(input),
                    };
                }
                return Ok(input);
            }
            if let Some(optional) = &query.optional_expand {
                if let Some(count_alias) = optional_direct_count_alias(query, optional)? {
                    return plan_optional_direct_count_return(
                        input,
                        &scope,
                        query,
                        optional,
                        count_alias,
                        parameters,
                    );
                }
            }
            if let Some(optional) = &query.optional_expand {
                let optional_row_projection = optional_direct_row_projection(query, optional);
                input = LogicalPlan::Expand {
                    source_variable: optional.source_variable.clone(),
                    source_label: optional.source_label.clone(),
                    rel_variable: optional.expand.variable.clone(),
                    rel_type: optional.expand.rel_type.clone(),
                    rel_properties: bind_properties(&optional.expand.properties, parameters)?,
                    direction: optional.expand.direction,
                    target_variable: optional.expand.target_variable.clone(),
                    target_label: optional.expand.target_label.clone(),
                    min_hops: 1,
                    max_hops: 1,
                    optional: optional_row_projection
                        || optional_direct_collect_alias(query, optional)?.is_some(),
                    input: Box::new(input),
                };
                if let Some(predicate) = combine_predicates(plan_node_pattern_predicates(
                    &optional.expand.target_variable,
                    &optional.expand.target_properties,
                    parameters,
                )?) {
                    input = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(input),
                    };
                }
            }
            let planned_returns = plan_return_items(&scope, &query.returns, parameters)?;
            let projection_names = planned_returns
                .names()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            let mut input = planned_returns.into_logical(input);
            if query.distinct {
                input = LogicalPlan::Distinct {
                    input: Box::new(input),
                };
            }
            if !query.order_by.is_empty() {
                input = LogicalPlan::Sort {
                    items: plan_sort_items(
                        planned_sort_scope(&input, &scope),
                        &projection_names,
                        &query.order_by,
                        parameters,
                    )?,
                    input: Box::new(input),
                };
            }
            let offset = match &query.offset {
                Some(offset) => bind_pagination_value(offset, parameters, "offset")?,
                None => 0,
            };
            let limit = query
                .limit
                .as_ref()
                .map(|limit| bind_pagination_value(limit, parameters, "limit"))
                .transpose()?;
            if offset != 0 || limit.is_some() {
                input = LogicalPlan::Limit {
                    offset,
                    limit,
                    input: Box::new(input),
                };
            }
            Ok(input)
        }
    }
}

fn plan_shortest_path_return(
    query: &ShortestPathReturn,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if query.direction == RelationshipDirection::Incoming {
        return Err(SkeinError::Semantic(
            "ALL SHORTEST path reads do not support incoming-only patterns".to_string(),
        ));
    }
    if !query.source_properties.is_empty() || !query.target_properties.is_empty() {
        return Err(SkeinError::Semantic(
            "ALL SHORTEST path reads require endpoint ids in WHERE predicates".to_string(),
        ));
    }
    if query.min_hops == 0 || query.max_hops == 0 || query.min_hops > query.max_hops {
        return Err(SkeinError::Semantic(
            "ALL SHORTEST path reads require a finite positive hop range".to_string(),
        ));
    }
    if query.rel_variable.is_some() && !query.rel_type.is_empty() {
        return Err(SkeinError::Semantic(
            "ALL SHORTEST path reads do not bind relationship variables".to_string(),
        ));
    }
    let scope = BTreeSet::from([query.source_variable.clone(), query.target_variable.clone()]);
    if let Some(predicate) = &query.predicate {
        validate_predicate(&scope, predicate)?;
    }
    let source_id = endpoint_id_value(
        query.predicate.as_ref(),
        &query.source_variable,
        parameters,
        "source",
    )?;
    let target_id = endpoint_id_value(
        query.predicate.as_ref(),
        &query.target_variable,
        parameters,
        "target",
    )?;
    let returns = query
        .returns
        .iter()
        .map(|item| {
            let expression = match &item.expression {
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable,
                    property,
                } => {
                    if path_variable != &query.path_variable {
                        return Err(SkeinError::Semantic(
                            "shortest path projection references an unknown path".to_string(),
                        ));
                    }
                    ShortestPathProjectionExpression::NodePropertyList {
                        property: property.clone(),
                    }
                }
                ShortestPathReturnExpression::Length { path_variable } => {
                    if path_variable != &query.path_variable {
                        return Err(SkeinError::Semantic(
                            "shortest path projection references an unknown path".to_string(),
                        ));
                    }
                    ShortestPathProjectionExpression::Length
                }
            };
            Ok(ShortestPathProjection {
                expression,
                name: item.alias.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::ShortestPath {
        source_variable: query.source_variable.clone(),
        source_label: query.source_label.clone(),
        source_id,
        rel_type: query.rel_type.clone(),
        direction: query.direction,
        target_variable: query.target_variable.clone(),
        target_label: query.target_label.clone(),
        target_id,
        min_hops: query.min_hops,
        max_hops: query.max_hops,
        returns,
    })
}

fn endpoint_id_value(
    predicate: Option<&PropertyPredicate>,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
    endpoint_name: &str,
) -> Result<Value> {
    let Some(predicate) = predicate else {
        return Err(SkeinError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id predicate"
        )));
    };
    find_endpoint_id_value(predicate, variable, parameters)?.ok_or_else(|| {
        SkeinError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id equality on '{variable}.id'"
        ))
    })
}

fn find_endpoint_id_value(
    predicate: &PropertyPredicate,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Value>> {
    match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                if let Some(value) = find_endpoint_id_value(predicate, variable, parameters)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        PropertyPredicate::Eq {
            variable: predicate_variable,
            property,
            value,
        } if predicate_variable == variable && property == "id" => {
            Ok(Some(bind_value(value, parameters)?))
        }
        _ => Ok(None),
    }
}

fn plan_schema_table_kind(kind: CypherSchemaTableKind) -> SchemaTableKind {
    match kind {
        CypherSchemaTableKind::Node => SchemaTableKind::Node,
        CypherSchemaTableKind::Relationship => SchemaTableKind::Relationship,
    }
}

fn validate_collect_with_match_return(
    query: &MatchReturn,
    collect_with: &WithCollect,
) -> Result<()> {
    let Some(expand) = &query.expand else {
        return Err(SkeinError::Semantic(
            "WITH COLLECT is supported only after a relationship MATCH".to_string(),
        ));
    };
    if query.optional_expand.is_some() || query.optional_with.is_some() {
        return Err(SkeinError::Semantic(
            "WITH COLLECT cannot be combined with OPTIONAL MATCH".to_string(),
        ));
    }
    if collect_with.group_variable != query.variable {
        return Err(SkeinError::Semantic(
            "WITH COLLECT must group by the source variable".to_string(),
        ));
    }
    if collect_with.collect_variable != expand.target_variable {
        return Err(SkeinError::Semantic(
            "WITH COLLECT must collect from the relationship target variable".to_string(),
        ));
    }
    if query.distinct
        || !query.order_by.is_empty()
        || query.offset.is_some()
        || query.limit.is_some()
    {
        return Err(SkeinError::Semantic(
            "WITH COLLECT currently supports only a direct RETURN".to_string(),
        ));
    }
    if query.returns.len() != 2 {
        return Err(SkeinError::Semantic(
            "WITH COLLECT currently supports exactly two RETURN items".to_string(),
        ));
    }
    let group_item = &query.returns[0];
    let ReturnExpression::Property { variable, .. } = &group_item.expression else {
        return Err(SkeinError::Semantic(
            "WITH COLLECT RETURN must start with the grouped variable property".to_string(),
        ));
    };
    if variable != &collect_with.group_variable {
        return Err(SkeinError::Semantic(
            "WITH COLLECT RETURN group property must use the grouped variable".to_string(),
        ));
    }
    let alias_item = &query.returns[1];
    match &alias_item.expression {
        ReturnExpression::Variable(variable) if variable == &collect_with.alias => Ok(()),
        _ => Err(SkeinError::Semantic(
            "WITH COLLECT RETURN must include the collected alias".to_string(),
        )),
    }
}

fn plan_collect_with_match_return(
    input: LogicalPlan,
    query: &MatchReturn,
    collect_with: &WithCollect,
) -> Result<LogicalPlan> {
    let group_item = &query.returns[0];
    let ReturnExpression::Property { variable, property } = &group_item.expression else {
        return Err(SkeinError::Semantic(
            "WITH COLLECT RETURN must start with the grouped variable property".to_string(),
        ));
    };
    Ok(LogicalPlan::Aggregate {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            },
            name: group_item
                .alias
                .clone()
                .unwrap_or_else(|| format!("{variable}.{property}")),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property {
                variable: collect_with.collect_variable.clone(),
                property: collect_with.collect_property.clone(),
            },
            distinct: collect_with.distinct,
            name: collect_with.alias.clone(),
        }],
        input: Box::new(input),
    })
}

fn validate_with_projection_match_return(
    query: &MatchReturn,
    scope: &BTreeSet<String>,
    with_projection: &WithProjection,
) -> Result<()> {
    if query.expand.is_some()
        || query.optional_expand.is_some()
        || query.optional_with.is_some()
        || query.collect_with.is_some()
        || query.distinct_with.is_some()
        || query.aggregate_with.is_some()
    {
        return Err(SkeinError::Semantic(
            "WITH projection currently supports only a direct node MATCH".to_string(),
        ));
    }
    if with_projection.items.len() < 2 {
        return Err(SkeinError::Semantic(
            "WITH projection requires the source variable and at least one alias".to_string(),
        ));
    }
    let ReturnExpression::Variable(variable) = &with_projection.items[0].expression else {
        return Err(SkeinError::Semantic(
            "WITH projection must start with the source variable".to_string(),
        ));
    };
    if variable != &query.variable || with_projection.items[0].alias.is_some() {
        return Err(SkeinError::Semantic(
            "WITH projection must preserve the source variable without alias".to_string(),
        ));
    }
    for item in &with_projection.items[1..] {
        if item.alias.is_none() {
            return Err(SkeinError::Semantic(
                "WITH projection expressions require aliases".to_string(),
            ));
        }
        if !return_expression_is_scoped(&item.expression, scope, &BTreeSet::new()) {
            return Err(SkeinError::Semantic(
                "WITH projection expression references an unknown variable".to_string(),
            ));
        }
    }
    validate_with_alias_filter(
        query.aggregate_with_filter.as_ref(),
        scope,
        &with_projection_column_names(with_projection),
    )
}

fn plan_with_projection(
    input: LogicalPlan,
    scope: &BTreeSet<String>,
    with_projection: &WithProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let projections = with_projection
        .items
        .iter()
        .map(|item| plan_projection(scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    })
}

fn with_projection_column_names(with_projection: &WithProjection) -> BTreeSet<String> {
    with_projection
        .items
        .iter()
        .map(|item| match &item.alias {
            Some(alias) => alias.clone(),
            None => match &item.expression {
                ReturnExpression::Variable(variable) => variable.clone(),
                ReturnExpression::Property { variable, property } => {
                    format!("{variable}.{property}")
                }
                _ => "expression".to_string(),
            },
        })
        .collect()
}

fn validate_distinct_with_match_return(
    query: &MatchReturn,
    distinct_with: &WithDistinctProjection,
) -> Result<()> {
    if query.expand.is_none() {
        return Err(SkeinError::Semantic(
            "WITH DISTINCT is supported only after a relationship MATCH".to_string(),
        ));
    }
    if query.optional_expand.is_some()
        || query.optional_with.is_some()
        || query.collect_with.is_some()
    {
        return Err(SkeinError::Semantic(
            "WITH DISTINCT cannot be combined with OPTIONAL MATCH or COLLECT".to_string(),
        ));
    }
    if query.distinct
        || !query.order_by.is_empty()
        || query.offset.is_some()
        || query.limit.is_some()
    {
        return Err(SkeinError::Semantic(
            "WITH DISTINCT currently supports only a direct aggregate RETURN".to_string(),
        ));
    }
    if distinct_with.items.is_empty() {
        return Err(SkeinError::Semantic(
            "WITH DISTINCT requires at least one projected item".to_string(),
        ));
    }
    for item in &distinct_with.items {
        if item.alias.is_none() {
            return Err(SkeinError::Semantic(
                "WITH DISTINCT projection items require aliases".to_string(),
            ));
        }
        if !matches!(item.expression, ReturnExpression::Property { .. }) {
            return Err(SkeinError::Semantic(
                "WITH DISTINCT currently supports only property projections".to_string(),
            ));
        }
    }
    if query.returns.len() != 1
        || !matches!(query.returns[0].expression, ReturnExpression::CountAll)
    {
        return Err(SkeinError::Semantic(
            "WITH DISTINCT currently supports only RETURN count(*)".to_string(),
        ));
    }
    Ok(())
}

fn plan_distinct_with_match_return(
    input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    distinct_with: &WithDistinctProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let projections = distinct_with
        .items
        .iter()
        .map(|item| plan_projection(scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    let distinct = LogicalPlan::Distinct {
        input: Box::new(LogicalPlan::Project {
            items: projections,
            input: Box::new(input),
        }),
    };
    Ok(LogicalPlan::Aggregate {
        group_keys: Vec::new(),
        items: vec![plan_aggregation(scope, &query.returns[0])?],
        input: Box::new(distinct),
    })
}

fn validate_aggregate_with_match_return(
    query: &MatchReturn,
    aggregate_with: &WithAggregateProjection,
) -> Result<()> {
    if query.optional_expand.is_some()
        || query.collect_with.is_some()
        || query.distinct_with.is_some()
    {
        return Err(SkeinError::Semantic(
            "WITH aggregate cannot be combined with OPTIONAL MATCH, COLLECT, or DISTINCT"
                .to_string(),
        ));
    }
    if query.distinct {
        return Err(SkeinError::Semantic(
            "WITH aggregate currently supports direct RETURN with optional LIMIT".to_string(),
        ));
    }
    let has_aggregate = aggregate_with
        .items
        .iter()
        .any(|item| is_aggregate_return_expression(&item.expression));
    let has_group_key = aggregate_with
        .items
        .iter()
        .any(|item| !is_aggregate_return_expression(&item.expression));
    if !has_aggregate || !has_group_key {
        return Err(SkeinError::Semantic(
            "WITH aggregate requires grouped projections and aggregate items".to_string(),
        ));
    }
    let column_names = aggregate_with_column_names(aggregate_with);
    let post_lookup_scope = query
        .post_with_match
        .as_ref()
        .map(|lookup| BTreeSet::from([lookup.variable.clone()]))
        .unwrap_or_default();
    for item in &query.returns {
        if is_aggregate_return_expression(&item.expression) {
            return Err(SkeinError::Semantic(
                "WITH aggregate RETURN currently supports only projected columns".to_string(),
            ));
        }
        if let Some(lookup) = &query.post_with_match {
            if matches!(&item.expression, ReturnExpression::Variable(variable) if variable == &lookup.variable)
            {
                return Err(SkeinError::Semantic(
                    "post-WITH MATCH RETURN does not support whole lookup node projection"
                        .to_string(),
                ));
            }
        }
        if !return_expression_is_scoped(&item.expression, &post_lookup_scope, &column_names) {
            return Err(SkeinError::Semantic(format!(
                "unknown WITH aggregate return expression {:?}",
                item.expression
            )));
        }
    }
    validate_with_alias_filter(
        query.aggregate_with_filter.as_ref(),
        &post_lookup_scope,
        &column_names,
    )?;
    if let Some(lookup) = &query.post_with_match {
        if !column_names.contains(&lookup.column) {
            return Err(SkeinError::Semantic(format!(
                "unknown post-WITH MATCH lookup column '{}'",
                lookup.column
            )));
        }
    }
    Ok(())
}

fn optional_with_as_aggregate(
    optional_with: &crate::cypher::OptionalWithAggregate,
) -> WithAggregateProjection {
    WithAggregateProjection {
        items: vec![
            ReturnItem {
                expression: ReturnExpression::Variable(optional_with.group_variable.clone()),
                alias: None,
            },
            ReturnItem {
                expression: ReturnExpression::CountVariable {
                    variable: optional_with.count_variable.clone(),
                    distinct: optional_with.distinct,
                },
                alias: Some(optional_with.alias.clone()),
            },
        ],
    }
}

fn validate_with_alias_filter(
    filter: Option<&WithAliasFilter>,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    validate_with_alias_filter_node(filter, scope, column_names)
}

fn validate_with_alias_filter_node(
    filter: &WithAliasFilter,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    match filter {
        WithAliasFilter::And(filters) | WithAliasFilter::Or(filters) => {
            for filter in filters {
                validate_with_alias_filter_node(filter, scope, column_names)?;
            }
            Ok(())
        }
        WithAliasFilter::Comparison { left, right, .. } => {
            validate_with_alias_filter_expression(left, scope, column_names)?;
            validate_with_alias_filter_expression(right, scope, column_names)
        }
    }
}

fn validate_with_alias_filter_expression(
    expression: &WithAliasFilterExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> Result<()> {
    match expression {
        WithAliasFilterExpression::Column(column) => {
            if column_names.contains(column) {
                Ok(())
            } else {
                Err(SkeinError::Semantic(format!(
                    "unknown WITH filter column '{column}'"
                )))
            }
        }
        WithAliasFilterExpression::Property { variable, .. } => {
            if scope.contains(variable) || column_names.contains(variable) {
                Ok(())
            } else {
                Err(SkeinError::Semantic(format!(
                    "unknown WITH filter variable '{variable}'"
                )))
            }
        }
        WithAliasFilterExpression::Value(_) => Ok(()),
    }
}

fn return_expression_is_scoped(
    expression: &ReturnExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> bool {
    match expression {
        ReturnExpression::Variable(variable) => {
            column_names.contains(variable) || scope.contains(variable)
        }
        ReturnExpression::Property { variable, .. } => {
            column_names.contains(variable) || scope.contains(variable)
        }
        ReturnExpression::Value(_) => true,
        ReturnExpression::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| return_value_expression_is_scoped(expression, scope, column_names)),
        ReturnExpression::Left { expression, .. } | ReturnExpression::Lower(expression) => {
            return_value_expression_is_scoped(expression, scope, column_names)
        }
        ReturnExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnExpression::DefaultIfNull { variable, .. }
        | ReturnExpression::CasePropertyNotNullOrEq { variable, .. }
        | ReturnExpression::CasePropertyEqualsRank { variable, .. }
        | ReturnExpression::CaseLowerPropertyDefault { variable, .. }
        | ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            column_names.contains(variable) || scope.contains(variable)
        }
        ReturnExpression::CaseEntitySearchRank(expression) => {
            column_names.contains(&expression.variable) || scope.contains(&expression.variable)
        }
        ReturnExpression::CaseColumnSearchRank(expression) => {
            column_names.contains(&expression.column)
        }
        ReturnExpression::Id(_)
        | ReturnExpression::RelationshipType(_)
        | ReturnExpression::DatePart { .. }
        | ReturnExpression::CountAll
        | ReturnExpression::CountVariable { .. }
        | ReturnExpression::CountProperty { .. }
        | ReturnExpression::CollectVariable { .. }
        | ReturnExpression::CollectProperty { .. }
        | ReturnExpression::MinProperty { .. }
        | ReturnExpression::MaxProperty { .. }
        | ReturnExpression::AvgProperty { .. } => false,
    }
}

fn return_value_expression_is_scoped(
    expression: &ReturnValueExpression,
    scope: &BTreeSet<String>,
    column_names: &BTreeSet<String>,
) -> bool {
    match expression {
        ReturnValueExpression::Variable(variable)
        | ReturnValueExpression::Property { variable, .. }
        | ReturnValueExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnValueExpression::DefaultIfNull { variable, .. }
        | ReturnValueExpression::CasePropertyNotNullOrEq { variable, .. }
        | ReturnValueExpression::CasePropertyEqualsRank { variable, .. }
        | ReturnValueExpression::CaseLowerPropertyDefault { variable, .. }
        | ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            column_names.contains(variable) || scope.contains(variable)
        }
        ReturnValueExpression::CaseEntitySearchRank(expression) => {
            column_names.contains(&expression.variable) || scope.contains(&expression.variable)
        }
        ReturnValueExpression::CaseColumnSearchRank(expression) => {
            column_names.contains(&expression.column)
        }
        ReturnValueExpression::Value(_) => true,
        ReturnValueExpression::Coalesce(expressions) => expressions
            .iter()
            .all(|expression| return_value_expression_is_scoped(expression, scope, column_names)),
        ReturnValueExpression::Left { expression, .. }
        | ReturnValueExpression::Lower(expression) => {
            return_value_expression_is_scoped(expression, scope, column_names)
        }
        ReturnValueExpression::Id(_)
        | ReturnValueExpression::RelationshipType(_)
        | ReturnValueExpression::DatePart { .. } => false,
    }
}

fn plan_aggregate_with_match_return(
    mut input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    aggregate_with: &WithAggregateProjection,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let planned_with = plan_return_items(scope, &aggregate_with.items, parameters)?;
    let column_names = planned_with.names().into_iter().collect::<BTreeSet<_>>();
    input = planned_with.into_logical(input);
    if let Some(filter) = &query.aggregate_with_filter {
        input = LogicalPlan::Filter {
            predicate: plan_with_alias_filter(filter, parameters)?,
            input: Box::new(input),
        };
    }
    if !query.order_by.is_empty() {
        input = LogicalPlan::Sort {
            items: plan_sort_items(&BTreeSet::new(), &column_names, &query.order_by, parameters)?,
            input: Box::new(input),
        };
    }
    let offset = query
        .offset
        .as_ref()
        .map(|offset| bind_pagination_value(offset, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = query
        .limit
        .as_ref()
        .map(|limit| bind_pagination_value(limit, parameters, "limit"))
        .transpose()?;
    if let Some(lookup) = &query.post_with_match {
        if offset > 0 || limit.is_some() {
            input = LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(input),
            };
        }
        input = plan_post_with_node_lookup(input, lookup);
        let lookup_scope = BTreeSet::from([lookup.variable.clone()]);
        let projections = query
            .returns
            .iter()
            .map(|item| {
                plan_projection_with_columns(&lookup_scope, &column_names, item, parameters)
            })
            .collect::<Result<Vec<_>>>()?;
        return Ok(LogicalPlan::Project {
            items: projections,
            input: Box::new(input),
        });
    }
    let projections = query
        .returns
        .iter()
        .map(|item| plan_projection_with_columns(&BTreeSet::new(), &column_names, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    input = LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    };
    if offset > 0 || limit.is_some() {
        input = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    }
    Ok(input)
}

fn plan_post_with_node_lookup(input: LogicalPlan, lookup: &PostWithNodeLookup) -> LogicalPlan {
    LogicalPlan::NodeColumnLookup {
        variable: lookup.variable.clone(),
        label: lookup.label.clone(),
        property: lookup.property.clone(),
        column: lookup.column.clone(),
        optional: lookup.optional,
        input: Box::new(input),
    }
}

fn optional_direct_count_alias(
    query: &MatchReturn,
    optional: &crate::cypher::OptionalRelationshipExpand,
) -> Result<Option<String>> {
    let mut count_alias = None;
    let mut has_projection = false;
    for item in &query.returns {
        match &item.expression {
            ReturnExpression::CountVariable { variable, distinct } => {
                if *distinct || count_alias.is_some() {
                    return Ok(None);
                }
                let count_matches_relationship =
                    optional.expand.variable.as_deref() == Some(variable);
                let count_matches_target = optional.expand.target_variable == *variable;
                if !count_matches_relationship && !count_matches_target {
                    return Ok(None);
                }
                count_alias = Some(
                    item.alias
                        .clone()
                        .unwrap_or_else(|| format!("count({variable})")),
                );
            }
            ReturnExpression::Variable(variable) => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Property { variable, .. }
            | ReturnExpression::Id(variable)
            | ReturnExpression::RelationshipType(variable) => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Value(_) => {
                has_projection = true;
            }
            ReturnExpression::Coalesce(expressions) => {
                if !return_value_expressions_are_source_only(expressions, &optional.source_variable)
                {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Left { expression, .. } | ReturnExpression::Lower(expression) => {
                if !return_value_expression_is_source_only(expression, &optional.source_variable) {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::DatePart { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::DefaultIfNullOrEq { variable, .. }
            | ReturnExpression::DefaultIfNull { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CasePropertyNotNullOrEq { variable, .. }
            | ReturnExpression::CasePropertyEqualsRank { variable, .. }
            | ReturnExpression::CaseLowerPropertyDefault { variable, .. }
            | ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CaseEntitySearchRank(expression) => {
                if expression.variable != optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CaseColumnSearchRank(_) => return Ok(None),
            ReturnExpression::CountAll
            | ReturnExpression::CountProperty { .. }
            | ReturnExpression::CollectVariable { .. }
            | ReturnExpression::CollectProperty { .. }
            | ReturnExpression::MinProperty { .. }
            | ReturnExpression::MaxProperty { .. }
            | ReturnExpression::AvgProperty { .. } => return Ok(None),
        }
    }
    Ok(if has_projection { count_alias } else { None })
}

fn optional_direct_collect_alias(
    query: &MatchReturn,
    optional: &crate::cypher::OptionalRelationshipExpand,
) -> Result<Option<String>> {
    let mut collect_alias = None;
    let mut has_projection = false;
    for item in &query.returns {
        match &item.expression {
            ReturnExpression::CollectProperty {
                variable,
                property,
                distinct,
            } => {
                if collect_alias.is_some() || variable != &optional.expand.target_variable {
                    return Ok(None);
                }
                collect_alias = Some(item.alias.clone().unwrap_or_else(|| {
                    if *distinct {
                        format!("collect(DISTINCT {variable}.{property})")
                    } else {
                        format!("collect({variable}.{property})")
                    }
                }));
            }
            ReturnExpression::Variable(variable) => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Property { variable, .. }
            | ReturnExpression::Id(variable)
            | ReturnExpression::RelationshipType(variable) => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Value(_) => {
                has_projection = true;
            }
            ReturnExpression::Coalesce(expressions) => {
                if !return_value_expressions_are_source_only(expressions, &optional.source_variable)
                {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::Left { expression, .. } | ReturnExpression::Lower(expression) => {
                if !return_value_expression_is_source_only(expression, &optional.source_variable) {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::DatePart { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::DefaultIfNullOrEq { variable, .. }
            | ReturnExpression::DefaultIfNull { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CasePropertyNotNullOrEq { variable, .. }
            | ReturnExpression::CasePropertyEqualsRank { variable, .. }
            | ReturnExpression::CaseLowerPropertyDefault { variable, .. }
            | ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
                if variable != &optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CaseEntitySearchRank(expression) => {
                if expression.variable != optional.source_variable {
                    return Ok(None);
                }
                has_projection = true;
            }
            ReturnExpression::CaseColumnSearchRank(_) => return Ok(None),
            ReturnExpression::CountAll
            | ReturnExpression::CountVariable { .. }
            | ReturnExpression::CountProperty { .. }
            | ReturnExpression::CollectVariable { .. }
            | ReturnExpression::MinProperty { .. }
            | ReturnExpression::MaxProperty { .. }
            | ReturnExpression::AvgProperty { .. } => return Ok(None),
        }
    }
    Ok(if has_projection { collect_alias } else { None })
}

fn optional_direct_row_projection(
    query: &MatchReturn,
    optional: &crate::cypher::OptionalRelationshipExpand,
) -> bool {
    query.returns.iter().all(|item| {
        optional_direct_row_projection_expression(
            &item.expression,
            &optional.source_variable,
            &optional.expand.target_variable,
            optional.expand.variable.as_deref(),
        )
    })
}

fn optional_direct_row_projection_expression(
    expression: &ReturnExpression,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    match expression {
        ReturnExpression::Variable(variable)
        | ReturnExpression::Property { variable, .. }
        | ReturnExpression::Id(variable)
        | ReturnExpression::RelationshipType(variable)
        | ReturnExpression::DatePart { variable, .. }
        | ReturnExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnExpression::DefaultIfNull { variable, .. }
        | ReturnExpression::CasePropertyNotNullOrEq { variable, .. }
        | ReturnExpression::CasePropertyEqualsRank { variable, .. }
        | ReturnExpression::CaseLowerPropertyDefault { variable, .. }
        | ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            optional_direct_row_projection_variable(
                variable,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ReturnExpression::CaseEntitySearchRank(expression) => {
            optional_direct_row_projection_variable(
                &expression.variable,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ReturnExpression::CaseColumnSearchRank(_) => false,
        ReturnExpression::Value(_) => true,
        ReturnExpression::Coalesce(expressions) => expressions.iter().all(|expression| {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }),
        ReturnExpression::Left { expression, .. } | ReturnExpression::Lower(expression) => {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ReturnExpression::CountAll
        | ReturnExpression::CountVariable { .. }
        | ReturnExpression::CountProperty { .. }
        | ReturnExpression::CollectVariable { .. }
        | ReturnExpression::CollectProperty { .. }
        | ReturnExpression::MinProperty { .. }
        | ReturnExpression::MaxProperty { .. }
        | ReturnExpression::AvgProperty { .. } => false,
    }
}

fn optional_direct_row_projection_value_expression(
    expression: &ReturnValueExpression,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    match expression {
        ReturnValueExpression::Variable(variable)
        | ReturnValueExpression::Property { variable, .. }
        | ReturnValueExpression::Id(variable)
        | ReturnValueExpression::RelationshipType(variable)
        | ReturnValueExpression::DatePart { variable, .. }
        | ReturnValueExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnValueExpression::DefaultIfNull { variable, .. }
        | ReturnValueExpression::CasePropertyNotNullOrEq { variable, .. }
        | ReturnValueExpression::CasePropertyEqualsRank { variable, .. }
        | ReturnValueExpression::CaseLowerPropertyDefault { variable, .. }
        | ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            optional_direct_row_projection_variable(
                variable,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ReturnValueExpression::CaseEntitySearchRank(expression) => {
            optional_direct_row_projection_variable(
                &expression.variable,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
        ReturnValueExpression::CaseColumnSearchRank(_) => false,
        ReturnValueExpression::Value(_) => true,
        ReturnValueExpression::Coalesce(expressions) => expressions.iter().all(|expression| {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }),
        ReturnValueExpression::Left { expression, .. }
        | ReturnValueExpression::Lower(expression) => {
            optional_direct_row_projection_value_expression(
                expression,
                source_variable,
                target_variable,
                rel_variable,
            )
        }
    }
}

fn optional_direct_row_projection_variable(
    variable: &str,
    source_variable: &str,
    target_variable: &str,
    rel_variable: Option<&str>,
) -> bool {
    variable == source_variable || variable == target_variable || rel_variable == Some(variable)
}

fn plan_optional_direct_count_return(
    mut input: LogicalPlan,
    scope: &BTreeSet<String>,
    query: &MatchReturn,
    optional: &crate::cypher::OptionalRelationshipExpand,
    count_alias: String,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    input = LogicalPlan::OptionalDegree {
        source_variable: optional.source_variable.clone(),
        rel_type: optional.expand.rel_type.clone(),
        rel_properties: bind_properties(&optional.expand.properties, parameters)?,
        direction: optional.expand.direction,
        target_label: optional.expand.target_label.clone(),
        target_properties: bind_properties(&optional.expand.target_properties, parameters)?,
        alias: count_alias.clone(),
        input: Box::new(input),
    };
    let projections = query
        .returns
        .iter()
        .map(|item| plan_optional_direct_count_projection(scope, item, &count_alias, parameters))
        .collect::<Result<Vec<_>>>()?;
    let projection_names = projections
        .iter()
        .map(|projection| projection.name.clone())
        .collect::<BTreeSet<_>>();
    input = LogicalPlan::Project {
        items: projections,
        input: Box::new(input),
    };
    if query.distinct {
        input = LogicalPlan::Distinct {
            input: Box::new(input),
        };
    }
    if !query.order_by.is_empty() {
        input = LogicalPlan::Sort {
            items: plan_sort_items(scope, &projection_names, &query.order_by, parameters)?,
            input: Box::new(input),
        };
    }
    let offset = query
        .offset
        .as_ref()
        .map(|offset| bind_pagination_value(offset, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = query
        .limit
        .as_ref()
        .map(|limit| bind_pagination_value(limit, parameters, "limit"))
        .transpose()?;
    if offset > 0 || limit.is_some() {
        input = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(input),
        };
    }
    Ok(input)
}

fn plan_optional_direct_count_projection(
    scope: &BTreeSet<String>,
    item: &ReturnItem,
    count_alias: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    if matches!(item.expression, ReturnExpression::CountVariable { .. }) {
        return Ok(Projection {
            expression: ProjectionExpression::Column(count_alias.to_string()),
            name: item
                .alias
                .clone()
                .unwrap_or_else(|| count_alias.to_string()),
        });
    }
    plan_projection_with_columns(scope, &BTreeSet::new(), item, parameters)
}

fn return_value_expressions_are_source_only(
    expressions: &[ReturnValueExpression],
    source_variable: &str,
) -> bool {
    expressions
        .iter()
        .all(|expression| return_value_expression_is_source_only(expression, source_variable))
}

fn return_value_expression_is_source_only(
    expression: &ReturnValueExpression,
    source_variable: &str,
) -> bool {
    match expression {
        ReturnValueExpression::Variable(variable)
        | ReturnValueExpression::Property { variable, .. }
        | ReturnValueExpression::Id(variable)
        | ReturnValueExpression::RelationshipType(variable)
        | ReturnValueExpression::DatePart { variable, .. }
        | ReturnValueExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnValueExpression::DefaultIfNull { variable, .. }
        | ReturnValueExpression::CasePropertyNotNullOrEq { variable, .. }
        | ReturnValueExpression::CasePropertyEqualsRank { variable, .. }
        | ReturnValueExpression::CaseLowerPropertyDefault { variable, .. }
        | ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            variable == source_variable
        }
        ReturnValueExpression::CaseEntitySearchRank(expression) => {
            expression.variable == source_variable
        }
        ReturnValueExpression::CaseColumnSearchRank(_) => false,
        ReturnValueExpression::Value(_) => true,
        ReturnValueExpression::Coalesce(expressions) => {
            return_value_expressions_are_source_only(expressions, source_variable)
        }
        ReturnValueExpression::Left { expression, .. }
        | ReturnValueExpression::Lower(expression) => {
            return_value_expression_is_source_only(expression, source_variable)
        }
    }
}

fn aggregate_with_column_names(aggregate_with: &WithAggregateProjection) -> BTreeSet<String> {
    aggregate_with
        .items
        .iter()
        .map(|item| {
            item.alias
                .clone()
                .unwrap_or_else(|| match &item.expression {
                    ReturnExpression::Variable(variable) => variable.clone(),
                    ReturnExpression::Property { variable, property } => {
                        format!("{variable}.{property}")
                    }
                    ReturnExpression::Value(_) => "literal".to_string(),
                    ReturnExpression::DatePart {
                        part,
                        variable,
                        property,
                    } => format!("date_part({part}, {variable}.{property})"),
                    ReturnExpression::CountAll => "count(*)".to_string(),
                    ReturnExpression::CountVariable { variable, distinct } if *distinct => {
                        format!("count(DISTINCT {variable})")
                    }
                    ReturnExpression::CountVariable { variable, .. } => {
                        format!("count({variable})")
                    }
                    ReturnExpression::CountProperty {
                        variable,
                        property,
                        distinct,
                    } if *distinct => format!("count(DISTINCT {variable}.{property})"),
                    ReturnExpression::CountProperty {
                        variable, property, ..
                    } => format!("count({variable}.{property})"),
                    ReturnExpression::CollectVariable { variable, distinct } if *distinct => {
                        format!("collect(DISTINCT {variable})")
                    }
                    ReturnExpression::CollectVariable { variable, .. } => {
                        format!("collect({variable})")
                    }
                    ReturnExpression::CollectProperty {
                        variable,
                        property,
                        distinct,
                    } if *distinct => format!("collect(DISTINCT {variable}.{property})"),
                    ReturnExpression::CollectProperty {
                        variable, property, ..
                    } => format!("collect({variable}.{property})"),
                    _ => String::new(),
                })
        })
        .collect()
}

fn is_aggregate_return_expression(expression: &ReturnExpression) -> bool {
    matches!(
        expression,
        ReturnExpression::CountAll
            | ReturnExpression::CountVariable { .. }
            | ReturnExpression::CountProperty { .. }
            | ReturnExpression::CollectVariable { .. }
            | ReturnExpression::CollectProperty { .. }
            | ReturnExpression::MinProperty { .. }
            | ReturnExpression::MaxProperty { .. }
            | ReturnExpression::AvgProperty { .. }
    )
}

fn plan_with_alias_filter(
    filter: &WithAliasFilter,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    Ok(match filter {
        WithAliasFilter::And(filters) => Predicate::And(
            filters
                .iter()
                .map(|filter| plan_with_alias_filter(filter, parameters))
                .collect::<Result<Vec<_>>>()?,
        ),
        WithAliasFilter::Or(filters) => Predicate::Or(
            filters
                .iter()
                .map(|filter| plan_with_alias_filter(filter, parameters))
                .collect::<Result<Vec<_>>>()?,
        ),
        WithAliasFilter::Comparison { left, op, right } => {
            let expression = plan_with_alias_filter_expression(left, parameters)?;
            let value = plan_with_alias_filter_expression(right, parameters)?;
            match op {
                WithAliasFilterOp::Eq => Predicate::ExpressionEq { expression, value },
                WithAliasFilterOp::Ne => Predicate::ExpressionNotEq { expression, value },
                WithAliasFilterOp::Lt => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Lt,
                    value,
                },
                WithAliasFilterOp::Lte => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Lte,
                    value,
                },
                WithAliasFilterOp::Gt => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Gt,
                    value,
                },
                WithAliasFilterOp::Gte => Predicate::ExpressionCompare {
                    expression,
                    op: ComparisonOp::Gte,
                    value,
                },
                WithAliasFilterOp::Contains => Predicate::ExpressionContains { expression, value },
            }
        }
    })
}

fn plan_with_alias_filter_expression(
    expression: &WithAliasFilterExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    Ok(match expression {
        WithAliasFilterExpression::Column(column) => ProjectionExpression::Column(column.clone()),
        WithAliasFilterExpression::Property { variable, property } => {
            ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            }
        }
        WithAliasFilterExpression::Value(value) => {
            ProjectionExpression::Literal(bind_value(value, parameters)?)
        }
    })
}

fn plan_schema_property_type(value_type: CypherSchemaPropertyType) -> SchemaPropertyType {
    match value_type {
        CypherSchemaPropertyType::Any => SchemaPropertyType::Any,
        CypherSchemaPropertyType::Bool => SchemaPropertyType::Bool,
        CypherSchemaPropertyType::Int => SchemaPropertyType::Int,
        CypherSchemaPropertyType::Float => SchemaPropertyType::Float,
        CypherSchemaPropertyType::String => SchemaPropertyType::String,
        CypherSchemaPropertyType::List => SchemaPropertyType::List,
    }
}

fn plan_schema_object_state(state: CypherSchemaObjectState) -> SchemaObjectState {
    match state {
        CypherSchemaObjectState::DeleteOnly => SchemaObjectState::DeleteOnly,
        CypherSchemaObjectState::WriteOnly => SchemaObjectState::WriteOnly,
        CypherSchemaObjectState::Backfill => SchemaObjectState::Backfill,
        CypherSchemaObjectState::Validating => SchemaObjectState::Validating,
        CypherSchemaObjectState::Public => SchemaObjectState::Public,
        CypherSchemaObjectState::Gc => SchemaObjectState::Gc,
    }
}

fn plan_graph_algorithm_kind(kind: CypherGraphAlgorithmKind) -> GraphAlgorithmKind {
    match kind {
        CypherGraphAlgorithmKind::PageRank => GraphAlgorithmKind::PageRank,
        CypherGraphAlgorithmKind::Louvain => GraphAlgorithmKind::Louvain,
    }
}

fn bind_graph_algorithm_options(
    options: &CypherGraphAlgorithmOptions,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphAlgorithmOptions> {
    Ok(GraphAlgorithmOptions {
        damping: options
            .damping
            .as_ref()
            .map(|value| bind_optional_f64(value, parameters, "damping"))
            .transpose()?,
        max_iterations: options
            .max_iterations
            .as_ref()
            .map(|value| bind_non_negative_usize(value, parameters, "maxIterations"))
            .transpose()?,
        max_levels: options
            .max_levels
            .as_ref()
            .map(|value| bind_non_negative_usize(value, parameters, "maxLevels"))
            .transpose()?,
    })
}

fn bind_properties(
    properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    properties
        .iter()
        .map(|(name, value)| Ok((name.clone(), bind_value(value, parameters)?)))
        .collect()
}

fn bind_relationship_count_legs(
    legs: &[crate::cypher::OptionalRelationshipCountLeg],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<RelationshipCountLeg>> {
    legs.iter()
        .map(|leg| {
            Ok(RelationshipCountLeg {
                rel_type: leg.rel_type.clone(),
                direction: leg.direction,
                distinct: leg.distinct,
                filter: leg
                    .filter
                    .as_ref()
                    .map(|filter| bind_relationship_count_filter(filter, parameters))
                    .transpose()?,
            })
        })
        .collect()
}

fn bind_relationship_count_filter(
    filter: &crate::cypher::OptionalRelationshipCountFilter,
    parameters: &BTreeMap<String, Value>,
) -> Result<RelationshipCountFilter> {
    match filter {
        crate::cypher::OptionalRelationshipCountFilter::PropertyNotEqOrEmpty {
            property,
            value,
        } => Ok(RelationshipCountFilter::PropertyNotEqOrEmpty {
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
    }
}

fn bind_on_create_set_properties(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE ON CREATE SET requires a bound node variable".to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != variable {
            return Err(SkeinError::Semantic(format!(
                "MERGE ON CREATE SET variable '{}' does not match bound variable '{variable}'",
                set.variable
            )));
        }
        let SetValueExpression::Value(value) = &set.value else {
            return Err(SkeinError::Semantic(
                "MERGE ON CREATE SET supports only value assignments".to_string(),
            ));
        };
        properties.insert(set.property.clone(), bind_value(value, parameters)?);
    }
    Ok(properties)
}

fn bind_on_match_set_assignments(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SetAssignment>> {
    if sets.is_empty() {
        return Ok(Vec::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE ON MATCH SET requires a bound node variable".to_string(),
        ));
    };
    sets.iter()
        .map(|set| {
            if set.variable != variable {
                return Err(SkeinError::Semantic(format!(
                    "MERGE ON MATCH SET variable '{}' does not match bound variable '{variable}'",
                    set.variable
                )));
            }
            Ok(SetAssignment {
                property: set.property.clone(),
                value: plan_set_value(set, parameters)?,
            })
        })
        .collect()
}

fn bind_post_merge_set_assignments(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SetAssignment>> {
    if sets.is_empty() {
        return Ok(Vec::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "MERGE SET requires a bound node variable".to_string(),
        ));
    };
    sets.iter()
        .map(|set| {
            if set.variable != variable {
                return Err(SkeinError::Semantic(format!(
                    "MERGE SET variable '{}' does not match bound variable '{variable}'",
                    set.variable
                )));
            }
            Ok(SetAssignment {
                property: set.property.clone(),
                value: plan_set_value(set, parameters)?,
            })
        })
        .collect()
}

fn bind_relationship_on_create_set_properties(
    variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(variable) = variable else {
        return Err(SkeinError::Semantic(
            "relationship MERGE ON CREATE SET requires a bound relationship variable".to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != variable {
            return Err(SkeinError::Semantic(format!(
                "relationship MERGE ON CREATE SET variable '{}' does not match bound relationship variable '{variable}'",
                set.variable
            )));
        }
        let SetValueExpression::Value(value) = &set.value else {
            return Err(SkeinError::Semantic(
                "relationship MERGE ON CREATE SET supports only value assignments".to_string(),
            ));
        };
        properties.insert(set.property.clone(), bind_value(value, parameters)?);
    }
    Ok(properties)
}

fn bind_relationship_copy_on_create_set_properties(
    new_variable: Option<&str>,
    old_variable: Option<&str>,
    sets: &[SetProperty],
    parameters: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, RelationshipOnCreateValue>> {
    if sets.is_empty() {
        return Ok(BTreeMap::new());
    }
    let Some(new_variable) = new_variable else {
        return Err(SkeinError::Semantic(
            "relationship-copy MERGE ON CREATE SET requires a bound new relationship variable"
                .to_string(),
        ));
    };
    let Some(old_variable) = old_variable else {
        return Err(SkeinError::Semantic(
            "relationship-copy MERGE ON CREATE SET requires a bound matched relationship variable"
                .to_string(),
        ));
    };
    let mut properties = BTreeMap::new();
    for set in sets {
        if set.variable != new_variable {
            return Err(SkeinError::Semantic(format!(
                "relationship-copy MERGE ON CREATE SET variable '{}' does not match bound relationship variable '{new_variable}'",
                set.variable
            )));
        }
        let value = match &set.value {
            SetValueExpression::Value(value) => {
                RelationshipOnCreateValue::Value(bind_value(value, parameters)?)
            }
            SetValueExpression::Property { variable, property } if variable == old_variable => {
                RelationshipOnCreateValue::MatchedRelationshipProperty {
                    property: property.clone(),
                }
            }
            SetValueExpression::Property { variable, .. } => {
                return Err(SkeinError::Semantic(format!(
                    "relationship-copy MERGE ON CREATE SET cannot read property from variable '{variable}'"
                )));
            }
            SetValueExpression::PropertyAdd { .. }
            | SetValueExpression::CoalesceProperty { .. }
            | SetValueExpression::DecrementFloorZero { .. }
            | SetValueExpression::PreserveNewerExisting { .. }
            | SetValueExpression::CoalescePropertyAdd { .. } => {
                return Err(SkeinError::Semantic(
                    "relationship-copy MERGE ON CREATE SET supports only values and matched relationship properties"
                        .to_string(),
                ));
            }
        };
        properties.insert(set.property.clone(), value);
    }
    Ok(properties)
}

fn plan_match_pattern_predicate(
    source_variable: &str,
    source_properties: &BTreeMap<String, ValueExpression>,
    expand: Option<&CypherRelationshipExpand>,
    post_expand: Option<&crate::cypher::PostMatchRelationshipExpand>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let mut predicates =
        plan_node_pattern_predicates(source_variable, source_properties, parameters)?;
    if let Some(expand) = expand {
        predicates.extend(plan_node_pattern_predicates(
            &expand.target_variable,
            &expand.target_properties,
            parameters,
        )?);
    }
    if let Some(post_expand) = post_expand {
        predicates.extend(plan_node_pattern_predicates(
            &post_expand.source_variable,
            &post_expand.source_properties,
            parameters,
        )?);
        predicates.extend(plan_node_pattern_predicates(
            &post_expand.expand.target_variable,
            &post_expand.expand.target_properties,
            parameters,
        )?);
    }
    Ok(combine_predicates(predicates))
}

fn plan_node_pattern_predicates(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<Predicate>> {
    bind_properties(properties, parameters).map(|properties| {
        properties
            .into_iter()
            .map(|(property, value)| Predicate::PropertyEq {
                variable: variable.to_string(),
                property,
                value,
            })
            .collect()
    })
}

fn bind_two_node_relationship_create_filters(
    source_variable: &str,
    source_properties: &BTreeMap<String, ValueExpression>,
    target_variable: &str,
    target_properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<&PropertyPredicate>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(BTreeMap<String, Value>, BTreeMap<String, Value>)> {
    let mut source = bind_properties(source_properties, parameters)?;
    let mut target = bind_properties(target_properties, parameters)?;
    if let Some(predicate) = predicate {
        bind_endpoint_equality_predicate(
            predicate,
            source_variable,
            &mut source,
            target_variable,
            &mut target,
            parameters,
        )?;
    }
    Ok((source, target))
}

fn bind_endpoint_equality_predicate(
    predicate: &PropertyPredicate,
    source_variable: &str,
    source: &mut BTreeMap<String, Value>,
    target_variable: &str,
    target: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                bind_endpoint_equality_predicate(
                    predicate,
                    source_variable,
                    source,
                    target_variable,
                    target,
                    parameters,
                )?;
            }
            Ok(())
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == source_variable => {
            insert_endpoint_property(source, property, bind_value(value, parameters)?)
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == target_variable => {
            insert_endpoint_property(target, property, bind_value(value, parameters)?)
        }
        _ => Err(SkeinError::Semantic(
            "two-node relationship CREATE supports only AND-connected equality predicates on matched node properties".to_string(),
        )),
    }
}

fn insert_endpoint_property(
    properties: &mut BTreeMap<String, Value>,
    property: &str,
    value: Value,
) -> Result<()> {
    if let Some(existing) = properties.get(property) {
        if existing != &value {
            return Err(SkeinError::Semantic(format!(
                "conflicting equality predicates for matched node property '{property}'"
            )));
        }
        return Ok(());
    }
    properties.insert(property.to_string(), value);
    Ok(())
}

fn combine_pattern_and_optional_cypher_predicate(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<&PropertyPredicate>,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let pattern_predicate =
        plan_match_pattern_predicate(variable, properties, None, None, parameters)?;
    combine_pattern_and_optional_cypher_predicate_parts(
        pattern_predicate,
        predicate,
        scope,
        parameters,
    )
}

fn combine_pattern_and_optional_cypher_predicate_parts(
    pattern_predicate: Option<Predicate>,
    predicate: Option<&PropertyPredicate>,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let planned_predicate = predicate
        .map(|predicate| plan_predicate(predicate, scope, parameters))
        .transpose()?;
    Ok(combine_optional_predicates(
        pattern_predicate,
        planned_predicate,
    ))
}

fn combine_pattern_and_optional_predicate(
    variable: &str,
    properties: &BTreeMap<String, ValueExpression>,
    predicate: Option<Predicate>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Predicate>> {
    let pattern_predicate =
        plan_match_pattern_predicate(variable, properties, None, None, parameters)?;
    Ok(combine_optional_predicates(pattern_predicate, predicate))
}

fn combine_optional_predicates(
    left: Option<Predicate>,
    right: Option<Predicate>,
) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(Predicate::And(vec![left, right])),
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (None, None) => None,
    }
}

fn pushdown_relationship_property_eq_predicates(
    input: &mut LogicalPlan,
    predicate: Predicate,
) -> Option<Predicate> {
    let predicates = match predicate {
        Predicate::And(predicates) => predicates,
        predicate => vec![predicate],
    };
    let mut residual = Vec::new();
    for predicate in predicates {
        if !try_pushdown_relationship_property_eq(input, &predicate) {
            residual.push(predicate);
        }
    }
    combine_predicates(residual)
}

fn try_pushdown_relationship_property_eq(input: &mut LogicalPlan, predicate: &Predicate) -> bool {
    let Predicate::PropertyEq {
        variable,
        property,
        value,
    } = predicate
    else {
        return false;
    };
    match input {
        LogicalPlan::Expand {
            rel_variable: Some(rel_variable),
            rel_properties,
            min_hops,
            max_hops,
            ..
        } if rel_variable == variable && *min_hops == 1 && *max_hops == 1 => {
            if let Some(existing) = rel_properties.get(property) {
                existing == value
            } else {
                rel_properties.insert(property.clone(), value.clone());
                true
            }
        }
        LogicalPlan::Expand { input, .. } => {
            try_pushdown_relationship_property_eq(input, predicate)
        }
        _ => false,
    }
}

fn bind_value(expression: &ValueExpression, parameters: &BTreeMap<String, Value>) -> Result<Value> {
    match expression {
        ValueExpression::Literal(value) => Ok(value.clone()),
        ValueExpression::Parameter(name) => parameters
            .get(name)
            .cloned()
            .ok_or_else(|| SkeinError::Semantic(format!("missing parameter '${name}'"))),
        ValueExpression::List(values) => values
            .iter()
            .map(|value| bind_value(value, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        ValueExpression::CurrentTimestamp => Ok(current_timestamp_value()),
        ValueExpression::Timestamp(value) => {
            bind_value(value, parameters).and_then(timestamp_value)
        }
    }
}

fn current_timestamp_value() -> Value {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0);
    Value::Int(nanos)
}

fn timestamp_value(value: Value) -> Result<Value> {
    match value {
        Value::Int(value) => Ok(Value::Int(scale_epoch_integer_to_nanos(value))),
        Value::Float(value) if value.is_finite() => {
            let nanos = (value * 1_000_000_000.0).round();
            if nanos < i64::MIN as f64 || nanos > i64::MAX as f64 {
                return Err(SkeinError::Semantic(format!(
                    "timestamp() value is out of range: {value}"
                )));
            }
            Ok(Value::Int(nanos as i64))
        }
        Value::String(value) => parse_timestamp_string(&value).map(Value::Int),
        value => Err(SkeinError::Semantic(format!(
            "timestamp() expects an ISO string or numeric epoch, got {value:?}"
        ))),
    }
}

fn scale_epoch_integer_to_nanos(value: i64) -> i64 {
    let abs = value.unsigned_abs();
    let multiplier = if abs < 100_000_000_000 {
        1_000_000_000
    } else if abs < 100_000_000_000_000 {
        1_000_000
    } else if abs < 100_000_000_000_000_000 {
        1_000
    } else {
        1
    };
    value.saturating_mul(multiplier)
}

fn parse_timestamp_string(input: &str) -> Result<i64> {
    let input = input.trim().trim_end_matches('Z');
    let (date, time) = input
        .split_once('T')
        .or_else(|| input.split_once(' '))
        .ok_or_else(|| {
            SkeinError::Semantic(format!(
                "timestamp() expects YYYY-MM-DDTHH:MM:SS, got '{input}'"
            ))
        })?;
    let (year, month, day) = parse_timestamp_date(date)?;
    let (hour, minute, second, nanos) = parse_timestamp_time(time)?;
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)
        .and_then(|value| value.checked_add((hour as i64) * 3_600))
        .and_then(|value| value.checked_add((minute as i64) * 60))
        .and_then(|value| value.checked_add(second as i64))
        .ok_or_else(|| {
            SkeinError::Semantic(format!("timestamp() value is out of range: '{input}'"))
        })?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos as i64))
        .ok_or_else(|| {
            SkeinError::Semantic(format!("timestamp() value is out of range: '{input}'"))
        })
}

fn parse_timestamp_date(input: &str) -> Result<(i32, u32, u32)> {
    let mut parts = input.split('-');
    let year = parse_timestamp_part::<i32>(parts.next(), "year", input)?;
    let month = parse_timestamp_part::<u32>(parts.next(), "month", input)?;
    let day = parse_timestamp_part::<u32>(parts.next(), "day", input)?;
    if parts.next().is_some() || !(1..=12).contains(&month) {
        return Err(invalid_timestamp_date(input));
    }
    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return Err(invalid_timestamp_date(input));
    }
    Ok((year, month, day))
}

fn parse_timestamp_time(input: &str) -> Result<(u32, u32, u32, u32)> {
    let mut parts = input.split(':');
    let hour = parse_timestamp_part::<u32>(parts.next(), "hour", input)?;
    let minute = parse_timestamp_part::<u32>(parts.next(), "minute", input)?;
    let second_part = parts
        .next()
        .ok_or_else(|| SkeinError::Semantic(format!("invalid timestamp time: '{input}'")))?;
    if parts.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp time: '{input}'"
        )));
    }
    let (second_text, fraction) = second_part
        .split_once('.')
        .map(|(second, fraction)| (second, Some(fraction)))
        .unwrap_or((second_part, None));
    let second = second_text
        .parse::<u32>()
        .map_err(|_| SkeinError::Semantic(format!("invalid timestamp time: '{input}'")))?;
    if hour > 23 || minute > 59 || second > 59 {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp time: '{input}'"
        )));
    }
    let nanos = fraction
        .map(parse_fractional_nanos)
        .transpose()?
        .unwrap_or(0);
    Ok((hour, minute, second, nanos))
}

fn parse_fractional_nanos(input: &str) -> Result<u32> {
    if input.is_empty() || input.len() > 9 || !input.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SkeinError::Semantic(format!(
            "invalid timestamp fractional seconds: '{input}'"
        )));
    }
    let mut nanos = input.parse::<u32>().map_err(|_| {
        SkeinError::Semantic(format!("invalid timestamp fractional seconds: '{input}'"))
    })?;
    for _ in input.len()..9 {
        nanos *= 10;
    }
    Ok(nanos)
}

fn parse_timestamp_part<T>(part: Option<&str>, name: &str, full: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    part.ok_or_else(|| SkeinError::Semantic(format!("invalid timestamp {name}: '{full}'")))?
        .parse::<T>()
        .map_err(|_| SkeinError::Semantic(format!("invalid timestamp {name}: '{full}'")))
}

fn invalid_timestamp_date(input: &str) -> SkeinError {
    SkeinError::Semantic(format!("invalid timestamp date: '{input}'"))
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year as i64 - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn bind_id_value(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Value> {
    match bind_value(expression, parameters)? {
        Value::Int(value) if value >= 0 => Ok(Value::Int(value)),
        value => Err(SkeinError::Semantic(format!(
            "id() predicate requires a non-negative integer value, got {value:?}"
        ))),
    }
}

fn validate_predicate(scope: &BTreeSet<String>, predicate: &PropertyPredicate) -> Result<()> {
    for variable in predicate_variables(predicate) {
        if !scope.contains(&variable) {
            return Err(SkeinError::Semantic(format!(
                "unknown variable '{variable}' in predicate"
            )));
        }
    }
    Ok(())
}

fn plan_predicate(
    predicate: &PropertyPredicate,
    scope: &BTreeSet<String>,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    match predicate {
        PropertyPredicate::And(predicates) => predicates
            .iter()
            .map(|predicate| plan_predicate(predicate, scope, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Predicate::And),
        PropertyPredicate::Or(predicates) => predicates
            .iter()
            .map(|predicate| plan_predicate(predicate, scope, parameters))
            .collect::<Result<Vec<_>>>()
            .map(Predicate::Or),
        PropertyPredicate::Not(predicate) => Ok(Predicate::Not(Box::new(plan_predicate(
            predicate, scope, parameters,
        )?))),
        PropertyPredicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => Ok(Predicate::RelationshipExists {
            variable: variable.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_label: target_label.clone(),
        }),
        PropertyPredicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => Ok(Predicate::BoundRelationshipExists {
            source_variable: source_variable.clone(),
            rel_type: rel_type.clone(),
            direction: *direction,
            target_variable: target_variable.clone(),
        }),
        PropertyPredicate::IdEq { variable, value } => Ok(Predicate::IdEq {
            variable: variable.clone(),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdNotEq { variable, value } => Ok(Predicate::IdNotEq {
            variable: variable.clone(),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdCompare {
            variable,
            op,
            value,
        } => Ok(Predicate::IdCompare {
            variable: variable.clone(),
            op: plan_comparison_op(*op),
            value: bind_id_value(value, parameters)?,
        }),
        PropertyPredicate::IdIn { variable, values } => match bind_value(values, parameters)? {
            Value::List(values) => Ok(Predicate::IdIn {
                variable: variable.clone(),
                values,
            }),
            value => Err(SkeinError::Semantic(format!(
                "IN predicate requires a list value, got {value:?}"
            ))),
        },
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyEq {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::NotEq {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyNotEq {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::Compare {
            variable,
            property,
            op,
            value,
        } => Ok(Predicate::PropertyCompare {
            variable: variable.clone(),
            property: property.clone(),
            op: plan_comparison_op(*op),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::ExpressionEq { expression, value } => Ok(Predicate::ExpressionEq {
            expression: plan_return_value_expression(scope, expression, parameters)?,
            value: plan_return_value_expression(scope, value, parameters)?,
        }),
        PropertyPredicate::ExpressionNotEq { expression, value } => {
            Ok(Predicate::ExpressionNotEq {
                expression: plan_return_value_expression(scope, expression, parameters)?,
                value: plan_return_value_expression(scope, value, parameters)?,
            })
        }
        PropertyPredicate::ExpressionCompare {
            expression,
            op,
            value,
        } => Ok(Predicate::ExpressionCompare {
            expression: plan_return_value_expression(scope, expression, parameters)?,
            op: plan_comparison_op(*op),
            value: plan_return_value_expression(scope, value, parameters)?,
        }),
        PropertyPredicate::ExpressionContains { expression, value } => {
            Ok(Predicate::ExpressionContains {
                expression: plan_return_value_expression(scope, expression, parameters)?,
                value: plan_return_value_expression(scope, value, parameters)?,
            })
        }
        PropertyPredicate::ListContains {
            variable,
            property,
            value,
        } => Ok(Predicate::PropertyListContains {
            variable: variable.clone(),
            property: property.clone(),
            value: bind_value(value, parameters)?,
        }),
        PropertyPredicate::Contains {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyContains {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "CONTAINS predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::StartsWith {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyStartsWith {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "STARTS WITH predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::EndsWith {
            variable,
            property,
            value,
        } => match bind_value(value, parameters)? {
            Value::String(value) => Ok(Predicate::PropertyEndsWith {
                variable: variable.clone(),
                property: property.clone(),
                value,
            }),
            value => Err(SkeinError::Semantic(format!(
                "ENDS WITH predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::RegexMatch {
            variable,
            property,
            pattern,
        } => match bind_value(pattern, parameters)? {
            Value::String(pattern) => {
                crate::regex_cache::validate_regex_pattern(&pattern)?;
                Ok(Predicate::PropertyRegexMatch {
                    variable: variable.clone(),
                    property: property.clone(),
                    pattern,
                })
            }
            value => Err(SkeinError::Semantic(format!(
                "regex match predicate requires a string value, got {value:?}"
            ))),
        },
        PropertyPredicate::IsNull { variable, property } => Ok(Predicate::PropertyIsNull {
            variable: variable.clone(),
            property: property.clone(),
        }),
        PropertyPredicate::IsNotNull { variable, property } => Ok(Predicate::PropertyIsNotNull {
            variable: variable.clone(),
            property: property.clone(),
        }),
        PropertyPredicate::ParameterIsNull { parameter } => Ok(Predicate::ConstantBool(
            bind_value(&ValueExpression::Parameter(parameter.clone()), parameters)? == Value::Null,
        )),
        PropertyPredicate::ParameterIsNotNull { parameter } => Ok(Predicate::ConstantBool(
            bind_value(&ValueExpression::Parameter(parameter.clone()), parameters)? != Value::Null,
        )),
        PropertyPredicate::ParameterEq { left, right } => Ok(Predicate::ConstantBool(
            bind_value(&ValueExpression::Parameter(left.clone()), parameters)?
                == bind_value(right, parameters)?,
        )),
        PropertyPredicate::ParameterNotEq { left, right } => Ok(Predicate::ConstantBool(
            bind_value(&ValueExpression::Parameter(left.clone()), parameters)?
                != bind_value(right, parameters)?,
        )),
        PropertyPredicate::In {
            variable,
            property,
            values,
        } => match bind_value(values, parameters)? {
            Value::List(values) => Ok(Predicate::PropertyIn {
                variable: variable.clone(),
                property: property.clone(),
                values,
            }),
            value => Err(SkeinError::Semantic(format!(
                "IN predicate requires a list value, got {value:?}"
            ))),
        },
    }
}

struct RelationshipMutationPredicatePlan {
    source_predicate: Option<Predicate>,
    rel_predicate: Option<Predicate>,
    target_properties: BTreeMap<String, Value>,
}

fn plan_relationship_mutation_predicate(
    predicate: Option<&PropertyPredicate>,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    target_pattern_properties: &BTreeMap<String, ValueExpression>,
    parameters: &BTreeMap<String, Value>,
) -> Result<RelationshipMutationPredicatePlan> {
    let mut target_properties = bind_properties(target_pattern_properties, parameters)?;
    let Some(predicate) = predicate else {
        return Ok(RelationshipMutationPredicatePlan {
            source_predicate: None,
            rel_predicate: None,
            target_properties,
        });
    };
    let (source_predicate, rel_predicate) = split_relationship_mutation_predicate(
        predicate,
        source_variable,
        rel_variable,
        target_variable,
        &mut target_properties,
        parameters,
    )?;
    Ok(RelationshipMutationPredicatePlan {
        source_predicate,
        rel_predicate,
        target_properties,
    })
}

fn split_relationship_mutation_predicate(
    predicate: &PropertyPredicate,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    target_properties: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    let scope = BTreeSet::from([
        source_variable.to_string(),
        rel_variable.to_string(),
        target_variable.to_string(),
    ]);
    match predicate {
        PropertyPredicate::And(predicates) => {
            let mut source_predicates = Vec::new();
            let mut rel_predicates = Vec::new();
            for predicate in predicates {
                let (source, rel) = split_relationship_mutation_predicate(
                    predicate,
                    source_variable,
                    rel_variable,
                    target_variable,
                    target_properties,
                    parameters,
                )?;
                if let Some(source) = source {
                    source_predicates.push(source);
                }
                if let Some(rel) = rel {
                    rel_predicates.push(rel);
                }
            }
            Ok((
                combine_predicates(source_predicates),
                combine_predicates(rel_predicates),
            ))
        }
        PropertyPredicate::Or(_) => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation OR predicates cannot mix node and relationship variables"
                        .to_string(),
                ));
            }
            let variable = variables
                .iter()
                .next()
                .expect("checked exactly one predicate variable");
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
        PropertyPredicate::Not(_) => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation NOT predicates cannot mix node and relationship variables"
                        .to_string(),
                ));
            }
            let variable = variables
                .iter()
                .next()
                .expect("checked exactly one predicate variable");
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
        _ => {
            let variables = predicate_variables(predicate);
            if variables.len() != 1 {
                return Err(SkeinError::Semantic(
                    "relationship mutation predicate must bind one variable".to_string(),
                ));
            }
            let variable = variables.iter().next().ok_or_else(|| {
                SkeinError::Semantic(
                    "relationship mutation predicate must bind one variable".to_string(),
                )
            })?;
            if variable == target_variable {
                return bind_relationship_mutation_target_predicate(
                    predicate,
                    target_variable,
                    target_properties,
                    parameters,
                );
            }
            let planned = plan_predicate(predicate, &scope, parameters)?;
            predicate_for_relationship_mutation_variable(
                variable,
                source_variable,
                rel_variable,
                target_variable,
                planned,
            )
        }
    }
}

fn predicate_for_relationship_mutation_variable(
    variable: &str,
    source_variable: &str,
    rel_variable: &str,
    target_variable: &str,
    predicate: Predicate,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    if variable == source_variable {
        return Ok((Some(predicate), None));
    }
    if variable == rel_variable {
        return Ok((None, Some(predicate)));
    }
    if variable == target_variable {
        return Err(SkeinError::Semantic(
            "relationship mutation target predicates support only equality filters".to_string(),
        ));
    }
    Err(SkeinError::Semantic(format!(
        "unknown variable '{variable}' in relationship mutation predicate"
    )))
}

fn bind_relationship_mutation_target_predicate(
    predicate: &PropertyPredicate,
    target_variable: &str,
    target_properties: &mut BTreeMap<String, Value>,
    parameters: &BTreeMap<String, Value>,
) -> Result<(Option<Predicate>, Option<Predicate>)> {
    match predicate {
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if variable == target_variable => {
            insert_endpoint_property(target_properties, property, bind_value(value, parameters)?)?;
            Ok((None, None))
        }
        _ => Err(SkeinError::Semantic(
            "relationship mutation target predicates support only equality filters".to_string(),
        )),
    }
}

fn combine_predicates(predicates: Vec<Predicate>) -> Option<Predicate> {
    match predicates.len() {
        0 => None,
        1 => predicates.into_iter().next(),
        _ => Some(Predicate::And(predicates)),
    }
}

fn predicate_variables(predicate: &PropertyPredicate) -> BTreeSet<String> {
    let mut variables = BTreeSet::new();
    collect_predicate_variables(predicate, &mut variables);
    variables
}

fn collect_predicate_variables(predicate: &PropertyPredicate, variables: &mut BTreeSet<String>) {
    match predicate {
        PropertyPredicate::And(predicates) | PropertyPredicate::Or(predicates) => {
            for predicate in predicates {
                collect_predicate_variables(predicate, variables);
            }
        }
        PropertyPredicate::Not(predicate) => {
            collect_predicate_variables(predicate, variables);
        }
        PropertyPredicate::RelationshipExists { variable, .. } => {
            variables.insert(variable.clone());
        }
        PropertyPredicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => {
            variables.insert(source_variable.clone());
            variables.insert(target_variable.clone());
        }
        PropertyPredicate::ExpressionEq { expression, .. }
        | PropertyPredicate::ExpressionNotEq { expression, .. }
        | PropertyPredicate::ExpressionCompare { expression, .. }
        | PropertyPredicate::ExpressionContains { expression, .. } => {
            collect_return_value_expression_variables(expression, variables);
            let value = match predicate {
                PropertyPredicate::ExpressionEq { value, .. }
                | PropertyPredicate::ExpressionNotEq { value, .. }
                | PropertyPredicate::ExpressionCompare { value, .. }
                | PropertyPredicate::ExpressionContains { value, .. } => Some(value),
                _ => None,
            };
            if let Some(value) = value {
                collect_return_value_expression_variables(value, variables);
            }
        }
        _ => {
            if let Some(variable) = predicate_variable(predicate) {
                variables.insert(variable.to_string());
            }
        }
    }
}

fn predicate_variable(predicate: &PropertyPredicate) -> Option<&str> {
    match predicate {
        PropertyPredicate::Eq { variable, .. }
        | PropertyPredicate::NotEq { variable, .. }
        | PropertyPredicate::IdEq { variable, .. }
        | PropertyPredicate::IdNotEq { variable, .. }
        | PropertyPredicate::IdCompare { variable, .. }
        | PropertyPredicate::IdIn { variable, .. }
        | PropertyPredicate::Compare { variable, .. }
        | PropertyPredicate::ListContains { variable, .. }
        | PropertyPredicate::Contains { variable, .. }
        | PropertyPredicate::StartsWith { variable, .. }
        | PropertyPredicate::EndsWith { variable, .. }
        | PropertyPredicate::RegexMatch { variable, .. }
        | PropertyPredicate::IsNull { variable, .. }
        | PropertyPredicate::IsNotNull { variable, .. }
        | PropertyPredicate::In { variable, .. } => Some(variable),
        PropertyPredicate::RelationshipExists { variable, .. } => Some(variable),
        PropertyPredicate::And(_)
        | PropertyPredicate::Or(_)
        | PropertyPredicate::Not(_)
        | PropertyPredicate::ExpressionEq { .. }
        | PropertyPredicate::ExpressionNotEq { .. }
        | PropertyPredicate::ExpressionCompare { .. }
        | PropertyPredicate::ExpressionContains { .. }
        | PropertyPredicate::ParameterIsNull { .. }
        | PropertyPredicate::ParameterIsNotNull { .. }
        | PropertyPredicate::ParameterEq { .. }
        | PropertyPredicate::ParameterNotEq { .. }
        | PropertyPredicate::BoundRelationshipExists { .. } => None,
    }
}

fn collect_return_value_expression_variables(
    expression: &ReturnValueExpression,
    variables: &mut BTreeSet<String>,
) {
    match expression {
        ReturnValueExpression::Variable(variable)
        | ReturnValueExpression::Property { variable, .. }
        | ReturnValueExpression::Id(variable)
        | ReturnValueExpression::RelationshipType(variable) => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::Value(_) => {}
        ReturnValueExpression::Coalesce(expressions) => {
            for expression in expressions {
                collect_return_value_expression_variables(expression, variables);
            }
        }
        ReturnValueExpression::Left { expression, .. } => {
            collect_return_value_expression_variables(expression, variables);
        }
        ReturnValueExpression::Lower(expression) => {
            collect_return_value_expression_variables(expression, variables);
        }
        ReturnValueExpression::DatePart { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::DefaultIfNullOrEq { variable, .. }
        | ReturnValueExpression::DefaultIfNull { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::CasePropertyNotNullOrEq { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::CasePropertyEqualsRank { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::CaseLowerPropertyDefault { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            variables.insert(variable.clone());
        }
        ReturnValueExpression::CaseEntitySearchRank(expression) => {
            variables.insert(expression.variable.clone());
        }
        ReturnValueExpression::CaseColumnSearchRank(_) => {}
    }
}

fn plan_comparison_op(op: CypherComparisonOp) -> ComparisonOp {
    match op {
        CypherComparisonOp::Lt => ComparisonOp::Lt,
        CypherComparisonOp::Lte => ComparisonOp::Lte,
        CypherComparisonOp::Gt => ComparisonOp::Gt,
        CypherComparisonOp::Gte => ComparisonOp::Gte,
    }
}

fn plan_sort_items(
    scope: &BTreeSet<String>,
    projection_names: &BTreeSet<String>,
    items: &[OrderItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<SortItem>> {
    items
        .iter()
        .map(|item| {
            let key = match &item.expression {
                OrderExpression::Property { variable, property } => {
                    let projected_property = format!("{variable}.{property}");
                    if projection_names.contains(&projected_property) {
                        return Ok(SortItem {
                            key: SortKey::Column(projected_property),
                            direction: match item.direction {
                                CypherOrderDirection::Asc => SortDirection::Asc,
                                CypherOrderDirection::Desc => SortDirection::Desc,
                            },
                        });
                    }
                    if projection_names.contains(variable) {
                        return Ok(SortItem {
                            key: SortKey::Expression(ProjectionExpression::ColumnProperty {
                                column: variable.clone(),
                                property: property.clone(),
                            }),
                            direction: match item.direction {
                                CypherOrderDirection::Asc => SortDirection::Asc,
                                CypherOrderDirection::Desc => SortDirection::Desc,
                            },
                        });
                    }
                    if !scope.contains(variable) {
                        return Err(SkeinError::Semantic(format!(
                            "unknown variable '{variable}' in order item"
                        )));
                    }
                    SortKey::Property {
                        variable: variable.clone(),
                        property: property.clone(),
                    }
                }
                OrderExpression::Id { variable } => {
                    if !scope.contains(variable) {
                        return Err(SkeinError::Semantic(format!(
                            "unknown variable '{variable}' in order item"
                        )));
                    }
                    SortKey::Id {
                        variable: variable.clone(),
                    }
                }
                OrderExpression::Value(expression) => SortKey::Expression(
                    plan_order_value_expression(scope, projection_names, expression, parameters)?,
                ),
                OrderExpression::Column(name) => {
                    if !projection_names.contains(name) {
                        return Err(SkeinError::Semantic(format!(
                            "unknown column '{name}' in order item"
                        )));
                    }
                    SortKey::Column(name.clone())
                }
            };
            let direction = match item.direction {
                CypherOrderDirection::Asc => SortDirection::Asc,
                CypherOrderDirection::Desc => SortDirection::Desc,
            };
            Ok(SortItem { key, direction })
        })
        .collect()
}

fn plan_order_value_expression(
    scope: &BTreeSet<String>,
    projection_names: &BTreeSet<String>,
    expression: &ReturnValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    if let ReturnValueExpression::CasePropertyNotNullOrEq {
        variable,
        property,
        empty,
        non_empty,
        null_or_empty,
    } = expression
    {
        let projected_property = format!("{variable}.{property}");
        if projection_names.contains(&projected_property) {
            return Ok(ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
                column: projected_property,
                empty: bind_value(empty, parameters)?,
                non_empty: bind_value(non_empty, parameters)?,
                null_or_empty: bind_value(null_or_empty, parameters)?,
            });
        }
    }
    if let ReturnValueExpression::DefaultIfNull {
        variable,
        property,
        default,
    } = expression
    {
        let projected_property = format!("{variable}.{property}");
        if projection_names.contains(&projected_property) {
            return Ok(ProjectionExpression::ColumnValueDefaultIfNull {
                column: projected_property,
                default: bind_value(default, parameters)?,
            });
        }
    }
    plan_return_value_expression_with_columns(scope, projection_names, expression, parameters)
}

fn bind_pagination_value(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<usize> {
    bind_non_negative_usize(expression, parameters, name)
}

fn bind_non_negative_usize(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<usize> {
    match bind_value(expression, parameters)? {
        Value::Int(value) if value >= 0 => Ok(value as usize),
        value => Err(SkeinError::Semantic(format!(
            "{name} must be a non-negative integer, got {value:?}"
        ))),
    }
}

fn bind_optional_f64(
    expression: &ValueExpression,
    parameters: &BTreeMap<String, Value>,
    name: &str,
) -> Result<f64> {
    match bind_value(expression, parameters)? {
        Value::Float(value) => Ok(value),
        Value::Int(value) => Ok(value as f64),
        value => Err(SkeinError::Semantic(format!(
            "{name} must be numeric, got {value:?}"
        ))),
    }
}

enum PlannedReturns {
    Projections(Vec<Projection>),
    Aggregations {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
    },
}

impl PlannedReturns {
    fn names(&self) -> Vec<String> {
        match self {
            PlannedReturns::Projections(items) => {
                items.iter().map(|item| item.name.clone()).collect()
            }
            PlannedReturns::Aggregations { group_keys, items } => {
                let mut names = group_keys
                    .iter()
                    .map(|item| item.name.clone())
                    .collect::<Vec<_>>();
                names.extend(items.iter().map(|item| item.name.clone()));
                names
            }
        }
    }

    fn into_logical(self, input: LogicalPlan) -> LogicalPlan {
        match self {
            PlannedReturns::Projections(items) => LogicalPlan::Project {
                items,
                input: Box::new(input),
            },
            PlannedReturns::Aggregations { group_keys, items } => LogicalPlan::Aggregate {
                group_keys,
                items,
                input: Box::new(input),
            },
        }
    }
}

fn planned_sort_scope<'a>(
    input: &LogicalPlan,
    scope: &'a BTreeSet<String>,
) -> &'a BTreeSet<String> {
    if matches!(input, LogicalPlan::Aggregate { .. }) {
        static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(BTreeSet::new)
    } else {
        scope
    }
}

fn plan_set_node_properties_return_mode(
    update: &crate::cypher::MatchSet,
    returns: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<SetNodePropertiesReturnMode> {
    if returns.len() == 1 {
        let item = &returns[0];
        match &item.expression {
            ReturnExpression::CountAll => {
                return Ok(SetNodePropertiesReturnMode::Count {
                    name: item.alias.clone().unwrap_or_else(|| "count(*)".to_string()),
                });
            }
            ReturnExpression::CountVariable { variable, distinct } if !distinct => {
                if variable != &update.variable {
                    return Err(SkeinError::Semantic(format!(
                        "SET RETURN count variable '{variable}' does not match updated variable '{}'",
                        update.variable
                    )));
                }
                return Ok(SetNodePropertiesReturnMode::Count {
                    name: item
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("count({variable})")),
                });
            }
            _ => {}
        }
    }
    let scope = BTreeSet::from([update.variable.clone()]);
    let projections = returns
        .iter()
        .map(|item| plan_projection(&scope, item, parameters))
        .collect::<Result<Vec<_>>>()?;
    Ok(SetNodePropertiesReturnMode::Project(projections))
}

fn plan_return_items(
    scope: &BTreeSet<String>,
    items: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<PlannedReturns> {
    let has_aggregate = items.iter().any(|item| {
        matches!(
            item.expression,
            ReturnExpression::CountAll
                | ReturnExpression::CountVariable { .. }
                | ReturnExpression::CountProperty { .. }
                | ReturnExpression::CollectVariable { .. }
                | ReturnExpression::CollectProperty { .. }
                | ReturnExpression::MinProperty { .. }
                | ReturnExpression::MaxProperty { .. }
                | ReturnExpression::AvgProperty { .. }
        )
    });
    if has_aggregate {
        let mut group_keys = Vec::new();
        let mut aggregations = Vec::new();
        for item in items {
            match item.expression {
                ReturnExpression::Variable(_)
                | ReturnExpression::Property { .. }
                | ReturnExpression::Value(_)
                | ReturnExpression::Id(_)
                | ReturnExpression::RelationshipType(_)
                | ReturnExpression::Coalesce(_)
                | ReturnExpression::Left { .. }
                | ReturnExpression::Lower(_)
                | ReturnExpression::DatePart { .. }
                | ReturnExpression::DefaultIfNullOrEq { .. }
                | ReturnExpression::DefaultIfNull { .. }
                | ReturnExpression::CasePropertyNotNullOrEq { .. }
                | ReturnExpression::CasePropertyEqualsRank { .. }
                | ReturnExpression::CaseLowerPropertyDefault { .. }
                | ReturnExpression::CaseCoalesceDifferenceFloorZero { .. }
                | ReturnExpression::CaseEntitySearchRank(_)
                | ReturnExpression::CaseColumnSearchRank(_) => {
                    group_keys.push(plan_projection(scope, item, parameters)?);
                }
                ReturnExpression::CountAll
                | ReturnExpression::CountVariable { .. }
                | ReturnExpression::CountProperty { .. }
                | ReturnExpression::CollectVariable { .. }
                | ReturnExpression::CollectProperty { .. }
                | ReturnExpression::MinProperty { .. }
                | ReturnExpression::MaxProperty { .. }
                | ReturnExpression::AvgProperty { .. } => {
                    aggregations.push(plan_aggregation(scope, item)?);
                }
            }
        }
        Ok(PlannedReturns::Aggregations {
            group_keys,
            items: aggregations,
        })
    } else {
        items
            .iter()
            .map(|item| plan_projection(scope, item, parameters))
            .collect::<Result<Vec<_>>>()
            .map(PlannedReturns::Projections)
    }
}

fn returns_are_count_only(items: &[ReturnItem]) -> bool {
    !items.is_empty()
        && items.iter().all(|item| {
            matches!(
                item.expression,
                ReturnExpression::CountAll
                    | ReturnExpression::CountVariable { .. }
                    | ReturnExpression::CountProperty { .. }
            )
        })
}

fn plan_projection(
    scope: &BTreeSet<String>,
    item: &ReturnItem,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    plan_projection_with_columns(scope, &BTreeSet::new(), item, parameters)
}

fn plan_projection_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    item: &ReturnItem,
    parameters: &BTreeMap<String, Value>,
) -> Result<Projection> {
    let (expression, default_name) = match &item.expression {
        ReturnExpression::Variable(variable) => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::Column(variable.clone()),
                    name: item.alias.clone().unwrap_or_else(|| variable.clone()),
                });
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Variable {
                    variable: variable.clone(),
                },
                variable.clone(),
            )
        }
        ReturnExpression::Property { variable, property } => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::ColumnProperty {
                        column: variable.clone(),
                        property: property.clone(),
                    },
                    name: item
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("{variable}.{property}")),
                });
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                format!("{variable}.{property}"),
            )
        }
        ReturnExpression::Id(variable) => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::Id {
                    variable: variable.clone(),
                },
                format!("id({variable})"),
            )
        }
        ReturnExpression::RelationshipType(variable) => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::RelationshipType {
                    variable: variable.clone(),
                },
                format!("label({variable})"),
            )
        }
        ReturnExpression::Value(value) => (
            ProjectionExpression::Literal(bind_value(value, parameters)?),
            "literal".to_string(),
        ),
        ReturnExpression::Coalesce(expressions) => (
            ProjectionExpression::Coalesce(
                expressions
                    .iter()
                    .map(|expression| {
                        plan_return_value_expression_with_columns(
                            scope,
                            column_scope,
                            expression,
                            parameters,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
            "coalesce".to_string(),
        ),
        ReturnExpression::Left { expression, length } => (
            ProjectionExpression::Left {
                expression: Box::new(plan_return_value_expression_with_columns(
                    scope,
                    column_scope,
                    expression,
                    parameters,
                )?),
                length: bind_non_negative_usize(length, parameters, "LEFT length")?,
            },
            "left".to_string(),
        ),
        ReturnExpression::Lower(expression) => (
            ProjectionExpression::Lower(Box::new(plan_return_value_expression_with_columns(
                scope,
                column_scope,
                expression,
                parameters,
            )?)),
            "lower".to_string(),
        ),
        ReturnExpression::DatePart {
            part,
            variable,
            property,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DatePart {
                    part: plan_date_part(part)?,
                    variable: variable.clone(),
                    property: property.clone(),
                },
                format!("date_part({part}, {variable}.{property})"),
            )
        }
        ReturnExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if column_scope.contains(variable) {
                return Ok(Projection {
                    expression: ProjectionExpression::ColumnDefaultIfNullOrEq {
                        column: variable.clone(),
                        property: property.clone(),
                        empty: bind_value(empty, parameters)?,
                        default: bind_value(default, parameters)?,
                    },
                    name: item.alias.clone().unwrap_or_else(|| property.clone()),
                });
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DefaultIfNullOrEq {
                    variable: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    default: bind_value(default, parameters)?,
                },
                property.clone(),
            )
        }
        ReturnExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::DefaultIfNull {
                    variable: variable.clone(),
                    property: property.clone(),
                    default: bind_value(default, parameters)?,
                },
                property.clone(),
            )
        }
        ReturnExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CasePropertyNotNullOrEq {
                    variable: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    non_empty: bind_value(non_empty, parameters)?,
                    null_or_empty: bind_value(null_or_empty, parameters)?,
                },
                "case".to_string(),
            )
        }
        ReturnExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CasePropertyEqualsRank {
                    variable: variable.clone(),
                    property: property.clone(),
                    branches: bind_value_pairs(branches, parameters)?,
                    default: bind_value(default, parameters)?,
                },
                "case".to_string(),
            )
        }
        ReturnExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CaseLowerPropertyDefault {
                    variable: variable.clone(),
                    property: property.clone(),
                    default: bind_value(default, parameters)?,
                },
                "case".to_string(),
            )
        }
        ReturnExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                ProjectionExpression::CaseCoalesceDifferenceFloorZero {
                    variable: variable.clone(),
                    terms: bind_coalesce_difference_terms(terms, parameters)?,
                },
                "case".to_string(),
            )
        }
        ReturnExpression::CaseEntitySearchRank(expression) => {
            if !scope.contains(&expression.variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{}' in return item",
                    expression.variable
                )));
            }
            (
                ProjectionExpression::CaseEntitySearchRank(Box::new(
                    CaseEntitySearchRankProjection {
                        variable: expression.variable.clone(),
                        name_property: expression.name_property.clone(),
                        aliases_property: expression.aliases_property.clone(),
                        raw_query: bind_value(&expression.raw_query, parameters)?,
                        normalized_query: bind_value(&expression.normalized_query, parameters)?,
                        raw_input: bind_value(&expression.raw_input, parameters)?,
                        exact_rank: bind_value(&expression.exact_rank, parameters)?,
                        alias_rank: bind_value(&expression.alias_rank, parameters)?,
                        fallback_rank: bind_value(&expression.fallback_rank, parameters)?,
                    },
                )),
                "case".to_string(),
            )
        }
        ReturnExpression::CaseColumnSearchRank(expression) => (
            ProjectionExpression::CaseColumnSearchRank(Box::new(CaseColumnSearchRankProjection {
                column: expression.column.clone(),
                raw_query: bind_value(&expression.raw_query, parameters)?,
                normalized_query: bind_value(&expression.normalized_query, parameters)?,
                exact_rank: bind_value(&expression.exact_rank, parameters)?,
                contains_rank: bind_value(&expression.contains_rank, parameters)?,
                fallback_rank: bind_value(&expression.fallback_rank, parameters)?,
            })),
            "case".to_string(),
        ),
        ReturnExpression::CountAll
        | ReturnExpression::CountVariable { .. }
        | ReturnExpression::CountProperty { .. }
        | ReturnExpression::CollectVariable { .. }
        | ReturnExpression::CollectProperty { .. }
        | ReturnExpression::MinProperty { .. }
        | ReturnExpression::MaxProperty { .. }
        | ReturnExpression::AvgProperty { .. } => {
            return Err(SkeinError::Semantic(
                "expected projection return item".to_string(),
            ));
        }
    };
    Ok(Projection {
        expression,
        name: item.alias.clone().unwrap_or(default_name),
    })
}

fn bind_relationship_set_value(
    value: &crate::cypher::SetValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<Value> {
    match value {
        crate::cypher::SetValueExpression::Value(value) => bind_value(value, parameters),
        crate::cypher::SetValueExpression::Property { .. } => Err(SkeinError::Semantic(
            "relationship property reference SET is only supported by relationship-copy MERGE"
                .to_string(),
        )),
        crate::cypher::SetValueExpression::PropertyAdd { .. }
        | crate::cypher::SetValueExpression::CoalesceProperty { .. }
        | crate::cypher::SetValueExpression::DecrementFloorZero { .. }
        | crate::cypher::SetValueExpression::PreserveNewerExisting { .. }
        | crate::cypher::SetValueExpression::CoalescePropertyAdd { .. } => {
            Err(SkeinError::Semantic(
                "relationship property increment SET is not supported".to_string(),
            ))
        }
    }
}

fn plan_set_value(
    set: &crate::cypher::SetProperty,
    parameters: &BTreeMap<String, Value>,
) -> Result<SetValue> {
    match &set.value {
        crate::cypher::SetValueExpression::Value(value) => {
            Ok(SetValue::Value(bind_value(value, parameters)?))
        }
        crate::cypher::SetValueExpression::Property { .. } => Err(SkeinError::Semantic(
            "property reference SET is only supported by relationship-copy MERGE".to_string(),
        )),
        crate::cypher::SetValueExpression::CoalesceProperty {
            variable,
            property,
            default,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(SkeinError::Semantic(
                    "COALESCE property SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            Ok(SetValue::Coalesce {
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        crate::cypher::SetValueExpression::PropertyAdd {
            variable,
            property,
            value,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(SkeinError::Semantic(
                    "property increment SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            let amount = bind_value(value, parameters)?;
            let Value::Int(amount) = amount else {
                return Err(SkeinError::Semantic(
                    "property increment SET requires an integer increment".to_string(),
                ));
            };
            Ok(SetValue::AddInt {
                property: property.clone(),
                amount,
            })
        }
        crate::cypher::SetValueExpression::DecrementFloorZero { variable, property } => {
            if variable != &set.variable || property != &set.property {
                return Err(SkeinError::Semantic(
                    "CASE decrement SET must read the same variable property it writes".to_string(),
                ));
            }
            Ok(SetValue::DecrementFloorZero {
                property: property.clone(),
            })
        }
        crate::cypher::SetValueExpression::PreserveNewerExisting {
            variable,
            property,
            incoming,
            preserve,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(SkeinError::Semantic(
                    "CASE preserve SET must read the same variable property it writes".to_string(),
                ));
            }
            let preserve = bind_value(preserve, parameters)?;
            let Value::Bool(preserve) = preserve else {
                return Err(SkeinError::Semantic(
                    "CASE preserve SET requires a boolean preserve flag".to_string(),
                ));
            };
            Ok(SetValue::PreserveNewerExisting {
                property: property.clone(),
                incoming: bind_value(incoming, parameters)?,
                preserve,
            })
        }
        crate::cypher::SetValueExpression::CoalescePropertyAdd {
            variable,
            property,
            default,
            value,
        } => {
            if variable != &set.variable || property != &set.property {
                return Err(SkeinError::Semantic(
                    "property increment SET must read the same variable property it writes"
                        .to_string(),
                ));
            }
            let default = bind_value(default, parameters)?;
            if default != Value::Int(0) {
                return Err(SkeinError::Semantic(
                    "COALESCE property increment SET only supports integer zero defaults"
                        .to_string(),
                ));
            }
            let amount = bind_value(value, parameters)?;
            let Value::Int(amount) = amount else {
                return Err(SkeinError::Semantic(
                    "property increment SET requires an integer increment".to_string(),
                ));
            };
            Ok(SetValue::AddInt {
                property: property.clone(),
                amount,
            })
        }
    }
}

fn plan_aggregation(scope: &BTreeSet<String>, item: &ReturnItem) -> Result<Aggregation> {
    let (function, target, distinct) = match &item.expression {
        ReturnExpression::CountAll => (AggregateFunction::Count, AggregateTarget::All, false),
        ReturnExpression::CountVariable { variable, distinct } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Count,
                AggregateTarget::Variable(variable.clone()),
                *distinct,
            )
        }
        ReturnExpression::CountProperty {
            variable,
            property,
            distinct,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Count,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                *distinct,
            )
        }
        ReturnExpression::CollectProperty {
            variable,
            property,
            distinct,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Collect,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                *distinct,
            )
        }
        ReturnExpression::CollectVariable { variable, distinct } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Collect,
                AggregateTarget::Variable(variable.clone()),
                *distinct,
            )
        }
        ReturnExpression::MinProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Min,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
        ReturnExpression::MaxProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Max,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
        ReturnExpression::AvgProperty { variable, property } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            (
                AggregateFunction::Avg,
                AggregateTarget::Property {
                    variable: variable.clone(),
                    property: property.clone(),
                },
                false,
            )
        }
        ReturnExpression::Id(_)
        | ReturnExpression::Variable(_)
        | ReturnExpression::Value(_)
        | ReturnExpression::RelationshipType(_)
        | ReturnExpression::Coalesce(_)
        | ReturnExpression::Left { .. }
        | ReturnExpression::Lower(_)
        | ReturnExpression::DatePart { .. }
        | ReturnExpression::DefaultIfNullOrEq { .. }
        | ReturnExpression::DefaultIfNull { .. }
        | ReturnExpression::CasePropertyNotNullOrEq { .. }
        | ReturnExpression::CasePropertyEqualsRank { .. }
        | ReturnExpression::CaseLowerPropertyDefault { .. }
        | ReturnExpression::CaseCoalesceDifferenceFloorZero { .. }
        | ReturnExpression::CaseEntitySearchRank(_)
        | ReturnExpression::CaseColumnSearchRank(_) => {
            return Err(SkeinError::Semantic(
                "expected aggregate return item".to_string(),
            ));
        }
        ReturnExpression::Property { .. } => {
            return Err(SkeinError::Semantic(
                "expected aggregate return item".to_string(),
            ));
        }
    };
    let name = item
        .alias
        .clone()
        .unwrap_or_else(|| default_aggregation_name(function, &target, distinct));
    Ok(Aggregation {
        function,
        target,
        distinct,
        name,
    })
}

fn plan_return_value_expression(
    scope: &BTreeSet<String>,
    expression: &ReturnValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    plan_return_value_expression_with_columns(scope, &BTreeSet::new(), expression, parameters)
}

fn plan_return_value_expression_with_columns(
    scope: &BTreeSet<String>,
    column_scope: &BTreeSet<String>,
    expression: &ReturnValueExpression,
    parameters: &BTreeMap<String, Value>,
) -> Result<ProjectionExpression> {
    match expression {
        ReturnValueExpression::Variable(variable) => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::Column(variable.clone()));
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Variable {
                variable: variable.clone(),
            })
        }
        ReturnValueExpression::Property { variable, property } => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::ColumnProperty {
                    column: variable.clone(),
                    property: property.clone(),
                });
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Property {
                variable: variable.clone(),
                property: property.clone(),
            })
        }
        ReturnValueExpression::Id(variable) => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::Id {
                variable: variable.clone(),
            })
        }
        ReturnValueExpression::RelationshipType(variable) => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in return item"
                )));
            }
            Ok(ProjectionExpression::RelationshipType {
                variable: variable.clone(),
            })
        }
        ReturnValueExpression::Value(value) => Ok(ProjectionExpression::Literal(bind_value(
            value, parameters,
        )?)),
        ReturnValueExpression::Coalesce(expressions) => Ok(ProjectionExpression::Coalesce(
            expressions
                .iter()
                .map(|expression| {
                    plan_return_value_expression_with_columns(
                        scope,
                        column_scope,
                        expression,
                        parameters,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        ReturnValueExpression::Left { expression, length } => Ok(ProjectionExpression::Left {
            expression: Box::new(plan_return_value_expression_with_columns(
                scope,
                column_scope,
                expression,
                parameters,
            )?),
            length: bind_non_negative_usize(length, parameters, "LEFT length")?,
        }),
        ReturnValueExpression::Lower(expression) => Ok(ProjectionExpression::Lower(Box::new(
            plan_return_value_expression_with_columns(scope, column_scope, expression, parameters)?,
        ))),
        ReturnValueExpression::DatePart {
            part,
            variable,
            property,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DatePart {
                part: plan_date_part(part)?,
                variable: variable.clone(),
                property: property.clone(),
            })
        }
        ReturnValueExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            if column_scope.contains(variable) {
                return Ok(ProjectionExpression::ColumnDefaultIfNullOrEq {
                    column: variable.clone(),
                    property: property.clone(),
                    empty: bind_value(empty, parameters)?,
                    default: bind_value(default, parameters)?,
                });
            }
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DefaultIfNullOrEq {
                variable: variable.clone(),
                property: property.clone(),
                empty: bind_value(empty, parameters)?,
                default: bind_value(default, parameters)?,
            })
        }
        ReturnValueExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::DefaultIfNull {
                variable: variable.clone(),
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        ReturnValueExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CasePropertyNotNullOrEq {
                variable: variable.clone(),
                property: property.clone(),
                empty: bind_value(empty, parameters)?,
                non_empty: bind_value(non_empty, parameters)?,
                null_or_empty: bind_value(null_or_empty, parameters)?,
            })
        }
        ReturnValueExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CasePropertyEqualsRank {
                variable: variable.clone(),
                property: property.clone(),
                branches: bind_value_pairs(branches, parameters)?,
                default: bind_value(default, parameters)?,
            })
        }
        ReturnValueExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CaseLowerPropertyDefault {
                variable: variable.clone(),
                property: property.clone(),
                default: bind_value(default, parameters)?,
            })
        }
        ReturnValueExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            if !scope.contains(variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{variable}' in expression"
                )));
            }
            Ok(ProjectionExpression::CaseCoalesceDifferenceFloorZero {
                variable: variable.clone(),
                terms: bind_coalesce_difference_terms(terms, parameters)?,
            })
        }
        ReturnValueExpression::CaseEntitySearchRank(expression) => {
            if !scope.contains(&expression.variable) {
                return Err(SkeinError::Semantic(format!(
                    "unknown variable '{}' in expression",
                    expression.variable
                )));
            }
            Ok(ProjectionExpression::CaseEntitySearchRank(Box::new(
                CaseEntitySearchRankProjection {
                    variable: expression.variable.clone(),
                    name_property: expression.name_property.clone(),
                    aliases_property: expression.aliases_property.clone(),
                    raw_query: bind_value(&expression.raw_query, parameters)?,
                    normalized_query: bind_value(&expression.normalized_query, parameters)?,
                    raw_input: bind_value(&expression.raw_input, parameters)?,
                    exact_rank: bind_value(&expression.exact_rank, parameters)?,
                    alias_rank: bind_value(&expression.alias_rank, parameters)?,
                    fallback_rank: bind_value(&expression.fallback_rank, parameters)?,
                },
            )))
        }
        ReturnValueExpression::CaseColumnSearchRank(expression) => {
            if !column_scope.contains(&expression.column) {
                return Err(SkeinError::Semantic(format!(
                    "unknown column '{}' in expression",
                    expression.column
                )));
            }
            Ok(ProjectionExpression::CaseColumnSearchRank(Box::new(
                CaseColumnSearchRankProjection {
                    column: expression.column.clone(),
                    raw_query: bind_value(&expression.raw_query, parameters)?,
                    normalized_query: bind_value(&expression.normalized_query, parameters)?,
                    exact_rank: bind_value(&expression.exact_rank, parameters)?,
                    contains_rank: bind_value(&expression.contains_rank, parameters)?,
                    fallback_rank: bind_value(&expression.fallback_rank, parameters)?,
                },
            )))
        }
    }
}

fn bind_coalesce_difference_terms(
    terms: &[crate::cypher::CoalesceDifferenceTerm],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<CoalesceDifferenceProjectionTerm>> {
    terms
        .iter()
        .map(|term| {
            Ok(CoalesceDifferenceProjectionTerm {
                property: term.property.clone(),
                default: bind_value(&term.default, parameters)?,
            })
        })
        .collect()
}

fn bind_value_pairs(
    pairs: &[(ValueExpression, ValueExpression)],
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<(Value, Value)>> {
    pairs
        .iter()
        .map(|(left, right)| {
            Ok((
                bind_value(left, parameters)?,
                bind_value(right, parameters)?,
            ))
        })
        .collect()
}

fn plan_date_part(part: &str) -> Result<DatePart> {
    match part.to_ascii_lowercase().as_str() {
        "year" => Ok(DatePart::Year),
        "month" => Ok(DatePart::Month),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported date_part component '{part}'"
        ))),
    }
}

fn default_aggregation_name(
    function: AggregateFunction,
    target: &AggregateTarget,
    distinct: bool,
) -> String {
    match (function, target) {
        (AggregateFunction::Count, AggregateTarget::All) => "count(*)".to_string(),
        (AggregateFunction::Count, AggregateTarget::Variable(variable)) if distinct => {
            format!("count(DISTINCT {variable})")
        }
        (AggregateFunction::Count, AggregateTarget::Variable(variable)) => {
            format!("count({variable})")
        }
        (AggregateFunction::Count, AggregateTarget::Property { variable, property })
            if distinct =>
        {
            format!("count(DISTINCT {variable}.{property})")
        }
        (AggregateFunction::Count, AggregateTarget::Property { variable, property }) => {
            format!("count({variable}.{property})")
        }
        (AggregateFunction::Min, AggregateTarget::Property { variable, property }) => {
            format!("min({variable}.{property})")
        }
        (AggregateFunction::Min, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "min(?)".to_string()
        }
        (AggregateFunction::Max, AggregateTarget::Property { variable, property }) => {
            format!("max({variable}.{property})")
        }
        (AggregateFunction::Max, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "max(?)".to_string()
        }
        (AggregateFunction::Avg, AggregateTarget::Property { variable, property }) => {
            format!("avg({variable}.{property})")
        }
        (AggregateFunction::Avg, AggregateTarget::All | AggregateTarget::Variable(_)) => {
            "avg(?)".to_string()
        }
        (AggregateFunction::Collect, AggregateTarget::Variable(variable)) if distinct => {
            format!("collect(DISTINCT {variable})")
        }
        (AggregateFunction::Collect, AggregateTarget::Variable(variable)) => {
            format!("collect({variable})")
        }
        (AggregateFunction::Collect, AggregateTarget::Property { variable, property })
            if distinct =>
        {
            format!("collect(DISTINCT {variable}.{property})")
        }
        (AggregateFunction::Collect, AggregateTarget::Property { variable, property }) => {
            format!("collect({variable}.{property})")
        }
        (AggregateFunction::Collect, AggregateTarget::All) => "collect(*)".to_string(),
    }
}
