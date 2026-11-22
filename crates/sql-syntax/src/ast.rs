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

use crate::Span;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgqStatement {
    CreatePropertyGraph(CreatePropertyGraph),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalStatementSyntax {
    Select(Box<PostgresSelectSyntax>),
    Query(Box<PostgresQuerySyntax>),
    Explain {
        analyze: bool,
        query: Box<PostgresQuerySyntax>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresQuerySyntax {
    pub with: Vec<PostgresCommonTableExpressionSyntax>,
    pub body: PostgresQueryBodySyntax,
    pub order_by: Vec<OrderBySyntax>,
    pub limit: Option<ExpressionSyntax>,
    pub offset: Option<ExpressionSyntax>,
    pub locking: Option<LockingClauseSyntax>,
    pub span: Span,
}

impl PostgresQuerySyntax {
    pub fn into_simple_select(self) -> Result<PostgresSelectSyntax, Box<Self>> {
        if !self.with.is_empty() || !matches!(&self.body, PostgresQueryBodySyntax::Select(_)) {
            return Err(Box::new(self));
        }
        let Self {
            with: _,
            body,
            order_by,
            limit,
            offset,
            locking,
            span: _,
        } = self;
        let PostgresQueryBodySyntax::Select(mut select) = body else {
            unreachable!("simple query body was checked")
        };
        select.order_by = order_by;
        select.limit = limit;
        select.offset = offset;
        select.locking = locking;
        Ok(*select)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresCommonTableExpressionSyntax {
    pub name: Identifier,
    pub columns: Vec<Identifier>,
    pub query: Box<PostgresQuerySyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresQueryBodySyntax {
    Select(Box<PostgresSelectSyntax>),
    Values {
        rows: Vec<Vec<ExpressionSyntax>>,
        span: Span,
    },
    Nested(Box<PostgresQuerySyntax>),
    SetOperation {
        left: Box<PostgresQueryBodySyntax>,
        operator: PostgresSetOperator,
        all: bool,
        right: Box<PostgresQueryBodySyntax>,
        span: Span,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresSetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresStatementSyntax {
    CreatePropertyGraph(CreatePropertyGraph),
    Select(Box<PostgresSelectSyntax>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresSelectSyntax {
    pub distinct: bool,
    pub projection: Vec<LabeledExpressionSyntax>,
    pub from: Vec<PostgresFromSyntax>,
    pub selection: Option<ExpressionSyntax>,
    pub group_by: Vec<ExpressionSyntax>,
    pub having: Option<ExpressionSyntax>,
    pub order_by: Vec<OrderBySyntax>,
    pub limit: Option<ExpressionSyntax>,
    pub offset: Option<ExpressionSyntax>,
    pub locking: Option<LockingClauseSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresFromSyntax {
    pub relation: PostgresFromItemSyntax,
    pub joins: Vec<PostgresJoinSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostgresFromItemSyntax {
    Relation(RelationTableSyntax),
    Derived(DerivedTableSyntax),
    GraphTable(GraphTable),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedTableSyntax {
    pub query: Box<PostgresQuerySyntax>,
    pub alias: TableAlias,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresJoinSyntax {
    pub kind: PostgresJoinKind,
    pub relation: PostgresFromItemSyntax,
    pub condition: Option<ExpressionSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostgresJoinKind {
    Inner,
    Left,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBySyntax {
    pub expression: ExpressionSyntax,
    pub direction: Option<OrderDirectionSyntax>,
    pub nulls: Option<NullOrderSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirectionSyntax {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrderSyntax {
    First,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockingClauseSyntax {
    pub nonblocking: bool,
    pub strength: LockStrengthSyntax,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockStrengthSyntax {
    Share,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFunctionArgumentSyntax {
    pub name: Option<Identifier>,
    pub expression: ExpressionSyntax,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationTableSyntax {
    pub name: QualifiedName,
    pub arguments: Option<Vec<TableFunctionArgumentSyntax>>,
    pub alias: Option<TableAlias>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub span: Span,
    pub quoted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QualifiedName {
    pub parts: Vec<Identifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePropertyGraph {
    pub temporary: bool,
    pub name: QualifiedName,
    pub vertex_tables: Vec<VertexTableDefinition>,
    pub edge_tables: Vec<EdgeTableDefinition>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VertexTableDefinition {
    pub table: QualifiedName,
    pub alias: Option<Identifier>,
    pub key: Vec<Identifier>,
    pub exposure: Option<ElementExposure>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTableDefinition {
    pub table: QualifiedName,
    pub alias: Option<Identifier>,
    pub key: Vec<Identifier>,
    pub source: EdgeEndpoint,
    pub destination: EdgeEndpoint,
    pub exposure: Option<ElementExposure>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEndpoint {
    pub key: Vec<Identifier>,
    pub vertex: Identifier,
    pub vertex_key: Vec<Identifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementExposure {
    pub labels: Vec<ElementLabelExposure>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElementLabelExposure {
    pub label: ElementLabel,
    pub properties: PropertyExposure,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElementLabel {
    Implicit,
    Default,
    Named(Identifier),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyExposure {
    AllColumns,
    NoProperties,
    Expressions(Vec<LabeledExpressionSyntax>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledExpressionSyntax {
    pub expression: ExpressionSyntax,
    pub alias: Option<Identifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTable {
    pub graph: QualifiedName,
    pub pattern: GraphPatternSyntax,
    pub columns: Vec<GraphTableColumn>,
    pub alias: Option<TableAlias>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPatternSyntax {
    pub paths: Vec<GraphPathSyntax>,
    pub predicate: Option<ExpressionSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPathSyntax {
    pub factors: Vec<GraphPathFactorSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPathFactorSyntax {
    pub primary: GraphPathPrimarySyntax,
    pub quantifier: Option<GraphPatternQuantifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphPathPrimarySyntax {
    Vertex(GraphElementPatternSyntax),
    Edge(GraphEdgePatternSyntax),
    Parenthesized {
        path: Box<GraphPathSyntax>,
        predicate: Option<ExpressionSyntax>,
        span: Span,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphElementPatternSyntax {
    pub variable: Option<Identifier>,
    pub labels: Vec<Identifier>,
    pub predicate: Option<ExpressionSyntax>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphEdgePatternSyntax {
    pub direction: GraphEdgeDirection,
    pub variable: Option<Identifier>,
    pub labels: Vec<Identifier>,
    pub predicate: Option<ExpressionSyntax>,
    pub abbreviated: bool,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphEdgeDirection {
    Left,
    Right,
    Any,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPatternQuantifier {
    pub min: u32,
    pub max: u32,
    pub span: Span,
}

/// The time zone clause of `TIMESTAMP [ (p) ] { WITH | WITHOUT } TIME ZONE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeZoneSyntax {
    With,
    Without,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpressionSyntax {
    pub kind: ExpressionKindSyntax,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpressionKindSyntax {
    Column(QualifiedName),
    Wildcard(Option<QualifiedName>),
    Parameter(u32),
    Literal(LiteralSyntax),
    TypedString {
        data_type: Identifier,
        /// `WITH TIME ZONE` or `WITHOUT TIME ZONE` after the type name.
        time_zone: Option<TimeZoneSyntax>,
        value: Span,
    },
    Unary {
        operator: UnaryOperatorSyntax,
        expression: Box<ExpressionSyntax>,
    },
    Binary {
        left: Box<ExpressionSyntax>,
        operator: BinaryOperatorSyntax,
        right: Box<ExpressionSyntax>,
    },
    Like {
        expression: Box<ExpressionSyntax>,
        pattern: Box<ExpressionSyntax>,
        escape: Option<Box<ExpressionSyntax>>,
        negated: bool,
        case_insensitive: bool,
    },
    Filtered {
        function: Box<ExpressionSyntax>,
        predicate: Box<ExpressionSyntax>,
    },
    IsNull {
        expression: Box<ExpressionSyntax>,
        negated: bool,
    },
    InList {
        expression: Box<ExpressionSyntax>,
        values: Vec<ExpressionSyntax>,
        negated: bool,
    },
    Exists {
        query: Box<PostgresQuerySyntax>,
    },
    InSubquery {
        expression: Box<ExpressionSyntax>,
        query: Box<PostgresQuerySyntax>,
        negated: bool,
    },
    /// `ROW(a, b, ...)` or the PostgreSQL shorthand `(a, b, ...)`.
    ///
    /// A single parenthesized expression remains `Parenthesized`; the
    /// shorthand row form therefore always contains at least two fields.
    Row(Vec<ExpressionSyntax>),
    Between {
        expression: Box<ExpressionSyntax>,
        low: Box<ExpressionSyntax>,
        high: Box<ExpressionSyntax>,
        negated: bool,
    },
    Function {
        name: QualifiedName,
        arguments: Vec<ExpressionSyntax>,
        distinct: bool,
    },
    Cast {
        expression: Box<ExpressionSyntax>,
        data_type: QualifiedName,
        type_parameters: Vec<Span>,
        /// `WITH TIME ZONE` or `WITHOUT TIME ZONE` after the type modifiers.
        time_zone: Option<TimeZoneSyntax>,
    },
    Collate {
        expression: Box<ExpressionSyntax>,
        collation: QualifiedName,
    },
    Parenthesized(Box<ExpressionSyntax>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiteralSyntax {
    Null,
    Boolean(bool),
    Number(Span),
    String(Span),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOperatorSyntax {
    Not,
    Plus,
    Minus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperatorSyntax {
    Or,
    And,
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Concat,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTableColumn {
    pub expression: ExpressionSyntax,
    pub alias: Option<Identifier>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableAlias {
    pub name: Identifier,
    pub columns: Vec<Identifier>,
    pub span: Span,
}
