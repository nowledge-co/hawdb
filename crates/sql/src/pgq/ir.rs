use skein_sql_syntax::{
    BinaryOperatorSyntax, GraphEdgeDirection, GraphPatternQuantifier, Span, UnaryOperatorSyntax,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgqSlotId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgqElementKind {
    Vertex,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgqDataType {
    Unknown,
    Null,
    Boolean,
    Int64,
    Float64,
    String,
    Binary,
    Uuid,
    GraphElement(PgqElementKind),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundPgqLiteral {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    TypedString { data_type: String, value: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqExpression {
    pub kind: BoundPgqExpressionKind,
    pub data_type: PgqDataType,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundPgqExpressionKind {
    Variable(PgqSlotId),
    Property {
        slot: PgqSlotId,
        property: String,
    },
    OuterColumn(Vec<String>),
    Parameter(u32),
    Literal(BoundPgqLiteral),
    Unary {
        operator: UnaryOperatorSyntax,
        expression: Box<BoundPgqExpression>,
    },
    Binary {
        left: Box<BoundPgqExpression>,
        operator: BinaryOperatorSyntax,
        right: Box<BoundPgqExpression>,
    },
    IsNull {
        expression: Box<BoundPgqExpression>,
        negated: bool,
    },
    InList {
        expression: Box<BoundPgqExpression>,
        values: Vec<BoundPgqExpression>,
        negated: bool,
    },
    Between {
        expression: Box<BoundPgqExpression>,
        low: Box<BoundPgqExpression>,
        high: Box<BoundPgqExpression>,
        negated: bool,
    },
    Function {
        name: Vec<String>,
        arguments: Vec<BoundPgqExpression>,
        distinct: bool,
    },
    Cast {
        expression: Box<BoundPgqExpression>,
        data_type: Vec<String>,
    },
    Collate {
        expression: Box<BoundPgqExpression>,
        collation: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundPgqVariable {
    pub slot: PgqSlotId,
    pub name: Option<String>,
    pub kind: PgqElementKind,
    pub labels: Vec<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqGraphTable {
    pub graph_name: Vec<String>,
    pub variables: Vec<BoundPgqVariable>,
    pub pattern: BoundPgqGraphPattern,
    pub columns: Vec<BoundPgqColumn>,
    pub alias: Option<String>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqGraphPattern {
    pub paths: Vec<BoundPgqPath>,
    pub predicate: Option<BoundPgqExpression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqPath {
    pub factors: Vec<BoundPgqPathFactor>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqPathFactor {
    pub primary: BoundPgqPathPrimary,
    pub quantifier: Option<GraphPatternQuantifier>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BoundPgqPathPrimary {
    Vertex {
        slot: PgqSlotId,
        predicate: Option<BoundPgqExpression>,
    },
    Edge {
        slot: PgqSlotId,
        direction: GraphEdgeDirection,
        predicate: Option<BoundPgqExpression>,
    },
    Parenthesized {
        path: Box<BoundPgqPath>,
        predicate: Option<BoundPgqExpression>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundPgqColumn {
    pub name: String,
    pub expression: BoundPgqExpression,
    pub data_type: PgqDataType,
}
