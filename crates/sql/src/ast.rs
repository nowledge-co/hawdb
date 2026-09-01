use skein_core::{LogicalType, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Explain(SqlExplainStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExplainStatement {
    pub analyze: bool,
    pub statement: Box<SqlStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub projection: Vec<SelectProjection>,
    pub distinct: bool,
    pub from: SqlTableName,
    pub from_alias: Option<String>,
    pub joins: Vec<SqlJoin>,
    pub selection: Option<SqlPredicate>,
    pub group_by: Vec<SqlColumnRef>,
    pub order_by: Vec<SqlOrderItem>,
    pub limit: Option<SqlBound>,
    pub offset: Option<SqlBound>,
    pub lock_strength: Option<SqlLockStrength>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLockStrength {
    Share,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: SqlTableName,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub on_conflict: Option<SqlOnConflict>,
    pub returning: Vec<SqlColumnRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub assignments: Vec<SqlAssignment>,
    pub selection: Option<SqlPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub selection: Option<SqlPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlAssignment {
    pub column: String,
    pub value: SqlAssignmentValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlAssignmentValue {
    Value(SqlValue),
    Column(SqlColumnRef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOnConflict {
    pub columns: Vec<String>,
    pub action: SqlConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlConflictAction {
    DoNothing,
    DoUpdate(Vec<SqlAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table: SqlTableName,
    pub if_not_exists: bool,
    pub columns: Vec<SqlColumnDefinition>,
    pub constraints: Vec<SqlTableConstraint>,
    pub storage: SqlTableStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SqlTableStorage {
    #[default]
    RowPage,
    StrictAppend {
        partition_key: Vec<String>,
        order_key: Vec<String>,
        generated_order: SqlGeneratedOrder,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqlGeneratedOrder {
    #[default]
    CallerProvided,
    CommitSequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumnDefinition {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub default: Option<SqlValue>,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<SqlForeignKeyReference>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDataType {
    Boolean,
    BigInt,
    DoublePrecision,
    Text,
    Bytea,
}

impl SqlDataType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::Boolean => LogicalType::Boolean,
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Text => LogicalType::Text,
            Self::Bytea => LogicalType::Binary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlTableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        reference: SqlForeignKeyReference,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlForeignKeyReference {
    pub table: SqlTableName,
    pub columns: Vec<String>,
    pub on_delete: SqlReferentialAction,
    pub on_update: SqlReferentialAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: SqlTableName,
    pub columns: Vec<SqlOrderItem>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddColumnStatement {
    pub table: SqlTableName,
    pub if_not_exists: bool,
    pub column: SqlColumnDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Literal(Value),
    Parameter(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBound {
    Literal(u64),
    Parameter(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlTableName {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Wildcard,
    Column {
        name: SqlColumnRef,
        alias: Option<String>,
    },
    Expression {
        expression: SqlExpression,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpression {
    Column(SqlColumnRef),
    Value(SqlValue),
    Function {
        name: String,
        arguments: Vec<SqlFunctionArgument>,
        distinct: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFunctionArgument {
    Expression(SqlExpression),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlColumnRef {
    pub qualifier: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoin {
    pub kind: SqlJoinKind,
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub on: SqlPredicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlJoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOrderItem {
    pub column: SqlColumnRef,
    pub direction: SqlOrderDirection,
    pub nulls: SqlNullOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlNullOrder {
    DialectDefault,
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlPredicate {
    And(Box<SqlPredicate>, Box<SqlPredicate>),
    Or(Box<SqlPredicate>, Box<SqlPredicate>),
    Not(Box<SqlPredicate>),
    Compare {
        left: SqlColumnRef,
        op: SqlComparisonOp,
        right: SqlValue,
    },
    CompareColumns {
        left: SqlColumnRef,
        op: SqlComparisonOp,
        right: SqlColumnRef,
    },
    InList {
        left: SqlColumnRef,
        values: Vec<SqlValue>,
        negated: bool,
    },
    IsNull {
        column: SqlColumnRef,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlComparisonOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl std::fmt::Display for SqlValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(value) => write!(formatter, "{value}"),
            Self::Parameter(position) => write!(formatter, "${position}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_data_types_use_shared_logical_types() {
        assert_eq!(SqlDataType::Boolean.logical_type(), LogicalType::Boolean);
        assert_eq!(SqlDataType::BigInt.logical_type(), LogicalType::Int64);
        assert_eq!(
            SqlDataType::DoublePrecision.logical_type(),
            LogicalType::Float64
        );
        assert_eq!(SqlDataType::Text.logical_type(), LogicalType::Text);
        assert_eq!(SqlDataType::Bytea.logical_type(), LogicalType::Binary);
    }
}
