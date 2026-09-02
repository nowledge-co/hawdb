use super::{
    lower_column_expr, lower_sql_expression, lower_table_name, normalize_ident, object_name_parts,
};
use crate::ast::*;
use skein_core::{Result, SkeinError};
use sqlparser::ast::{
    AlterTableOperation, ColumnOption, CreateTableOptions, DataType, Expr, HiveDistributionStyle,
    ReferentialAction, SqlOption, TableConstraint, ValueWithSpan,
};

pub(super) fn lower_create_table_statement(
    create: &sqlparser::ast::CreateTable,
) -> Result<SqlStatement> {
    if create.or_replace
        || create.temporary
        || create.external
        || create.dynamic
        || create.global.is_some()
        || create.transient
        || create.volatile
        || create.iceberg
        || !matches!(create.hive_distribution, HiveDistributionStyle::NONE)
        || create.hive_formats.is_some()
        || create.file_format.is_some()
        || create.location.is_some()
        || create.query.is_some()
        || create.without_rowid
        || create.like.is_some()
        || create.clone.is_some()
        || create.version.is_some()
        || create.comment.is_some()
        || create.on_commit.is_some()
        || create.on_cluster.is_some()
        || create.primary_key.is_some()
        || create.order_by.is_some()
        || create.partition_by.is_some()
        || create.cluster_by.is_some()
        || create.clustered_by.is_some()
        || create.inherits.is_some()
        || create.partition_of.is_some()
        || create.for_values.is_some()
        || create.strict
        || create.copy_grants
        || create.enable_schema_evolution.is_some()
        || create.change_tracking.is_some()
        || create.data_retention_time_in_days.is_some()
        || create.max_data_extension_time_in_days.is_some()
        || create.default_ddl_collation.is_some()
        || create.with_aggregation_policy.is_some()
        || create.with_row_access_policy.is_some()
        || create.with_tags.is_some()
        || create.external_volume.is_some()
        || create.base_location.is_some()
        || create.catalog.is_some()
        || create.catalog_sync.is_some()
        || create.storage_serialization_policy.is_some()
        || create.target_lag.is_some()
        || create.warehouse.is_some()
        || create.refresh_mode.is_some()
        || create.initialize.is_some()
        || create.require_user
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL CREATE TABLE clause".to_string(),
        ));
    }
    Ok(SqlStatement::CreateTable(CreateTableStatement {
        table: lower_table_name(&create.name)?,
        if_not_exists: create.if_not_exists,
        columns: create
            .columns
            .iter()
            .map(lower_column_definition)
            .collect::<Result<_>>()?,
        constraints: create
            .constraints
            .iter()
            .map(lower_table_constraint)
            .collect::<Result<_>>()?,
        storage: lower_table_storage(&create.table_options)?,
    }))
}

fn lower_table_storage(options: &CreateTableOptions) -> Result<SqlTableStorage> {
    let options = match options {
        CreateTableOptions::None => return Ok(SqlTableStorage::RowPage),
        CreateTableOptions::With(options) => options,
        _ => {
            return Err(SkeinError::Semantic(
                "PostgreSQL CREATE TABLE storage options require WITH (...)".to_string(),
            ));
        }
    };
    let mut storage_mode = None;
    let mut partition_key = None;
    let mut order_key = None;
    let mut generated_order = None;
    for option in options {
        let SqlOption::KeyValue { key, value } = option else {
            return Err(SkeinError::Semantic(
                "PostgreSQL CREATE TABLE storage options require key = value entries".to_string(),
            ));
        };
        let name = normalize_ident(key);
        match name.as_str() {
            "storage_mode" => set_once(
                &mut storage_mode,
                lower_storage_mode(value)?,
                "storage_mode",
            )?,
            "partition_key" => set_once(
                &mut partition_key,
                lower_storage_key(value, "partition_key")?,
                "partition_key",
            )?,
            "order_key" => set_once(
                &mut order_key,
                lower_storage_key(value, "order_key")?,
                "order_key",
            )?,
            "generated_order" => set_once(
                &mut generated_order,
                lower_generated_order(value)?,
                "generated_order",
            )?,
            _ => {
                return Err(SkeinError::Semantic(format!(
                    "unsupported PostgreSQL CREATE TABLE storage option {name}"
                )));
            }
        }
    }
    match storage_mode.as_deref() {
        Some("strict_append") => Ok(SqlTableStorage::StrictAppend {
            partition_key: partition_key.ok_or_else(|| {
                SkeinError::Semantic("strict_append storage requires partition_key".to_string())
            })?,
            order_key: order_key.ok_or_else(|| {
                SkeinError::Semantic("strict_append storage requires order_key".to_string())
            })?,
            generated_order: generated_order.unwrap_or_default(),
        }),
        Some(mode) => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL CREATE TABLE storage_mode {mode}"
        ))),
        None if partition_key.is_none() && order_key.is_none() && generated_order.is_none() => {
            Ok(SqlTableStorage::RowPage)
        }
        None => Err(SkeinError::Semantic(
            "partition_key, order_key, and generated_order require storage_mode = 'strict_append'"
                .to_string(),
        )),
    }
}

fn set_once<T>(slot: &mut Option<T>, value: T, name: &str) -> Result<()> {
    if slot.replace(value).is_some() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL CREATE TABLE storage option {name} is specified more than once"
        )));
    }
    Ok(())
}

fn lower_storage_mode(value: &Expr) -> Result<String> {
    let Expr::Value(ValueWithSpan {
        value: sqlparser::ast::Value::SingleQuotedString(value),
        ..
    }) = value
    else {
        return Err(SkeinError::Semantic(
            "CREATE TABLE storage_mode must be a string literal".to_string(),
        ));
    };
    Ok(value.to_ascii_lowercase())
}

fn lower_generated_order(value: &Expr) -> Result<crate::SqlGeneratedOrder> {
    let Expr::Value(ValueWithSpan {
        value: sqlparser::ast::Value::SingleQuotedString(value),
        ..
    }) = value
    else {
        return Err(SkeinError::Semantic(
            "CREATE TABLE generated_order must be a string literal".to_string(),
        ));
    };
    match value.to_ascii_lowercase().as_str() {
        "commit_sequence" => Ok(crate::SqlGeneratedOrder::CommitSequence),
        value => Err(SkeinError::Semantic(format!(
            "unsupported strict_append generated_order {value}"
        ))),
    }
}

fn lower_storage_key(value: &Expr, name: &str) -> Result<Vec<String>> {
    let Expr::Value(ValueWithSpan {
        value: sqlparser::ast::Value::SingleQuotedString(value),
        ..
    }) = value
    else {
        return Err(SkeinError::Semantic(format!(
            "CREATE TABLE {name} must be a string literal"
        )));
    };
    if value.is_empty() {
        return Err(SkeinError::Semantic(format!(
            "CREATE TABLE {name} must not contain an empty column name"
        )));
    }
    Ok(vec![value.clone()])
}

fn lower_column_definition(column: &sqlparser::ast::ColumnDef) -> Result<SqlColumnDefinition> {
    let mut nullable = true;
    let mut default = None;
    let mut primary_key = false;
    let mut unique = false;
    let mut references = None;
    for option in &column.options {
        if option.name.is_some() {
            return Err(SkeinError::Semantic(
                "named column constraints are not supported".to_string(),
            ));
        }
        match &option.option {
            ColumnOption::Null => nullable = true,
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Default(expr) => default = Some(lower_column_default(expr)?),
            ColumnOption::PrimaryKey(_) => {
                primary_key = true;
                nullable = false;
            }
            ColumnOption::Unique(_) => unique = true,
            ColumnOption::ForeignKey(reference) => {
                references = Some(lower_foreign_key_reference(reference)?);
            }
            other => {
                return Err(SkeinError::Semantic(format!(
                    "unsupported PostgreSQL column option {other}"
                )));
            }
        }
    }
    Ok(SqlColumnDefinition {
        name: normalize_ident(&column.name),
        data_type: lower_data_type(&column.data_type)?,
        nullable,
        default,
        primary_key,
        unique,
        references,
    })
}

fn lower_column_default(expr: &Expr) -> Result<SqlColumnDefault> {
    match lower_sql_expression(expr)? {
        SqlExpression::Value(value) => Ok(SqlColumnDefault::Literal(value)),
        SqlExpression::Function {
            name,
            arguments,
            distinct: false,
            filter: None,
        } if name == "uuidv7" && arguments.is_empty() => Ok(SqlColumnDefault::UuidV7),
        SqlExpression::Function { name, .. } => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL column default function {name}"
        ))),
        SqlExpression::Column(_) => Err(SkeinError::Semantic(
            "column defaults cannot reference a column".to_string(),
        )),
    }
}

fn lower_data_type(data_type: &DataType) -> Result<SqlDataType> {
    match data_type {
        DataType::Boolean | DataType::Bool => Ok(SqlDataType::Boolean),
        DataType::BigInt(_) | DataType::Int8(_) => Ok(SqlDataType::BigInt),
        DataType::DoublePrecision | DataType::Float8 => Ok(SqlDataType::DoublePrecision),
        DataType::Text => Ok(SqlDataType::Text),
        DataType::Bytea => Ok(SqlDataType::Bytea),
        DataType::Uuid => Ok(SqlDataType::Uuid),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL data type {data_type}"
        ))),
    }
}

fn lower_table_constraint(constraint: &TableConstraint) -> Result<SqlTableConstraint> {
    match constraint {
        TableConstraint::PrimaryKey(constraint) => {
            if constraint.name.is_some()
                || constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
            {
                return Err(SkeinError::Semantic(
                    "unsupported PRIMARY KEY clause".to_string(),
                ));
            }
            Ok(SqlTableConstraint::PrimaryKey(lower_index_columns(
                &constraint.columns,
            )?))
        }
        TableConstraint::Unique(constraint) => {
            if constraint.name.is_some()
                || constraint.index_name.is_some()
                || constraint.index_type.is_some()
                || !constraint.index_options.is_empty()
                || constraint.characteristics.is_some()
                || !matches!(
                    constraint.nulls_distinct,
                    sqlparser::ast::NullsDistinctOption::None
                )
            {
                return Err(SkeinError::Semantic(
                    "unsupported UNIQUE clause".to_string(),
                ));
            }
            Ok(SqlTableConstraint::Unique(lower_index_columns(
                &constraint.columns,
            )?))
        }
        TableConstraint::ForeignKey(constraint) => {
            if constraint.name.is_some()
                || constraint.index_name.is_some()
                || constraint.match_kind.is_some()
                || constraint.characteristics.is_some()
            {
                return Err(SkeinError::Semantic(
                    "unsupported FOREIGN KEY clause".to_string(),
                ));
            }
            Ok(SqlTableConstraint::ForeignKey {
                columns: constraint.columns.iter().map(normalize_ident).collect(),
                reference: lower_foreign_key_reference(constraint)?,
            })
        }
        _ => Err(SkeinError::Semantic(
            "CHECK and inline index constraints are not supported".to_string(),
        )),
    }
}

fn lower_foreign_key_reference(
    reference: &sqlparser::ast::ForeignKeyConstraint,
) -> Result<SqlForeignKeyReference> {
    Ok(SqlForeignKeyReference {
        table: lower_table_name(&reference.foreign_table)?,
        columns: reference
            .referred_columns
            .iter()
            .map(normalize_ident)
            .collect(),
        on_delete: lower_referential_action(reference.on_delete)?,
        on_update: lower_referential_action(reference.on_update)?,
    })
}

fn lower_referential_action(action: Option<ReferentialAction>) -> Result<SqlReferentialAction> {
    match action.unwrap_or(ReferentialAction::NoAction) {
        ReferentialAction::NoAction => Ok(SqlReferentialAction::NoAction),
        ReferentialAction::Restrict => Ok(SqlReferentialAction::Restrict),
        ReferentialAction::Cascade => Ok(SqlReferentialAction::Cascade),
        ReferentialAction::SetNull => Ok(SqlReferentialAction::SetNull),
        ReferentialAction::SetDefault => Err(SkeinError::Semantic(
            "ON DELETE/UPDATE SET DEFAULT is not supported".to_string(),
        )),
    }
}

pub(super) fn lower_create_index_statement(
    create: &sqlparser::ast::CreateIndex,
) -> Result<SqlStatement> {
    if create.using.is_some()
        || create.concurrently
        || !create.include.is_empty()
        || create.nulls_distinct.is_some()
        || !create.with.is_empty()
        || create.predicate.is_some()
        || !create.index_options.is_empty()
        || !create.alter_options.is_empty()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL CREATE INDEX clause".to_string(),
        ));
    }
    let Some(name) = &create.name else {
        return Err(SkeinError::Semantic(
            "CREATE INDEX requires an explicit name".to_string(),
        ));
    };
    let name_parts = object_name_parts(name)?;
    let [name] = name_parts.as_slice() else {
        return Err(SkeinError::Semantic(
            "CREATE INDEX names must be unqualified".to_string(),
        ));
    };
    Ok(SqlStatement::CreateIndex(CreateIndexStatement {
        name: name.clone(),
        table: lower_table_name(&create.table_name)?,
        columns: create
            .columns
            .iter()
            .map(lower_index_order_item)
            .collect::<Result<_>>()?,
        unique: create.unique,
        if_not_exists: create.if_not_exists,
    }))
}

fn lower_index_columns(columns: &[sqlparser::ast::IndexColumn]) -> Result<Vec<String>> {
    columns
        .iter()
        .map(|column| {
            let item = lower_index_order_item(column)?;
            if item.direction != SqlOrderDirection::Asc
                || item.nulls != SqlNullOrder::DialectDefault
            {
                return Err(SkeinError::Semantic(
                    "constraint columns do not support ordering options".to_string(),
                ));
            }
            Ok(item.column.name)
        })
        .collect()
}

fn lower_index_order_item(column: &sqlparser::ast::IndexColumn) -> Result<SqlOrderItem> {
    if column.operator_class.is_some() {
        return Err(SkeinError::Semantic(
            "index operator classes are not supported".to_string(),
        ));
    }
    Ok(SqlOrderItem {
        column: lower_column_expr(&column.column.expr)?,
        direction: match column.column.options.asc {
            Some(false) => SqlOrderDirection::Desc,
            Some(true) | None => SqlOrderDirection::Asc,
        },
        nulls: match column.column.options.nulls_first {
            Some(true) => SqlNullOrder::First,
            Some(false) => SqlNullOrder::Last,
            None => SqlNullOrder::DialectDefault,
        },
    })
}

pub(super) fn lower_alter_table_statement(
    alter: &sqlparser::ast::AlterTable,
) -> Result<SqlStatement> {
    if alter.if_exists
        || alter.only
        || alter.location.is_some()
        || alter.on_cluster.is_some()
        || alter.table_type.is_some()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL ALTER TABLE clause".to_string(),
        ));
    }
    let [AlterTableOperation::AddColumn {
        if_not_exists,
        column_def,
        column_position,
        ..
    }] = alter.operations.as_slice()
    else {
        return Err(SkeinError::Semantic(
            "ALTER TABLE supports one ADD COLUMN operation".to_string(),
        ));
    };
    if column_position.is_some() {
        return Err(SkeinError::Semantic(
            "ALTER TABLE column positioning is not supported".to_string(),
        ));
    }
    Ok(SqlStatement::AlterTableAddColumn(
        AlterTableAddColumnStatement {
            table: lower_table_name(&alter.name)?,
            if_not_exists: *if_not_exists,
            column: lower_column_definition(column_def)?,
        },
    ))
}
