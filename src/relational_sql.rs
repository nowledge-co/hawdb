use crate::content_store_sql_corpus::{ContentStoreSqlCorpus, ContentStoreSqlStatementKind};
use crate::error::{Result, SkeinError};
use crate::sql::{
    AlterTableAddColumnStatement, CreateIndexStatement, CreateTableStatement, SqlAssignmentValue,
    SqlColumnDefinition, SqlComparisonOp, SqlConflictAction, SqlDataType, SqlPredicate,
    SqlReferentialAction, SqlStatement, SqlTableConstraint, SqlValue,
};
use crate::value::Value;
use skein_storage::{
    RelationalColumnSchema, RelationalComparisonOp, RelationalConflictAction,
    RelationalForeignKeySchema, RelationalIndexSchema, RelationalInsertMode, RelationalPredicate,
    RelationalReferentialAction, RelationalRow, RelationalScalarType, RelationalState,
    RelationalTableSchema, RelationalTransaction, RelationalUpdateAssignment,
    RelationalUpdateValue, RelationalUpsertAssignment, RelationalUpsertValue, RelationalValue,
    RelationalWrite,
};

#[allow(dead_code)]
mod query;

#[allow(unused_imports)]
pub(crate) use query::{
    execute_relational_query_sql, RelationalQueryLimits, RelationalQueryOutput,
};

pub(crate) fn compile_schema_corpus(
    corpus: &ContentStoreSqlCorpus,
) -> Result<RelationalTransaction> {
    let mut writes = Vec::new();
    for statement in corpus
        .statements
        .iter()
        .filter(|statement| statement.kind == ContentStoreSqlStatementKind::Schema)
    {
        let lowered = skein_sql::prepare_postgres_sql(&statement.sql)?.statement;
        writes.extend(compile_schema_statement(lowered)?);
    }
    Ok(RelationalTransaction { writes })
}

#[allow(dead_code)]
pub(crate) fn compile_relational_mutation_sql(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<RelationalTransaction> {
    let prepared = skein_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    compile_relational_mutation(prepared.statement, parameters, state)
}

pub(crate) fn compile_relational_statement_sql(
    sql: &str,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<RelationalTransaction> {
    let prepared = skein_sql::prepare_postgres_sql(sql)?;
    if prepared.parameters.len() != parameters.len() {
        return Err(SkeinError::Semantic(format!(
            "PostgreSQL statement requires {} parameters, but {} parameters were supplied",
            prepared.parameters.len(),
            parameters.len()
        )));
    }
    match prepared.statement {
        statement @ (SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_)) => Ok(RelationalTransaction {
            writes: compile_schema_statement(statement)?,
        }),
        statement => compile_relational_mutation(statement, parameters, state),
    }
}

fn compile_relational_mutation(
    statement: SqlStatement,
    parameters: &[Value],
    state: &RelationalState,
) -> Result<RelationalTransaction> {
    let write = match statement {
        SqlStatement::Insert(insert) => {
            reject_non_public_schema(insert.table.schema.as_deref())?;
            let schema = state.table_schema(&insert.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", insert.table.name))
            })?;
            let mut positions = Vec::with_capacity(insert.columns.len());
            let mut unique = std::collections::BTreeSet::new();
            for column in &insert.columns {
                let position = schema.column_position(column).ok_or_else(|| {
                    SkeinError::Semantic(format!("table {} has no column {column}", schema.name))
                })?;
                if !unique.insert(position) {
                    return Err(SkeinError::Semantic(format!(
                        "INSERT column {column} is specified more than once"
                    )));
                }
                positions.push(position);
            }
            let rows = insert
                .rows
                .into_iter()
                .map(|values| {
                    let mut row = schema
                        .columns
                        .iter()
                        .map(|column| column.default.clone().unwrap_or(RelationalValue::Null))
                        .collect::<Vec<_>>();
                    for ((position, value), column_name) in
                        positions.iter().zip(values).zip(insert.columns.iter())
                    {
                        row[*position] =
                            bind_relational_value(value, parameters).map_err(|error| {
                                SkeinError::Semantic(format!(
                                    "failed to bind INSERT column {column_name}: {error}"
                                ))
                            })?;
                    }
                    Ok(RelationalRow::new(row))
                })
                .collect::<Result<Vec<_>>>()?;
            if let Some(conflict) = insert.on_conflict {
                let action = match conflict.action {
                    SqlConflictAction::DoNothing => RelationalConflictAction::DoNothing,
                    SqlConflictAction::DoUpdate(assignments) => RelationalConflictAction::Update(
                        assignments
                            .into_iter()
                            .map(|assignment| {
                                let value = match assignment.value {
                                    SqlAssignmentValue::Column(column)
                                        if column.qualifier.as_deref() == Some("excluded") =>
                                    {
                                        RelationalUpsertValue::ExcludedColumn(column.name)
                                    }
                                    SqlAssignmentValue::Column(_) => {
                                        return Err(SkeinError::Semantic(
                                            "ON CONFLICT assignments only support EXCLUDED columns"
                                                .to_string(),
                                        ));
                                    }
                                    SqlAssignmentValue::Value(value) => {
                                        RelationalUpsertValue::Value(bind_relational_value(
                                            value, parameters,
                                        )?)
                                    }
                                };
                                Ok(RelationalUpsertAssignment {
                                    column: assignment.column,
                                    value,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                };
                RelationalWrite::Upsert {
                    table: insert.table.name,
                    rows,
                    conflict_columns: conflict.columns,
                    action,
                }
            } else {
                RelationalWrite::Insert {
                    table: insert.table.name,
                    rows,
                    mode: RelationalInsertMode::Error,
                }
            }
        }
        SqlStatement::Delete(delete) => {
            reject_non_public_schema(delete.table.schema.as_deref())?;
            let schema = state.table_schema(&delete.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", delete.table.name))
            })?;
            let selection = delete.selection.ok_or_else(|| {
                SkeinError::Semantic(
                    "unbounded relational DELETE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            RelationalWrite::DeleteWhere {
                table: delete.table.name.clone(),
                predicate: compile_mutation_predicate(
                    selection,
                    parameters,
                    schema,
                    delete.alias.as_deref(),
                    &delete.table.name,
                )?,
            }
        }
        SqlStatement::Update(update) => {
            reject_non_public_schema(update.table.schema.as_deref())?;
            let schema = state.table_schema(&update.table.name).ok_or_else(|| {
                SkeinError::Semantic(format!("unknown relational table {}", update.table.name))
            })?;
            let selection = update.selection.ok_or_else(|| {
                SkeinError::Semantic(
                    "unbounded relational UPDATE requires an explicit qualified workflow"
                        .to_string(),
                )
            })?;
            let assignments = update
                .assignments
                .into_iter()
                .map(|assignment| {
                    if schema.column_position(&assignment.column).is_none() {
                        return Err(SkeinError::Semantic(format!(
                            "table {} has no column {}",
                            schema.name, assignment.column
                        )));
                    }
                    let value = match assignment.value {
                        SqlAssignmentValue::Column(column) => {
                            validate_mutation_column(
                                &column,
                                schema,
                                update.alias.as_deref(),
                                &update.table.name,
                            )?;
                            RelationalUpdateValue::Column(column.name)
                        }
                        SqlAssignmentValue::Value(value) => {
                            RelationalUpdateValue::Value(bind_relational_value(value, parameters)?)
                        }
                    };
                    Ok(RelationalUpdateAssignment {
                        column: assignment.column,
                        value,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            RelationalWrite::UpdateWhere {
                table: update.table.name.clone(),
                assignments,
                predicate: compile_mutation_predicate(
                    selection,
                    parameters,
                    schema,
                    update.alias.as_deref(),
                    &update.table.name,
                )?,
            }
        }
        SqlStatement::Select(_)
        | SqlStatement::CreateTable(_)
        | SqlStatement::CreateIndex(_)
        | SqlStatement::AlterTableAddColumn(_) => {
            return Err(SkeinError::Semantic(
                "relational mutation entrypoint requires INSERT, UPDATE, or DELETE".to_string(),
            ));
        }
    };
    Ok(RelationalTransaction {
        writes: vec![write],
    })
}

fn compile_mutation_predicate(
    predicate: SqlPredicate,
    parameters: &[Value],
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<RelationalPredicate> {
    let compile =
        |predicate| compile_mutation_predicate(predicate, parameters, schema, alias, table);
    Ok(match predicate {
        SqlPredicate::And(left, right) => {
            RelationalPredicate::And(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        SqlPredicate::Or(left, right) => {
            RelationalPredicate::Or(Box::new(compile(*left)?), Box::new(compile(*right)?))
        }
        SqlPredicate::Not(predicate) => RelationalPredicate::Not(Box::new(compile(*predicate)?)),
        SqlPredicate::Compare { left, op, right } => {
            validate_mutation_column(&left, schema, alias, table)?;
            RelationalPredicate::Compare {
                column: left.name,
                op: compile_comparison_op(op),
                value: bind_relational_value(right, parameters)?,
            }
        }
        SqlPredicate::CompareColumns { .. } => {
            return Err(SkeinError::Semantic(
                "single-table mutation predicates do not support column-to-column comparison"
                    .to_string(),
            ));
        }
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => {
            validate_mutation_column(&left, schema, alias, table)?;
            let mut predicates = values
                .into_iter()
                .map(|value| {
                    Ok(RelationalPredicate::Compare {
                        column: left.name.clone(),
                        op: if negated {
                            RelationalComparisonOp::NotEq
                        } else {
                            RelationalComparisonOp::Eq
                        },
                        value: bind_relational_value(value, parameters)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter();
            let first = predicates.next().ok_or_else(|| {
                SkeinError::Semantic("mutation IN list must not be empty".to_string())
            })?;
            predicates.fold(first, |left, right| {
                if negated {
                    RelationalPredicate::And(Box::new(left), Box::new(right))
                } else {
                    RelationalPredicate::Or(Box::new(left), Box::new(right))
                }
            })
        }
        SqlPredicate::IsNull { column, negated } => {
            validate_mutation_column(&column, schema, alias, table)?;
            RelationalPredicate::IsNull {
                column: column.name,
                negated,
            }
        }
    })
}

fn validate_mutation_column(
    column: &crate::sql::SqlColumnRef,
    schema: &RelationalTableSchema,
    alias: Option<&str>,
    table: &str,
) -> Result<()> {
    if column
        .qualifier
        .as_deref()
        .is_some_and(|qualifier| Some(qualifier) != alias && qualifier != table)
    {
        return Err(SkeinError::Semantic(format!(
            "mutation predicate has unknown qualifier {}",
            column.qualifier.as_deref().unwrap_or_default()
        )));
    }
    if schema.column_position(&column.name).is_none() {
        return Err(SkeinError::Semantic(format!(
            "table {} has no column {}",
            schema.name, column.name
        )));
    }
    Ok(())
}

fn compile_comparison_op(op: SqlComparisonOp) -> RelationalComparisonOp {
    match op {
        SqlComparisonOp::Eq => RelationalComparisonOp::Eq,
        SqlComparisonOp::NotEq => RelationalComparisonOp::NotEq,
        SqlComparisonOp::Lt => RelationalComparisonOp::Lt,
        SqlComparisonOp::Lte => RelationalComparisonOp::Lte,
        SqlComparisonOp::Gt => RelationalComparisonOp::Gt,
        SqlComparisonOp::Gte => RelationalComparisonOp::Gte,
    }
}

fn bind_relational_value(value: SqlValue, parameters: &[Value]) -> Result<RelationalValue> {
    let value = match value {
        SqlValue::Literal(value) => value,
        SqlValue::Parameter(position) => parameters
            .get(position.saturating_sub(1))
            .cloned()
            .ok_or_else(|| {
                SkeinError::Semantic(format!("missing PostgreSQL parameter ${position}"))
            })?,
    };
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational SQL parameters must be scalar".to_string(),
        )),
    }
}

fn compile_schema_statement(statement: SqlStatement) -> Result<Vec<RelationalWrite>> {
    match statement {
        SqlStatement::CreateTable(create) => Ok(vec![RelationalWrite::CreateTable(
            compile_create_table(create)?,
        )]),
        SqlStatement::CreateIndex(create) => Ok(vec![compile_create_index(create)?]),
        SqlStatement::AlterTableAddColumn(alter) => compile_add_column(alter),
        _ => Err(SkeinError::Semantic(
            "content-store schema corpus contains a non-schema statement".to_string(),
        )),
    }
}

fn compile_create_table(create: CreateTableStatement) -> Result<RelationalTableSchema> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if create.if_not_exists {
        return Err(SkeinError::Semantic(
            "content-store schema must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    let mut primary_key = Vec::new();
    let mut unique_constraints = Vec::new();
    let mut foreign_keys = Vec::new();
    for column in &create.columns {
        if column.primary_key {
            if !primary_key.is_empty() {
                return Err(SkeinError::Semantic(
                    "table declares more than one primary key".to_string(),
                ));
            }
            primary_key.push(column.name.clone());
        }
        if column.unique {
            unique_constraints.push(vec![column.name.clone()]);
        }
        if let Some(reference) = &column.references {
            foreign_keys.push(RelationalForeignKeySchema {
                columns: vec![column.name.clone()],
                referenced_table: reference.table.name.clone(),
                referenced_columns: reference.columns.clone(),
                on_delete: compile_referential_action(reference.on_delete)?,
                on_update: compile_referential_action(reference.on_update)?,
            });
        }
    }
    for constraint in create.constraints {
        match constraint {
            SqlTableConstraint::PrimaryKey(columns) => {
                if !primary_key.is_empty() {
                    return Err(SkeinError::Semantic(
                        "table declares more than one primary key".to_string(),
                    ));
                }
                primary_key = columns;
            }
            SqlTableConstraint::Unique(columns) => unique_constraints.push(columns),
            SqlTableConstraint::ForeignKey { columns, reference } => {
                foreign_keys.push(RelationalForeignKeySchema {
                    columns,
                    referenced_table: reference.table.name,
                    referenced_columns: reference.columns,
                    on_delete: compile_referential_action(reference.on_delete)?,
                    on_update: compile_referential_action(reference.on_update)?,
                });
            }
        }
    }
    if primary_key.is_empty() {
        return Err(SkeinError::Semantic(format!(
            "relational table {} must declare a primary key",
            create.table.name
        )));
    }
    let column_names = create
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut primary_key_columns = std::collections::BTreeSet::new();
    for column in &primary_key {
        if !column_names.contains(column.as_str()) || !primary_key_columns.insert(column.clone()) {
            return Err(SkeinError::Semantic(format!(
                "primary key references unknown or duplicate column {column}"
            )));
        }
    }
    let columns = create
        .columns
        .into_iter()
        .map(|column| {
            let is_primary_key = primary_key_columns.contains(&column.name);
            let mut column = compile_column(column)?;
            if is_primary_key {
                column.nullable = false;
            }
            Ok(column)
        })
        .collect::<Result<_>>()?;
    Ok(RelationalTableSchema {
        name: create.table.name,
        columns,
        primary_key,
        unique_constraints,
        foreign_keys,
        indexes: Vec::new(),
    })
}

fn compile_column(column: SqlColumnDefinition) -> Result<RelationalColumnSchema> {
    Ok(RelationalColumnSchema {
        name: column.name,
        scalar_type: compile_data_type(column.data_type),
        nullable: column.nullable,
        default: column.default.map(compile_schema_value).transpose()?,
    })
}

fn compile_data_type(data_type: SqlDataType) -> RelationalScalarType {
    match data_type {
        SqlDataType::Boolean => RelationalScalarType::Boolean,
        SqlDataType::BigInt => RelationalScalarType::BigInt,
        SqlDataType::DoublePrecision => RelationalScalarType::DoublePrecision,
        SqlDataType::Text => RelationalScalarType::Text,
        SqlDataType::Bytea => RelationalScalarType::Bytea,
    }
}

fn compile_schema_value(value: SqlValue) -> Result<RelationalValue> {
    let SqlValue::Literal(value) = value else {
        return Err(SkeinError::Semantic(
            "schema defaults cannot contain parameters".to_string(),
        ));
    };
    match value {
        Value::Null => Ok(RelationalValue::Null),
        Value::Bool(value) => Ok(RelationalValue::Boolean(value)),
        Value::Int(value) => Ok(RelationalValue::BigInt(value)),
        Value::Float(value) => Ok(RelationalValue::DoublePrecision(value)),
        Value::String(value) => Ok(RelationalValue::Text(value)),
        Value::List(_) | Value::Map(_) => Err(SkeinError::Semantic(
            "relational schema defaults must be scalar".to_string(),
        )),
    }
}

fn compile_referential_action(action: SqlReferentialAction) -> Result<RelationalReferentialAction> {
    match action {
        SqlReferentialAction::NoAction => Ok(RelationalReferentialAction::NoAction),
        SqlReferentialAction::Restrict => Ok(RelationalReferentialAction::Restrict),
        SqlReferentialAction::Cascade | SqlReferentialAction::SetNull => Err(SkeinError::Semantic(
            "CASCADE and SET NULL require a qualified relational mutation executor".to_string(),
        )),
    }
}

fn compile_create_index(create: CreateIndexStatement) -> Result<RelationalWrite> {
    reject_non_public_schema(create.table.schema.as_deref())?;
    if create.if_not_exists {
        return Err(SkeinError::Semantic(
            "content-store indexes must not hide drift with IF NOT EXISTS".to_string(),
        ));
    }
    Ok(RelationalWrite::CreateIndex {
        table: create.table.name,
        index: RelationalIndexSchema {
            name: create.name,
            columns: create
                .columns
                .into_iter()
                .map(|column| column.column.name)
                .collect(),
            unique: create.unique,
        },
    })
}

fn compile_add_column(alter: AlterTableAddColumnStatement) -> Result<Vec<RelationalWrite>> {
    reject_non_public_schema(alter.table.schema.as_deref())?;
    let _ = alter;
    Err(SkeinError::Semantic(
        "ALTER TABLE ADD COLUMN requires durable catalog-version publication".to_string(),
    ))
}

fn reject_non_public_schema(schema: Option<&str>) -> Result<()> {
    if schema.is_some_and(|schema| schema != "public") {
        return Err(SkeinError::Semantic(
            "relational content tables must use the public schema".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use skein_storage::{RelationalKey, RelationalStore};
    use std::collections::BTreeMap;

    #[test]
    fn materializes_embedded_content_store_schema_as_relational_tables() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let transaction = compile_schema_corpus(&corpus).expect("compiled schema transaction");
        let store = RelationalStore::default();
        let snapshot = store
            .commit(transaction, |_, _| Ok(()))
            .expect("materialized relational schema");

        assert_eq!(
            snapshot
                .value()
                .table_schema("thread_messages")
                .expect("thread_messages schema")
                .primary_key,
            ["content_message_id"]
        );
        assert!(snapshot
            .value()
            .table_schema("content_chunks")
            .expect("content_chunks schema")
            .unique_constraints
            .contains(&vec![
                "content_doc_id".to_string(),
                "chunk_index".to_string()
            ]));
        let anchors = snapshot
            .value()
            .table_schema("content_anchors")
            .expect("content_anchors schema");
        assert_eq!(anchors.foreign_keys.len(), 1);
        assert_eq!(anchors.indexes.len(), 3);
        assert!(anchors.column_position("quote_hash").is_some());
        assert!(anchors.column_position("content_message_id").is_some());
        assert!(anchors.column_position("content_hash").is_none());
        assert!(anchors.column_position("updated_at").is_none());
    }

    #[test]
    fn database_sql_entrypoint_executes_relational_ddl_dml_and_select() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY, body TEXT NOT NULL)")
            .expect("create relational table");
        database
            .query_sql_with_params(
                "INSERT INTO public.messages (id, body) VALUES ($1, $2)",
                &[
                    Value::String("message-1".to_string()),
                    Value::String("payload".to_string()),
                ],
            )
            .expect("insert relational row");

        let output = database
            .query_sql_with_params(
                "SELECT id, body FROM public.messages WHERE id = $1",
                &[Value::String("message-1".to_string())],
            )
            .expect("query relational row");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0],
            BTreeMap::from([
                ("body".to_string(), Value::String("payload".to_string())),
                ("id".to_string(), Value::String("message-1".to_string())),
            ])
        );

        database
            .query_sql_with_params(
                "INSERT INTO public.messages (id, body) VALUES ($1, $2)",
                &[
                    Value::String("message-2".to_string()),
                    Value::String("payload-2".to_string()),
                ],
            )
            .expect("insert second relational row");
        let count = database
            .query_sql_bounded(
                "SELECT COUNT(*) AS message_count FROM public.messages",
                Some(1),
            )
            .expect("aggregate with one output row scans the configured intermediate budget");
        assert_eq!(count.rows[0]["message_count"], Value::Int(2));

        let snapshot = database.begin_read_transaction();
        let snapshot_output = snapshot
            .query_sql("SELECT id FROM public.messages ORDER BY id")
            .expect("query relational snapshot");
        assert_eq!(snapshot_output.rows.len(), 2);
        assert_eq!(
            snapshot_output.rows[0]["id"],
            Value::String("message-1".to_string())
        );
    }

    #[test]
    fn database_transaction_commits_cypher_and_sql_in_one_epoch() {
        let mut database = Database::new();
        {
            let mut transaction = database.begin_transaction();
            transaction
                .query("CREATE (:Marker {id: 'graph-1'})")
                .expect("stage graph mutation");
            transaction
                .query_sql("CREATE TABLE public.messages (id TEXT PRIMARY KEY)")
                .expect("stage relational schema");
            transaction
                .query_sql_with_params(
                    "INSERT INTO public.messages (id) VALUES ($1)",
                    &[Value::String("message-1".to_string())],
                )
                .expect("stage relational row");
            let staged = transaction
                .query_sql("SELECT id FROM public.messages")
                .expect("read staged relational row");
            assert_eq!(staged.rows.len(), 1);
            transaction.commit().expect("commit mixed transaction");
        }

        assert_eq!(database.commit_epoch(), 1);
        assert_eq!(
            database
                .query("MATCH (m:Marker) RETURN m.id AS id")
                .expect("read committed graph row")
                .rows
                .len(),
            1
        );
        assert_eq!(
            database
                .query_sql("SELECT id FROM public.messages")
                .expect("read committed relational row")
                .rows
                .len(),
            1
        );
    }

    #[test]
    fn relational_ddl_requires_exactly_one_primary_key_declaration() {
        let missing = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (id TEXT NOT NULL, payload TEXT NOT NULL)",
        )
        .expect("valid PostgreSQL syntax");
        let error = compile_schema_statement(missing.statement)
            .expect_err("Skein relational tables require a primary key");
        assert_eq!(
            error,
            SkeinError::Semantic(
                "relational table documents must declare a primary key".to_string()
            )
        );

        let duplicate = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (tenant_id TEXT PRIMARY KEY, id TEXT PRIMARY KEY)",
        )
        .expect("parser preserves duplicate declarations for semantic validation");
        let error = compile_schema_statement(duplicate.statement)
            .expect_err("multiple primary-key declarations must be rejected");
        assert_eq!(
            error,
            SkeinError::Semantic("table declares more than one primary key".to_string())
        );
    }

    #[test]
    fn table_level_composite_primary_key_is_not_nullable() {
        let prepared = skein_sql::prepare_postgres_sql(
            "CREATE TABLE documents (tenant_id TEXT, id TEXT, payload TEXT, \
             PRIMARY KEY (tenant_id, id))",
        )
        .expect("valid composite primary key");
        let writes = compile_schema_statement(prepared.statement).expect("compiled schema");
        let [RelationalWrite::CreateTable(schema)] = writes.as_slice() else {
            panic!("expected one CREATE TABLE write");
        };

        assert_eq!(schema.primary_key, ["tenant_id", "id"]);
        assert!(!schema.columns[0].nullable);
        assert!(!schema.columns[1].nullable);
        assert!(schema.columns[2].nullable);
    }

    #[test]
    fn every_content_store_mutation_compiles_against_materialized_schema() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let store = RelationalStore::default();
        store
            .commit(
                compile_schema_corpus(&corpus).expect("compiled schema"),
                |_, _| Ok(()),
            )
            .expect("materialized schema");
        let snapshot = store.snapshot().expect("schema snapshot");

        for statement in corpus
            .statements
            .iter()
            .filter(|statement| statement.kind == ContentStoreSqlStatementKind::Mutation)
        {
            let parameters = statement
                .parameters
                .iter()
                .enumerate()
                .map(|(position, data_type)| sample_parameter(data_type, position))
                .collect::<Vec<_>>();
            compile_relational_mutation_sql(&statement.sql, &parameters, snapshot.value())
                .unwrap_or_else(|error| panic!("{} did not compile: {error}", statement.name));
        }
    }

    #[test]
    fn every_content_store_read_executes_against_materialized_schema() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let store = RelationalStore::default();
        store
            .commit(
                compile_schema_corpus(&corpus).expect("compiled schema"),
                |_, _| Ok(()),
            )
            .expect("materialized schema");
        let snapshot = store.snapshot().expect("schema snapshot");

        for statement in corpus
            .statements
            .iter()
            .filter(|statement| statement.kind == ContentStoreSqlStatementKind::Read)
        {
            let parameters = statement
                .parameters
                .iter()
                .enumerate()
                .map(|(position, data_type)| sample_parameter(data_type, position + 1))
                .collect::<Vec<_>>();
            execute_relational_query_sql(
                &statement.sql,
                &parameters,
                snapshot.value(),
                RelationalQueryLimits {
                    max_output_rows: statement.max_rows,
                    max_output_payload_bytes: statement.max_payload_bytes,
                    max_intermediate_rows: 1_000_000,
                    blocking_operator_bytes: std::num::NonZeroUsize::new(64 * 1024 * 1024)
                        .expect("non-zero aggregate memory budget"),
                    hydration: skein_storage::RelationalHydrationBudget::default(),
                },
            )
            .unwrap_or_else(|error| panic!("{} did not execute: {error}", statement.name));
        }
    }

    #[test]
    fn content_document_upsert_uses_unique_owner_target_and_defaults() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let store = RelationalStore::default();
        store
            .commit(
                compile_schema_corpus(&corpus).expect("compiled schema"),
                |_, _| Ok(()),
            )
            .expect("materialized schema");
        let statement = corpus
            .statements
            .iter()
            .find(|statement| statement.name == "upsert_content_document")
            .expect("upsert statement");

        for (document_id, media_type, updated_at) in [
            ("doc-1", "text/plain", "2026-08-07T00:00:00Z"),
            ("doc-2", "text/markdown", "2026-08-07T00:01:00Z"),
        ] {
            let parameters = vec![
                Value::String(document_id.to_string()),
                Value::String("thread".to_string()),
                Value::String("thread-1".to_string()),
                Value::String("default".to_string()),
                Value::String(media_type.to_string()),
                Value::Int(1),
                Value::String("2026-08-07T00:00:00Z".to_string()),
                Value::String(updated_at.to_string()),
            ];
            let snapshot = store.snapshot().expect("snapshot before upsert");
            let transaction =
                compile_relational_mutation_sql(&statement.sql, &parameters, snapshot.value())
                    .expect("compiled upsert");
            store
                .commit(transaction, |_, _| Ok(()))
                .expect("committed upsert");
        }

        let snapshot = store.snapshot().expect("snapshot after upsert");
        assert_eq!(snapshot.value().row_count("content_documents"), 1);
        let key = RelationalKey(vec![RelationalValue::Text("doc-1".to_string())]);
        let row = snapshot
            .value()
            .row("content_documents", &key)
            .expect("original primary key is preserved");
        assert_eq!(
            row.values()[4],
            RelationalValue::Text("text/markdown".to_string())
        );
        assert_eq!(row.values()[6], RelationalValue::Text(String::new()));
        assert_eq!(row.values()[7], RelationalValue::BigInt(0));
    }

    #[test]
    fn parameterized_update_uses_sql_snapshot_semantics() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let store = RelationalStore::default();
        store
            .commit(
                compile_schema_corpus(&corpus).expect("compiled schema"),
                |_, _| Ok(()),
            )
            .expect("materialized schema");
        let insert = compile_relational_mutation_sql(
            "INSERT INTO content_migration_state (key, value, updated_at) VALUES ($1, $2, $3)",
            &[
                Value::String("revision".to_string()),
                Value::String("pending".to_string()),
                Value::String("t0".to_string()),
            ],
            store.snapshot().expect("schema snapshot").value(),
        )
        .expect("compiled insert");
        store.commit(insert, |_, _| Ok(())).expect("inserted row");

        let update = compile_relational_mutation_sql(
            "UPDATE content_migration_state SET value = $1, updated_at = value WHERE key = $2",
            &[
                Value::String("qualified".to_string()),
                Value::String("revision".to_string()),
            ],
            store.snapshot().expect("insert snapshot").value(),
        )
        .expect("compiled update");
        store.commit(update, |_, _| Ok(())).expect("updated row");

        let key = RelationalKey(vec![RelationalValue::Text("revision".to_string())]);
        let snapshot = store.snapshot().expect("updated snapshot");
        let row = snapshot
            .value()
            .row("content_migration_state", &key)
            .expect("migration row");
        assert_eq!(
            row.values()[1],
            RelationalValue::Text("qualified".to_string())
        );
        assert_eq!(
            row.values()[2],
            RelationalValue::Text("pending".to_string())
        );
    }

    #[test]
    fn relational_queries_join_aggregate_and_hydrate_after_limit() {
        let corpus = crate::nowledge_content_store_sql_corpus().expect("valid corpus");
        let store = RelationalStore::default();
        store
            .commit(
                compile_schema_corpus(&corpus).expect("compiled schema"),
                |_, _| Ok(()),
            )
            .expect("materialized schema");

        commit_named_mutation(
            &store,
            &corpus,
            "upsert_content_document",
            vec![
                text("doc-thread"),
                text("thread"),
                text("thread-1"),
                text("default"),
                text("text/plain"),
                Value::Int(1),
                text("t0"),
                text("t0"),
            ],
        );
        for (id, order) in [("content-message-1", 1), ("content-message-2", 2)] {
            commit_named_mutation(
                &store,
                &corpus,
                "upsert_thread_message",
                vec![
                    text(id),
                    text(&format!("message-{order}")),
                    text("thread-storage-1"),
                    text("thread-1"),
                    text("doc-thread"),
                    text("default"),
                    Value::Int(order),
                    text("user"),
                    text(&format!("body-{order}-{}", "x".repeat(8 * 1024))),
                    Value::Null,
                    Value::Int(10),
                    text("{}"),
                    text(&format!("external-{order}")),
                    Value::Bool(false),
                    text(&format!("hash-{order}")),
                    text("t0"),
                    text("t0"),
                ],
            );
        }
        commit_named_mutation(
            &store,
            &corpus,
            "upsert_memory_message_anchor",
            vec![
                text("anchor-1"),
                text("memory-1"),
                text("doc-thread"),
                text("thread-storage-1"),
                text("content-message-1"),
                text("message-1"),
                Value::Int(1),
                text("hash-1"),
                text("{}"),
                text("t0"),
            ],
        );
        commit_named_mutation(
            &store,
            &corpus,
            "upsert_content_document",
            vec![
                text("doc-source"),
                text("source"),
                text("source-1"),
                text("default"),
                text("text/plain"),
                Value::Int(1),
                text("t0"),
                text("t0"),
            ],
        );
        commit_named_mutation(
            &store,
            &corpus,
            "insert_source_chunk",
            vec![
                text("chunk-1"),
                text("doc-source"),
                Value::Int(0),
                text("source body"),
                Value::Int(0),
                Value::Int(11),
                Value::Int(2),
                text("{}"),
                text("chunk-hash"),
                text("t0"),
                text("t0"),
            ],
        );
        commit_named_mutation(
            &store,
            &corpus,
            "upsert_content_migration_state",
            vec![text("revision"), text("qualified"), text("t0")],
        );

        let owner_lookup = execute_relational_query_sql(
            "SELECT content_doc_id FROM content_documents \
             WHERE owner_kind = 'thread' AND owner_id = $1",
            &[text("thread-1")],
            store.snapshot().expect("owner lookup snapshot").value(),
            query_limits(1, 4096),
        )
        .expect("composite owner lookup");
        assert_eq!(owner_lookup.rows.len(), 1);
        assert_eq!(owner_lookup.rows[0]["content_doc_id"], text("doc-thread"));
        assert_eq!(owner_lookup.access_path.name, "__unique_0");
        assert_eq!(owner_lookup.access_path.equality_prefix_len, 2);
        assert!(owner_lookup.access_path.unique_point);
        assert_eq!(owner_lookup.intermediate_rows, 1);

        let message_lookup = execute_relational_query_sql(
            "SELECT content_message_id FROM thread_messages \
             WHERE thread_storage_id = $1 AND order_index = $2",
            &[text("thread-storage-1"), Value::Int(2)],
            store.snapshot().expect("message lookup snapshot").value(),
            query_limits(1, 4096),
        )
        .expect("composite message lookup");
        assert_eq!(message_lookup.rows.len(), 1);
        assert_eq!(
            message_lookup.rows[0]["content_message_id"],
            text("content-message-2")
        );
        assert_eq!(message_lookup.access_path.name, "idx_thread_messages_order");
        assert_eq!(message_lookup.access_path.equality_prefix_len, 2);
        assert_eq!(message_lookup.access_path.estimated_rows, 1);

        let trailing_only = execute_relational_query_sql(
            "SELECT content_message_id FROM thread_messages WHERE order_index = $1",
            &[Value::Int(2)],
            store.snapshot().expect("trailing lookup snapshot").value(),
            query_limits(2, 4096),
        )
        .expect("trailing-column lookup");
        assert_eq!(trailing_only.rows.len(), 1);
        assert_eq!(trailing_only.access_path.name, "__full_scan");

        let page = execute_relational_query_sql(
            &named_statement(&corpus, "thread_messages_page").sql,
            &[text("thread-storage-1"), Value::Int(1), Value::Int(1)],
            store.snapshot().expect("query snapshot").value(),
            query_limits(1, 64 * 1024),
        )
        .expect("bounded message page");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0]["order_index"], Value::Int(2));
        assert_eq!(page.hydration.hydrated_rows, 1);
        assert!(page.hydration.decompressed_bytes > 8 * 1024);

        let summary = execute_relational_query_sql(
            &named_statement(&corpus, "thread_message_summary").sql,
            &[text("thread-storage-1")],
            store.snapshot().expect("summary snapshot").value(),
            query_limits(1, 4096),
        )
        .expect("message summary");
        assert_eq!(summary.rows[0]["message_count"], Value::Int(2));
        assert_eq!(summary.rows[0]["token_count"], Value::Int(20));
        assert_eq!(summary.hydration.hydrated_rows, 0);

        let mut constrained_limits = query_limits(1, 4096);
        constrained_limits.blocking_operator_bytes =
            std::num::NonZeroUsize::new(1).expect("non-zero aggregate memory budget");
        let error = execute_relational_query_sql(
            &named_statement(&corpus, "thread_message_summary").sql,
            &[text("thread-storage-1")],
            store.snapshot().expect("bounded summary snapshot").value(),
            constrained_limits,
        )
        .expect_err("aggregate must honor the shared blocking memory budget");
        assert!(error.to_string().contains("blocking_operator_bytes"));

        let covered = execute_relational_query_sql(
            &named_statement(&corpus, "thread_covered_message_count").sql,
            &[text("thread-storage-1")],
            store.snapshot().expect("anchor snapshot").value(),
            query_limits(1, 4096),
        )
        .expect("covered message count");
        assert_eq!(covered.rows[0]["covered_messages"], Value::Int(1));
        assert_eq!(covered.join_access_paths.len(), 1);
        assert_eq!(
            covered.join_access_paths[0].name,
            "idx_content_anchors_content_message"
        );
        assert_eq!(covered.join_access_paths[0].equality_prefix_len, 1);

        let source_page = execute_relational_query_sql(
            &named_statement(&corpus, "source_chunks_page").sql,
            &[Value::Int(10), Value::Int(0)],
            store.snapshot().expect("source snapshot").value(),
            query_limits(10, 4096),
        )
        .expect("source chunk page");
        assert_eq!(source_page.rows.len(), 1);
        assert_eq!(source_page.rows[0]["source_id"], text("source-1"));
        assert_eq!(source_page.join_access_paths.len(), 1);
        assert_eq!(
            source_page.join_access_paths[0].kind,
            skein_optimizer::RelationalAccessPathKind::PrimaryKey
        );
        assert!(source_page.join_access_paths[0].unique_point);

        let source_specific = execute_relational_query_sql(
            &named_statement(&corpus, "source_chunks_by_source").sql,
            &[text("source-1"), Value::Int(10)],
            store.snapshot().expect("source snapshot").value(),
            query_limits(10, 4096),
        )
        .expect("source-specific chunk page");
        assert_eq!(source_specific.rows.len(), 1);

        let tail = execute_relational_query_sql(
            &named_statement(&corpus, "thread_tail_signature").sql,
            &[text("thread-storage-1"), Value::Int(1), Value::Int(10)],
            store.snapshot().expect("tail snapshot").value(),
            query_limits(10, 4096),
        )
        .expect("thread tail signature");
        assert_eq!(tail.rows.len(), 2);

        let tail_anchors = execute_relational_query_sql(
            &named_statement(&corpus, "thread_tail_anchor_count").sql,
            &[text("thread-storage-1"), Value::Int(1)],
            store.snapshot().expect("tail anchor snapshot").value(),
            query_limits(1, 4096),
        )
        .expect("thread tail anchor count");
        assert_eq!(tail_anchors.rows[0]["anchor_count"], Value::Int(1));

        let migration = execute_relational_query_sql(
            &named_statement(&corpus, "content_migration_state").sql,
            &[],
            store.snapshot().expect("migration snapshot").value(),
            query_limits(10, 4096),
        )
        .expect("migration state query");
        assert_eq!(migration.rows[0]["value"], text("qualified"));
    }

    fn sample_parameter(data_type: &str, position: usize) -> Value {
        match data_type {
            "BOOLEAN" => Value::Bool(false),
            "BIGINT" => Value::Int(position as i64),
            "DOUBLE PRECISION" => Value::Float(position as f64),
            "TEXT" => Value::String(format!("value-{position}")),
            other => panic!("unsupported fixture parameter type {other}"),
        }
    }

    fn named_statement<'a>(
        corpus: &'a ContentStoreSqlCorpus,
        name: &str,
    ) -> &'a crate::content_store_sql_corpus::ContentStoreSqlStatementSpec {
        corpus
            .statements
            .iter()
            .find(|statement| statement.name == name)
            .unwrap_or_else(|| panic!("missing statement {name}"))
    }

    fn commit_named_mutation(
        store: &RelationalStore,
        corpus: &ContentStoreSqlCorpus,
        name: &str,
        parameters: Vec<Value>,
    ) {
        let snapshot = store.snapshot().expect("mutation snapshot");
        let transaction = compile_relational_mutation_sql(
            &named_statement(corpus, name).sql,
            &parameters,
            snapshot.value(),
        )
        .unwrap_or_else(|error| panic!("failed to compile {name}: {error}"));
        store
            .commit(transaction, |_, _| Ok(()))
            .unwrap_or_else(|error| panic!("failed to commit {name}: {error}"));
    }

    fn query_limits(
        max_output_rows: usize,
        max_output_payload_bytes: usize,
    ) -> RelationalQueryLimits {
        RelationalQueryLimits {
            max_output_rows,
            max_output_payload_bytes,
            max_intermediate_rows: 10_000,
            blocking_operator_bytes: std::num::NonZeroUsize::new(64 * 1024 * 1024)
                .expect("non-zero aggregate memory budget"),
            hydration: skein_storage::RelationalHydrationBudget::default(),
        }
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }
}
