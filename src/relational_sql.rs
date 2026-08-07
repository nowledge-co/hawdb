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

mod query;

pub(crate) use query::{execute_relational_query_sql_with_runtime, RelationalQueryLimits};

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
        | SqlStatement::Explain(_)
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
    use skein_storage::RelationalStore;
    use std::collections::BTreeMap;

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

        let explain = database
            .query_sql_with_params(
                "EXPLAIN SELECT id FROM public.messages WHERE id = $1",
                &[Value::String("message-1".to_string())],
            )
            .expect("plan relational SQL through the public query entrypoint");
        assert!(explain.rows.iter().any(|row| {
            matches!(
                row.get("access object"),
                Some(Value::String(access)) if access.contains("primary_key")
            )
        }));

        let analyze = database
            .query_sql("EXPLAIN ANALYZE SELECT id FROM public.messages ORDER BY id")
            .expect("profile relational SQL through the public query entrypoint");
        assert_eq!(analyze.rows[0]["actRows"], Value::Int(2));

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
            let staged_plan = transaction
                .query_sql("EXPLAIN SELECT id FROM public.messages")
                .expect("plan against staged relational state");
            assert!(!staged_plan.rows.is_empty());
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
    fn relational_index_join_aggregate_and_late_hydration_are_bounded() {
        let store = RelationalStore::default();
        for ddl in [
            "CREATE TABLE documents (id TEXT PRIMARY KEY, owner_kind TEXT NOT NULL, owner_id TEXT NOT NULL, UNIQUE (owner_kind, owner_id))",
            "CREATE TABLE messages (id TEXT PRIMARY KEY, document_id TEXT NOT NULL REFERENCES documents(id), stream_id TEXT NOT NULL, order_index BIGINT NOT NULL, body TEXT NOT NULL, token_count BIGINT NOT NULL)",
            "CREATE TABLE anchors (id TEXT PRIMARY KEY, document_id TEXT NOT NULL REFERENCES documents(id), message_id TEXT NOT NULL)",
            "CREATE INDEX idx_messages_order ON messages (stream_id, order_index, id)",
            "CREATE INDEX idx_anchors_message ON anchors (document_id, message_id)",
        ] {
            commit_sql(&store, ddl, &[]);
        }
        commit_sql(
            &store,
            "INSERT INTO documents (id, owner_kind, owner_id) VALUES ($1, $2, $3)",
            &[text("doc-1"), text("thread"), text("thread-1")],
        );
        for (id, order) in [("message-1", 1), ("message-2", 2)] {
            commit_sql(
                &store,
                "INSERT INTO messages (id, document_id, stream_id, order_index, body, token_count) VALUES ($1, $2, $3, $4, $5, $6)",
                &[
                    text(id),
                    text("doc-1"),
                    text("stream-1"),
                    Value::Int(order),
                    text(&format!("body-{order}-{}", "x".repeat(8 * 1024))),
                    Value::Int(10),
                ],
            );
        }
        commit_sql(
            &store,
            "INSERT INTO anchors (id, document_id, message_id) VALUES ($1, $2, $3)",
            &[text("anchor-1"), text("doc-1"), text("message-1")],
        );

        let snapshot = store.snapshot().expect("query snapshot");
        let owner = execute_relational_query_sql_with_runtime(
            "SELECT id FROM documents WHERE owner_kind = 'thread' AND owner_id = $1",
            &[text("thread-1")],
            snapshot.value(),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("composite unique lookup");
        assert_eq!(owner.rows[0]["id"], text("doc-1"));
        assert_eq!(owner.access_path.name, "__unique_0");
        assert_eq!(owner.access_path.equality_prefix_len, 2);
        assert!(owner.access_path.unique_point);

        let page = execute_relational_query_sql_with_runtime(
            "SELECT id, body FROM messages WHERE stream_id = $1 ORDER BY order_index ASC, id ASC LIMIT $2 OFFSET $3",
            &[text("stream-1"), Value::Int(1), Value::Int(1)],
            snapshot.value(),
            query_limits(1, 64 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("bounded message page");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0]["id"], text("message-2"));
        assert_eq!(page.access_path.name, "idx_messages_order");
        assert_eq!(page.hydration.hydrated_rows, 1);
        assert!(page.hydration.decompressed_bytes > 8 * 1024);

        let joined = execute_relational_query_sql_with_runtime(
            "SELECT m.id FROM messages AS m INNER JOIN anchors AS a ON a.document_id = m.document_id AND a.message_id = m.id WHERE m.stream_id = $1",
            &[text("stream-1")],
            snapshot.value(),
            query_limits(2, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("indexed join");
        assert_eq!(joined.rows.len(), 1);
        assert_eq!(joined.join_access_paths[0].name, "idx_anchors_message");
        assert_eq!(joined.join_access_paths[0].equality_prefix_len, 2);

        let summary_sql =
            "SELECT COUNT(*) AS message_count, SUM(token_count) AS token_count FROM messages WHERE stream_id = $1";
        let summary = execute_relational_query_sql_with_runtime(
            summary_sql,
            &[text("stream-1")],
            snapshot.value(),
            query_limits(1, 4 * 1024),
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect("bounded aggregate");
        assert_eq!(summary.rows[0]["message_count"], Value::Int(2));
        assert_eq!(summary.rows[0]["token_count"], Value::Int(20));
        assert_eq!(summary.hydration.hydrated_rows, 0);

        let mut constrained = query_limits(1, 4 * 1024);
        constrained.blocking_operator_bytes =
            std::num::NonZeroUsize::new(1).expect("non-zero aggregate memory budget");
        let error = execute_relational_query_sql_with_runtime(
            summary_sql,
            &[text("stream-1")],
            snapshot.value(),
            constrained,
            &skein_executor::ExecutionMemoryConfig::default(),
            None,
        )
        .expect_err("aggregate must honor its memory budget");
        assert!(error.to_string().contains("blocking_operator_bytes"));
    }

    fn commit_sql(store: &RelationalStore, sql: &str, parameters: &[Value]) {
        let snapshot = store.snapshot().expect("SQL mutation snapshot");
        let transaction = compile_relational_statement_sql(sql, parameters, snapshot.value())
            .unwrap_or_else(|error| panic!("failed to compile SQL '{sql}': {error}"));
        store
            .commit(transaction, |_, _| Ok(()))
            .unwrap_or_else(|error| panic!("failed to commit SQL '{sql}': {error}"));
    }

    fn text(value: &str) -> Value {
        Value::String(value.to_string())
    }

    fn query_limits(
        max_output_rows: usize,
        max_output_payload_bytes: usize,
    ) -> RelationalQueryLimits {
        RelationalQueryLimits {
            max_output_rows,
            max_output_payload_bytes,
            max_intermediate_rows: 10_000,
            batch_rows: std::num::NonZeroUsize::new(256).expect("non-zero batch row budget"),
            blocking_operator_bytes: std::num::NonZeroUsize::new(64 * 1024 * 1024)
                .expect("non-zero aggregate memory budget"),
            hydration: skein_storage::RelationalHydrationBudget::default(),
        }
    }

    #[test]
    fn relational_sort_and_distinct_spill_and_pipeline_cancellation_are_bounded() {
        let store = RelationalStore::default();
        let rows = (0..256)
            .rev()
            .map(|value| {
                RelationalRow::new(vec![
                    RelationalValue::BigInt(value),
                    RelationalValue::BigInt(value % 17),
                ])
            })
            .collect();
        store
            .commit(
                RelationalTransaction {
                    writes: vec![
                        RelationalWrite::CreateTable(RelationalTableSchema {
                            name: "spill_rows".to_string(),
                            columns: vec![
                                RelationalColumnSchema {
                                    name: "id".to_string(),
                                    scalar_type: RelationalScalarType::BigInt,
                                    nullable: false,
                                    default: None,
                                },
                                RelationalColumnSchema {
                                    name: "value".to_string(),
                                    scalar_type: RelationalScalarType::BigInt,
                                    nullable: false,
                                    default: None,
                                },
                            ],
                            primary_key: vec!["id".to_string()],
                            unique_constraints: Vec::new(),
                            foreign_keys: Vec::new(),
                            indexes: Vec::new(),
                        }),
                        RelationalWrite::Insert {
                            table: "spill_rows".to_string(),
                            rows,
                            mode: RelationalInsertMode::Error,
                        },
                    ],
                },
                |_, _| Ok(()),
            )
            .expect("materialize spill fixture");
        let snapshot = store.snapshot().expect("spill fixture snapshot");
        let mut limits = query_limits(256, 64 * 1024);
        limits.batch_rows = std::num::NonZeroUsize::new(8).expect("non-zero batch rows");
        limits.blocking_operator_bytes =
            std::num::NonZeroUsize::new(16 * 1_024).expect("non-zero blocking memory");
        let memory = skein_executor::ExecutionMemoryConfig {
            batch_rows: limits.batch_rows,
            blocking_operator_bytes: limits.blocking_operator_bytes,
            min_spill_free_bytes: std::num::NonZeroU64::MIN,
            spill_directory: std::env::temp_dir().join(format!(
                "skein-relational-spill-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
            )),
            ..skein_executor::ExecutionMemoryConfig::default()
        };

        let sorted = execute_relational_query_sql_with_runtime(
            "SELECT id FROM spill_rows ORDER BY value ASC, id ASC",
            &[],
            snapshot.value(),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational sort");
        assert_eq!(sorted.rows.len(), 256);
        assert!(sorted
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "TopNExec" && report.spill_run_count > 0));

        let distinct = execute_relational_query_sql_with_runtime(
            "SELECT DISTINCT id FROM spill_rows ORDER BY id ASC",
            &[],
            snapshot.value(),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational distinct");
        assert_eq!(distinct.rows.len(), 256);
        assert!(distinct
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "DistinctExec" && report.spill_run_count > 0));

        let distinct_count = execute_relational_query_sql_with_runtime(
            "SELECT COUNT(DISTINCT id) AS item_count FROM spill_rows",
            &[],
            snapshot.value(),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed relational distinct aggregate");
        assert_eq!(distinct_count.rows[0]["item_count"], Value::Int(256));
        assert!(distinct_count
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "DistinctExec" && report.spill_run_count > 0));

        let grouped = execute_relational_query_sql_with_runtime(
            "SELECT value, COUNT(*) AS item_count FROM spill_rows GROUP BY value",
            &[],
            snapshot.value(),
            limits,
            &memory,
            None,
        )
        .expect("spill-backed grouped relational aggregate");
        assert_eq!(grouped.rows.len(), 17);
        assert_eq!(
            grouped
                .rows
                .iter()
                .map(|row| match row["item_count"] {
                    Value::Int(value) => value,
                    ref value => panic!("unexpected aggregate value {value:?}"),
                })
                .sum::<i64>(),
            256
        );
        assert!(grouped
            .blocking_operator_memory_reports
            .iter()
            .any(|report| report.operator == "SortExec" && report.spill_run_count > 0));

        let explain_cancellation = skein_core::RuntimeCancellationToken::new();
        explain_cancellation.cancel();
        let explain_context =
            skein_core::RuntimeTaskContext::without_deadline(explain_cancellation);
        let explained = execute_relational_query_sql_with_runtime(
            "EXPLAIN SELECT id FROM spill_rows WHERE id = $1",
            &[Value::Int(7)],
            snapshot.value(),
            limits,
            &memory,
            Some(&explain_context),
        )
        .expect("plain EXPLAIN must plan without executing the cancelled scan");
        assert!(explained.rows.iter().any(|row| {
            matches!(
                row.get("access object"),
                Some(Value::String(access)) if access.contains("primary_key")
            )
        }));
        assert!(explained
            .rows
            .iter()
            .all(|row| !row.contains_key("actRows")));

        let analyzed = execute_relational_query_sql_with_runtime(
            "EXPLAIN ANALYZE SELECT id FROM spill_rows ORDER BY value ASC, id ASC",
            &[],
            snapshot.value(),
            limits,
            &memory,
            None,
        )
        .expect("EXPLAIN ANALYZE must expose measured spill evidence");
        assert_eq!(analyzed.rows[0]["actRows"], Value::Int(256));
        assert!(analyzed.rows.iter().any(|row| {
            matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec"))
                && !matches!(row.get("disk"), None | Some(Value::Null))
        }));

        let cancellation = skein_core::RuntimeCancellationToken::new();
        cancellation.cancel();
        let task_context = skein_core::RuntimeTaskContext::without_deadline(cancellation);
        let error = execute_relational_query_sql_with_runtime(
            "SELECT id FROM spill_rows",
            &[],
            snapshot.value(),
            limits,
            &memory,
            Some(&task_context),
        )
        .expect_err("cancelled relational scan must stop at a batch checkpoint");
        assert!(error
            .to_string()
            .contains("runtime task stopped: cancelled"));

        std::fs::remove_dir_all(&memory.spill_directory).expect("remove spill test directory");
    }
}
