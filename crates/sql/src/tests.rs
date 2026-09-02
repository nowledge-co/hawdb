use super::{
    parse_postgres_sql, prepare_postgres_sql, SelectProjection, SqlArithmeticOperand,
    SqlArithmeticOperator, SqlAssignmentValue, SqlBound, SqlColumnRef, SqlComparisonOp,
    SqlConflictAction, SqlDataType, SqlExpression, SqlFunctionArgument, SqlJoinKind, SqlLikeEscape,
    SqlLockStrength, SqlOrderDirection, SqlPredicate, SqlStatement, SqlTableName, SqlValue,
};
use skein_core::Value;

#[test]
fn exposes_owned_postgres_sql_pgq_syntax() {
    let statement = super::syntax::parse_pgq_statement(
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (memories KEY (id))",
    )
    .expect("valid PostgreSQL SQL/PGQ syntax");
    assert!(matches!(
        statement,
        super::syntax::PgqStatement::CreatePropertyGraph(_)
    ));
}

#[test]
fn parses_postgres_select_subset() {
    let statement = parse_postgres_sql(
        "SELECT query, elapsed_micros AS elapsed FROM system.slow_queries \
         WHERE start_time >= '2026-07-23T00:00:00Z' AND work_class IN ('query', 'shadow') \
        ORDER BY elapsed_micros DESC LIMIT 20 OFFSET 5",
    )
    .expect("valid PostgreSQL select");

    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };
    assert_eq!(
        select.from,
        SqlTableName {
            schema: Some("system".to_string()),
            name: "slow_queries".to_string(),
        }
    );
    assert_eq!(
        select.projection,
        vec![
            SelectProjection::Column {
                name: SqlColumnRef {
                    qualifier: None,
                    name: "query".to_string(),
                },
                alias: None,
            },
            SelectProjection::Column {
                name: SqlColumnRef {
                    qualifier: None,
                    name: "elapsed_micros".to_string(),
                },
                alias: Some("elapsed".to_string()),
            },
        ]
    );
    assert_eq!(select.limit, Some(SqlBound::Literal(20)));
    assert_eq!(select.offset, Some(SqlBound::Literal(5)));
    assert_eq!(select.order_by[0].direction, SqlOrderDirection::Desc);

    let Some(SqlPredicate::And(left, right)) = select.selection else {
        panic!("expected conjunctive predicate");
    };
    assert_eq!(
        *left,
        SqlPredicate::Compare {
            left: SqlColumnRef {
                qualifier: None,
                name: "start_time".to_string(),
            },
            op: SqlComparisonOp::Gte,
            right: SqlValue::Literal(Value::String("2026-07-23T00:00:00Z".to_string())),
        }
    );
    assert_eq!(
        *right,
        SqlPredicate::InList {
            left: SqlColumnRef {
                qualifier: None,
                name: "work_class".to_string(),
            },
            values: vec![
                SqlValue::Literal(Value::String("query".to_string())),
                SqlValue::Literal(Value::String("shadow".to_string())),
            ],
            negated: false,
        }
    );
}

#[test]
fn parses_postgres_locking_selects_and_rejects_nonblocking_variants() {
    for (suffix, expected) in [
        ("FOR SHARE", SqlLockStrength::Share),
        ("FOR UPDATE", SqlLockStrength::Update),
    ] {
        let statement =
            parse_postgres_sql(&format!("SELECT id FROM messages WHERE id = $1 {suffix}"))
                .expect("valid PostgreSQL locking SELECT");
        let SqlStatement::Select(select) = statement else {
            panic!("expected SELECT statement");
        };
        assert_eq!(select.lock_strength, Some(expected));
    }

    for suffix in ["FOR UPDATE NOWAIT", "FOR SHARE SKIP LOCKED"] {
        let error = parse_postgres_sql(&format!("SELECT id FROM messages {suffix}"))
            .expect_err("nonblocking locking reads are not implemented");
        assert!(error.to_string().contains("NOWAIT, and SKIP LOCKED"));
    }
}

#[test]
fn parses_postgres_parameters_in_predicates_and_bounds() {
    let statement = parse_postgres_sql(
        "SELECT * FROM system.slow_queries \
         WHERE work_class = $1 AND elapsed_micros >= $2 \
         ORDER BY elapsed_micros DESC LIMIT $3 OFFSET $4",
    )
    .expect("valid parameterized PostgreSQL select");

    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };
    assert_eq!(select.limit, Some(SqlBound::Parameter(3)));
    assert_eq!(select.offset, Some(SqlBound::Parameter(4)));
    let Some(SqlPredicate::And(left, right)) = select.selection else {
        panic!("expected conjunctive predicate");
    };
    assert!(matches!(
        *left,
        SqlPredicate::Compare {
            right: SqlValue::Parameter(1),
            ..
        }
    ));
    assert!(matches!(
        *right,
        SqlPredicate::Compare {
            right: SqlValue::Parameter(2),
            ..
        }
    ));
}

#[test]
fn rejects_zero_based_postgres_parameter() {
    let error = parse_postgres_sql("SELECT * FROM system.slow_queries WHERE query = $0")
        .expect_err("PostgreSQL parameters are one-based");
    assert!(error.to_string().contains("one-based"));
}

#[test]
fn normalizes_unquoted_identifiers_with_postgres_rules() {
    let statement = parse_postgres_sql(
        r#"SELECT "QueryText", ELAPSED_MICROS FROM SYSTEM.SLOW_QUERIES WHERE "QueryText" IS NOT NULL"#,
    )
    .expect("valid PostgreSQL select");

    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };
    assert_eq!(
        select.from,
        SqlTableName {
            schema: Some("system".to_string()),
            name: "slow_queries".to_string(),
        }
    );
    assert_eq!(
        select.projection[0],
        SelectProjection::Column {
            name: SqlColumnRef {
                qualifier: None,
                name: "QueryText".to_string(),
            },
            alias: None,
        }
    );
    assert_eq!(
        select.projection[1],
        SelectProjection::Column {
            name: SqlColumnRef {
                qualifier: None,
                name: "elapsed_micros".to_string(),
            },
            alias: None,
        }
    );
}

#[test]
fn parses_delete_statement() {
    let statement = parse_postgres_sql("DELETE FROM thread_messages WHERE thread_storage_id = $1")
        .expect("supported DELETE statement");
    let SqlStatement::Delete(delete) = statement else {
        panic!("expected DELETE statement");
    };
    assert_eq!(delete.table.name, "thread_messages");
    assert!(matches!(
        delete.selection,
        Some(SqlPredicate::Compare {
            right: SqlValue::Parameter(1),
            ..
        })
    ));
}

#[test]
fn parses_inner_join_with_aliases() {
    let statement = parse_postgres_sql(
        "SELECT * FROM system.slow_queries q JOIN system.plan_cache p ON q.digest = p.digest",
    )
    .expect("supported inner join shape");
    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };
    assert_eq!(select.from_alias.as_deref(), Some("q"));
    assert_eq!(select.joins.len(), 1);
    assert_eq!(select.joins[0].kind, SqlJoinKind::Inner);
    assert_eq!(select.joins[0].alias.as_deref(), Some("p"));
    assert!(matches!(
        select.joins[0].on,
        SqlPredicate::CompareColumns { .. }
    ));
}

#[test]
fn parses_aggregate_projection_and_distinct_argument() {
    let statement = parse_postgres_sql(
        "SELECT COUNT(DISTINCT tm.content_message_id) AS covered_messages \
         FROM thread_messages AS tm GROUP BY tm.thread_storage_id",
    )
    .expect("supported aggregate SELECT");
    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT statement");
    };
    assert_eq!(select.group_by.len(), 1);
    assert!(matches!(
        &select.projection[0],
        SelectProjection::Expression {
            expression: SqlExpression::Function {
                name,
                arguments,
                distinct: true,
                filter: None,
            },
            alias: Some(alias),
        } if name == "count"
            && alias == "covered_messages"
            && matches!(arguments.as_slice(), [SqlFunctionArgument::Expression(_)])
    ));
}

#[test]
fn parses_aggregate_filter_predicate_and_parameters() {
    let prepared = prepare_postgres_sql(
        "SELECT COUNT(*) FILTER (WHERE is_read = $1) AS unread_count FROM entries",
    )
    .expect("supported aggregate filter");
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        vec![1]
    );
    let SqlStatement::Select(select) = prepared.statement else {
        panic!("expected SELECT statement");
    };
    assert!(matches!(
        &select.projection[0],
        SelectProjection::Expression {
            expression: SqlExpression::Function {
                name,
                arguments,
                distinct: false,
                filter: Some(SqlPredicate::Compare {
                    left,
                    op: SqlComparisonOp::Eq,
                    right: SqlValue::Parameter(1),
                }),
            },
            alias: Some(alias),
        } if name == "count"
            && alias == "unread_count"
            && left.name == "is_read"
            && matches!(arguments.as_slice(), [SqlFunctionArgument::Wildcard])
    ));

    let error = parse_postgres_sql("SELECT MAX(id) FILTER (WHERE id = 'entry-1') FROM entries")
        .expect_err("non-supported aggregate filter must fail");
    assert!(error.to_string().contains("FILTER"));
}

#[test]
fn parses_content_table_schema() {
    let statement = parse_postgres_sql(
        "CREATE TABLE content_chunks (\
           chunk_id TEXT PRIMARY KEY, \
           content_doc_id TEXT NOT NULL REFERENCES content_documents(content_doc_id), \
           chunk_index BIGINT NOT NULL, \
           text TEXT NOT NULL, \
           UNIQUE (content_doc_id, chunk_index)\
         )",
    )
    .expect("supported CREATE TABLE statement");
    let SqlStatement::CreateTable(create) = statement else {
        panic!("expected CREATE TABLE statement");
    };
    assert_eq!(create.table.name, "content_chunks");
    assert_eq!(create.columns[0].data_type, SqlDataType::Text);
    assert!(create.columns[0].primary_key);
    assert!(!create.columns[1].nullable);
    assert!(create.columns[1].references.is_some());
    assert_eq!(create.constraints.len(), 1);
    assert_eq!(create.storage, crate::SqlTableStorage::RowPage);
}

#[test]
fn parses_strict_append_table_storage() {
    let statement = parse_postgres_sql(
        "CREATE TABLE events (\
           stream_id TEXT NOT NULL, \
           sequence BIGINT NOT NULL, \
           payload BYTEA NOT NULL\
         ) WITH (\
           storage_mode = 'strict_append', \
           partition_key = 'stream_id', \
           order_key = 'sequence'\
         )",
    )
    .expect("supported strict append CREATE TABLE statement");
    let SqlStatement::CreateTable(create) = statement else {
        panic!("expected CREATE TABLE statement");
    };
    assert_eq!(
        create.storage,
        crate::SqlTableStorage::StrictAppend {
            partition_key: vec!["stream_id".to_string()],
            order_key: vec!["sequence".to_string()],
            generated_order: crate::SqlGeneratedOrder::CallerProvided,
        }
    );
}

#[test]
fn parses_commit_sequence_strict_append_storage() {
    let statement = parse_postgres_sql(
        "CREATE TABLE events (stream_id TEXT NOT NULL, sequence BIGINT NOT NULL) \
         WITH (storage_mode = 'strict_append', partition_key = 'stream_id', \
         order_key = 'sequence', generated_order = 'commit_sequence')",
    )
    .expect("supported generated-order CREATE TABLE statement");
    let SqlStatement::CreateTable(create) = statement else {
        panic!("expected CREATE TABLE statement");
    };
    assert!(matches!(
        create.storage,
        crate::SqlTableStorage::StrictAppend {
            generated_order: crate::SqlGeneratedOrder::CommitSequence,
            ..
        }
    ));
}

#[test]
fn rejects_incomplete_strict_append_table_storage() {
    let error = parse_postgres_sql(
        "CREATE TABLE events (stream_id TEXT NOT NULL, sequence BIGINT NOT NULL) \
         WITH (storage_mode = 'strict_append', partition_key = 'stream_id')",
    )
    .expect_err("missing order key must fail closed");
    assert!(error.to_string().contains("requires order_key"));
}

#[test]
fn parses_insert_on_conflict_update() {
    let statement = parse_postgres_sql(
        "INSERT INTO content_migration_state (key, value, updated_at) \
         VALUES ($1, $2, $3), ($4, $5, $6) \
         ON CONFLICT (key) DO UPDATE \
         SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
    )
    .expect("supported upsert statement");
    let SqlStatement::Insert(insert) = statement else {
        panic!("expected INSERT statement");
    };
    assert_eq!(insert.rows.len(), 2);
    assert!(matches!(
        insert.on_conflict.map(|conflict| conflict.action),
        Some(SqlConflictAction::DoUpdate(assignments)) if assignments.len() == 2
    ));
}

#[test]
fn parses_prepared_bigint_update_arithmetic() {
    let prepared = prepare_postgres_sql(
        "UPDATE feeds SET failure_count = failure_count + $2, retry_count = $3 + retry_count, \
         success_count = success_count - $4 WHERE id = $1",
    )
    .expect("supported BIGINT UPDATE arithmetic");
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );
    let SqlStatement::Update(update) = prepared.statement else {
        panic!("expected UPDATE statement");
    };
    assert!(matches!(
        update.assignments[0].value,
        SqlAssignmentValue::Arithmetic {
            left: SqlArithmeticOperand::Column(_),
            operator: SqlArithmeticOperator::Add,
            right: SqlArithmeticOperand::Value(SqlValue::Parameter(2)),
        }
    ));
    assert!(matches!(
        update.assignments[1].value,
        SqlAssignmentValue::Arithmetic {
            left: SqlArithmeticOperand::Value(SqlValue::Parameter(3)),
            operator: SqlArithmeticOperator::Add,
            right: SqlArithmeticOperand::Column(_),
        }
    ));
    assert!(matches!(
        update.assignments[2].value,
        SqlAssignmentValue::Arithmetic {
            left: SqlArithmeticOperand::Column(_),
            operator: SqlArithmeticOperator::Subtract,
            right: SqlArithmeticOperand::Value(SqlValue::Parameter(4)),
        }
    ));

    for sql in [
        "UPDATE feeds SET failure_count = 1 - failure_count WHERE id = $1",
        "UPDATE feeds SET failure_count = failure_count * 2 WHERE id = $1",
    ] {
        let error = parse_postgres_sql(sql).expect_err("unsupported arithmetic shape");
        assert!(error.to_string().contains("arithmetic"));
    }
}

#[test]
fn parses_insert_on_conflict_do_nothing_returning() {
    let statement = parse_postgres_sql(
        "INSERT INTO raw_turns (org_id, request_id, raw_turn_id) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (org_id, request_id) DO NOTHING \
         RETURNING raw_turn_id",
    )
    .expect("supported idempotent insert statement");
    let SqlStatement::Insert(insert) = statement else {
        panic!("expected INSERT statement");
    };
    assert!(matches!(
        insert.on_conflict.map(|conflict| conflict.action),
        Some(SqlConflictAction::DoNothing)
    ));
    assert_eq!(insert.returning.len(), 1);
    assert_eq!(insert.returning[0].name, "raw_turn_id");
    assert_eq!(insert.returning[0].qualifier, None);
}

#[test]
fn rejects_insert_returning_expression() {
    let error = parse_postgres_sql(
        "INSERT INTO raw_turns (raw_turn_id) VALUES ($1) RETURNING upper(raw_turn_id)",
    )
    .expect_err("RETURNING expressions must fail closed");
    assert!(error
        .to_string()
        .contains("INSERT RETURNING supports column references only"));
}

#[test]
fn prepares_dense_repeated_postgres_parameters() {
    let prepared = prepare_postgres_sql(
        "SELECT * FROM thread_messages \
         WHERE thread_storage_id = $1 OR message_id = $1 LIMIT $2",
    )
    .expect("dense parameter contract");
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
}

#[test]
fn parses_explain_and_preserves_inner_parameters() {
    let prepared = prepare_postgres_sql(
        "EXPLAIN ANALYZE SELECT id FROM content_documents WHERE id = $1 LIMIT $2",
    )
    .expect("supported EXPLAIN ANALYZE statement");
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let SqlStatement::Explain(explain) = prepared.statement else {
        panic!("expected EXPLAIN statement");
    };
    assert!(explain.analyze);
    assert!(matches!(*explain.statement, SqlStatement::Select(_)));
}

#[test]
fn rejects_unsupported_explain_options_and_mutations() {
    let verbose = parse_postgres_sql("EXPLAIN VERBOSE SELECT id FROM content_documents")
        .expect_err("EXPLAIN VERBOSE must remain outside the supported contract");
    assert!(verbose
        .to_string()
        .contains("unsupported PostgreSQL EXPLAIN option"));

    let mutation = parse_postgres_sql(
        "EXPLAIN UPDATE content_documents SET content = 'changed' WHERE id = 'doc-1'",
    )
    .expect_err("EXPLAIN only supports read statements");
    assert!(mutation
        .to_string()
        .contains("EXPLAIN only supports relational SELECT"));
}

#[test]
fn rejects_gapped_postgres_parameters() {
    let error =
        prepare_postgres_sql("SELECT * FROM thread_messages WHERE thread_storage_id = $1 LIMIT $3")
            .expect_err("gapped parameter positions must fail");
    assert!(error.to_string().contains("must be dense"));
}

#[test]
fn parses_nested_octet_length_aggregate() {
    let statement = parse_postgres_sql(
        "SELECT COALESCE(SUM(OCTET_LENGTH(content)), 0) AS payload_bytes FROM thread_messages",
    )
    .unwrap();
    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    assert!(matches!(
        &select.projection[0],
        SelectProjection::Expression {
            expression: SqlExpression::Function { name, .. },
            alias: Some(alias),
        } if name == "coalesce" && alias == "payload_bytes"
    ));
}

#[test]
fn parses_like_and_ilike_predicates_with_parameters_and_escape() {
    let statement = parse_postgres_sql(
        "SELECT id FROM documents WHERE title LIKE $1 ESCAPE '!' OR title NOT ILIKE 'guide%'",
    )
    .expect("valid LIKE and ILIKE predicates");
    let SqlStatement::Select(select) = statement else {
        panic!("expected SELECT");
    };
    let Some(SqlPredicate::Or(left, right)) = select.selection else {
        panic!("expected disjunctive predicate");
    };
    assert!(matches!(
        *left,
        SqlPredicate::Like {
            pattern: SqlValue::Parameter(1),
            case_insensitive: false,
            negated: false,
            escape: SqlLikeEscape::Character('!'),
            ..
        }
    ));
    assert!(matches!(
        *right,
        SqlPredicate::Like {
            pattern: SqlValue::Literal(Value::String(ref pattern)),
            case_insensitive: true,
            negated: true,
            escape: SqlLikeEscape::Character('\\'),
            ..
        } if pattern == "guide%"
    ));
}

#[test]
fn like_matcher_handles_wildcards_escaping_and_unicode_case_insensitivity() {
    assert!(super::sql_like_matches(
        "prefix-middle-suffix",
        "prefix%suffix",
        SqlLikeEscape::Character('\\'),
        false,
    )
    .expect("valid LIKE pattern"));
    assert!(super::sql_like_matches(
        "road_map",
        r"road\_map",
        SqlLikeEscape::Character('\\'),
        false,
    )
    .expect("escaped underscore"));
    assert!(super::sql_like_matches(
        "100% complete",
        "100!% complete",
        SqlLikeEscape::Character('!'),
        false,
    )
    .expect("custom escape"));
    assert!(super::sql_like_matches(
        "ÄPFEL guide",
        "%äpfel%",
        SqlLikeEscape::Character('\\'),
        true,
    )
    .expect("locale-independent Unicode case folding"));
    assert!(
        super::sql_like_matches("Straße", "%STRASSE%", SqlLikeEscape::Character('\\'), true,)
            .expect("default case folding expands sharp s")
    );
    assert!(
        super::sql_like_matches("ß", "_", SqlLikeEscape::Character('\\'), true,)
            .expect("wildcard consumes one source character after case folding")
    );
    assert!(
        !super::sql_like_matches("ß", "__", SqlLikeEscape::Character('\\'), true,)
            .expect("wildcards do not consume case-folded expansions")
    );
    assert!(!super::sql_like_matches(
        "roadXmap",
        r"road\_map",
        SqlLikeEscape::Character('\\'),
        false,
    )
    .expect("literal underscore does not match arbitrary character"));
    assert!(
        super::sql_like_matches("trailing\\", "trailing\\", SqlLikeEscape::Disabled, false,)
            .expect("disabled escape retains literal backslash")
    );
    assert!(
        super::sql_like_matches("value", "value\\", SqlLikeEscape::Character('\\'), false,)
            .expect_err("dangling escape must fail")
            .to_string()
            .contains("ends with its escape")
    );
}
