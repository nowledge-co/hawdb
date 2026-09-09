use super::{lower_statement, Parser, ParserStatement, PostgreSqlDialect};
use crate::{
    parse_postgres_sql, prepare_postgres_sql, CreateTableStatement, InsertStatement,
    PostgresParameterMetadata, SqlColumnDefinition, SqlDataType, SqlStatement, SqlTableName,
    SqlTableStorage, SqlValue,
};
use skein_core::{SkeinError, Value};
use sqlparser::ast::*;

type ClauseCase<T> = (&'static str, fn(&mut T));

fn name() -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new("private_name"))])
}

fn expr() -> Expr {
    Expr::Identifier(Ident::new("private_payload"))
}

fn upstream(sql: &str) -> ParserStatement {
    Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .unwrap()
        .remove(0)
}

fn create_base() -> CreateTable {
    let ParserStatement::CreateTable(create) = upstream("CREATE TABLE t (id BIGINT)") else {
        unreachable!()
    };
    create
}

fn insert_base() -> Insert {
    let ParserStatement::Insert(insert) = upstream("INSERT INTO t (id) VALUES (1)") else {
        unreachable!()
    };
    insert
}

// Independent test inputs in the original rejection-wall priority order.
// Some(empty) and Some(false) intentionally exercise presence, not truthiness.
fn create_cases() -> Vec<ClauseCase<CreateTable>> {
    vec![
        ("OR REPLACE", |c| c.or_replace = true),
        ("TEMPORARY", |c| c.temporary = true),
        ("EXTERNAL", |c| c.external = true),
        ("DYNAMIC", |c| c.dynamic = true),
        ("GLOBAL", |c| c.global = Some(true)),
        ("TRANSIENT", |c| c.transient = true),
        ("VOLATILE", |c| c.volatile = true),
        ("ICEBERG", |c| c.iceberg = true),
        ("Hive distribution", |c| {
            c.hive_distribution = HiveDistributionStyle::PARTITIONED { columns: vec![] }
        }),
        ("Hive format", |c| {
            c.hive_formats = Some(HiveFormat::default())
        }),
        ("STORED AS", |c| c.file_format = Some(FileFormat::PARQUET)),
        ("LOCATION", |c| c.location = Some("private/location".into())),
        ("AS query", |c| {
            let ParserStatement::Query(query) = upstream("SELECT 'private literal'") else {
                unreachable!()
            };
            c.query = Some(query);
        }),
        ("WITHOUT ROWID", |c| c.without_rowid = true),
        ("LIKE", |c| {
            c.like = Some(CreateTableLikeKind::Plain(CreateTableLike {
                name: name(),
                defaults: None,
            }))
        }),
        ("CLONE", |c| c.clone = Some(name())),
        ("VERSION", |c| {
            c.version = Some(TableVersion::VersionAsOf(expr()))
        }),
        ("COMMENT", |c| {
            c.comment = Some(CommentDef::WithoutEq("private literal".into()))
        }),
        ("ON COMMIT", |c| c.on_commit = Some(OnCommit::Drop)),
        ("ON CLUSTER", |c| {
            c.on_cluster = Some(Ident::new("private_cluster"))
        }),
        ("PRIMARY KEY expression", |c| {
            c.primary_key = Some(Box::new(expr()))
        }),
        ("ORDER BY", |c| {
            c.order_by = Some(OneOrManyWithParens::One(expr()))
        }),
        ("PARTITION BY", |c| c.partition_by = Some(Box::new(expr()))),
        ("CLUSTER BY", |c| {
            c.cluster_by = Some(WrappedCollection::NoWrapping(vec![]))
        }),
        ("CLUSTERED BY", |c| {
            c.clustered_by = Some(ClusteredBy {
                columns: vec![],
                sorted_by: None,
                num_buckets: sqlparser::ast::Value::Number("1".into(), false),
            })
        }),
        ("INHERITS", |c| c.inherits = Some(vec![])),
        ("PARTITION OF", |c| c.partition_of = Some(name())),
        ("FOR VALUES", |c| c.for_values = Some(ForValues::Default)),
        ("STRICT", |c| c.strict = true),
        ("COPY GRANTS", |c| c.copy_grants = true),
        ("ENABLE_SCHEMA_EVOLUTION", |c| {
            c.enable_schema_evolution = Some(false)
        }),
        ("CHANGE_TRACKING", |c| c.change_tracking = Some(false)),
        ("DATA_RETENTION_TIME_IN_DAYS", |c| {
            c.data_retention_time_in_days = Some(0)
        }),
        ("MAX_DATA_EXTENSION_TIME_IN_DAYS", |c| {
            c.max_data_extension_time_in_days = Some(0)
        }),
        ("DEFAULT_DDL_COLLATION", |c| {
            c.default_ddl_collation = Some(String::new())
        }),
        ("WITH AGGREGATION POLICY", |c| {
            c.with_aggregation_policy = Some(name())
        }),
        ("WITH ROW ACCESS POLICY", |c| {
            c.with_row_access_policy = Some(RowAccessPolicy::new(name(), vec![]))
        }),
        ("WITH TAG", |c| c.with_tags = Some(vec![])),
        ("EXTERNAL_VOLUME", |c| {
            c.external_volume = Some(String::new())
        }),
        ("BASE_LOCATION", |c| c.base_location = Some(String::new())),
        ("CATALOG", |c| c.catalog = Some(String::new())),
        ("CATALOG_SYNC", |c| c.catalog_sync = Some(String::new())),
        ("STORAGE_SERIALIZATION_POLICY", |c| {
            c.storage_serialization_policy = Some(StorageSerializationPolicy::Compatible)
        }),
        ("TARGET_LAG", |c| c.target_lag = Some(String::new())),
        ("WAREHOUSE", |c| {
            c.warehouse = Some(Ident::new("private_warehouse"))
        }),
        ("REFRESH_MODE", |c| {
            c.refresh_mode = Some(RefreshModeKind::Auto)
        }),
        ("INITIALIZE", |c| {
            c.initialize = Some(InitializeKind::OnCreate)
        }),
        ("REQUIRE USER", |c| c.require_user = true),
    ]
}

fn insert_cases() -> Vec<ClauseCase<Insert>> {
    vec![
        ("optimizer hint", |i| {
            i.optimizer_hint = Some(OptimizerHint {
                text: "private_hint".into(),
                style: OptimizerHintStyle::MultiLine,
            })
        }),
        ("OR conflict action", |i| {
            i.or = Some(SqliteOnConflict::Ignore)
        }),
        ("IGNORE", |i| i.ignore = true),
        ("missing INTO", |i| i.into = false),
        ("table alias", |i| {
            i.table_alias = Some(Ident::new("private_alias"))
        }),
        ("OVERWRITE", |i| i.overwrite = true),
        ("SET assignments", |i| {
            i.assignments = vec![Assignment {
                target: AssignmentTarget::ColumnName(name()),
                value: expr(),
            }]
        }),
        ("PARTITION", |i| i.partitioned = Some(vec![])),
        ("columns after PARTITION", |i| {
            i.after_columns = vec![Ident::new("private_column")]
        }),
        ("TABLE keyword", |i| i.has_table_keyword = true),
        ("REPLACE INTO", |i| i.replace_into = true),
        ("priority", |i| {
            i.priority = Some(MysqlInsertPriority::LowPriority)
        }),
        ("row alias", |i| {
            i.insert_alias = Some(InsertAliases {
                row_alias: name(),
                col_aliases: None,
            })
        }),
        ("SETTINGS", |i| i.settings = Some(vec![])),
        ("FORMAT", |i| {
            i.format_clause = Some(InputFormatClause {
                ident: Ident::new("private_format"),
                values: vec![],
            })
        }),
    ]
}

fn assert_error(error: SkeinError, statement: &str, clause: &str) {
    let SkeinError::Semantic(message) = error else {
        panic!("expected a lowering rejection, got {error:?}");
    };
    assert_eq!(
        message,
        format!("unsupported PostgreSQL {statement} clause: {clause}")
    );
}

#[test]
fn every_original_clause_condition_is_rejected_by_lowering() {
    let create = create_base();
    assert!(lower_statement(&ParserStatement::CreateTable(create.clone())).is_ok());
    let cases = create_cases();
    assert_eq!(cases.len(), 48);
    for (clause, set) in cases {
        let mut input = create.clone();
        set(&mut input);
        assert_error(
            lower_statement(&ParserStatement::CreateTable(input)).unwrap_err(),
            "CREATE TABLE",
            clause,
        );
    }
    let insert = insert_base();
    assert!(lower_statement(&ParserStatement::Insert(insert.clone())).is_ok());
    let cases = insert_cases();
    assert_eq!(cases.len(), 15);
    for (clause, set) in cases {
        let mut input = insert.clone();
        set(&mut input);
        assert_error(
            lower_statement(&ParserStatement::Insert(input)).unwrap_err(),
            "INSERT",
            clause,
        );
    }
}

#[test]
fn local_scope_is_present_even_when_the_global_flag_is_false() {
    let mut input = create_base();
    input.global = Some(false);
    assert_error(
        lower_statement(&ParserStatement::CreateTable(input)).unwrap_err(),
        "CREATE TABLE",
        "LOCAL",
    );
}

fn assert_source_error(sql: &str, statement: &str, clause: &str) {
    assert_error(parse_postgres_sql(sql).unwrap_err(), statement, clause);
    assert_error(prepare_postgres_sql(sql).unwrap_err(), statement, clause);
}

fn assert_accepted(sql: &str, expected: SqlStatement, positions: &[usize]) {
    assert_eq!(parse_postgres_sql(sql).unwrap(), expected, "source: {sql}");
    let prepared = prepare_postgres_sql(sql).unwrap();
    assert_eq!(prepared.statement, expected, "source: {sql}");
    assert_eq!(
        prepared.parameters,
        positions
            .iter()
            .map(|&position| PostgresParameterMetadata { position })
            .collect::<Vec<_>>()
    );
}

// Expected ASTs are built from inputs, never by parsing another SQL string.
fn source_contract(case: usize) {
    let separator = [" ", "\n", "\t", " /* private comment */ "][case % 4];
    let keyword = |word: &str| {
        if case & 4 == 0 {
            word.to_string()
        } else {
            word.to_ascii_lowercase()
        }
    };
    let table_name = format!("Private_\u{e9}_{}", case / 8);
    let table = SqlTableName {
        schema: Some("PrivateSchema".into()),
        name: table_name.clone(),
    };
    let quoted_table = format!("\"PrivateSchema\".\"{table_name}\"");
    let payload = [
        "private literal",
        "quote's payload",
        "line\nbreak",
        "\u{1f512}\u{e9}",
    ][case / 8 % 4];
    let literal = format!("'{}'", payload.replace('\'', "''"));
    let tokens = |parts: Vec<String>| parts.join(separator);
    let create = tokens(vec![
        keyword("CREATE"),
        keyword("TABLE"),
        quoted_table.clone(),
        format!("(id {}, payload {})", keyword("BIGINT"), keyword("TEXT")),
    ]);
    assert_accepted(
        &create,
        SqlStatement::CreateTable(CreateTableStatement {
            table: table.clone(),
            if_not_exists: false,
            columns: [("id", SqlDataType::BigInt), ("payload", SqlDataType::Text)]
                .into_iter()
                .map(|(name, data_type)| SqlColumnDefinition {
                    name: name.into(),
                    data_type,
                    nullable: true,
                    default: None,
                    primary_key: false,
                    unique: false,
                    references: None,
                })
                .collect(),
            constraints: vec![],
            storage: SqlTableStorage::RowPage,
        }),
        &[],
    );
    let insert = tokens(vec![
        keyword("INSERT"),
        keyword("INTO"),
        quoted_table.clone(),
        "(id, payload)".into(),
        keyword("VALUES"),
        format!("($1, {literal}), ($1, {literal})"),
    ]);
    assert_accepted(
        &insert,
        SqlStatement::Insert(InsertStatement {
            table,
            columns: vec!["id".into(), "payload".into()],
            rows: vec![
                vec![
                    SqlValue::Parameter(1),
                    SqlValue::Literal(Value::String(payload.into()))
                ];
                2
            ],
            on_conflict: None,
            returning: vec![],
        }),
        &[1],
    );
    for (prefix, suffix, clause) in [
        ("CREATE TEMP TABLE", "(id BIGINT)".to_string(), "TEMPORARY"),
        ("CREATE TABLE", format!("AS SELECT {literal}"), "AS query"),
        (
            "CREATE TEMP TABLE",
            format!("AS SELECT {literal}"),
            "TEMPORARY",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) ON COMMIT DROP".to_string(),
            "ON COMMIT",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) INHERITS (private_parent)".to_string(),
            "INHERITS",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) PARTITION BY RANGE (id)".to_string(),
            "PARTITION BY",
        ),
    ] {
        let sql = tokens(vec![keyword(prefix), quoted_table.clone(), suffix]);
        assert_source_error(&sql, "CREATE TABLE", clause);
    }
    let sql = tokens(vec![
        keyword("INSERT INTO"),
        quoted_table,
        keyword("AS"),
        "private_alias (id, payload)".into(),
        keyword("VALUES"),
        format!("($2, {literal})"),
    ]);
    assert_source_error(&sql, "INSERT", "table alias");
}

#[test]
fn accepted_ast_and_multiple_clause_priority_are_preserved() {
    for case in 0..32 {
        source_contract(case);
    }
}

#[test]
#[ignore = "deterministic local-only clause diagnostic campaign"]
fn clause_diagnostics_campaign() {
    every_original_clause_condition_is_rejected_by_lowering();
    local_scope_is_present_even_when_the_global_flag_is_false();
    let create = create_base();
    let cases = create_cases();
    let mut pairs = 0;
    for (index, &(clause, first)) in cases.iter().enumerate() {
        for &(_, second) in &cases[index + 1..] {
            let mut input = create.clone();
            second(&mut input);
            first(&mut input);
            assert_error(
                lower_statement(&ParserStatement::CreateTable(input)).unwrap_err(),
                "CREATE TABLE",
                clause,
            );
            pairs += 1;
        }
    }
    let insert = insert_base();
    let cases = insert_cases();
    for (index, &(clause, first)) in cases.iter().enumerate() {
        for &(_, second) in &cases[index + 1..] {
            let mut input = insert.clone();
            second(&mut input);
            first(&mut input);
            assert_error(
                lower_statement(&ParserStatement::Insert(input)).unwrap_err(),
                "INSERT",
                clause,
            );
            pairs += 1;
        }
    }
    assert_eq!(pairs, 1233);
    for case in 0..1024 {
        source_contract(case);
    }
    println!("clause diagnostics: {pairs} AST condition pairs; 9216 SQL sources through parse and prepare");
}
