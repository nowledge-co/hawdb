use super::*;

fn having_fixture() -> Database {
    let mut database = Database::new();
    for sql in [
        "CREATE TABLE having_items (id BIGINT PRIMARY KEY, owner TEXT, amount BIGINT, enabled BOOLEAN)",
        "INSERT INTO having_items (id, owner, amount, enabled) VALUES (1, 'a', 2, TRUE), (2, 'a', 3, FALSE), (3, 'b', NULL, TRUE), (4, 'c', 5, TRUE), (5, 'c', 5, TRUE), (6, 'd', 9, FALSE)",
    ] {
        database.query_sql(sql).unwrap();
    }
    database
}

#[test]
fn relational_having_filters_groups_with_hidden_aggregates_before_limits() {
    let mut database = having_fixture();
    let output = database
        .query_sql(
            "SELECT owner, COUNT(*) AS total FROM having_items GROUP BY owner HAVING COUNT(*) > 1",
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert_eq!(output.rows[0]["owner"], Value::String("a".into()));
    assert_eq!(output.rows[1]["owner"], Value::String("c".into()));
    assert!(output.rows.iter().all(|row| row["total"] == Value::Int(2)));
    let output = database.query_sql(
        "SELECT owner FROM having_items GROUP BY owner HAVING SUM(amount) >= 5 AND COUNT(*) > 1 LIMIT 1 OFFSET 1",
    ).unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["owner"], Value::String("c".into()));
    assert_eq!(output.rows[0].len(), 1);
    let output = database
        .query_sql_with_params_options(
            "SELECT owner FROM having_items GROUP BY owner HAVING COUNT(*) > 1",
            &[],
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: None,
            },
        )
        .unwrap();
    assert_eq!(output.rows.len(), 2);
    assert!(database
        .query_sql_with_params_options(
            "SELECT owner FROM having_items GROUP BY owner HAVING COUNT(*) > 0",
            &[],
            QueryStreamOptions {
                max_rows: Some(2),
                max_payload_bytes: None
            },
        )
        .unwrap_err()
        .to_string()
        .contains("max_output_rows 2"));
}

#[test]
fn relational_having_preserves_implicit_and_empty_groups() {
    let mut database = having_fixture();
    for empty in [false, true] {
        if empty {
            database
                .query_sql("DELETE FROM having_items WHERE id >= 0")
                .unwrap();
        }
        for (predicate, expected) in [
            ("TRUE", 1),
            ("FALSE", 0),
            ("NULL", 0),
            ("COUNT(*) = 0", usize::from(empty)),
            ("COUNT(*) > 0", usize::from(!empty)),
            ("SUM(amount) IS NULL", usize::from(empty)),
        ] {
            let sql = format!("SELECT 42 AS answer FROM having_items HAVING {predicate}");
            let output = database.query_sql(&sql).unwrap();
            assert_eq!(output.rows.len(), expected, "{sql}; empty={empty}");
            assert!(output
                .rows
                .iter()
                .all(|row| row["answer"] == Value::Int(42)));
        }
        let output = database
            .query_sql(
                "SELECT COALESCE(SUM(amount), 0) AS amount FROM having_items HAVING COUNT(*) >= 0",
            )
            .unwrap();
        assert_eq!(
            output.rows[0]["amount"],
            Value::Int(if empty { 0 } else { 24 })
        );
    }
    assert!(database
        .query_sql("SELECT owner FROM having_items GROUP BY owner HAVING TRUE",)
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn relational_having_applies_three_valued_logic_and_scalar_predicates() {
    let mut database = having_fixture();
    for (predicate, expected) in [
        ("SUM(amount) IS NULL", vec!["b"]),
        ("NOT (SUM(amount) > 5)", vec!["a"]),
        (
            "SUM(amount) > 5 OR SUM(amount) IS NULL",
            vec!["b", "c", "d"],
        ),
        ("SUM(amount) IN (5, NULL)", vec!["a"]),
        ("SUM(amount) NOT IN (5, NULL)", vec![]),
        ("SUM(amount) IN (MAX(amount), NULL)", vec!["d"]),
        ("owner LIKE 'a%' OR owner ILIKE 'C'", vec!["a", "c"]),
        ("COALESCE(SUM(amount), 0) = 0", vec!["b"]),
        ("SUM(amount) > 4.5 AND SUM(amount) < 5.5", vec!["a"]),
    ] {
        let sql = format!("SELECT owner FROM having_items GROUP BY owner HAVING {predicate}");
        let output = database.query_sql(&sql).unwrap();
        let owners = output
            .rows
            .iter()
            .map(|row| row["owner"].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            owners,
            expected
                .into_iter()
                .map(|owner| Value::String(owner.into()))
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
    let output = database
        .query_sql("SELECT enabled FROM having_items GROUP BY enabled HAVING enabled")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0]["enabled"], Value::Bool(true));
}

#[test]
fn relational_having_rebinds_projection_filter_having_and_bounds() {
    let mut database = having_fixture();
    let sql = "SELECT owner, SUM($1) AS total FROM having_items WHERE id >= $2 GROUP BY owner HAVING COUNT(DISTINCT amount) FILTER (WHERE enabled = $3) >= $4 LIMIT $5 OFFSET $6";
    for (enabled, offset, owner) in [
        (true, 0, "a"),
        (true, 1, "c"),
        (false, 1, "d"),
        (true, 0, "a"),
    ] {
        let output = database
            .query_sql_with_params(
                sql,
                &[
                    Value::Int(7),
                    Value::Int(1),
                    Value::Bool(enabled),
                    Value::Int(1),
                    Value::Int(1),
                    Value::Int(offset),
                ],
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0]["owner"], Value::String(owner.into()));
        assert_eq!(
            output.rows[0]["total"],
            Value::Int(if owner == "d" { 7 } else { 14 })
        );
    }
    assert!(database
        .query_sql_with_params(sql, &[Value::Int(7)])
        .is_err());
    database
        .query_sql("DELETE FROM having_items WHERE owner = 'a'")
        .unwrap();
    let output = database
        .query_sql_with_params(
            sql,
            &[
                Value::Int(3),
                Value::Int(1),
                Value::Bool(true),
                Value::Int(1),
                Value::Int(1),
                Value::Int(0),
            ],
        )
        .unwrap();
    assert_eq!(output.rows[0]["owner"], Value::String("c".into()));
    assert_eq!(output.rows[0]["total"], Value::Int(6));
}

#[test]
fn relational_having_validates_types_names_and_grouping_before_empty_scans() {
    let mut database = having_fixture();
    let error = database
        .query_sql("SELECT query FROM system.slow_queries HAVING FALSE")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("system SQL does not support HAVING"),
        "{error}"
    );
    for empty in [false, true] {
        if empty {
            database
                .query_sql("DELETE FROM having_items WHERE id >= 0")
                .unwrap();
        }
        for sql in [
            "SELECT id FROM having_items HAVING COUNT(*) > 0",
            "SELECT owner FROM having_items GROUP BY owner HAVING amount > 0",
            "SELECT owner FROM having_items GROUP BY owner HAVING SUM(missing) > 0",
            "SELECT owner AS name FROM having_items GROUP BY owner HAVING name = 'a'",
            "SELECT COUNT(*) AS total FROM having_items HAVING total > 1",
            "SELECT COUNT(*) FROM having_items HAVING SUM(owner) > 0",
            "SELECT COUNT(*) FROM having_items HAVING COUNT(*) LIKE '1%'",
            "SELECT COUNT(*) FROM having_items HAVING SUM(MAX(amount)) > 0",
            "SELECT COUNT(*) FROM having_items HAVING COUNT(*) > 'wrong type'",
            "SELECT COUNT(*) FROM having_items HAVING 42",
            "SELECT COUNT(*) FROM having_items HAVING COUNT(*) FILTER (WHERE missing = 1) > 0",
            "SELECT * FROM having_items HAVING TRUE",
            "SELECT id FROM having_items GROUP BY id HAVING TRUE FOR UPDATE",
        ] {
            assert!(
                database.query_sql(sql).is_err(),
                "accepted {sql}; empty={empty}"
            );
        }
    }
}

#[test]
fn relational_having_handles_primary_key_dependencies_and_comma_inputs() {
    let mut database = having_fixture();
    let output = database
        .query_sql("SELECT id, owner FROM having_items GROUP BY id HAVING enabled")
        .unwrap();
    assert_eq!(output.rows.len(), 4);
    let output = database.query_sql(
        "SELECT a.owner FROM having_items a, having_items b WHERE a.id = b.id GROUP BY a.owner HAVING COUNT(b.id) > 1",
    ).unwrap();
    assert_eq!(output.rows.len(), 2);
    let explain = database
        .query_sql(
            "EXPLAIN SELECT owner FROM having_items GROUP BY owner HAVING COUNT(*) > 1 LIMIT 1",
        )
        .unwrap();
    let text = format!("{:?}", explain.rows);
    assert!(text.contains("phase=having"), "{text}");
    assert!(text.contains("count(*) > 1"), "{text}");
}

#[test]
fn relational_having_accounts_for_hidden_distinct_and_value_states() {
    let mut config = DatabaseConfig::default();
    config.execution_memory.blocking_operator_bytes = std::num::NonZeroUsize::new(4096).unwrap();
    let mut database = Database::new_with_config(config);
    database
        .query_sql("CREATE TABLE having_budget (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    for id in 0..128 {
        database
            .query_sql_with_params(
                "INSERT INTO having_budget (id, body) VALUES ($1, $2)",
                &[Value::Int(id), Value::String("x".repeat(8192))],
            )
            .unwrap();
    }
    assert_eq!(
        database
            .query_sql("SELECT 1 AS result FROM having_budget HAVING COUNT(*) > 0")
            .unwrap()
            .rows
            .len(),
        1
    );
    for sql in [
        "SELECT 1 AS result FROM having_budget HAVING COUNT(DISTINCT id) > 0",
        "SELECT 1 AS result FROM having_budget HAVING MAX(body) IS NOT NULL",
    ] {
        let error = database.query_sql(sql).unwrap_err();
        assert!(
            error.to_string().contains("RelationalAggregateExec"),
            "{sql}: {error}"
        );
    }
}

#[test]
fn relational_having_reads_hidden_overflow_metadata_and_hydrates_value_comparisons() {
    let path = unique_test_dir("having_overflow_metadata");
    {
        let mut database = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: skein_storage::RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        database
            .query_sql(
                "CREATE TABLE having_overflow (id BIGINT PRIMARY KEY, owner TEXT, body TEXT)",
            )
            .unwrap();
        database
            .query_sql_with_params(
                "INSERT INTO having_overflow (id, owner, body) VALUES (1, 'a', $1), (2, 'a', NULL)",
                &[Value::String("x".repeat(96 * 1024))],
            )
            .unwrap();
        database.checkpoint().unwrap();
    }
    let config = DatabaseConfig {
        read_only: true,
        storage_residency_mode: skein_storage::StorageResidencyMode::OutOfCore,
        relational_index_mode: skein_storage::RelationalIndexMode::Authoritative,
        max_relational_hydration_bytes: std::num::NonZeroUsize::new(32 * 1024).unwrap(),
        ..DatabaseConfig::default()
    };
    for _ in 0..2 {
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        for sql in [
            "SELECT 1 AS result FROM having_overflow HAVING COUNT(body) = 1 AND SUM(OCTET_LENGTH(body)) = 98304",
            "SELECT owner FROM having_overflow GROUP BY owner HAVING COUNT(body) = 1 AND SUM(OCTET_LENGTH(body)) = 98304",
        ] {
            assert_eq!(database.query_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}")).rows.len(), 1, "{sql}");
            let explain = database.query_sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap();
            assert!(format!("{:?}", explain.rows).contains("hydrated_rows=0"));
        }
        for sql in [
            "SELECT 1 FROM having_overflow HAVING MAX(body) = 'x'",
            "SELECT owner FROM having_overflow GROUP BY owner HAVING COUNT(DISTINCT body) > 0",
            "SELECT 1 FROM having_overflow HAVING COUNT(*) FILTER (WHERE body = 'x') > 0",
        ] {
            let error = database.query_sql(sql).unwrap_err();
            assert!(
                error.to_string().contains("overflow hydration"),
                "{sql}: {error}"
            );
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}
