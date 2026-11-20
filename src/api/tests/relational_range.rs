// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn seed(database: &mut Database, index: Option<&str>) {
    database.query_sql("CREATE TABLE ranges (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL, score BIGINT, body TEXT NOT NULL)").unwrap();
    let rows = (0..180)
        .map(|id| {
            let score = if id % 19 == 0 {
                "NULL".into()
            } else {
                (id / 3).to_string()
            };
            format!(
                "({id}, '{}', {score}, '{}')",
                if id < 120 { "a" } else { "b" },
                if id % 2 == 0 { "even" } else { "odd" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    database
        .query_sql(&format!(
            "INSERT INTO ranges (id, tenant, score, body) VALUES {rows}"
        ))
        .unwrap();
    if let Some(columns) = index {
        database
            .query_sql(&format!("CREATE INDEX ranges_order ON ranges ({columns})"))
            .unwrap();
    }
}

fn assert_same(database: &mut Database, reference: &mut Database, sql: &str, params: &[Value]) {
    let actual = database.query_sql_with_params(sql, params).unwrap();
    let expected = reference.query_sql_with_params(sql, params).unwrap();
    assert_eq!(actual.schema(), expected.schema(), "{sql}");
    assert_eq!(actual.rows, expected.rows, "{sql}, {params:?}");
}

fn assert_ordered_range(database: &mut Database, sql: &str, params: &[Value], column: &str) {
    let plan = database
        .query_sql_with_params(&format!("EXPLAIN {sql}"), params)
        .unwrap();
    assert!(plan.rows.iter().any(|row| matches!(row.get("operator info"), Some(Value::String(info)) if info.contains(&format!("range_column={column}")))), "{plan:?}");
    assert!(plan.rows.iter().any(|row| matches!(row.get("id"), Some(Value::String(id)) if id.contains("IndexRangeScanExec"))), "{plan:?}");
    assert!(plan.rows.iter().all(|row| !matches!(row.get("id"), Some(Value::String(id)) if id.contains("TopNExec") || id.contains("SortExec"))), "{plan:?}");
}

#[test]
fn sql_composite_range_bounds_preserve_nulls_duplicates_and_parameter_rebinding() {
    let mut db = Database::new();
    let mut reference = Database::new();
    seed(&mut db, Some("tenant, score, id"));
    seed(&mut reference, None);
    for lower in [">", ">="] {
        for upper in ["<", "<="] {
            for direction in ["ASC", "DESC"] {
                let sql = format!("SELECT id, score, body FROM ranges AS r WHERE r.tenant = 'a' AND r.score {lower} $1 AND r.score {upper} $2 ORDER BY score {direction}, id {direction} OFFSET 1 LIMIT 5");
                for params in [
                    vec![Value::Int(9), Value::Int(14)],
                    vec![Value::Int(14), Value::Int(14)],
                    vec![Value::Int(18), Value::Int(9)],
                    vec![Value::Null, Value::Int(10)],
                ] {
                    assert_same(&mut db, &mut reference, &sql, &params);
                    assert_ordered_range(&mut db, &sql, &params, "score");
                }
            }
        }
    }
    for predicate in [
        "tenant = 'a' AND score <= 3",
        "tenant = 'a' AND score >= 30",
        "tenant = 'a' AND score > 9 AND score >= 9 AND score < 12 AND score <= 15",
        "tenant = 'a' AND score >= 9 AND score > 9 AND score <= 15 AND score < 12",
    ] {
        for direction in ["ASC", "DESC"] {
            let sql = format!("SELECT id, score FROM ranges WHERE {predicate} ORDER BY score {direction}, id {direction} LIMIT 7");
            assert_same(&mut db, &mut reference, &sql, &[]);
            assert_ordered_range(&mut db, &sql, &[], "score");
        }
    }
    // A keyset continuation intersects the domain bounds before LIMIT; neither
    // the OR cursor nor the additional interval may become an unhandled filter.
    for (op, direction) in [(">", "ASC"), ("<", "DESC")] {
        let sql = format!("SELECT id, score FROM ranges WHERE tenant = 'a' AND score >= $1 AND score <= $2 AND (score {op} $3 OR (score = $3 AND id {op} $4)) ORDER BY score {direction}, id {direction} LIMIT 5");
        for params in [
            vec![
                Value::Int(9),
                Value::Int(15),
                Value::Int(11),
                Value::Int(34),
            ],
            vec![
                Value::Int(12),
                Value::Int(15),
                Value::Int(30),
                Value::Int(90),
            ],
        ] {
            assert_same(&mut db, &mut reference, &sql, &params);
            assert_ordered_range(&mut db, &sql, &params, "score");
        }
    }
    // Uncovered conditions must still be evaluated, even after an interval was
    // derived. They cannot authorize the predicate-free ordered projection.
    for predicate in [
        "tenant = 'a' AND score > 9 AND score < 16 AND body = 'odd'",
        "tenant = 'a' AND tenant = 'b' AND score > 9",
        "tenant = 'a' AND score > 9 AND (score < 12 OR body = 'odd')",
        "tenant = 'a' AND score > 9 AND score < 16 AND (score > 10 OR (score = 10 AND id > 30)) AND body = 'odd'",
    ] {
        let sql = format!("SELECT id FROM ranges WHERE {predicate} ORDER BY score, id LIMIT 5");
        assert_same(&mut db, &mut reference, &sql, &[]);
    }
    for sql in [
        "SELECT COUNT(*) AS n FROM ranges WHERE tenant = 'a' AND score > 9 AND score < 14",
        "SELECT COUNT(*) AS n FROM ranges WHERE tenant = 'a' AND score > NULL",
    ] {
        assert_same(&mut db, &mut reference, sql, &[]);
    }
}

#[test]
fn sql_index_range_on_first_column_needs_no_equality_prefix() {
    let mut db = Database::new();
    let mut reference = Database::new();
    seed(&mut db, Some("score, id"));
    seed(&mut reference, None);
    for predicate in [
        "score > 10",
        "score >= 10 AND score <= 14",
        "score < 4",
        "score <= 0",
        "score > NULL",
    ] {
        for direction in ["ASC", "DESC"] {
            let sql = format!("SELECT id, score FROM ranges WHERE {predicate} ORDER BY score {direction}, id {direction} LIMIT 5");
            assert_same(&mut db, &mut reference, &sql, &[]);
            assert_ordered_range(&mut db, &sql, &[], "score");
        }
    }
}

#[test]
fn sql_composite_ranges_preserve_checkpoint_live_transaction_and_recovery_views() {
    use hawdb_storage::config::RelationalIndexMode;
    let sql = "SELECT id, score FROM ranges WHERE tenant = 'a' AND score > 9 AND score <= 15 ORDER BY score DESC, id DESC LIMIT 20";
    for mode in [
        RelationalIndexMode::Shadow,
        RelationalIndexMode::DemandPaged,
        RelationalIndexMode::Authoritative,
    ] {
        let path = unique_test_dir("sql-composite-range");
        let config = DatabaseConfig {
            relational_index_mode: mode,
            ..Default::default()
        };
        let mut reference = Database::new();
        seed(&mut reference, None);
        let bootstrap = DatabaseConfig {
            relational_index_mode: if mode == RelationalIndexMode::Authoritative {
                RelationalIndexMode::DemandPaged
            } else {
                mode
            },
            ..config.clone()
        };
        let mut db = Database::open_with_config(&path, bootstrap).unwrap();
        seed(&mut db, Some("tenant, score, id"));
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        assert_same(&mut db, &mut reference, sql, &[]);
        assert_ordered_range(&mut db, sql, &[], "score");
        let pinned = db.begin_read_transaction().unwrap();
        let before = pinned.query_sql(sql).unwrap();
        for mutation in [
            "UPDATE ranges SET score = 15 WHERE id = 3",
            "UPDATE ranges SET score = 40 WHERE id = 31",
            "DELETE FROM ranges WHERE id = 32",
            "INSERT INTO ranges (id, tenant, score, body) VALUES (1000, 'a', 12, 'live')",
        ] {
            db.query_sql(mutation).unwrap();
            reference.query_sql(mutation).unwrap();
        }
        assert_same(&mut db, &mut reference, sql, &[]);
        assert_eq!(pinned.query_sql(sql).unwrap().rows, before.rows);
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query_sql(
                "INSERT INTO ranges (id, tenant, score, body) VALUES (1001, 'a', 13, 'private')",
            )
            .unwrap();
        let rows = transaction.query_sql(sql).unwrap();
        assert!(rows
            .rows
            .iter()
            .any(|row| row.get("id") == Some(&Value::Int(1001))));
        transaction.rollback();
        assert_same(&mut db, &mut reference, sql, &[]);
        drop(pinned);
        drop(db);
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_same(&mut db, &mut reference, sql, &[]);
        db.checkpoint().unwrap();
        assert_same(&mut db, &mut reference, sql, &[]);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn sql_unique_composite_range_requires_complete_nullable_suffix_coverage() {
    let mut reference = Database::new();
    seed(&mut reference, None);
    let mut db = Database::new();
    seed(&mut db, None);
    db.query_sql("CREATE UNIQUE INDEX ranges_order ON ranges (tenant, score, id)")
        .unwrap();
    let sql = "SELECT id, score FROM ranges WHERE tenant = 'a' AND score <= 4 ORDER BY score DESC, id DESC LIMIT 5";
    assert_same(&mut db, &mut reference, sql, &[]);
    assert_ordered_range(&mut db, sql, &[], "score");
    let mut sparse = Database::new();
    seed(&mut sparse, None);
    sparse
        .query_sql("ALTER TABLE ranges ADD COLUMN extra TEXT")
        .unwrap();
    sparse
        .query_sql("CREATE UNIQUE INDEX ranges_sparse ON ranges (tenant, score, extra)")
        .unwrap();
    assert_same(&mut sparse, &mut reference, sql, &[]);
    let plan = sparse.query_sql(&format!("EXPLAIN {sql}")).unwrap();
    assert!(plan.rows.iter().all(|row| !matches!(row.get("access object"), Some(Value::String(access)) if access.contains("ranges_sparse"))));
}

#[test]
fn sql_composite_range_seeks_a_late_page_under_a_four_row_read_budget() {
    use hawdb_storage::config::RelationalIndexMode;
    let path = unique_test_dir("sql-range-budget");
    let config = DatabaseConfig {
        max_read_result_rows: Some(4),
        relational_index_mode: RelationalIndexMode::DemandPaged,
        ..Default::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    db.query_sql("CREATE TABLE budget_ranges (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL, score BIGINT NOT NULL)").unwrap();
    db.query_sql("CREATE INDEX budget_ranges_order ON budget_ranges (tenant, score, id)")
        .unwrap();
    for start in (0..5_000).step_by(100) {
        let rows = (start..start + 100)
            .map(|id| format!("({id}, 'a', {id})"))
            .collect::<Vec<_>>()
            .join(",");
        db.query_sql(&format!(
            "INSERT INTO budget_ranges (id, tenant, score) VALUES {rows}"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    let sql = "SELECT id FROM budget_ranges WHERE tenant = 'a' AND score >= 4900 AND score < 4950 ORDER BY score DESC, id DESC LIMIT 3";
    assert_ordered_range(&mut db, sql, &[], "score");
    let rows = db.query_sql(sql).unwrap();
    assert_eq!(
        rows.rows
            .iter()
            .map(|row| row["id"].clone())
            .collect::<Vec<_>>(),
        [Value::Int(4949), Value::Int(4948), Value::Int(4947)]
    );
    let profile = db.query_sql(&format!("EXPLAIN ANALYZE {sql}")).unwrap();
    assert!(profile.rows.iter().any(|row| matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("runtime_path=demand_paged") && info.contains("index_rows=3,") && info.contains("early_stop_lookups=1"))), "{profile:?}");
    let error = db.query_sql("SELECT id FROM budget_ranges WHERE tenant = 'a' AND score >= 4900 AND score < 4950 ORDER BY score DESC, id DESC").unwrap_err();
    assert!(
        matches!(
            error,
            HawDBError::ReadBudgetExceeded(_) | HawDBError::Execution(_)
        ),
        "{error}"
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

fn query_ids(database: &mut Database, sql: &str, parameters: &[Value]) -> Vec<Value> {
    database
        .query_sql_with_params(sql, parameters)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row["id"].clone())
        .collect()
}

#[test]
fn sql_row_comparisons_preserve_postgresql_null_and_lexicographic_semantics() {
    let mut db = Database::new();
    db.query_sql(
        "CREATE TABLE row_cmp (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, body TEXT NOT NULL)",
    )
    .unwrap();
    db.query_sql("INSERT INTO row_cmp (id, a, b, body) VALUES (1, 1, 1, 'one'), (2, 1, 2, 'two'), (3, 2, 0, 'three'), (4, NULL, 0, 'null-a'), (5, 1, NULL, 'null-b')")
        .unwrap();

    for (predicate, expected) in [
        ("(a, b) > (1, 1)", vec![2, 3]),
        ("(a, b) >= (1, 1)", vec![1, 2, 3]),
        ("(a, b) < (1, 1)", vec![]),
        ("(a, b) <= (1, 1)", vec![1]),
        ("(a, b) = (1, 1)", vec![1]),
        ("(a, b) <> (1, 1)", vec![2, 3, 4]),
        ("ROW(a, b) <= ROW(1, 2)", vec![1, 2]),
        ("(a + 1, b) > (2, 1)", vec![2, 3]),
        ("(1, 1) < (a, b)", vec![2, 3]),
        // The first field decides these rows, so a later NULL is not read.
        ("(a, b) < (2, NULL)", vec![1, 2, 5]),
        // The first fields tie, so the NULL second field makes the result unknown.
        ("(a, b) < (1, NULL)", vec![]),
    ] {
        let sql = format!("SELECT id FROM row_cmp WHERE {predicate} ORDER BY id");
        assert_eq!(
            query_ids(&mut db, &sql, &[]),
            expected.into_iter().map(Value::Int).collect::<Vec<_>>(),
            "{predicate}"
        );
    }
    assert_eq!(
        query_ids(
            &mut db,
            "SELECT id FROM row_cmp WHERE (a, b) > ($1, $2) ORDER BY id",
            &[Value::Int(1), Value::Int(1)],
        ),
        vec![Value::Int(2), Value::Int(3)]
    );
    assert!(query_ids(
        &mut db,
        "SELECT id FROM row_cmp WHERE (a, b) > ($1, $2) ORDER BY id",
        &[Value::Null, Value::Int(0)],
    )
    .is_empty());
    let type_error = db
        .query_sql("SELECT id FROM row_cmp WHERE (a, b) > (1, 'not-a-number')")
        .unwrap_err();
    assert!(
        matches!(
            type_error,
            HawDBError::Semantic(_) | HawDBError::Execution(_)
        ),
        "{type_error}"
    );

    let joined = db
        .query_sql("SELECT r.id FROM row_cmp AS l JOIN row_cmp AS r ON (l.a, l.b) = (r.a, r.b) WHERE l.id = 2 ORDER BY r.id")
        .unwrap();
    assert_eq!(joined.rows.len(), 1);
    assert_eq!(joined.rows[0]["id"], Value::Int(2));

    let grouped = db
        .query_sql("SELECT a, COUNT(*) AS n FROM row_cmp GROUP BY a HAVING (a, COUNT(*)) = (1, 3)")
        .unwrap();
    assert_eq!(grouped.rows.len(), 1);
    assert_eq!(grouped.rows[0]["a"], Value::Int(1));
    assert_eq!(grouped.rows[0]["n"], Value::Int(3));

    db.query_sql("UPDATE row_cmp SET body = 'hit' WHERE (a, b) >= (2, 0)")
        .unwrap();
    assert_eq!(
        query_ids(
            &mut db,
            "SELECT id FROM row_cmp WHERE body = 'hit' ORDER BY id",
            &[]
        ),
        vec![Value::Int(3)]
    );
    db.query_sql("DELETE FROM row_cmp WHERE (a, b) <> (1, 1)")
        .unwrap();
    assert_eq!(
        query_ids(&mut db, "SELECT id FROM row_cmp ORDER BY id", &[]),
        vec![Value::Int(1), Value::Int(5)]
    );
}

fn seed_row_keyset(database: &mut Database, indexed: bool) {
    database
        .query_sql("CREATE TABLE row_keyset (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL, bucket BIGINT NOT NULL, rank BIGINT NOT NULL)")
        .unwrap();
    let rows = (0..120)
        .map(|id| {
            format!(
                "({id}, '{}', {}, {})",
                if id < 100 { "a" } else { "b" },
                id / 20,
                id % 5
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    database
        .query_sql(&format!(
            "INSERT INTO row_keyset (id, tenant, bucket, rank) VALUES {rows}"
        ))
        .unwrap();
    if indexed {
        database
            .query_sql("CREATE INDEX row_keyset_cursor ON row_keyset (tenant, bucket, rank, id)")
            .unwrap();
    }
}

#[test]
fn sql_arbitrary_row_keyset_uses_complete_index_bound_after_reopen() {
    use hawdb_storage::config::RelationalIndexMode;

    let path = unique_test_dir("sql-row-keyset");
    let config = DatabaseConfig {
        max_read_result_rows: Some(8),
        relational_index_mode: RelationalIndexMode::DemandPaged,
        ..Default::default()
    };
    let mut reference = Database::new();
    seed_row_keyset(&mut reference, false);
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    seed_row_keyset(&mut db, true);
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open_with_config(&path, config).unwrap();

    for (operator, direction, parameters) in [
        (
            ">",
            "ASC",
            vec![Value::Int(3), Value::Int(1), Value::Int(61)],
        ),
        (
            "<",
            "DESC",
            vec![Value::Int(3), Value::Int(1), Value::Int(61)],
        ),
    ] {
        let sql = format!(
            "SELECT id, bucket, rank FROM row_keyset WHERE tenant = 'a' AND (bucket, rank, id) {operator} ($1, $2, $3) ORDER BY bucket {direction}, rank {direction}, id {direction} LIMIT 5"
        );
        assert_same(&mut db, &mut reference, &sql, &parameters);
        let plan = db
            .query_sql_with_params(&format!("EXPLAIN {sql}"), &parameters)
            .unwrap();
        assert!(plan.rows.iter().any(|row| matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("exclusive_seek=true") && info.contains("order_prefix=3"))), "{plan:?}");
        assert!(plan.rows.iter().all(|row| !matches!(row.get("id"), Some(Value::String(id)) if id.contains("SortExec") || id.contains("SelectionExec"))), "{plan:?}");
    }

    for (operator, direction) in [(">", "ASC"), ("<", "DESC")] {
        let parameters = [
            Value::String("a".to_string()),
            Value::Int(3),
            Value::Int(1),
            Value::Int(61),
        ];
        let sql = format!(
            "SELECT id, bucket, rank FROM row_keyset WHERE (tenant, bucket, rank, id) {operator} ($1, $2, $3, $4) ORDER BY tenant {direction}, bucket {direction}, rank {direction}, id {direction} LIMIT 5"
        );
        assert_same(&mut db, &mut reference, &sql, &parameters);
        let plan = db
            .query_sql_with_params(&format!("EXPLAIN {sql}"), &parameters)
            .unwrap();
        assert!(plan.rows.iter().any(|row| matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("exclusive_seek=true") && info.contains("order_prefix=4"))), "{plan:?}");
        assert!(plan.rows.iter().all(|row| !matches!(row.get("id"), Some(Value::String(id)) if id.contains("SortExec") || id.contains("SelectionExec"))), "{plan:?}");
    }

    let null_plan = db
        .query_sql_with_params(
            "EXPLAIN SELECT id FROM row_keyset WHERE (tenant, bucket, rank, id) > ($1, $2, $3, $4) ORDER BY tenant, bucket, rank, id LIMIT 5",
            &[Value::Null, Value::Int(3), Value::Int(1), Value::Int(61)],
        )
        .unwrap();
    assert!(null_plan.rows.iter().all(|row| !matches!(row.get("operator info"), Some(Value::String(info)) if info.contains("exclusive_seek=true"))), "{null_plan:?}");

    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
