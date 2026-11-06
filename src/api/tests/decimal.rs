// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::StorageResidencyMode;
use hawdb_executor::Row;
use hawdb_plan_cypher::{PhysicalPlan, Projection, ProjectionExpression};
use hawdb_storage::config::RelationalIndexMode;

fn decimal(text: &str) -> Value {
    Value::Decimal(text.parse().unwrap())
}

fn assert_money(database: &mut Database) {
    let keys = database
        .query_sql("SELECT k, marker FROM decimal_keys ORDER BY k, n")
        .unwrap()
        .rows;
    assert_eq!(
        keys,
        vec![
            Row::from([
                ("k".into(), decimal("1.24")),
                ("marker".into(), Value::Int(3))
            ]),
            Row::from([
                ("k".into(), decimal("1.25")),
                ("marker".into(), Value::Int(0))
            ]),
        ]
    );
    let result = database
        .query_sql("SELECT id, amount, quantity FROM money ORDER BY id")
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            Row::from([
                ("id".into(), decimal("1.00")),
                ("amount".into(), decimal("18014398509481986.03")),
                ("quantity".into(), Value::Int(3))
            ]),
            Row::from([
                ("id".into(), decimal("2.00")),
                ("amount".into(), decimal("1.24")),
                ("quantity".into(), Value::Int(2))
            ]),
        ]
    );
    let Value::Decimal(amount) = result.rows[0]["amount"] else {
        panic!("native Decimal");
    };
    assert_eq!(amount.scale(), 2);
    let result = database
        .query_sql_with_params(
            "SELECT id FROM money WHERE amount = $1",
            &[decimal("1.24000")],
        )
        .unwrap();
    assert_eq!(result.rows, vec![Row::from([("id".into(), decimal("2"))])]);
    assert_eq!(
        database
            .query_sql("SELECT SUM(amount) AS total FROM money")
            .unwrap()
            .rows[0]["total"],
        decimal("18014398509481987.27")
    );
}

#[test]
fn native_decimal_schema_wal_checkpoint_and_index_recovery() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("native-decimal-recovery");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: RelationalIndexMode::DemandPaged,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        database.query_sql("CREATE TABLE money (id DECIMAL(10,2) PRIMARY KEY, amount DECIMAL(38,2) DEFAULT 1.235, quantity BIGINT NOT NULL)").unwrap();
        database.query_sql("CREATE TABLE decimal_keys (k DECIMAL(5,2) NOT NULL, n BIGINT NOT NULL, marker BIGINT NOT NULL, PRIMARY KEY (k,n))").unwrap();
        database.query_sql("INSERT INTO decimal_keys (k,n,marker) VALUES (1.23,1,0),(1.23,2,0),(1.24,1,0),(1.25,1,0)").unwrap();
        database
            .query_sql("CREATE INDEX money_amount ON money (amount)")
            .unwrap();
        database
            .query_sql_with_params(
                "INSERT INTO money (id, amount, quantity) VALUES ($1, $2, 2)",
                &[decimal("1"), decimal("9007199254740993.01")],
            )
            .unwrap();
        database.checkpoint().unwrap();
        drop(database);

        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        let mut transaction = database.begin_transaction().unwrap();
        transaction
            .query_sql(
                "UPDATE decimal_keys SET marker = marker + 3 WHERE k >= 1.2301 AND k <= 1.2401",
            )
            .unwrap();
        transaction
            .query_sql("DELETE FROM decimal_keys WHERE k = 1.2300")
            .unwrap();
        transaction.query_sql_with_params("UPDATE money SET amount = amount * quantity + $1, quantity = quantity + 1 WHERE id = $2", &[decimal("0.005"), decimal("1.000")]).unwrap();
        transaction
            .query_sql("INSERT INTO money (id, quantity) VALUES (2, 2)")
            .unwrap();
        transaction.commit().unwrap();
        assert_money(&mut database);
        assert!(database
            .query_sql("INSERT INTO money (id, quantity) VALUES (1.001, 3)")
            .is_err());
        assert!(database
            .query_sql(
                "UPDATE money SET quantity = quantity / CAST(id - 2 AS BIGINT) WHERE id <= 2"
            )
            .is_err());
        assert_money(&mut database);
        drop(database);

        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        assert_money(&mut database);
        database.checkpoint().unwrap();
        drop(database);
        let mut database = Database::open_with_config(&path, config).unwrap();
        assert_money(&mut database);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn native_decimal_fromless_projection_and_explicit_float_cast() {
    let mut database = Database::new();
    let result = database
        .query_sql_with_params(
            "SELECT CAST(($1 + 0.2) * 10 / 3 AS DECIMAL(20,4)) AS amount",
            &[decimal("0.1")],
        )
        .unwrap();
    let Value::Decimal(value) = result.rows[0]["amount"] else {
        panic!("native Decimal");
    };
    assert_eq!(value.to_string(), "1.0000");
    // #1516: a numeric literal beside a DOUBLE operand converts to DOUBLE,
    // as PostgreSQL's implicit numeric-to-float8 cast does; typed DECIMAL
    // operands beside DOUBLE still require an explicit CAST (#1429).
    assert_eq!(
        database
            .query_sql_with_params("SELECT $1 + 0.1 AS x", &[Value::Float(1.0)])
            .unwrap()
            .rows[0]["x"],
        Value::Float(1.0 + 0.1)
    );
    assert!(database
        .query_sql_with_params(
            "SELECT $1 + CAST(0.1 AS DECIMAL(8,2))",
            &[Value::Float(1.0)]
        )
        .is_err());
    assert_eq!(
        database
            .query_sql_with_params(
                "SELECT CAST($1 AS DECIMAL(8,2)) + 0.1 AS amount",
                &[Value::Float(1.0)]
            )
            .unwrap()
            .rows[0]["amount"],
        decimal("1.1")
    );
}

#[test]
fn native_decimal_lock_boundaries_preserve_unrounded_comparison_values() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("native-decimal-lock-boundaries");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            ..Default::default()
        };
        let mut setup = Database::open_with_config(&path, config.clone()).unwrap();
        setup
            .query_sql("CREATE TABLE prices (k DECIMAL(5,2) PRIMARY KEY)")
            .unwrap();
        setup.checkpoint().unwrap();
        drop(setup);
        let db = Database::open_with_config(&path, config)
            .unwrap()
            .into_concurrent();
        let options = crate::ConcurrentTransactionOptions::pessimistic(std::time::Duration::ZERO);
        let mut owner = db.begin_transaction(options).unwrap();
        assert!(owner
            .query_sql("SELECT k FROM prices WHERE k > 1.235 AND k < 1.245 FOR UPDATE")
            .unwrap()
            .rows
            .is_empty());
        let mut blocked = db.begin_transaction(options).unwrap();
        let error = blocked
            .query_sql("INSERT INTO prices (k) VALUES (1.24)")
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("transaction lock wait timed out"),
            "{error}"
        );
        drop(blocked);
        for key in ["1.23", "1.25"] {
            let mut disjoint = db.begin_transaction(options).unwrap();
            disjoint
                .query_sql_with_params("INSERT INTO prices (k) VALUES ($1)", &[decimal(key)])
                .unwrap();
            disjoint.commit().unwrap();
        }
        owner.rollback();
        let mut released = db.begin_transaction(options).unwrap();
        released
            .query_sql("INSERT INTO prices (k) VALUES (1.24)")
            .unwrap();
        released.commit().unwrap();
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn native_decimal_graph_wal_and_checkpoint_keep_payload_representation() {
    let path = unique_test_dir("native-decimal-graph-recovery");
    let parameters = BTreeMap::from([
        ("amount".into(), decimal("9007199254740993.0100")),
        (
            "date".into(),
            Value::Date(crate::temporal::parse_date("2026-10-30").unwrap()),
        ),
        (
            "time".into(),
            Value::LocalTime(crate::temporal::parse_local_time("12:34:56").unwrap()),
        ),
        (
            "local".into(),
            Value::LocalDateTime(
                crate::temporal::parse_local_datetime("2026-10-30T12:34:56").unwrap(),
            ),
        ),
        (
            "zoned".into(),
            Value::DateTime(crate::temporal::parse_datetime("2026-10-30T12:34:56+02:00").unwrap()),
        ),
        (
            "duration".into(),
            Value::duration(crate::temporal::parse_duration("P1DT2H").unwrap()),
        ),
    ]);
    let assert_value = |database: &mut Database| {
        let rows = database
            .query("MATCH (n:Money) RETURN n.amount AS amount, n.date AS date, n.time AS time, n.local AS local, n.zoned AS zoned, n.duration AS duration")
            .unwrap()
            .rows;
        let Value::Decimal(amount) = rows[0]["amount"] else {
            panic!("native Decimal");
        };
        assert_eq!(amount.to_string(), "9007199254740993.0100");
        for name in ["date", "time", "local", "zoned", "duration"] {
            assert_eq!(rows[0][name], parameters[name]);
        }
    };
    let mut database = Database::open(&path).unwrap();
    database
        .query_with_params("CREATE (:Money {amount: $amount, date: $date, time: $time, local: $local, zoned: $zoned, duration: $duration})", &parameters)
        .unwrap();
    drop(database);
    let mut database = Database::open(&path).unwrap();
    assert_value(&mut database);
    database.checkpoint().unwrap();
    drop(database);
    let mut database = Database::open(&path).unwrap();
    assert_value(&mut database);
    drop(database);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn native_decimal_cypher_predicates_and_order_survive_indexed_recovery() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir("native-decimal-cypher-predicates");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        for (id, amount) in ["-2", "1.00", "1.25", "2", "9007199254740993.01"]
            .into_iter()
            .enumerate()
        {
            database
                .query_with_params(
                    "CREATE (:Price {id: $id, bucket: 'main', amount: $amount})",
                    &BTreeMap::from([
                        ("id".into(), Value::Int(id as i64)),
                        ("amount".into(), decimal(amount)),
                    ]),
                )
                .unwrap();
        }
        database
            .query("CREATE (:Price {id: 5, bucket: 'main'})")
            .unwrap();
        for id in 6..134 {
            database
                .query_with_params(
                    "CREATE (:Price {id: $id, bucket: 'padding'})",
                    &BTreeMap::from([("id".into(), Value::Int(id))]),
                )
                .unwrap();
        }
        database
            .query("CREATE RANGE INDEX ON :Price(amount)")
            .unwrap();
        database
            .query("CREATE INDEX ON :Price(bucket, amount)")
            .unwrap();
        let parameters = BTreeMap::from([
            ("lower".into(), decimal("1.0000")),
            ("upper".into(), decimal("1.2500")),
            ("wide".into(), Value::Int(9_007_199_254_740_993)),
            ("float".into(), Value::Float(1.1)),
            ("integers".into(), Value::List(vec![Value::Int(1)])),
            (
                "set".into(),
                Value::List(vec![decimal("1.000"), decimal("1.250"), Value::Null]),
            ),
        ]);
        for stage in 0..3 {
            assert_decimal_range_operators(&mut database);
            for (predicate, expected) in [
                ("n.amount > $lower", vec![2, 3, 4]),
                ("n.amount <= $upper", vec![0, 1, 2]),
                ("n.amount > 1 AND n.amount <= $upper", vec![2]),
                ("n.amount > $lower AND n.amount <= 2", vec![2, 3]),
                ("n.amount > $lower AND n.amount > 2", vec![4]),
                ("n.amount > 2 AND n.amount > $lower", vec![4]),
                ("n.amount > $wide", vec![4]),
                ("n.amount > $float", vec![]),
                ("n.amount > $lower AND n.amount > $float", vec![]),
                ("n.amount > $float AND n.amount > $lower", vec![]),
                ("n.amount IN $set", vec![1, 2]),
                ("n.amount IN $integers", vec![]),
                ("NOT (n.amount <= $lower)", vec![2, 3, 4]),
                ("n.amount IS NOT NULL", vec![0, 1, 2, 3, 4]),
            ] {
                for prefix in ["", "n.bucket = 'main' AND "] {
                    let query = format!(
                        "MATCH (n:Price) WHERE {prefix}{predicate} RETURN n.id AS id ORDER BY n.amount, n.id"
                    );
                    let rows = database
                        .query_with_params(&query, &parameters)
                        .unwrap()
                        .rows;
                    assert_eq!(
                        rows.iter().map(|row| row["id"].clone()).collect::<Vec<_>>(),
                        expected.iter().copied().map(Value::Int).collect::<Vec<_>>(),
                        "{residency:?} stage {stage}: {query}"
                    );
                }
            }
            if stage == 1 {
                database.checkpoint().unwrap();
            }
            drop(database);
            database = Database::open_with_config(&path, config.clone()).unwrap();
        }
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn assert_decimal_range_operators(database: &mut Database) {
    // Decimal properties currently have no histograms, so cost-based planning
    // may prefer a scan. Execute each index operator directly in every recovery
    // stage, independently of the ordinary query correctness checks above.
    for (lower, upper, expected) in [
        (
            Some((decimal("1.0000"), false)),
            Some((decimal("1.2500"), true)),
            vec![Value::Int(2)],
        ),
        (
            Some((Value::Int(9_007_199_254_740_993), false)),
            None,
            vec![Value::Int(4)],
        ),
        (Some((Value::Float(1.1), false)), None, vec![]),
    ] {
        for input in [
            PhysicalPlan::IndexNodeRangeSeek {
                variable: "n".into(),
                label: "Price".into(),
                property: "amount".into(),
                lower: lower.clone(),
                upper: upper.clone(),
            },
            PhysicalPlan::IndexNodeCompositeRangeSeek {
                variable: "n".into(),
                label: "Price".into(),
                seek: hawdb_core::access::CompositeRangeSeek {
                    index_properties: vec!["bucket".into(), "amount".into()],
                    equality_prefix: vec![("bucket".into(), Value::String("main".into()))],
                    range_property: "amount".into(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                },
            },
        ] {
            let plan = PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "n".into(),
                        property: "id".into(),
                    },
                    name: "id".into(),
                }],
                input: Box::new(input),
            };
            let mut access = database.runtime.get_mut().unwrap();
            let runtime = &mut *access;
            let rows =
                crate::executor::execute(&plan, &mut runtime.catalog, &mut runtime.store).unwrap();
            let mut ids = rows
                .into_iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>();
            ids.sort();
            assert_eq!(ids, expected, "{}", plan.explain(0));
        }
    }
}

#[test]
fn native_decimal_defaults_preserve_main_type_aliases_and_contextual_strings() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE decimal_defaults (id INTEGER PRIMARY KEY, quoted NUMERIC(22,2) DEFAULT '9007199254740993.125', exact DECIMAL(22,2) DEFAULT 9007199254740993.125, approximate FLOAT(53) DEFAULT '1.25', enabled BOOLEAN DEFAULT 'yes', label VARCHAR(10) DEFAULT 'kept')").unwrap();
    db.query_sql("INSERT INTO decimal_defaults (id) VALUES (1)")
        .unwrap();
    let rows = db
        .query_sql("SELECT quoted, exact, approximate, enabled, label FROM decimal_defaults")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["quoted"], decimal("9007199254740993.13"));
    assert_eq!(rows[0]["exact"], decimal("9007199254740993.13"));
    assert_eq!(rows[0]["approximate"], Value::Float(1.25));
    assert_eq!(rows[0]["enabled"], Value::Bool(true));
    assert_eq!(rows[0]["label"], Value::String("kept".into()));
    for default in ["'NaN'", "'10.00'", "'1e100'"] {
        assert!(db.query_sql(&format!("CREATE TABLE invalid_decimal_default (id BIGINT PRIMARY KEY, amount DECIMAL(3,2) DEFAULT {default})")).is_err());
        assert!(db
            .query_sql("SELECT id FROM invalid_decimal_default")
            .is_err());
    }
}

#[test]
fn native_decimal_secondary_rekeys_lock_coerced_values_and_preserve_disjoint_reads() {
    for (residency, indexes) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ] {
        let path = unique_test_dir("decimal-secondary-locks");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: indexes,
            ..Default::default()
        };
        let mut setup = Database::open_with_config(&path, config.clone()).unwrap();
        setup
            .query_sql("CREATE TABLE decimal_locks (id BIGINT PRIMARY KEY, amount DECIMAL(5,2))")
            .unwrap();
        setup
            .query_sql("CREATE INDEX decimal_amount ON decimal_locks (amount)")
            .unwrap();
        setup
            .query_sql("INSERT INTO decimal_locks VALUES (1, 1.00), (2, 5.00)")
            .unwrap();
        setup.checkpoint().unwrap();
        drop(setup);
        let db = Database::open_with_config(&path, config)
            .unwrap()
            .into_concurrent();
        let options = crate::ConcurrentTransactionOptions::pessimistic(std::time::Duration::ZERO);
        let mut writer = db.begin_transaction(options).unwrap();
        writer
            .query_sql_with_params(
                "UPDATE decimal_locks SET amount = $1 WHERE id = 1",
                &[decimal("1.235")],
            )
            .unwrap();
        for bound in ["1.00", "1.24"] {
            let mut blocked = db.begin_transaction(options).unwrap();
            let error = blocked
                .query_sql_with_params(
                    "SELECT id FROM decimal_locks WHERE amount = $1 FOR UPDATE",
                    &[decimal(bound)],
                )
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("transaction lock wait timed out"),
                "{error}"
            );
            blocked.rollback();
        }
        let mut disjoint = db.begin_transaction(options).unwrap();
        assert_eq!(
            disjoint
                .query_sql("SELECT id FROM decimal_locks WHERE amount = 5.00 FOR UPDATE")
                .unwrap()
                .rows
                .len(),
            1
        );
        disjoint.rollback();
        writer.commit().unwrap();
        assert_eq!(
            db.query_sql("SELECT amount FROM decimal_locks WHERE id = 1")
                .unwrap()
                .rows[0]["amount"],
            decimal("1.24")
        );
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
