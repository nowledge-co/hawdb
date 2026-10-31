// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::StorageResidencyMode;
use hawdb_executor::Row;
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
    assert!(database
        .query_sql_with_params("SELECT $1 + 0.1", &[Value::Float(1.0)])
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
fn native_decimal_graph_wal_and_checkpoint_keep_payload_representation() {
    let path = unique_test_dir("native-decimal-graph-recovery");
    let parameters = BTreeMap::from([("amount".into(), decimal("9007199254740993.0100"))]);
    let assert_value = |database: &mut Database| {
        let rows = database
            .query("MATCH (n:Money) RETURN n.amount AS amount")
            .unwrap()
            .rows;
        let Value::Decimal(amount) = rows[0]["amount"] else {
            panic!("native Decimal");
        };
        assert_eq!(amount.to_string(), "9007199254740993.0100");
    };
    let mut database = Database::open(&path).unwrap();
    database
        .query_with_params("CREATE (:Money {amount: $amount})", &parameters)
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
