// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn decimal(value: &str) -> Value {
    Value::Decimal(value.parse().unwrap())
}

fn write(state: &mut RelationalState, sql: &str, parameters: &[Value]) -> Result<()> {
    let transaction = compile_relational_statement_sql(sql, parameters, state)?;
    *state = state
        .stage_transaction(transaction, Default::default(), Default::default())
        .map_err(|error| HawDBError::Execution(error.to_string()))?;
    Ok(())
}

fn query(state: &RelationalState, sql: &str, parameters: &[Value]) -> Result<QueryRows> {
    let modes = RelationalQueryReadModes::new(
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
        RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
    );
    Ok(execute_relational_query_sql_with_runtime(
        sql,
        parameters,
        state,
        modes,
        batched_index_join_limits(),
        &hawdb_executor::ExecutionMemoryConfig::default(),
        None,
    )?
    .rows)
}

fn state() -> RelationalState {
    let mut state = RelationalState::default();
    for sql in [
        "CREATE TABLE money (id BIGINT PRIMARY KEY, amount DECIMAL(8,2), quantity BIGINT)",
        "CREATE INDEX money_amount ON money (amount)",
        "INSERT INTO money (id, amount, quantity) VALUES (1, 1.25, 2), (2, 2.50, 3), (3, NULL, 4)",
    ] {
        write(&mut state, sql, &[]).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    state
}

#[test]
fn decimal_query_predicates_preserve_unrounded_index_bounds() {
    let state = state();
    for sql in [
        "SELECT id FROM money WHERE amount > $1 ORDER BY id",
        "SELECT id FROM money WHERE amount + 0 > $1 ORDER BY id",
        "SELECT id FROM money WHERE $1 < amount ORDER BY id",
    ] {
        assert_eq!(
            query(&state, sql, &[decimal("1.249")]).unwrap(),
            vec![
                Row::from([("id".into(), Value::Int(1))]),
                Row::from([("id".into(), Value::Int(2))]),
            ],
            "{sql}"
        );
    }
    assert!(query(
        &state,
        "SELECT id FROM money WHERE amount = $1",
        &[decimal("1.251")]
    )
    .unwrap()
    .is_empty());
    assert_eq!(
        query(
            &state,
            "SELECT id FROM money WHERE id > 1.5 ORDER BY id",
            &[]
        )
        .unwrap()
        .len(),
        2
    );
    assert_eq!(
        query(
            &state,
            "SELECT id FROM money WHERE id > 0 AND id < 1.5 ORDER BY id",
            &[]
        )
        .unwrap(),
        vec![Row::from([("id".into(), Value::Int(1))])]
    );
    assert_eq!(
        query(
            &state,
            "SELECT id FROM money WHERE (amount * quantity) IN (2.5, 7.5) ORDER BY id",
            &[]
        )
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn decimal_query_projection_and_aggregate_use_exact_parameter_arithmetic() {
    let state = state();
    assert_eq!(
        query(
            &state,
            "SELECT amount * quantity + $1 AS total FROM money WHERE id = 1",
            &[decimal("0.000000000000000001")]
        )
        .unwrap(),
        vec![Row::from([(
            "total".into(),
            decimal("2.500000000000000001")
        )]),]
    );
    assert_eq!(
        query(
            &state,
            "SELECT SUM(amount * quantity + $1) / 2 AS total FROM money HAVING SUM(amount) > 3.749",
            &[decimal("0.25")]
        )
        .unwrap(),
        vec![Row::from([("total".into(), decimal("5.25"))]),]
    );
    assert_eq!(
        query(
            &state,
            "SELECT SUM(amount) AS total FROM money WHERE id < 0",
            &[]
        )
        .unwrap(),
        vec![Row::from([("total".into(), Value::Null)])]
    );
    assert_eq!(
        query(
            &state,
            "SELECT SUM(DISTINCT amount) AS total FROM money",
            &[]
        )
        .unwrap(),
        vec![Row::from([("total".into(), decimal("3.75"))])]
    );
    assert!(query(
        &state,
        "SELECT amount + $1 AS total FROM money",
        &[Value::Float(0.5)]
    )
    .is_err());
    assert_eq!(
        query(
            &state,
            "SELECT amount + CAST($1 AS DECIMAL(8,2)) AS total FROM money WHERE id = 1",
            &[Value::Float(0.5)]
        )
        .unwrap(),
        vec![Row::from([("total".into(), decimal("1.75"))])]
    );
}

#[test]
fn decimal_query_mutation_rounds_once_and_refuses_partial_overflow() {
    let mut state = state();
    write(
        &mut state,
        "UPDATE money SET amount = amount * quantity + $1, quantity = quantity + 1 WHERE id = 1",
        &[decimal("0.005")],
    )
    .unwrap();
    assert_eq!(
        query(
            &state,
            "SELECT amount, quantity FROM money WHERE id = 1",
            &[]
        )
        .unwrap(),
        vec![Row::from([
            ("amount".into(), decimal("2.51")),
            ("quantity".into(), Value::Int(3)),
        ])]
    );
    let before = query(
        &state,
        "SELECT id, amount, quantity FROM money ORDER BY id",
        &[],
    )
    .unwrap();
    assert!(write(
        &mut state,
        "UPDATE money SET amount = amount * 400000 WHERE id <= 2",
        &[]
    )
    .is_err());
    assert_eq!(
        query(
            &state,
            "SELECT id, amount, quantity FROM money ORDER BY id",
            &[]
        )
        .unwrap(),
        before
    );
    assert!(write(
        &mut state,
        "UPDATE money SET quantity = quantity / (id - 2) WHERE id <= 2",
        &[]
    )
    .is_err());
    assert_eq!(
        query(
            &state,
            "SELECT id, amount, quantity FROM money ORDER BY id",
            &[]
        )
        .unwrap(),
        before
    );
}

#[test]
fn decimal_join_keys_promote_integers_without_rounding_fractional_values() {
    let mut state = state();
    for sql in [
        "CREATE TABLE exact_keys (id DECIMAL(22,2) PRIMARY KEY)",
        "INSERT INTO exact_keys (id) VALUES (1.00), (1.25), (2.00), (9007199254740993.00)",
        "INSERT INTO money (id, amount, quantity) VALUES (9007199254740993, 0.01, 1)",
    ] {
        write(&mut state, sql, &[]).unwrap();
    }
    let expected = [1, 2, 9_007_199_254_740_993]
        .map(|value| Row::from([("id".into(), Value::Int(value))]))
        .to_vec();
    for sql in [
        "SELECT m.id FROM money m JOIN exact_keys d ON m.id = d.id ORDER BY m.id",
        "SELECT m.id FROM exact_keys d JOIN money m ON d.id = m.id ORDER BY m.id",
        "SELECT m.id FROM money m JOIN exact_keys d ON m.id + 0 = d.id ORDER BY m.id",
    ] {
        assert_eq!(query(&state, sql, &[]).unwrap(), expected, "{sql}");
    }
}
