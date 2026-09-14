use super::state::{AggregateExpressionState, NumericAggregate};
use super::*;
use std::cell::RefCell;
use std::num::NonZeroUsize;

mod fixtures;
use fixtures::*;

#[test]
fn ordinary_states_preserve_empty_null_distinct_and_coalesce_results() {
    let state = state();
    let mut rng = Rng(19);
    let mut rows = vec![null_row(), null_row()];
    rows.extend(rng.rows(64));
    let select = select(&format!("SELECT {PROJECTIONS} FROM records"));
    for rows in [&rows[..0], &rows[..1], &rows[..2], &rows[..]] {
        assert_eq!(
            execute(&select, &[], &state, rows).unwrap(),
            reference(rows, false, None, 0)
        );
    }
}

#[test]
fn distinct_memory_is_charged_once_and_max_replacements_release_bytes() {
    for function in ["COUNT(DISTINCT n)", "SUM(DISTINCT n)"] {
        let mut state = AggregateExpressionState::new(&expr(function), &[]).unwrap();
        let null = RelationalValue::Null;
        let delta = state
            .update(&|_| Ok((&null, RelationalScalarType::BigInt)), &[])
            .unwrap();
        assert_eq!((delta.added_bytes, delta.released_bytes), (0, 0));
        let n = RelationalValue::BigInt(7);
        let delta = state
            .update(&|_| Ok((&n, RelationalScalarType::BigInt)), &[])
            .unwrap();
        let node = value_bytes(&n) + 4 * std::mem::size_of::<usize>();
        assert_eq!(
            delta.added_bytes,
            node + if function.starts_with("SUM") {
                value_bytes(&n)
            } else {
                0
            }
        );
        let duplicate = state
            .update(&|_| Ok((&n, RelationalScalarType::BigInt)), &[])
            .unwrap();
        assert_eq!((duplicate.added_bytes, duplicate.released_bytes), (0, 0));
        assert_eq!(
            state.finish().unwrap(),
            Value::Int(if function.starts_with("SUM") { 7 } else { 1 })
        );
    }
    let mut state = AggregateExpressionState::new(&expr("MAX(body)"), &[]).unwrap();
    let long = RelationalValue::Text("a".repeat(100));
    let short = RelationalValue::Text("z".into());
    let first = state
        .update(&|_| Ok((&long, RelationalScalarType::Text)), &[])
        .unwrap();
    assert_eq!(
        (first.added_bytes, first.released_bytes),
        (value_bytes(&long), 0)
    );
    let second = state
        .update(&|_| Ok((&short, RelationalScalarType::Text)), &[])
        .unwrap();
    assert_eq!((second.added_bytes, second.released_bytes), (0, 99));
    assert_eq!(state.finish().unwrap(), Value::String("z".into()));
}

#[test]
fn first_values_and_coalesce_keep_update_and_finish_order() {
    let mut first = AggregateExpressionState::new(&expr("body"), &[]).unwrap();
    let null = RelationalValue::Null;
    first
        .update(&|_| Ok((&null, RelationalScalarType::Text)), &[])
        .unwrap();
    first
        .update(&|_| panic!("first NULL is still captured"), &[])
        .unwrap();
    assert_eq!(first.finish().unwrap(), Value::Null);
    let mut coalesce = AggregateExpressionState::new(&expr("COALESCE(9, SUM(n))"), &[]).unwrap();
    let calls = RefCell::new(0);
    let value = RelationalValue::BigInt(4);
    let delta = coalesce
        .update(
            &|_| {
                *calls.borrow_mut() += 1;
                Ok((&value, RelationalScalarType::BigInt))
            },
            &[],
        )
        .unwrap();
    assert_eq!(*calls.borrow(), 1);
    assert_eq!(delta.added_bytes, value_bytes(&value));
    assert_eq!(coalesce.finish().unwrap(), Value::Int(9));
    let error = AggregateExpressionState::new(&expr("COALESCE(body, 9)"), &[])
        .unwrap()
        .finish()
        .unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "aggregate column has no input row")
    );
}

#[test]
fn filters_short_circuit_row_access_and_preserve_parameter_failures() {
    let mut state =
        AggregateExpressionState::new(&expr("SUM(n) FILTER (WHERE flag = $1)"), &[]).unwrap();
    let flag = RelationalValue::Boolean(false);
    let calls = RefCell::new(Vec::new());
    let delta = state
        .update(
            &|column| {
                calls.borrow_mut().push(column.name.clone());
                if column.name == "flag" {
                    Ok((&flag, RelationalScalarType::Boolean))
                } else {
                    panic!("filtered numeric value must not be read")
                }
            },
            &[Value::Bool(true)],
        )
        .unwrap();
    assert_eq!(*calls.borrow(), vec!["flag"]);
    assert_eq!((delta.added_bytes, delta.released_bytes), (0, 0));
    let error = state
        .update(&|_| Ok((&flag, RelationalScalarType::Boolean)), &[])
        .map(|_| ())
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("missing PostgreSQL parameter $1"));
    let error = state
        .update(
            &|_| Err(SkeinError::Execution("row sentinel".into())),
            &[Value::Bool(true)],
        )
        .map(|_| ())
        .unwrap_err();
    assert!(matches!(error, SkeinError::Execution(ref message) if message == "row sentinel"));
}

#[test]
fn numeric_overflow_and_type_errors_keep_the_prior_state() {
    for (first, next) in [(i64::MAX, 1), (i64::MIN, -1)] {
        let mut result = Some(RelationalValue::BigInt(first));
        let error = state::update_numeric_aggregate(
            &mut result,
            RelationalValue::BigInt(next),
            NumericAggregate::Sum,
        )
        .map(|_| ())
        .unwrap_err();
        assert!(
            matches!(error, SkeinError::Execution(ref message) if message == "BIGINT SUM overflow")
        );
        assert_eq!(result, Some(RelationalValue::BigInt(first)));
    }
    let mut result = Some(RelationalValue::BigInt(2));
    let error = state::update_numeric_aggregate(
        &mut result,
        RelationalValue::DoublePrecision(1.0),
        NumericAggregate::Sum,
    )
    .map(|_| ())
    .unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "SUM requires BIGINT or DOUBLE PRECISION input")
    );
    assert_eq!(result, Some(RelationalValue::BigInt(2)));
    let mut result = Some(RelationalValue::DoublePrecision(1.5));
    state::update_numeric_aggregate(
        &mut result,
        RelationalValue::DoublePrecision(2.5),
        NumericAggregate::Sum,
    )
    .unwrap();
    assert_eq!(result, Some(RelationalValue::DoublePrecision(4.0)));
}

#[test]
fn constructor_rejections_keep_exact_aggregate_errors() {
    let column = SqlFunctionArgument::Expression(expr("n"));
    let literal = SqlFunctionArgument::Expression(expr("1"));
    for (expression, message) in [
        (
            function("count", vec![], false),
            "COUNT requires exactly one argument",
        ),
        (
            function("count", vec![literal], false),
            "COUNT supports wildcard or a column argument",
        ),
        (
            function("count", vec![SqlFunctionArgument::Wildcard], true),
            "COUNT(DISTINCT *) is not supported",
        ),
        (
            function("sum", vec![SqlFunctionArgument::Wildcard], false),
            "numeric aggregate requires exactly one expression",
        ),
        (
            function("coalesce", vec![column.clone()], true),
            "COALESCE does not accept DISTINCT",
        ),
        (
            function("coalesce", vec![SqlFunctionArgument::Wildcard], false),
            "COALESCE does not accept wildcard",
        ),
        (
            function("unknown", vec![column], false),
            "unsupported relational aggregate function unknown",
        ),
    ] {
        let error = AggregateExpressionState::new(&expression, &[])
            .err()
            .expect("invalid shape");
        assert!(
            matches!(error, SkeinError::Semantic(ref actual) if actual == message),
            "{error}"
        );
    }
    let error =
        AggregateExpressionState::new(&Expr::value(SqlValue::Parameter(2)), &[Value::Int(1)])
            .err()
            .unwrap();
    assert!(error
        .to_string()
        .contains("missing PostgreSQL parameter $2"));
}

#[test]
fn row_expressions_keep_metadata_and_parameter_boundaries() {
    let value = overflow(u64::MAX);
    assert_eq!(
        evaluate_row_expression(&expr("OCTET_LENGTH(payload)"), &|_| Ok((
            &value,
            RelationalScalarType::Bytea
        )))
        .unwrap(),
        RelationalValue::BigInt(i64::MAX)
    );
    let error = evaluate_row_expression(&Expr::value(SqlValue::Parameter(1)), &|_| {
        panic!("parameter is not a column")
    })
    .unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "aggregate row expression cannot bind parameter $1")
    );
    let value = RelationalValue::Boolean(true);
    let error = evaluate_row_expression(&expr("OCTET_LENGTH(payload)"), &|_| {
        Ok((&value, RelationalScalarType::Bytea))
    })
    .unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "OCTET_LENGTH requires TEXT or BYTEA input")
    );
}

#[test]
fn having_unknown_is_rejected_and_hidden_state_cannot_be_projected() {
    let state = state();
    for condition in [
        "SUM(n) > 0",
        "NOT (SUM(n) = 0)",
        "SUM(n) IN (1, NULL)",
        "SUM(n) NOT IN (1, NULL)",
    ] {
        let query = select(&format!("SELECT COUNT(*) FROM records HAVING {condition}"));
        assert!(
            execute(&query, &[], &state, &[]).unwrap().is_empty(),
            "{condition}"
        );
    }
    let query = select("SELECT COUNT(*) FROM records HAVING SUM(n) IS NULL");
    assert_eq!(
        execute(&query, &[], &state, &[]).unwrap()[0].1,
        vec![("count".into(), Value::Int(0))]
    );
    let having = having::HavingState::new(&query, &[], &state)
        .unwrap()
        .unwrap();
    let error = AggregateExpressionState::Having(Box::new(having))
        .finish()
        .unwrap_err();
    assert!(
        matches!(error, SkeinError::Execution(ref message) if message == "HAVING state reached output projection")
    );
}

#[test]
fn having_group_binding_requires_all_primary_key_columns_and_resolves_aliases() {
    let state = state();
    for sql in [
        "SELECT body FROM records GROUP BY id HAVING COUNT(*) >= 0",
        "SELECT body FROM composite GROUP BY id, bucket HAVING COUNT(*) >= 0",
        "SELECT r.n FROM records r GROUP BY r.id HAVING COUNT(*) >= 0",
        "SELECT r.id FROM records r JOIN peers p ON r.id = p.id GROUP BY r.id HAVING COUNT(p.n) >= 0",
    ] {
        validate_having(&select(sql), &[], &state).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    for (sql, message) in [
        (
            "SELECT body FROM records GROUP BY bucket HAVING COUNT(*) >= 0",
            "column body must appear in GROUP BY or an aggregate",
        ),
        (
            "SELECT body FROM composite GROUP BY id HAVING COUNT(*) >= 0",
            "column body must appear in GROUP BY or an aggregate",
        ),
        (
            "SELECT records.n FROM records r GROUP BY r.id HAVING COUNT(*) >= 0",
            "unknown HAVING/grouped column n",
        ),
        (
            "SELECT COUNT(*) FROM records r JOIN peers p ON r.id = p.id HAVING MAX(n) > 0",
            "ambiguous HAVING/grouped column n",
        ),
        (
            "SELECT COUNT(*) FROM records HAVING MAX(missing) > 0",
            "unknown HAVING/grouped column missing",
        ),
    ] {
        let error = validate_having(&select(sql), &[], &state).unwrap_err();
        assert!(
            matches!(error, SkeinError::Semantic(ref actual) if actual == message),
            "{sql}: {error}"
        );
    }
}

#[test]
fn having_coercion_and_bound_parameters_remain_consistent() {
    let state = state();
    let mut row = null_row();
    row[2] = RelationalValue::BigInt(3);
    row[3] = RelationalValue::DoublePrecision(3.0);
    row[4] = RelationalValue::Text("zebra".into());
    for (sql, parameters) in [
        ("SELECT SUM(n) FROM records HAVING SUM(n) = 3.0", vec![]),
        (
            "SELECT MAX(body) FROM records HAVING MAX(body) ILIKE 'Z%' ESCAPE '!'",
            vec![],
        ),
        (
            "SELECT SUM($1) FROM records HAVING SUM($1) >= $2",
            vec![Value::Int(3), Value::Int(2)],
        ),
    ] {
        assert_eq!(
            execute(&select(sql), &parameters, &state, &[row.clone()])
                .unwrap()
                .len(),
            1,
            "{sql}"
        );
    }
    for sql in [
        "SELECT COUNT(*) FROM records HAVING SUM(n) = MAX(body)",
        "SELECT COUNT(*) FROM records HAVING SUM(body) > 0",
        "SELECT COUNT(*) FROM records HAVING SUM(SUM(n)) > 0",
        "SELECT COUNT(*) FROM records HAVING COALESCE(MAX(body), SUM(n)) IS NULL",
    ] {
        assert!(validate_having(&select(sql), &[], &state).is_err(), "{sql}");
    }
    let error = validate_having(
        &select("SELECT COUNT(*) FROM records HAVING COUNT(*) > $1"),
        &[],
        &state,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("missing PostgreSQL parameter $1"));
}

#[test]
fn state_memory_limits_fail_before_charging_and_release_through_the_ledger() {
    let budget = NonZeroUsize::new(64).unwrap();
    let ledger = skein_executor::QueryMemoryLedger::new(budget);
    let mut tracker = OperatorMemoryTracker::with_account(
        budget,
        ledger.account(
            skein_executor::QueryMemoryClass::BlockingState,
            "aggregate",
            budget,
        ),
    );
    let error = charge_aggregate_memory(65, &mut tracker).unwrap_err();
    assert!(error.to_string().contains("item uses 65 bytes"));
    assert_eq!((tracker.used_bytes, ledger.snapshot().used_bytes), (0, 0));
    charge_aggregate_memory(40, &mut tracker).unwrap();
    let error = charge_aggregate_memory(25, &mut tracker).unwrap_err();
    assert!(error
        .to_string()
        .contains("RelationalAggregateExec state exceeds blocking_operator_bytes 64"));
    assert_eq!((tracker.used_bytes, ledger.snapshot().used_bytes), (40, 40));
    tracker.release(20);
    charge_aggregate_memory(44, &mut tracker).unwrap();
    assert_eq!((tracker.used_bytes, ledger.snapshot().used_bytes), (64, 64));
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn having_slot_output_memory_releases_replaced_values_twice() {
    let state = state();
    let query = select("SELECT COUNT(*) FROM records HAVING MAX(body) IS NOT NULL");
    let mut projections = projection_template(&query, &[], &state).unwrap();
    let long = RelationalValue::Text("a".repeat(100));
    let short = RelationalValue::Text("z".into());
    let having = projections.last_mut().unwrap();
    let first = having
        .update(&|_| Ok((&long, RelationalScalarType::Text)), &[])
        .unwrap();
    let output_before = std::mem::size_of::<Value>();
    let owned = value_bytes(&long);
    assert_eq!(
        first.added_bytes,
        owned + owned.saturating_sub(output_before)
    );
    assert_eq!(first.released_bytes, output_before.saturating_sub(owned));
    let second = having
        .update(&|_| Ok((&short, RelationalScalarType::Text)), &[])
        .unwrap();
    assert_eq!((second.added_bytes, second.released_bytes), (0, 198));
    assert_eq!(filter_group(projections).unwrap().unwrap().len(), 1);
}

#[test]
fn having_boolean_and_uuid_constants_keep_binding_coercion() {
    let state = state();
    for value in [Value::Bool(true), Value::Bool(false), Value::Null] {
        let mut query = select("SELECT COUNT(*) FROM records");
        query.having = Some(Expr::value(SqlValue::Parameter(1)));
        let output = execute(&query, std::slice::from_ref(&value), &state, &[]).unwrap();
        assert_eq!(!output.is_empty(), value == Value::Bool(true));
    }
    let uuid = "12345678-1234-1234-1234-123456789abc";
    let value = RelationalValue::Uuid(skein_core::Uuid::parse_str(uuid).unwrap());
    let query = select("SELECT COUNT(*) FROM ids HAVING MAX(id) = $1");
    let parameters = [Value::String(uuid.into())];
    validate_having(&query, &parameters, &state).unwrap();
    let mut projections = projection_template(&query, &parameters, &state).unwrap();
    for projection in &mut projections {
        projection
            .update(&|_| Ok((&value, RelationalScalarType::Uuid)), &parameters)
            .unwrap();
    }
    assert_eq!(
        filter_group(projections)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(AggregateProjectionState::finish)
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        vec![("count".into(), Value::Int(1))]
    );
    let error = validate_having(&query, &[Value::String("invalid".into())], &state).unwrap_err();
    assert!(error.to_string().contains("invalid UUID"));
}

#[test]
fn having_validation_preserves_lock_and_projection_error_precedence() {
    let state = state();
    let mut query = select("SELECT COUNT(*) FROM missing HAVING COUNT(*) > 0");
    query.lock_strength = Some(skein_sql::SqlLockStrength::Share);
    let error = validate_having(&query, &[], &state).unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "HAVING does not support row locking")
    );
    query.having = None;
    validate_having(&query, &[], &state).unwrap();
    let query = select("SELECT * FROM records HAVING COUNT(*) >= 0");
    let error = validate_having(&query, &[], &state).unwrap_err();
    assert!(
        matches!(error, SkeinError::Semantic(ref message) if message == "aggregate SELECT does not support wildcard projection")
    );
}

#[test]
fn memory_deltas_saturate_and_shared_query_admission_stays_atomic() {
    let mut delta = AggregateMemoryDelta {
        added_bytes: usize::MAX,
        released_bytes: usize::MAX,
    };
    delta.combine(AggregateMemoryDelta {
        added_bytes: 1,
        released_bytes: 1,
    });
    assert_eq!(
        (delta.added_bytes, delta.released_bytes),
        (usize::MAX, usize::MAX)
    );
    let delta = AggregateMemoryDelta::between(usize::MAX, 0);
    assert_eq!((delta.added_bytes, delta.released_bytes), (0, usize::MAX));
    let ledger = skein_executor::QueryMemoryLedger::new(NonZeroUsize::new(40).unwrap());
    let budget = NonZeroUsize::new(64).unwrap();
    let mut tracker = OperatorMemoryTracker::with_account(
        budget,
        ledger.account(
            skein_executor::QueryMemoryClass::BlockingState,
            "aggregate shared",
            budget,
        ),
    );
    charge_aggregate_memory(40, &mut tracker).unwrap();
    assert!(charge_aggregate_memory(1, &mut tracker).is_err());
    assert_eq!((tracker.used_bytes, ledger.snapshot().used_bytes), (40, 40));
    drop(tracker);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

fn differential_campaign(seeds: u64, cases: usize) -> usize {
    let state = state();
    let mut checks = 0;
    for seed in 0..seeds {
        let mut rng = Rng(seed + 1);
        for case in 0..cases {
            let rows = rng.rows([0, 1, 2, 3, 8, 31][case % 6]);
            let condition = case % HAVING.len();
            let threshold = (rng.next() % 7) as i64 - 2;
            for grouped in [false, true] {
                for having in [false, true] {
                    let projection = if grouped {
                        format!("bucket, {PROJECTIONS}")
                    } else {
                        PROJECTIONS.into()
                    };
                    let group = if grouped { " GROUP BY bucket" } else { "" };
                    let having_sql = if having {
                        format!(" HAVING {}", HAVING[condition])
                    } else {
                        String::new()
                    };
                    let query = select(&format!(
                        "SELECT {projection} FROM records{group}{having_sql}"
                    ));
                    let actual = execute(&query, &[Value::Int(threshold)], &state, &rows).unwrap();
                    let expected =
                        reference(&rows, grouped, having.then_some(condition), threshold);
                    assert_eq!(
                        actual, expected,
                        "seed={seed} case={case} grouped={grouped} having={having}"
                    );
                    checks += 1;
                }
            }
        }
    }
    checks
}

#[test]
fn aggregate_state_differential_smoke() {
    assert_eq!(differential_campaign(2, 20), 160);
}

#[test]
#[ignore = "explicit deterministic aggregate and HAVING campaign"]
fn aggregate_state_differential_campaign() {
    let checks = differential_campaign(128, 64);
    assert_eq!(checks, 32_768);
    println!("skein-relational-aggregate-state-fuzz-v1: 128 seeds, 8192 cases, {checks} complete outcomes");
}
