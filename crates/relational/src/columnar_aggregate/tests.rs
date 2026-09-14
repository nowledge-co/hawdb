use super::*;
use skein_storage::RelationalColumnSchema;

mod fixtures;
use fixtures::*;

#[test]
fn fast_path_shape_gates_do_not_reserve_memory_on_fallback() {
    for projection in [
        "COUNT(*)",
        "COUNT(n)",
        "COUNT(records.n)",
        "COUNT(r.body)",
        "COUNT(flag)",
        "COUNT(ratio)",
        "SUM(n)",
        "SUM(OCTET_LENGTH(body))",
        "SUM(OCTET_LENGTH(payload))",
        "COALESCE(SUM(n), NULL, 0, 1)",
        "COALESCE(COUNT(n), 0)",
    ] {
        let ledger = ledger();
        let aggregate = executor(projection, 8, &ledger);
        assert!(ledger.snapshot().used_bytes > 0, "{projection}");
        drop(aggregate);
        assert_eq!(ledger.snapshot().used_bytes, 0, "{projection}");
    }
    for projection in [
        "*",
        "n",
        "42",
        "MAX(n)",
        "SUM(ratio)",
        "SUM(body)",
        "SUM(*)",
        "COUNT(DISTINCT n)",
        "SUM(DISTINCT n)",
        "COUNT(missing)",
        "SUM(other.n)",
        "SUM(OCTET_LENGTH(n))",
        "SUM(OCTET_LENGTH(missing))",
        "SUM(OCTET_LENGTH(*))",
        "COUNT(*) FILTER (WHERE n IS NOT NULL)",
        "SUM(n) FILTER (WHERE n IS NOT NULL)",
        "COALESCE(SUM(n), $1)",
        "COALESCE(SUM(n), COUNT(*))",
        "COALESCE(SUM(n), *)",
    ] {
        let ledger = ledger();
        assert!(
            ColumnarAggregateExecutor::try_new(
                &select(projection),
                &schema(),
                "records",
                "r",
                true,
                8,
                nz(1),
                &ledger,
            )
            .unwrap()
            .is_none(),
            "{projection}"
        );
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(ledger.snapshot().account_count, 0, "{projection}");
    }
    let mut empty = select("COUNT(*)");
    empty.projection.clear();
    for (select, no_joins) in [
        (empty, true),
        (select("COUNT(*)"), false),
        (
            parse("SELECT COUNT(*) FROM records HAVING COUNT(*) >= 0"),
            true,
        ),
    ] {
        let ledger = ledger();
        assert!(ColumnarAggregateExecutor::try_new(
            &select,
            &schema(),
            "records",
            "r",
            no_joins,
            8,
            nz(1),
            &ledger,
        )
        .unwrap()
        .is_none());
        assert_eq!(ledger.snapshot().account_count, 0);
    }
}

#[test]
fn expression_names_and_aliases_keep_the_original_output_contract() {
    let ledger = ledger();
    assert_eq!(
        executor(
            "COUNT(*), COUNT(n) AS named, COALESCE(SUM(n), 4)",
            1,
            &ledger
        )
        .finish()
        .unwrap(),
        vec![
            ("count".into(), Value::Int(0)),
            ("named".into(), Value::Int(0)),
            ("coalesce".into(), Value::Int(4))
        ],
    );
    for (source, expected) in [("n", "n"), ("1", "value"), ("SUM(n)", "sum")] {
        let select = select(source);
        let SelectProjection::Expression { expression, .. } = &select.projection[0] else {
            panic!("expression")
        };
        assert_eq!(expression_name(expression), expected);
    }
    let predicate = parse("SELECT n FROM records WHERE n IS NULL")
        .selection
        .unwrap();
    assert_eq!(expression_name(&predicate), "expression");
    // Duplicate names are rejected by the caller's output validation, not renamed here.
    assert_eq!(
        executor("COUNT(*), COUNT(n)", 1, &ledger)
            .finish()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn empty_null_and_metadata_inputs_match_the_scalar_reference() {
    let mut rng = Rng(17);
    let mut rows = vec![
        [
            RelationalValue::Null,
            RelationalValue::Null,
            RelationalValue::Null,
        ],
        [
            RelationalValue::BigInt(0),
            RelationalValue::Text(String::new()),
            RelationalValue::Bytea(vec![]),
        ],
        [
            RelationalValue::BigInt(-3),
            RelationalValue::Text("\u{e9}\u{1f980}".into()),
            overflow(RelationalScalarType::Bytea, 73),
        ],
    ];
    rows.extend(rng.rows(130));
    for input in [&rows[..0], &rows[..1], &rows[..2], &rows[..3], &rows[..]] {
        for batch in [0, 1, 2, 7, 63, 64, 65, 128, 256] {
            let ledger = ledger();
            let mut aggregate = executor(PROJECTION, batch, &ledger);
            let reserved = ledger.snapshot().used_bytes;
            let blocking = aggregate.blocking_state_bytes();
            for row in input {
                push_row(&mut aggregate, row).unwrap();
                assert_eq!(ledger.snapshot().used_bytes, reserved);
                assert_eq!(aggregate.blocking_state_bytes(), blocking);
            }
            assert_eq!(
                aggregate.finish().unwrap(),
                reference(input),
                "batch={batch}"
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn resolver_call_order_and_first_failure_are_preserved() {
    let ledger = ledger();
    let mut aggregate = executor(
        "COUNT(*), COUNT(r.n), SUM(n), SUM(OCTET_LENGTH(body))",
        8,
        &ledger,
    );
    let value = RelationalValue::BigInt(9);
    let mut calls = Vec::new();
    let error = aggregate
        .push(|column| {
            calls.push((column.qualifier.clone(), column.name.clone()));
            if column.name == "body" {
                Err(SkeinError::Execution("binding sentinel".into()))
            } else {
                Ok(&value)
            }
        })
        .unwrap_err();
    assert_eq!(
        calls,
        vec![
            (Some("r".into()), "n".into()),
            (None, "n".into()),
            (None, "body".into())
        ]
    );
    assert!(matches!(error, SkeinError::Execution(ref message) if message == "binding sentinel"));
    drop(aggregate);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let mut count = executor("COUNT(*)", 1, &ledger);
    count
        .push(|_| panic!("COUNT(*) must not bind columns"))
        .unwrap();
    assert_eq!(
        count.finish().unwrap(),
        vec![("count".into(), Value::Int(1))]
    );
}

#[test]
fn invalid_values_fail_with_existing_semantic_errors_and_release_the_lease() {
    for (projection, value, expected) in [
        (
            "SUM(n)",
            RelationalValue::Text("x".into()),
            "SUM requires BIGINT or DOUBLE PRECISION input",
        ),
        (
            "SUM(n)",
            RelationalValue::DoublePrecision(1.0),
            "SUM requires BIGINT or DOUBLE PRECISION input",
        ),
        (
            "SUM(OCTET_LENGTH(body))",
            RelationalValue::Boolean(true),
            "OCTET_LENGTH requires TEXT or BYTEA input",
        ),
    ] {
        let ledger = ledger();
        let mut aggregate = executor(projection, 1, &ledger);
        let error = aggregate.push(|_| Ok(&value)).unwrap_err();
        assert!(matches!(error, SkeinError::Semantic(ref message) if message == expected));
        drop(aggregate);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn sum_overflow_is_identical_inside_across_and_at_the_tail_of_batches() {
    for values in [[i64::MAX, 1], [i64::MIN, -1]] {
        for batch in [1, 2, 3] {
            let ledger = ledger();
            let mut aggregate = executor("SUM(n)", batch, &ledger);
            let outcome = values.iter().try_for_each(|value| {
                let value = RelationalValue::BigInt(*value);
                aggregate.push(|_| Ok(&value))
            });
            let error = match outcome {
                Err(error) => {
                    drop(aggregate);
                    error
                }
                Ok(()) => aggregate.finish().unwrap_err(),
            };
            assert!(
                matches!(error, SkeinError::Execution(ref message) if message == "BIGINT SUM overflow"),
                "batch={batch}: {error}"
            );
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn count_and_metadata_length_saturation_keep_the_existing_boundary() {
    let ledger = ledger();
    let mut aggregate = executor("COUNT(*)", 1, &ledger);
    aggregate.projections[0].accumulator = ColumnarAggregateAccumulator::Count(usize::MAX);
    aggregate.push(|_| panic!("COUNT(*)")).unwrap();
    assert_eq!(
        aggregate.finish().unwrap()[0].1,
        Value::Int(i64::try_from(usize::MAX).unwrap_or(i64::MAX))
    );
    let mut aggregate = executor("SUM(OCTET_LENGTH(body))", 1, &ledger);
    let reference = overflow(RelationalScalarType::Text, u64::MAX);
    aggregate.push(|_| Ok(&reference)).unwrap();
    assert_eq!(aggregate.finish().unwrap()[0].1, Value::Int(i64::MAX));
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn batch_admission_is_maximal_at_payload_and_validity_boundaries() {
    for requested in [0usize, 1, 2, 7, 63, 64, 65, 127, 128, 129] {
        for capacity in [1usize, 2, 7, 63, 64, 65, 129] {
            for limit in [
                reference_batch_bytes(capacity) - 1,
                reference_batch_bytes(capacity),
            ] {
                let ledger = ledger();
                let expected = (1..=requested.max(1))
                    .rev()
                    .find(|rows| reference_batch_bytes(*rows) <= limit);
                let actual = ColumnarAggregateExecutor::try_new(
                    &select(PROJECTION),
                    &schema(),
                    "records",
                    "r",
                    true,
                    requested,
                    nz(limit),
                    &ledger,
                );
                match expected {
                    Some(rows) => {
                        let aggregate = actual.unwrap().unwrap();
                        assert_eq!(aggregate.batch_rows, rows);
                        assert_eq!(ledger.snapshot().used_bytes, reference_batch_bytes(rows));
                        assert_eq!(
                            ledger.snapshot().classes[0].class,
                            QueryMemoryClass::PipelineBatch
                        );
                        drop(aggregate);
                    }
                    None => {
                        let error = actual.err().expect("one row must be refused");
                        assert!(
                            matches!(error, SkeinError::Execution(ref message) if message == &format!("relational columnar aggregate cannot fit one row within batch_payload_bytes {limit}"))
                        );
                        assert_eq!(ledger.snapshot().account_count, 0);
                    }
                }
                assert_eq!(ledger.snapshot().used_bytes, 0);
            }
        }
    }
}

#[test]
fn query_memory_admission_is_shared_and_released_on_drop() {
    let bytes = reference_batch_bytes(1);
    let ledger = QueryMemoryLedger::new(nz(bytes * 2 - 1));
    let first = executor(PROJECTION, 1, &ledger);
    let error = ColumnarAggregateExecutor::try_new(
        &select(PROJECTION),
        &schema(),
        "records",
        "r",
        true,
        1,
        nz(bytes),
        &ledger,
    )
    .err()
    .expect("second lease exceeds shared budget");
    assert!(matches!(error, SkeinError::Execution(_)));
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    drop(first);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    let second = executor(PROJECTION, 1, &ledger);
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    drop(second);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

fn differential_campaign(seeds: u64, cases_per_seed: usize) -> usize {
    let select = select(PROJECTION);
    let schema = schema();
    let counts = [0, 1, 2, 6, 7, 8, 63, 64, 65, 127, 129];
    let mut checks = 0;
    for seed in 0..seeds {
        let mut rng = Rng(seed + 1);
        for case in 0..cases_per_seed {
            let rows = rng.rows(counts[case % counts.len()]);
            let expected = reference(&rows);
            for (requested, admitted) in [(1, 1), (7, 7), (64, 64), (129, 65)] {
                let bytes = reference_batch_bytes(admitted);
                let ledger = QueryMemoryLedger::new(nz(bytes));
                let mut aggregate = ColumnarAggregateExecutor::try_new(
                    &select,
                    &schema,
                    "records",
                    "r",
                    true,
                    requested,
                    nz(bytes),
                    &ledger,
                )
                .unwrap()
                .unwrap();
                assert_eq!(aggregate.batch_rows, admitted, "seed={seed} case={case}");
                for row in &rows {
                    push_row(&mut aggregate, row).unwrap();
                    assert_eq!(ledger.snapshot().used_bytes, bytes);
                }
                assert_eq!(
                    aggregate.finish().unwrap(),
                    expected,
                    "seed={seed} case={case} batch={admitted}"
                );
                let snapshot = ledger.snapshot();
                assert_eq!(snapshot.used_bytes, 0);
                assert_eq!(snapshot.peak_bytes, bytes);
                checks += 1;
            }
        }
    }
    checks
}

#[test]
fn columnar_aggregate_differential_smoke() {
    assert_eq!(differential_campaign(2, 11), 88);
}

#[test]
#[ignore = "explicit deterministic campaign"]
fn columnar_aggregate_differential_campaign() {
    let checks = differential_campaign(128, 64);
    assert_eq!(checks, 32_768);
    println!("skein-relational-columnar-aggregate-fuzz-v1: 128 seeds, 8192 cases, {checks} complete outcomes");
}

#[test]
fn recognizes_coalesced_sum_octet_length_as_columnar_aggregate() {
    let skein_sql::SqlStatement::Select(select) = skein_sql::parse_postgres_sql(
        "SELECT COALESCE(SUM(OCTET_LENGTH(body)), 0) AS body_bytes FROM documents",
    )
    .expect("parse length aggregate") else {
        panic!("expected SELECT statement")
    };
    let schema = RelationalTableSchema {
        name: "documents".to_string(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "body".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".to_string()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    let ledger =
        QueryMemoryLedger::new(NonZeroUsize::new(64 * 1024).expect("non-zero query memory"));
    let executor = ColumnarAggregateExecutor::try_new(
        &select,
        &schema,
        "documents",
        "documents",
        true,
        64,
        NonZeroUsize::new(16 * 1024).expect("non-zero batch bytes"),
        &ledger,
    )
    .expect("plan columnar aggregate")
    .expect("length aggregate must use the columnar path");

    assert!(matches!(
        executor.projections[0].kind,
        ColumnarAggregateKind::SumOctetLength(_)
    ));
    assert_eq!(executor.projections[0].null_fallback, Some(Value::Int(0)));
}
