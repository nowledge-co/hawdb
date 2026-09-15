use super::*;
use crate::query_value::*;
use crate::row_runtime::RelationalReadRowRef;
use skein_sql::{SqlBound, SqlStatement, SqlValue};
use skein_storage::{
    RelationalColumnSchema, RelationalKey, RelationalOverflowRef, RelationalProjectedField,
    RelationalProjectedRow, RelationalTableSchema,
};
use std::cell::RefCell;

fn predicate(source: &str) -> SqlPredicate {
    let prepared =
        skein_sql::prepare_postgres_sql(&format!("SELECT x FROM records AS r WHERE {source}"))
            .unwrap_or_else(|error| panic!("{source}: {error}"));
    let SqlStatement::Select(select) = prepared.statement else {
        panic!("expected SELECT");
    };
    select.selection.expect("predicate")
}

fn schema() -> RelationalTableSchema {
    RelationalTableSchema {
        name: "records".into(),
        columns: [
            ("x", RelationalScalarType::BigInt),
            ("y", RelationalScalarType::BigInt),
            ("flag", RelationalScalarType::Boolean),
            ("body", RelationalScalarType::Text),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: true,
            default: None,
        })
        .collect(),
        primary_key: vec!["x".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    }
}

fn ordinary(
    predicate: &SqlPredicate,
    parameters: &[Value],
    values: &[RelationalValue],
) -> Result<Option<bool>> {
    let schema = schema();
    predicate_truth_with(predicate, parameters, &|column| {
        let ordinal = schema.column_position(&column.name).unwrap();
        Ok((&values[ordinal], schema.columns[ordinal].scalar_type))
    })
}

fn streaming(
    predicate: &SqlPredicate,
    parameters: &[Value],
    values: &[RelationalValue],
) -> Result<Option<bool>> {
    let bound = BoundStreamingPredicate::bind(predicate, parameters, &schema(), "records", "r")?;
    bound.truth_with(&|ordinal| Ok(values[ordinal].as_ref()))
}

fn unknown_bool(value: Option<bool>) -> RelationalValue {
    value.map_or(RelationalValue::Null, RelationalValue::Boolean)
}

fn and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

#[test]
fn three_valued_truth_tables_and_resolution_order_are_preserved() {
    for left in [None, Some(false), Some(true)] {
        for right in [None, Some(false), Some(true)] {
            let values = [unknown_bool(left), unknown_bool(right)];
            for (sql, expected, short_circuit) in [
                (
                    "a = TRUE AND b = TRUE",
                    and(left, right),
                    left == Some(false),
                ),
                ("a = TRUE OR b = TRUE", or(left, right), left == Some(true)),
                (
                    "NOT (a = TRUE AND b = TRUE)",
                    and(left, right).map(|v| !v),
                    left == Some(false),
                ),
            ] {
                let visits = RefCell::new(Vec::new());
                let result = predicate_truth_with(&predicate(sql), &[], &|column| {
                    visits.borrow_mut().push(column.name.clone());
                    let ordinal = usize::from(column.name == "b");
                    Ok((&values[ordinal], RelationalScalarType::Boolean))
                });
                assert_eq!(result.unwrap(), expected, "{sql}, {left:?}, {right:?}");
                let expected_visits = if short_circuit {
                    vec!["a"]
                } else {
                    vec!["a", "b"]
                };
                assert_eq!(*visits.borrow(), expected_visits, "{sql}");
            }
        }
    }
}

#[test]
fn short_circuit_preserves_resolver_errors_and_in_list_evaluation_order() {
    let value = RelationalValue::BigInt(7);
    let missing = SkeinError::Execution("column was not hydrated".into());
    for (sql, expected) in [
        ("x = 0 AND y = 1", Ok(Some(false))),
        ("x = 7 OR y = 1", Ok(Some(true))),
        ("x = 7 AND y = 1", Err(missing.clone())),
        (
            "x IN (7, $1)",
            Err(SkeinError::Semantic(
                "missing PostgreSQL parameter $1".into(),
            )),
        ),
    ] {
        let actual = predicate_truth_with(&predicate(sql), &[], &|column| {
            if column.name == "x" {
                Ok((&value, RelationalScalarType::BigInt))
            } else {
                Err(missing.clone())
            }
        });
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn comparison_preserves_scalar_ordering_and_overflow_before_null() {
    use std::cmp::Ordering;
    let pairs = [
        (
            RelationalValue::Boolean(false),
            RelationalValue::Boolean(true),
            Ordering::Less,
        ),
        (
            RelationalValue::BigInt(i64::MIN),
            RelationalValue::BigInt(i64::MAX),
            Ordering::Less,
        ),
        (
            RelationalValue::DoublePrecision(-0.0),
            RelationalValue::DoublePrecision(0.0),
            Ordering::Less,
        ),
        (
            RelationalValue::DoublePrecision(f64::NAN),
            RelationalValue::DoublePrecision(f64::INFINITY),
            Ordering::Greater,
        ),
        (
            RelationalValue::Text("alpha".into()),
            RelationalValue::Text("beta".into()),
            Ordering::Less,
        ),
        (
            RelationalValue::Bytea(vec![0, 128]),
            RelationalValue::Bytea(vec![0, 255]),
            Ordering::Less,
        ),
        (
            RelationalValue::Uuid(skein_core::Uuid::nil()),
            RelationalValue::Uuid(skein_core::Uuid::nil()),
            Ordering::Equal,
        ),
    ];
    for (left, right, ordering) in pairs {
        for (op, expected) in [
            (SqlComparisonOp::Eq, ordering.is_eq()),
            (SqlComparisonOp::NotEq, !ordering.is_eq()),
            (SqlComparisonOp::Lt, ordering.is_lt()),
            (SqlComparisonOp::Lte, !ordering.is_gt()),
            (SqlComparisonOp::Gt, ordering.is_gt()),
            (SqlComparisonOp::Gte, !ordering.is_lt()),
        ] {
            assert_eq!(
                compare_value_refs(left.as_ref(), right.as_ref(), op),
                Ok(Some(expected))
            );
            assert_eq!(compare_values(&left, &right, op), Ok(Some(expected)));
            assert_eq!(compare_values(&left, &RelationalValue::Null, op), Ok(None));
        }
    }
    let overflow = overflow();
    for (left, right) in [
        (&overflow, &RelationalValue::Null),
        (&RelationalValue::Null, &overflow),
        (&overflow, &RelationalValue::Text("body".into())),
    ] {
        assert_eq!(
            compare_values(left, right, SqlComparisonOp::Eq),
            Err(SkeinError::Execution(
                "relational filter or join requires overflow hydration before qualification".into()
            ))
        );
    }
    assert_eq!(
        compare_values(
            &RelationalValue::BigInt(1),
            &RelationalValue::Text("1".into()),
            SqlComparisonOp::Eq
        ),
        Err(SkeinError::Semantic(
            "relational comparison has incompatible scalar types".into()
        ))
    );
}

fn overflow() -> RelationalValue {
    RelationalValue::Overflow(RelationalOverflowRef {
        digest: "00".repeat(32).parse().unwrap(),
        scalar_type: RelationalScalarType::Text,
        compressed_bytes: 20,
        uncompressed_bytes: 4096,
    })
}

#[test]
fn scalar_binding_round_trips_and_keeps_exact_errors() {
    for value in [
        Value::Null,
        Value::Bool(true),
        Value::Int(i64::MAX),
        Value::Float(-0.0),
        Value::Float(f64::NAN),
        Value::String("naive \u{e9}".into()),
        Value::Binary(vec![0, 128, 255]),
        Value::Uuid(skein_core::Uuid::nil()),
    ] {
        let relational = value_to_relational(value.clone()).unwrap();
        assert_eq!(relational_to_value(&relational), Ok(value.clone()));
        assert_eq!(
            relational_ref_to_value(relational.as_ref()),
            Ok(value.clone())
        );
        assert_eq!(
            bind_sql_value(&SqlValue::Parameter(1), std::slice::from_ref(&value)),
            Ok(value.clone())
        );
        assert_eq!(
            bind_sql_value(&SqlValue::Literal(value.clone()), &[]),
            Ok(value)
        );
    }
    for value in [Value::List(vec![]), Value::Map(Default::default())] {
        assert_eq!(
            value_to_relational(value),
            Err(SkeinError::Semantic(
                "relational SQL values must be scalar".into()
            ))
        );
    }
    for position in [1, usize::MAX] {
        assert_eq!(
            bind_sql_value(&SqlValue::Parameter(position), &[]),
            Err(SkeinError::Semantic(format!(
                "missing PostgreSQL parameter ${position}"
            )))
        );
    }
    assert_eq!(
        bind_sql_value(&SqlValue::Parameter(0), &[Value::Int(9)]),
        Ok(Value::Int(9))
    );
    let overflow = overflow();
    let expected = Err(SkeinError::Execution(
        "overflow value reached projection without hydration".into(),
    ));
    assert_eq!(relational_to_value(&overflow), expected);
    assert_eq!(relational_ref_to_value(overflow.as_ref()), expected);
}

#[test]
fn bounds_and_uuid_conversion_keep_their_existing_contracts() {
    assert_eq!(bind_bound(None, &[], "LIMIT"), Ok(None));
    assert_eq!(
        bind_bound(Some(SqlBound::Literal(u64::MAX)), &[], "LIMIT"),
        Ok(Some(u64::MAX))
    );
    for name in ["LIMIT", "OFFSET"] {
        for value in [
            Value::Int(-1),
            Value::Null,
            Value::Float(1.0),
            Value::String("1".into()),
        ] {
            assert_eq!(
                bind_bound(Some(SqlBound::Parameter(1)), &[value], name),
                Err(SkeinError::Semantic(format!(
                    "PostgreSQL {name} parameter $1 must be a non-negative integer"
                )))
            );
        }
        for value in [0, i64::MAX] {
            assert_eq!(
                bind_bound(Some(SqlBound::Parameter(1)), &[Value::Int(value)], name),
                Ok(Some(value as u64))
            );
        }
        assert_eq!(
            bind_bound(Some(SqlBound::Parameter(2)), &[], name),
            Err(SkeinError::Semantic(
                "missing PostgreSQL parameter $2".into()
            ))
        );
    }
    let uuid = skein_core::Uuid::nil();
    assert_eq!(
        value_to_relational_as(Value::String(uuid.to_string()), RelationalScalarType::Uuid),
        Ok(RelationalValue::Uuid(uuid))
    );
    assert_eq!(
        value_to_relational_as(Value::String("bad".into()), RelationalScalarType::Uuid),
        Err(SkeinError::Semantic("invalid UUID value \"bad\"".into()))
    );
}

#[test]
fn like_handles_unicode_escapes_nulls_and_unhydrated_values() {
    for (source, text, expected) in [
        ("body LIKE 'a!_%' ESCAPE '!'", "a_tail", true),
        ("body LIKE 'a!_%' ESCAPE '!'", "abtail", false),
        ("body NOT LIKE 'a!_%' ESCAPE '!'", "abtail", true),
        ("body ILIKE 'ab%'", "ABcd", true),
        ("body LIKE '_x'", "\u{e9}x", true),
        ("body LIKE 'a_%' ESCAPE ''", "abc", true),
    ] {
        let values = [
            RelationalValue::Null,
            RelationalValue::Null,
            RelationalValue::Null,
            RelationalValue::Text(text.into()),
        ];
        let predicate = predicate(source);
        assert_eq!(
            ordinary(&predicate, &[], &values),
            Ok(Some(expected)),
            "{source}"
        );
        assert_eq!(
            streaming(&predicate, &[], &values),
            Ok(Some(expected)),
            "{source}"
        );
    }
    let expression = predicate("body LIKE $1");
    for (body, pattern, expected) in [
        (RelationalValue::Null, Value::String("%".into()), Ok(None)),
        (overflow(), Value::Null, Ok(None)),
        (
            overflow(),
            Value::String("%".into()),
            Err(SkeinError::Execution(
                "LIKE reached an overflow value without hydration".into(),
            )),
        ),
    ] {
        let values = [
            RelationalValue::Null,
            RelationalValue::Null,
            RelationalValue::Null,
            body,
        ];
        assert_eq!(
            ordinary(&expression, std::slice::from_ref(&pattern), &values),
            expected
        );
        assert_eq!(streaming(&expression, &[pattern], &values), expected);
    }
}

#[test]
fn streaming_binding_keeps_eager_validation_and_qualifier_errors() {
    for (source, parameters, message) in [
        ("x = $1", vec![], "missing PostgreSQL parameter $1"),
        (
            "x = $1",
            vec![Value::String("bad".into())],
            "relational comparison on x has an incompatible scalar type",
        ),
        (
            "x = body",
            vec![],
            "relational comparison between x and body has incompatible scalar types",
        ),
        ("x LIKE '%'", vec![], "LIKE and ILIKE require a TEXT column"),
        ("other.x = 1", vec![], "column x is unknown or ambiguous"),
        (
            "missing = 1",
            vec![],
            "column missing is unknown or ambiguous",
        ),
        (
            "x = 1 OR y = $1",
            vec![Value::String("bad".into())],
            "relational comparison on y has an incompatible scalar type",
        ),
    ] {
        let bound = BoundStreamingPredicate::bind(
            &predicate(source),
            &parameters,
            &schema(),
            "records",
            "r",
        );
        assert_eq!(
            bound.err(),
            Some(SkeinError::Semantic(message.into())),
            "{source}"
        );
    }
    for source in ["records.x = 1", "r.x = 1", "x = 1"] {
        assert!(
            BoundStreamingPredicate::bind(&predicate(source), &[], &schema(), "records", "r")
                .is_ok()
        );
    }
}

#[test]
fn borrowed_row_binding_short_circuits_before_missing_fields() {
    let schema = RelationalTableSchema {
        name: "logic_rows".to_string(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".to_string(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "flag".to_string(),
                scalar_type: RelationalScalarType::Boolean,
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
    let row = RelationalProjectedRow {
        primary_key: RelationalKey(vec![RelationalValue::Text("row-1".to_string())]),
        fields: vec![RelationalProjectedField {
            ordinal: 1,
            value: RelationalValue::Boolean(false),
        }],
    };
    let row = RelationalReadRowRef::from_projected(&row);

    let and = BoundStreamingPredicate::bind(
        &predicate("flag = TRUE AND body = 'unused'"),
        &[],
        &schema,
        "logic_rows",
        "logic_rows",
    )
    .expect("bind short-circuit AND predicate");
    assert_eq!(
        and.truth_with(&|ordinal| row.value(ordinal)),
        Ok(Some(false))
    );

    let or = BoundStreamingPredicate::bind(
        &predicate("flag = FALSE OR body = 'unused'"),
        &[],
        &schema,
        "logic_rows",
        "logic_rows",
    )
    .expect("bind short-circuit OR predicate");
    assert_eq!(or.truth_with(&|ordinal| row.value(ordinal)), Ok(Some(true)));
}

#[test]
fn borrowed_streaming_resolution_keeps_short_circuit_and_error_order() {
    let missing = SkeinError::Execution("unavailable borrowed ordinal".into());
    for (source, expected, ordinals) in [
        ("x = 0 AND body = 'unused'", Ok(Some(false)), vec![0]),
        ("x = 1 OR body = 'unused'", Ok(Some(true)), vec![0]),
        (
            "x = 1 AND body = 'unused'",
            Err(missing.clone()),
            vec![0, 3],
        ),
        (
            "x = NULL AND body = 'unused'",
            Err(missing.clone()),
            vec![0, 3],
        ),
        (
            "x = NULL OR body = 'unused'",
            Err(missing.clone()),
            vec![0, 3],
        ),
        ("y = x", Err(missing.clone()), vec![1]),
        ("x IN (1, 2)", Ok(Some(true)), vec![0]),
    ] {
        let bound =
            BoundStreamingPredicate::bind(&predicate(source), &[], &schema(), "records", "r")
                .unwrap();
        let visits = RefCell::new(Vec::new());
        let actual = bound.truth_with(&|ordinal| {
            visits.borrow_mut().push(ordinal);
            if ordinal == 0 {
                Ok(RelationalValueRef::BigInt(1))
            } else {
                Err(missing.clone())
            }
        });
        assert_eq!(actual, expected, "{source}");
        assert_eq!(*visits.borrow(), ordinals, "{source}");
    }
}

#[test]
fn internal_operands_preserve_borrows_and_reject_unsupported_shapes() {
    let value = RelationalValue::Text("borrowed body".into());
    let resolver = |_: &SqlColumnRef| Ok((&value, RelationalScalarType::Text));
    let column = Expr::column(SqlColumnRef {
        qualifier: None,
        name: "body".into(),
    });
    let operand = predicate_operand(&column, &[], RelationalScalarType::Text, &resolver).unwrap();
    let std::borrow::Cow::Borrowed(borrowed) = operand else {
        panic!("column operand must not clone its value");
    };
    assert!(std::ptr::eq(borrowed, &value));

    for boolean in [false, true] {
        let expression = Expr::value(SqlValue::Literal(Value::Bool(boolean)));
        assert_eq!(
            predicate_truth_with(&expression, &[], &resolver),
            Ok(Some(boolean))
        );
        assert_eq!(
            BoundStreamingPredicate::bind(&expression, &[], &schema(), "records", "r").err(),
            Some(SkeinError::Semantic(
                "unsupported streaming predicate expression".into()
            ))
        );
    }
    assert_eq!(
        predicate_truth_with(&column, &[], &resolver),
        Err(SkeinError::Semantic(
            "unsupported relational predicate expression".into()
        ))
    );
    let unsupported = Expr::unspanned(ExprKind::Not(Box::new(column)));
    assert_eq!(
        predicate_operand(&unsupported, &[], RelationalScalarType::Text, &resolver),
        Err(SkeinError::Semantic("unsupported predicate operand".into()))
    );
}

#[test]
fn uuid_binding_does_not_unify_the_two_existing_paths() {
    let uuid = skein_core::Uuid::nil();
    let value = RelationalValue::Uuid(uuid);
    let parameters = [Value::String(uuid.to_string())];
    let expression = predicate("x = $1");
    let mut schema = schema();
    schema.columns[0].scalar_type = RelationalScalarType::Uuid;
    assert_eq!(
        predicate_truth_with(&expression, &parameters, &|_| {
            Ok((&value, RelationalScalarType::Uuid))
        }),
        Ok(Some(true))
    );
    assert_eq!(
        BoundStreamingPredicate::bind(&expression, &parameters, &schema, "records", "r").err(),
        Some(SkeinError::Semantic(
            "relational comparison on x has an incompatible scalar type".into()
        ))
    );
    let bound =
        BoundStreamingPredicate::bind(&expression, &[Value::Uuid(uuid)], &schema, "records", "r")
            .unwrap();
    assert_eq!(bound.truth_with(&|_| Ok(value.as_ref())), Ok(Some(true)));
}

fn next(random: &mut u64) -> u64 {
    *random ^= *random << 13;
    *random ^= *random >> 7;
    *random ^= *random << 17;
    *random
}

fn qualification_campaign(seeds: u64, cases_per_seed: usize) {
    let sources = [
        "x = $1",
        "x <> $1",
        "x < y",
        "x <= y",
        "x > y",
        "x >= y",
        "x IN ($1, $2, NULL)",
        "x NOT IN ($1, $2, NULL)",
        "x IS NULL",
        "x IS NOT NULL",
        "x = $1 AND flag = TRUE",
        "x = $1 OR flag = TRUE",
        "NOT (x = $1 OR flag = TRUE)",
        "(x = $1 OR flag = TRUE) AND y IS NOT NULL",
    ];
    let predicates = sources.map(predicate);
    for seed in 1..=seeds {
        let mut random = seed.wrapping_mul(0x418_5ca1_a123);
        for case in 0..cases_per_seed {
            let mut integer = || {
                let value = next(&mut random);
                if value.is_multiple_of(4) {
                    None
                } else {
                    Some((value % 7) as i64 - 3)
                }
            };
            let x = integer();
            let y = integer();
            let p = integer();
            let q = integer();
            let flag = match next(&mut random) % 3 {
                0 => None,
                1 => Some(false),
                _ => Some(true),
            };
            let values = [
                x.map_or(RelationalValue::Null, RelationalValue::BigInt),
                y.map_or(RelationalValue::Null, RelationalValue::BigInt),
                unknown_bool(flag),
                RelationalValue::Text("payload".into()),
            ];
            let parameters = [
                p.map_or(Value::Null, Value::Int),
                q.map_or(Value::Null, Value::Int),
            ];
            let eq = x.zip(p).map(|(x, p)| x == p);
            let matched = x.is_some() && (x == p || x == q);
            let in_list = if matched { Some(true) } else { None };
            let expected = [
                eq,
                eq.map(|v| !v),
                x.zip(y).map(|(x, y)| x < y),
                x.zip(y).map(|(x, y)| x <= y),
                x.zip(y).map(|(x, y)| x > y),
                x.zip(y).map(|(x, y)| x >= y),
                in_list,
                in_list.map(|v| !v),
                Some(x.is_none()),
                Some(x.is_some()),
                and(eq, flag),
                or(eq, flag),
                or(eq, flag).map(|v| !v),
                and(or(eq, flag), Some(y.is_some())),
            ];
            for (index, predicate) in predicates.iter().enumerate() {
                let expected = Ok(expected[index]);
                assert_eq!(
                    ordinary(predicate, &parameters, &values),
                    expected,
                    "ordinary seed={seed} case={case} source={}",
                    sources[index]
                );
                assert_eq!(
                    streaming(predicate, &parameters, &values),
                    expected,
                    "streaming seed={seed} case={case} source={}",
                    sources[index]
                );
            }
        }
    }
    println!(
        "relational qualification seeds={seeds} cases={} exact_results={}",
        seeds as usize * cases_per_seed,
        seeds as usize * cases_per_seed * sources.len() * 2
    );
}

#[test]
fn qualification_differential_smoke() {
    qualification_campaign(2, 16);
}

#[test]
#[ignore = "explicit local relational qualification fuzz campaign"]
fn qualification_differential_campaign() {
    qualification_campaign(128, 64);
}
