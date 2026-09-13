use super::*;
use skein_core::Value;
use skein_expression::sql::SqlNullOrder;

fn column(qualifier: Option<&str>, name: &str) -> SqlColumnRef {
    SqlColumnRef {
        qualifier: qualifier.map(str::to_owned),
        name: name.to_owned(),
    }
}

fn compare(name: &str, op: SqlComparisonOp, position: usize) -> SqlPredicate {
    SqlPredicate::Compare {
        left: column(Some("m"), name),
        op,
        right: SqlValue::Parameter(position),
    }
}

fn and(left: SqlPredicate, right: SqlPredicate) -> SqlPredicate {
    SqlPredicate::And(Box::new(left), Box::new(right))
}

fn or(left: SqlPredicate, right: SqlPredicate) -> SqlPredicate {
    SqlPredicate::Or(Box::new(left), Box::new(right))
}

fn access_columns() -> BTreeSet<String> {
    BTreeSet::from(["owner".to_owned()])
}

fn order(direction: SqlOrderDirection) -> Vec<SqlOrderItem> {
    ["created", "id"]
        .into_iter()
        .map(|name| SqlOrderItem {
            column: column(Some("m"), name),
            direction,
            nulls: SqlNullOrder::DialectDefault,
        })
        .collect()
}

fn cursor(direction: SqlOrderDirection, swap_or: bool, swap_tie: bool) -> SqlPredicate {
    let op = match direction {
        SqlOrderDirection::Asc => SqlComparisonOp::Gt,
        SqlOrderDirection::Desc => SqlComparisonOp::Lt,
    };
    let first = compare("created", op, 1);
    let tie_first = compare("created", SqlComparisonOp::Eq, 1);
    let second = compare("id", op, 2);
    let tie = if swap_tie {
        and(second, tie_first)
    } else {
        and(tie_first, second)
    };
    if swap_or {
        or(tie, first)
    } else {
        or(first, tie)
    }
}

#[test]
fn equalities_preserve_duplicates_and_do_not_cross_or_or_not() {
    let first = compare("owner", SqlComparisonOp::Eq, 1);
    let duplicate = compare("owner", SqlComparisonOp::Eq, 2);
    let predicate = and(
        first,
        and(
            or(
                compare("hidden", SqlComparisonOp::Eq, 3),
                compare("hidden", SqlComparisonOp::Eq, 4),
            ),
            and(
                SqlPredicate::Not(Box::new(compare("negated", SqlComparisonOp::Eq, 5))),
                and(compare("range", SqlComparisonOp::Gt, 6), duplicate),
            ),
        ),
    );
    let mut extracted = Vec::new();
    collect_conjunctive_equalities(&predicate, &mut extracted);
    assert_eq!(
        extracted,
        vec![
            (&column(Some("m"), "owner"), &SqlValue::Parameter(1)),
            (&column(Some("m"), "owner"), &SqlValue::Parameter(2)),
        ]
    );
}

#[test]
fn coverage_requires_exact_distinct_equalities_for_the_target() {
    let columns = access_columns();
    let equality = compare("owner", SqlComparisonOp::Eq, 1);
    assert!(predicate_is_covered_by_equalities(
        None, &columns, "memories", "m"
    ));
    for qualifier in [None, Some("memories"), Some("m")] {
        let predicate = SqlPredicate::Compare {
            left: column(qualifier, "owner"),
            op: SqlComparisonOp::Eq,
            right: SqlValue::Parameter(1),
        };
        assert!(predicate_is_covered_by_equalities(
            Some(&predicate),
            &columns,
            "memories",
            "m"
        ));
    }
    let other_table = SqlPredicate::Compare {
        left: column(Some("other"), "owner"),
        op: SqlComparisonOp::Eq,
        right: SqlValue::Parameter(1),
    };
    for predicate in [
        and(equality.clone(), equality.clone()),
        and(equality.clone(), compare("id", SqlComparisonOp::Eq, 2)),
        and(equality.clone(), compare("id", SqlComparisonOp::Gt, 2)),
        or(equality.clone(), equality),
        compare("id", SqlComparisonOp::Eq, 1),
        other_table,
    ] {
        assert!(!predicate_is_covered_by_equalities(
            Some(&predicate),
            &columns,
            "memories",
            "m"
        ));
    }
}

#[test]
fn keyset_recognition_preserves_direction_and_boolean_permutations() {
    for direction in [SqlOrderDirection::Asc, SqlOrderDirection::Desc] {
        for swap_or in [false, true] {
            for swap_tie in [false, true] {
                for prefix_first in [false, true] {
                    for qualifier in [None, Some("memories"), Some("m")] {
                        let prefix = SqlPredicate::Compare {
                            left: column(qualifier, "owner"),
                            op: SqlComparisonOp::Eq,
                            right: SqlValue::Parameter(3),
                        };
                        let cursor = cursor(direction, swap_or, swap_tie);
                        let predicate = if prefix_first {
                            and(prefix, cursor)
                        } else {
                            and(cursor, prefix)
                        };
                        assert_eq!(
                            canonical_keyset_values(
                                Some(&predicate),
                                &access_columns(),
                                &order(direction),
                                "memories",
                                "m"
                            ),
                            Some((&SqlValue::Parameter(1), &SqlValue::Parameter(2)))
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn keyset_recognition_keeps_unproven_terms_as_residuals() {
    let direction = SqlOrderDirection::Asc;
    let prefix = compare("owner", SqlComparisonOp::Eq, 3);
    let keyset = cursor(direction, false, false);
    let valid = and(prefix.clone(), keyset.clone());
    let mismatched_value = or(
        compare("created", SqlComparisonOp::Gt, 1),
        and(
            compare("created", SqlComparisonOp::Eq, 4),
            compare("id", SqlComparisonOp::Gt, 2),
        ),
    );
    let literal_tie = or(
        compare("created", SqlComparisonOp::Gt, 1),
        and(
            SqlPredicate::Compare {
                left: column(Some("m"), "created"),
                op: SqlComparisonOp::Eq,
                right: SqlValue::Literal(Value::Int(1)),
            },
            compare("id", SqlComparisonOp::Gt, 2),
        ),
    );
    for predicate in [
        keyset.clone(),
        and(valid.clone(), prefix.clone()),
        and(valid.clone(), keyset),
        and(valid.clone(), compare("other", SqlComparisonOp::Eq, 4)),
        and(valid.clone(), compare("other", SqlComparisonOp::Gt, 4)),
        and(prefix.clone(), mismatched_value),
        and(prefix, literal_tie),
    ] {
        assert!(canonical_keyset_values(
            Some(&predicate),
            &access_columns(),
            &order(direction),
            "memories",
            "m"
        )
        .is_none());
    }
    let mut mixed_order = order(direction);
    mixed_order[1].direction = SqlOrderDirection::Desc;
    for ordering in [
        Vec::new(),
        order(direction)[..1].to_vec(),
        mixed_order,
        order(SqlOrderDirection::Desc),
    ] {
        assert!(canonical_keyset_values(
            Some(&valid),
            &access_columns(),
            &ordering,
            "memories",
            "m"
        )
        .is_none());
    }
}

#[test]
fn join_equalities_require_exactly_one_qualified_target_and_keep_the_first() {
    let join = |left, right| SqlPredicate::CompareColumns {
        left,
        op: SqlComparisonOp::Eq,
        right,
    };
    let first = join(column(Some("m"), "owner"), column(Some("u"), "id"));
    let duplicate = join(
        column(Some("u"), "other_id"),
        column(Some("memories"), "owner"),
    );
    let accepted = and(first, duplicate);
    let ignored = [
        join(
            column(Some("m"), "local"),
            column(Some("memories"), "other"),
        ),
        join(column(None, "unqualified"), column(None, "other")),
        join(column(Some("a"), "foreign"), column(Some("b"), "other")),
        or(
            join(column(Some("m"), "disjunct"), column(Some("u"), "id")),
            join(column(Some("m"), "disjunct"), column(Some("u"), "other")),
        ),
    ];
    let predicate = ignored.into_iter().fold(accepted, and);
    let mut columns = BTreeMap::new();
    collect_conjunctive_join_equalities(&predicate, "memories", "m", &mut columns);
    assert_eq!(
        columns,
        BTreeMap::from([("owner".to_owned(), column(Some("u"), "id"))])
    );
}
