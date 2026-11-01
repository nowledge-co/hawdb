// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Temporal functions, postfix property access, map literals and scalar
//! arithmetic through the bounded PEG statement frontend.

use crate::{
    parse, ClauseKind, OrderExpression, ProjectionClause, PropertyPredicate, QueryPipeline,
    ReturnExpressionKind, ScalarBinaryOp, ScalarExpression, ScalarExpressionKind,
    SetValueExpression, Statement, ValueExpressionKind,
};
use hawdb_core::{TemporalFunction, TemporalKind, Value};

fn pipeline(query: &str) -> QueryPipeline {
    match parse(query).unwrap() {
        Statement::Pipeline(parsed) | Statement::UnwindMutation(parsed) => *parsed,
        other => panic!("expected a pipeline, got {other:?}"),
    }
}

fn returned(query: &QueryPipeline) -> &ProjectionClause {
    let ClauseKind::Return(projection) = &query.clauses.last().unwrap().kind else {
        panic!("expected a concluding RETURN clause");
    };
    projection
}

fn returned_scalars(query: &str) -> Vec<ScalarExpression> {
    returned(&pipeline(query))
        .items
        .iter()
        .map(|item| match &item.expression.kind {
            ReturnExpressionKind::Value(value) => value.clone(),
            other => panic!("expected scalar return item, got {other:?}"),
        })
        .collect()
}

fn where_predicate(query: &str) -> PropertyPredicate {
    let parsed = pipeline(query);
    let ClauseKind::Match {
        predicate: Some(predicate),
        ..
    } = &parsed.clauses[0].kind
    else {
        panic!("expected MATCH ... WHERE");
    };
    predicate.kind.clone()
}

fn temporal(expression: &ScalarExpression) -> (TemporalFunction, usize) {
    match &expression.kind {
        ScalarExpressionKind::TemporalFunction {
            function,
            arguments,
        } => (*function, arguments.len()),
        other => panic!("expected a temporal function, got {other:?}"),
    }
}

#[test]
fn temporal_functions_parse_namespaces_case_insensitively() {
    let items = returned_scalars(
        "MATCH (n:N) RETURN date() AS a, DATE('2026-10-30') AS b, localtime('12:00') AS c, \
         localDateTime({year: 2026}) AS d, datetime(n.at) AS e, duration('P1D') AS f, \
         duration.between(n.a, n.b) AS g, Duration.InMonths(n.a, n.b) AS h, \
         duration.inDays(n.a, n.b) AS i, duration.inSeconds(n.a, n.b) AS j, \
         date.truncate('month', n.a) AS k, datetime.truncate('day', n.a) AS l, \
         localtime.truncate('hour', n.a) AS m, localdatetime.truncate('year', n.a) AS o, \
         datetime.fromepoch(1, 2) AS p, datetime.fromepochmillis(3) AS q",
    );
    let expected = [
        (TemporalFunction::Construct(TemporalKind::Date), 0),
        (TemporalFunction::Construct(TemporalKind::Date), 1),
        (TemporalFunction::Construct(TemporalKind::LocalTime), 1),
        (TemporalFunction::Construct(TemporalKind::LocalDateTime), 1),
        (TemporalFunction::Construct(TemporalKind::DateTime), 1),
        (TemporalFunction::Duration, 1),
        (TemporalFunction::DurationBetween, 2),
        (TemporalFunction::DurationInMonths, 2),
        (TemporalFunction::DurationInDays, 2),
        (TemporalFunction::DurationInSeconds, 2),
        (TemporalFunction::Truncate(TemporalKind::Date), 2),
        (TemporalFunction::Truncate(TemporalKind::DateTime), 2),
        (TemporalFunction::Truncate(TemporalKind::LocalTime), 2),
        (TemporalFunction::Truncate(TemporalKind::LocalDateTime), 2),
        (TemporalFunction::DateTimeFromEpoch, 2),
        (TemporalFunction::DateTimeFromEpochMillis, 1),
    ];
    assert_eq!(items.len(), expected.len());
    for (item, expected) in items.iter().zip(expected) {
        assert_eq!(temporal(item), expected);
    }
    let ScalarExpressionKind::TemporalFunction { arguments, .. } = &items[3].kind else {
        unreachable!();
    };
    assert!(matches!(
        &arguments[0].kind,
        ScalarExpressionKind::MapLiteral(entries) if entries.len() == 1 && entries[0].0 == "year"
    ));
}

#[test]
fn temporal_names_remain_ordinary_variables_without_a_call() {
    let items = returned_scalars(
        "MATCH (date:N), (duration:M) RETURN date.year AS a, duration.days AS b, date AS c",
    );
    assert!(matches!(
        &items[0].kind,
        ScalarExpressionKind::Property { variable, property } if variable == "date" && property == "year"
    ));
    assert!(matches!(
        &items[1].kind,
        ScalarExpressionKind::Property { variable, property } if variable == "duration" && property == "days"
    ));
    assert!(matches!(&items[2].kind, ScalarExpressionKind::Variable(name) if name == "date"));
}

#[test]
fn postfix_property_access_chains_on_any_scalar() {
    let items = returned_scalars(
        "MATCH (p:P) RETURN p.birthday.month AS a, duration.between(p.a, p.b).minutes AS b, \
         $map.key AS c, p.list[0].year AS d, date('2026-10-30').dayOfWeek AS e",
    );
    let ScalarExpressionKind::PropertyAccess { expression, key } = &items[0].kind else {
        panic!("expected property access");
    };
    assert_eq!(key, "month");
    assert!(matches!(
        &expression.kind,
        ScalarExpressionKind::Property { variable, property } if variable == "p" && property == "birthday"
    ));
    let ScalarExpressionKind::PropertyAccess { expression, key } = &items[1].kind else {
        panic!("expected property access");
    };
    assert_eq!(key, "minutes");
    assert_eq!(temporal(expression), (TemporalFunction::DurationBetween, 2));
    assert!(matches!(
        &items[2].kind,
        ScalarExpressionKind::PropertyAccess { expression, key }
            if key == "key" && matches!(expression.kind, ScalarExpressionKind::Value(_))
    ));
    assert!(matches!(
        &items[3].kind,
        ScalarExpressionKind::PropertyAccess { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::ListIndex { .. })
    ));
    // Slices keep `..`, which is never a property access.
    let items = returned_scalars("MATCH (p:P) RETURN p.list[1..2] AS a, p.list[..1] AS b");
    assert!(matches!(
        items[0].kind,
        ScalarExpressionKind::ListSlice { .. }
    ));
}

#[test]
fn where_clauses_accept_temporal_arithmetic_and_component_predicates() {
    let PropertyPredicate::And(children) = where_predicate(
        "MATCH (m:Message) WHERE m.creationDate >= datetime($start) \
         AND m.creationDate < datetime($start) + duration({days: $durationDays}) RETURN m",
    ) else {
        panic!("expected a conjunction");
    };
    let PropertyPredicate::ExpressionCompare { value, .. } = &children[1] else {
        panic!("expected an expression comparison, got {:?}", children[1]);
    };
    let ScalarExpressionKind::Binary { left, op, right } = &value.kind else {
        panic!("expected arithmetic");
    };
    assert_eq!(*op, ScalarBinaryOp::Add);
    assert_eq!(
        temporal(left),
        (TemporalFunction::Construct(TemporalKind::DateTime), 1)
    );
    assert_eq!(temporal(right), (TemporalFunction::Duration, 1));

    let PropertyPredicate::Or(branches) = where_predicate(
        "MATCH (p:Person) WHERE (p.birthday.month = $month AND p.birthday.day >= 21) \
         OR (p.birthday.month = ($month % 12) + 1 AND p.birthday.day < 22) RETURN p",
    ) else {
        panic!("expected a disjunction");
    };
    let PropertyPredicate::And(second) = &branches[1] else {
        panic!("expected a conjunction");
    };
    let PropertyPredicate::ExpressionEq { expression, value } = &second[0] else {
        panic!("expected component equality, got {:?}", second[0]);
    };
    assert!(matches!(
        expression.kind,
        ScalarExpressionKind::PropertyAccess { .. }
    ));
    let ScalarExpressionKind::Binary { left, op, .. } = &value.kind else {
        panic!("expected `(... % 12) + 1`");
    };
    assert_eq!(*op, ScalarBinaryOp::Add);
    assert!(matches!(
        left.kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Remainder,
            ..
        }
    ));
    // `*` binds tighter than `+`, `-` is left-associative, unary minus works.
    let PropertyPredicate::ExpressionEq { value, .. } =
        where_predicate("MATCH (n:N) WHERE n.x = 1 + 2 * 3 - -n.y RETURN n")
    else {
        panic!("expected equality");
    };
    let ScalarExpressionKind::Binary { left, op, .. } = &value.kind else {
        panic!("expected arithmetic");
    };
    assert_eq!(*op, ScalarBinaryOp::Subtract);
    assert!(matches!(
        &left.kind,
        ScalarExpressionKind::Binary { op: ScalarBinaryOp::Add, right, .. }
            if matches!(right.kind, ScalarExpressionKind::Binary { op: ScalarBinaryOp::Multiply, .. })
    ));
    // A temporal call may start a predicate.
    assert!(matches!(
        where_predicate(
            "MATCH (n:N) WHERE date.truncate('month', n.on) = date('2026-10-01') RETURN n"
        ),
        PropertyPredicate::ExpressionEq { .. }
    ));
}

#[test]
fn value_positions_bind_temporal_calls_statically() {
    let Statement::CreateNode(create) = parse(
        "CREATE (:Event {on: date('2026-10-30'), took: duration({hours: 1, minutes: 30}), \
         at: datetime.truncate('day', datetime('2026-10-30T12:00Z'))})",
    )
    .unwrap() else {
        panic!("expected CREATE");
    };
    let properties = &create.properties;
    assert!(matches!(
        &properties["at"].kind,
        ValueExpressionKind::Temporal { function: TemporalFunction::Truncate(TemporalKind::DateTime), arguments }
            if matches!(arguments[1].kind, ValueExpressionKind::Temporal { .. })
    ));
    assert!(matches!(
        &properties["on"].kind,
        ValueExpressionKind::Temporal { function: TemporalFunction::Construct(TemporalKind::Date), arguments }
            if matches!(&arguments[0].kind, ValueExpressionKind::Literal(Value::String(text)) if text == "2026-10-30")
    ));
    assert!(matches!(
        &properties["took"].kind,
        ValueExpressionKind::Temporal { arguments, .. }
            if matches!(&arguments[0].kind, ValueExpressionKind::Map(entries) if entries.len() == 2)
    ));
    let parsed = pipeline("MATCH (e:Event) SET e.on = date.truncate('month', date('2026-10-30'))");
    let ClauseKind::Set(sets) = &parsed.clauses[1].kind else {
        panic!("expected SET");
    };
    assert!(matches!(
        &sets[0].value,
        SetValueExpression::Value(value) if matches!(value.kind, ValueExpressionKind::Temporal { .. })
    ));
}

#[test]
fn order_by_accepts_components_and_temporal_calls() {
    let parsed = pipeline(
        "MATCH (p:P) RETURN p.id AS id ORDER BY p.birthday.month DESC, date.truncate('year', p.on), id",
    );
    let order = &returned(&parsed).order_by;
    assert!(matches!(
        &order[0].expression,
        OrderExpression::Value(value) if matches!(value.kind, ScalarExpressionKind::PropertyAccess { .. })
    ));
    assert!(matches!(&order[1].expression, OrderExpression::Value(_)));
    assert!(matches!(&order[2].expression, OrderExpression::Column(name) if name == "id"));
}

#[test]
fn temporal_syntax_errors_are_parse_errors() {
    for query in [
        "MATCH (n:N) RETURN date(1, 2) AS a",
        "MATCH (n:N) RETURN duration() AS a",
        "MATCH (n:N) RETURN duration.since(n.a, n.b) AS a",
        "MATCH (n:N) RETURN duration.between(n.a) AS a",
        "MATCH (n:N) RETURN date({year: 1, year: 2}) AS a",
        "CREATE (:N {on: date.truncate('day')})",
    ] {
        assert!(
            matches!(parse(query), Err(hawdb_core::HawDBError::Parse(_))),
            "{query}: {:?}",
            parse(query)
        );
    }
}

#[test]
fn deep_arithmetic_and_postfix_chains_are_bounded() {
    let sum = std::iter::repeat_n("1", 64).collect::<Vec<_>>().join(" + ");
    let query = format!("MATCH (n:N) WHERE n.x = {sum} RETURN n");
    let error = parse(&query).unwrap_err().to_string();
    assert!(error.contains("nesting exceeds limit"), "{error}");
    let chain = std::iter::repeat_n(".a", 64).collect::<String>();
    let error = parse(&format!("MATCH (n:N) RETURN n.p{chain} AS x"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("nesting exceeds limit"), "{error}");
}
