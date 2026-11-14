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

use crate::{
    parse, ClauseKind, OrderExpression, ProjectionClause, PropertyPredicate, QueryPipeline,
    ReturnExpressionKind, ScalarBinaryOp, ScalarExpression, ScalarExpressionKind, Statement,
    ValueExpressionKind,
};
use hawdb_core::{ListFunction, Value};

fn pipeline(query: &str) -> QueryPipeline {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    *parsed
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

fn int(expression: &ScalarExpression) -> i64 {
    match &expression.kind {
        ScalarExpressionKind::Value(value) => match &value.kind {
            ValueExpressionKind::Literal(Value::Int(value)) => *value,
            other => panic!("expected int literal, got {other:?}"),
        },
        other => panic!("expected value, got {other:?}"),
    }
}

#[test]
fn list_functions_parse_case_insensitively_with_arity_checks() {
    let items = returned_scalars(
        "MATCH (n:Item) RETURN size(n.tags) AS a, HEAD(n.tags) AS b, last(n.tags) AS c, \
         tail(n.tags) AS d, Reverse(n.name) AS e, range(0, 10) AS f, range(10, 0, -2) AS g, \
         size([1, 2]) AS h, coalesce(head(n.tags), 'none') AS i",
    );
    let expected = [
        (ListFunction::Size, 1),
        (ListFunction::Head, 1),
        (ListFunction::Last, 1),
        (ListFunction::Tail, 1),
        (ListFunction::Reverse, 1),
        (ListFunction::Range, 2),
        (ListFunction::Range, 3),
        (ListFunction::Size, 1),
    ];
    for (item, (function, count)) in items.iter().zip(expected) {
        let ScalarExpressionKind::ListFunction {
            function: parsed,
            arguments,
        } = &item.kind
        else {
            panic!("expected list function, got {item:?}");
        };
        assert_eq!(*parsed, function);
        assert_eq!(arguments.len(), count);
    }
    let ScalarExpressionKind::ListFunction { arguments, .. } = &items[6].kind else {
        unreachable!()
    };
    assert_eq!(int(&arguments[2]), -2);
    let ScalarExpressionKind::Coalesce(arguments) = &items[8].kind else {
        panic!("expected coalesce");
    };
    assert!(matches!(
        arguments[0].kind,
        ScalarExpressionKind::ListFunction {
            function: ListFunction::Head,
            ..
        }
    ));

    for (query, message) in [
        (
            "MATCH (n) RETURN size()",
            "size() expects 1 argument(s), got 0",
        ),
        (
            "MATCH (n) RETURN head(n.a, n.b)",
            "head() expects 1 argument(s), got 2",
        ),
        (
            "MATCH (n) RETURN range(1)",
            "range() expects 2 or 3 argument(s), got 1",
        ),
        (
            "MATCH (n) RETURN range(1, 2, 3, 4)",
            "range() expects 2 or 3 argument(s), got 4",
        ),
    ] {
        let error = parse(query).unwrap_err().to_string();
        assert!(error.contains(message), "{query}: {error}");
    }
}

#[test]
fn list_comprehensions_parse_optional_where_and_projection() {
    let items = returned_scalars(
        "MATCH (n:Item) RETURN [x IN n.tags] AS a, [x IN n.tags WHERE x <> 'b'] AS b, \
         [x IN n.tags | lower(x)] AS c, [x IN range(1, 5) WHERE x > 2 AND x IN [3, 4] | x] AS d, \
         [x IN [[1], [2, 3]] | [y IN x WHERE y > 1]] AS e",
    );
    let shapes: Vec<_> = items
        .iter()
        .map(|item| match &item.kind {
            ScalarExpressionKind::ListComprehension {
                variable,
                predicate,
                projection,
                ..
            } => (variable.as_str(), predicate.is_some(), projection.is_some()),
            other => panic!("expected comprehension, got {other:?}"),
        })
        .collect();
    assert_eq!(
        shapes,
        [
            ("x", false, false),
            ("x", true, false),
            ("x", false, true),
            ("x", true, true),
            ("x", false, true),
        ]
    );
    let ScalarExpressionKind::ListComprehension { predicate, .. } = &items[3].kind else {
        unreachable!()
    };
    let ScalarExpressionKind::Binary {
        op: ScalarBinaryOp::And,
        right,
        ..
    } = &predicate.as_ref().unwrap().kind
    else {
        panic!("expected AND predicate");
    };
    assert!(matches!(
        right.kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::In,
            ..
        }
    ));
    let ScalarExpressionKind::ListComprehension { projection, .. } = &items[4].kind else {
        unreachable!()
    };
    assert!(matches!(
        projection.as_ref().unwrap().kind,
        ScalarExpressionKind::ListComprehension { .. }
    ));

    // A list literal whose first element is a boolean keyword is not a comprehension.
    let items = returned_scalars("MATCH (n) RETURN [true, false] AS flags");
    assert!(matches!(items[0].kind, ScalarExpressionKind::Value(_)));
    assert!(parse("MATCH (n) RETURN [x IN n.tags WHERE] AS broken").is_err());
    assert!(parse("MATCH (n) RETURN [x IN n.tags | ] AS broken").is_err());
}

#[test]
fn list_index_and_slice_postfixes_parse_with_negative_and_open_bounds() {
    let items = returned_scalars(
        "MATCH (n) RETURN n.tags[0] AS a, n.tags[-1] AS b, n.tags[1..3] AS c, n.tags[..2] AS d, \
         n.tags[-2..] AS e, $values[0][1] AS f, range(0, 9)[2..4] AS g, n.tags[size(n.tags)] AS h",
    );
    let ScalarExpressionKind::ListIndex { index, .. } = &items[0].kind else {
        panic!("expected index");
    };
    assert_eq!(int(index), 0);
    let ScalarExpressionKind::ListIndex { index, .. } = &items[1].kind else {
        panic!("expected index");
    };
    assert_eq!(int(index), -1);
    let ScalarExpressionKind::ListSlice { start, end, .. } = &items[2].kind else {
        panic!("expected slice");
    };
    assert_eq!(
        (int(start.as_ref().unwrap()), int(end.as_ref().unwrap())),
        (1, 3)
    );
    let ScalarExpressionKind::ListSlice { start, end, .. } = &items[3].kind else {
        panic!("expected slice");
    };
    assert!(start.is_none());
    assert_eq!(int(end.as_ref().unwrap()), 2);
    let ScalarExpressionKind::ListSlice { start, end, .. } = &items[4].kind else {
        panic!("expected slice");
    };
    assert_eq!(int(start.as_ref().unwrap()), -2);
    assert!(end.is_none());
    let ScalarExpressionKind::ListIndex { list, .. } = &items[5].kind else {
        panic!("expected chained index");
    };
    assert!(matches!(list.kind, ScalarExpressionKind::ListIndex { .. }));
    assert!(matches!(
        items[6].kind,
        ScalarExpressionKind::ListSlice { .. }
    ));
    let ScalarExpressionKind::ListIndex { index, .. } = &items[7].kind else {
        panic!("expected computed index");
    };
    assert!(matches!(
        index.kind,
        ScalarExpressionKind::ListFunction { .. }
    ));
    assert!(parse("MATCH (n) RETURN n.tags[0").is_err());
    // Chained postfixes count against the parser recursion budget.
    let deep = format!("MATCH (n) RETURN n.tags{} AS deep", "[0]".repeat(40));
    let error = parse(&deep).unwrap_err().to_string();
    assert!(error.contains("nesting exceeds limit"), "{error}");
    assert!(parse(&format!("MATCH (n) RETURN n.tags{} AS ok", "[0]".repeat(8))).is_ok());
    // Hop ranges keep their existing `*1..2` syntax.
    assert!(parse("MATCH (a)-[:R*1..2]->(b) RETURN b").is_ok());
}

#[test]
fn aggregate_arguments_produce_return_level_list_functions() {
    let query = pipeline(
        "MATCH (n:Item) RETURN size(collect(n.id)) AS total, head(collect(n)) AS first, \
         size(collect(n.id)) + 1 AS bumped, size(n.tags) AS scalar",
    );
    let items = &returned(&query).items;
    let ReturnExpressionKind::ListFunction {
        function,
        arguments,
    } = &items[0].expression.kind
    else {
        panic!("expected aggregate list function");
    };
    assert_eq!(*function, ListFunction::Size);
    assert!(matches!(
        arguments[0].kind,
        ReturnExpressionKind::Aggregate(_)
    ));
    assert!(matches!(
        items[1].expression.kind,
        ReturnExpressionKind::ListFunction {
            function: ListFunction::Head,
            ..
        }
    ));
    let ReturnExpressionKind::Arithmetic { first, .. } = &items[2].expression.kind else {
        panic!("expected arithmetic");
    };
    assert!(matches!(
        first.kind,
        ReturnExpressionKind::ListFunction { .. }
    ));
    assert!(matches!(
        &items[3].expression.kind,
        ReturnExpressionKind::Value(value)
            if matches!(value.kind, ScalarExpressionKind::ListFunction { .. })
    ));
    let error = parse("MATCH (n) RETURN range(count(n))")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("range() expects 2 or 3 argument(s), got 1"),
        "{error}"
    );
}

#[test]
fn where_and_order_by_accept_list_expressions() {
    let query = pipeline(
        "MATCH (n:Item) WITH n, n.tags AS tags WHERE size(tags) > 1 AND tags[0] = 'a' \
         AND n.id IN tags AND n.tags[-1] <> 'z' AND n.id IN [1, 2] AND head(tags) IN $allowed \
         RETURN n.id AS id ORDER BY size(tags) DESC, tags[0], [t IN tags | t][0], n.tags[1] ASC",
    );
    let ClauseKind::With(with) = &query.clauses[1].kind else {
        panic!("expected WITH");
    };
    let PropertyPredicate::And(predicates) = &with.predicate.as_ref().unwrap().kind else {
        panic!("expected conjunction");
    };
    assert!(matches!(
        &predicates[0],
        PropertyPredicate::ExpressionCompare { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::ListFunction { .. })
    ));
    assert!(matches!(
        &predicates[1],
        PropertyPredicate::ExpressionEq { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::ListIndex { .. })
    ));
    // A non-literal list uses the generic three-valued IN operator.
    let PropertyPredicate::ExpressionEq { expression, value } = &predicates[2] else {
        panic!("expected generic IN predicate, got {:?}", predicates[2]);
    };
    assert!(matches!(
        expression.kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::In,
            ..
        }
    ));
    assert!(matches!(
        &value.kind,
        ScalarExpressionKind::Value(value)
            if value.kind == ValueExpressionKind::Literal(Value::Bool(true))
    ));
    assert!(matches!(
        &predicates[3],
        PropertyPredicate::ExpressionNotEq { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::ListIndex { .. })
    ));
    // Literal lists keep the existing value-list predicate.
    assert!(matches!(&predicates[4], PropertyPredicate::In { .. }));
    assert!(matches!(
        &predicates[5],
        PropertyPredicate::ExpressionIn { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::ListFunction { .. })
    ));

    let order = &returned(&query).order_by;
    assert_eq!(order.len(), 4);
    for item in order {
        assert!(
            matches!(item.expression, OrderExpression::Value(_)),
            "{item:?}"
        );
    }
}

#[test]
fn parameter_in_list_predicates_parse() {
    let query = pipeline("MATCH (n:Item) WHERE $id IN n.ids AND $id IN [1, 2] RETURN n");
    let ClauseKind::Match { predicate, .. } = &query.clauses[0].kind else {
        panic!("expected MATCH");
    };
    let PropertyPredicate::And(predicates) = &predicate.as_ref().unwrap().kind else {
        panic!("expected conjunction");
    };
    assert!(matches!(
        &predicates[0],
        PropertyPredicate::ExpressionEq { expression, .. }
            if matches!(expression.kind, ScalarExpressionKind::Binary { op: ScalarBinaryOp::In, .. })
    ));
    assert!(matches!(
        &predicates[1],
        PropertyPredicate::ExpressionIn { expression, .. }
            if matches!(&expression.kind, ScalarExpressionKind::Value(value)
                if value.kind == ValueExpressionKind::Parameter("id".to_string()))
    ));
}
