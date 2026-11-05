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

//! Numeric functions, `reduce(...)` and `startNode`/`endNode` (#1487).

use crate::{
    parse, parse_pipeline, ClauseKind, PropertyPredicate, ReturnExpressionKind, ScalarBinaryOp,
    ScalarExpression, ScalarExpressionKind, Statement,
};
use hawdb_core::{NumericFunction, RelationshipEndpoint};

fn returned_scalars(query: &str) -> Vec<ScalarExpression> {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    let pipeline = parse_pipeline(query).unwrap();
    assert_eq!(
        format!("{:?}", parsed.clauses),
        format!("{:?}", pipeline.clauses)
    );
    let ClauseKind::Return(projection) = &parsed.clauses.last().unwrap().kind else {
        panic!("expected a concluding RETURN clause");
    };
    projection
        .items
        .iter()
        .map(|item| match &item.expression.kind {
            ReturnExpressionKind::Value(value) => value.clone(),
            other => panic!("expected scalar return item, got {other:?}"),
        })
        .collect()
}

fn parse_error(query: &str) -> String {
    parse(query).unwrap_err().to_string()
}

#[test]
fn every_numeric_function_parses_in_return_and_where() {
    for function in NumericFunction::ALL {
        let name = function.function_name();
        let items = returned_scalars(&format!(
            "MATCH (n) WHERE {name}(n.x) > 0 RETURN {}(n.x) AS a, {name}(-1.5) AS b",
            name.to_uppercase()
        ));
        let ScalarExpressionKind::NumericFunction {
            function: parsed,
            argument,
        } = &items[0].kind
        else {
            panic!("expected {name}, got {:?}", items[0]);
        };
        assert_eq!(*parsed, function);
        assert!(matches!(
            argument.kind,
            ScalarExpressionKind::Property { .. }
        ));
        assert!(
            matches!(&items[1].kind, ScalarExpressionKind::NumericFunction { argument, .. }
                if matches!(argument.kind, ScalarExpressionKind::Value(_))),
            "{:?}",
            items[1]
        );
    }
    for query in [
        "MATCH (n) RETURN floor(1, 2) AS f",
        "MATCH (n) RETURN abs() AS f",
        "MATCH (n) RETURN [x IN [1] | round(x, 2)] AS f",
    ] {
        assert!(
            parse_error(query).contains("expects 1 argument(s)"),
            "{query}"
        );
    }
}

#[test]
fn conversion_arguments_admit_arithmetic_for_ldbc_ic7() {
    let items = returned_scalars(
        "MATCH (n) RETURN toInteger(floor(toFloat(n.a - n.b)/1000.0)/60.0) AS minutes",
    );
    let ScalarExpressionKind::Conversion { argument, .. } = &items[0].kind else {
        panic!("expected toInteger, got {:?}", items[0]);
    };
    let ScalarExpressionKind::Binary {
        left,
        op: ScalarBinaryOp::Divide,
        ..
    } = &argument.kind
    else {
        panic!("expected division, got {argument:?}");
    };
    let ScalarExpressionKind::NumericFunction {
        function: NumericFunction::Floor,
        argument,
    } = &left.kind
    else {
        panic!("expected floor, got {left:?}");
    };
    let ScalarExpressionKind::Binary { left, .. } = &argument.kind else {
        panic!("expected division, got {argument:?}");
    };
    assert!(matches!(
        &left.kind,
        ScalarExpressionKind::Conversion { argument, .. }
            if matches!(argument.kind, ScalarExpressionKind::Binary { op: ScalarBinaryOp::Subtract, .. })
    ));
    // A single-operand argument keeps its historical shape.
    let items = returned_scalars("MATCH (n) RETURN toFloat(n.a) AS f");
    assert!(matches!(
        &items[0].kind,
        ScalarExpressionKind::Conversion { argument, .. }
            if matches!(argument.kind, ScalarExpressionKind::Property { .. })
    ));
}

#[test]
fn reduce_parses_its_scoped_fold() {
    let items = returned_scalars(
        "MATCH (n) RETURN reduce(total = n.base, x IN n.xs | total + x) AS a, \
         [r IN n.rs | REDUCE(w = 0.0, v IN [1.0, r] | w + v)] AS b",
    );
    let ScalarExpressionKind::Reduce {
        accumulator,
        initial,
        variable,
        list,
        expression,
    } = &items[0].kind
    else {
        panic!("expected reduce, got {:?}", items[0]);
    };
    assert_eq!((accumulator.as_str(), variable.as_str()), ("total", "x"));
    assert!(matches!(
        initial.kind,
        ScalarExpressionKind::Property { .. }
    ));
    assert!(matches!(list.kind, ScalarExpressionKind::Property { .. }));
    assert!(matches!(
        expression.kind,
        ScalarExpressionKind::Binary {
            op: ScalarBinaryOp::Add,
            ..
        }
    ));
    let ScalarExpressionKind::ListComprehension {
        projection: Some(projection),
        ..
    } = &items[1].kind
    else {
        panic!("expected comprehension, got {:?}", items[1]);
    };
    assert!(matches!(
        projection.kind,
        ScalarExpressionKind::Reduce { .. }
    ));
    let Statement::Pipeline(parsed) =
        parse("MATCH (n) WHERE reduce(s = 0, x IN n.xs | s + x) > 2 RETURN n").unwrap()
    else {
        panic!("expected a pipeline");
    };
    let ClauseKind::Match { predicate, .. } = &parsed.clauses[0].kind else {
        panic!("expected MATCH");
    };
    assert!(
        matches!(predicate.as_ref().map(|predicate| &predicate.kind),
            Some(PropertyPredicate::ExpressionCompare { expression, .. })
                if matches!(expression.kind, ScalarExpressionKind::Reduce { .. })),
        "{predicate:?}"
    );
    for query in [
        "MATCH (n) RETURN reduce(n.xs) AS r",
        "MATCH (n) RETURN [x IN [1] | reduce(x)] AS r",
    ] {
        assert!(parse_error(query).contains("reduce() requires"), "{query}");
    }
    assert!(parse("MATCH (n) RETURN reduce(s = 0, x IN n.xs) AS r").is_err());
}

#[test]
fn start_and_end_node_take_a_relationship_variable() {
    let items = returned_scalars(
        "MATCH (a)-[r]->(b) WHERE startNode(r).id = 1 RETURN STARTNODE(r) AS s, endNode(r).id AS e",
    );
    assert!(matches!(
        &items[0].kind,
        ScalarExpressionKind::RelationshipEndpoint { endpoint: RelationshipEndpoint::Start, variable }
            if variable == "r"
    ));
    assert!(matches!(
        &items[1].kind,
        ScalarExpressionKind::PropertyAccess { expression, key }
            if key == "id" && matches!(&expression.kind,
                ScalarExpressionKind::RelationshipEndpoint { endpoint: RelationshipEndpoint::End, .. })
    ));
}
