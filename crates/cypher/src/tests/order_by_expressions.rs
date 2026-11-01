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
    parse, AggregateExpression, AstNode, ClauseKind, OrderDirection, OrderExpression, OrderItem,
    ReturnExpressionKind, ScalarExpressionKind, Statement,
};

fn order_by(query: &str) -> Vec<OrderItem> {
    let Statement::Pipeline(parsed) = parse(query).unwrap() else {
        panic!("expected a pipeline");
    };
    let ClauseKind::Return(projection) = &parsed.clauses.last().unwrap().kind else {
        panic!("expected a concluding RETURN clause");
    };
    projection.order_by.clone()
}

fn scalar(item: &OrderItem) -> &ScalarExpressionKind {
    match &item.expression {
        OrderExpression::Value(value) => &value.kind,
        other => panic!("expected a scalar order key, got {other:?}"),
    }
}

#[test]
fn order_keys_accept_scalar_function_calls() {
    let items = order_by(
        "MATCH (n:Item)-[r]->(m) RETURN n.name \
         ORDER BY size(n.tags) DESC, type(r), head(reverse(n.tags)) ASC, n.tags[0], \
         coalesce($fallback, n.rank), CASE WHEN n.rank > 1 THEN 0 ELSE 1 END DESC, \
         n.name IS NULL",
    );
    assert_eq!(items.len(), 7);
    assert!(matches!(
        scalar(&items[0]),
        ScalarExpressionKind::ListFunction { .. }
    ));
    assert!(matches!(
        scalar(&items[1]),
        ScalarExpressionKind::RelationshipType(variable) if variable == "r"
    ));
    assert!(matches!(
        scalar(&items[2]),
        ScalarExpressionKind::ListFunction { arguments, .. }
            if matches!(arguments[0].kind, ScalarExpressionKind::ListFunction { .. })
    ));
    assert!(matches!(
        scalar(&items[3]),
        ScalarExpressionKind::ListIndex { .. }
    ));
    assert!(matches!(
        scalar(&items[4]),
        ScalarExpressionKind::Coalesce(_)
    ));
    assert!(matches!(
        scalar(&items[5]),
        ScalarExpressionKind::Case { .. }
    ));
    assert!(matches!(
        scalar(&items[6]),
        ScalarExpressionKind::IsNull { negated: false, .. }
    ));
    let directions = items.iter().map(|item| item.direction).collect::<Vec<_>>();
    assert_eq!(
        directions,
        [
            OrderDirection::Desc,
            OrderDirection::Asc,
            OrderDirection::Asc,
            OrderDirection::Asc,
            OrderDirection::Asc,
            OrderDirection::Desc,
            OrderDirection::Asc,
        ]
    );
}

#[test]
fn order_keys_keep_direct_shapes_for_columns_and_properties() {
    let items = order_by(
        "MATCH (n:Item) RETURN n.name AS name, count(*), count(DISTINCT n.rank) \
         ORDER BY name DESC, n.rank, id(n), (n.name), count(*), COUNT(DISTINCT n.rank) DESC",
    );
    let expressions = items
        .into_iter()
        .map(|item| item.kind.expression)
        .collect::<Vec<_>>();
    assert_eq!(
        expressions,
        [
            OrderExpression::Column("name".to_string()),
            OrderExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            OrderExpression::Id {
                variable: "n".to_string(),
            },
            OrderExpression::Property {
                variable: "n".to_string(),
                property: "name".to_string(),
            },
            OrderExpression::Projection(AstNode::synthetic(ReturnExpressionKind::Aggregate(
                AggregateExpression::CountAll
            ))),
            OrderExpression::Projection(AstNode::synthetic(ReturnExpressionKind::Aggregate(
                AggregateExpression::CountProperty {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                    distinct: true,
                }
            ))),
        ]
    );
}

#[test]
fn order_keys_accept_arithmetic_and_general_aggregates() {
    let items = order_by(
        "MATCH (n:Item) RETURN n.group AS group, sum(n.rank) AS total \
         ORDER BY -n.rank, n.rank % 2 DESC, sum(n.rank), size(collect(n.name)) DESC",
    );
    assert_eq!(items.len(), 4);
    for item in &items[..2] {
        assert!(
            matches!(
                &item.expression,
                OrderExpression::Projection(expression)
                    if matches!(expression.kind, ReturnExpressionKind::Arithmetic { .. })
            ),
            "{item:?}"
        );
    }
    assert!(matches!(
        &items[2].expression,
        OrderExpression::Projection(expression)
            if matches!(
                expression.kind,
                ReturnExpressionKind::Aggregate(AggregateExpression::Expression { .. })
            )
    ));
    assert!(matches!(
        &items[3].expression,
        OrderExpression::Projection(expression)
            if matches!(expression.kind, ReturnExpressionKind::ListFunction { .. })
    ));
}

#[test]
fn order_keys_stop_before_pagination_and_reject_trailing_input() {
    let Statement::Pipeline(parsed) = parse(
        "MATCH (n:Item) WITH n ORDER BY lower(n.name) DESC, size(n.tags) SKIP 1 LIMIT 2 \
         RETURN n.name",
    )
    .unwrap() else {
        panic!("expected a pipeline");
    };
    let ClauseKind::With(projection) = &parsed.clauses[1].kind else {
        panic!("expected WITH");
    };
    assert_eq!(projection.order_by.len(), 2);
    assert!(projection.offset.is_some() && projection.limit.is_some());

    for query in [
        "MATCH (n:Item) RETURN n ORDER BY size(n.tags",
        "MATCH (n:Item) RETURN n ORDER BY n.name n.rank",
        "MATCH (n:Item) RETURN n ORDER BY",
        "MATCH (n:Item) RETURN n ORDER BY count(DISTINCT *)",
    ] {
        assert!(parse(query).is_err(), "{query}");
    }
}
