use super::*;
use hawdb_cypher::ArithmeticOp;

pub(super) fn plan_composed_returns(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    items: &[ReturnItem],
    parameters: &BTreeMap<String, Value>,
) -> Result<PlannedReturns> {
    let aggregate = items
        .iter()
        .any(|item| contains_aggregate(&item.expression));
    let mut binder = ComposedReturnBinder {
        scope,
        columns,
        parameters,
        aggregate,
        group_keys: Vec::new(),
        aggregations: Vec::new(),
    };
    let projections = items
        .iter()
        .map(|item| {
            let (expression, name) = binder.bind(&item.expression)?;
            Ok(Projection {
                expression,
                name: item.alias.clone().unwrap_or(name),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if aggregate {
        Ok(PlannedReturns::AggregateProjection {
            group_keys: binder.group_keys,
            items: binder.aggregations,
            projections,
        })
    } else {
        Ok(PlannedReturns::Projections(projections))
    }
}

pub(super) fn contains_aggregate(expression: &ReturnExpression) -> bool {
    match &expression.kind {
        ReturnExpressionKind::Aggregate(_) => true,
        ReturnExpressionKind::Arithmetic { first, rest } => {
            contains_aggregate(first) || rest.iter().any(|(_, child)| contains_aggregate(child))
        }
        ReturnExpressionKind::Value(_) | ReturnExpressionKind::Path(_) => false,
    }
}

struct ComposedReturnBinder<'a> {
    scope: &'a BTreeSet<String>,
    columns: &'a BTreeSet<String>,
    parameters: &'a BTreeMap<String, Value>,
    aggregate: bool,
    group_keys: Vec<Projection>,
    aggregations: Vec<Aggregation>,
}

impl ComposedReturnBinder<'_> {
    fn bind(&mut self, expression: &ReturnExpression) -> Result<(ProjectionExpression, String)> {
        match &expression.kind {
            ReturnExpressionKind::Arithmetic { first, rest } => {
                let (mut bound, mut name) = self.bind(first)?;
                for (op, right) in rest {
                    let (right, right_name) = self.bind(right)?;
                    let (op, symbol) = match op {
                        ArithmeticOp::Add => (ScalarBinaryOp::Add, "+"),
                        ArithmeticOp::Subtract => (ScalarBinaryOp::Subtract, "-"),
                        ArithmeticOp::Multiply => (ScalarBinaryOp::Multiply, "*"),
                        ArithmeticOp::Divide => (ScalarBinaryOp::Divide, "/"),
                        ArithmeticOp::Remainder => (ScalarBinaryOp::Remainder, "%"),
                    };
                    bound = ProjectionExpression::Binary {
                        left: Box::new(bound),
                        op,
                        right: Box::new(right),
                    };
                    name = format!("({name} {symbol} {right_name})");
                }
                Ok((bound, name))
            }
            ReturnExpressionKind::Aggregate(_) => {
                let item = AstNode::synthetic(ReturnItemKind {
                    expression: expression.clone(),
                    alias: None,
                });
                let mut aggregation =
                    plan_aggregation_with_columns(self.scope, self.columns, &item)?;
                let name = aggregation.name.clone();
                // Internal columns cannot collide with Cypher identifiers or public aliases.
                aggregation.name = format!("\0aggregate.{}", self.aggregations.len());
                let column = ProjectionExpression::Column(aggregation.name.clone());
                self.aggregations.push(aggregation);
                Ok((column, name))
            }
            ReturnExpressionKind::Value(value) => {
                let item = AstNode::synthetic(ReturnItemKind {
                    expression: expression.clone(),
                    alias: None,
                });
                let mut projection =
                    plan_projection_with_columns(self.scope, self.columns, &item, self.parameters)?;
                let name = projection.name.clone();
                if self.aggregate
                    && !scalar_expression_is_scoped(value, &BTreeSet::new(), &BTreeSet::new())
                {
                    projection.name = format!("\0group.{}", self.group_keys.len());
                    let column = ProjectionExpression::Column(projection.name.clone());
                    self.group_keys.push(projection);
                    Ok((column, name))
                } else {
                    Ok((projection.expression, name))
                }
            }
            ReturnExpressionKind::Path(_) => Err(HawDBError::Semantic(
                "path projection requires a bound path".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bind(query: &str) -> PlannedReturns {
        let pipeline = hawdb_cypher::parse_pipeline(query).unwrap();
        let hawdb_cypher::ClauseKind::Return(projection) = &pipeline.clauses.last().unwrap().kind
        else {
            panic!("expected RETURN");
        };
        plan_return_items_with_columns(
            &BTreeSet::from(["n".to_string()]),
            &BTreeSet::new(),
            &projection.items,
            &BTreeMap::new(),
        )
        .unwrap()
    }

    #[test]
    fn aggregate_arithmetic_keeps_constants_out_of_grouping_and_hides_internal_columns() {
        let bound = bind("MATCH (n:Item) RETURN COUNT(n) + 1 AS total");
        assert_eq!(bound.names(), vec!["total"]);
        let PlannedReturns::AggregateProjection {
            group_keys,
            items,
            projections,
        } = bound
        else {
            panic!("expected aggregate projection")
        };
        assert!(group_keys.is_empty());
        assert_eq!(items.len(), 1);
        assert_eq!(projections.len(), 1);
        let ProjectionExpression::Binary { left, op, right } = &projections[0].expression else {
            panic!("expected arithmetic")
        };
        assert_eq!(*op, ScalarBinaryOp::Add);
        assert_eq!(
            left.as_ref(),
            &ProjectionExpression::Column(items[0].name.clone())
        );
        assert_eq!(
            right.as_ref(),
            &ProjectionExpression::Literal(Value::Int(1))
        );
    }

    #[test]
    fn arithmetic_preserves_grouping_and_public_projection_order() {
        let bound = bind("MATCH (n:Item) RETURN COUNT(n) + 2 AS total, n.category AS category, n.weight + COUNT(n) AS weighted");
        assert_eq!(bound.names(), vec!["total", "category", "weighted"]);
        let PlannedReturns::AggregateProjection {
            group_keys,
            items,
            projections,
        } = bound
        else {
            panic!("expected aggregate projection")
        };
        assert_eq!(group_keys.len(), 2);
        assert_eq!(items.len(), 2);
        assert_eq!(
            projections[1].expression,
            ProjectionExpression::Column(group_keys[0].name.clone())
        );
        assert_eq!(
            group_keys[0].expression,
            ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "category".to_string()
            }
        );
        assert_eq!(
            group_keys[1].expression,
            ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "weight".to_string()
            }
        );
    }

    #[test]
    fn scalar_arithmetic_is_left_associative_with_multiplication_precedence() {
        let PlannedReturns::Projections(projections) = bind("RETURN 20 - 3 * 2 - 4 AS value")
        else {
            panic!("expected project")
        };
        let expression = &projections[0].expression;
        let ProjectionExpression::Binary { left, op, right } = expression else {
            panic!("expected arithmetic")
        };
        assert_eq!(*op, ScalarBinaryOp::Subtract);
        assert_eq!(
            right.as_ref(),
            &ProjectionExpression::Literal(Value::Int(4))
        );
        let ProjectionExpression::Binary { right, op, .. } = left.as_ref() else {
            panic!("expected left-associative subtraction")
        };
        assert_eq!(*op, ScalarBinaryOp::Subtract);
        assert!(matches!(
            right.as_ref(),
            ProjectionExpression::Binary {
                op: ScalarBinaryOp::Multiply,
                ..
            }
        ));
    }
}
