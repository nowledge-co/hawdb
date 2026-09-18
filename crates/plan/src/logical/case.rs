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

use super::*;
use hawdb_cypher::ScalarBinaryOp as AstOp;
use hawdb_expression::ScalarBinaryOp;

pub(super) fn plan_case_scalar(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    expression: &ScalarExpression,
    parameters: &BTreeMap<String, Value>,
    top_level: bool,
) -> Result<ProjectionExpression> {
    let bind = |child| plan_scalar_expression_with_columns(scope, columns, child, parameters);
    Ok(match &expression.kind {
        ScalarExpressionKind::Case {
            operand,
            branches,
            otherwise,
        } => {
            if let Some(specialized) =
                specialize_case(scope, columns, expression, parameters, top_level)
            {
                return specialized;
            }
            ProjectionExpression::Case {
                operand: operand.as_deref().map(bind).transpose()?.map(Box::new),
                branches: branches
                    .iter()
                    .map(|(condition, result)| Ok((bind(condition)?, bind(result)?)))
                    .collect::<Result<_>>()?,
                otherwise: otherwise.as_deref().map(bind).transpose()?.map(Box::new),
            }
        }
        ScalarExpressionKind::Binary { left, op, right } => ProjectionExpression::Binary {
            left: Box::new(bind(left)?),
            op: match op {
                AstOp::Eq => ScalarBinaryOp::Eq,
                AstOp::NotEq => ScalarBinaryOp::NotEq,
                AstOp::Lt => ScalarBinaryOp::Lt,
                AstOp::Lte => ScalarBinaryOp::Lte,
                AstOp::Gt => ScalarBinaryOp::Gt,
                AstOp::Gte => ScalarBinaryOp::Gte,
                AstOp::Contains => ScalarBinaryOp::Contains,
                AstOp::ListContains => ScalarBinaryOp::ListContains,
                AstOp::And => ScalarBinaryOp::And,
                AstOp::Or => ScalarBinaryOp::Or,
            },
            right: Box::new(bind(right)?),
        },
        ScalarExpressionKind::Not(child) => ProjectionExpression::Not(Box::new(bind(child)?)),
        ScalarExpressionKind::IsNull {
            expression,
            negated,
        } => ProjectionExpression::IsNull {
            expression: Box::new(bind(expression)?),
            negated: *negated,
        },
        _ => unreachable!("case scalar dispatcher only accepts conditional expressions"),
    })
}

fn binary(
    expression: &ScalarExpression,
    expected: AstOp,
) -> Option<(&ScalarExpression, &ScalarExpression)> {
    match &expression.kind {
        ScalarExpressionKind::Binary { left, op, right } if *op == expected => Some((left, right)),
        _ => None,
    }
}

fn value(expression: &ScalarExpression) -> Option<&ValueExpression> {
    match &expression.kind {
        ScalarExpressionKind::Value(value) => Some(value),
        _ => None,
    }
}

/// Derive the existing physical fast paths from a general searched CASE tree.
/// Unmatched shapes retain branch order and lazy evaluation in the generic executor.
fn specialize_case(
    scope: &BTreeSet<String>,
    columns: &BTreeSet<String>,
    expression: &ScalarExpression,
    parameters: &BTreeMap<String, Value>,
    top_level: bool,
) -> Option<Result<ProjectionExpression>> {
    let ScalarExpressionKind::Case {
        operand: None,
        branches,
        otherwise: Some(otherwise),
    } = &expression.kind
    else {
        return None;
    };
    let default = value(otherwise)?;
    match branches.as_slice() {
        [(first, exact), (second, second_exact), (third, alias)] => {
            let (lowered_name, raw) = binary(first, AstOp::Eq)?;
            let (second_name, normalized) = binary(second, AstOp::Eq)?;
            let (aliases, input) = binary(third, AstOp::ListContains)?;
            let ScalarExpressionKind::Lower(name) = &lowered_name.kind else {
                return None;
            };
            let ScalarExpressionKind::Property {
                variable,
                property: name_property,
            } = &name.kind
            else {
                return None;
            };
            let ScalarExpressionKind::Property {
                variable: alias_variable,
                property: aliases_property,
            } = &aliases.kind
            else {
                return None;
            };
            if lowered_name != second_name || variable != alias_variable || exact != second_exact {
                return None;
            }
            if columns.contains(variable) {
                return None;
            }
            let (raw, normalized, input, exact, alias) = (
                value(raw)?,
                value(normalized)?,
                value(input)?,
                value(exact)?,
                value(alias)?,
            );
            Some((|| {
                if !scope.contains(variable) {
                    let position = if top_level {
                        "return item"
                    } else {
                        "expression"
                    };
                    return Err(HawDBError::Semantic(format!(
                        "unknown variable '{variable}' in {position}"
                    )));
                }
                Ok(ProjectionExpression::CaseEntitySearchRank(Box::new(
                    CaseEntitySearchRankProjection {
                        variable: variable.clone(),
                        name_property: name_property.clone(),
                        aliases_property: aliases_property.clone(),
                        raw_query: bind_value(raw, parameters)?,
                        normalized_query: bind_value(normalized, parameters)?,
                        raw_input: bind_value(input, parameters)?,
                        exact_rank: bind_value(exact, parameters)?,
                        alias_rank: bind_value(alias, parameters)?,
                        fallback_rank: bind_value(default, parameters)?,
                    },
                )))
            })())
        }
        [(first, exact), (second, second_exact), (third, contains), (fourth, second_contains)] => {
            let (column, raw) = binary(first, AstOp::Eq)?;
            let (second_column, normalized) = binary(second, AstOp::Eq)?;
            let (third_column, third_raw) = binary(third, AstOp::Contains)?;
            let (fourth_column, fourth_normalized) = binary(fourth, AstOp::Contains)?;
            let ScalarExpressionKind::Variable(column_name) = &column.kind else {
                return None;
            };
            if column != second_column
                || column != third_column
                || column != fourth_column
                || raw != third_raw
                || normalized != fourth_normalized
                || exact != second_exact
                || contains != second_contains
            {
                return None;
            }
            if !columns.contains(column_name) {
                return None;
            }
            let (raw, normalized, exact, contains) = (
                value(raw)?,
                value(normalized)?,
                value(exact)?,
                value(contains)?,
            );
            Some((|| {
                Ok(ProjectionExpression::CaseColumnSearchRank(Box::new(
                    CaseColumnSearchRankProjection {
                        column: column_name.clone(),
                        raw_query: bind_value(raw, parameters)?,
                        normalized_query: bind_value(normalized, parameters)?,
                        exact_rank: bind_value(exact, parameters)?,
                        contains_rank: bind_value(contains, parameters)?,
                        fallback_rank: bind_value(default, parameters)?,
                    },
                )))
            })())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_shape_specialization_respects_projected_column_shadowing() {
        let statement = hawdb_cypher::parse("MATCH (n:Item) RETURN CASE WHEN lower(n.name) = 'first' THEN 0 WHEN lower(n.name) = 'second' THEN 0 WHEN list_contains(n.aliases, 'alias') THEN 1 ELSE 2 END").unwrap();
        let Statement::MatchReturn(query) = statement else {
            panic!("expected MATCH");
        };
        let ReturnExpressionKind::Value(expression) = &query.returns[0].expression.kind else {
            panic!("expected scalar");
        };
        let scope = BTreeSet::from(["n".to_string()]);
        let planned = plan_case_scalar(&scope, &scope, expression, &BTreeMap::new(), true).unwrap();
        let ProjectionExpression::Case { branches, .. } = planned else {
            panic!("a projected column must keep generic column semantics");
        };
        let ProjectionExpression::Binary { left, .. } = &branches[0].0 else {
            panic!("expected comparison");
        };
        let ProjectionExpression::Lower(name) = left.as_ref() else {
            panic!("expected LOWER");
        };
        assert!(
            matches!(name.as_ref(), ProjectionExpression::ColumnProperty { column, property } if column == "n" && property == "name")
        );
    }
}
