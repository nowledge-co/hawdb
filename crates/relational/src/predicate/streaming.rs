//! Bound single-relation qualification with caller-owned borrowed row values.

use super::compare_value_refs;
use crate::query_value::{bind_sql_value, value_to_relational};
use skein_core::{Result, SkeinError, Value};
use skein_sql::{ExprKind, SqlColumnRef, SqlComparisonOp, SqlLikeEscape, SqlPredicate};
use skein_storage::{
    RelationalScalarType, RelationalTableSchema, RelationalValue, RelationalValueRef,
};

/// An internal, validated predicate plan. Row ownership remains with the caller.
pub struct BoundStreamingPredicate {
    root: StreamingPredicate,
}

impl BoundStreamingPredicate {
    pub fn bind(
        predicate: &SqlPredicate,
        parameters: &[Value],
        schema: &RelationalTableSchema,
        table: &str,
        qualifier: &str,
    ) -> Result<Self> {
        StreamingPredicate::bind(predicate, parameters, schema, table, qualifier)
            .map(|root| Self { root })
    }

    pub fn truth_with<'a>(
        &self,
        resolve: &impl Fn(usize) -> Result<RelationalValueRef<'a>>,
    ) -> Result<Option<bool>> {
        self.root.truth(resolve)
    }
}

enum StreamingPredicate {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    CompareValue {
        left: usize,
        op: SqlComparisonOp,
        right: RelationalValue,
    },
    CompareColumns {
        left: usize,
        op: SqlComparisonOp,
        right: usize,
    },
    InList {
        left: usize,
        values: Box<[RelationalValue]>,
        negated: bool,
    },
    Like {
        left: usize,
        pattern: RelationalValue,
        case_insensitive: bool,
        negated: bool,
        escape: SqlLikeEscape,
    },
    IsNull {
        column: usize,
        negated: bool,
    },
}

impl StreamingPredicate {
    fn bind(
        predicate: &SqlPredicate,
        parameters: &[Value],
        schema: &RelationalTableSchema,
        table: &str,
        qualifier: &str,
    ) -> Result<Self> {
        match &predicate.kind {
            ExprKind::And(left, right) => Ok(Self::And(
                Box::new(Self::bind(left, parameters, schema, table, qualifier)?),
                Box::new(Self::bind(right, parameters, schema, table, qualifier)?),
            )),
            ExprKind::Or(left, right) => Ok(Self::Or(
                Box::new(Self::bind(left, parameters, schema, table, qualifier)?),
                Box::new(Self::bind(right, parameters, schema, table, qualifier)?),
            )),
            ExprKind::Not(predicate) => Ok(Self::Not(Box::new(Self::bind(
                predicate, parameters, schema, table, qualifier,
            )?))),
            ExprKind::Compare { left, op, right } => {
                let left = bind_streaming_column(left.require_column()?, schema, table, qualifier)?;
                match &right.kind {
                    ExprKind::Value(right) => {
                        let right = value_to_relational(bind_sql_value(right, parameters)?)?;
                        validate_value_type(schema, left, &right)?;
                        Ok(Self::CompareValue {
                            left,
                            op: *op,
                            right,
                        })
                    }
                    ExprKind::Column(right) => {
                        let right = bind_streaming_column(right, schema, table, qualifier)?;
                        if schema.columns[left].scalar_type != schema.columns[right].scalar_type {
                            return Err(SkeinError::Semantic(format!(
                                "relational comparison between {} and {} has incompatible scalar types",
                                schema.columns[left].name, schema.columns[right].name
                            )));
                        }
                        Ok(Self::CompareColumns {
                            left,
                            op: *op,
                            right,
                        })
                    }
                    _ => Err(SkeinError::Semantic(
                        "unsupported streaming comparison operand".to_owned(),
                    )),
                }
            }
            ExprKind::InList {
                left,
                values,
                negated,
            } => {
                let left = bind_streaming_column(left.require_column()?, schema, table, qualifier)?;
                let values = values
                    .iter()
                    .map(|value| {
                        let value = value_to_relational(bind_sql_value(
                            value.require_value()?,
                            parameters,
                        )?)?;
                        validate_value_type(schema, left, &value)?;
                        Ok(value)
                    })
                    .collect::<Result<Box<[_]>>>()?;
                Ok(Self::InList {
                    left,
                    values,
                    negated: *negated,
                })
            }
            ExprKind::Like {
                left,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                let left = bind_streaming_column(left.require_column()?, schema, table, qualifier)?;
                if schema.columns[left].scalar_type != RelationalScalarType::Text {
                    return Err(SkeinError::Semantic(
                        "LIKE and ILIKE require a TEXT column".to_string(),
                    ));
                }
                let pattern =
                    value_to_relational(bind_sql_value(pattern.require_value()?, parameters)?)?;
                validate_value_type(schema, left, &pattern)?;
                Ok(Self::Like {
                    left,
                    pattern,
                    case_insensitive: *case_insensitive,
                    negated: *negated,
                    escape: *escape,
                })
            }
            ExprKind::IsNull {
                expression: column,
                negated,
            } => Ok(Self::IsNull {
                column: bind_streaming_column(column.require_column()?, schema, table, qualifier)?,
                negated: *negated,
            }),
            _ => Err(SkeinError::Semantic(
                "unsupported streaming predicate expression".to_owned(),
            )),
        }
    }

    fn truth<'a>(
        &self,
        resolve: &impl Fn(usize) -> Result<RelationalValueRef<'a>>,
    ) -> Result<Option<bool>> {
        match self {
            Self::And(left, right) => match left.truth(resolve)? {
                Some(false) => Ok(Some(false)),
                Some(true) => right.truth(resolve),
                None => match right.truth(resolve)? {
                    Some(false) => Ok(Some(false)),
                    Some(true) | None => Ok(None),
                },
            },
            Self::Or(left, right) => match left.truth(resolve)? {
                Some(true) => Ok(Some(true)),
                Some(false) => right.truth(resolve),
                None => match right.truth(resolve)? {
                    Some(true) => Ok(Some(true)),
                    Some(false) | None => Ok(None),
                },
            },
            Self::Not(predicate) => predicate
                .truth(resolve)
                .map(|truth| truth.map(|value| !value)),
            Self::CompareValue { left, op, right } => {
                compare_value_refs(resolve(*left)?, right.as_ref(), *op)
            }
            Self::CompareColumns { left, op, right } => {
                compare_value_refs(resolve(*left)?, resolve(*right)?, *op)
            }
            Self::InList {
                left,
                values,
                negated,
            } => {
                let left = resolve(*left)?;
                let mut has_unknown = false;
                for value in values {
                    match compare_value_refs(left, value.as_ref(), SqlComparisonOp::Eq)? {
                        Some(true) => return Ok(Some(!*negated)),
                        None => has_unknown = true,
                        Some(false) => {}
                    }
                }
                Ok(if has_unknown { None } else { Some(*negated) })
            }
            Self::Like {
                left,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => match (resolve(*left)?, pattern.as_ref()) {
                (RelationalValueRef::Null, _) | (_, RelationalValueRef::Null) => Ok(None),
                (RelationalValueRef::Text(value), RelationalValueRef::Text(pattern)) => {
                    let matched =
                        skein_sql::sql_like_matches(value, pattern, *escape, *case_insensitive)?;
                    Ok(Some(matched != *negated))
                }
                (RelationalValueRef::Overflow(_), _) => Err(SkeinError::Execution(
                    "LIKE reached an overflow value without hydration".to_string(),
                )),
                _ => Err(SkeinError::Semantic(
                    "LIKE and ILIKE require TEXT values".to_string(),
                )),
            },
            Self::IsNull { column, negated } => Ok(Some(
                matches!(resolve(*column)?, RelationalValueRef::Null) != *negated,
            )),
        }
    }
}

pub fn bind_streaming_column(
    column: &SqlColumnRef,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
) -> Result<usize> {
    if column
        .qualifier
        .as_deref()
        .is_some_and(|candidate| candidate != qualifier && candidate != table)
    {
        return Err(SkeinError::Semantic(format!(
            "column {} is unknown or ambiguous",
            column.name
        )));
    }
    schema.column_position(&column.name).ok_or_else(|| {
        SkeinError::Semantic(format!("column {} is unknown or ambiguous", column.name))
    })
}

fn validate_value_type(
    schema: &RelationalTableSchema,
    ordinal: usize,
    value: &RelationalValue,
) -> Result<()> {
    if value
        .scalar_type()
        .is_some_and(|scalar_type| scalar_type != schema.columns[ordinal].scalar_type)
    {
        return Err(SkeinError::Semantic(format!(
            "relational comparison on {} has an incompatible scalar type",
            schema.columns[ordinal].name
        )));
    }
    Ok(())
}
