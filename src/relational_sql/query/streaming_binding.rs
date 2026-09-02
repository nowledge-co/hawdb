use super::{bind_sql_value, compare_value_refs, relational_ref_to_value, value_to_relational};
use crate::error::{Result, SkeinError};
use crate::relational_sql::row_access::RelationalReadRowRef;
use crate::sql::{
    SelectProjection, SqlColumnRef, SqlComparisonOp, SqlExpression, SqlLikeEscape, SqlPredicate,
};
use crate::value::Value;
use skein_executor::{QueryRowsBuilder, QuerySchema};
use skein_storage::{
    RelationalScalarType, RelationalTableSchema, RelationalValue, RelationalValueRef,
};

pub(super) enum BoundStreamingPredicate {
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

impl BoundStreamingPredicate {
    pub(super) fn bind(
        predicate: &SqlPredicate,
        parameters: &[Value],
        schema: &RelationalTableSchema,
        table: &str,
        qualifier: &str,
    ) -> Result<Self> {
        match predicate {
            SqlPredicate::And(left, right) => Ok(Self::And(
                Box::new(Self::bind(left, parameters, schema, table, qualifier)?),
                Box::new(Self::bind(right, parameters, schema, table, qualifier)?),
            )),
            SqlPredicate::Or(left, right) => Ok(Self::Or(
                Box::new(Self::bind(left, parameters, schema, table, qualifier)?),
                Box::new(Self::bind(right, parameters, schema, table, qualifier)?),
            )),
            SqlPredicate::Not(predicate) => Ok(Self::Not(Box::new(Self::bind(
                predicate, parameters, schema, table, qualifier,
            )?))),
            SqlPredicate::Compare { left, op, right } => {
                let left = bind_column(left, schema, table, qualifier)?;
                let right = value_to_relational(bind_sql_value(right, parameters)?)?;
                validate_value_type(schema, left, &right)?;
                Ok(Self::CompareValue {
                    left,
                    op: *op,
                    right,
                })
            }
            SqlPredicate::CompareColumns { left, op, right } => {
                let left = bind_column(left, schema, table, qualifier)?;
                let right = bind_column(right, schema, table, qualifier)?;
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
            SqlPredicate::InList {
                left,
                values,
                negated,
            } => {
                let left = bind_column(left, schema, table, qualifier)?;
                let values = values
                    .iter()
                    .map(|value| {
                        let value = value_to_relational(bind_sql_value(value, parameters)?)?;
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
            SqlPredicate::Like {
                left,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                let left = bind_column(left, schema, table, qualifier)?;
                if schema.columns[left].scalar_type != RelationalScalarType::Text {
                    return Err(SkeinError::Semantic(
                        "LIKE and ILIKE require a TEXT column".to_string(),
                    ));
                }
                let pattern = value_to_relational(bind_sql_value(pattern, parameters)?)?;
                validate_value_type(schema, left, &pattern)?;
                Ok(Self::Like {
                    left,
                    pattern,
                    case_insensitive: *case_insensitive,
                    negated: *negated,
                    escape: *escape,
                })
            }
            SqlPredicate::IsNull { column, negated } => Ok(Self::IsNull {
                column: bind_column(column, schema, table, qualifier)?,
                negated: *negated,
            }),
        }
    }

    pub(super) fn truth(&self, row: RelationalReadRowRef<'_>) -> Result<Option<bool>> {
        match self {
            Self::And(left, right) => match left.truth(row)? {
                Some(false) => Ok(Some(false)),
                Some(true) => right.truth(row),
                None => match right.truth(row)? {
                    Some(false) => Ok(Some(false)),
                    Some(true) | None => Ok(None),
                },
            },
            Self::Or(left, right) => match left.truth(row)? {
                Some(true) => Ok(Some(true)),
                Some(false) => right.truth(row),
                None => match right.truth(row)? {
                    Some(true) => Ok(Some(true)),
                    Some(false) | None => Ok(None),
                },
            },
            Self::Not(predicate) => predicate.truth(row).map(|truth| truth.map(|value| !value)),
            Self::CompareValue { left, op, right } => {
                compare_value_refs(row.value(*left)?, right.as_ref(), *op)
            }
            Self::CompareColumns { left, op, right } => {
                compare_value_refs(row.value(*left)?, row.value(*right)?, *op)
            }
            Self::InList {
                left,
                values,
                negated,
            } => {
                let left = row.value(*left)?;
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
            } => match (row.value(*left)?, pattern.as_ref()) {
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
                matches!(row.value(*column)?, RelationalValueRef::Null) != *negated,
            )),
        }
    }
}

pub(super) struct BoundStreamingProjection {
    columns: Box<[BoundStreamingColumn]>,
    schema: QuerySchema,
    row_name_bytes: usize,
}

struct BoundStreamingColumn {
    value: BoundStreamingValue,
    output_name: String,
}

#[derive(Clone, Copy)]
enum BoundStreamingValue {
    Column(usize),
    UuidV7,
}

impl BoundStreamingProjection {
    pub(super) fn bind(
        projection: &[SelectProjection],
        schema: &RelationalTableSchema,
        table: &str,
        qualifier: &str,
    ) -> Result<Self> {
        let mut columns = Vec::new();
        for item in projection {
            match item {
                SelectProjection::Wildcard => {
                    columns.extend(schema.columns.iter().enumerate().map(|(ordinal, column)| {
                        BoundStreamingColumn {
                            value: BoundStreamingValue::Column(ordinal),
                            output_name: column.name.clone(),
                        }
                    }));
                }
                SelectProjection::Column { name, alias } => {
                    columns.push(BoundStreamingColumn {
                        value: BoundStreamingValue::Column(bind_column(
                            name, schema, table, qualifier,
                        )?),
                        output_name: alias.clone().unwrap_or_else(|| name.name.clone()),
                    });
                }
                SelectProjection::Expression { expression, alias } => match expression {
                    SqlExpression::Function {
                        name,
                        arguments,
                        distinct: false,
                        filter: None,
                    } if name == "uuidv7" && arguments.is_empty() => {
                        columns.push(BoundStreamingColumn {
                            value: BoundStreamingValue::UuidV7,
                            output_name: alias.clone().unwrap_or_else(|| name.clone()),
                        });
                    }
                    _ => {
                        return Err(SkeinError::Semantic(
                            "non-aggregate relational projection expressions are not supported"
                                .to_string(),
                        ));
                    }
                },
            }
        }
        let mut names = std::collections::BTreeSet::new();
        if let Some(duplicate) = columns
            .iter()
            .find(|column| !names.insert(column.output_name.as_str()))
        {
            return Err(SkeinError::Semantic(format!(
                "relational projection contains duplicate output column {}",
                duplicate.output_name
            )));
        }
        let schema = QuerySchema::try_new(columns.iter().map(|column| column.output_name.clone()))?;
        let row_name_bytes = columns.iter().fold(0usize, |total, column| {
            total.saturating_add(column.output_name.len())
        });
        Ok(Self {
            columns: columns.into_boxed_slice(),
            schema,
            row_name_bytes,
        })
    }

    pub(super) fn schema(&self) -> &QuerySchema {
        &self.schema
    }

    pub(super) fn project_into(
        &self,
        row: RelationalReadRowRef<'_>,
        output: &mut QueryRowsBuilder,
        payload_bytes: &mut usize,
        max_payload_bytes: usize,
    ) -> Result<()> {
        let previous_payload_bytes = *payload_bytes;
        *payload_bytes = payload_bytes.saturating_add(self.row_name_bytes);
        let result = output.try_push_values(self.columns.iter().map(|column| {
            let value = match column.value {
                BoundStreamingValue::Column(ordinal) => {
                    relational_ref_to_value(row.value(ordinal)?)?
                }
                BoundStreamingValue::UuidV7 => {
                    Value::Uuid(super::super::uuidv7::generate_uuidv7()?)
                }
            };
            *payload_bytes =
                payload_bytes.saturating_add(skein_executor::query_value_payload_bytes(&value));
            if *payload_bytes > max_payload_bytes {
                return Err(SkeinError::Execution(format!(
                    "relational SQL output exceeds max_output_payload_bytes {max_payload_bytes}"
                )));
            }
            Ok(value)
        }));
        if result.is_err() {
            *payload_bytes = previous_payload_bytes;
        }
        result
    }
}

fn bind_column(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SqlStatement;
    use skein_storage::{
        RelationalColumnSchema, RelationalKey, RelationalProjectedField, RelationalProjectedRow,
        RelationalScalarType,
    };

    #[test]
    fn boolean_short_circuit_does_not_read_the_unneeded_ordinal() {
        let schema = schema();
        let row = RelationalProjectedRow {
            primary_key: RelationalKey(vec![RelationalValue::Text("row-1".to_string())]),
            fields: vec![RelationalProjectedField {
                ordinal: 1,
                value: RelationalValue::Boolean(false),
            }],
        };
        let row = RelationalReadRowRef::from_projected(&row);

        let and = bind_predicate(
            "SELECT id FROM logic_rows WHERE flag = TRUE AND body = 'unused'",
            &schema,
        );
        assert_eq!(and.truth(row).unwrap(), Some(false));

        let or = bind_predicate(
            "SELECT id FROM logic_rows WHERE flag = FALSE OR body = 'unused'",
            &schema,
        );
        assert_eq!(or.truth(row).unwrap(), Some(true));
    }

    fn bind_predicate(sql: &str, schema: &RelationalTableSchema) -> BoundStreamingPredicate {
        let prepared = skein_sql::prepare_postgres_sql(sql).unwrap();
        let SqlStatement::Select(select) = prepared.statement else {
            panic!("expected SELECT")
        };
        BoundStreamingPredicate::bind(
            select.selection.as_ref().expect("selection"),
            &[],
            schema,
            "logic_rows",
            "logic_rows",
        )
        .unwrap()
    }

    fn schema() -> RelationalTableSchema {
        RelationalTableSchema {
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
        }
    }
}
