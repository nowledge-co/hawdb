use super::relational_ref_to_value;
use crate::error::{Result, SkeinError};
use crate::relational_sql::row_access::RelationalReadRowRef;
use crate::sql::{Expr, ExprKind, SelectProjection};
use crate::value::Value;
use skein_executor::{QueryRowsBuilder, QuerySchema};
use skein_relational::predicate::bind_streaming_column;
use skein_storage::RelationalTableSchema;

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
                SelectProjection::Expression {
                    expression:
                        Expr {
                            kind: ExprKind::Column(name),
                            ..
                        },
                    alias,
                    ..
                } => {
                    columns.push(BoundStreamingColumn {
                        value: BoundStreamingValue::Column(bind_streaming_column(
                            name, schema, table, qualifier,
                        )?),
                        output_name: alias.clone().unwrap_or_else(|| name.name.clone()),
                    });
                }
                SelectProjection::Expression { expression, alias } => match expression {
                    Expr {
                        kind:
                            ExprKind::Function {
                                name,
                                arguments,
                                distinct: false,
                                filter: None,
                            },
                        ..
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
                BoundStreamingValue::UuidV7 => Value::Uuid(skein_core::generate_uuidv7()?),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SqlStatement;
    use skein_relational::predicate::BoundStreamingPredicate;
    use skein_storage::{
        RelationalColumnSchema, RelationalKey, RelationalProjectedField, RelationalProjectedRow,
        RelationalScalarType, RelationalValue,
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
        assert_eq!(
            and.truth_with(&|ordinal| row.value(ordinal)).unwrap(),
            Some(false)
        );

        let or = bind_predicate(
            "SELECT id FROM logic_rows WHERE flag = FALSE OR body = 'unused'",
            &schema,
        );
        assert_eq!(
            or.truth_with(&|ordinal| row.value(ordinal)).unwrap(),
            Some(true)
        );
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
