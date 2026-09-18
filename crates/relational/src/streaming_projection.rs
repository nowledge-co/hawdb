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

//! Borrowed-row streaming projection owned by the relational runtime.

use crate::predicate::bind_streaming_column;
use crate::query_value::relational_ref_to_value;
use crate::row_runtime::RelationalReadRowRef;
use hawdb_core::{HawDBError, Result, Value};
use hawdb_executor::{QueryRowsBuilder, QuerySchema};
use hawdb_sql::{Expr, ExprKind, SelectProjection};
use hawdb_storage::RelationalTableSchema;

#[doc(hidden)]
pub struct BoundStreamingProjection {
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
    pub fn bind(
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
                        return Err(HawDBError::Semantic(
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
            return Err(HawDBError::Semantic(format!(
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

    pub fn schema(&self) -> &QuerySchema {
        &self.schema
    }

    pub fn project_into(
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
                BoundStreamingValue::UuidV7 => Value::Uuid(hawdb_core::generate_uuidv7()?),
            };
            *payload_bytes =
                payload_bytes.saturating_add(hawdb_executor::query_value_payload_bytes(&value));
            if *payload_bytes > max_payload_bytes {
                return Err(HawDBError::Execution(format!(
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
    use hawdb_executor::Row;
    use hawdb_sql::SqlStatement;
    use hawdb_storage::{
        RelationalColumnSchema, RelationalKey, RelationalProjectedField, RelationalProjectedRow,
        RelationalScalarType, RelationalValue,
    };

    #[test]
    fn projection_uses_the_schema_once_for_borrowed_rows() {
        let projection = projection("SELECT id AS external_id, body FROM records");
        let projection = BoundStreamingProjection::bind(&projection, &schema(), "records", "r")
            .expect("bind streaming projection");
        let row = RelationalProjectedRow {
            primary_key: RelationalKey(vec![RelationalValue::Text("row-1".to_string())]),
            fields: vec![
                RelationalProjectedField {
                    ordinal: 0,
                    value: RelationalValue::Text("row-1".to_string()),
                },
                RelationalProjectedField {
                    ordinal: 1,
                    value: RelationalValue::Text("body".to_string()),
                },
            ],
        };
        let mut output = QueryRowsBuilder::with_schema(projection.schema().clone(), 1);
        let mut payload_bytes = 0;

        projection
            .project_into(
                RelationalReadRowRef::from_projected(&row),
                &mut output,
                &mut payload_bytes,
                usize::MAX,
            )
            .expect("project borrowed row");

        assert_eq!(
            output.finish().into_rows(),
            vec![Row::from([
                (
                    "external_id".to_string(),
                    Value::String("row-1".to_string())
                ),
                ("body".to_string(), Value::String("body".to_string())),
            ])]
        );
        assert_eq!(
            payload_bytes,
            "external_id".len() + "body".len() + "row-1".len() + "body".len()
        );
    }

    #[test]
    fn projection_rolls_back_the_payload_counter_when_the_output_budget_is_exceeded() {
        let projection = projection("SELECT id FROM records");
        let projection = BoundStreamingProjection::bind(&projection, &schema(), "records", "r")
            .expect("bind streaming projection");
        let row = RelationalProjectedRow {
            primary_key: RelationalKey(vec![RelationalValue::Text("row-1".to_string())]),
            fields: vec![RelationalProjectedField {
                ordinal: 0,
                value: RelationalValue::Text("row-1".to_string()),
            }],
        };
        let mut output = QueryRowsBuilder::with_schema(projection.schema().clone(), 1);
        let mut payload_bytes = 0;

        let error = projection
            .project_into(
                RelationalReadRowRef::from_projected(&row),
                &mut output,
                &mut payload_bytes,
                1,
            )
            .expect_err("payload cap must reject the row");

        assert_eq!(
            error,
            HawDBError::Execution(
                "relational SQL output exceeds max_output_payload_bytes 1".to_string()
            )
        );
        assert_eq!(payload_bytes, 0);
        assert!(output.finish().is_empty());
    }

    #[test]
    fn projection_rejects_duplicate_output_columns() {
        let projection = projection("SELECT id AS value, body AS value FROM records");

        let error = match BoundStreamingProjection::bind(&projection, &schema(), "records", "r") {
            Ok(_) => panic!("duplicate output columns must fail during binding"),
            Err(error) => error,
        };
        assert_eq!(
            error,
            HawDBError::Semantic(
                "relational projection contains duplicate output column value".to_string()
            )
        );
    }

    fn projection(source: &str) -> Vec<SelectProjection> {
        let prepared = hawdb_sql::prepare_postgres_sql(source).expect("parse select");
        let SqlStatement::Select(select) = prepared.statement else {
            panic!("expected SELECT");
        };
        select.projection
    }

    fn schema() -> RelationalTableSchema {
        RelationalTableSchema {
            name: "records".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "id".to_string(),
                    scalar_type: RelationalScalarType::Text,
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
