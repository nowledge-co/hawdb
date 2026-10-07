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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

fn schema() -> RelationalTableSchema {
    let mut schema = RelationalTableSchema {
        name: "table-界".repeat(10000),
        columns: vec![RelationalColumnSchema {
            name: "id".into(),
            scalar_type: RelationalScalarType::BigInt,
            nullable: false,
            default: None,
        }],
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
    };
    for id in 1..1025 {
        let name = if id == 1024 {
            "column-界".repeat(10000)
        } else {
            format!("column-{id}-界")
        };
        let (scalar_type, default) = match id % 6 {
            0 => (
                RelationalScalarType::Boolean,
                RelationalValue::Boolean(true),
            ),
            1 => (
                RelationalScalarType::BigInt,
                RelationalValue::BigInt(id as i64),
            ),
            2 => (
                RelationalScalarType::DoublePrecision,
                RelationalValue::DoublePrecision(id as f64 / 3.0),
            ),
            3 => (
                RelationalScalarType::Text,
                RelationalValue::Text("value-界".repeat(if id == 1023 { 10000 } else { 1 })),
            ),
            4 => (
                RelationalScalarType::Bytea,
                RelationalValue::Bytea(vec![0; if id == 1024 { 65537 } else { 3 }]),
            ),
            5 => (
                RelationalScalarType::Uuid,
                RelationalValue::Uuid(Uuid::from_u128(id as u128)),
            ),
            _ => unreachable!(),
        };
        schema.columns.push(RelationalColumnSchema {
            name: name.clone(),
            scalar_type,
            nullable: true,
            default: Some(RelationalColumnDefault::Literal(default)),
        });
        schema.unique_constraints.push(vec![name.clone()]);
        schema.foreign_keys.push(RelationalForeignKeySchema {
            columns: vec![name.clone()],
            referenced_table: "parents".into(),
            referenced_columns: vec!["id".into()],
            on_delete: RelationalReferentialAction::Restrict,
            on_update: RelationalReferentialAction::Cascade,
        });
        schema.indexes.push(RelationalIndexSchema {
            name: format!("index-{id}-界"),
            columns: vec![name],
            unique: id % 2 == 0,
        });
    }
    schema
}

fn row(schema: &RelationalTableSchema) -> RelationalRow {
    RelationalRow::new(
        schema
            .columns
            .iter()
            .enumerate()
            .map(|(id, column)| {
                if id == 0 {
                    RelationalValue::BigInt(31)
                } else if id % 7 == 0 {
                    RelationalValue::Null
                } else if let Some(RelationalColumnDefault::Literal(value)) = &column.default {
                    value.clone()
                } else {
                    unreachable!()
                }
            })
            .collect(),
    )
}

#[test]
fn checkpoint_units_relational_validation_wide_schema_positions_and_row_match_ordinary() {
    let schema = schema();
    let row = row(&schema);
    crate::relational::validate_table_schema(&schema).unwrap();
    crate::relational::validate_row(&schema, &row).unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let positions = validate_table_schema_with_work_context(&schema, &work).unwrap();
    assert_eq!(positions.len(), schema.columns.len());
    for (id, column) in schema.columns.iter().enumerate() {
        let (name, position) = positions.get_key_value(column.name.as_str()).unwrap();
        assert_eq!(*position, id);
        assert!(
            std::ptr::eq(name.as_ptr(), column.name.as_ptr()),
            "position map must borrow the original column name"
        );
    }
    assert_eq!(
        primary_key_positions_with_work_context(&schema, &positions, &work).unwrap(),
        crate::relational::column_positions(&schema, &schema.primary_key).unwrap()
    );
    validate_row_with_work_context(&schema, &row, &work).unwrap();
    assert!(probe.completed.load(Ordering::SeqCst) > 10000);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_relational_validation_preserves_schema_and_row_error_priority() {
    let valid = schema();
    let local = scheduler();
    for scenario in 0..23 {
        let mut schema = valid.clone();
        match scenario {
            0 => schema.name.clear(),
            1 => schema.columns.clear(),
            2 => schema.primary_key.clear(),
            3 => schema.columns[1].name.clear(),
            4 => schema.columns[1].name = schema.columns[0].name.clone(),
            5 => {
                schema.columns[0].default =
                    Some(RelationalColumnDefault::Literal(RelationalValue::Null))
            }
            6 => schema.columns[1].default = Some(RelationalColumnDefault::UuidV7),
            7 => schema.primary_key = vec!["missing".into()],
            8 => schema.primary_key.push("id".into()),
            9 => schema.columns[0].nullable = true,
            10 => schema.unique_constraints[0].clear(),
            11 => schema.unique_constraints[0] = vec!["missing".into()],
            12 => schema.foreign_keys[0].columns.clear(),
            13 => schema.foreign_keys[0].referenced_columns.clear(),
            14 => schema.indexes[0].name.clear(),
            15 => schema.indexes[0].name = crate::relational::RELATIONAL_PRIMARY_INDEX_NAME.into(),
            16 => schema.indexes[1].name = schema.indexes[0].name.clone(),
            17 => schema.indexes[0].columns.clear(),
            18 => schema.indexes[0].columns = vec!["id".into(), "id".into()],
            19 => {
                schema.primary_key = vec!["missing".into()];
                schema.columns[0].default =
                    Some(RelationalColumnDefault::Literal(RelationalValue::Null));
            }
            20 => {
                schema.columns[1].name = "id".into();
                schema.columns[1].default = Some(RelationalColumnDefault::UuidV7);
            }
            21 => {
                schema.unique_constraints[0].clear();
                schema.foreign_keys[0].referenced_columns.clear();
            }
            22 => {
                schema.foreign_keys[0].referenced_columns.clear();
                schema.indexes[0].name.clear();
            }
            _ => unreachable!(),
        }
        let expected = crate::relational::validate_table_schema(&schema).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual =
            validate_table_schema_with_work_context(&schema, &probe.context(local.clone()))
                .unwrap_err();
        assert!(
            actual == expected,
            "schema diagnostic differs: scenario={scenario}"
        );
        probe.assert_released(&local);
    }
    let valid_row = row(&valid);
    for scenario in 0..6 {
        let mut values = valid_row.values().to_vec();
        match scenario {
            0 => {
                values.pop();
            }
            1 => values.push(RelationalValue::Null),
            2 => values[0] = RelationalValue::Null,
            3 => values[0] = RelationalValue::Text("wrong id".into()),
            4 => values[1] = RelationalValue::Bytea(vec![1]),
            5 => {
                values[0] = RelationalValue::Null;
                values[1] = RelationalValue::Bytea(vec![1]);
            }
            _ => unreachable!(),
        }
        let row = RelationalRow::new(values);
        let expected = crate::relational::validate_row(&valid, &row).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = validate_row_with_work_context(&valid, &row, &probe.context(local.clone()))
            .unwrap_err();
        assert!(
            actual == expected,
            "row diagnostic differs: scenario={scenario}"
        );
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_relational_validation_cancels_schema_and_row_inner_and_final_then_retries() {
    let schema = schema();
    let row = row(&schema);
    let local = scheduler();
    for phase in 0..2 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        if phase == 0 {
            validate_table_schema_with_work_context(&schema, &work).unwrap();
        } else {
            validate_row_with_work_context(&schema, &row, &work).unwrap();
        }
        let units = probe.completed.load(Ordering::SeqCst);
        probe.assert_released(&local);
        for limit in [1, 17, 511, 1025, units / 2, units - 1, units] {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(limit, Ordering::SeqCst);
            let work = probe.context(local.clone());
            let error = if phase == 0 {
                validate_table_schema_with_work_context(&schema, &work).unwrap_err()
            } else {
                validate_row_with_work_context(&schema, &row, &work).unwrap_err()
            };
            assert!(
                matches!(error,RelationalError::Admission(ref text) if text.contains("cancelled")),
                "{error:?}"
            );
            assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
            probe.assert_released(&local);
        }
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        if phase == 0 {
            assert_eq!(
                validate_table_schema_with_work_context(&schema, &work)
                    .unwrap()
                    .len(),
                1025
            );
        } else {
            validate_row_with_work_context(&schema, &row, &work).unwrap();
        }
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_relational_validation_denies_before_work_and_retries_after_release() {
    let schema = schema();
    let row = row(&schema);
    for phase in 0..2 {
        let local = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(4),
            max_background_operations_by_class: [Some(1); hawdb_qos::WORK_CLASS_COUNT],
            ..LocalQosPolicy::default()
        });
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(hawdb_qos::WorkRequest::background(
                hawdb_qos::WorkClass::Mutation,
                1,
            ))
            .unwrap();
        let error = if phase == 0 {
            validate_table_schema_with_work_context(&schema, &work).unwrap_err()
        } else {
            validate_row_with_work_context(&schema, &row, &work).unwrap_err()
        };
        assert!(
            matches!(error, RelationalError::Admission(ref text) if text.contains("admission deferred")),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        let retry = Arc::new(CheckpointWorkProbe::default());
        let work = retry.context(local.clone());
        if phase == 0 {
            assert_eq!(
                validate_table_schema_with_work_context(&schema, &work)
                    .unwrap()
                    .len(),
                1025
            );
        } else {
            validate_row_with_work_context(&schema, &row, &work).unwrap();
        }
        retry.assert_released(&local);
    }
}
