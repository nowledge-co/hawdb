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
use crate::relational::{RelationalIndexRangeScan, RelationalIndexScanDirection};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn fixture() -> RelationalState {
    let column = |name: &str, nullable, scalar_type| RelationalColumnSchema {
        name: name.into(),
        nullable,
        scalar_type,
        default: None,
    };
    let parents = RelationalTableSchema {
        name: "parents".into(),
        columns: vec![
            column("id", false, RelationalScalarType::BigInt),
            column("alt", false, RelationalScalarType::BigInt),
        ],
        primary_key: vec!["id".into()],
        unique_constraints: vec![vec!["alt".into()]],
        foreign_keys: Vec::new(),
        indexes: vec![RelationalIndexSchema {
            name: "declared_alt".into(),
            columns: vec!["alt".into()],
            unique: true,
        }],
    };
    let children = RelationalTableSchema {
        name: "children".into(),
        columns: vec![
            column("id", false, RelationalScalarType::BigInt),
            column("group", false, RelationalScalarType::BigInt),
            column("parent", true, RelationalScalarType::BigInt),
            column("label", true, RelationalScalarType::Text),
        ],
        primary_key: vec!["id".into(), "group".into()],
        unique_constraints: vec![vec!["id".into()]],
        foreign_keys: vec![RelationalForeignKeySchema {
            columns: vec!["parent".into()],
            referenced_table: "parents".into(),
            referenced_columns: vec!["id".into()],
            on_delete: RelationalReferentialAction::Restrict,
            on_update: RelationalReferentialAction::Cascade,
        }],
        indexes: vec![
            RelationalIndexSchema {
                name: "group".into(),
                columns: vec!["group".into()],
                unique: false,
            },
            RelationalIndexSchema {
                name: "group_id".into(),
                columns: vec!["group".into(), "id".into()],
                unique: false,
            },
            RelationalIndexSchema {
                name: "label".into(),
                columns: vec!["label".into()],
                unique: true,
            },
        ],
    };
    let mut state = RelationalState::default();
    for schema in [children, parents] {
        crate::relational::validate_table_schema(&schema).unwrap();
        let positions = crate::relational::column_positions(&schema, &schema.primary_key).unwrap();
        let rows = (0..1025)
            .map(|id| {
                let values = if schema.name == "parents" {
                    vec![
                        RelationalValue::BigInt(id),
                        RelationalValue::BigInt(id + 10000),
                    ]
                } else {
                    vec![
                        RelationalValue::BigInt(id),
                        RelationalValue::BigInt(0),
                        if id % 7 == 0 {
                            RelationalValue::Null
                        } else {
                            RelationalValue::BigInt(id)
                        },
                        if id % 5 == 0 {
                            RelationalValue::Null
                        } else {
                            RelationalValue::Text(format!("label-{id}-界"))
                        },
                    ]
                };
                let row = RelationalRow::new(values);
                let key = crate::relational::row_key(&row, &positions);
                (key, row)
            })
            .collect();
        state.segments.insert(
            schema.name.clone(),
            Arc::new(RelationalTableSegment {
                rows: RelationalRowPages::from_map(rows),
                indexes: BTreeMap::new(),
            }),
        );
        state.schemas.insert(schema.name.clone(), Arc::new(schema));
    }
    crate::relational::rebuild_indexes(&mut state, "children").unwrap();
    crate::relational::rebuild_indexes(&mut state, "parents").unwrap();
    crate::relational::validate_foreign_keys(&state).unwrap();
    state
}

fn postings(index: &RelationalIndexPages) -> BTreeMap<RelationalKey, Vec<RelationalKey>> {
    index
        .pages
        .iter()
        .flat_map(|page| page.iter())
        .map(|(key, values)| (key.clone(), values.iter().cloned().collect()))
        .collect()
}

fn assert_complete(expected: &RelationalState, actual: &RelationalState) {
    assert_eq!(actual.schemas, expected.schemas);
    assert_eq!(
        actual.materialized_index_postings_resident,
        expected.materialized_index_postings_resident
    );
    for (name, expected) in &expected.segments {
        let actual = &actual.segments[name];
        assert_eq!(actual.rows.len(), 1025);
        assert!(
            actual.rows.iter().eq(expected.rows.iter()),
            "all rows must match: {name}"
        );
        assert_eq!(actual.indexes.len(), expected.indexes.len());
        for (name, expected) in &expected.indexes {
            let actual = &actual.indexes[name];
            assert!(
                postings(actual) == postings(expected),
                "all index postings must match: {name}"
            );
            for page in actual.pages.iter() {
                assert!(!page.is_empty() && page.len() <= RELATIONAL_INDEX_PAGE_MAX_KEYS);
                for keys in page.values() {
                    assert_eq!(keys.len, keys.iter().count());
                    assert!(keys
                        .pages
                        .iter()
                        .all(|page| !page.is_empty()
                            && page.len() <= RELATIONAL_POSTING_PAGE_MAX_KEYS));
                }
            }
        }
    }
}

#[test]
fn checkpoint_units_relational_runtime_rebuilds_all_postings_and_bidirectional_ranges() {
    let source = fixture();
    let mut actual = source.clone();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    for table in actual.schemas.keys() {
        rebuild_indexes(&actual.schemas, &mut actual.segments, table, &work).unwrap();
    }
    validate_foreign_keys(&actual, &work).unwrap();
    assert_complete(&source, &actual);
    assert!(Arc::ptr_eq(
        &actual.segments["children"].rows.pages,
        &source.segments["children"].rows.pages
    ));
    let child = &actual.segments["children"];
    assert!(child.indexes["group_id"].pages.len() > 1);
    let group = &child.indexes["group"];
    assert!(
        group
            .get(&RelationalKey(vec![RelationalValue::BigInt(0)]))
            .unwrap()
            .pages
            .len()
            > 1
    );
    let prefix = RelationalKey(vec![RelationalValue::BigInt(0)]);
    for direction in [
        RelationalIndexScanDirection::Forward,
        RelationalIndexScanDirection::Backward,
    ] {
        for exclusive_bound in [
            None,
            Some(RelationalKey(vec![
                RelationalValue::BigInt(0),
                RelationalValue::BigInt(513),
            ])),
        ] {
            let scan = RelationalIndexRangeScan {
                prefix: prefix.clone(),
                exclusive_bound,
                direction,
            };
            let mut expected = Vec::new();
            let mut found = Vec::new();
            source.segments["children"].indexes["group_id"].visit_range_entries(
                &scan,
                |index, key| {
                    expected.push((index.clone(), key.clone()));
                    true
                },
            );
            child.indexes["group_id"].visit_range_entries(&scan, |index, key| {
                found.push((index.clone(), key.clone()));
                true
            });
            assert!(
                found == expected,
                "range must preserve order and exclusive bound"
            );
            assert_eq!(
                found.len(),
                if scan.exclusive_bound.is_none() {
                    1025
                } else if direction == RelationalIndexScanDirection::Forward {
                    511
                } else {
                    513
                }
            );
        }
    }
    assert_eq!(child.indexes["group_id"].prefix_cardinality(&prefix), 1025);
    for limit in [0, 1, 255, 256, 257, 1025, 2048] {
        assert_eq!(
            child.indexes["group_id"].prefix_primary_keys(&prefix, limit),
            source.segments["children"].indexes["group_id"].prefix_primary_keys(&prefix, limit)
        );
    }
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_relational_runtime_wide_keys_and_row_page_boundaries_match_ordinary() {
    let key = RelationalKey(vec![
        RelationalValue::Null,
        RelationalValue::Boolean(true),
        RelationalValue::BigInt(-31),
        RelationalValue::DoublePrecision(1.5),
        RelationalValue::Text("界".repeat(50000)),
        RelationalValue::Bytea(vec![0; 131073]),
        RelationalValue::Uuid(Uuid::from_u128(31)),
        RelationalValue::Overflow(RelationalOverflowRef {
            digest: integrity_digest(b"unhydrated").sha256,
            scalar_type: RelationalScalarType::Bytea,
            compressed_bytes: 11,
            uncompressed_bytes: 999999,
        }),
    ]);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    assert!(clone_key(&key, &work).unwrap() == key);
    let row = RelationalRow::new(key.0.clone());
    let positions = vec![5, 4, 2, 7, 0, 6, 3, 1];
    assert!(
        row_key(&row, &positions, &work).unwrap() == crate::relational::row_key(&row, &positions)
    );
    let rows: BTreeMap<_, _> = (0..1025)
        .map(|id| {
            (
                RelationalKey(vec![RelationalValue::BigInt(id)]),
                RelationalRow::new(vec![
                    RelationalValue::BigInt(id),
                    RelationalValue::Bytea(vec![0; if id == 511 { 1024 * 1024 + 1 } else { 17 }]),
                ]),
            )
        })
        .collect();
    let expected = RelationalRowPages::from_map(rows.clone());
    let actual = row_pages(rows, &work).unwrap();
    assert_eq!(actual.len(), 1025);
    assert!(actual.iter().eq(expected.iter()));
    assert_eq!(actual.pages.len(), expected.pages.len());
    for (left, right) in actual.pages.iter().zip(expected.pages.iter()) {
        assert!(left == right);
    }
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_relational_runtime_preserves_unique_and_foreign_key_error_priority() {
    let source = fixture();
    let local = scheduler();
    for scenario in 0..10 {
        let mut state = source.clone();
        match scenario {
            0 => {
                Arc::make_mut(state.schemas.get_mut("children").unwrap()).foreign_keys[0]
                    .referenced_table = "missing".into();
            }
            1 => {
                Arc::make_mut(state.schemas.get_mut("children").unwrap()).foreign_keys[0].columns =
                    vec!["missing".into()];
            }
            2 => {
                Arc::make_mut(state.schemas.get_mut("children").unwrap()).foreign_keys[0]
                    .referenced_columns = vec!["missing".into()];
            }
            3 => {
                Arc::make_mut(state.schemas.get_mut("parents").unwrap()).primary_key =
                    vec!["alt".into()];
            }
            4 => {
                Arc::make_mut(state.schemas.get_mut("parents").unwrap()).columns[0].scalar_type =
                    RelationalScalarType::Text;
            }
            5 => {
                state.segments.remove("parents");
            }
            6 => {
                Arc::make_mut(state.segments.get_mut("parents").unwrap()).rows =
                    RelationalRowPages::default();
            }
            7 => {
                let schema = Arc::make_mut(state.schemas.get_mut("children").unwrap());
                schema.foreign_keys[0].columns = vec!["missing".into()];
                schema.foreign_keys[0].referenced_table = "missing".into();
            }
            8 => {
                let schema = Arc::make_mut(state.schemas.get_mut("parents").unwrap());
                schema.primary_key = vec!["alt".into()];
                schema.columns[0].scalar_type = RelationalScalarType::Text;
            }
            9 => {
                state.segments.remove("children");
                state.segments.remove("parents");
            }
            _ => unreachable!(),
        }
        let expected =
            crate::relational::validate_foreign_keys(&state).map_err(|error| error.to_string());
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = validate_foreign_keys(&state, &probe.context(local.clone()))
            .map_err(|error| error.to_string());
        assert_eq!(actual, expected, "foreign-key diagnostic: {scenario}");
        probe.assert_released(&local);
    }
    for role in ["unique", "declared"] {
        let mut state = source.clone();
        let segment = Arc::make_mut(state.segments.get_mut("children").unwrap());
        let key = RelationalKey(vec![
            RelationalValue::BigInt(1024),
            RelationalValue::BigInt(0),
        ]);
        let mut values = segment.rows.get(&key).unwrap().values().to_vec();
        if role == "unique" {
            values[0] = RelationalValue::BigInt(1023);
        } else {
            values[3] = RelationalValue::Text("label-1023-界".into());
        }
        segment.rows.insert(key, RelationalRow::new(values));
        let mut expected = state.clone();
        let error = crate::relational::rebuild_indexes(&mut expected, "children").unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = rebuild_indexes(
            &state.schemas,
            &mut state.segments,
            "children",
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert_eq!(
            actual.to_string(),
            error.to_string(),
            "unique diagnostic: {role}"
        );
        probe.assert_released(&local);
    }
    assert_eq!(source.row_count("children"), 1025);
}

fn phase(
    state: &mut RelationalState,
    phase: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalError> {
    match phase {
        0 => {
            let rows = state.segments["children"]
                .rows
                .iter()
                .map(|(key, row)| (key.clone(), row.clone()))
                .collect();
            row_pages(rows, work).map(|_| ())
        }
        1 => rebuild_indexes(&state.schemas, &mut state.segments, "children", work),
        2 => validate_foreign_keys(state, work),
        _ => unreachable!(),
    }
}

#[test]
fn checkpoint_units_relational_runtime_cancels_reconstruction_then_retries_complete_source() {
    let source = fixture();
    for id in 0..3 {
        let local = scheduler();
        let reference = Arc::new(CheckpointWorkProbe::default());
        phase(&mut source.clone(), id, &reference.context(local.clone())).unwrap();
        let total = reference.completed.load(Ordering::SeqCst);
        assert!(total > 1000);
        reference.assert_released(&local);
        for cancel_at in [1, 17, 256, 257, total / 2, total - 1, total] {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(cancel_at, Ordering::SeqCst);
            let error = phase(&mut source.clone(), id, &probe.context(local.clone())).unwrap_err();
            assert!(
                matches!(error,RelationalError::Admission(ref text) if text.contains("stopped")),
                "{error:?}"
            );
            probe.assert_released(&local);
            let retry = Arc::new(CheckpointWorkProbe::default());
            let mut actual = source.clone();
            phase(&mut actual, id, &retry.context(local.clone())).unwrap();
            assert_complete(&source, &actual);
            retry.assert_released(&local);
        }
    }
}

#[test]
fn checkpoint_units_relational_runtime_denies_before_work_then_retries_all_rows() {
    let source = fixture();
    for id in 0..3 {
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let error = phase(&mut source.clone(), id, &work).unwrap_err();
        assert!(
            matches!(error,RelationalError::Admission(ref text) if text.contains("admission deferred")),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        let retry = Arc::new(CheckpointWorkProbe::default());
        let mut actual = source.clone();
        phase(&mut actual, id, &retry.context(local.clone())).unwrap();
        assert_complete(&source, &actual);
        retry.assert_released(&local);
    }
}

struct CheckpointFile(std::path::PathBuf);
impl Drop for CheckpointFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn checkpoint_units_relational_runtime_mounts_complete_primary_unique_and_declared_foreign_targets()
{
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    for target in 0..3 {
        let mut source = fixture();
        if target > 0 {
            Arc::make_mut(source.schemas.get_mut("children").unwrap()).foreign_keys[0]
                .referenced_columns = vec!["alt".into()];
            let rows = source.segments["children"]
                .rows
                .iter()
                .map(|(key, row)| {
                    let mut values = row.values().to_vec();
                    if let RelationalValue::BigInt(id) = &mut values[2] {
                        *id += 10000;
                    }
                    (key.clone(), RelationalRow::new(values))
                })
                .collect();
            Arc::make_mut(source.segments.get_mut("children").unwrap()).rows =
                RelationalRowPages::from_map(rows);
            if target == 2 {
                Arc::make_mut(source.schemas.get_mut("parents").unwrap())
                    .unique_constraints
                    .clear();
            }
            crate::relational::rebuild_indexes(&mut source, "children").unwrap();
            crate::relational::rebuild_indexes(&mut source, "parents").unwrap();
            crate::relational::validate_foreign_keys(&source).unwrap();
        }
        let bytes = encode_relational_checkpoint(31, &source).unwrap();
        let file = CheckpointFile(std::env::temp_dir().join(format!(
            "hawdb-controlled-relational-runtime-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        )));
        std::fs::write(&file.0, &bytes).unwrap();
        for load in [
            RelationalCheckpointIndexLoad::MaterializedPostings,
            RelationalCheckpointIndexLoad::OmitMaterializedPostings,
        ] {
            let expected = decode_relational_checkpoint_file_with_index_load(
                &file.0,
                RelationalDecodeLimits::checkpoint(),
                load,
            )
            .unwrap();
            let local = scheduler();
            let probe = Arc::new(CheckpointWorkProbe::default());
            let actual = decode_relational_checkpoint_file_with_work_context(
                &file.0,
                RelationalDecodeLimits::checkpoint(),
                load,
                &probe.context(local.clone()),
            )
            .unwrap();
            assert_eq!(actual.epoch, 31);
            assert_complete(&expected.state, &actual.state);
            for table in ["children", "parents"] {
                assert!(source.segments[table]
                    .rows
                    .iter()
                    .eq(actual.state.segments[table].rows.iter()));
            }
            assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
            probe.assert_released(&local);
        }
        assert_eq!(std::fs::read(&file.0).unwrap(), bytes);
    }
}
