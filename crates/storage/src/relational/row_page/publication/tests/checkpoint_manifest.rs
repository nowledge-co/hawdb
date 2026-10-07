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
use crate::relational::{
    RelationalColumnDefault, RelationalForeignKeySchema, RelationalIndexSchema,
    RelationalReferentialAction, RelationalScalarType, RelationalTableSchema, Uuid,
};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::{atomic::Ordering, Arc};

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn metadata(len: u64) -> RelationalRowPageArtifactMetadata {
    let digest = hawdb_integrity::integrity_digest(&[]);
    RelationalRowPageArtifactMetadata {
        encoded_len: len,
        encoded_crc32c: digest.crc32c.get(),
        encoded_sha256: digest.sha256,
    }
}

fn empty_manifest(count: usize) -> RelationalRowPageRootManifest {
    let tables: Vec<_> = (0..count)
        .map(|ordinal| {
            let table = format!("documents-{ordinal:04}");
            let schema = crate::relational::row_page::test_row_page_schema(&table, 2);
            let schema_digest =
                crate::relational::index_shadow::relational_schema_digest(&schema).unwrap();
            RelationalRowPageTableRoot {
                table,
                schema,
                schema_digest,
                column_count: NonZeroU32::new(2).unwrap(),
                row_count: 0,
                next_page_id: NonZeroU64::MIN,
                first_descriptor: 0,
                page_count: 0,
                lower_bound: Vec::new(),
                upper_bound: Vec::new(),
            }
        })
        .collect();
    RelationalRowPageRootManifest {
        generation: 1,
        source_commit_epoch: 10,
        previous_generation: None,
        page_bytes: RelationalRowPageLimits::default().max_page_bytes.get() as u64,
        dirty_page_count: 0,
        relocated_page_count: 0,
        root_page_count: 0,
        page_artifact: metadata(0),
        root_descriptor_artifact: metadata(0),
        root_key_artifact: metadata(0),
        root_set_digest: manifest::root_set_digest(&tables).unwrap(),
        overflow_root: None,
        tables,
        physical_generations: Vec::new(),
    }
}

pub(super) fn schema() -> RelationalTableSchema {
    let mut schema = crate::relational::row_page::test_row_page_schema("documents", 1025);
    let defaults = [
        (
            RelationalScalarType::Boolean,
            RelationalValue::Boolean(true),
        ),
        (
            RelationalScalarType::BigInt,
            RelationalValue::BigInt(i64::MIN),
        ),
        (
            RelationalScalarType::DoublePrecision,
            RelationalValue::DoublePrecision(f64::from_bits(0xfff8_0000_0000_0011)),
        ),
        (
            RelationalScalarType::Text,
            RelationalValue::Text("界\0text".repeat(12 * 1024 + 3)),
        ),
        (
            RelationalScalarType::Bytea,
            RelationalValue::Bytea(vec![0; 128 * 1024 + 1]),
        ),
        (
            RelationalScalarType::Uuid,
            RelationalValue::Uuid(Uuid::from_bytes([7; 16])),
        ),
        (RelationalScalarType::Text, RelationalValue::Null),
        (
            RelationalScalarType::DoublePrecision,
            RelationalValue::DoublePrecision(-0.0),
        ),
    ];
    for (column, (scalar_type, value)) in schema.columns.iter_mut().skip(1).zip(defaults) {
        column.scalar_type = scalar_type;
        column.default = Some(RelationalColumnDefault::Literal(value));
    }
    schema.columns[9].scalar_type = RelationalScalarType::Uuid;
    schema.columns[9].default = Some(RelationalColumnDefault::UuidV7);
    for ordinal in 0..1025 {
        schema.unique_constraints.push(vec!["id".into()]);
        let actions = [
            RelationalReferentialAction::NoAction,
            RelationalReferentialAction::Restrict,
            RelationalReferentialAction::Cascade,
        ];
        schema.foreign_keys.push(RelationalForeignKeySchema {
            columns: vec!["id".into()],
            referenced_table: "documents".into(),
            referenced_columns: vec!["id".into()],
            on_delete: actions[ordinal % 3],
            on_update: actions[(ordinal + 1) % 3],
        });
        schema.indexes.push(RelationalIndexSchema {
            name: format!("index-{ordinal:04}"),
            columns: vec!["id".into()],
            unique: ordinal % 2 == 0,
        });
    }
    schema
}

#[test]
fn checkpoint_units_row_manifest_preserves_all_1025_tables_and_schema_items_bytes_and_values() {
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let schema = schema();
    let before = schema.clone();
    let expected = crate::relational::codec::encode_relational_table_schema(&schema).unwrap();
    let actual =
        crate::relational::codec::encode_relational_table_schema_with_work_context(&schema, &work)
            .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        crate::relational::codec::decode_relational_table_schema(&actual, actual.len(), 4096)
            .unwrap(),
        schema
    );
    assert_eq!(
        crate::relational::index_shadow::relational_schema_digest_with_work_context(&schema, &work)
            .unwrap(),
        crate::relational::index_shadow::relational_schema_digest(&schema).unwrap()
    );
    assert_eq!(schema, before);
    let mut root = empty_manifest(1025);
    root.tables[0].schema = schema;
    root.tables[0].schema.name = root.tables[0].table.clone();
    root.tables[0].schema_digest =
        crate::relational::index_shadow::relational_schema_digest(&root.tables[0].schema).unwrap();
    root.tables[0].column_count = NonZeroU32::new(1025).unwrap();
    root.root_set_digest = manifest::root_set_digest(&root.tables).unwrap();
    let before = root.clone();
    let config = RelationalRowPagePublicationConfig::default();
    let expected = manifest::encode_manifest(&root, config).unwrap();
    let actual = manifest::encode_manifest_with_work_context(&root, config, &work).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        manifest::root_set_digest_with_work_context(&root.tables, &work).unwrap(),
        root.root_set_digest
    );
    let directory = unique_test_dir("root-manifest-all");
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("manifest.hawdb");
    fs::write(&path, actual).unwrap();
    assert_eq!(manifest::read_manifest(&path, config).unwrap(), root);
    assert_eq!(root, before);
    probe.assert_released(&local);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    fs::remove_dir_all(directory).unwrap();
}

fn wide_manifest() -> RelationalRowPageRootManifest {
    let mut root = empty_manifest(1);
    root.tables[0].schema.columns[1].default = Some(RelationalColumnDefault::Literal(
        RelationalValue::Text("界\0".repeat(20 * 1024 + 3)),
    ));
    root.tables[0].schema_digest =
        crate::relational::index_shadow::relational_schema_digest(&root.tables[0].schema).unwrap();
    root.tables[0].page_count = 1;
    root.tables[0].row_count = 1;
    root.tables[0].lower_bound = vec![0; 64 * 1024];
    root.tables[0].upper_bound = vec![0; 64 * 1024];
    root.root_page_count = 1;
    root.dirty_page_count = 1;
    root.page_artifact = metadata(root.page_bytes);
    root.root_descriptor_artifact = metadata(root::ROOT_DESCRIPTOR_BYTES as u64);
    root.root_key_artifact = metadata(128 * 1024);
    root.physical_generations
        .push(RelationalRowPagePhysicalGeneration {
            generation: 1,
            allocated_pages: 1,
            live_pages: 1,
        });
    root.root_set_digest = manifest::root_set_digest(&root.tables).unwrap();
    root
}

#[test]
fn checkpoint_units_row_manifest_cancel_every_actual_unit_and_deny_without_source_mutation_retries()
{
    let root = wide_manifest();
    let before = root.clone();
    let config = RelationalRowPagePublicationConfig::default();
    let expected = manifest::encode_manifest(&root, config).unwrap();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        manifest::encode_manifest_with_work_context(
            &root,
            config,
            &baseline.context(local.clone())
        )
        .unwrap(),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    assert!(units > 100);
    for limit in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = manifest::encode_manifest_with_work_context(
            &root,
            config,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        probe.assert_released(&local);
        assert_eq!(root, before);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            manifest::encode_manifest_with_work_context(
                &root,
                config,
                &retry.context(local.clone())
            )
            .unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error = manifest::encode_manifest_with_work_context(&root, config, &work).unwrap_err();
    assert!(error.to_string().contains("admission deferred"));
    drop(held);
    probe.assert_released(&local);
    assert_eq!(root, before);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        manifest::encode_manifest_with_work_context(&root, config, &retry.context(local.clone()))
            .unwrap(),
        expected
    );
    retry.assert_released(&local);
}

#[test]
fn checkpoint_units_row_manifest_matches_invalid_metadata_schema_and_budget_error_priority() {
    let original = empty_manifest(3);
    let before = original.clone();
    for scenario in 0..24 {
        let mut root = original.clone();
        let mut config = RelationalRowPagePublicationConfig::default();
        match scenario {
            0 => root.generation = 0,
            1 => root.source_commit_epoch = 0,
            2 => root.previous_generation = Some(1),
            3 => root.page_bytes += 1,
            4 => root.dirty_page_count = config.max_dirty_pages.get() as u64 + 1,
            5 => root.dirty_page_count = 1,
            6 => root.page_artifact.encoded_len = 1,
            7 => root.root_page_count = config.max_root_pages.get() + 1,
            8 => root
                .physical_generations
                .push(RelationalRowPagePhysicalGeneration {
                    generation: 0,
                    allocated_pages: 1,
                    live_pages: 1,
                }),
            9 => root
                .physical_generations
                .push(RelationalRowPagePhysicalGeneration {
                    generation: 1,
                    allocated_pages: 1,
                    live_pages: 0,
                }),
            10 => root.root_descriptor_artifact.encoded_len = 1,
            11 => root.root_key_artifact.encoded_len = config.max_root_key_bytes.get() + 1,
            12 => config.max_tables = std::num::NonZeroUsize::MIN,
            13 => root.tables[0].table.clear(),
            14 => root.tables.swap(0, 1),
            15 => root.tables[0].column_count = NonZeroU32::new(4097).unwrap(),
            16 => root.tables[0].schema.name = "other".into(),
            17 => root.tables[0].schema.columns[0].nullable = true,
            18 => root.tables[0].schema.columns[1].name = "id".into(),
            19 => root.tables[0].schema_digest = metadata(0).encoded_sha256,
            20 => root.tables[0].first_descriptor = 1,
            21 => root.tables[0].row_count = 1,
            22 => root.root_set_digest = metadata(0).encoded_sha256,
            _ => {
                config.max_manifest_bytes =
                    std::num::NonZeroUsize::new(manifest::MANIFEST_HEADER_BYTES).unwrap()
            }
        }
        let expected = manifest::encode_manifest(&root, config).unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = manifest::encode_manifest_with_work_context(
            &root,
            config,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert_eq!(
            actual.to_string(),
            expected.to_string(),
            "scenario={scenario}"
        );
        probe.assert_released(&local);
    }
    assert_eq!(original, before);
}

#[test]
fn checkpoint_units_row_schema_digest_cancels_each_actual_escape_hash_unit_and_preserves_diagnostics(
) {
    let mut schema = crate::relational::row_page::test_row_page_schema("documents", 2);
    schema.columns[1].scalar_type = RelationalScalarType::Bytea;
    schema.columns[1].default = Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(
        vec![0; 128 * 1024 + 1],
    )));
    let before = schema.clone();
    let expected = crate::relational::index_shadow::relational_schema_digest(&schema).unwrap();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        crate::relational::index_shadow::relational_schema_digest_with_work_context(
            &schema,
            &baseline.context(local.clone())
        )
        .unwrap(),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    for limit in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = crate::relational::index_shadow::relational_schema_digest_with_work_context(
            &schema,
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        probe.assert_released(&local);
        assert_eq!(schema, before);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            crate::relational::index_shadow::relational_schema_digest_with_work_context(
                &schema,
                &retry.context(local.clone())
            )
            .unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
    schema.columns[1].default = Some(RelationalColumnDefault::Literal(RelationalValue::Overflow(
        crate::relational::RelationalOverflowRef {
            digest: metadata(0).encoded_sha256,
            scalar_type: RelationalScalarType::Bytea,
            compressed_bytes: 1,
            uncompressed_bytes: 1,
        },
    )));
    let expected = crate::relational::index_shadow::relational_schema_digest(&schema).unwrap_err();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = crate::relational::index_shadow::relational_schema_digest_with_work_context(
        &schema,
        &probe.context(local.clone()),
    )
    .unwrap_err();
    assert_eq!(actual.to_string(), expected.to_string());
    probe.assert_released(&local);
}
