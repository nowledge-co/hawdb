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
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::{atomic::Ordering, Arc};

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

#[test]
fn checkpoint_units_memory_manifest_open_denies_before_payload_allocation_preserves_source_and_retries(
) {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };
    let (directory, expected) = metadata_directory(3, false);
    let names = [
        RELATIONAL_ROW_PAGE_MANIFEST_FILE.into(),
        relational_row_page_manifest_generation_file(1),
        relational_row_page_artifact_file(1),
        relational_row_page_root_descriptor_file(1),
        relational_row_page_root_key_file(1),
    ];
    let before = names
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect::<Vec<_>>();
    let payload_bytes = before[0].len() as u64;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(payload_bytes * 4 + 128),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let denied = governor
        .try_admit(
            RuntimeWorkRequest::background_maintenance(payload_bytes + 23).with_io_wave_slots(1),
        )
        .unwrap();
    let work = crate::background::CheckpointWorkContext::new(
        denied.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    let error = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        RelationalRowPagePublicationConfig::default(),
        Some(&work),
    )
    .unwrap_err();
    assert!(
        matches!(error, RelationalRowPagePublicationError::Admission(_))
            && error.to_string().contains("memory admission deferred"),
        "{error:?}"
    );
    drop(denied);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let admitted = governor
        .try_admit(
            RuntimeWorkRequest::background_maintenance(payload_bytes + 24).with_io_wave_slots(1),
        )
        .unwrap();
    let work = crate::background::CheckpointWorkContext::new(
        admitted.bind_task_context(hawdb_core::RuntimeTaskContext::default()),
    );
    let reader = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        RelationalRowPagePublicationConfig::default(),
        Some(&work),
    )
    .unwrap()
    .unwrap();
    assert_eq!(reader.manifest(), &expected);
    drop(reader);
    drop(admitted);
    let idle = governor.snapshot();
    assert_eq!(idle.admitted_memory_bytes, 0);
    assert_eq!(idle.active_cpu_slots, 0);
    assert_eq!(idle.active_background_io_slots, 0);
    for (name, bytes) in names.iter().zip(before) {
        assert_eq!(fs::read(directory.join(name)).unwrap(), bytes);
    }
    fs::remove_dir_all(directory).unwrap();
}

fn metadata_directory(count: usize, wide: bool) -> (PathBuf, RelationalRowPageRootManifest) {
    let mut root = super::checkpoint_manifest::empty_manifest(count);
    if wide {
        let mut schema = super::checkpoint_manifest::schema();
        schema.name = root.tables[0].table.clone();
        root.tables[0].column_count = NonZeroU32::new(schema.columns.len() as u32).unwrap();
        root.tables[0].schema_digest =
            crate::relational::index_shadow::relational_schema_digest(&schema).unwrap();
        root.tables[0].schema = schema;
        root.root_set_digest = manifest::root_set_digest(&root.tables).unwrap();
    }
    let directory = unique_test_dir("admitted-manifest-read");
    fs::create_dir_all(&directory).unwrap();
    let encoded =
        manifest::encode_manifest(&root, RelationalRowPagePublicationConfig::default()).unwrap();
    fs::write(
        directory.join(relational_row_page_manifest_generation_file(1)),
        &encoded,
    )
    .unwrap();
    fs::write(directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE), encoded).unwrap();
    for name in [
        relational_row_page_artifact_file(1),
        relational_row_page_root_descriptor_file(1),
        relational_row_page_root_key_file(1),
    ] {
        fs::write(directory.join(name), []).unwrap();
    }
    (directory, root)
}

#[test]
fn checkpoint_units_row_reader_preserves_all_1025_tables_schema_items_and_source_bytes() {
    let (directory, expected) = metadata_directory(1025, true);
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let before = fs::read(&path).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let ordinary = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        config,
        Some(&probe.context(local.clone())),
    )
    .unwrap()
    .unwrap();
    assert_eq!(actual.manifest(), ordinary.manifest());
    assert_eq!(actual.manifest(), &expected);
    assert_eq!(actual.manifest().tables.len(), 1025);
    assert_eq!(fs::read(&path).unwrap(), before);
    probe.assert_released(&local);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_reader_cancel_every_actual_cpu_io_boundary_retains_source_and_retries() {
    let (directory, expected) = metadata_directory(3, false);
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let before = fs::read(&path).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        config,
        Some(&baseline.context(local.clone())),
    )
    .unwrap()
    .unwrap();
    let units = baseline.completed.load(Ordering::SeqCst);
    let waves = baseline.io_waves.load(Ordering::SeqCst);
    assert!(units > 0 && waves > 0);
    baseline.assert_released(&local);
    for io in [false, true] {
        for limit in 1..=if io { waves } else { units } {
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(limit, Ordering::SeqCst);
            }
            let error = RelationalRowPageRootReader::open_latest_with_work_context(
                &directory,
                config,
                Some(&probe.context(local.clone())),
            )
            .unwrap_err();
            assert!(
                matches!(error, RelationalRowPagePublicationError::Admission(_))
                    && error.to_string().contains("stopped"),
                "{error:?}"
            );
            probe.assert_released(&local);
            assert_eq!(fs::read(&path).unwrap(), before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            let reader = RelationalRowPageRootReader::open_latest_with_work_context(
                &directory,
                config,
                Some(&retry.context(local.clone())),
            )
            .unwrap()
            .unwrap();
            assert_eq!(reader.manifest(), &expected);
            retry.assert_released(&local);
        }
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_reader_denial_retains_all_source_files_and_retries() {
    let (directory, expected) = metadata_directory(3, false);
    let names = [
        RELATIONAL_ROW_PAGE_MANIFEST_FILE.into(),
        relational_row_page_artifact_file(1),
        relational_row_page_root_descriptor_file(1),
        relational_row_page_root_key_file(1),
    ];
    let before = names
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect::<Vec<_>>();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        RelationalRowPagePublicationConfig::default(),
        Some(&work),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("admission deferred"),
        "{error:?}"
    );
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    drop(held);
    probe.assert_released(&local);
    let retry = Arc::new(CheckpointWorkProbe::default());
    let actual = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        RelationalRowPagePublicationConfig::default(),
        Some(&retry.context(local.clone())),
    )
    .unwrap()
    .unwrap();
    assert_eq!(actual.manifest(), &expected);
    retry.assert_released(&local);
    for (name, expected) in names.iter().zip(before) {
        assert_eq!(fs::read(directory.join(name)).unwrap(), expected);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_reader_schema_decode_cancels_each_actual_wide_scalar_unit_and_retries() {
    // The complete metadata test retains all 1025 entries. This fixture keeps
    // all scalar/default forms and each nested collection for every-unit cuts.
    let mut schema = super::checkpoint_manifest::schema();
    schema.columns.truncate(10);
    schema.unique_constraints.truncate(3);
    schema.foreign_keys.truncate(3);
    schema.indexes.truncate(3);
    let encoded = crate::relational::codec::encode_relational_table_schema(&schema).unwrap();
    let before = encoded.clone();
    let local = scheduler();
    let operation = |work: &crate::background::CheckpointWorkContext| {
        crate::relational::codec::decode_relational_table_schema_with_work_context(
            &encoded,
            encoded.len(),
            4096,
            work,
        )
    };
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(operation(&baseline.context(local.clone())).unwrap(), schema);
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 100);
    baseline.assert_released(&local);
    for limit in 1..=units {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = operation(&probe.context(local.clone())).unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        probe.assert_released(&local);
        assert_eq!(encoded, before);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(operation(&retry.context(local.clone())).unwrap(), schema);
        retry.assert_released(&local);
    }
}

fn resign(encoded: &mut [u8]) {
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    hasher.update(&encoded[..280]);
    hasher.update(&encoded[316..]);
    let digest = hasher.finish();
    encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
}

#[test]
fn checkpoint_units_row_reader_preserves_all_corruption_budget_and_artifact_diagnostics() {
    let (directory, _) = metadata_directory(3, false);
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    let original = fs::read(&path).unwrap();
    let schema_start = 324 + u32::from_le_bytes(original[316..320].try_into().unwrap()) as usize;
    let page_path = directory.join(relational_row_page_artifact_file(1));
    let key_path = directory.join(relational_row_page_root_key_file(1));
    let local = scheduler();
    for case in 0..18 {
        let mut encoded = original.clone();
        let mut config = RelationalRowPagePublicationConfig::default();
        fs::write(&page_path, []).unwrap();
        fs::write(&key_path, []).unwrap();
        match case {
            0 => encoded.truncate(315),
            1 => encoded[0] ^= 1,
            2 => encoded[8..10].copy_from_slice(&99u16.to_le_bytes()),
            3 => encoded[10..12].copy_from_slice(&0u16.to_le_bytes()),
            4 => encoded[10..12].copy_from_slice(&2u16.to_le_bytes()),
            5 => {
                encoded[60..64].copy_from_slice(&(config.max_tables.get() as u32 + 1).to_le_bytes())
            }
            6 => {
                let len = u32::from_le_bytes(encoded[64..68].try_into().unwrap());
                encoded[64..68].copy_from_slice(&(len + 1).to_le_bytes());
            }
            7 => encoded[68] ^= 1,
            8 => encoded[284] ^= 1,
            9 => {
                encoded.pop();
            }
            10 => {
                encoded[320] = 0xff;
                resign(&mut encoded);
            }
            11 => {
                encoded[schema_start + 8] = 0xff;
                resign(&mut encoded);
            }
            12 => {
                encoded[schema_start..schema_start + 8].copy_from_slice(&u64::MAX.to_le_bytes());
                resign(&mut encoded);
            }
            13 => config.max_manifest_bytes = NonZeroUsize::new(original.len() - 1).unwrap(),
            14 => config.max_tables = NonZeroUsize::new(2).unwrap(),
            15 => config.page_limits.max_columns = NonZeroUsize::MIN,
            16 => fs::write(&page_path, [1]).unwrap(),
            17 => fs::remove_file(&key_path).unwrap(),
            _ => unreachable!(),
        }
        fs::write(&path, &encoded).unwrap();
        let expected = RelationalRowPageRootReader::open_latest(&directory, config).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = RelationalRowPageRootReader::open_latest_with_work_context(
            &directory,
            config,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string(), "case {case}");
        probe.assert_released(&local);
        assert_eq!(fs::read(&path).unwrap(), encoded);
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_reader_utf8_crosses_file_and_decode_blocks_with_exact_error_offsets() {
    let (directory, mut root) = metadata_directory(1, false);
    let name = format!("{}🙂{}", "a".repeat(64 * 1024 - 1), "界".repeat(1000));
    root.tables[0].table = name.clone();
    root.tables[0].schema.name = name;
    root.tables[0].schema_digest =
        crate::relational::index_shadow::relational_schema_digest(&root.tables[0].schema).unwrap();
    root.root_set_digest = manifest::root_set_digest(&root.tables).unwrap();
    let config = RelationalRowPagePublicationConfig {
        max_table_name_bytes: NonZeroUsize::new(128 * 1024).unwrap(),
        ..RelationalRowPagePublicationConfig::default()
    };
    let original = manifest::encode_manifest(&root, config).unwrap();
    let path = directory.join(RELATIONAL_ROW_PAGE_MANIFEST_FILE);
    fs::write(&path, &original).unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = RelationalRowPageRootReader::open_latest_with_work_context(
        &directory,
        config,
        Some(&probe.context(local.clone())),
    )
    .unwrap()
    .unwrap();
    assert_eq!(actual.manifest(), &root);
    probe.assert_released(&local);
    let name_len = root.tables[0].table.len();
    for offset in [64 * 1024, name_len - 1] {
        let mut encoded = original.clone();
        encoded[320 + offset] = if offset == name_len - 1 { 0xe2 } else { 0xff };
        resign(&mut encoded);
        fs::write(&path, &encoded).unwrap();
        let expected = RelationalRowPageRootReader::open_latest(&directory, config).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = RelationalRowPageRootReader::open_latest_with_work_context(
            &directory,
            config,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        probe.assert_released(&local);
        assert_eq!(fs::read(&path).unwrap(), encoded);
    }
    fs::remove_dir_all(directory).unwrap();
}

fn binding(
    directory: &Path,
    root: &RelationalRowPageRootManifest,
) -> RelationalRowPageGenerationArtifacts {
    let encoded = fs::read(directory.join(relational_row_page_manifest_generation_file(
        root.generation,
    )))
    .unwrap();
    let digest = hawdb_integrity::integrity_digest(&encoded);
    RelationalRowPageGenerationArtifacts {
        generation: root.generation,
        source_commit_epoch: root.source_commit_epoch,
        root_set_digest: root.root_set_digest,
        manifest_artifact: RelationalRowPageArtifactMetadata {
            encoded_len: encoded.len() as u64,
            encoded_crc32c: digest.crc32c.get(),
            encoded_sha256: digest.sha256,
        },
    }
}

#[test]
fn checkpoint_units_row_reader_generation_and_bound_open_cancel_all_units_and_preserve_bindings() {
    let (directory, expected) = metadata_directory(3, false);
    let binding = binding(&directory, &expected);
    let path = directory.join(relational_row_page_manifest_generation_file(1));
    let before = fs::read(&path).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let local = scheduler();
    for bound in [false, true] {
        let operation = |work: Option<&crate::background::CheckpointWorkContext>| {
            if bound {
                RelationalRowPageRootReader::open_bound_generation_with_work_context(
                    &directory, binding, config, work,
                )
            } else {
                RelationalRowPageRootReader::open_generation_with_work_context(
                    &directory, 1, config, work,
                )
            }
        };
        let ordinary = operation(None).unwrap();
        assert_eq!(ordinary.manifest(), &expected);
        let baseline = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            operation(Some(&baseline.context(local.clone())))
                .unwrap()
                .manifest(),
            &expected
        );
        let units = baseline.completed.load(Ordering::SeqCst);
        let waves = baseline.io_waves.load(Ordering::SeqCst);
        assert!(units > 0 && waves > 0, "bound={bound}");
        baseline.assert_released(&local);
        for io in [false, true] {
            for limit in 1..=if io { waves } else { units } {
                let probe = Arc::new(CheckpointWorkProbe::default());
                if io {
                    probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
                } else {
                    probe.cancel_after.store(limit, Ordering::SeqCst);
                }
                let error = operation(Some(&probe.context(local.clone()))).unwrap_err();
                assert!(
                    matches!(error, RelationalRowPagePublicationError::Admission(_))
                        && error.to_string().contains("stopped"),
                    "{error:?}"
                );
                probe.assert_released(&local);
                assert_eq!(fs::read(&path).unwrap(), before);
                let retry = Arc::new(CheckpointWorkProbe::default());
                assert_eq!(
                    operation(Some(&retry.context(local.clone())))
                        .unwrap()
                        .manifest(),
                    &expected
                );
                retry.assert_released(&local);
            }
        }
    }
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_reader_rejects_all_canonical_binding_drift_without_source_changes() {
    let (directory, root) = metadata_directory(3, false);
    let original_binding = binding(&directory, &root);
    let path = directory.join(relational_row_page_manifest_generation_file(1));
    let before = fs::read(&path).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    let local = scheduler();
    for case in 0..5 {
        let mut changed = original_binding;
        match case {
            0 => changed.manifest_artifact.encoded_len += 1,
            1 => changed.manifest_artifact.encoded_crc32c ^= 1,
            2 => changed.manifest_artifact.encoded_sha256 = Sha256Digest::from_bytes([0; 32]),
            3 => changed.source_commit_epoch += 1,
            4 => changed.root_set_digest = Sha256Digest::from_bytes([0; 32]),
            _ => unreachable!(),
        }
        let expected =
            RelationalRowPageRootReader::open_bound_generation(&directory, changed, config)
                .unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = RelationalRowPageRootReader::open_bound_generation_with_work_context(
            &directory,
            changed,
            config,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        probe.assert_released(&local);
        assert_eq!(fs::read(&path).unwrap(), before);
    }
    fs::remove_dir_all(directory).unwrap();
}
