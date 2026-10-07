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
use crate::relational::{RelationalColumnDefault, RelationalTableSchema};
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
fn checkpoint_units_row_preflight_schema_copy_and_equality_preserve_all_fields_and_negative_controls(
) {
    let schema = super::checkpoint_manifest::schema();
    let before = schema.clone();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let actual = root::checkpoint::clone_schema(&schema, &work).unwrap();
    assert_eq!(actual, schema);
    assert_eq!(
        crate::relational::codec::encode_relational_table_schema(&actual).unwrap(),
        crate::relational::codec::encode_relational_table_schema(&schema).unwrap()
    );
    assert!(root::checkpoint::same_schema(&schema, &actual, &work).unwrap());
    for case in 0..25 {
        let mut changed = schema.clone();
        match case {
            0 => changed.name.push('x'),
            1 => {
                changed.columns.pop();
            }
            2 => changed.columns[1024].name.push('x'),
            3 => changed.columns[1024].nullable = !changed.columns[1024].nullable,
            4 => changed.columns[1024].scalar_type = crate::relational::RelationalScalarType::Uuid,
            5 => changed.columns[1].default = None,
            6 => changed.columns[2].default = Some(RelationalColumnDefault::UuidV7),
            7 => {
                changed.columns[3].default = Some(RelationalColumnDefault::Literal(
                    RelationalValue::DoublePrecision(f64::from_bits(0xfff8_0000_0000_0012)),
                ))
            }
            8 => {
                if let Some(RelationalColumnDefault::Literal(RelationalValue::Text(value))) =
                    &mut changed.columns[4].default
                {
                    value.push('x');
                }
            }
            9 => {
                if let Some(RelationalColumnDefault::Literal(RelationalValue::Bytea(value))) =
                    &mut changed.columns[5].default
                {
                    *value.last_mut().unwrap() = 1;
                }
            }
            10 => {
                changed.columns[8].default = Some(RelationalColumnDefault::Literal(
                    RelationalValue::DoublePrecision(0.0),
                ))
            }
            11 => changed.primary_key.push("other".into()),
            12 => changed.primary_key[0].push('x'),
            13 => {
                changed.unique_constraints.pop();
            }
            14 => changed.unique_constraints[1024][0].push('x'),
            15 => {
                changed.foreign_keys.pop();
            }
            16 => changed.foreign_keys[1024].columns[0].push('x'),
            17 => changed.foreign_keys[1024].referenced_table.push('x'),
            18 => changed.foreign_keys[1024].referenced_columns[0].push('x'),
            19 => {
                changed.foreign_keys[1024].on_delete =
                    crate::relational::RelationalReferentialAction::Cascade
            }
            20 => {
                changed.indexes.pop();
            }
            21 => changed.indexes[1024].name.push('x'),
            22 => changed.indexes[1024].unique = !changed.indexes[1024].unique,
            23 => {
                changed.foreign_keys[1024].on_update =
                    crate::relational::RelationalReferentialAction::NoAction
            }
            24 => changed.indexes[1024].columns[0].push('x'),
            _ => unreachable!(),
        }
        assert_ne!(changed, schema, "ineffective control {case}");
        assert_eq!(
            root::checkpoint::same_schema(&schema, &changed, &work).unwrap(),
            schema == changed,
            "case {case}"
        );
    }
    probe.assert_released(&local);
    assert_eq!(schema, before);
}

fn cancellation_schema() -> RelationalTableSchema {
    // The separate complete-value fixture keeps all 1025 collection entries.
    // This cancellation fixture exercises all scalar/default forms and every
    // nested collection with wide payloads, without quadratic 1025-item setup.
    let mut schema = super::checkpoint_manifest::schema();
    schema.columns.truncate(10);
    schema.unique_constraints.truncate(3);
    schema.foreign_keys.truncate(3);
    schema.indexes.truncate(3);
    schema
}

#[test]
fn checkpoint_units_row_preflight_schema_cancel_every_copy_and_comparison_unit_denial_and_full_retry(
) {
    let schema = cancellation_schema();
    let before = schema.clone();
    let local = scheduler();
    for copying in [false, true] {
        let operation = |work: &crate::background::CheckpointWorkContext| {
            if copying {
                root::checkpoint::clone_schema(&schema, work)
                    .map(|output| assert_eq!(output, schema))
            } else {
                root::checkpoint::same_schema(&schema, &schema, work).map(|output| assert!(output))
            }
        };
        let baseline = Arc::new(CheckpointWorkProbe::default());
        operation(&baseline.context(local.clone())).unwrap();
        let units = baseline.completed.load(Ordering::SeqCst);
        assert!(units > 50);
        baseline.assert_released(&local);
        for limit in 1..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(limit, Ordering::SeqCst);
            let error = operation(&probe.context(local.clone())).unwrap_err();
            assert!(error.to_string().contains("stopped"), "{error:?}");
            probe.assert_released(&local);
            assert_eq!(schema, before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            operation(&retry.context(local.clone())).unwrap();
            retry.assert_released(&local);
            assert_eq!(schema, before);
        }
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let error = operation(&work).unwrap_err();
        assert!(
            error.to_string().contains("admission deferred"),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        assert_eq!(schema, before);
        let retry = Arc::new(CheckpointWorkProbe::default());
        operation(&retry.context(local.clone())).unwrap();
        retry.assert_released(&local);
    }
}

fn prepared_pages(count: u64) -> Vec<super::super::publisher::PreparedDirtyPage> {
    let mut schema = crate::relational::row_page::test_row_page_schema("documents", 2);
    schema.columns[0].scalar_type = crate::relational::RelationalScalarType::Text;
    let digest = crate::relational::index_shadow::relational_schema_digest(&schema).unwrap();
    (1..=count)
        .rev()
        .map(|ordinal| {
            let key = format!("{}-{ordinal:04}", "界".repeat(700));
            let mut value = page(ordinal, 2, 11, ordinal as i64, ordinal as i64);
            value.schema_digest = digest;
            value.rows[0].primary_key = RelationalKey(vec![RelationalValue::Text(key.clone())]);
            value.rows[0].row = RelationalRow::new(vec![
                RelationalValue::Text(key),
                RelationalValue::Text(format!("row-{ordinal}")),
            ]);
            root::prepare_dirty_page(value, RelationalRowPageLimits::default()).unwrap()
        })
        .collect()
}

#[test]
fn checkpoint_units_row_preflight_sort_preserves_all_1025_pages_and_complete_descriptors() {
    let mut expected = prepared_pages(1025);
    expected.sort_by(|left, right| {
        left.descriptor
            .lower_bound
            .cmp(&right.descriptor.lower_bound)
    });
    let mut actual = prepared_pages(1025);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    super::super::publisher::checkpoint::sort_dirty_pages(
        &mut actual,
        Some(&probe.context(local.clone())),
    )
    .unwrap();
    assert_eq!(actual.len(), 1025);
    for (ordinal, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
        assert_eq!(actual.page, expected.page);
        assert_eq!(actual.descriptor, expected.descriptor);
        assert_eq!(actual.page.page_id.get(), ordinal as u64 + 1);
    }
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_row_preflight_sort_cancel_every_actual_unit_denial_and_full_retry() {
    let local = scheduler();
    let mut expected = prepared_pages(7);
    expected.sort_by(|left, right| {
        left.descriptor
            .lower_bound
            .cmp(&right.descriptor.lower_bound)
    });
    let verify = |pages: &[super::super::publisher::PreparedDirtyPage]| {
        for (actual, expected) in pages.iter().zip(&expected) {
            assert_eq!(actual.page, expected.page);
            assert_eq!(actual.descriptor, expected.descriptor);
        }
    };
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let mut pages = prepared_pages(7);
    super::super::publisher::checkpoint::sort_dirty_pages(
        &mut pages,
        Some(&baseline.context(local.clone())),
    )
    .unwrap();
    verify(&pages);
    let units = baseline.completed.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    assert!(units > 20);
    for limit in 1..=units {
        let mut pages = prepared_pages(7);
        let before = pages
            .iter()
            .map(|page| page.page.clone())
            .collect::<Vec<_>>();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = super::super::publisher::checkpoint::sort_dirty_pages(
            &mut pages,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        probe.assert_released(&local);
        for original in before {
            assert!(pages.iter().any(|page| page.page == original));
        }
        let retry = Arc::new(CheckpointWorkProbe::default());
        super::super::publisher::checkpoint::sort_dirty_pages(
            &mut pages,
            Some(&retry.context(local.clone())),
        )
        .unwrap();
        verify(&pages);
        retry.assert_released(&local);
    }
    let mut pages = prepared_pages(7);
    let original = pages
        .iter()
        .map(|page| page.page.clone())
        .collect::<Vec<_>>();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error =
        super::super::publisher::checkpoint::sort_dirty_pages(&mut pages, Some(&work)).unwrap_err();
    assert!(
        error.to_string().contains("admission deferred"),
        "{error:?}"
    );
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(
        pages
            .iter()
            .map(|page| page.page.clone())
            .collect::<Vec<_>>(),
        original
    );
    drop(held);
    probe.assert_released(&local);
    let retry = Arc::new(CheckpointWorkProbe::default());
    super::super::publisher::checkpoint::sort_dirty_pages(
        &mut pages,
        Some(&retry.context(local.clone())),
    )
    .unwrap();
    verify(&pages);
    retry.assert_released(&local);
}

#[test]
fn checkpoint_units_row_preflight_preserves_rejection_diagnostics_and_creates_no_candidate() {
    for case in 0..18 {
        let mut config = RelationalRowPagePublicationConfig::default();
        let mut deltas = vec![table_delta(
            "documents",
            vec![page(1, 2, 11, 1, 1), page(2, 2, 11, 2, 2)],
        )];
        match case {
            0 => config.max_dirty_pages = NonZeroUsize::MIN,
            1 => config.max_dirty_bytes = NonZeroU64::MIN,
            2 => {
                config.max_tables = NonZeroUsize::MIN;
                deltas.push(deltas[0].clone());
            }
            3 => deltas[0].table.clear(),
            4 => deltas.push(deltas[0].clone()),
            5 => deltas[0].schema.as_mut().unwrap().name.push('x'),
            6 => {
                deltas[0].schema.as_mut().unwrap().columns.pop();
            }
            7 => deltas[0].schema_digest = Sha256Digest::from_bytes([0; 32]),
            8 => deltas[0].deleted_page_ids = vec![page_id(42), page_id(42)],
            9 => deltas[0].deleted_page_ids = vec![page_id(42), page_id(7)],
            10 => deltas[0].dirty_pages[0].page_id = page_id(42),
            11 => deltas[0].dirty_pages[0].generation = 3,
            12 => deltas[0].dirty_pages[0].schema_digest = Sha256Digest::from_bytes([0; 32]),
            13 => deltas[0].dirty_pages[0].column_count = 1,
            14 => {
                let duplicate = deltas[0].dirty_pages[0].clone();
                deltas[0].dirty_pages.push(duplicate);
            }
            15 => deltas[0].deleted_page_ids = vec![page_id(1)],
            16 => {
                deltas[0].dirty_pages[1].rows[0].primary_key =
                    RelationalKey(vec![RelationalValue::BigInt(1)])
            }
            17 => deltas[0].schema = None,
            _ => unreachable!(),
        }
        let ordinary = unique_test_dir("preflight-error-reference");
        let controlled = unique_test_dir("preflight-error-controlled");
        let request = |directory| RelationalRowPageGenerationRequest {
            directory,
            generation: 2,
            source_commit_epoch: 11,
            base: None,
            expected_previous_generation: None,
            overflow_root: None,
        };
        let expected = RelationalRowPagePublisher::new(config)
            .persist_generation(request(&ordinary), deltas.clone())
            .unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = RelationalRowPagePublisher::new(config)
            .with_work_context(&probe.context(local.clone()))
            .persist_generation(request(&controlled), deltas)
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string(), "case {case}");
        probe.assert_released(&local);
        for directory in [ordinary, controlled] {
            for name in [
                relational_row_page_artifact_file(2),
                relational_row_page_root_descriptor_file(2),
                relational_row_page_root_key_file(2),
                relational_row_page_manifest_generation_file(2),
                RELATIONAL_ROW_PAGE_MANIFEST_FILE.into(),
            ] {
                assert!(!directory.join(name).exists(), "case {case}");
            }
            if directory.exists() {
                assert_no_temporary_files(&directory);
                fs::remove_dir_all(directory).unwrap();
            }
        }
    }
}
