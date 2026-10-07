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
use crate::background::{CheckpointWorkContext, CheckpointWorkProbe};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::{atomic::Ordering, Arc};

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn setup(
    directory: &std::path::Path,
    config: RelationalRowPagePublicationConfig,
) -> RelationalRowPageRootReader {
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    RelationalRowPageRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap()
}

fn config() -> RelationalRowPagePublicationConfig {
    RelationalRowPagePublicationConfig {
        page_limits: RelationalRowPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(256 * 1024 + 17).unwrap(),
            ..RelationalRowPageLimits::default()
        },
        ..RelationalRowPagePublicationConfig::default()
    }
}

fn names(generation: u64) -> [String; 5] {
    [
        relational_row_page_artifact_file(generation),
        relational_row_page_root_descriptor_file(generation),
        relational_row_page_root_key_file(generation),
        relational_row_page_manifest_generation_file(generation),
        RELATIONAL_ROW_PAGE_MANIFEST_FILE.into(),
    ]
}

fn authority(directory: &std::path::Path) -> Vec<Vec<u8>> {
    names(1)
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect()
}

fn deltas() -> Vec<RelationalRowPageTableDelta> {
    let mut dirty = page(1, 2, 11, 1, 2);
    for (index, entry) in dirty.rows.iter_mut().enumerate() {
        entry.row = RelationalRow::new(vec![
            RelationalValue::BigInt(index as i64 + 1),
            RelationalValue::Text(format!("updated-{index}")),
        ]);
    }
    let mut delta = table_delta("documents", vec![dirty]);
    delta.next_page_id = NonZeroU64::new(3).unwrap();
    vec![delta]
}

fn publish(
    directory: &std::path::Path,
    base: &RelationalRowPageRootReader,
    mode: usize,
    work: Option<&CheckpointWorkContext>,
) -> Result<RelationalRowPagePublicationReport, RelationalRowPagePublicationError> {
    let mut publisher = RelationalRowPagePublisher::new(config());
    if let Some(work) = work {
        publisher = publisher.with_work_context(work);
    }
    let request = RelationalRowPageGenerationRequest {
        directory,
        generation: 2,
        source_commit_epoch: 11,
        base: Some(base),
        expected_previous_generation: Some(1),
        overflow_root: None,
    };
    match mode {
        0 => publisher.persist_generation(request, deltas()),
        1 => publisher.persist_generation_compacting(
            request,
            deltas(),
            RelationalRowPageRewriteConfig {
                max_live_ratio_percent: 100,
                ..RelationalRowPageRewriteConfig::default()
            },
            &hawdb_core::RuntimeTaskContext::default(),
        ),
        2 => publisher.publish(directory, 2, 11, Some(1), deltas()),
        _ => unreachable!(),
    }
}

fn verify(directory: &std::path::Path, mode: usize) {
    let reader = RelationalRowPageRootReader::open_generation(directory, 2, config()).unwrap();
    reader.scrub_physical_pages().unwrap();
    assert_eq!(reader.manifest().root_page_count, 2);
    let descriptors = collect_descriptors(&reader, "documents");
    assert_eq!(page_id_values(&descriptors), vec![1, 2]);
    assert_eq!(
        physical_generations(&descriptors),
        vec![2, if mode == 1 { 2 } else { 1 }]
    );
    let expected = deltas()
        .remove(0)
        .dirty_pages
        .remove(0)
        .rows
        .into_iter()
        .chain(page(2, 1, 10, 3, 4).rows)
        .collect::<Vec<_>>();
    let actual = descriptors
        .iter()
        .flat_map(|descriptor| reader.read_page(descriptor).unwrap().rows)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn checkpoint_units_row_page_publisher_preserves_complete_artifacts_in_every_mode() {
    for mode in 0..3 {
        let ordinary = unique_test_dir("ordinary-page-units");
        let ordinary_base = setup(&ordinary, config());
        let expected = publish(&ordinary, &ordinary_base, mode, None).unwrap();
        let controlled = unique_test_dir("controlled-page-units");
        let base = setup(&controlled, config());
        let before = authority(&controlled);
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = publish(
            &controlled,
            &base,
            mode,
            Some(&probe.context(local.clone())),
        )
        .unwrap();
        assert_eq!(actual.generation_artifacts, expected.generation_artifacts);
        assert_eq!(actual.events, expected.events);
        for name in names(2) {
            assert_eq!(
                fs::read(controlled.join(&name)).unwrap(),
                fs::read(ordinary.join(name)).unwrap()
            );
        }
        if mode != 2 {
            assert_eq!(authority(&controlled), before);
        }
        assert!(probe.io_waves.load(Ordering::SeqCst) >= 7);
        probe.assert_released(&local);
        verify(&ordinary, mode);
        verify(&controlled, mode);
        assert_no_temporary_files(&controlled);
        fs::remove_dir_all(ordinary).unwrap();
        fs::remove_dir_all(controlled).unwrap();
    }
}

#[test]
fn checkpoint_units_row_page_publisher_cancel_every_actual_cpu_and_io_unit_retries_without_selection(
) {
    for mode in 0..3 {
        let baseline_directory = unique_test_dir("page-unit-count");
        let base = setup(&baseline_directory, config());
        let local = scheduler();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        publish(
            &baseline_directory,
            &base,
            mode,
            Some(&baseline.context(local.clone())),
        )
        .unwrap();
        let units = baseline.completed.load(Ordering::SeqCst);
        let waves = baseline.io_waves.load(Ordering::SeqCst);
        baseline.assert_released(&local);
        fs::remove_dir_all(baseline_directory).unwrap();
        for io in [false, true] {
            let count = if io { waves } else { units };
            for limit in 1..=count {
                let directory = unique_test_dir("page-unit-cancel");
                let base = setup(&directory, config());
                let before = authority(&directory);
                let probe = Arc::new(CheckpointWorkProbe::default());
                if io {
                    probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
                } else {
                    probe.cancel_after.store(limit, Ordering::SeqCst);
                }
                let result = publish(&directory, &base, mode, Some(&probe.context(local.clone())));
                assert!(
                    matches!(result, Err(RelationalRowPagePublicationError::Admission(_))),
                    "{result:?}"
                );
                probe.assert_released(&local);
                assert_eq!(authority(&directory), before);
                for name in names(2).iter().take(4) {
                    assert!(!directory.join(name).exists());
                }
                assert_no_temporary_files(&directory);
                let retry = Arc::new(CheckpointWorkProbe::default());
                publish(&directory, &base, mode, Some(&retry.context(local.clone()))).unwrap();
                retry.assert_released(&local);
                verify(&directory, mode);
                assert_no_temporary_files(&directory);
                fs::remove_dir_all(directory).unwrap();
            }
        }
        let directory = unique_test_dir("page-unit-deny");
        let base = setup(&directory, config());
        let before = authority(&directory);
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let error = publish(&directory, &base, mode, Some(&work)).unwrap_err();
        assert!(
            error.to_string().contains("admission deferred"),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        assert_eq!(authority(&directory), before);
        assert_no_temporary_files(&directory);
        let retry = Arc::new(CheckpointWorkProbe::default());
        publish(&directory, &base, mode, Some(&retry.context(local.clone()))).unwrap();
        retry.assert_released(&local);
        verify(&directory, mode);
        fs::remove_dir_all(directory).unwrap();
    }
}
