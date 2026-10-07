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

fn setup(directory: &std::path::Path, count: u64) -> RelationalRowPageRootReader {
    let config = RelationalRowPagePublicationConfig::default();
    let batch = setup_batch_size();
    let mut base: Option<RelationalRowPageRootReader> = None;
    for generation in 1..=count.div_ceil(batch) {
        let first = (generation - 1) * batch + 1;
        let last = (generation * batch).min(count);
        RelationalRowPagePublisher::new(config)
            .publish(
                directory,
                generation,
                10,
                base.as_ref().map(|reader| reader.manifest().generation),
                vec![table_delta(
                    "documents",
                    (first..=last)
                        .map(|id| page(id, generation, 10, id as i64, id as i64))
                        .collect(),
                )],
            )
            .unwrap();
        base = RelationalRowPageRootReader::open_latest(directory, config).unwrap();
    }
    base.unwrap()
}

fn setup_batch_size() -> u64 {
    let config = RelationalRowPagePublicationConfig::default();
    (config.max_dirty_bytes.get() / config.page_limits.max_page_bytes.get() as u64)
        .min(config.max_dirty_pages.get() as u64)
}

fn images(directory: &std::path::Path, selected: u64) -> Vec<Vec<u8>> {
    let mut names = Vec::new();
    for generation in 1..=selected {
        names.extend([
            relational_row_page_artifact_file(generation),
            relational_row_page_root_descriptor_file(generation),
            relational_row_page_root_key_file(generation),
            relational_row_page_manifest_generation_file(generation),
        ]);
    }
    names.push(RELATIONAL_ROW_PAGE_MANIFEST_FILE.into());
    names
        .iter()
        .map(|name| fs::read(directory.join(name)).unwrap())
        .collect()
}

fn visit(
    reader: &RelationalRowPageRootReader,
    work: &CheckpointWorkContext,
) -> Result<Vec<RelationalRowPageRootDescriptor>, RelationalRowPagePublicationError> {
    let mut descriptors = Vec::new();
    reader.visit_table_pages_with_work_context("documents", work, |descriptor| {
        descriptors.push(descriptor.clone());
        Ok(())
    })?;
    Ok(descriptors)
}

#[test]
fn checkpoint_units_row_root_visits_all_1025_descriptors_without_leases_across_nested_callbacks() {
    let directory = unique_test_dir("root-visit-all");
    let reader = setup(&directory, 1025);
    let expected = collect_descriptors(&reader, "documents");
    let before = images(&directory, reader.manifest().generation);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let mut actual = Vec::new();
    reader
        .visit_table_pages_with_work_context("documents", &work, |descriptor| {
            assert_eq!(probe.active_units.load(Ordering::SeqCst), 0);
            assert_eq!(probe.active_io.load(Ordering::SeqCst), 0);
            let unit = work.start_unit().unwrap();
            unit.finish();
            let wave = work.io_wave().unwrap();
            drop(wave);
            actual.push(descriptor.clone());
            Ok(())
        })
        .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 1025);
    for descriptor in &actual {
        assert_eq!(
            reader.read_page(descriptor).unwrap(),
            page(
                descriptor.logical_page_id.get(),
                descriptor
                    .logical_page_id
                    .get()
                    .div_ceil(setup_batch_size()),
                10,
                descriptor.logical_page_id.get() as i64,
                descriptor.logical_page_id.get() as i64
            )
        );
    }
    probe.assert_released(&local);
    assert_eq!(images(&directory, reader.manifest().generation), before);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_row_root_visit_cancel_every_actual_cpu_io_denial_and_callback_retries() {
    let directory = unique_test_dir("root-visit-cancel");
    let reader = setup(&directory, 3);
    let expected = collect_descriptors(&reader, "documents");
    let before = images(&directory, reader.manifest().generation);
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        visit(&reader, &baseline.context(local.clone())).unwrap(),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    let waves = baseline.io_waves.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    for io in [false, true] {
        for limit in 1..=if io { waves } else { units } {
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(limit, Ordering::SeqCst);
            }
            let error = visit(&reader, &probe.context(local.clone())).unwrap_err();
            assert!(
                matches!(error, RelationalRowPagePublicationError::Admission(_)),
                "{error:?}"
            );
            probe.assert_released(&local);
            assert_eq!(images(&directory, reader.manifest().generation), before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(
                visit(&reader, &retry.context(local.clone())).unwrap(),
                expected
            );
            retry.assert_released(&local);
        }
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error = visit(&reader, &work).unwrap_err();
    assert!(error.to_string().contains("admission deferred"));
    drop(held);
    probe.assert_released(&local);
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let mut count = 0;
    let error = reader
        .visit_table_pages_with_work_context("documents", &work, |_| {
            count += 1;
            probe.cancellation.cancel();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(count, 1);
    assert!(error.to_string().contains("stopped"));
    probe.assert_released(&local);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        visit(&reader, &retry.context(local.clone())).unwrap(),
        expected
    );
    retry.assert_released(&local);
    assert_eq!(images(&directory, reader.manifest().generation), before);
    fs::remove_dir_all(directory).unwrap();
}

fn read_descriptor(
    directory: &std::path::Path,
    reader: &RelationalRowPageRootReader,
    ordinal: u64,
    config: RelationalRowPagePublicationConfig,
    work: Option<&CheckpointWorkContext>,
) -> Result<RelationalRowPageRootDescriptor, RelationalRowPagePublicationError> {
    let mut descriptors =
        crate::file_io::File::open(directory.join(relational_row_page_root_descriptor_file(1)))
            .unwrap();
    let mut keys =
        crate::file_io::File::open(directory.join(relational_row_page_root_key_file(1))).unwrap();
    match work {
        Some(work) => root::checkpoint::read_descriptor(
            &mut descriptors,
            &mut keys,
            ordinal,
            reader.manifest(),
            config,
            work,
        ),
        None => root::read_descriptor(
            &mut descriptors,
            &mut keys,
            ordinal,
            reader.manifest(),
            config,
        ),
    }
}

#[test]
fn checkpoint_units_row_root_descriptor_preserves_corruption_range_and_truncation_errors() {
    let directory = unique_test_dir("root-errors");
    let reader = setup(&directory, 3);
    let before = images(&directory, reader.manifest().generation);
    let original = fs::read(directory.join(relational_row_page_root_descriptor_file(1))).unwrap();
    let keys = fs::read(directory.join(relational_row_page_root_key_file(1))).unwrap();
    let config = RelationalRowPagePublicationConfig::default();
    for scenario in 0..12 {
        fs::write(
            directory.join(relational_row_page_root_descriptor_file(1)),
            &original,
        )
        .unwrap();
        fs::write(directory.join(relational_row_page_root_key_file(1)), &keys).unwrap();
        let mut damaged = reader.clone();
        let mut ordinal = 0;
        let mut config = config;
        match scenario {
            0 => ordinal = 3,
            1 => {
                let mut bytes = original.clone();
                bytes[100] ^= 1;
                fs::write(
                    directory.join(relational_row_page_root_descriptor_file(1)),
                    bytes,
                )
                .unwrap();
            }
            2 => {
                fs::write(
                    directory.join(relational_row_page_root_descriptor_file(1)),
                    &original[..13],
                )
                .unwrap();
            }
            3 => {
                fs::write(
                    directory.join(relational_row_page_root_key_file(1)),
                    &keys[..2],
                )
                .unwrap();
            }
            4 => {
                let mut bytes = original.clone();
                bytes[48..52].copy_from_slice(&0u32.to_le_bytes());
                fs::write(
                    directory.join(relational_row_page_root_descriptor_file(1)),
                    bytes,
                )
                .unwrap();
            }
            5 => {
                let mut bytes = original.clone();
                bytes[40..48].copy_from_slice(&u64::MAX.to_le_bytes());
                fs::write(
                    directory.join(relational_row_page_root_descriptor_file(1)),
                    bytes,
                )
                .unwrap();
            }
            6 => {
                let mut bytes = original.clone();
                bytes[52..60].copy_from_slice(&(keys.len() as u64).to_le_bytes());
                fs::write(
                    directory.join(relational_row_page_root_descriptor_file(1)),
                    bytes,
                )
                .unwrap();
            }
            7 => {
                config.page_limits.max_key_bytes = std::num::NonZeroUsize::MIN;
            }
            8 => {
                Arc::make_mut(&mut damaged.manifest).generation = 2;
            }
            9 => {
                Arc::make_mut(&mut damaged.manifest)
                    .physical_generations
                    .clear();
            }
            10 => {
                Arc::make_mut(&mut damaged.manifest).physical_generations[0].allocated_pages = 0;
            }
            _ => {
                Arc::make_mut(&mut damaged.manifest).source_commit_epoch = 0;
            }
        }
        let expected = read_descriptor(&directory, &damaged, ordinal, config, None).unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = read_descriptor(
            &directory,
            &damaged,
            ordinal,
            config,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert_eq!(
            actual.to_string(),
            expected.to_string(),
            "scenario={scenario}"
        );
        probe.assert_released(&local);
    }
    fs::write(
        directory.join(relational_row_page_root_descriptor_file(1)),
        original,
    )
    .unwrap();
    fs::write(directory.join(relational_row_page_root_key_file(1)), keys).unwrap();
    assert_eq!(images(&directory, reader.manifest().generation), before);
    fs::remove_dir_all(directory).unwrap();
}

fn publish_incremental(
    directory: &std::path::Path,
    base: &RelationalRowPageRootReader,
    work: Option<&CheckpointWorkContext>,
) -> RelationalRowPagePublicationReport {
    let mut publisher =
        RelationalRowPagePublisher::new(RelationalRowPagePublicationConfig::default());
    if let Some(work) = work {
        publisher = publisher.with_work_context(work);
    }
    let mut delta = table_delta(
        "documents",
        (1..=1025)
            .filter(|id| id % 37 == 0)
            .map(|id| {
                let mut changed =
                    page(id, base.manifest().generation + 1, 11, id as i64, id as i64);
                changed.rows[0].row = RelationalRow::new(vec![
                    RelationalValue::BigInt(id as i64),
                    RelationalValue::Text(format!("updated-{id}")),
                ]);
                changed
            })
            .collect(),
    );
    delta.next_page_id = NonZeroU64::new(1026).unwrap();
    delta.deleted_page_ids = vec![page_id(1025)];
    publisher
        .persist_generation(
            RelationalRowPageGenerationRequest {
                directory,
                generation: base.manifest().generation + 1,
                source_commit_epoch: 11,
                base: Some(base),
                expected_previous_generation: Some(base.manifest().generation),
                overflow_root: None,
            },
            vec![delta],
        )
        .unwrap()
}

#[test]
fn checkpoint_units_row_root_incremental_builder_matches_all_artifacts_1024_survivor_values_and_occupancy(
) {
    let ordinary = unique_test_dir("root-ordinary-generation");
    let ordinary_base = setup(&ordinary, 1025);
    let expected = publish_incremental(&ordinary, &ordinary_base, None);
    let controlled = unique_test_dir("root-controlled-generation");
    let base = setup(&controlled, 1025);
    let before = images(&controlled, base.manifest().generation);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = publish_incremental(&controlled, &base, Some(&probe.context(local.clone())));
    assert_eq!(actual.generation_artifacts, expected.generation_artifacts);
    assert_eq!(actual.root_pages, 1024);
    assert_eq!(actual.dirty_pages_written, 27);
    assert_eq!(actual.reused_pages, 997);
    assert_eq!(images(&controlled, base.manifest().generation), before);
    for name in [
        relational_row_page_artifact_file(base.manifest().generation + 1),
        relational_row_page_root_descriptor_file(base.manifest().generation + 1),
        relational_row_page_root_key_file(base.manifest().generation + 1),
        relational_row_page_manifest_generation_file(base.manifest().generation + 1),
    ] {
        assert_eq!(
            fs::read(controlled.join(&name)).unwrap(),
            fs::read(ordinary.join(name)).unwrap()
        );
    }
    let reader = RelationalRowPageRootReader::open_generation(
        &controlled,
        base.manifest().generation + 1,
        RelationalRowPagePublicationConfig::default(),
    )
    .unwrap();
    reader.scrub_physical_pages().unwrap();
    let descriptors = collect_descriptors(&reader, "documents");
    assert_eq!(page_id_values(&descriptors), (1..1025).collect::<Vec<_>>());
    for descriptor in descriptors {
        let id = descriptor.logical_page_id.get();
        let mut expected = page(
            id,
            if id % 37 == 0 {
                base.manifest().generation + 1
            } else {
                id.div_ceil(setup_batch_size())
            },
            if id % 37 == 0 { 11 } else { 10 },
            id as i64,
            id as i64,
        );
        if id % 37 == 0 {
            expected.rows[0].row = RelationalRow::new(vec![
                RelationalValue::BigInt(id as i64),
                RelationalValue::Text(format!("updated-{id}")),
            ]);
        }
        assert_eq!(reader.read_page(&descriptor).unwrap(), expected);
    }
    assert_eq!(
        reader
            .manifest()
            .physical_generations
            .iter()
            .map(|entry| (entry.generation, entry.allocated_pages, entry.live_pages))
            .collect::<Vec<_>>(),
        vec![(1, 512, 499), (2, 512, 498), (4, 27, 27)]
    );
    probe.assert_released(&local);
    assert_no_temporary_files(&controlled);
    fs::remove_dir_all(ordinary).unwrap();
    fs::remove_dir_all(controlled).unwrap();
}

#[test]
fn checkpoint_units_row_root_wide_key_reads_hashes_and_comparisons_match_and_cancel_each_actual_unit(
) {
    let directory = unique_test_dir("root-wide-keys");
    let config = RelationalRowPagePublicationConfig {
        page_limits: RelationalRowPageLimits {
            max_page_bytes: std::num::NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
            max_key_bytes: std::num::NonZeroUsize::new(512 * 1024).unwrap(),
            ..RelationalRowPageLimits::default()
        },
        ..RelationalRowPagePublicationConfig::default()
    };
    let mut pages = vec![page(1, 1, 10, 1, 1), page(2, 1, 10, 2, 2)];
    for (index, page) in pages.iter_mut().enumerate() {
        page.rows[0].primary_key = RelationalKey(vec![RelationalValue::Text(format!(
            "{}{}",
            "common-界\0".repeat(12 * 1024 + 3),
            index
        ))]);
    }
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", pages)],
        )
        .unwrap();
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let expected = collect_descriptors(&reader, "documents");
    let before = images(&directory, reader.manifest().generation);
    assert!(expected
        .iter()
        .all(|descriptor| descriptor.lower_bound.len() > 64 * 1024));
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        visit(&reader, &baseline.context(local.clone())).unwrap(),
        expected
    );
    let units = baseline.completed.load(Ordering::SeqCst);
    let waves = baseline.io_waves.load(Ordering::SeqCst);
    baseline.assert_released(&local);
    assert!(waves > 14);
    for io in [false, true] {
        for limit in 1..=if io { waves } else { units } {
            let probe = Arc::new(CheckpointWorkProbe::default());
            if io {
                probe.cancel_on_io_wave.store(limit, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(limit, Ordering::SeqCst);
            }
            let error = visit(&reader, &probe.context(local.clone())).unwrap_err();
            assert!(
                matches!(error, RelationalRowPagePublicationError::Admission(_)),
                "{error:?}"
            );
            probe.assert_released(&local);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(
                visit(&reader, &retry.context(local.clone())).unwrap(),
                expected
            );
            retry.assert_released(&local);
        }
    }
    assert_eq!(images(&directory, reader.manifest().generation), before);
    for descriptor in &expected {
        reader.read_page(descriptor).unwrap();
    }
    fs::remove_dir_all(directory).unwrap();
}
