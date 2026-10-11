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
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

fn directory(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-checkpoint-reference-{name}-{}-{}",
        std::process::id(),
        NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
    ))
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn reference(seed: u16) -> RelationalOverflowRef {
    let mut digest = [0u8; 32];
    digest[..2].copy_from_slice(&seed.to_be_bytes());
    RelationalOverflowRef {
        digest: Sha256Digest::from_bytes(digest),
        scalar_type: RelationalScalarType::Text,
        compressed_bytes: u64::from(seed) + 1,
        uncompressed_bytes: u64::from(seed) + 2,
    }
}

fn small_set(directory: &Path, spilled: bool) -> RelationalOverflowReferenceSet {
    fs::create_dir_all(directory).unwrap();
    let references = (0..17).map(reference).collect();
    let source = if spilled {
        let paths = (0..3)
            .map(|run| {
                let path = directory.join(format!("run-{run}"));
                let references = (0..17)
                    .filter(|seed| seed % 3 == run || seed % 5 == 0)
                    .map(reference)
                    .collect::<Vec<_>>();
                write_run(&path, &references, 1024).unwrap();
                path
            })
            .collect();
        ReferenceSetSource::Spilled {
            paths,
            config: RelationalOverflowReferenceSortConfig::default(),
        }
    } else {
        ReferenceSetSource::Memory(references)
    };
    RelationalOverflowReferenceSet {
        source,
        report: RelationalOverflowReferenceSortReport {
            unique_references: 17,
            ..Default::default()
        },
    }
}

fn collect(
    set: &RelationalOverflowReferenceSet,
    work: Option<&CheckpointWorkContext>,
) -> Vec<RelationalOverflowRef> {
    let mut values = Vec::new();
    set.visit_with_work_context(
        &mut |value| {
            values.push(value);
            Ok(true)
        },
        work,
    )
    .unwrap();
    values
}

fn images(set: &RelationalOverflowReferenceSet) -> Vec<Vec<u8>> {
    match &set.source {
        ReferenceSetSource::Memory(_) => Vec::new(),
        ReferenceSetSource::Spilled { paths, .. } => {
            paths.iter().map(|path| fs::read(path).unwrap()).collect()
        }
    }
}

#[test]
fn checkpoint_units_overflow_reference_visit_preserves_all_1025_references_and_nested_admission() {
    for spilled in [false, true] {
        let directory = directory("large");
        let mut config = RelationalOverflowReferenceSortConfig::default();
        if spilled {
            config.max_memory_bytes = NonZeroUsize::new(64 * 1024).unwrap();
        }
        let mut builder =
            RelationalOverflowReferenceSetBuilder::new(&directory, 1, config).unwrap();
        for seed in (0..1025).rev().chain(0..1025) {
            builder.push(reference(seed)).unwrap();
        }
        let set = builder.finish().unwrap();
        assert_eq!(set.report.unique_references, 1025);
        assert_eq!(set.report.spill_run_count > 1, spilled);
        let before = images(&set);
        let expected = collect(&set, None);
        assert_eq!(expected, (0..1025).map(reference).collect::<Vec<_>>());
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let mut actual = Vec::new();
        set.visit_with_work_context(
            &mut |value| {
                probe.assert_released(&local);
                let unit = work.start_unit().map_err(work_error)?;
                actual.push(value);
                unit.finish();
                Ok(true)
            },
            Some(&work),
        )
        .unwrap();
        assert_eq!(actual, expected);
        assert_eq!(images(&set), before);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst) > 0, spilled);
        probe.assert_released(&local);
        drop(set);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn checkpoint_units_overflow_reference_visit_cancels_each_actual_unit_and_io_then_retries() {
    for spilled in [false, true] {
        let directory = directory("cancel");
        let set = small_set(&directory, spilled);
        let before = images(&set);
        let expected = collect(&set, None);
        let local = scheduler();
        let baseline = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            collect(&set, Some(&baseline.context(local.clone()))),
            expected
        );
        let units = baseline.completed.load(Ordering::SeqCst);
        let waves = baseline.io_waves.load(Ordering::SeqCst);
        baseline.assert_released(&local);
        assert!(units >= expected.len());
        for (stop, on_io) in (1..=units)
            .map(|stop| (stop, false))
            .chain((1..=waves).map(|stop| (stop, true)))
        {
            let probe = Arc::new(CheckpointWorkProbe::default());
            if on_io {
                probe.cancel_on_io_wave.store(stop, Ordering::SeqCst);
            } else {
                probe.cancel_after.store(stop, Ordering::SeqCst);
            }
            let error = set
                .visit_with_work_context(&mut |_| Ok(true), Some(&probe.context(local.clone())))
                .unwrap_err();
            assert!(error.to_string().contains("stopped"), "{error:?}");
            probe.assert_released(&local);
            assert_eq!(images(&set), before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(collect(&set, Some(&retry.context(local.clone()))), expected);
            retry.assert_released(&local);
        }
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let error = set
            .visit_with_work_context(&mut |_| Ok(true), Some(&work))
            .unwrap_err();
        assert!(
            error.to_string().contains("admission deferred"),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        assert_eq!(images(&set), before);
        drop(set);
        fs::remove_dir_all(directory).unwrap();
    }
}

#[test]
fn checkpoint_units_overflow_reference_visit_preserves_corruption_errors_and_early_stop() {
    let directory = directory("errors");
    let set = small_set(&directory, true);
    let ReferenceSetSource::Spilled { paths, .. } = &set.source else {
        unreachable!()
    };
    let original = fs::read(&paths[1]).unwrap();
    let mut bad_header = original.clone();
    bad_header[0] ^= 1;
    let mut reserved = original.clone();
    reserved[RUN_HEADER.len() + 33] = 1;
    let mut checksum = original.clone();
    checksum[RUN_HEADER.len() + 56] ^= 1;
    let mut conflicting = original.clone();
    let mut altered = reference(0);
    altered.uncompressed_bytes += 1;
    conflicting[RUN_HEADER.len()..RUN_HEADER.len() + RUN_RECORD_BYTES as usize]
        .copy_from_slice(&encode_reference(altered).unwrap());
    for corrupted in [
        bad_header,
        reserved,
        checksum,
        original[..3].to_vec(),
        original[..original.len() - 1].to_vec(),
        conflicting,
    ] {
        fs::write(&paths[1], corrupted).unwrap();
        let expected = set.visit(&mut |_| Ok(true)).unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = set
            .visit_with_work_context(&mut |_| Ok(true), Some(&probe.context(local.clone())))
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        probe.assert_released(&local);
    }
    fs::write(&paths[1], original).unwrap();
    let expected = collect(&set, None);
    let before = images(&set);
    for stop in [1, 7, 17] {
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let mut actual = Vec::new();
        set.visit_with_work_context(
            &mut |value| {
                actual.push(value);
                Ok(actual.len() < stop)
            },
            Some(&work),
        )
        .unwrap();
        assert_eq!(actual, expected[..stop]);
        probe.assert_released(&local);
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let mut visited = 0;
        let error = set
            .visit_with_work_context(
                &mut |_| {
                    visited += 1;
                    if visited == stop {
                        probe.cancellation.cancel();
                    }
                    Ok(visited < stop)
                },
                Some(&work),
            )
            .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        assert_eq!(visited, stop);
        probe.assert_released(&local);
        assert_eq!(images(&set), before);
    }
    drop(set);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_reference_visit_publishes_spilled_closure_with_all_values_and_bytes() {
    use crate::relational::overflow::{
        encode_overflow_envelope, relational_overflow_descriptor_file,
        relational_overflow_extent_file, relational_overflow_manifest_generation_file,
        RelationalHydrationBudget, RelationalOverflowConfig,
        RelationalOverflowExactGenerationRequest, RelationalOverflowExtentInput,
        RelationalOverflowPublicationConfig, RelationalOverflowPublisher,
        RelationalOverflowRootReader, RELATIONAL_OVERFLOW_MANIFEST_FILE,
    };
    use crate::relational::RelationalValue;

    let values = (0..1025)
        .map(|seed| {
            let value = format!("value-{seed:04}-{}", "界🙂".repeat(12));
            let encoded = encode_overflow_envelope(
                RelationalScalarType::Text,
                value.as_bytes(),
                RelationalOverflowConfig::default(),
            )
            .unwrap();
            (
                RelationalOverflowExtentInput::Write {
                    reference: encoded.reference,
                    encoded: encoded.bytes,
                },
                value,
            )
        })
        .collect::<Vec<_>>();
    let ordinary = directory("publish-ordinary");
    let controlled = directory("publish-controlled");
    let config = RelationalOverflowPublicationConfig::default();
    let mut reports = Vec::new();
    for (path, admitted) in [(&ordinary, false), (&controlled, true)] {
        RelationalOverflowPublisher::new(config)
            .publish(
                path,
                1,
                10,
                None,
                values.iter().map(|(input, _)| input.clone()).collect(),
            )
            .unwrap();
        let authority_names = [
            RELATIONAL_OVERFLOW_MANIFEST_FILE.to_string(),
            relational_overflow_manifest_generation_file(1),
            relational_overflow_extent_file(1),
            relational_overflow_descriptor_file(1),
        ];
        let before = authority_names
            .iter()
            .map(|name| fs::read(path.join(name)).unwrap())
            .collect::<Vec<_>>();
        let sort = RelationalOverflowReferenceSortConfig {
            max_memory_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
            ..Default::default()
        };
        let mut builder = RelationalOverflowReferenceSetBuilder::new(path, 2, sort).unwrap();
        for (input, _) in values.iter().rev().chain(values.iter()) {
            builder.push(*input.reference()).unwrap();
        }
        let references = builder.finish().unwrap();
        assert!(references.report().spill_run_count > 1);
        let source_images = images(&references);
        let base = RelationalOverflowRootReader::open_latest(path, config)
            .unwrap()
            .unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let mut publisher = RelationalOverflowPublisher::new(config);
        if admitted {
            publisher = publisher.with_work_context(&probe.context(local.clone()));
        }
        let report = publisher
            .persist_generation_exact_references(
                RelationalOverflowExactGenerationRequest {
                    directory: path,
                    generation: 2,
                    source_commit_epoch: 11,
                    base: &base,
                    expected_previous_generation: 1,
                    references: &references,
                    task: &RuntimeTaskContext::default(),
                },
                |_| panic!("all selected references must copy their captured base"),
            )
            .unwrap();
        assert_eq!(report.copied_base_extent_count, 1025);
        assert_eq!(report.introduced_extent_count, 0);
        assert_eq!(images(&references), source_images);
        assert_eq!(
            authority_names
                .iter()
                .map(|name| fs::read(path.join(name)).unwrap())
                .collect::<Vec<_>>(),
            before
        );
        probe.assert_released(&local);
        let reader = RelationalOverflowRootReader::open_generation(path, 2, config).unwrap();
        assert_eq!(reader.manifest().extent_count, 1025);
        for (input, expected) in &values {
            assert_eq!(
                reader
                    .hydrate(
                        input.reference(),
                        &mut RelationalHydrationBudget::default(),
                        None
                    )
                    .unwrap(),
                RelationalValue::Text(expected.clone())
            );
        }
        reports.push(report);
        drop(references);
    }
    assert_eq!(reports[0], reports[1]);
    for name in [
        relational_overflow_extent_file(2),
        relational_overflow_descriptor_file(2),
        relational_overflow_manifest_generation_file(2),
    ] {
        assert_eq!(
            fs::read(ordinary.join(&name)).unwrap(),
            fs::read(controlled.join(&name)).unwrap()
        );
    }
    fs::remove_dir_all(ordinary).unwrap();
    fs::remove_dir_all(controlled).unwrap();
}
