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
use std::num::NonZeroUsize;

#[test]
fn checkpoint_units_overflow_publication_sorts_and_publishes_all_1025_values_with_identical_bytes()
{
    let fixture = (0u32..1025)
        .rev()
        .map(|number| {
            let bytes = number.to_le_bytes().to_vec();
            (
                encoded_input(RelationalScalarType::Bytea, &bytes).0,
                RelationalValue::Bytea(bytes),
            )
        })
        .collect::<Fixture>();
    let ordinary = unique_test_dir("operations-ordinary");
    let controlled = unique_test_dir("operations-controlled");
    let config = RelationalOverflowPublicationConfig::default();
    let inputs = fixture
        .iter()
        .map(|entry| entry.0.clone())
        .collect::<Vec<_>>();
    let report = RelationalOverflowPublisher::new(config)
        .persist_generation(&ordinary, 1, 10, None, None, inputs.clone())
        .unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = RelationalOverflowPublisher::new(config)
        .with_work_context(&probe.context(local.clone()))
        .persist_generation(&controlled, 1, 10, None, None, inputs)
        .unwrap();
    assert_eq!(actual, report);
    for name in [
        relational_overflow_extent_file(1),
        relational_overflow_descriptor_file(1),
        relational_overflow_manifest_generation_file(1),
    ] {
        assert_eq!(
            fs::read(ordinary.join(&name)).unwrap(),
            fs::read(controlled.join(name)).unwrap()
        );
    }
    let reader = RelationalOverflowRootReader::open_generation(&controlled, 1, config).unwrap();
    assert_eq!(reader.manifest().extent_count, 1025);
    for (input, expected) in fixture {
        assert_eq!(
            reader
                .hydrate(
                    input.reference(),
                    &mut RelationalHydrationBudget::default(),
                    None
                )
                .unwrap(),
            expected
        );
    }
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert!(probe.io_waves.load(Ordering::SeqCst) > 2050);
    probe.assert_released(&local);
    assert_no_temporary_files(&controlled);
    fs::remove_dir_all(ordinary).unwrap();
    fs::remove_dir_all(controlled).unwrap();
}

#[test]
fn checkpoint_units_overflow_publication_cancels_every_io_wave_and_preserves_authority_and_retry() {
    let fixture = fixture();
    for mode in 0..3 {
        let baseline = unique_test_dir("operations-io-baseline");
        setup(&baseline, &fixture);
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        candidate(
            &baseline,
            &fixture,
            mode,
            Some(&probe.context(local.clone())),
        )
        .unwrap();
        let waves = probe.io_waves.load(Ordering::SeqCst);
        assert!(waves > 10);
        probe.assert_released(&local);
        fs::remove_dir_all(baseline).unwrap();
        for stop in 1..=waves {
            let directory = unique_test_dir("operations-io-cancel-retry");
            setup(&directory, &fixture);
            let before = authority(&directory);
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_on_io_wave.store(stop, Ordering::SeqCst);
            let error = candidate(
                &directory,
                &fixture,
                mode,
                Some(&probe.context(local.clone())),
            )
            .unwrap_err();
            assert!(
                error.to_string().contains("checkpoint build I/O stopped"),
                "{error:?}"
            );
            assert!(probe.cancellation.is_cancelled());
            probe.assert_released(&local);
            assert_eq!(authority(&directory), before);
            assert_no_temporary_files(&directory);
            if directory
                .join(relational_overflow_manifest_generation_file(2))
                .exists()
            {
                verify(&directory, &fixture, mode);
            }
            let retry = Arc::new(CheckpointWorkProbe::default());
            candidate_generation(
                &directory,
                &fixture,
                mode,
                Some(&retry.context(local.clone())),
                3,
            )
            .unwrap();
            assert_eq!(authority(&directory), before);
            verify_generation(&directory, &fixture, mode, 3);
            retry.assert_released(&local);
            fs::remove_dir_all(directory).unwrap();
        }
    }
}

#[test]
fn checkpoint_units_overflow_publication_cancellation_after_selection_retains_complete_result() {
    let fixture = fixture();
    let baseline = unique_test_dir("operations-selected-baseline");
    setup(&baseline, &fixture);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    candidate(&baseline, &fixture, 3, Some(&probe.context(local.clone()))).unwrap();
    let units = probe.completed.load(Ordering::SeqCst);
    probe.assert_released(&local);
    fs::remove_dir_all(baseline).unwrap();
    let directory = unique_test_dir("operations-lost-selected-reply");
    setup(&directory, &fixture);
    let old = authority(&directory);
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(units, Ordering::SeqCst);
    let error =
        candidate(&directory, &fixture, 3, Some(&probe.context(local.clone()))).unwrap_err();
    assert!(
        error.to_string().contains("checkpoint build stopped"),
        "{error:?}"
    );
    probe.assert_released(&local);
    let selected = RelationalOverflowRootReader::open_latest(
        &directory,
        RelationalOverflowPublicationConfig::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(selected.manifest().generation, 2);
    verify(&directory, &fixture, 3);
    assert_eq!(&authority(&directory)[1..], &old[1..]);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_publication_preserves_first_duplicate_and_sort_cancellation() {
    let config = RelationalOverflowPublicationConfig::default();
    let mut inputs = (0u32..257)
        .map(|value| encoded_input(RelationalScalarType::Bytea, &value.to_le_bytes()).0)
        .collect::<Vec<_>>();
    let expected = inputs
        .iter()
        .map(|input| input.reference().digest)
        .min()
        .unwrap();
    let lowest = inputs
        .iter()
        .find(|input| input.reference().digest == expected)
        .unwrap()
        .clone();
    inputs.push(inputs[0].clone());
    inputs.push(lowest);
    let ordinary = unique_test_dir("operations-duplicate-ordinary");
    let reference = RelationalOverflowPublisher::new(config)
        .persist_generation(&ordinary, 1, 10, None, None, inputs.clone())
        .unwrap_err();
    assert!(reference.to_string().contains(&expected.to_string()));
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let controlled = unique_test_dir("operations-duplicate-controlled");
    let actual = RelationalOverflowPublisher::new(config)
        .with_work_context(&probe.context(local.clone()))
        .persist_generation(&controlled, 1, 10, None, None, inputs.clone())
        .unwrap_err();
    assert_eq!(actual.to_string(), reference.to_string());
    assert!(!ordinary.exists() && !controlled.exists());
    let units = probe.completed.load(Ordering::SeqCst);
    probe.assert_released(&local);
    for stop in [1, units / 4, units / 2, units - 1] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        let error = RelationalOverflowPublisher::new(config)
            .with_work_context(&probe.context(local.clone()))
            .persist_generation(&controlled, 1, 10, None, None, inputs.clone())
            .unwrap_err();
        assert!(
            error.to_string().contains("checkpoint build stopped"),
            "{error:?}"
        );
        assert!(!controlled.exists());
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_overflow_publication_bounds_manifest_reads_and_defers_contended_lock() {
    let fixture = fixture();
    let config = RelationalOverflowPublicationConfig::default();
    let directory = unique_test_dir("operations-metadata");
    setup(&directory, &fixture);
    let path = directory.join(RELATIONAL_OVERFLOW_MANIFEST_FILE);
    let original = fs::read(&path).unwrap();
    let local = scheduler();
    for size in [0, 7, 207, 209, config.max_manifest_bytes.get() + 1] {
        fs::write(&path, vec![b'x'; size]).unwrap();
        let ordinary =
            super::super::super::manifest::read_manifest_if_exists(&path, config).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = super::super::super::manifest::read_manifest_if_exists_with_work_context(
            &path,
            config,
            Some(&probe.context(local.clone())),
        )
        .unwrap_err();
        assert_eq!(actual.to_string(), ordinary.to_string());
        probe.assert_released(&local);
    }
    fs::write(&path, &original).unwrap();
    let mut limited = config;
    limited.max_manifest_bytes = NonZeroUsize::new(usize::MAX).unwrap();
    let ordinary =
        super::super::super::manifest::read_manifest_if_exists(&path, limited).unwrap_err();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = super::super::super::manifest::read_manifest_if_exists_with_work_context(
        &path,
        limited,
        Some(&probe.context(local.clone())),
    )
    .unwrap_err();
    assert_eq!(actual.to_string(), ordinary.to_string());
    probe.assert_released(&local);
    let before = authority(&directory);
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join(RELATIONAL_OVERFLOW_PUBLICATION_LOCK_FILE))
        .unwrap();
    held.lock().unwrap();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let error =
        candidate(&directory, &fixture, 0, Some(&probe.context(local.clone()))).unwrap_err();
    assert!(
        matches!(error, RelationalOverflowPublicationError::Admission(ref message)
        if message.contains("publication lock is busy")),
        "{error:?}"
    );
    probe.assert_released(&local);
    assert_eq!(authority(&directory), before);
    assert_no_temporary_files(&directory);
    drop(held);
    candidate(&directory, &fixture, 0, Some(&probe.context(local.clone()))).unwrap();
    verify(&directory, &fixture, 0);
    probe.assert_released(&local);
    fs::remove_dir_all(directory).unwrap();
}
