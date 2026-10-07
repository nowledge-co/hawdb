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
use std::path::Path;
use std::sync::Arc;

#[path = "checkpoint_publication.rs"]
mod operations;

#[path = "checkpoint_reader.rs"]
mod encoded_reads;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

type Fixture = Vec<(RelationalOverflowExtentInput, RelationalValue)>;

fn fixture() -> Fixture {
    let mut binary = Vec::new();
    let mut random = 0x1234_5678u32;
    for _ in 0..3 * 64 * 1024 + 17 {
        random ^= random << 13;
        random ^= random >> 17;
        random ^= random << 5;
        binary.push(random as u8);
    }
    let text = format!("{}🙂{}", "a".repeat(64 * 1024 - 1), "界é🙂".repeat(20000));
    [
        RelationalValue::Text("retained base".into()),
        RelationalValue::Bytea(b"unmentioned base".to_vec()),
        RelationalValue::Text(text),
        RelationalValue::Bytea(binary),
    ]
    .into_iter()
    .map(|value| {
        let (scalar, bytes) = match &value {
            RelationalValue::Text(text) => (RelationalScalarType::Text, text.as_bytes()),
            RelationalValue::Bytea(bytes) => (RelationalScalarType::Bytea, bytes.as_slice()),
            _ => unreachable!(),
        };
        (encoded_input(scalar, bytes).0, value)
    })
    .collect()
}

fn setup(directory: &Path, fixture: &Fixture) {
    RelationalOverflowPublisher::new(RelationalOverflowPublicationConfig::default())
        .publish(
            directory,
            1,
            10,
            None,
            vec![fixture[0].0.clone(), fixture[1].0.clone()],
        )
        .unwrap();
}

fn candidate(
    directory: &Path,
    fixture: &Fixture,
    mode: usize,
    work: Option<&CheckpointWorkContext>,
) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
    candidate_generation(directory, fixture, mode, work, 2)
}

fn candidate_generation(
    directory: &Path,
    fixture: &Fixture,
    mode: usize,
    work: Option<&CheckpointWorkContext>,
    generation: u64,
) -> Result<RelationalOverflowPublicationReport, RelationalOverflowPublicationError> {
    let config = RelationalOverflowPublicationConfig::default();
    let mut publisher = RelationalOverflowPublisher::new(config);
    if let Some(work) = work {
        publisher = publisher.with_work_context(work);
    }
    let base = RelationalOverflowRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap();
    let inputs = [0, 2, 3].map(|index| fixture[index].0.clone()).to_vec();
    match mode {
        0 => publisher.persist_generation(directory, generation, 11, Some(&base), Some(1), inputs),
        1 => {
            publisher.persist_generation_retaining_base(directory, generation, 11, &base, 1, inputs)
        }
        2 => {
            let mut references = RelationalOverflowReferenceSetBuilder::new(
                directory,
                generation,
                RelationalOverflowReferenceSortConfig::default(),
            )
            .unwrap();
            for input in &inputs {
                references.push(*input.reference()).unwrap();
            }
            let references = references.finish().unwrap();
            publisher
                .persist_generation_exact_references(
                    RelationalOverflowExactGenerationRequest {
                        directory,
                        generation,
                        source_commit_epoch: 11,
                        base: &base,
                        expected_previous_generation: 1,
                        references: &references,
                        task: &RuntimeTaskContext::default(),
                    },
                    |reference| {
                        Ok(inputs.iter().find_map(|input| match input {
                            RelationalOverflowExtentInput::Write {
                                reference: candidate,
                                encoded,
                            } if candidate == reference => Some(Arc::clone(encoded)),
                            _ => None,
                        }))
                    },
                )
                .map(|report| report.publication)
        }
        3 => publisher.publish(directory, generation, 11, Some(1), inputs),
        _ => unreachable!(),
    }
}

fn authority(directory: &Path) -> Vec<Vec<u8>> {
    [
        RELATIONAL_OVERFLOW_MANIFEST_FILE.to_string(),
        relational_overflow_manifest_generation_file(1),
        relational_overflow_extent_file(1),
        relational_overflow_descriptor_file(1),
    ]
    .into_iter()
    .map(|name| fs::read(directory.join(name)).unwrap())
    .collect()
}

fn verify(directory: &Path, fixture: &Fixture, mode: usize) {
    verify_generation(directory, fixture, mode, 2)
}

fn verify_generation(directory: &Path, fixture: &Fixture, mode: usize, generation: u64) {
    let reader = RelationalOverflowRootReader::open_generation(
        directory,
        generation,
        RelationalOverflowPublicationConfig::default(),
    )
    .unwrap();
    assert_eq!(
        reader.manifest().extent_count,
        if mode == 1 { 4 } else { 3 }
    );
    for (index, (input, expected)) in fixture.iter().enumerate() {
        if index == 1 && mode != 1 {
            assert!(!reader.contains(input.reference()).unwrap());
        } else {
            assert_eq!(
                reader
                    .hydrate(
                        input.reference(),
                        &mut RelationalHydrationBudget::default(),
                        None
                    )
                    .unwrap(),
                *expected
            );
        }
    }
    assert_no_temporary_files(directory);
}

#[test]
fn checkpoint_units_overflow_publication_validation_preserves_all_modes_and_complete_artifacts() {
    let fixture = fixture();
    for mode in 0..3 {
        let ordinary = unique_test_dir("validation-ordinary");
        let controlled = unique_test_dir("validation-controlled");
        setup(&ordinary, &fixture);
        setup(&controlled, &fixture);
        let before = authority(&controlled);
        let expected = candidate(&ordinary, &fixture, mode, None).unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let report = candidate(
            &controlled,
            &fixture,
            mode,
            Some(&probe.context(local.clone())),
        )
        .unwrap();
        assert_eq!(report, expected);
        for name in [
            relational_overflow_extent_file(2),
            relational_overflow_descriptor_file(2),
            relational_overflow_manifest_generation_file(2),
        ] {
            assert_eq!(
                fs::read(ordinary.join(&name)).unwrap(),
                fs::read(controlled.join(name)).unwrap()
            );
        }
        assert_eq!(authority(&controlled), before);
        verify(&controlled, &fixture, mode);
        probe.assert_released(&local);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        assert!(probe.completed.load(Ordering::SeqCst) > 3);
        fs::remove_dir_all(ordinary).unwrap();
        fs::remove_dir_all(controlled).unwrap();
    }
}

#[test]
fn checkpoint_units_overflow_publication_validation_cancellation_and_denial_preserve_base_then_retry(
) {
    let fixture = fixture();
    for mode in 0..3 {
        let baseline = unique_test_dir("validation-baseline");
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
        let units = probe.completed.load(Ordering::SeqCst);
        probe.assert_released(&local);
        fs::remove_dir_all(baseline).unwrap();
        for stop in [0, 1, units / 2, units] {
            let directory = unique_test_dir("validation-cancel-retry");
            setup(&directory, &fixture);
            let before = authority(&directory);
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            let work = probe.context(local.clone());
            let held = (stop == 0).then(|| {
                local
                    .try_start(WorkRequest::background(WorkClass::Mutation, 1))
                    .unwrap()
            });
            let error = candidate(&directory, &fixture, mode, Some(&work)).unwrap_err();
            assert!(
                error.to_string().contains(if stop == 0 {
                    "admission deferred"
                } else {
                    "checkpoint build stopped"
                }),
                "{error:?}"
            );
            drop(held);
            probe.assert_released(&local);
            assert_eq!(authority(&directory), before);
            if directory
                .join(relational_overflow_manifest_generation_file(2))
                .exists()
            {
                verify(&directory, &fixture, mode);
            }
            assert_no_temporary_files(&directory);
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
fn checkpoint_units_overflow_publication_preserves_unowned_temporaries_in_every_mode() {
    let fixture = fixture();
    for controlled in [false, true] {
        for mode in 0..4 {
            for stage in 0..4 {
                let directory = unique_test_dir("unowned-evidence");
                setup(&directory, &fixture);
                let before = authority(&directory);
                let names = [
                    relational_overflow_extent_file(2),
                    relational_overflow_descriptor_file(2),
                    relational_overflow_manifest_generation_file(2),
                    RELATIONAL_OVERFLOW_MANIFEST_FILE.into(),
                ];
                let evidence = directory.join(&names[stage]).with_extension("hawdb.tmp");
                let bytes = b"interrupted candidate evidence: preserve all bytes";
                fs::write(&evidence, bytes).unwrap();
                let local = scheduler();
                let probe = Arc::new(CheckpointWorkProbe::default());
                let work = controlled.then(|| probe.context(local.clone()));
                let result = candidate(&directory, &fixture, mode, work.as_ref());
                if stage == 3 && mode != 3 {
                    result.unwrap();
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        matches!(error, RelationalOverflowPublicationError::Durability(_)),
                        "{error:?}"
                    );
                }
                assert_eq!(fs::read(&evidence).unwrap(), bytes);
                assert_eq!(authority(&directory), before);
                let remaining = fs::read_dir(&directory)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension() == Some(std::ffi::OsStr::new("tmp")))
                    .collect::<Vec<_>>();
                assert_eq!(remaining, vec![evidence.clone()]);
                for name in names.iter().take(3) {
                    assert_eq!(directory.join(name).exists(), stage == 3);
                }
                probe.assert_released(&local);
                // The test owns this fixture file; the publisher never did.
                fs::remove_file(&evidence).unwrap();
                if stage == 3 {
                    verify(&directory, &fixture, mode);
                } else {
                    let retry = Arc::new(CheckpointWorkProbe::default());
                    candidate(
                        &directory,
                        &fixture,
                        mode,
                        Some(&retry.context(local.clone())),
                    )
                    .unwrap();
                    verify(&directory, &fixture, mode);
                    retry.assert_released(&local);
                }
                fs::remove_dir_all(directory).unwrap();
            }
        }
    }
}

#[test]
fn checkpoint_units_overflow_publication_denial_never_claims_existing_evidence() {
    let fixture = fixture();
    for mode in 0..4 {
        let directory = unique_test_dir("unowned-denied");
        setup(&directory, &fixture);
        let before = authority(&directory);
        let evidence = directory
            .join(relational_overflow_extent_file(2))
            .with_extension("hawdb.tmp");
        fs::write(&evidence, b"prior unpublished data").unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(local.clone());
        let held = local
            .try_start(WorkRequest::background(WorkClass::Mutation, 1))
            .unwrap();
        let error = candidate(&directory, &fixture, mode, Some(&work)).unwrap_err();
        assert!(
            error.to_string().contains("admission deferred"),
            "{error:?}"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
        drop(held);
        probe.assert_released(&local);
        assert_eq!(fs::read(&evidence).unwrap(), b"prior unpublished data");
        assert_eq!(authority(&directory), before);
        for name in [
            relational_overflow_extent_file(2),
            relational_overflow_descriptor_file(2),
            relational_overflow_manifest_generation_file(2),
        ] {
            assert!(!directory.join(name).exists());
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
