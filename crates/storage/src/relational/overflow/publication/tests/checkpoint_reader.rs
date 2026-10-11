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

fn all_base(directory: &Path, fixture: &Fixture) -> RelationalOverflowRootReader {
    let config = RelationalOverflowPublicationConfig::default();
    RelationalOverflowPublisher::new(config)
        .publish(
            directory,
            1,
            10,
            None,
            fixture.iter().map(|(input, _)| input.clone()).collect(),
        )
        .unwrap();
    RelationalOverflowRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap()
}

#[test]
fn checkpoint_units_overflow_reference_lookup_verifies_all_1025_values_and_preserves_metadata_errors(
) {
    let fixture = (0u32..1025)
        .map(|ordinal| {
            let bytes = ordinal.to_le_bytes().to_vec();
            (
                encoded_input(RelationalScalarType::Bytea, &bytes).0,
                RelationalValue::Bytea(bytes),
            )
        })
        .collect::<Fixture>();
    let directory = unique_test_dir("lookup-all-values");
    let reader = all_base(&directory, &fixture);
    let before = authority(&directory);
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    for (input, expected) in &fixture {
        assert!(reader
            .contains_with_work_context(input.reference(), &work)
            .unwrap());
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
    let absent = encoded_input(RelationalScalarType::Bytea, b"absent from this generation").0;
    assert!(!reader
        .contains_with_work_context(absent.reference(), &work)
        .unwrap());
    let mut changed = *fixture[0].0.reference();
    changed.scalar_type = RelationalScalarType::Text;
    let expected = reader.contains(&changed).unwrap_err();
    let actual = reader
        .contains_with_work_context(&changed, &work)
        .unwrap_err();
    assert_eq!(actual.to_string(), expected.to_string());
    probe.assert_released(&local);
    assert_eq!(authority(&directory), before);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_reference_lookup_cancels_every_cpu_io_unit_denial_and_full_retry() {
    let directory = unique_test_dir("lookup-cancel-retry");
    let fixture = fixture();
    let reader = all_base(&directory, &fixture);
    let before = authority(&directory);
    let local = scheduler();
    for (input, expected) in &fixture {
        let baseline = Arc::new(CheckpointWorkProbe::default());
        assert!(reader
            .contains_with_work_context(input.reference(), &baseline.context(local.clone()))
            .unwrap());
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
                let error = reader
                    .contains_with_work_context(input.reference(), &probe.context(local.clone()))
                    .unwrap_err();
                assert!(error.to_string().contains("stopped"), "{error:?}");
                probe.assert_released(&local);
                assert_eq!(authority(&directory), before);
                let retry = Arc::new(CheckpointWorkProbe::default());
                assert!(reader
                    .contains_with_work_context(input.reference(), &retry.context(local.clone()))
                    .unwrap());
                retry.assert_released(&local);
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
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error = reader
        .contains_with_work_context(fixture[0].0.reference(), &work)
        .unwrap_err();
    assert!(
        error.to_string().contains("admission deferred"),
        "{error:?}"
    );
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    drop(held);
    probe.assert_released(&local);
    assert_eq!(authority(&directory), before);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert!(reader
        .contains_with_work_context(fixture[0].0.reference(), &retry.context(local.clone()))
        .unwrap());
    retry.assert_released(&local);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_encoded_reads_match_complete_bytes_and_each_unit_cancellation() {
    let directory = unique_test_dir("encoded-read-units");
    let fixture = fixture();
    let reader = all_base(&directory, &fixture);
    let before = authority(&directory);
    let local = scheduler();
    for (input, _) in &fixture {
        let descriptor = reader.find_descriptor(input.reference()).unwrap().unwrap();
        let expected = reader.read_encoded_extent(&descriptor).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let encoded = reader
            .read_encoded_extent_with_work_context(&descriptor, &probe.context(local.clone()))
            .unwrap();
        assert_eq!(encoded.as_slice(), expected.as_ref());
        let RelationalOverflowExtentInput::Write {
            encoded: original, ..
        } = input
        else {
            unreachable!()
        };
        assert_eq!(encoded.as_slice(), original.as_ref());
        let blocks = encoded.len().div_ceil(64 * 1024);
        assert_eq!(probe.io_waves.load(Ordering::SeqCst), blocks + 3);
        assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
        let units = probe.completed.load(Ordering::SeqCst);
        assert_eq!(units, blocks * 3 + 7);
        probe.assert_released(&local);
        for stop in 1..=units {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(stop, Ordering::SeqCst);
            let error = reader
                .read_encoded_extent_with_work_context(&descriptor, &probe.context(local.clone()))
                .unwrap_err();
            assert!(error.to_string().contains("stopped"), "{error:?}");
            assert_eq!(probe.completed.load(Ordering::SeqCst), stop);
            probe.assert_released(&local);
            assert_eq!(authority(&directory), before);
            let retry = Arc::new(CheckpointWorkProbe::default());
            assert_eq!(
                reader
                    .read_encoded_extent_with_work_context(
                        &descriptor,
                        &retry.context(local.clone())
                    )
                    .unwrap()
                    .as_slice(),
                expected.as_ref()
            );
            retry.assert_released(&local);
        }
        for stop in 1..=blocks + 3 {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_on_io_wave.store(stop, Ordering::SeqCst);
            let error = reader
                .read_encoded_extent_with_work_context(&descriptor, &probe.context(local.clone()))
                .unwrap_err();
            assert!(
                error.to_string().contains("checkpoint build I/O stopped"),
                "{error:?}"
            );
            probe.assert_released(&local);
            assert_eq!(authority(&directory), before);
        }
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let permit = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let descriptor = reader
        .find_descriptor(fixture[3].0.reference())
        .unwrap()
        .unwrap();
    let error = reader
        .read_encoded_extent_with_work_context(&descriptor, &work)
        .unwrap_err();
    assert!(
        error.to_string().contains("admission deferred"),
        "{error:?}"
    );
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    assert_eq!(authority(&directory), before);
    drop(permit);
    probe.assert_released(&local);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_encoded_reads_preserve_range_metadata_and_checksum_errors() {
    let directory = unique_test_dir("encoded-read-errors");
    let fixture = fixture();
    let reader = all_base(&directory, &fixture);
    let descriptor = reader
        .find_descriptor(fixture[3].0.reference())
        .unwrap()
        .unwrap();
    let local = scheduler();
    let compare = |descriptor: &RelationalOverflowExtentDescriptor| {
        let expected = reader.read_encoded_extent(descriptor).unwrap_err();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = reader
            .read_encoded_extent_with_work_context(descriptor, &probe.context(local.clone()))
            .unwrap_err();
        assert_eq!(actual.to_string(), expected.to_string());
        probe.assert_released(&local);
    };
    let mut invalid = descriptor;
    invalid.physical_offset = u64::MAX;
    compare(&invalid);
    invalid = descriptor;
    invalid.envelope_bytes += 1;
    invalid.physical_offset = fs::metadata(directory.join(relational_overflow_extent_file(1)))
        .unwrap()
        .len();
    compare(&invalid);
    invalid = descriptor;
    invalid.envelope_crc32c ^= 1;
    compare(&invalid);
    invalid = descriptor;
    invalid.reference.digest = hawdb_integrity::integrity_digest(b"wrong content identity").sha256;
    compare(&invalid);
    let extent_path = directory.join(relational_overflow_extent_file(1));
    let original = fs::read(&extent_path).unwrap();
    let mut corrupt = original.clone();
    corrupt[descriptor.physical_offset as usize + 17] ^= 1;
    fs::write(&extent_path, corrupt).unwrap();
    compare(&descriptor);
    fs::write(&extent_path, &original).unwrap();
    let saved_path = directory.join("test-owned-saved-extent");
    fs::rename(&extent_path, &saved_path).unwrap();
    compare(&descriptor);
    fs::rename(&saved_path, &extent_path).unwrap();
    assert_eq!(
        reader.read_encoded_extent(&descriptor).unwrap().as_ref(),
        reader
            .read_encoded_extent_with_work_context(
                &descriptor,
                &CheckpointWorkContext::new(RuntimeTaskContext::default())
            )
            .unwrap()
            .as_slice()
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_overflow_exact_compaction_reads_all_base_values_and_matches_complete_artifacts()
{
    let fixture = fixture();
    let ordinary = unique_test_dir("encoded-exact-ordinary");
    let controlled = unique_test_dir("encoded-exact-controlled");
    all_base(&ordinary, &fixture);
    all_base(&controlled, &fixture);
    let before = authority(&controlled);
    let expected = candidate(&ordinary, &fixture, 2, None).unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = candidate(
        &controlled,
        &fixture,
        2,
        Some(&probe.context(local.clone())),
    )
    .unwrap();
    assert_eq!(actual, expected);
    assert!(probe.io_waves.load(Ordering::SeqCst) > 20);
    probe.assert_released(&local);
    assert_eq!(authority(&controlled), before);
    verify(&ordinary, &fixture, 2);
    verify(&controlled, &fixture, 2);
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
    assert_no_temporary_files(&controlled);
    fs::remove_dir_all(ordinary).unwrap();
    fs::remove_dir_all(controlled).unwrap();
}
