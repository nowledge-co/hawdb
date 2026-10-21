// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use hawdb_integrity::integrity_digest;

const NAMES: [&str; 3] = [
    "search_lexical.17.hawdb",
    "search_lexical.18.hawdb",
    "search_projection_segments.17.hawdb",
];
fn limits() -> MetadataLimits {
    MetadataLimits {
        encoded_bytes: 64 << 10,
        artifacts: 128,
        artifact_bytes: 1 << 20,
    }
}
fn binding() -> ConsumerBinding {
    ConsumerBinding {
        database_uuid: Uuid::from_bytes([1; 16]),
        projection_uuid: Uuid::from_bytes([2; 16]),
        registration_uuid: Uuid::from_bytes([3; 16]),
        checkpoint_uuid: Uuid::from_bytes([4; 16]),
        consumer_id: "main".into(),
    }
}
fn artifacts() -> Vec<ArtifactReference<'static>> {
    NAMES
        .into_iter()
        .enumerate()
        .map(|(index, name)| ArtifactReference {
            name,
            reference: ObjectReference::for_bytes(
                ObjectKind::CheckpointArtifact,
                1,
                &[index as u8],
            ),
        })
        .collect()
}
fn input<'a>(
    binding: &'a ConsumerBinding,
    artifacts: &'a [ArtifactReference<'a>],
) -> MetadataInput<'a> {
    MetadataInput {
        binding,
        source_epoch: 91,
        target_incarnation: NonZeroU64::new(7).unwrap(),
        generation: NonZeroU64::new(19).unwrap(),
        operation: NonZeroU64::new(23).unwrap(),
        component_uuid: Uuid::from_bytes([5; 16]),
        search_manifest: ObjectReference::for_bytes(
            ObjectKind::CheckpointArtifact,
            1,
            b"search head",
        ),
        artifacts,
    }
}
fn encode(input: &MetadataInput<'_>) -> Vec<u8> {
    let mut bytes = Vec::new();
    let length = input
        .encode_to(&mut bytes, limits(), &RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(length, bytes.len());
    assert_eq!(
        input
            .encoded_len(limits(), &RuntimeTaskContext::default())
            .unwrap(),
        bytes.len()
    );
    bytes
}
fn resign(bytes: &mut [u8]) {
    let split = bytes.len() - CHECKSUM_BYTES;
    let checksum = integrity_digest(&bytes[..split]).sha256;
    bytes[split..].copy_from_slice(checksum.as_bytes());
}

#[test]
fn complete_reference_index_roundtrips_and_exact_lookup_never_falls_back() {
    let binding = binding();
    let artifacts = artifacts();
    let input = input(&binding, &artifacts);
    let bytes = encode(&input);
    let view = MetadataView::decode(&bytes, limits(), &RuntimeTaskContext::default()).unwrap();
    assert!(view.matches_binding(&binding));
    assert_eq!(view.source_epoch(), 91);
    assert_eq!(view.target_incarnation().get(), 7);
    assert_eq!(view.generation().get(), 19);
    assert_eq!(view.operation().get(), 23);
    assert_eq!(view.component_uuid(), input.component_uuid);
    assert_eq!(view.search_manifest(), input.search_manifest);
    assert_eq!(view.artifact_count(), 3);
    let mut old_baseline = bytes.clone();
    old_baseline[..MAGIC.len()].copy_from_slice(b"HAWCGM01");
    resign(&mut old_baseline);
    assert!(MetadataView::decode(&old_baseline, limits(), &RuntimeTaskContext::default()).is_err());
    for artifact in artifacts {
        assert_eq!(view.artifact(artifact.name), Some(artifact.reference));
    }
    for name in [
        "",
        "search_lexical.16.hawdb",
        "search_lexical.19.hawdb",
        "../search_lexical.17.hawdb",
        "search_projection_segments.17.hawdb.retry",
    ] {
        assert_eq!(view.artifact(name), None);
    }
    for field in 0..5 {
        let mut changed = binding.clone();
        match field {
            0 => changed.database_uuid = Uuid::from_bytes([6; 16]),
            1 => changed.projection_uuid = Uuid::from_bytes([6; 16]),
            2 => changed.registration_uuid = Uuid::from_bytes([6; 16]),
            3 => changed.checkpoint_uuid = Uuid::from_bytes([6; 16]),
            _ => changed.consumer_id.push('x'),
        }
        assert!(!view.matches_binding(&changed));
    }
}

#[test]
fn every_torn_prefix_and_body_corruption_is_rejected() {
    let binding = binding();
    let artifacts = artifacts();
    let bytes = encode(&input(&binding, &artifacts));
    for length in 0..bytes.len() {
        assert!(
            MetadataView::decode(&bytes[..length], limits(), &RuntimeTaskContext::default())
                .is_err(),
            "prefix {length}"
        );
    }
    for index in 0..bytes.len() {
        let mut damaged = bytes.clone();
        damaged[index] ^= 1;
        assert!(
            MetadataView::decode(&damaged, limits(), &RuntimeTaskContext::default()).is_err(),
            "byte {index}"
        );
    }
}

#[test]
fn valid_checksum_does_not_authorize_bad_offsets_counts_or_reference_types() {
    let binding = binding();
    let artifacts = artifacts();
    let bytes = encode(&input(&binding, &artifacts));
    let view = MetadataView::decode(&bytes, limits(), &RuntimeTaskContext::default()).unwrap();
    let index = bytes.len() - CHECKSUM_BYTES - view.records.len() - view.offsets.len();
    let count = index - 4;
    let incarnation = 8 + 4 * 16 + 2 + binding.consumer_id.len() + 8;
    let reference = incarnation + 3 * 8 + 16;
    for (offset, replacement) in [
        (index, 1u32.to_le_bytes().to_vec()),
        (index + 4, 0u32.to_le_bytes().to_vec()),
        (index + 3 * 4, u32::MAX.to_le_bytes().to_vec()),
        (count, u32::MAX.to_le_bytes().to_vec()),
        (incarnation, 0u64.to_le_bytes().to_vec()),
        (incarnation + 8, 0u64.to_le_bytes().to_vec()),
        (incarnation + 16, 0u64.to_le_bytes().to_vec()),
        (incarnation + 24, vec![0; 16]),
        (reference, vec![ObjectKind::SealedWal as u8]),
        (reference + 1, 2u16.to_le_bytes().to_vec()),
        (
            reference + 3,
            (limits().artifact_bytes + 1).to_le_bytes().to_vec(),
        ),
    ] {
        let mut damaged = bytes.clone();
        damaged[offset..offset + replacement.len()].copy_from_slice(&replacement);
        resign(&mut damaged);
        assert!(
            MetadataView::decode(&damaged, limits(), &RuntimeTaskContext::default()).is_err(),
            "offset {offset}"
        );
    }
    // Duplicated canonical names remain invalid even with valid record offsets.
    let mut damaged = bytes.clone();
    let second = index + view.offsets.len() + view.offset(1).unwrap() + 2;
    damaged[second..second + NAMES[0].len()].copy_from_slice(NAMES[0].as_bytes());
    resign(&mut damaged);
    assert!(MetadataView::decode(&damaged, limits(), &RuntimeTaskContext::default()).is_err());
    let mut trailing = bytes[..bytes.len() - CHECKSUM_BYTES].to_vec();
    trailing.push(0);
    trailing.extend_from_slice(&[0; CHECKSUM_BYTES]);
    resign(&mut trailing);
    assert!(MetadataView::decode(&trailing, limits(), &RuntimeTaskContext::default()).is_err());
}

#[test]
fn producer_validation_and_exact_capacity_limits_precede_any_output() {
    let binding = binding();
    let artifacts = artifacts();
    let mut input = input(&binding, &artifacts);
    let length = input
        .encoded_len(limits(), &RuntimeTaskContext::default())
        .unwrap();
    let exact = MetadataLimits {
        encoded_bytes: length,
        artifacts: artifacts.len(),
        ..limits()
    };
    let mut output = Vec::new();
    input
        .encode_to(&mut output, exact, &RuntimeTaskContext::default())
        .unwrap();
    MetadataView::decode(&output, exact, &RuntimeTaskContext::default()).unwrap();
    for too_small in [
        MetadataLimits {
            encoded_bytes: length - 1,
            ..exact
        },
        MetadataLimits {
            artifacts: artifacts.len() - 1,
            ..exact
        },
        MetadataLimits {
            artifact_bytes: input.search_manifest.byte_length - 1,
            ..exact
        },
    ] {
        let mut refused = Vec::new();
        let error = input
            .encode_to(&mut refused, too_small, &RuntimeTaskContext::default())
            .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        assert!(refused.is_empty());
        let error =
            MetadataView::decode(&output, too_small, &RuntimeTaskContext::default()).unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    }
    input.component_uuid = Uuid::nil();
    let mut refused = Vec::new();
    assert!(input
        .encode_to(&mut refused, limits(), &RuntimeTaskContext::default())
        .is_err());
    assert!(refused.is_empty());
    for name in [
        "../search_lexical.17.hawdb",
        "search_lexical.017.hawdb",
        "search_lexical.17.hawdb.retry",
        "a/search_lexical.17.hawdb",
    ] {
        let refs = [ArtifactReference {
            name,
            reference: artifacts[0].reference,
        }];
        let invalid = super::tests::input(&binding, &refs);
        assert!(invalid
            .encode_to(&mut refused, limits(), &RuntimeTaskContext::default())
            .is_err());
        assert!(refused.is_empty());
    }
    for refs in [[artifacts[0], artifacts[0]], [artifacts[1], artifacts[0]]] {
        assert!(super::tests::input(&binding, &refs)
            .encode_to(&mut refused, limits(), &RuntimeTaskContext::default())
            .is_err());
        assert!(refused.is_empty());
    }
}

struct ChunkedWriter {
    bytes: Vec<u8>,
    stop: usize,
}
impl Write for ChunkedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = bytes.len().min(3).min(self.stop - self.bytes.len());
        if count == 0 {
            return Err(io::Error::other("injected candidate write failure"));
        }
        self.bytes.extend_from_slice(&bytes[..count]);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        panic!("codec must not flush or acknowledge a candidate")
    }
}
#[test]
fn short_writes_work_and_failure_leaves_only_the_callers_private_prefix() {
    let binding = binding();
    let artifacts = artifacts();
    let input = input(&binding, &artifacts);
    let expected = encode(&input);
    let mut complete = ChunkedWriter {
        bytes: Vec::new(),
        stop: usize::MAX,
    };
    input
        .encode_to(&mut complete, limits(), &RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(complete.bytes, expected);
    for stop in [0, 7, 64, expected.len() / 2, expected.len() - 1] {
        let mut refused = ChunkedWriter {
            bytes: Vec::new(),
            stop,
        };
        assert!(input
            .encode_to(&mut refused, limits(), &RuntimeTaskContext::default())
            .is_err());
        assert_eq!(refused.bytes, expected[..stop]);
    }
}

#[test]
fn decode_and_lookup_allocate_no_owned_metadata_collection() {
    let _serial = crate::test_allocation::serial();
    assert_eq!(crate::test_allocation::live(), 0);
    let binding = binding();
    let artifacts = artifacts();
    let bytes = encode(&input(&binding, &artifacts));
    let task = RuntimeTaskContext::default();
    let (view, peak) = crate::test_allocation::measure(|| {
        let view = MetadataView::decode(&bytes, limits(), &task).unwrap();
        for name in NAMES {
            assert!(view.artifact(name).is_some());
        }
        assert!(view.artifact("missing").is_none());
        view
    });
    assert_eq!(peak, 0);
    assert_eq!(crate::test_allocation::live(), 0);
    assert_eq!(view.artifact_count(), 3);
}

#[test]
fn cancelled_admission_refuses_before_hashing_or_candidate_output() {
    let binding = binding();
    let artifacts = artifacts();
    let input = input(&binding, &artifacts);
    let bytes = encode(&input);
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    let mut output = Vec::new();
    for error in [
        input.encoded_len(limits(), &task).unwrap_err(),
        input.encode_to(&mut output, limits(), &task).unwrap_err(),
        MetadataView::decode(&bytes, limits(), &task).unwrap_err(),
    ] {
        assert!(matches!(error, HawDBError::Execution(_)));
        assert!(error.to_string().contains("cancel"));
    }
    assert!(output.is_empty());
}

#[test]
fn cancellation_during_output_never_finishes_the_frame() {
    struct CancelWriter<'a> {
        bytes: Vec<u8>,
        task: &'a RuntimeTaskContext,
    }
    impl Write for CancelWriter<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            self.task.cancellation().cancel();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            panic!("codec must not flush")
        }
    }
    let binding = binding();
    let artifacts = artifacts();
    for count in [0, artifacts.len()] {
        let input = input(&binding, &artifacts[..count]);
        let expected = encode(&input);
        let task = RuntimeTaskContext::default();
        let mut output = CancelWriter {
            bytes: Vec::new(),
            task: &task,
        };
        let error = input.encode_to(&mut output, limits(), &task).unwrap_err();
        assert!(error.to_string().contains("cancel"));
        assert!(!output.bytes.is_empty());
        assert!(output.bytes.len() < expected.len());
        assert_eq!(output.bytes, expected[..output.bytes.len()]);
        assert!(
            MetadataView::decode(&output.bytes, limits(), &RuntimeTaskContext::default()).is_err()
        );
    }
}

#[test]
fn late_cancellation_retains_a_complete_private_candidate() {
    struct CancelAtEnd<'a> {
        bytes: Vec<u8>,
        length: usize,
        task: &'a RuntimeTaskContext,
    }
    impl Write for CancelAtEnd<'_> {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(bytes);
            if self.bytes.len() == self.length {
                self.task.cancellation().cancel();
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            panic!("codec must not flush")
        }
    }
    let binding = binding();
    let artifacts = artifacts();
    let input = input(&binding, &artifacts);
    let expected = encode(&input);
    let task = RuntimeTaskContext::default();
    let mut output = CancelAtEnd {
        bytes: Vec::new(),
        length: expected.len(),
        task: &task,
    };
    let error = input.encode_to(&mut output, limits(), &task).unwrap_err();
    assert!(error.to_string().contains("cancel"));
    assert_eq!(output.bytes, expected);
    let view =
        MetadataView::decode(&output.bytes, limits(), &RuntimeTaskContext::default()).unwrap();
    assert!(view.matches_binding(&binding));
    assert_eq!(view.artifact_count(), artifacts.len());
}
