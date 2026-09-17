use super::super::tests::{document, test_dir};
use super::*;
use crate::build_memory::BuildMemory;
use skein_core::RuntimeMemoryReservation;
use skein_vector_projection::RaBitQBitWidth;
use std::num::NonZeroUsize;
use std::path::PathBuf;

struct Directory(PathBuf);

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    input: SearchOutOfCoreGenerationWriter,
    document: SearchDocument,
    memory: BuildMemory,
    budget: usize,
    _directory: Directory,
}

impl Fixture {
    fn new(budget: usize, dimension: usize, bits: RaBitQBitWidth, rows: usize) -> Self {
        let root = test_dir("vector_context");
        // Keep the original vector working capacity after mandatory cleanup admission.
        let budget = budget + crate::build_memory::directory::stage_removal_bytes(&root).unwrap();
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(budget as u64, 0));
        let options = super::super::SearchOutOfCoreGenerationBuildOptions {
            rabitq_bit_width: bits,
            rabitq_segment_rows: NonZeroUsize::new(rows).unwrap(),
            ..Default::default()
        };
        let mut input =
            SearchOutOfCoreGenerationWriter::create_with_context(&root, options, task).unwrap();
        let mut document = document(0);
        document.embedding = Some(vec![0.25; dimension]);
        input.push(document.clone()).unwrap();
        let memory = input.memory.clone();
        Self {
            input,
            document,
            memory,
            budget,
            _directory: Directory(root),
        }
    }

    fn used(&self) -> usize {
        self.memory.ledger.snapshot().used_bytes
    }

    fn occupy_remaining(&self) -> skein_executor::QueryMemoryLease {
        self.memory
            .spool
            .reserve(self.budget - self.used())
            .unwrap()
    }
}

#[test]
fn native_vector_state_is_admitted_before_core_creation() {
    let fixture = Fixture::new(1024 * 1024, 4096, RaBitQBitWidth::Four, 1024);
    let baseline = fixture.used();
    evidence::take();
    let error = RaBitQArtifactBuilder::new(&fixture.input, 1).err().unwrap();
    assert!(error.to_string().contains("query memory"), "{error}");
    assert_eq!(evidence::take(), (0, 0, 0));
    assert_eq!(fixture.used(), baseline);
    assert!(!fixture
        .input
        .stage
        .path
        .join(crate::rabitq_artifact_file(1))
        .exists());
}

#[test]
fn vector_push_shares_other_stage_capacity_and_poisoning_prevents_retry() {
    for bits in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let fixture = Fixture::new(32 * 1024 * 1024, 64, bits, 4);
        let baseline = fixture.used();
        evidence::take();
        let mut builder = RaBitQArtifactBuilder::new(&fixture.input, 1).unwrap();
        let held = fixture.occupy_remaining();
        let error = builder.push(&fixture.document).unwrap_err();
        assert!(error.to_string().contains("query memory"), "{error}");
        assert_eq!(evidence::take(), (1, 0, 0));
        drop(held);
        assert!(builder
            .push(&fixture.document)
            .unwrap_err()
            .to_string()
            .contains("already failed"));
        assert!(builder.finish().is_err());
        assert_eq!(evidence::take(), (0, 0, 0));
        assert_eq!(fixture.used(), baseline);
    }
}

#[test]
fn vector_finalization_admission_precedes_core_finish_and_reopen() {
    let fixture = Fixture::new(32 * 1024 * 1024, 64, RaBitQBitWidth::Four, 1);
    let baseline = fixture.used();
    let mut builder = RaBitQArtifactBuilder::new(&fixture.input, 1).unwrap();
    builder.push(&fixture.document).unwrap();
    evidence::take();
    let held = fixture.occupy_remaining();
    let error = builder.finish().unwrap_err();
    assert!(error.to_string().contains("query memory"), "{error}");
    assert_eq!(evidence::take(), (0, 0, 0));
    assert_eq!(fixture.used(), baseline + held.bytes());
    drop(held);
    assert_eq!(fixture.used(), baseline);
    assert!(!fixture
        .input
        .stage
        .path
        .join(crate::rabitq_artifact_file(1))
        .exists());
}

#[test]
fn vector_directory_growth_is_admitted_before_flushing_another_segment() {
    let mut fixture = Fixture::new(32 * 1024 * 1024, 64, RaBitQBitWidth::One, 1);
    for index in 1..5 {
        let mut next = fixture.document.clone();
        next.id = format!("next-{index:04}");
        fixture.input.push(next).unwrap();
    }
    let baseline = fixture.used();
    let mut builder = RaBitQArtifactBuilder::new(&fixture.input, 1).unwrap();
    for _ in 0..4 {
        builder.push(&fixture.document).unwrap();
    }
    evidence::take();
    let held = fixture.occupy_remaining();
    let error = builder.push(&fixture.document).unwrap_err();
    assert!(error.to_string().contains("query memory"), "{error}");
    assert_eq!(evidence::take(), (0, 0, 0));
    drop(held);
    assert!(builder.finish().is_err());
    assert_eq!(fixture.used(), baseline);
}

#[test]
fn reopened_vector_state_and_returned_name_keep_their_owning_leases() {
    for bits in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
        let fixture = Fixture::new(32 * 1024 * 1024, 64, bits, 1);
        let baseline = fixture.used();
        let memory = fixture.memory.clone();
        evidence::take();
        evidence::take_reopened_bytes();
        let mut builder = RaBitQArtifactBuilder::new(&fixture.input, u64::MAX).unwrap();
        builder.push(&fixture.document).unwrap();
        let before_finish = fixture.used();
        let output = builder.finish().unwrap().unwrap();
        assert_eq!(evidence::take(), (1, 1, 1));
        assert!(evidence::take_reopened_bytes() > before_finish);
        assert_eq!(*output.file_name, crate::rabitq_artifact_file(u64::MAX));
        assert_eq!(fixture.used(), baseline + output.file_name.capacity());
        assert_eq!(memory.ledger.snapshot().account_count, 3);
        let held = fixture.occupy_remaining();
        assert!(memory.input.reserve(1).is_err());
        drop(output);
        assert_eq!(fixture.used(), baseline + held.bytes());
        drop(held);
        drop(fixture);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
