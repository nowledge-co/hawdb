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
use crate::lexical_projection::manifest_encoding::tests::manifest;
use crate::lexical_projection::{LexicalProjectionWriter, ARTIFACT_HEADER};
use crate::{SearchAnalyzerLexicon, SearchDocument};
use hawdb_core::{RuntimeCancellationToken, RuntimeMemoryReservation};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

thread_local! {
    static BEFORE_VERIFY: RefCell<Option<Action>> = const { RefCell::new(None) };
}

enum Action {
    Cancel,
    Corrupt,
    Grow,
    Truncate,
    CorruptArtifact,
    Panic,
}

pub(super) fn before_verify(paths: &Paths, task: &RuntimeTaskContext) {
    let path = &paths.manifest_tmp;
    BEFORE_VERIFY.with_borrow_mut(|action| match action.take() {
        Some(Action::Cancel) => {
            task.cancellation().cancel();
        }
        Some(Action::Corrupt) => {
            let mut bytes = fs::read(path).unwrap();
            bytes[0] = b'!';
            fs::write(path, bytes).unwrap();
        }
        Some(Action::Grow) => {
            let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
            file.write_all(b" ").unwrap();
        }
        Some(Action::Truncate) => {
            File::create(path).unwrap();
        }
        Some(Action::CorruptArtifact) => {
            let mut bytes = fs::read(&paths.artifact_tmp).unwrap();
            bytes[0] ^= 1;
            fs::write(&paths.artifact_tmp, bytes).unwrap();
        }
        Some(Action::Panic) => panic!("injected manifest verification panic"),
        None => {}
    });
}

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-build-manifest-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn files(&self) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
        fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                assert!(path.is_file(), "unexpected directory {path:?}");
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(path).unwrap(),
                )
            })
            .collect()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn context(bytes: usize) -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(bytes as u64, 0));
    (BuildMemory::new(&task).unwrap(), task)
}

fn document() -> SearchDocument {
    SearchDocument {
        id: "a".into(),
        title: "alpha".into(),
        content: "beta".into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn write(
    fixture: &Fixture,
    generation: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Arc<LexicalProjectionReader>> {
    LexicalProjectionWriter::new(Default::default())
        .with_context(memory.clone(), task.clone())
        .write(
            &fixture.0,
            generation,
            None,
            11,
            13,
            std::iter::once(&document()),
            &SearchAnalyzerLexicon::default(),
        )
}

#[test]
fn trusted_shape_admission_covers_decoded_capacities() {
    let task = RuntimeTaskContext::default();
    for count in [0, 1, 3, 4, 5, 8, 9, 16, 17, 1000] {
        let body = manifest(
            (0..count)
                .map(|n| format!("{n:04}-\u{1f980}-\n-\\-\"-{}", "x".repeat(n % 73)))
                .collect(),
        );
        let plan = DecodePlan::new(&body, &task).unwrap();
        let bytes = body.encode(u64::MAX).unwrap();
        let decoded = ManifestBody::decode(&bytes).unwrap();
        assert_eq!(body, decoded);
        assert!(retained_bytes(&decoded, &task).unwrap() <= plan.output_bytes);
        assert!(plan.scratch_bytes >= 3 * 128);
    }
}

#[test]
fn verified_build_reader_retains_metadata_until_its_last_clone_drops() {
    let fixture = Fixture::new();
    let (memory, task) = context(16 * 1024 * 1024);
    let reader = write(&fixture, 1, &memory, &task).unwrap();
    let actual = retained_bytes(&reader.manifest, &task).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, actual);
    let other = Arc::clone(&reader);
    drop(reader);
    assert_eq!(memory.ledger.snapshot().used_bytes, actual);
    let default_reader =
        LexicalProjectionReader::load(&fixture.0, None, 11, 13, Default::default())
            .unwrap()
            .unwrap();
    assert_eq!(other.manifest, default_reader.manifest);
    let query = other
        .tokenize_query(
            "alpha",
            &Default::default(),
            LexicalProjectionConfig::default().max_term_bytes,
        )
        .unwrap();
    assert!(query.contains("alpha"));
    drop(other);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
}

#[test]
fn changed_private_manifest_and_cancellation_preserve_the_active_projection() {
    for action in [
        Action::Cancel,
        Action::Corrupt,
        Action::Grow,
        Action::Truncate,
        Action::CorruptArtifact,
    ] {
        let fixture = Fixture::new();
        let (old_memory, old_task) = context(16 * 1024 * 1024);
        let old_reader = write(&fixture, 1, &old_memory, &old_task).unwrap();
        let previous = fixture.files();
        let (memory, task) = context(16 * 1024 * 1024);
        BEFORE_VERIFY.with_borrow_mut(|slot| *slot = Some(action));
        assert!(write(&fixture, 2, &memory, &task).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fixture.files(), previous);
        let reopened = LexicalProjectionReader::load(&fixture.0, None, 11, 13, Default::default())
            .unwrap()
            .unwrap();
        assert_eq!(old_reader.manifest, reopened.manifest);
    }
}

#[test]
fn verification_panic_releases_build_memory_and_preserves_the_active_projection() {
    let fixture = Fixture::new();
    let (old_memory, old_task) = context(16 * 1024 * 1024);
    let old_reader = write(&fixture, 1, &old_memory, &old_task).unwrap();
    let previous = fixture.files();
    let (memory, task) = context(16 * 1024 * 1024);
    BEFORE_VERIFY.with_borrow_mut(|slot| *slot = Some(Action::Panic));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        write(&fixture, 2, &memory, &task)
    }));
    assert!(result.is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fixture.files(), previous);
    let reopened = LexicalProjectionReader::load(&fixture.0, None, 11, 13, Default::default())
        .unwrap()
        .unwrap();
    assert_eq!(old_reader.manifest, reopened.manifest);
}

#[test]
fn decode_output_and_scratch_admission_precede_manifest_file_creation() {
    use crate::lexical_projection::artifacts::ArtifactBuilder;

    const BUDGET: usize = 16 * 1024 * 1024;
    for deny_scratch in [false, true] {
        let fixture = Fixture::new();
        let (memory, task) = context(BUDGET);
        let mut paths = Paths::new(&fixture.0, 7, &memory, &task).unwrap();
        let artifact = ArtifactBuilder::new_with_context(
            &paths.artifact_tmp,
            7,
            Default::default(),
            memory.clone(),
            task.clone(),
        )
        .unwrap()
        .finish()
        .unwrap();
        let mut body = manifest(Vec::new());
        body.artifact_checksum = artifact.checksum;
        body.artifact_file = std::mem::take(&mut paths.artifact_name);
        assert_eq!(body.artifact_len, artifact.len);
        let encoded_length = body.encode(u64::MAX).unwrap().len();
        let plan = DecodePlan::new(&body, &task).unwrap();
        let available = encoded_length + plan.output_bytes - 1
            + if deny_scratch { plan.scratch_bytes } else { 0 };
        assert!(available >= encoded_length + 3 * 128);
        let baseline = memory.ledger.snapshot().used_bytes;
        let blocker = memory.input.reserve(BUDGET - baseline - available).unwrap();
        let previous = fixture.files();
        let error = finish(
            body,
            artifact.memory,
            &paths,
            Default::default(),
            &memory,
            &task,
        )
        .unwrap_err();
        assert!(error.to_string().contains("query memory"), "{error}");
        assert!(!paths.manifest_tmp.exists());
        assert_eq!(fixture.files(), previous);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            baseline + blocker.bytes()
        );
        drop(blocker);
        drop(paths);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn artifact_digest_verification_remains_complete_before_manifest_publication() {
    let fixture = Fixture::new();
    let (memory, task) = context(16 * 1024 * 1024);
    let path = fixture.0.join("payload");
    let bytes = vec![b'x'; SPILL_IO_BUFFER_BYTES * 3 + 1];
    fs::write(&path, &bytes).unwrap();
    let file = File::open(&path).unwrap();
    let mut expected = Crc32cHasher::new();
    expected.update(&bytes);
    assert_eq!(
        file_digest(&file, &memory, &task).unwrap(),
        (bytes.len() as u64, expected.finish())
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let blocker = memory
        .input
        .reserve(16 * 1024 * 1024 - SPILL_IO_BUFFER_BYTES + 1)
        .unwrap();
    assert!(file_digest(&file, &memory, &task).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, blocker.bytes());
    drop(blocker);
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let cancelled = RuntimeTaskContext::without_deadline(token);
    assert!(file_digest(&file, &memory, &cancelled).is_err());
}

#[test]
fn exact_file_verification_rejects_same_size_bytes_and_incomplete_files() {
    let fixture = Fixture::new();
    let (memory, task) = context(1024 * 1024);
    let path = fixture.0.join("manifest");
    let expected = vec![b'x'; SPILL_IO_BUFFER_BYTES * 2 + 1];
    fs::write(&path, &expected).unwrap();
    verify_file_bytes(&path, &expected, &memory, &task).unwrap();
    let mut wrong = expected.clone();
    wrong[SPILL_IO_BUFFER_BYTES + 1] = b'y';
    fs::write(&path, &wrong).unwrap();
    assert!(verify_file_bytes(&path, &expected, &memory, &task)
        .unwrap_err()
        .to_string()
        .contains("bytes changed"));
    fs::write(&path, ARTIFACT_HEADER).unwrap();
    assert!(verify_file_bytes(&path, &expected, &memory, &task)
        .unwrap_err()
        .to_string()
        .contains("length changed"));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn windows_rename_scratch_keeps_all_four_input_dependent_path_bounds() {
    assert_eq!(windows_rename_bytes([0; 4]).unwrap(), 4 * 4 * 3 * 2);
    assert_eq!(
        windows_rename_bytes([3, 260, 32_767, 65_536]).unwrap(),
        591_420
    );
    for position in 0..4 {
        for length in [usize::MAX, usize::MAX / 3, usize::MAX / 6] {
            let mut lengths = [0; 4];
            lengths[position] = length;
            assert!(windows_rename_bytes(lengths).is_err());
        }
    }
}
