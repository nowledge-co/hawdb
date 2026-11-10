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

//! Same-process publication records: which identity re-hashes are skipped,
//! and which still run.

use super::*;
use std::io::{Seek, SeekFrom};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Fixture {
    root: PathBuf,
    store: ImmutableObjectStore,
    reference: ObjectReference,
    object: PathBuf,
}

impl Fixture {
    /// Streams a multi-buffer payload through `publish_file`.
    fn published(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("hawdb-published-{label}-{nanos}"));
        std::fs::create_dir_all(&root).unwrap();
        // Unique content: parallel tests never share a reference.
        let mut payload = label.as_bytes().to_vec();
        payload.extend((0..(2 * STREAM_BUFFER_BYTES + 5)).map(|index| (index % 239) as u8));
        let source = root.join("source.bin");
        std::fs::write(&source, &payload).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &payload);
        let mut store = ImmutableObjectStore::open(&root).unwrap();
        assert_eq!(
            store.publish_file(reference, &source).unwrap(),
            PublishOutcome::Published
        );
        let object = store.object_path(reference);
        Self {
            root,
            store,
            reference,
            object,
        }
    }

    fn skipped(&self) -> Option<u64> {
        published_object_skipped_rehashes(&self.object).unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Flips one byte in place (same inode and length). `modified` selects the
/// resulting modification time; `None` restores the original one.
fn flip_byte(path: &Path, offset: u64, modified: Option<SystemTime>) {
    let original = std::fs::metadata(path).unwrap().modified().unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0_u8];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0x5a;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    file.set_times(std::fs::FileTimes::new().set_modified(modified.unwrap_or(original)))
        .unwrap();
}

#[test]
fn streamed_publication_skips_same_process_selector_rehash() {
    let fixture = Fixture::published("skip");
    assert_eq!(fixture.skipped(), Some(0));
    fixture
        .store
        .verify_stored_trusting_publication(fixture.reference)
        .unwrap();
    fixture
        .store
        .verify_stored_trusting_publication(fixture.reference)
        .unwrap();
    assert_eq!(fixture.skipped(), Some(2));
}

#[test]
fn buffered_publication_and_reuse_do_not_record() {
    let mut fixture = Fixture::published("buffered");
    let payload = b"buffered payload is read back, not recorded".to_vec();
    let reference = ObjectReference::for_bytes(ObjectKind::Checkpoint, 1, &payload);
    assert_eq!(
        fixture.store.publish(reference, &payload).unwrap(),
        PublishOutcome::Published
    );
    let path = fixture.store.object_path(reference);
    assert_eq!(published_object_skipped_rehashes(&path).unwrap(), None);
    fixture
        .store
        .verify_stored_trusting_publication(reference)
        .unwrap();
    assert_eq!(published_object_skipped_rehashes(&path).unwrap(), None);

    // Reusing an existing object validates it fully and records nothing new.
    assert!(forget_published_object(&fixture.object).unwrap());
    let source = fixture.root.join("source.bin");
    let reference = fixture.reference;
    assert_eq!(
        fixture.store.publish_file(reference, &source).unwrap(),
        PublishOutcome::Reused
    );
    assert_eq!(fixture.skipped(), None);
}

#[test]
fn forgotten_publication_is_fully_rehashed_and_corruption_rejected() {
    let fixture = Fixture::published("cold");
    assert!(forget_published_object(&fixture.object).unwrap());
    assert!(!forget_published_object(&fixture.object).unwrap());
    fixture
        .store
        .verify_stored_trusting_publication(fixture.reference)
        .unwrap();
    assert_eq!(fixture.skipped(), None);

    // A cold check hashes every byte: even an mtime-preserving flip fails.
    flip_byte(&fixture.object, fixture.reference.byte_length / 2, None);
    assert!(matches!(
        fixture
            .store
            .verify_stored_trusting_publication(fixture.reference),
        Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
    ));
}

#[test]
fn modified_published_object_is_rehashed_and_rejected() {
    let fixture = Fixture::published("modified");
    let later = std::fs::metadata(&fixture.object)
        .unwrap()
        .modified()
        .unwrap()
        + Duration::from_secs(1);
    flip_byte(&fixture.object, 7, Some(later));
    // The record no longer describes the file, so nothing can be skipped.
    assert_eq!(fixture.skipped(), None);
    assert!(matches!(
        fixture
            .store
            .verify_stored_trusting_publication(fixture.reference),
        Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
    ));
}

#[test]
fn replaced_published_object_is_rehashed() {
    let fixture = Fixture::published("replaced");
    // Same bytes, new inode: identity differs, so the record does not apply.
    let bytes = std::fs::read(&fixture.object).unwrap();
    let replacement = fixture.root.join("replacement.bin");
    std::fs::write(&replacement, &bytes).unwrap();
    std::fs::rename(&replacement, &fixture.object).unwrap();
    fixture
        .store
        .verify_stored_trusting_publication(fixture.reference)
        .unwrap();
    assert_eq!(fixture.skipped(), None);
}

#[test]
fn reclamation_verification_ignores_publication_records() {
    let fixture = Fixture::published("reclaim");
    // An mtime-preserving flip is the documented gap of the record; the
    // reclamation/recovery verifier never consults it.
    flip_byte(&fixture.object, 3, None);
    assert!(matches!(
        fixture.store.verify(fixture.reference),
        Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
    ));
    assert!(matches!(
        fixture.store.verify_and_sync(fixture.reference),
        Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
    ));
    assert_eq!(fixture.skipped(), Some(0));
}

#[test]
fn diagnostic_verification_ignores_publication_records() {
    let fixture = Fixture::published("diagnostic");
    fixture.store.verify_stored(fixture.reference).unwrap();
    assert_eq!(fixture.skipped(), Some(0));
    // Doctor and derived-artifact health checks hash every byte, so even the
    // mtime-preserving flip that the record cannot see is reported.
    flip_byte(&fixture.object, 11, None);
    assert!(matches!(
        fixture.store.verify_stored(fixture.reference),
        Err(ImmutableObjectError::ExistingObjectCorrupt { .. })
    ));
    assert_eq!(fixture.skipped(), Some(0));
}
