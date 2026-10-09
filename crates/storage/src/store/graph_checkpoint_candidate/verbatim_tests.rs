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
use crate::wal::binary::{decode_binary_wal_record, BinaryWalRecordDecode};
use crate::wal::frame::{BinaryWalReadEvent, BinaryWalReader};

struct Fixture {
    root: PathBuf,
    store: GraphStore,
    pinned: GraphStore,
    candidate: CheckpointCandidate,
}

fn copy_tree(source: &Path, destination: &Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let path = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &path);
        } else {
            fs::copy(entry.path(), path).unwrap();
        }
    }
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-verbatim-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(1)),
                    ("payload".into(), Value::Binary(vec![5; 17])),
                ]),
            )
            .unwrap();
        let pinned = store.checkpoint_source();
        let candidate = pinned
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(2)),
                    ("payload".into(), Value::Binary(vec![7; 70_001])),
                ]),
            )
            .unwrap();
        Self {
            root,
            store,
            pinned,
            candidate,
        }
    }

    fn replace_suffix(&mut self, edit: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let durable = self.store.durable.as_ref().unwrap();
        let mut reader = BinaryWalReader::range(
            fs::File::open(&durable.wal_path).unwrap(),
            durable.wal_generation,
            None,
            self.candidate.captured_wal_bytes,
            durable.wal_bytes,
        )
        .unwrap();
        let mut payload = match reader.next_event().unwrap() {
            BinaryWalReadEvent::Record { payload, .. } => payload,
            _ => panic!("fixture suffix must contain one complete transaction"),
        };
        assert!(matches!(
            reader.next_event().unwrap(),
            BinaryWalReadEvent::Eof
        ));
        drop(reader);
        edit(&mut payload);
        let mut bytes = fs::read(&durable.wal_path).unwrap();
        bytes.truncate(self.candidate.captured_wal_bytes as usize);
        bytes.extend(crate::wal::frame::frame_binary_wal_record(
            durable.wal_generation,
            &payload,
            self.candidate.captured_wal_bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
        ));
        fs::write(&durable.wal_path, &bytes).unwrap();
        self.store.durable.as_mut().unwrap().wal_bytes = bytes.len() as u64;
        payload
    }

    fn private_wal(&self) -> Vec<u8> {
        fs::read(
            &self
                .candidate
                .store
                .as_ref()
                .unwrap()
                .durable
                .as_ref()
                .unwrap()
                .wal_path,
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
        let _ = fs::remove_dir_all(self.root.with_extension("reopen"));
    }
}

#[test]
fn checkpoint_suffix_preserves_valid_noncanonical_payload_bytes() {
    let mut fixture = Fixture::new();
    let payload = fixture.replace_suffix(|payload| {
        // An overlong opcode varint is accepted by the independent ordinary
        // decoder. Re-encoding normalizes it and changes authoritative bytes.
        assert!(payload[21] < 128);
        payload[21] |= 128;
        payload.insert(22, 0);
    });
    let decoded = match decode_binary_wal_record(&payload).unwrap() {
        BinaryWalRecordDecode::Entry {
            entry,
            commit_epoch,
        } => (entry, commit_epoch),
        _ => panic!("fixture must remain valid under the ordinary decoder"),
    };
    assert_ne!(
        crate::wal::binary::encode_binary_wal_record(&decoded.0, decoded.1).unwrap(),
        payload,
    );
    let expected = fixture
        .store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let epoch = fixture.store.commit_epoch();
    fixture.candidate.catch_up(&fixture.store).unwrap();
    let private = fixture.private_wal();
    let mut reader = BinaryWalReader::range(
        std::io::Cursor::new(&private),
        fixture.candidate.prepared.as_ref().unwrap().generation,
        None,
        WAL_BINARY_FILE_HEADER_BYTES as u64,
        private.len() as u64,
    )
    .unwrap();
    match reader.next_event().unwrap() {
        BinaryWalReadEvent::Record {
            payload: actual, ..
        } => assert_eq!(actual, payload),
        _ => panic!("private WAL must contain the complete original payload"),
    }
    assert!(matches!(
        reader.next_event().unwrap(),
        BinaryWalReadEvent::Eof
    ));
    fixture
        .store
        .publish_checkpoint_candidate(&mut fixture.candidate, None, &BTreeSet::new())
        .unwrap();
    assert_eq!(fixture.pinned.scan_nodes(None).count(), 1);
    // Legacy read-only opens share the exclusive open lock. Reopen a complete
    // independent copy while the writer and old pin remain alive.
    let reopened_root = fixture.root.with_extension("reopen");
    copy_tree(&fixture.root, &reopened_root);
    let mut reopened_catalog = Catalog::default();
    let reopened = GraphStore::open_read_only_with_durability(
        &reopened_root,
        &mut reopened_catalog,
        DurabilityPolicy::SyncOnEveryWrite,
        RecoveryMode::Strict,
    )
    .unwrap();
    assert_eq!(reopened.commit_epoch(), epoch);
    assert_eq!(
        reopened
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
    );
}

#[test]
fn checkpoint_suffix_rejects_valid_framing_with_wrong_commit_epoch() {
    let mut fixture = Fixture::new();
    let epoch = fixture.store.commit_epoch();
    fixture.replace_suffix(|payload| payload[9..17].copy_from_slice(&(epoch + 5).to_le_bytes()));
    let manifest = fs::read(fixture.store.durable.as_ref().unwrap().manifest_path()).unwrap();
    let private = fixture.private_wal();
    let result = fixture.candidate.catch_up(&fixture.store);
    assert!(
        matches!(result, Err(HawDBError::StorageIntegrity(_))),
        "incorrect epoch must not be normalized: {result:?}"
    );
    assert_eq!(fixture.private_wal(), private);
    assert_eq!(
        fixture.candidate.commit_epoch(),
        fixture.pinned.commit_epoch()
    );
    assert!(!fixture.candidate.can_continue_from(&fixture.store));
    assert!(fixture.store.ensure_usable().is_err());
    assert_eq!(
        fs::read(fixture.store.durable.as_ref().unwrap().manifest_path()).unwrap(),
        manifest
    );
}
