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

struct ResetApplyFailure;

impl Drop for ResetApplyFailure {
    fn drop(&mut self) {
        crate::store::set_wal_apply_failpoint(None);
    }
}

#[test]
fn failed_schema_data_transaction_never_resumes_a_partial_candidate() {
    let mut fixture = Fixture::new();
    fixture
        .store
        .commit_mutations(
            &mut fixture.catalog,
            vec![
                crate::mutation::GraphMutation::CreateNodeLabel {
                    label: "NewLabel".into(),
                },
                crate::mutation::GraphMutation::CreateNode {
                    label: "NewLabel".into(),
                    properties: BTreeMap::new(),
                },
            ],
        )
        .unwrap();
    let original = fixture.store.durable.as_ref().unwrap();
    fixture.identity = fixture.store.checkpoint_source_identity();
    fixture.wal = fs::read(&original.wal_path).unwrap();
    fixture.manifest = fs::read(original.manifest_path()).unwrap();
    let expected = fixture
        .store
        .node_records_owned()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let base_epoch = fixture.pinned.commit_epoch();
    let reset = ResetApplyFailure;
    // Two whole node records, then the new label inside the third record.
    // Failure before that record's node mutation leaves a partial schema/data
    // transaction, even though the candidate's commit epoch stays at C-1.
    crate::store::set_wal_apply_failpoint(Some(3));
    let error = fixture.candidate.catch_up(&fixture.store).unwrap_err();
    drop(reset);
    assert!(error.to_string().contains("injected failure"), "{error}");
    assert_eq!(fixture.candidate.commit_epoch(), base_epoch + 2);
    assert!(fixture
        .candidate
        .catalog
        .as_ref()
        .unwrap()
        .label_id("NewLabel")
        .is_some());
    assert_eq!(
        fixture
            .candidate
            .store
            .as_ref()
            .unwrap()
            .node_count_for_label(None),
        3
    );
    assert!(fixture.candidate.failed);
    assert!(!fixture.candidate.can_continue_from(&fixture.store));
    assert!(fixture.candidate.catch_up(&fixture.store).is_err());
    assert!(fixture.candidate.finish_catch_up().is_err());
    fixture.unchanged_source();
    let staging = fixture
        .candidate
        .prepared
        .as_ref()
        .unwrap()
        .staging_path
        .clone();
    drop(fixture.candidate);
    assert!(!staging.exists());
    drop(fixture.pinned);
    drop(fixture.store);
    let reopened = GraphStore::open(&fixture.root, &mut fixture.catalog).unwrap();
    assert_eq!(
        reopened
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(reopened);
    fs::remove_dir_all(fixture.root).unwrap();
}

#[test]
fn lost_captured_source_bytes_poison_the_writer_and_cannot_resume() {
    let mut fixture = Fixture::new();
    let original = fixture.store.durable.as_ref().unwrap();
    let source_path = original.wal_path.clone();
    let captured_bytes = original.wal_bytes;
    let file = fs::OpenOptions::new()
        .write(true)
        .open(&source_path)
        .unwrap();
    file.set_len(captured_bytes - 1).unwrap();
    let error = fixture.candidate.catch_up(&fixture.store).unwrap_err();
    assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    assert!(fixture.store.storage_handle_poisoned());
    assert!(fixture.candidate.failed);
    assert!(!fixture.candidate.can_continue_from(&fixture.store));
    assert!(fixture.candidate.catch_up(&fixture.store).is_err());
    assert_eq!(
        fixture.candidate.commit_epoch(),
        fixture.pinned.commit_epoch()
    );
    assert_eq!(
        fs::metadata(&source_path).unwrap().len(),
        captured_bytes - 1
    );
    assert_eq!(
        fs::read(original.manifest_path()).unwrap(),
        fixture.manifest
    );
    drop(file);
    drop(fixture.candidate);
    drop(fixture.pinned);
    drop(fixture.store);
    fs::remove_dir_all(fixture.root).unwrap();
}

#[test]
fn delta_preflight_denial_releases_unapplied_decoded_record_ownership() {
    use crate::background::CheckpointWorkContext;
    use crate::config::{StorageResidencyMode, WalReplayConfig};
    use hawdb_core::RuntimeMemoryError;
    let root = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-resume-delta-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &root,
        &mut catalog,
        Default::default(),
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            max_out_of_core_delta_bytes: Some(32_000),
            ..Default::default()
        },
    )
    .unwrap();
    store
        .create_node(&mut catalog, "Anchor", BTreeMap::new())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    let identity = store.checkpoint_source_identity();
    let entry = crate::wal::WalEntry {
        lsn: store.durable.as_ref().unwrap().next_lsn,
        op: WalOp::CreateNode {
            id: NodeId(2),
            label: "Rejected".into(),
            properties: BTreeMap::from([("payload".into(), Value::String("x".repeat(64 * 1024)))]),
        },
    };
    let payload =
        crate::wal::binary::encode_binary_wal_record(&entry, store.commit_epoch() + 1).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let decoded =
        match crate::wal::binary::decode_binary_wal_record_with_work_context(&payload, &work)
            .unwrap()
        {
            crate::wal::binary::BinaryWalRecordDecode::Entry { entry, .. } => entry,
            _ => panic!("complete record must decode"),
        };
    let mut mutation_started = false;
    let error = decoded
        .replay_into_with_boundary(&mut store, &mut catalog, &work, &mut mutation_started)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("out-of-core mutation delta admission rejected"),
        "{error}"
    );
    assert!(!mutation_started);
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(store.node_count_for_label(None), 1);
    assert_eq!(catalog.label_id("Rejected"), None);
    assert!(
        matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
    );
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        0,
        "the surviving candidate must not pin leases for an unapplied record"
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
