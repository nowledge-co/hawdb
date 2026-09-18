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
use crate::schema::Catalog;
use crate::store::GraphStore;
use crate::Value;
use std::collections::BTreeMap;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn prepared_repair_blocks_open_and_can_continue() {
    let (path, wal_path) = database_with_torn_wal("prepared");
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    {
        let _lease = DatabaseDirectoryLease::acquire(&path).unwrap();
        prepare_repair(&path, &wal_path, &plan).unwrap();
    }

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error.to_string().contains("interrupted WAL doctor repair"));

    let report = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    assert!(!report.resumed_interrupted_repair);
    assert!(!pending_record_path(&path, &plan).exists());
    assert!(applied_record_path(&path, &plan).exists());
    GraphStore::open(&path, &mut catalog).unwrap();
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn truncated_pending_repair_is_resumable_and_blocks_open_until_finalized() {
    let (path, wal_path) = database_with_torn_wal("truncated_pending");
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    {
        let _lease = DatabaseDirectoryLease::acquire(&path).unwrap();
        prepare_repair(&path, &wal_path, &plan).unwrap();
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&wal_path)
            .unwrap();
        wal.set_len(plan.retained_wal_len).unwrap();
        wal.sync_all().unwrap();
    }

    let mut catalog = Catalog::default();
    let error = GraphStore::open(&path, &mut catalog).unwrap_err();
    assert!(error.to_string().contains("interrupted WAL doctor repair"));
    let resumed_plan =
        DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    assert_eq!(resumed_plan, plan);
    let report = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &resumed_plan,
        resumed_plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap();
    assert!(report.resumed_interrupted_repair);
    GraphStore::open(&path, &mut catalog).unwrap();
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn apply_rejects_toctou_change_without_preparing_repair() {
    let (path, wal_path) = database_with_torn_wal("toctou");
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"changed")
        .unwrap();
    let changed_len = fs::metadata(&wal_path).unwrap().len();

    let error = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        plan.acknowledge_potential_data_loss(),
        WalDoctorOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("WAL changed"));
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), changed_len);
    assert!(pending_repair_records(&path).unwrap().is_empty());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn apply_requires_acknowledgement_for_the_exact_plan() {
    let (path, wal_path) = database_with_torn_wal("acknowledgement");
    let plan = DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
    let error = DatabaseDoctor::apply_wal_tail_repair(
        &path,
        &plan,
        WalRepairAcknowledgement::with_parts(
            WAL_DOCTOR_REPAIR_PROTOCOL.to_string(),
            "different-plan".to_string(),
            true,
        ),
        WalDoctorOptions::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("explicit acknowledgement"));
    assert_eq!(
        fs::metadata(&wal_path).unwrap().len(),
        plan.original_wal_len
    );
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn doctor_rejects_complete_checksum_corruption_without_modifying_wal() {
    let path = unique_test_dir("checksum_corruption");
    let wal_path = create_database(&path);
    // Flip one payload byte of the final complete fragment chain: the
    // chain stays structurally complete, so its checksum failure is
    // corruption, never a repairable torn tail.
    let mut corrupt = fs::read(&wal_path).unwrap();
    *corrupt.last_mut().unwrap() ^= 0xff;
    fs::write(&wal_path, &corrupt).unwrap();

    let error =
        DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap_err();
    assert!(error.to_string().contains("rejected corruption"));
    assert_eq!(fs::read(&wal_path).unwrap(), corrupt);
    assert!(!doctor_directory(&path).exists());
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn doctor_rejects_invalid_checkpoint_boundary_without_modifying_wal() {
    let path = unique_test_dir("checkpoint_boundary");
    let wal_path = {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".to_string(), Value::Int(1))]),
            )
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
        let checkpoint_path = manifest.checkpoint_path(&path);
        let mut checkpoint = fs::read(&checkpoint_path).unwrap();
        let last = checkpoint.last_mut().unwrap();
        *last ^= 0xff;
        fs::write(checkpoint_path, checkpoint).unwrap();
        manifest.wal_path(&path)
    };
    OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"torn-entry")
        .unwrap();
    let wal_before = fs::read(&wal_path).unwrap();

    let error =
        DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap_err();
    assert!(error.to_string().contains("checkpoint generation"));
    assert_eq!(fs::read(&wal_path).unwrap(), wal_before);
    fs::remove_dir_all(path).unwrap();
}

fn database_with_torn_wal(name: &str) -> (PathBuf, PathBuf) {
    let path = unique_test_dir(name);
    let wal_path = create_database(&path);
    OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(b"torn-entry")
        .unwrap();
    (path, wal_path)
}

fn create_database(path: &Path) -> PathBuf {
    {
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(path, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("id".to_string(), Value::Int(1))]),
            )
            .unwrap();
    }
    let manifest = DurableManifest::load(&path.join(MANIFEST_FILE)).unwrap();
    manifest.wal_path(path)
}

fn unique_test_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-wal-doctor-{name}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}
