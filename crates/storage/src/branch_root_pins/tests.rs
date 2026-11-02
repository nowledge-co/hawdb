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
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

const PROJECT: [u8; 16] = [41; 16];
const BRANCH: [u8; 16] = [17; 16];

struct Fixture {
    project: PathBuf,
    branch: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let project = std::env::temp_dir().join(format!(
            "hawdb-root-pins-{}-{time}-{sequence}",
            std::process::id()
        ));
        let branch = project.join(hawdb_core::Uuid::from_bytes(BRANCH).to_string());
        std::fs::create_dir_all(&branch).unwrap();
        Self { project, branch }
    }

    fn open(&self, root: ObjectReference) -> Arc<RootPin> {
        let owner = Arc::new(DatabaseDirectoryLease::acquire(&self.branch).unwrap());
        RootRegistry::open(&self.project, &self.branch, PROJECT, BRANCH, owner, root).unwrap()
    }

    fn roots(&self, limit: usize) -> io::Result<Option<Vec<ObjectReference>>> {
        let metadata = CatalogMetadataLease::acquire_blocking(&self.project)?;
        active_roots(&self.branch, PROJECT, BRANCH, limit, &metadata)
    }

    fn record(&self) -> PathBuf {
        self.branch.join(DIRECTORY).join(RECORD)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.project);
    }
}

fn root(value: u8) -> ObjectReference {
    ObjectReference::for_bytes(ObjectKind::SealedRoot, 2, &[value; 32])
}

#[test]
fn exact_roots_outlive_the_runtime_and_retire_after_the_last_clone() {
    let fixture = Fixture::new();
    let first = fixture.open(root(1));
    let historical_reader = Arc::clone(&first);
    let next = first
        .registry()
        .publication()
        .unwrap()
        .pin(root(2))
        .unwrap();
    assert_eq!(next.root(), root(2));
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(1), root(2)]));
    drop(first);
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(1), root(2)]));
    drop(historical_reader);
    assert_eq!(fixture.roots(1).unwrap(), Some(vec![root(2)]));
    assert!(matches!(
        DatabaseDirectoryLease::acquire(&fixture.branch),
        Err(DatabaseDirectoryLeaseError::AlreadyOpen)
    ));
    drop(next);
    assert_eq!(fixture.roots(1).unwrap(), None);
    DatabaseDirectoryLease::acquire(&fixture.branch).unwrap();
}

#[test]
fn stale_bytes_never_register_an_untracked_live_owner() {
    let fixture = Fixture::new();
    let first = fixture.open(root(1));
    let record = std::fs::read(fixture.record()).unwrap();
    drop(first);
    std::fs::write(fixture.record(), record).unwrap();
    let owner = Arc::new(DatabaseDirectoryLease::acquire(&fixture.branch).unwrap());
    assert_eq!(fixture.roots(1).unwrap(), None);
    let current = RootRegistry::open(
        &fixture.project,
        &fixture.branch,
        PROJECT,
        BRANCH,
        owner,
        root(2),
    )
    .unwrap();
    assert_eq!(fixture.roots(1).unwrap(), Some(vec![root(2)]));
    drop(current);
}

#[test]
fn live_registry_damage_and_inventory_limits_fail_closed() {
    let fixture = Fixture::new();
    let owner = fixture.open(root(1));
    let record = std::fs::read(fixture.record()).unwrap();
    assert!(fixture.roots(0).is_err());
    for length in [0, HEADER_BYTES - 1, record.len() - 1] {
        std::fs::write(fixture.record(), &record[..length]).unwrap();
        assert!(fixture.roots(1).is_err());
    }
    let mut corrupt = record.clone();
    corrupt[HEADER_BYTES + 10] ^= 1;
    std::fs::write(fixture.record(), corrupt).unwrap();
    assert!(fixture.roots(1).is_err());
    std::fs::remove_file(fixture.record()).unwrap();
    assert!(fixture.roots(1).is_err());
    std::fs::write(fixture.record(), &record).unwrap();
    let metadata = CatalogMetadataLease::acquire_blocking(&fixture.project).unwrap();
    assert!(active_roots(&fixture.branch, [42; 16], BRANCH, 1, &metadata).is_err());
    assert!(active_roots(&fixture.branch, PROJECT, [18; 16], 1, &metadata).is_err());
    drop(metadata);
    assert_eq!(fixture.roots(1).unwrap(), Some(vec![owner.root()]));
}

#[test]
fn failed_retirement_retains_surplus_roots_until_a_successful_update() {
    let fixture = Fixture::new();
    let first = fixture.open(root(1));
    let next = first
        .registry()
        .publication()
        .unwrap()
        .pin(root(2))
        .unwrap();
    let pending = fixture.branch.join(DIRECTORY).join(PENDING);
    std::fs::create_dir(&pending).unwrap();
    drop(first);
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(1), root(2)]));
    assert!(next.registry().publication().unwrap().pin(root(3)).is_err());
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(1), root(2)]));
    std::fs::remove_dir(pending).unwrap();
    let newest = next.registry().publication().unwrap().pin(root(3)).unwrap();
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(2), root(3)]));
    drop((next, newest));
}

#[test]
fn publication_barrier_excludes_a_concurrent_sweep_until_the_root_is_registered() {
    let fixture = Fixture::new();
    let first = fixture.open(root(1));
    let publication = first.registry().publication().unwrap();
    let project = fixture.project.clone();
    let blocked =
        std::thread::spawn(move || CatalogMetadataLease::acquire(&project).unwrap_err().kind())
            .join()
            .unwrap();
    assert_eq!(blocked, io::ErrorKind::WouldBlock);
    let next = publication.pin(root(2)).unwrap();
    drop(publication);
    assert_eq!(fixture.roots(2).unwrap(), Some(vec![root(1), root(2)]));
    drop((first, next));
}

#[cfg(unix)]
#[test]
fn root_publication_preserves_and_rejects_dangling_destinations() {
    for destination in [RECORD, PENDING] {
        let fixture = Fixture::new();
        let owner = fixture.open(root(1));
        let path = fixture.branch.join(DIRECTORY).join(destination);
        let original = std::fs::read(&path).ok();
        if original.is_some() {
            std::fs::remove_file(&path).unwrap();
        }
        std::os::unix::fs::symlink("missing-evidence", &path).unwrap();
        assert!(owner
            .registry()
            .publication()
            .unwrap()
            .pin(root(2))
            .is_err());
        assert_eq!(
            std::fs::read_link(&path).unwrap(),
            Path::new("missing-evidence")
        );
        if destination == RECORD {
            assert!(fixture.roots(2).is_err());
        }
        std::fs::remove_file(&path).unwrap();
        if let Some(bytes) = original {
            std::fs::write(&path, bytes).unwrap();
        }
        assert_eq!(fixture.roots(1).unwrap(), Some(vec![root(1)]));
    }
}

#[test]
fn a_live_foreign_process_declares_roots_and_a_killed_owner_cannot_keep_authority() {
    const ENV: &str = "HAWDB_ROOT_PIN_TEST_DIRECTORY";
    if let Some(directory) = std::env::var_os(ENV) {
        let project = PathBuf::from(directory);
        let branch = project.join(hawdb_core::Uuid::from_bytes(BRANCH).to_string());
        let lease = Arc::new(DatabaseDirectoryLease::acquire(&branch).unwrap());
        let _root = RootRegistry::open(&project, &branch, PROJECT, BRANCH, lease, root(7)).unwrap();
        println!("root-pin-child-ready");
        std::io::stdout().flush().unwrap();
        let mut byte = [0];
        std::io::stdin().read_exact(&mut byte).unwrap();
        return;
    }
    let fixture = Fixture::new();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "branch_root_pins::tests::a_live_foreign_process_declares_roots_and_a_killed_owner_cannot_keep_authority", "--nocapture"])
        .env(ENV, &fixture.project)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert!(
            output.read_line(&mut line).unwrap() > 0,
            "child exited before registering roots"
        );
        if line.contains("root-pin-child-ready") {
            break;
        }
    }
    assert!(matches!(
        DatabaseDirectoryLease::acquire(&fixture.branch),
        Err(DatabaseDirectoryLeaseError::AlreadyOpen)
    ));
    assert_eq!(fixture.roots(1).unwrap(), Some(vec![root(7)]));
    child.kill().unwrap();
    assert!(!child.wait().unwrap().success());
    let _new_unregistered_owner = DatabaseDirectoryLease::acquire(&fixture.branch).unwrap();
    assert_eq!(fixture.roots(1).unwrap(), None);
}
