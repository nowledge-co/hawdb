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

use super::{context_for_path, ProjectFileDescriptors, DEFAULT_MAX_OPEN_FILES};
use crate::file_io::{self, File};
use crate::immutable_files::ImmutableFileBinding;
use crate::immutable_object::{ObjectKind, ObjectReference};
use hawdb_core::error::{file_descriptor_error, FileDescriptorError, HawDBError};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

struct Fixture {
    root: PathBuf,
    project: ProjectFileDescriptors,
}

impl Fixture {
    fn new(limit: usize) -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "hawdb-fd-budget-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let project = ProjectFileDescriptors::acquire(&root, limit).unwrap();
        Self { root, project }
    }

    fn binding(&self, name: &str, bytes: &[u8]) -> ImmutableFileBinding {
        let object_path = self.root.join(name);
        std::fs::write(&object_path, bytes).unwrap();
        ImmutableFileBinding {
            reference: ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, bytes),
            object_path,
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn default_budget_admits_large_reader_reservations_without_opening_files() {
    let fixture = Fixture::new(crate::config::WalReplayConfig::default().max_open_files);
    let sibling = ProjectFileDescriptors::acquire_existing(
        &fixture.root,
        crate::config::WalReplayConfig::default().max_open_files,
    )
    .unwrap();
    assert_eq!(sibling.metrics().configured_limit, 1024);
    let reader_count = 300.min(sibling.metrics().effective_limit);
    let readers = fixture.project.reserve(reader_count).unwrap();
    assert_eq!(sibling.metrics().open, 0);
    assert_eq!(sibling.metrics().reserved, reader_count);
    let remaining = sibling
        .reserve(sibling.metrics().effective_limit - reader_count)
        .unwrap();
    let error = fixture.project.reserve(1).unwrap_err();
    assert!(matches!(
        file_descriptor_error(&error),
        Some(FileDescriptorError::BudgetExceeded { available: 0, .. })
    ));
    drop((readers, remaining));
    assert_eq!(sibling.metrics().open, 0);
    assert_eq!(sibling.metrics().reserved, 0);
    assert!(Fixture::new(32).project.reserve(33).is_err());
}

#[test]
fn descriptor_cap_covers_clones_failures_and_reservation_reuse() {
    let fixture = Fixture::new(4);
    let path = fixture.root.join("data");
    std::fs::write(&path, b"value").unwrap();
    let owner = File::open(&path).unwrap();
    let copy = owner.try_clone().unwrap();
    let reservation = fixture.project.reserve(2).unwrap();
    let temporary = File::open(&path).unwrap();
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (3, 1)
    );
    let final_slot = File::open(&path).unwrap();
    let error = File::open(&path).unwrap_err();
    assert_eq!(
        file_descriptor_error(&error),
        Some(FileDescriptorError::BudgetExceeded {
            requested: 1,
            available: 0,
            limit: 4
        })
    );
    drop(temporary);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (3, 1)
    );
    let error = File::open(fixture.root.join("missing")).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(
        (
            fixture.project.metrics().open,
            fixture.project.metrics().reserved
        ),
        (3, 1)
    );
    drop(reservation);
    assert_eq!(fixture.project.metrics().reserved, 0);
    drop((owner, copy, final_slot));
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().high_water, 4);
}

#[test]
fn descriptor_exhaustion_preserves_relational_checkpoint_for_retry() {
    use crate::relational::{
        decode_relational_checkpoint_file, encode_relational_checkpoint, RelationalDecodeLimits,
        RelationalState,
    };

    let fixture = Fixture::new(4);
    let checkpoint = fixture.root.join("checkpoint");
    let encoded = encode_relational_checkpoint(7, &RelationalState::default()).unwrap();
    std::fs::write(&checkpoint, &encoded).unwrap();
    let held = (0..4)
        .map(|_| File::open(&checkpoint).unwrap())
        .collect::<Vec<_>>();
    let error =
        decode_relational_checkpoint_file(&checkpoint, RelationalDecodeLimits::checkpoint())
            .unwrap_err();
    let expected = FileDescriptorError::BudgetExceeded {
        requested: 1,
        available: 0,
        limit: 4,
    };
    assert_eq!(file_descriptor_error(&error), Some(expected.clone()));
    assert_eq!(
        HawDBError::from_storage_error(error),
        HawDBError::FileDescriptors(expected)
    );
    assert_eq!(fixture.project.metrics().open, 4);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(std::fs::read(&checkpoint).unwrap(), encoded);
    drop(held);
    let recovered =
        decode_relational_checkpoint_file(&checkpoint, RelationalDecodeLimits::checkpoint())
            .unwrap();
    assert_eq!(recovered.epoch, 7);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn independent_contexts_share_the_limit_and_reject_conflicting_configuration() {
    let fixture = Fixture::new(3);
    let second = ProjectFileDescriptors::acquire(&fixture.root, 3).unwrap();
    assert!(Arc::ptr_eq(&fixture.project.state, &second.state));
    let file = File::create(fixture.root.join("data")).unwrap();
    assert_eq!(second.metrics().open, 1);
    assert!(matches!(
        ProjectFileDescriptors::acquire(&fixture.root, 4),
        Err(HawDBError::FileDescriptors(
            FileDescriptorError::ConfigurationConflict {
                configured: 3,
                requested: 4
            }
        ))
    ));
    assert!(matches!(
        ProjectFileDescriptors::acquire(&fixture.root, 0),
        Err(HawDBError::FileDescriptors(
            FileDescriptorError::InvalidBudget { limit: 0 }
        ))
    ));
    drop(file);
    assert_eq!(second.metrics().open, 0);
}

#[test]
fn directory_entries_retain_one_permit_and_nested_removal_is_bounded() {
    let fixture = Fixture::new(2);
    for index in 0..80 {
        std::fs::write(fixture.root.join(format!("value-{index}")), b"x").unwrap();
    }
    let mut directory = file_io::read_dir(&fixture.root).unwrap();
    let entry = directory.next().unwrap().unwrap();
    drop(directory);
    assert_eq!(fixture.project.metrics().open, 1);
    let file = File::open(entry.path()).unwrap();
    assert!(File::open(entry.path()).is_err());
    drop((file, entry));
    assert_eq!(fixture.project.metrics().open, 0);
    let subtree = fixture.root.join("nested");
    std::fs::create_dir_all(subtree.join("a/b/c")).unwrap();
    std::fs::write(subtree.join("a/b/c/payload"), b"data").unwrap();
    file_io::remove_dir_all(&subtree).unwrap();
    assert!(!subtree.exists());
    assert_eq!(fixture.project.metrics().open, 0);
    assert!(fixture.project.metrics().high_water <= 2);
}

#[test]
fn shared_immutable_handles_evict_only_idle_files_and_revalidate_on_reopen() {
    let fixture = Fixture::new(2);
    let context = context_for_path(&fixture.root).unwrap();
    let binding = fixture.binding("immutable", b"payload");
    let first = fixture
        .project
        .immutable_handles
        .get(&binding, &context)
        .unwrap();
    let sibling = fixture
        .project
        .immutable_handles
        .get(&binding, &context)
        .unwrap();
    assert!(Arc::ptr_eq(&first, &sibling));
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let owner = File::create(fixture.root.join("owner")).unwrap();
    assert!(File::create(fixture.root.join("rejected")).is_err());
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    drop((first, sibling));
    let replacement = File::create(fixture.root.join("replacement")).unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert_eq!(fixture.project.metrics().cache_evictions, 1);
    drop((owner, replacement));
    std::fs::write(&binding.object_path, b"changed").unwrap();
    assert_eq!(
        fixture
            .project
            .immutable_handles
            .get(&binding, &context)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );
    assert_eq!(fixture.project.metrics().open, 0);
    std::fs::write(&binding.object_path, b"payload").unwrap();
    let reopened = fixture
        .project
        .immutable_handles
        .get(&binding, &context)
        .unwrap();
    let mut value = [0; 7];
    crate::io::read_exact_at(&reopened, &mut value, 0).unwrap();
    assert_eq!(&value, b"payload");
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    assert!(fixture.project.metrics().cache_hits >= 1);
    assert!(fixture.project.metrics().cache_misses >= 3);
}

#[test]
fn immutable_handle_pressure_evicts_the_least_recently_used_idle_file() {
    let fixture = Fixture::new(3);
    let context = context_for_path(&fixture.root).unwrap();
    let mut bindings = [
        fixture.binding("first", b"first"),
        fixture.binding("second", b"second"),
        fixture.binding("third", b"third"),
    ];
    // Make the previously used key-order policy choose a hot entry.
    bindings.sort_by_key(|binding| binding.reference);
    let handles = bindings.each_ref().map(|binding| {
        fixture
            .project
            .immutable_handles
            .get(binding, &context)
            .unwrap()
    });
    for binding in &bindings[..2] {
        let (file, validation_bytes) = fixture
            .project
            .immutable_handles
            .get_admitted(binding, &context, |_| {
                panic!("warm handles must not repeat validation admission")
            })
            .unwrap();
        assert_eq!(validation_bytes, 0);
        drop(file);
    }
    let observers = handles.each_ref().map(Arc::downgrade);
    drop(handles);
    let pressure = File::create(fixture.root.join("pressure")).unwrap();
    assert!(observers[0].upgrade().is_some());
    assert!(observers[1].upgrade().is_some());
    assert!(observers[2].upgrade().is_none());
    assert_eq!(fixture.project.metrics().cached_handles, 2);
    assert_eq!(fixture.project.metrics().cache_evictions, 1);
    assert_eq!(fixture.project.metrics().open, 3);
    drop(pressure);

    let evicted = &bindings[2];
    let mut admission_calls = 0;
    let error = fixture
        .project
        .immutable_handles
        .get_admitted(evicted, &context, |bytes| {
            admission_calls += 1;
            assert_eq!(bytes, evicted.reference.byte_length);
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "validation budget refused",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(admission_calls, 1);
    assert_eq!(fixture.project.metrics().cached_handles, 2);
    assert_eq!(fixture.project.metrics().open, 2);

    let (file, validation_bytes) = fixture
        .project
        .immutable_handles
        .get_admitted(evicted, &context, |bytes| {
            admission_calls += 1;
            assert_eq!(bytes, evicted.reference.byte_length);
            Ok(())
        })
        .unwrap();
    assert_eq!(admission_calls, 2);
    assert_eq!(validation_bytes, evicted.reference.byte_length);
    assert_eq!(fixture.project.metrics().cached_handles, 3);
    assert!(fixture.project.metrics().high_water <= 3);
    drop(file);
}

#[test]
fn immutable_handle_eviction_preserves_a_concurrent_native_read() {
    let fixture = Fixture::new(2);
    let active = fixture.binding("active", b"active reader");
    let idle = fixture.binding("idle", b"idle");
    let alias = fixture.root.join("logical");
    fixture
        .project
        .immutable_handles
        .bind(&alias, active)
        .unwrap();
    let logical = File::open(&alias).unwrap();
    let context = context_for_path(&fixture.root).unwrap();
    drop(
        fixture
            .project
            .immutable_handles
            .get(&idle, &context)
            .unwrap(),
    );
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let reader_barrier = barrier.clone();
    let reader = std::thread::spawn(move || {
        logical
            .with_native(|native| {
                // The cache Arc is pinned for the complete native operation, even
                // while another thread needs a slot and evicts an idle handle.
                reader_barrier.wait();
                reader_barrier.wait();
                let mut native = native;
                native.seek(SeekFrom::Start(0))?;
                let mut bytes = [0; 13];
                native.read_exact(&mut bytes)?;
                Ok(bytes)
            })
            .unwrap()
    });
    barrier.wait();
    let pressure = File::create(fixture.root.join("pressure")).unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    assert_eq!(fixture.project.metrics().cache_evictions, 1);
    assert_eq!(fixture.project.metrics().open, 2);
    barrier.wait();
    assert_eq!(&reader.join().unwrap(), b"active reader");
    drop(pressure);
    assert_eq!(fixture.project.metrics().open, 1);
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
fn descriptor_os_limits() -> libc::rlimit {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: limit points to writable, correctly sized storage.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn project_admission_observes_the_os_soft_limit_and_reuses_the_configured_domain() {
    if run_descriptor_child(
        "file_descriptors::tests::project_admission_observes_the_os_soft_limit_and_reuses_the_configured_domain",
    ) {
        return;
    }
    let original = descriptor_os_limits();
    let lowered = libc::rlimit {
        rlim_cur: 256,
        rlim_max: original.rlim_max,
    };
    // SAFETY: This isolated child lowers only its own soft limit.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) }, 0);
    let fixture = Fixture::new(DEFAULT_MAX_OPEN_FILES);
    let metrics = fixture.project.metrics();
    let effective = 256 - super::os_limit::HOST_HEADROOM;
    assert_eq!(descriptor_os_limits().rlim_cur, lowered.rlim_cur);
    assert_eq!(descriptor_os_limits().rlim_max, original.rlim_max);
    assert_eq!(metrics.os_soft_limit, Some(256));
    assert_eq!(metrics.configured_limit, 1024);
    assert_eq!(
        (metrics.limit, metrics.effective_limit),
        (effective, effective)
    );
    assert!(metrics.os_limit_clamped);
    assert_eq!(metrics.open, 0);
    let sibling = ProjectFileDescriptors::acquire_existing(&fixture.root, 1024).unwrap();
    assert_eq!(sibling.metrics(), metrics);
    assert_eq!(
        ProjectFileDescriptors::acquire_existing(&fixture.root, effective).unwrap_err(),
        HawDBError::FileDescriptors(FileDescriptorError::ConfigurationConflict {
            configured: 1024,
            requested: effective,
        })
    );
    let held = sibling.reserve(effective).unwrap();
    assert!(matches!(
        file_descriptor_error(&fixture.project.reserve(1).unwrap_err()),
        Some(FileDescriptorError::BudgetExceeded { available: 0, limit, .. }) if limit == effective
    ));
    drop(held);
    let smaller = Fixture::new(16);
    assert_eq!(descriptor_os_limits().rlim_cur, lowered.rlim_cur);
    assert_eq!(smaller.project.metrics().configured_limit, 16);
    assert_eq!(smaller.project.metrics().effective_limit, 16);
    assert!(!smaller.project.metrics().os_limit_clamped);
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn project_admission_preserves_a_low_hard_limit_and_admits_a_smaller_budget() {
    if run_descriptor_child("file_descriptors::tests::project_admission_preserves_a_low_hard_limit_and_admits_a_smaller_budget") {
        return;
    }
    let restricted = libc::rlimit {
        rlim_cur: 96,
        rlim_max: 96,
    };
    // SAFETY: Lowering the hard limit is confined to this expendable child.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &restricted) },
        0
    );
    let fixture = Fixture::new(DEFAULT_MAX_OPEN_FILES);
    assert_eq!(descriptor_os_limits().rlim_cur, 96);
    assert_eq!(descriptor_os_limits().rlim_max, 96);
    assert_eq!(fixture.project.metrics().configured_limit, 1024);
    assert_eq!(fixture.project.metrics().effective_limit, 32);
    assert!(fixture.project.metrics().os_limit_clamped);
    let held = fixture.project.reserve(32).unwrap();
    assert!(fixture.project.reserve(1).is_err());
    drop(held);
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
#[test]
fn project_admission_reports_insufficient_os_capacity_without_installing_a_root() {
    if run_descriptor_child("file_descriptors::tests::project_admission_reports_insufficient_os_capacity_without_installing_a_root") {
        return;
    }
    let restricted = libc::rlimit {
        rlim_cur: 68,
        rlim_max: 96,
    };
    // SAFETY: Both changes are confined to this expendable child.
    assert_eq!(
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &restricted) },
        0
    );
    let root = std::env::temp_dir().join(format!(
        "hawdb-fd-insufficient-limit-{}",
        std::process::id()
    ));
    let requested = 8 + super::os_limit::HOST_HEADROOM;
    assert_eq!(
        ProjectFileDescriptors::acquire(&root, DEFAULT_MAX_OPEN_FILES).unwrap_err(),
        HawDBError::FileDescriptors(FileDescriptorError::OsLimit {
            requested,
            os_code: None,
            soft: Some(68),
            hard: Some(96),
        })
    );
    assert!(!root.exists());
    assert_eq!(descriptor_os_limits().rlim_cur, 68);
    assert_eq!(descriptor_os_limits().rlim_max, 96);
    let retry = ProjectFileDescriptors::acquire(&root, 4).unwrap();
    assert_eq!(retry.metrics().limit, 4);
    assert_eq!((retry.metrics().open, retry.metrics().reserved), (0, 0));
    drop(retry);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn immutable_identity_includes_kind_version_and_project() {
    let fixture = Fixture::new(3);
    let context = context_for_path(&fixture.root).unwrap();
    let binding = fixture.binding("first", b"payload");
    let mut wrong = binding.clone();
    wrong.reference.kind = ObjectKind::Checkpoint;
    assert!(fixture
        .project
        .immutable_handles
        .get(&wrong, &context)
        .is_err());
    wrong = binding.clone();
    wrong.reference.format_version += 1;
    assert!(fixture
        .project
        .immutable_handles
        .get(&wrong, &context)
        .is_err());
    let opened = fixture
        .project
        .immutable_handles
        .get(&binding, &context)
        .unwrap();
    let other = Fixture::new(3);
    let other_binding = other.binding("same", b"payload");
    let other_context = context_for_path(&other.root).unwrap();
    let other_opened = other
        .project
        .immutable_handles
        .get(&other_binding, &other_context)
        .unwrap();
    assert!(!Arc::ptr_eq(&opened, &other_opened));
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    assert_eq!(other.project.metrics().cached_handles, 1);
}

#[test]
fn shared_positioned_reads_do_not_share_a_logical_cursor() {
    let fixture = Fixture::new(2);
    let binding = fixture.binding("parallel", b"abcdefgh");
    let context = context_for_path(&fixture.root).unwrap();
    let file = fixture
        .project
        .immutable_handles
        .get(&binding, &context)
        .unwrap();
    std::thread::scope(|scope| {
        for offset in 0..8 {
            let file = file.clone();
            scope.spawn(move || {
                for _ in 0..128 {
                    let mut byte = [0];
                    crate::io::read_exact_at(&file, &mut byte, offset).unwrap();
                    assert_eq!(byte[0], b'a' + offset as u8);
                }
            });
        }
    });
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let mutable = fixture.root.join("mutable");
    File::create(&mutable).unwrap().write_all(b"plain").unwrap();
    let mut value = String::new();
    File::open(mutable)
        .unwrap()
        .read_to_string(&mut value)
        .unwrap();
    assert_eq!(value, "plain");
}

#[test]
fn immutable_logical_cursors_survive_eviction_and_share_only_between_clones() {
    let fixture = Fixture::new(1);
    let binding = fixture.binding("object", b"abcdef");
    let alias = fixture.root.join("logical");
    fixture
        .project
        .immutable_handles
        .bind(&alias, binding.clone())
        .unwrap();
    let mut first = File::open(&alias).unwrap();
    let mut clone = first.try_clone().unwrap();
    let mut independent = File::open(&alias).unwrap();
    assert_eq!(fixture.project.metrics().open, 0);
    let mut bytes = [0; 2];
    first.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ab");
    clone.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"cd");
    independent.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ab");
    crate::io::read_exact_at(&first, &mut bytes, 0).unwrap();
    assert_eq!(&bytes, b"ab");
    assert_eq!(first.stream_position().unwrap(), 4);
    assert!(first.seek(SeekFrom::Current(-5)).is_err());
    assert_eq!(clone.stream_position().unwrap(), 4);
    assert_eq!(clone.seek(SeekFrom::End(-2)).unwrap(), 4);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let mutable = File::create(fixture.root.join("pressure")).unwrap();
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    assert!(matches!(
        first.read_exact(&mut bytes),
        Err(error) if matches!(file_descriptor_error(&error), Some(FileDescriptorError::BudgetExceeded { .. }))
    ));
    assert_eq!(first.stream_position().unwrap(), 4);
    drop(mutable);
    first.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"ef");
    assert_eq!(clone.stream_position().unwrap(), 6);
    assert_eq!(first.read(&mut bytes).unwrap(), 0);
    assert_eq!(independent.stream_position().unwrap(), 2);
    assert_eq!(fixture.project.metrics().high_water, 1);
}

#[test]
fn mutable_reopen_invalidates_only_future_readers_of_a_logical_path() {
    let fixture = Fixture::new(2);
    let binding = fixture.binding("object", b"original");
    let alias = fixture.root.join("logical");
    std::fs::write(&alias, b"original").unwrap();
    fixture
        .project
        .immutable_handles
        .bind(&alias, binding)
        .unwrap();
    let mut snapshot = File::open(&alias).unwrap();
    File::create(&alias).unwrap().write_all(b"updated").unwrap();
    assert!(fixture
        .project
        .immutable_handles
        .binding(&alias)
        .unwrap()
        .is_none());
    let mut current = File::open(&alias).unwrap();
    let mut text = String::new();
    current.read_to_string(&mut text).unwrap();
    assert_eq!(text, "updated");
    text.clear();
    snapshot.read_to_string(&mut text).unwrap();
    assert_eq!(text, "original");
    assert_eq!(
        snapshot.write(b"change").unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(fixture.project.metrics().high_water, 2);
}

#[cfg(any(unix, windows))]
fn run_descriptor_child(test_name: &str) -> bool {
    const CHILD_MARKER: &str = "HAWDB_FD_QUALIFICATION_CHILD";
    if std::env::var(CHILD_MARKER).as_deref() == Ok(test_name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(CHILD_MARKER, test_name)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "isolated descriptor qualification failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    true
}

#[cfg(unix)]
fn native_descriptor_count() -> usize {
    let directory = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(directory)
        .unwrap()
        .try_fold(0, |count, entry| entry.map(|_| count + 1))
        .unwrap()
}

#[cfg(windows)]
fn native_descriptor_count() -> usize {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetCurrentProcess"]
        fn current_process() -> *mut std::ffi::c_void;
        #[link_name = "GetProcessHandleCount"]
        fn process_handle_count(process: *mut std::ffi::c_void, count: *mut u32) -> i32;
    }
    let mut count = 0;
    // SAFETY: The pseudo-handle refers to this process without opening a handle;
    // count points to writable DWORD-sized storage for the duration of the call.
    let success = unsafe { process_handle_count(current_process(), &mut count) };
    assert_ne!(success, 0, "{}", std::io::Error::last_os_error());
    usize::try_from(count).unwrap()
}

#[cfg(any(unix, windows))]
#[test]
fn immutable_logical_files_do_not_retain_a_native_descriptor_per_alias() {
    if run_descriptor_child("file_descriptors::tests::immutable_logical_files_do_not_retain_a_native_descriptor_per_alias") {
        return;
    }
    let fixture = Fixture::new(1);
    let binding = fixture.binding("object", b"abcdef");
    let before = native_descriptor_count();
    let files = (0..128)
        .map(|index| {
            let alias = fixture.root.join(format!("alias-{index}"));
            fixture
                .project
                .immutable_handles
                .bind(&alias, binding.clone())
                .unwrap();
            File::open(alias).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(native_descriptor_count(), before);
    for file in &files {
        let mut bytes = [0; 3];
        crate::io::read_exact_at(file, &mut bytes, 2).unwrap();
        assert_eq!(&bytes, b"cde");
    }
    assert_eq!(native_descriptor_count(), before + 1);
    assert_eq!(fixture.project.metrics().cached_handles, 1);
    let pressure = File::create(fixture.root.join("pressure")).unwrap();
    drop(pressure);
    assert_eq!(native_descriptor_count(), before);
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    let mut bytes = [0; 3];
    crate::io::read_exact_at(&files[127], &mut bytes, 0).unwrap();
    assert_eq!(&bytes, b"abc");
    assert_eq!(native_descriptor_count(), before + 1);
    assert_eq!(fixture.project.metrics().high_water, 1);
    drop(files);
    drop(binding);
    drop(fixture);
    assert_eq!(native_descriptor_count(), before);
}

#[cfg(windows)]
#[test]
fn windows_sharing_violation_returns_quota_and_native_handles_for_retry() {
    use std::os::windows::fs::OpenOptionsExt;

    if run_descriptor_child("file_descriptors::tests::windows_sharing_violation_returns_quota_and_native_handles_for_retry") {
        return;
    }
    let fixture = Fixture::new(2);
    let path = fixture.root.join("data");
    std::fs::write(&path, b"unchanged").unwrap();
    let before = native_descriptor_count();
    // This host-owned handle causes a real CreateFile sharing violation after
    // engine quota acquisition without consuming the engine's reservation.
    let held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&path)
        .unwrap();
    assert_eq!(native_descriptor_count(), before + 1);
    let reservation = fixture.project.reserve(1).unwrap();
    let expected = fixture.project.metrics();
    assert_eq!((expected.open, expected.reserved), (0, 1));
    for _ in 0..3 {
        for error in [
            File::open(&path).unwrap_err(),
            File::create(&path).unwrap_err(),
        ] {
            assert_eq!(error.raw_os_error(), Some(32));
            assert_eq!(file_descriptor_error(&error), None);
            let observed = fixture.project.metrics();
            assert_eq!((observed.open, observed.reserved), (0, 1));
            assert_eq!(observed.budget_rejections, expected.budget_rejections);
            assert_eq!(observed.os_limit_rejections, expected.os_limit_rejections);
            assert!(observed.high_water <= observed.limit);
            assert_eq!(native_descriptor_count(), before + 1);
        }
    }
    drop(held);
    assert_eq!(native_descriptor_count(), before);
    let mut reopened = File::open(&path).unwrap();
    let mut contents = String::new();
    reopened.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "unchanged");
    assert_eq!(native_descriptor_count(), before + 1);
    assert_eq!(fixture.project.metrics().open, 1);
    drop(reopened);
    assert_eq!(fixture.project.metrics().reserved, 1);
    drop(reservation);
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(native_descriptor_count(), before);
}

#[cfg(target_os = "linux")]
#[test]
fn native_os_limit_releases_descriptor_reservations_for_retry() {
    if run_descriptor_child(
        "file_descriptors::tests::native_os_limit_releases_descriptor_reservations_for_retry",
    ) {
        return;
    }

    struct RestoreLimit(libc::rlimit);
    impl Drop for RestoreLimit {
        fn drop(&mut self) {
            // SAFETY: The saved limit is initialized and only this isolated
            // child changes its soft limit; the hard limit remains unchanged.
            assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) }, 0);
        }
    }

    let fixture = Fixture::new(16);
    let native_before = native_descriptor_count();
    let path = fixture.root.join("data");
    std::fs::write(&path, b"unchanged").unwrap();
    let owner = File::open(&path).unwrap();
    let reservation = fixture.project.reserve(3).unwrap();
    let mut expected = fixture.project.metrics();
    assert_eq!((expected.open, expected.reserved), (1, 3));

    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: The pointer refers to a writable, correctly sized rlimit value.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    let _restore = RestoreLimit(limit);
    limit.rlim_cur = limit.rlim_cur.min(64);
    assert!(limit.rlim_cur > native_before as libc::rlim_t + 4);
    // SAFETY: Only the isolated child lowers its soft limit, preserving the
    // hard limit so Drop can restore the original configuration.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    expected.os_soft_limit = fixture.project.metrics().os_soft_limit;

    // Host-owned handles consume the OS allowance without consuming the
    // engine's reserved quota. Engine admission must succeed before native IO.
    let mut pressure = Vec::new();
    loop {
        match std::fs::File::open("/dev/null") {
            Ok(file) => {
                pressure.push(file);
                assert!(pressure.len() <= 64);
            }
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
                break;
            }
        }
    }
    let mut assert_rejection = |error: std::io::Error| {
        assert_eq!(
            file_descriptor_error(&error),
            Some(FileDescriptorError::OsLimit {
                requested: 1,
                os_code: Some(libc::EMFILE),
                soft: None,
                hard: None,
            })
        );
        expected.os_limit_rejections += 1;
        let observed = fixture.project.metrics();
        // A clone retains its owner's pre-reservation context and can raise
        // the historical peak while returning its temporary slot on failure.
        assert!(observed.high_water >= expected.high_water);
        assert!(observed.high_water <= expected.limit);
        expected.high_water = observed.high_water;
        assert_eq!(observed, expected);
    };
    for _ in 0..3 {
        assert_rejection(File::open(&path).unwrap_err());
        assert_rejection(owner.try_clone().unwrap_err());
        assert_rejection(file_io::read_dir(&fixture.root).unwrap_err());
    }

    // One released native slot must support each retry in turn without a
    // leaked handle, even while the remaining host pressure is retained.
    drop(pressure.pop().unwrap());
    let mut reopened = File::open(&path).unwrap();
    let mut contents = String::new();
    reopened.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "unchanged");
    drop(reopened);
    drop(owner.try_clone().unwrap());
    assert_eq!(file_io::read_dir(&fixture.root).unwrap().count(), 1);
    assert_eq!(fixture.project.metrics(), expected);
    drop(pressure);
    drop((reservation, owner));
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(native_descriptor_count(), native_before);
}

#[test]
fn admission_quota_is_shared_by_nested_calls_and_leaves_unreserved_capacity_available() {
    let fixture = Fixture::new(6);
    let owner = File::create(fixture.root.join("owner")).unwrap();
    let reservation = fixture.project.reserve_admission(2).unwrap();
    assert_eq!(fixture.project.metrics().reserved, 2);
    let borrowed = fixture.project.reserve_admission(2).unwrap();
    assert_eq!(fixture.project.metrics().reserved, 2);
    drop(borrowed);
    assert_eq!(fixture.project.metrics().reserved, 2);
    let root = fixture.root.clone();
    let held = std::thread::spawn(move || {
        let held = (0..3)
            .map(|index| File::create(root.join(format!("competing-{index}"))).unwrap())
            .collect::<Vec<_>>();
        let error = File::create(root.join("excess")).unwrap_err();
        assert!(matches!(
            file_descriptor_error(&error),
            Some(FileDescriptorError::BudgetExceeded { available: 0, .. })
        ));
        held
    })
    .join()
    .unwrap();
    let temporary = File::create(fixture.root.join("candidate")).unwrap();
    assert_eq!(fixture.project.metrics().reserved, 1);
    assert_eq!(fixture.project.metrics().open, 5);
    drop(temporary);
    assert_eq!(fixture.project.metrics().reserved, 2);
    drop(held);
    drop(reservation);
    assert_eq!(fixture.project.metrics().reserved, 0);
    drop(owner);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn a_new_project_can_start_while_an_independent_project_is_full() {
    let first = Fixture::new(1);
    let first_owner = File::create(first.root.join("owner")).unwrap();
    assert_eq!(first.project.metrics().open, 1);
    let second = Fixture::new(1);
    let second_owner = File::create(second.root.join("owner")).unwrap();
    assert_eq!(second.project.metrics().open, 1);
    assert_eq!(first.project.metrics().open, 1);
    assert_eq!(first.project.metrics().high_water, 1);
    assert_eq!(second.project.metrics().high_water, 1);
    drop((first_owner, second_owner));
}

#[test]
fn nested_admission_rejects_insufficient_remaining_quota_before_opening() {
    let fixture = Fixture::new(3);
    let outer = fixture.project.reserve_admission(2).unwrap();
    let held = File::create(fixture.root.join("held")).unwrap();
    let error = fixture.project.reserve_admission(2).unwrap_err();
    assert_eq!(
        file_descriptor_error(&error),
        Some(FileDescriptorError::BudgetExceeded {
            requested: 2,
            available: 1,
            limit: 3,
        })
    );
    assert_eq!(fixture.project.metrics().open, 1);
    assert_eq!(fixture.project.metrics().reserved, 1);
    drop(held);
    let nested = fixture.project.reserve_admission(2).unwrap();
    assert_eq!(fixture.project.metrics().reserved, 2);
    drop(nested);
    drop(outer);
    assert_eq!(fixture.project.metrics().reserved, 0);
}

#[test]
fn failed_copy_on_write_preserves_the_logical_binding_and_snapshot() {
    let fixture = Fixture::new(2);
    let binding = fixture.binding("object", b"snapshot");
    let alias = fixture.root.join("missing-directory/logical");
    fixture
        .project
        .immutable_handles
        .bind(&alias, binding)
        .unwrap();
    let mut snapshot = File::open(&alias).unwrap();
    assert_eq!(
        File::create(&alias).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(fixture
        .project
        .immutable_handles
        .binding(&alias)
        .unwrap()
        .is_some());
    // Failure occurred before a private inode could be installed. Keep the
    // original logical reader rather than losing its immutable identity.
    assert_eq!(file_io::read(&alias).unwrap(), b"snapshot");
    let mut text = String::new();
    snapshot.read_to_string(&mut text).unwrap();
    assert_eq!(text, "snapshot");
}

#[cfg(unix)]
#[test]
fn ancestry_barriers_use_one_project_descriptor_at_a_time() {
    let fixture = Fixture::new(1);
    let nested = fixture.root.join("new/a/b/c");
    file_io::create_dir_all(&nested).unwrap();
    crate::durability::sync_directory_ancestors(&nested).unwrap();
    assert_eq!(fixture.project.metrics().open, 0);
    assert_eq!(fixture.project.metrics().high_water, 1);
}

#[cfg(unix)]
#[test]
fn writable_admission_retries_root_ancestry_after_a_read_only_domain() {
    let fixture = Fixture::new(1);
    let root = fixture.root.with_extension("read-only");
    std::fs::create_dir_all(&root).unwrap();
    let read_only = ProjectFileDescriptors::acquire_existing(&root, 1).unwrap();
    let failure = crate::durability::fail_sync_directory_for(read_only.root());
    assert!(ProjectFileDescriptors::acquire(&root, 1).is_err());
    assert!(!read_only
        .state
        .root_namespace_durable
        .load(Ordering::Acquire));
    assert_eq!(read_only.metrics().open, 0);
    drop(failure);
    let writable = ProjectFileDescriptors::acquire(&root, 1).unwrap();
    assert!(Arc::ptr_eq(&read_only.state, &writable.state));
    assert!(writable
        .state
        .root_namespace_durable
        .load(Ordering::Acquire));
    assert_eq!(writable.metrics().open, 0);
    assert_eq!(writable.metrics().high_water, 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn same_project_rename_and_hard_link_use_one_remaining_slot() {
    let fixture = Fixture::new(4);
    let source = fixture.root.join("source");
    let destination = fixture.root.join("renamed");
    let linked = fixture.root.join("linked");
    std::fs::write(&source, b"same-project-publication").unwrap();
    let held = (0..3)
        .map(|_| File::open(&source).unwrap())
        .collect::<Vec<_>>();
    file_io::rename(&source, &destination).unwrap();
    file_io::hard_link(&destination, &linked).unwrap();
    assert!(!source.exists());
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"same-project-publication"
    );
    assert_eq!(std::fs::read(&linked).unwrap(), b"same-project-publication");
    assert_eq!(fixture.project.metrics().open, 3);
    assert_eq!(fixture.project.metrics().reserved, 0);
    assert_eq!(fixture.project.metrics().high_water, 4);
    drop(held);
    assert_eq!(fixture.project.metrics().open, 0);
}

#[test]
fn cross_project_rename_admits_both_domains_before_mutation() {
    let source_project = Fixture::new(4);
    let destination_project = Fixture::new(4);
    let source = source_project.root.join("source");
    let destination = destination_project.root.join("destination");
    let sentinel = destination_project.root.join("sentinel");
    std::fs::write(&source, b"cross-project-publication").unwrap();
    std::fs::write(&sentinel, b"destination-budget-owner").unwrap();
    let source_held = (0..3)
        .map(|_| File::open(&source).unwrap())
        .collect::<Vec<_>>();
    let mut destination_held = (0..4)
        .map(|_| File::open(&sentinel).unwrap())
        .collect::<Vec<_>>();
    let error = file_io::rename(&source, &destination).unwrap_err();
    assert!(matches!(
        file_descriptor_error(&error),
        Some(FileDescriptorError::BudgetExceeded { .. })
    ));
    assert_eq!(
        std::fs::read(&source).unwrap(),
        b"cross-project-publication"
    );
    assert!(!destination.exists());
    assert_eq!(source_project.project.metrics().open, 3);
    assert_eq!(destination_project.project.metrics().open, 4);
    drop(destination_held.pop());
    file_io::rename(&source, &destination).unwrap();
    assert!(!source.exists());
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"cross-project-publication"
    );
    assert_eq!(source_project.project.metrics().high_water, 4);
    assert_eq!(destination_project.project.metrics().high_water, 4);
    drop((source_held, destination_held));
    assert_eq!(source_project.project.metrics().open, 0);
    assert_eq!(destination_project.project.metrics().open, 0);
}
