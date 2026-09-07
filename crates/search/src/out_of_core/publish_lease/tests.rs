use super::*;
use std::cell::Cell;
use std::fs;
use std::path::{Component, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

thread_local! {
    static CANCEL_REGISTER: Cell<bool> = const { Cell::new(false) };
    static CANCEL_LOCK: Cell<bool> = const { Cell::new(false) };
    static REGISTRATIONS: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn registered(task: &RuntimeTaskContext) {
    REGISTRATIONS.set(REGISTRATIONS.get() + 1);
    if CANCEL_REGISTER.replace(false) {
        task.cancellation().cancel();
    }
}

pub(super) fn locked(task: &RuntimeTaskContext) {
    if CANCEL_LOCK.replace(false) {
        task.cancellation().cancel();
    }
}

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-publisher-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn task(limit: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(limit as u64, 0))
}

fn entries_for(root: &Path) -> usize {
    let canonical = fs::canonicalize(root).unwrap();
    active_publishers()
        .entries
        .iter()
        .filter(|entry| entry.root.as_ref() == canonical)
        .count()
}

fn admission(root: &Path) -> (usize, usize) {
    let canonical = fs::canonicalize(root).unwrap();
    let path = canonical.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE);
    let native_len = canonical.as_os_str().as_encoded_bytes().len();
    let length = native_len + SEARCH_PROJECTION_PUBLISH_LOCK_FILE.len() + 1;
    let verbatim = matches!(canonical.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim());
    let join = if verbatim {
        4 * length.max(8) + 4 * length.max(4) * size_of::<Component<'_>>()
    } else {
        3 * length.max(8)
    };
    #[cfg(unix)]
    let canonical_peak = root.as_os_str().as_encoded_bytes().len() + 1 + native_len;
    #[cfg(windows)]
    let canonical_peak = {
        use std::os::windows::ffi::OsStrExt;
        let count = canonical.as_os_str().encode_wide().count();
        12 * count.max(8) + if count >= 512 { 2 * (count + 1) } else { 0 }
    };
    let node = size_of::<Registration>() + 2 * size_of::<usize>();
    let retained = canonical.capacity() + node;
    (
        canonical_peak
            .max(canonical.capacity() + join)
            .max(retained + path.capacity()),
        retained,
    )
}

#[test]
fn publication_lease_rejects_concurrent_owner_and_recovers_after_drop() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let first = SearchProjectionPublishLease::acquire(&root).unwrap();
    assert!(SearchProjectionPublishLease::acquire(&root)
        .unwrap_err()
        .to_string()
        .contains("another search projection publication is active"));
    assert!(SearchProjectionPublishLease::acquire(&root.join(".")).is_err());
    drop(first);
    SearchProjectionPublishLease::acquire(&root).unwrap();
    assert_eq!(entries_for(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_publisher_budget_covers_paths_and_node_without_global_spare_capacity() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let (peak, retained) = admission(&root);
    for limit in [peak + 137 - 1, peak + 137] {
        let task = task(limit);
        let memory = BuildMemory::new(&task).unwrap();
        let other = memory.input.reserve(137).unwrap();
        REGISTRATIONS.set(0);
        let result = SearchProjectionPublishLease::acquire_with_context(&root, &memory, &task);
        assert_eq!(result.is_ok(), limit == peak + 137);
        if let Ok(lease) = result {
            assert_eq!(REGISTRATIONS.get(), 1);
            assert_eq!(entries_for(&root), 1);
            assert_eq!(memory.ledger.snapshot().used_bytes, retained + 137);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak + 137);
            let ledger = memory.ledger.clone();
            drop(memory);
            drop(other);
            assert_eq!(ledger.snapshot().used_bytes, retained);
            drop(lease);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert_eq!(REGISTRATIONS.get(), 0);
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            assert!(!root.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE).exists());
        }
        assert_eq!(entries_for(&root), 0);
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn independent_publishers_release_only_their_originating_root() {
    let left = root();
    let right = root();
    fs::create_dir(&left).unwrap();
    fs::create_dir(&right).unwrap();
    let task = task(1024 * 1024);
    let a = BuildMemory::new(&task).unwrap();
    let b = BuildMemory::new(&task).unwrap();
    let first = SearchProjectionPublishLease::acquire_with_context(&left, &a, &task).unwrap();
    let second = SearchProjectionPublishLease::acquire_with_context(&right, &b, &task).unwrap();
    let live = b.ledger.snapshot().used_bytes;
    assert!(live > 0 && a.ledger.snapshot().used_bytes > 0);
    drop(first);
    assert_eq!(a.ledger.snapshot().used_bytes, 0);
    assert_eq!(b.ledger.snapshot().used_bytes, live);
    assert_eq!(entries_for(&left), 0);
    assert_eq!(entries_for(&right), 1);
    drop(second);
    assert_eq!(b.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(left).unwrap();
    fs::remove_dir_all(right).unwrap();
}

#[test]
fn registration_node_is_admitted_before_insertion_and_retains_its_path() {
    let root = root();
    fs::create_dir(&root).unwrap();
    for allowed in [
        registration_bytes().unwrap() - 1,
        registration_bytes().unwrap(),
    ] {
        let limit = 1024 * 1024;
        let task = task(limit);
        let memory = BuildMemory::new(&task).unwrap();
        let path = OwnedPath::canonicalize(&root, &memory, &task).unwrap();
        let path_bytes = path.capacity();
        let padding = memory.input.reserve(limit - path_bytes - allowed).unwrap();
        REGISTRATIONS.set(0);
        let result = RegistrationGuard::acquire(path, &memory, &task);
        if allowed == registration_bytes().unwrap() {
            let owner = result.unwrap();
            assert_eq!(REGISTRATIONS.get(), 1);
            assert_eq!(entries_for(&root), 1);
            assert_eq!(memory.ledger.snapshot().used_bytes, limit);
            drop(owner);
        } else {
            assert!(result.unwrap_err().to_string().contains("query memory"));
            assert_eq!(REGISTRATIONS.get(), 0);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, padding.bytes());
        assert_eq!(entries_for(&root), 0);
        drop(padding);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn registration_rejects_a_duplicate_before_any_os_lock_is_opened() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let task = task(1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let first = RegistrationGuard::acquire(
        OwnedPath::canonicalize(&root, &memory, &task).unwrap(),
        &memory,
        &task,
    )
    .unwrap();
    let live = memory.ledger.snapshot().used_bytes;
    let duplicate = OwnedPath::canonicalize(&root.join("."), &memory, &task).unwrap();
    REGISTRATIONS.set(0);
    assert!(RegistrationGuard::acquire(duplicate, &memory, &task)
        .unwrap_err()
        .to_string()
        .contains("another search projection publication"));
    assert_eq!(REGISTRATIONS.get(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, live);
    assert_eq!(entries_for(&root), 1);
    assert!(!root.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE).exists());
    drop(first);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(entries_for(&root), 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn cancellation_after_os_acquisition_closes_the_handle_and_unregisters() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let task = task(1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    CANCEL_LOCK.set(true);
    assert!(
        SearchProjectionPublishLease::acquire_with_context(&root, &memory, &task)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert_eq!(entries_for(&root), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(root.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE).is_file());
    probe(&root, false);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_and_open_failure_roll_back_registration_without_deleting_the_lock() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let lock = root.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE);
    let task = task(1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    CANCEL_REGISTER.set(true);
    assert!(
        SearchProjectionPublishLease::acquire_with_context(&root, &memory, &task)
            .unwrap_err()
            .to_string()
            .contains("cancelled")
    );
    assert!(!lock.exists());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(entries_for(&root), 0);
    fs::create_dir(&lock).unwrap();
    assert!(SearchProjectionPublishLease::acquire_with_context(
        &root,
        &memory,
        &RuntimeTaskContext::default()
    )
    .is_err());
    assert!(lock.is_dir());
    assert_eq!(entries_for(&root), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(&lock).unwrap();
    fs::write(&lock, b"sticky lock contents").unwrap();
    drop(SearchProjectionPublishLease::acquire(&root).unwrap());
    assert_eq!(fs::read(&lock).unwrap(), b"sticky lock contents");
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn publisher_aliases_keep_the_same_canonical_lock_identity() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let actual = root.join("actual");
    fs::create_dir(&actual).unwrap();
    let alias = root.join("alias");
    std::os::unix::fs::symlink(&actual, &alias).unwrap();
    let first = SearchProjectionPublishLease::acquire(&actual).unwrap();
    assert!(SearchProjectionPublishLease::acquire(&alias).is_err());
    drop(first);
    drop(SearchProjectionPublishLease::acquire(&alias).unwrap());
    fs::remove_dir_all(root).unwrap();
}

fn probe(root: &Path, busy: bool) {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "out_of_core::publish_lease::tests::subprocess_probe",
            "--ignored",
            "--nocapture",
        ])
        .env("SKEIN_TEST_PUBLISH_LOCK_ROOT", root)
        .env("SKEIN_TEST_PUBLISH_LOCK_BUSY", if busy { "1" } else { "0" })
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

#[test]
fn publication_lease_keeps_the_os_lock_until_drop() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let lease = SearchProjectionPublishLease::acquire(&root).unwrap();
    probe(&root, true);
    drop(lease);
    probe(&root, false);
    assert_eq!(entries_for(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "subprocess-only lock fixture invoked by the parent regression"]
fn subprocess_probe() {
    let root =
        std::env::var_os("SKEIN_TEST_PUBLISH_LOCK_ROOT").expect("parent supplies fixture root");
    let busy = std::env::var("SKEIN_TEST_PUBLISH_LOCK_BUSY").unwrap() == "1";
    let task = task(1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    let result =
        SearchProjectionPublishLease::acquire_with_context(Path::new(&root), &memory, &task);
    if busy {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("another search projection publication is active"));
    } else {
        drop(result.unwrap());
    }
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(entries_for(Path::new(&root)), 0);
}
