//! Contracts exercised through each real publication-lock acquisition path.

use std::fmt::Debug;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

const CHILD_DIRECTORY: &str = "SKEIN_PUBLICATION_LOCK_TEST_DIRECTORY";
const CHILD_SIDECAR: &str = "SKEIN_PUBLICATION_LOCK_TEST_SIDECAR";
const WAIT_LIMIT: Duration = Duration::from_secs(10);
const SENTINEL: &[u8] = b"persistent lock sidecar";

pub(crate) fn assert_contract<E: Debug + ToString + 'static>(
    sidecar: &str,
    acquire: fn(&Path) -> Result<File, E>,
    error_context: &str,
) {
    let directory = TestDirectory::new();
    let path = directory.0.join(sidecar);
    fs::write(&path, SENTINEL).unwrap();
    let holder = acquire(&directory.0).unwrap();
    assert_contended(&path);

    let mut child = ChildGuard(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "file_lock_tests::child_contends_for_publication_lock",
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD_DIRECTORY, &directory.0)
            .env(CHILD_SIDECAR, sidecar)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + WAIT_LIMIT;
    while !directory.0.join("ready").exists() {
        assert!(child.0.try_wait().unwrap().is_none(), "child exited early");
        assert!(
            Instant::now() < deadline,
            "child did not probe the held lock"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!directory.0.join("acquired").exists());
    drop(holder);
    wait_for_child(&mut child.0);
    assert!(directory.0.join("acquired").exists());
    assert_eq!(fs::read(&path).unwrap(), SENTINEL);

    // Reverse the roles: the actual publication adapter must wait, not fail
    // immediately or enter its publication section while another handle owns it.
    let holder = open_sidecar(&path);
    holder.lock().unwrap();
    let worker_directory = directory.0.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (locked_tx, locked_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let lock = acquire(&worker_directory).unwrap();
        let _ = locked_tx.send(lock);
    });
    started_rx.recv_timeout(WAIT_LIMIT).unwrap();
    assert!(matches!(
        locked_rx.recv_timeout(Duration::from_millis(30)),
        Err(RecvTimeoutError::Timeout)
    ));
    drop(holder);
    let lock = locked_rx.recv_timeout(WAIT_LIMIT).unwrap();
    worker.join().unwrap();
    assert_contended(&path);
    drop(lock);

    let invalid_directory = directory.0.join("not-a-directory");
    fs::write(&invalid_directory, SENTINEL).unwrap();
    let error = acquire(&invalid_directory).unwrap_err().to_string();
    assert!(error.contains(error_context), "{error}");
    assert_eq!(fs::read(&invalid_directory).unwrap(), SENTINEL);
    drop(acquire(&directory.0).unwrap());
    assert_eq!(fs::read(&path).unwrap(), SENTINEL);
}

pub(crate) fn assert_state_machine<E: Debug>(sidecar: &str, acquire: fn(&Path) -> Result<File, E>) {
    let mut action_counts = [0_usize; 6];
    for seed in 0_u64..32 {
        let directory = TestDirectory::new();
        let path = directory.0.join(sidecar);
        fs::write(&path, SENTINEL).unwrap();
        let unrelated = directory.0.join("independent.lock");
        let mut holder = None;
        let mut state = seed + 1;
        for _ in 0..64 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let action = ((state >> 32) % 6) as usize;
            action_counts[action] += 1;
            match action {
                0 if holder.is_none() => holder = Some(acquire(&directory.0).unwrap()),
                0 | 1 => probe(&path, holder.is_some()),
                2 => drop(holder.take()),
                3 => {
                    if let Some(lock) = holder.take() {
                        lock.unlock().unwrap();
                    }
                }
                4 => {
                    let other = open_sidecar(&unrelated);
                    other.try_lock().unwrap();
                }
                5 if holder.is_none() => {
                    // A publication failure must release its local guard.
                    let failed_publication = || -> Result<(), &'static str> {
                        let _lock = acquire(&directory.0).unwrap();
                        assert_contended(&path);
                        Err("injected publication failure")
                    };
                    assert_eq!(failed_publication(), Err("injected publication failure"));
                }
                5 => probe(&path, true),
                _ => unreachable!(),
            }
            probe(&path, holder.is_some());
        }
        drop(holder);
        probe(&path, false);
        assert_eq!(fs::read(&path).unwrap(), SENTINEL);
    }
    assert!(action_counts.iter().all(|count| *count > 0));
    eprintln!("{sidecar}: 32 seeds, 2048 actions; action counts {action_counts:?}");
}

fn probe(path: &Path, held: bool) {
    let contender = open_sidecar(path);
    match contender.try_lock() {
        Err(TryLockError::WouldBlock) if held => {}
        Ok(()) if !held => {}
        result => panic!("unexpected lock state (held={held}): {result:?}"),
    }
}

fn assert_contended(path: &Path) {
    probe(path, true);
}

fn open_sidecar(path: &Path) -> File {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap()
}

#[test]
#[ignore = "subprocess helper; invoked by publication lock contract tests"]
fn child_contends_for_publication_lock() {
    let directory = PathBuf::from(std::env::var_os(CHILD_DIRECTORY).unwrap());
    let sidecar = std::env::var_os(CHILD_SIDECAR).unwrap();
    let file = open_sidecar(&directory.join(sidecar));
    assert!(matches!(file.try_lock(), Err(TryLockError::WouldBlock)));
    fs::write(directory.join("ready"), b"ready").unwrap();
    file.lock().unwrap();
    fs::write(directory.join("acquired"), b"acquired").unwrap();
}

fn wait_for_child(child: &mut Child) {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "child failed: {status}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "child did not acquire the released lock"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        loop {
            let path = std::env::temp_dir().join(format!(
                "skein-publication-lock-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create lock test directory: {error}"),
            }
        }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
