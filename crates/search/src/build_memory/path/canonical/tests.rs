use super::*;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

fn root() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "skein-canonical-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}

fn task(limit: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(limit as u64, 0))
}

fn peak(input: &Path, output: &Path) -> usize {
    #[cfg(unix)]
    {
        input.as_os_str().as_encoded_bytes().len() + 1 + output.as_os_str().as_encoded_bytes().len()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let _ = input;
        let length = output.as_os_str().encode_wide().count();
        12 * length.max(8) + if length >= 512 { 2 * (length + 1) } else { 0 }
    }
}

fn check_path(path: &Path) {
    let expected = fs::canonicalize(path).unwrap();
    let peak = peak(path, &expected) + 137;
    for limit in [peak - 1, peak] {
        let task = task(limit);
        let memory = BuildMemory::new(&task).unwrap();
        let competing = memory.input.reserve(137).unwrap();
        evidence::take();
        let result = OwnedPath::canonicalize(path, &memory, &task);
        assert_eq!(result.is_ok(), limit == peak);
        assert_eq!(evidence::take(), usize::from(limit == peak));
        if let Ok(owned) = result {
            assert_eq!(owned.as_ref(), expected);
            let bytes = owned.capacity();
            assert_eq!(memory.ledger.snapshot().used_bytes, bytes + 137);
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            let ledger = memory.ledger.clone();
            drop(memory);
            drop(competing);
            assert_eq!(ledger.snapshot().used_bytes, bytes);
            drop(owned);
            assert_eq!(ledger.snapshot().used_bytes, 0);
        } else {
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        }
    }
}

#[test]
fn native_canonical_output_is_admitted_before_copy_and_retains_its_charge() {
    let root = root();
    fs::create_dir(&root).unwrap();
    fs::create_dir(root.join("child")).unwrap();
    check_path(&root);
    check_path(&root.join("child/.."));
    let mut deep = root.clone();
    for _ in 0..10 {
        deep.push("long-canonical-component-with-unicode-\u{130}-and-a-final-segment");
        fs::create_dir(&deep).unwrap();
    }
    check_path(&deep);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancelled_or_invalid_resolution_releases_native_and_owned_state() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let task = task(1024 * 1024);
    let memory = BuildMemory::new(&task).unwrap();
    evidence::cancel_next(task.cancellation().clone());
    evidence::take();
    assert!(OwnedPath::canonicalize(&root, &memory, &task)
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert_eq!(evidence::take(), 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(OwnedPath::canonicalize(&root, &memory, &task).is_err());
    assert_eq!(evidence::take(), 0);
    for path in [root.join("missing"), PathBuf::from("invalid\0path")] {
        assert!(OwnedPath::canonicalize(&path, &memory, &RuntimeTaskContext::default()).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(evidence::take(), 0);
    }
    fs::remove_dir(root).unwrap();
}

#[cfg(unix)]
#[test]
fn canonicalization_preserves_symlinks_and_opaque_native_bytes() {
    use std::os::unix::{ffi::OsStringExt, fs::symlink};
    let root = root();
    fs::create_dir(&root).unwrap();
    let actual = root.join("native-unicode-\u{130}");
    fs::create_dir(&actual).unwrap();
    let alias = root.join("alias");
    symlink(&actual, &alias).unwrap();
    check_path(&alias);
    let directory = root.join(OsString::from_vec(b"data-\xff".to_vec()));
    let memory = BuildMemory::new(&RuntimeTaskContext::default()).unwrap();
    match fs::create_dir(&directory) {
        Ok(()) => {
            let opaque_alias = root.join("opaque-alias");
            symlink(&directory, &opaque_alias).unwrap();
            check_path(&opaque_alias);
            let owned =
                OwnedPath::canonicalize(&opaque_alias, &memory, &RuntimeTaskContext::default())
                    .unwrap();
            assert!(owned.as_os_str().as_encoded_bytes().ends_with(b"data-\xff"));
            drop(owned);
        }
        Err(error) => {
            // APFS rejects ill-formed UTF-8 names; the macOS sandbox may reject
            // them first. Still verify resolution against the native error.
            assert!(
                error.raw_os_error() == Some(libc::EILSEQ)
                    || (cfg!(target_os = "macos") && error.raw_os_error() == Some(libc::EPERM)),
                "unexpected opaque-name creation failure: {error}",
            );
            let expected = native_error(fs::canonicalize(&directory).unwrap_err());
            let actual =
                OwnedPath::canonicalize(&directory, &memory, &RuntimeTaskContext::default())
                    .unwrap_err();
            assert_eq!(actual.to_string(), expected.to_string());
        }
    }
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}
