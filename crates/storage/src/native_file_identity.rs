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

//! Shared native identity for held files and power-loss capture.

use std::fs::File;
use std::io;

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NativeIdentity {
    volume: u64,
    file: [u8; 16],
}

#[cfg(unix)]
pub(crate) fn of_metadata(metadata: &std::fs::Metadata) -> NativeIdentity {
    use std::os::unix::fs::MetadataExt;
    NativeIdentity {
        volume: metadata.dev(),
        file: u128::from(metadata.ino()).to_le_bytes(),
    }
}

#[cfg(unix)]
pub(crate) fn of_file(file: &File) -> io::Result<NativeIdentity> {
    Ok(of_metadata(&file.metadata()?))
}

#[cfg(windows)]
pub(crate) fn of_file(file: &File) -> io::Result<NativeIdentity> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetFileInformationByHandleEx"]
        fn get_file_information_by_handle_ex(
            file: *mut c_void,
            class: i32,
            information: *mut c_void,
            size: u32,
        ) -> i32;
    }
    // FILE_ID_INFO combines a volume serial with the full 128-bit file ID.
    // A truncated BY_HANDLE_FILE_INFORMATION ID is not unique on ReFS.
    const FILE_ID_INFO: i32 = 18;
    let mut identity = NativeIdentity::default();
    // SAFETY: the borrowed file keeps the handle alive; repr(C) exactly matches
    // FILE_ID_INFO, and the output pointer and size describe its writable buffer.
    let success = unsafe {
        get_file_information_by_handle_ex(
            file.as_raw_handle(),
            FILE_ID_INFO,
            std::ptr::from_mut(&mut identity).cast(),
            std::mem::size_of::<NativeIdentity>() as u32,
        )
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(identity)
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn of_file(_file: &File) -> io::Result<NativeIdentity> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "native identity requires Unix or Windows",
    ))
}

#[cfg(all(test, any(unix, windows)))]
mod tests {
    use crate::file_io::{File, OpenOptions};
    use std::fs;
    use std::path::PathBuf;

    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "hawdb-native-identity-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            fs::create_dir(&root).unwrap();
            Self(root)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn path_identity_distinguishes_equal_bytes_and_tracks_held_file_after_rename() {
        let fixture = Fixture::new();
        let original = fixture.0.join("original");
        let alias = fixture.0.join("alias");
        let copied = fixture.0.join("copied");
        fs::write(&original, b"equal bytes").unwrap();
        fs::hard_link(&original, &alias).unwrap();
        fs::copy(&original, &copied).unwrap();
        let file = File::open(&original).unwrap();
        assert!(file.matches_native_path(&original).unwrap());
        assert!(file.matches_native_path(&alias).unwrap());
        assert!(!file.matches_native_path(&copied).unwrap());
        let moved = fixture.0.join("moved");
        fs::rename(&original, &moved).unwrap();
        fs::write(&original, b"equal bytes").unwrap();
        assert!(file.matches_native_path(&moved).unwrap());
        assert!(!file.matches_native_path(&original).unwrap());
        fs::remove_file(&original).unwrap();
        assert_eq!(
            file.matches_native_path(&original).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn identity_checks_preserve_native_lock_exclusion() {
        const CHILD_PATH: &str = "HAWDB_TEST_NATIVE_IDENTITY_LOCK_PATH";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            assert!(matches!(
                file.try_lock(),
                Err(std::fs::TryLockError::WouldBlock)
            ));
            return;
        }
        let fixture = Fixture::new();
        let path = fixture.0.join("locked");
        fs::write(&path, b"locked bytes").unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.try_lock().unwrap();
        for _ in 0..3 {
            assert!(file.matches_native_path(&path).unwrap());
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_file_identity::tests::identity_checks_preserve_native_lock_exclusion",
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("1 passed; 0 failed"),
            "child selected no test: {stdout}"
        );
    }
    #[cfg(unix)]
    fn try_posix_write_lock(file: &File) -> std::io::Result<()> {
        use std::os::fd::AsRawFd;
        file.with_native(|raw| {
            // SAFETY: flock is a C integer-only input structure; all-zero fields
            // are valid, and the explicit type/whence define a whole-file lock.
            let mut lock: libc::flock = unsafe { std::mem::zeroed() };
            lock.l_type = libc::F_WRLCK as _;
            lock.l_whence = libc::SEEK_SET as _;
            // SAFETY: with_native retains the live descriptor and flock input;
            // F_SETLK reads it synchronously and does not retain its pointer.
            let status = unsafe { libc::fcntl(raw.as_raw_fd(), libc::F_SETLK, &lock) };
            if status == -1 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        })
    }

    #[cfg(unix)]
    #[test]
    fn identity_checks_preserve_process_owned_posix_locks() {
        const CHILD_PATH: &str = "HAWDB_TEST_POSIX_IDENTITY_LOCK_PATH";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            let error = try_posix_write_lock(&file).unwrap_err();
            assert!(
                error
                    .raw_os_error()
                    .is_some_and(|code| code == libc::EACCES || code == libc::EAGAIN),
                "{error}"
            );
            return;
        }
        let fixture = Fixture::new();
        let path = fixture.0.join("posix-locked");
        fs::write(&path, b"locked bytes").unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        try_posix_write_lock(&file).unwrap();
        for _ in 0..3 {
            assert!(file.matches_native_path(&path).unwrap());
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_file_identity::tests::identity_checks_preserve_process_owned_posix_locks",
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("1 passed; 0 failed"),
            "child selected no test: {stdout}"
        );
    }
}
