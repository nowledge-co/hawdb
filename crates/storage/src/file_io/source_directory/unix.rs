// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::ffi::CStr;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::OpenOptionsExt;

#[cfg(target_vendor = "apple")]
const NATIVE_COMPONENT_BUFFER: usize = libc::PATH_MAX as usize;
#[cfg(not(target_vendor = "apple"))]
const NATIVE_COMPONENT_BUFFER: usize = 256;

pub(super) fn open_root(path: &Path) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)
}

fn name_bytes(name: ChildName<'_>) -> [u8; 256] {
    let mut bytes = [0; 256];
    bytes[..name.0.len()].copy_from_slice(name.0.as_bytes());
    bytes
}

pub(super) fn link_child_new(
    source: &std::fs::File,
    source_name: ChildName<'_>,
    _expected: &std::fs::File,
    target: &std::fs::File,
    target_name: ChildName<'_>,
    _context: &FileOpenContext,
) -> io::Result<()> {
    let source_name = name_bytes(source_name);
    let target_name = name_bytes(target_name);
    // SAFETY: both captured roots and terminated single components remain
    // live. Flags zero never follow a source symlink; an occupied destination
    // is rejected. The owner excludes source replacement and validates the
    // linked inode against its original held source before acknowledgement.
    let status = unsafe {
        libc::linkat(
            source.as_raw_fd(),
            source_name.as_ptr().cast(),
            target.as_raw_fd(),
            target_name.as_ptr().cast(),
            0,
        )
    };
    if status == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn create_directory(root: &std::fs::File, name: ChildName<'_>) -> io::Result<()> {
    create_directory_encoded(
        root,
        CStr::from_bytes_until_nul(&name_bytes(name)).expect("terminated artifact name"),
    )
}

fn component_bytes(name: DirectoryComponent<'_>) -> [u8; NATIVE_COMPONENT_BUFFER] {
    use std::os::unix::ffi::OsStrExt;
    let mut bytes = [0; NATIVE_COMPONENT_BUFFER];
    bytes[..name.0.len()].copy_from_slice(name.0.as_bytes());
    bytes
}

pub(super) fn create_directory_component(
    root: &std::fs::File,
    name: DirectoryComponent<'_>,
) -> io::Result<()> {
    create_directory_encoded(
        root,
        CStr::from_bytes_until_nul(&component_bytes(name)).expect("terminated native component"),
    )
}

fn create_directory_encoded(root: &std::fs::File, name: &CStr) -> io::Result<()> {
    // SAFETY: the live parent FD and terminated validated single component
    // outlive mkdirat. Creation is exclusive; an existing symlink is not followed.
    let result = unsafe { libc::mkdirat(root.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn create_control_candidate(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    let name = name_bytes(name);
    // SAFETY: live root, terminated validated component, exclusive regular-file
    // creation with a supplied mode. No existing entry or symlink is followed.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            name.as_ptr().cast(),
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat transferred one fresh owned descriptor.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

pub(super) fn replace_control(
    root: &std::fs::File,
    _file: &std::fs::File,
    source: ChildName<'_>,
    target: ChildName<'_>,
) -> io::Result<()> {
    let source = name_bytes(source);
    let target = name_bytes(target);
    // SAFETY: both canonical components and the same borrowed native directory
    // remain live. The caller excludes mutation/reuse of the private candidate.
    let result = unsafe {
        libc::renameat(
            root.as_raw_fd(),
            source.as_ptr().cast(),
            root.as_raw_fd(),
            target.as_ptr().cast(),
        )
    };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn open_wal_append_child(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    let name = name_bytes(name);
    // SAFETY: held root, validated terminated child, existing regular file.
    // O_APPEND preserves end-of-file writes independently of the replay cursor.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            name.as_ptr().cast(),
            libc::O_RDWR | libc::O_APPEND | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat transfers one fresh owned descriptor.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

pub(super) fn open_mutable_child(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    let name = name_bytes(name);
    // SAFETY: live root and terminated canonical component. No creation,
    // truncation, symlink traversal or path lookup outside this capture.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            name.as_ptr().cast(),
            libc::O_RDWR | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat transferred one fresh owned descriptor.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[cfg(all(
    any(test, feature = "test-support"),
    any(target_os = "linux", target_vendor = "apple")
))]
pub(super) fn reopen_directory(root: &std::fs::File) -> io::Result<std::fs::File> {
    open_directory(root, ChildName("."))
}

pub(super) fn sync_directory(root: &std::fs::File, _context: &FileOpenContext) -> io::Result<()> {
    root.sync_all()
}

#[cfg(any(test, feature = "test-support"))]
pub(super) fn open_recorded_lock(
    root: &std::fs::File,
    name: ChildName<'_>,
    create_new: bool,
) -> io::Result<std::fs::File> {
    let bytes = name_bytes(name);
    let create = if create_new {
        libc::O_CREAT | libc::O_EXCL
    } else {
        0
    };
    // SAFETY: live parent, validated terminated component and ordinary mode.
    // Exclusive creation cannot adopt a foreign entry; existing opens cannot
    // recreate an entry missing from the recorded native namespace.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            bytes.as_ptr().cast(),
            libc::O_RDWR | create | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: one fresh owned descriptor from successful openat.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

pub(super) fn open_child(root: &std::fs::File, name: ChildName<'_>) -> io::Result<std::fs::File> {
    let bytes = name_bytes(name);
    // SAFETY: root is borrowed live; validated ASCII name is NUL-terminated in
    // fixed scratch. Read-only open needs no mode argument. NONBLOCK prevents a
    // corrupt FIFO entry blocking before the caller checks regular-file type.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            bytes.as_ptr().cast(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful openat returned a fresh owned descriptor exactly once.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

pub(super) fn open_directory(
    root: &std::fs::File,
    name: ChildName<'_>,
) -> io::Result<std::fs::File> {
    open_directory_encoded(
        root,
        CStr::from_bytes_until_nul(&name_bytes(name)).expect("terminated artifact name"),
    )
}

pub(super) fn open_directory_component(
    root: &std::fs::File,
    name: DirectoryComponent<'_>,
) -> io::Result<std::fs::File> {
    open_directory_encoded(
        root,
        CStr::from_bytes_until_nul(&component_bytes(name)).expect("terminated native component"),
    )
}

fn open_directory_encoded(root: &std::fs::File, bytes: &CStr) -> io::Result<std::fs::File> {
    // SAFETY: live borrowed directory, validated NUL-terminated component;
    // O_DIRECTORY and O_NOFOLLOW reject files and symlinks before returning.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            bytes.as_ptr().cast(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: exactly one fresh owned descriptor from successful openat.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

pub(super) fn remove_child(
    root: &std::fs::File,
    name: ChildName<'_>,
    _context: &FileOpenContext,
) -> io::Result<()> {
    let bytes = name_bytes(name);
    // SAFETY: borrowed live root and NUL-terminated single-component name; flags
    // zero unlink a file entry and cannot recursively delete a directory.
    let status = unsafe { libc::unlinkat(root.as_raw_fd(), bytes.as_ptr().cast(), 0) };
    if status == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn remove_directory(
    root: &std::fs::File,
    name: ChildName<'_>,
    _context: &FileOpenContext,
) -> io::Result<()> {
    let name = name_bytes(name);
    // SAFETY: live captured parent and validated, terminated child component.
    // AT_REMOVEDIR rejects regular files and nonempty directories.
    let result =
        unsafe { libc::unlinkat(root.as_raw_fd(), name.as_ptr().cast(), libc::AT_REMOVEDIR) };
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(super) fn open_lock_file(
    root: &std::fs::File,
    name: ChildName<'_>,
    mode: SourceDirectoryLockMode,
) -> io::Result<std::fs::File> {
    let bytes = name_bytes(name);
    let access = match mode {
        SourceDirectoryLockMode::ReadExisting => libc::O_RDONLY,
        SourceDirectoryLockMode::ReadWriteCreate => libc::O_RDWR | libc::O_CREAT,
    };
    // SAFETY: validated NUL-terminated component, borrowed live directory,
    // valid open flags and permission mode. No truncation or symlink traversal.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            bytes.as_ptr().cast(),
            access | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            0o600,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: one fresh owned descriptor from successful openat.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

// A new open file description avoids sharing enumeration offsets between
// concurrent/repeated visitors. Duplicating root's FD would share its offset.
#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(super) fn visit_children(
    root: &std::fs::File,
    visitor: &mut dyn FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    visit_names(root, &mut |name| {
        let name = name.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source directory contains a non-UTF-8 name",
            )
        })?;
        visitor(name)
    })
}

#[cfg(any(target_os = "linux", target_vendor = "apple"))]
pub(super) fn visit_names(
    root: &std::fs::File,
    visitor: &mut dyn FnMut(&std::ffi::OsStr) -> io::Result<()>,
) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: borrowed directory and static NUL-terminated relative component.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the fresh descriptor is exclusively transferred on success.
    let directory = unsafe { libc::fdopendir(fd) };
    if directory.is_null() {
        let error = io::Error::last_os_error();
        // SAFETY: fdopendir failed, so ownership was not transferred.
        unsafe {
            libc::close(fd);
        }
        return Err(error);
    }
    struct Cursor(*mut libc::DIR);
    impl Drop for Cursor {
        fn drop(&mut self) {
            // SAFETY: owns one live stream; closedir also closes its FD.
            unsafe {
                libc::closedir(self.0);
            }
        }
    }
    let cursor = Cursor(directory);
    loop {
        // SAFETY: errno is thread-local; this cursor is local to this call.
        let entry = unsafe {
            #[cfg(target_os = "linux")]
            {
                *libc::__errno_location() = 0;
            }
            #[cfg(target_vendor = "apple")]
            {
                *libc::__error() = 0;
            }
            libc::readdir(cursor.0)
        };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(0) {
                Ok(())
            } else {
                Err(error)
            };
        }
        // SAFETY: readdir returns a NUL-terminated name valid until the next
        // call on this stream. It cannot escape the borrowed callback.
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
        if matches!(name.to_bytes(), b"." | b"..") {
            continue;
        }
        visitor(std::ffi::OsStr::from_bytes(name.to_bytes()))?;
    }
}

#[cfg(all(
    any(test, feature = "test-support"),
    any(target_os = "linux", target_vendor = "apple")
))]
pub(super) fn open_recorded_child(
    root: &std::fs::File,
    name: &std::ffi::OsStr,
) -> io::Result<std::fs::File> {
    use std::os::unix::ffi::OsStrExt;
    let bounded = super::native_component_within_input_bound(name);
    let name = name.as_bytes();
    if name.is_empty()
        || !bounded
        || matches!(name, b"." | b"..")
        || name.iter().any(|byte| matches!(byte, 0 | b'/'))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid native child name",
        ));
    }
    let mut encoded = [0_u8; NATIVE_COMPONENT_BUFFER];
    encoded[..name.len()].copy_from_slice(name);
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: live parent, validated terminated component, valid stat output.
    // Do not open devices or FIFOs merely to discover their type.
    let result = unsafe {
        libc::fstatat(
            root.as_raw_fd(),
            encoded.as_ptr().cast(),
            metadata.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatat initialized the entire stat record.
    let kind = unsafe { metadata.assume_init() }.st_mode & libc::S_IFMT;
    if kind != libc::S_IFREG && kind != libc::S_IFDIR {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "captured project contains a nonregular file",
        ));
    }
    // SAFETY: counted live parent, validated single native component in
    // NUL-terminated scratch. NONBLOCK rejects blocking FIFO opens; NOFOLLOW
    // rejects links. The caller validates regular-file/directory metadata.
    let fd = unsafe {
        libc::openat(
            root.as_raw_fd(),
            encoded.as_ptr().cast(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: one fresh owned descriptor from successful openat.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
pub(super) fn visit_children(
    _root: &std::fs::File,
    _visitor: &mut dyn FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "source directory enumeration requires Linux, Apple or Windows",
    ))
}

// Do not open/close another descriptor to a POSIX process-owned lock inode.
pub(super) fn matches_child_file(
    root: &std::fs::File,
    name: ChildName<'_>,
    file: &std::fs::File,
    _context: &FileOpenContext,
) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let name = name_bytes(name);
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: live root, fixed terminated component, writable stat, and no
    // symlink traversal. On success fstatat initializes the whole stat object.
    if unsafe {
        libc::fstatat(
            root.as_raw_fd(),
            name.as_ptr().cast(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatat initialized stat above.
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "source child is not a regular non-symlink file",
        ));
    }
    let held = file.metadata()?;
    // Darwin's dev_t is signed; normalize its sign extension just as MetadataExt
    // does. Other Unix device types are unsigned. Both fit exactly in i128.
    fn device_number(value: impl Into<i128>) -> u64 {
        value.into() as u64
    }
    Ok(held.is_file() && held.dev() == device_number(stat.st_dev) && held.ino() == stat.st_ino)
}
