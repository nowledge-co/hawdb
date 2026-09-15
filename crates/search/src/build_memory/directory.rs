//! Pinned native workspace bounds for generated-file directory operations.

use super::{checked_add as add, checked_mul as mul, path, reserved::native_path};
use crate::Result;
use std::fs;
use std::mem::size_of;
use std::path::{Component, Path};

// WIN32_FIND_DATAW has a fixed 260-WCHAR filename; Unix directory records have
// a u16 byte extent. Both the entry and file_name() may retain an owned copy.
#[cfg(windows)]
pub(crate) const ENTRY_NAME_BYTES: usize = 3 * 260;
#[cfg(not(windows))]
pub(crate) const ENTRY_NAME_BYTES: usize = u16::MAX as usize + 1;

pub(crate) fn retained_scan_bytes(root: &Path) -> Result<usize> {
    let retained = add(
        root.as_os_str().as_encoded_bytes().len(),
        size_of::<fs::ReadDir>() + 128,
    )?;
    // glibc caps its DIR buffer at one MiB. Windows uses fixed Find data.
    let enumeration = if cfg!(windows) { 0 } else { 1024 * 1024 };
    add(retained, enumeration)
}

pub(crate) fn scan_startup_bytes(root: &Path) -> Result<usize> {
    // Windows read_dir constructs its wildcard path before native conversion.
    add(
        path::join_bytes(root.as_os_str().as_encoded_bytes().len(), 1, true)?,
        native_path::child_bytes(root, 1)?,
    )
}

pub(crate) fn scan_bytes(root: &Path) -> Result<usize> {
    let root_bytes = root.as_os_str().as_encoded_bytes().len();
    let verbatim = matches!(root.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim());
    let names = mul(2, ENTRY_NAME_BYTES)?;
    let joined = path::join_bytes(root_bytes, ENTRY_NAME_BYTES, verbatim)?;
    let native = native_path::child_bytes(root, ENTRY_NAME_BYTES)?;
    add(
        retained_scan_bytes(root)?,
        add(add(names, joined)?, add(native, scan_startup_bytes(root)?)?)?,
    )
}

pub(crate) fn stage_removal_bytes(root: &Path) -> Result<usize> {
    // Every producer writes regular files directly into the owned stage. Keep
    // std's handle-relative, symlink-safe removal instead of a path walker.
    // Unix retains one DIR and entry. Windows retains a 1-KiB DirBuff and its
    // one-directory handle vector. No generated child directory can add a level.
    let traversal = if cfg!(windows) {
        1024 + 4 * size_of::<fs::File>()
    } else {
        1024 * 1024 + ENTRY_NAME_BYTES + 256
    };
    add(native_path::bytes(root)?, traversal)
}
