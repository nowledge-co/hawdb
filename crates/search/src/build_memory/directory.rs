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

//! Pinned native workspace bounds for generated-file directory operations.

use super::{checked_add as add, checked_mul as mul, path, reserved::native_path};
use crate::Result;
use hawdb_storage::file_io as fs;
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

// Owned stages are flat and usually small. Four paths keep the cleanup charge
// near its existing scan envelope without changing general GC traversal batches.
pub(crate) const STAGE_REMOVAL_BATCH_ENTRIES: usize = 4;

pub(crate) fn stage_removal_bytes(root: &Path) -> Result<usize> {
    // The admitted walker closes its iterator before deleting each batch. Stage
    // producers create files directly under the owned root, so only one level
    // of pending directory paths can exist. Retain all child paths in the batch,
    // plus the iterator/entry/native-path scratch and overlapping Vec growth.
    let root_bytes = root.as_os_str().as_encoded_bytes().len();
    let verbatim = matches!(root.components().next(), Some(Component::Prefix(prefix)) if prefix.kind().is_verbatim());
    // scan_bytes covers one transient normalization workspace. Each completed
    // child contributes only its retained capacity, shared with join_bytes.
    let child_capacity = path::retained_join_bytes(root_bytes, ENTRY_NAME_BYTES, verbatim)?;
    let paths = mul(STAGE_REMOVAL_BATCH_ENTRIES, child_capacity)?;
    let tuples = mul(
        2 * STAGE_REMOVAL_BATCH_ENTRIES,
        size_of::<(std::path::PathBuf, fs::FileType)>(),
    )?;
    let pending = add(root_bytes, 4 * size_of::<std::path::PathBuf>())?;
    add(scan_bytes(root)?, add(paths, add(tuples, pending)?)?)
}
