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

//! Pinned Rust 1.97.1 filesystem path conversion scratch, excluding error reports.

use crate::build_memory::{checked_add as add, checked_mul as mul};
use crate::Result;
use std::path::Path;

pub(crate) fn with_scratch<T>(
    progress: Option<&super::ReservedMemory>,
    path: &Path,
    work: impl FnOnce() -> Result<T>,
) -> Result<T> {
    match progress {
        Some(progress) => progress.with_scratch(bytes(path)?, work),
        None => work(),
    }
}

pub(crate) fn bytes(path: &Path) -> Result<usize> {
    let length = path.as_os_str().as_encoded_bytes().len();
    bytes_for_length(length, path.is_absolute())
}

pub(crate) fn child_bytes(parent: &Path, name_bytes: usize) -> Result<usize> {
    let length = add(
        parent.as_os_str().as_encoded_bytes().len(),
        add(name_bytes, 1)?,
    )?;
    bytes_for_length(length, parent.is_absolute())
}

fn bytes_for_length(length: usize, absolute: bool) -> Result<usize> {
    #[cfg(windows)]
    {
        // UTF-16 input, GetFullPathNameW's geometrically grown buffer, and the
        // reconstructed verbatim path can coexist. Relative paths can prepend
        // a current directory up to the Windows native path representation bound.
        let directory = if absolute { 0 } else { 32_768 };
        mul(add(add(length, directory)?, 9)?, 8)
    }
    #[cfg(not(windows))]
    {
        let _ = absolute;
        let threshold = if cfg!(target_os = "espidf") { 32 } else { 384 };
        if length < threshold {
            Ok(0)
        } else {
            // CString conversion can grow the copied bytes for the terminator
            // and shrink to boxed storage; admit old/replacement coexistence.
            mul(add(length, 1)?, 3)
        }
    }
}
