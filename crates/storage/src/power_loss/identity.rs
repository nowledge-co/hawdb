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

use std::fs::Metadata;
use std::io;

pub(super) use crate::native_file_identity::{of_file, NativeIdentity};

pub(super) fn require_direct_entry(metadata: &Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink() {
        return Err(super::invalid(
            "symlinks are outside the captured fault model",
        ));
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(super::invalid(
                "reparse points are outside the captured fault model",
            ));
        }
    }
    Ok(())
}
