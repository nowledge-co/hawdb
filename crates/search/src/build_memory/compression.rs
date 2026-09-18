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

//! Qualify the native zstd version used by the admitted build workspaces.

use crate::{HawDBError, Result};

const QUALIFIED_ZSTD_VERSION: u32 = 10507;
const _: () = assert!(
    zstd::zstd_safe::zstd_sys::ZSTD_VERSION_NUMBER == QUALIFIED_ZSTD_VERSION,
    "requalify search zstd memory admission before updating the bindings"
);

pub(crate) fn require_qualified_zstd(purpose: &'static str) -> Result<()> {
    // The native build can use pkg-config instead of the bundled source, so
    // the pinned binding version alone does not identify the linked library.
    validate_version(zstd::zstd_safe::version_number(), purpose)
}

fn validate_version(version: u32, purpose: &str) -> Result<()> {
    if version != QUALIFIED_ZSTD_VERSION {
        return Err(HawDBError::Execution(format!(
            "search zstd {purpose} requires qualification for this version"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linked_library_substitution_keeps_both_admission_paths_closed() {
        for purpose in ["decode admission", "workspace admission"] {
            validate_version(QUALIFIED_ZSTD_VERSION, purpose).unwrap();
            for version in [
                0,
                QUALIFIED_ZSTD_VERSION - 1,
                QUALIFIED_ZSTD_VERSION + 1,
                u32::MAX,
            ] {
                assert_eq!(
                    validate_version(version, purpose),
                    Err(HawDBError::Execution(format!(
                        "search zstd {purpose} requires qualification for this version"
                    )))
                );
            }
        }
    }
}
