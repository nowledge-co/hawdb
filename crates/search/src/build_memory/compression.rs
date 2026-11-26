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

//! Admit workspace using the selected compression implementation's bound.

use crate::Result;

#[cfg(test)]
pub(crate) const COMPRESSION_WORKSPACE_BYTES: usize = 8 * 1024 * 1024 + 32 * 1024;

pub(crate) fn workspace_bytes() -> Result<usize> {
    super::checked_add(
        hawdb_storage::compression::encoder_workspace_bytes(crate::SEARCH_COMPRESSION_LEVEL)?,
        32 * 1024,
    )
}
