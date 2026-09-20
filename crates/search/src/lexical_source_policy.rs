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

use crate::error::{HawDBError, Result};
use std::num::NonZeroU64;

/// Host-selected admission for the UTF-8 bytes of one source document.
///
/// This remains independent from encoded-record, input, analyzer, token,
/// spill, and query budgets. Raising it admits a larger source only when those
/// other limits also admit the operation. It is a host-side write policy and
/// is never inferred from an index artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchLexicalSourcePolicy {
    max_document_source_bytes: NonZeroU64,
}

impl SearchLexicalSourcePolicy {
    /// Creates a source limit that can be represented by local allocations.
    pub fn new(max_document_source_bytes: NonZeroU64) -> Result<Self> {
        if max_document_source_bytes.get() > isize::MAX as u64 {
            return Err(HawDBError::Storage(
                "lexical source policy exceeds the platform allocation limit".into(),
            ));
        }
        Ok(Self {
            max_document_source_bytes,
        })
    }

    pub const fn max_document_source_bytes(self) -> NonZeroU64 {
        self.max_document_source_bytes
    }
}

impl Default for SearchLexicalSourcePolicy {
    fn default() -> Self {
        Self {
            max_document_source_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        }
    }
}
