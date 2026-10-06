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
use std::num::{NonZeroU64, NonZeroUsize};

/// Host-selected byte and weighted-token admission for one source document.
///
/// This remains independent from encoded-record, input, analyzer,
/// spill, and query budgets. Raising it admits a larger source only when those
/// other limits also admit the operation. It governs writes and old-version
/// reanalysis during mutation validation, and is never inferred from an artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchLexicalSourcePolicy {
    max_document_source_bytes: NonZeroU64,
    max_document_tokens: NonZeroUsize,
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
            max_document_tokens: NonZeroUsize::new(1_000_000).unwrap(),
        })
    }

    pub const fn max_document_source_bytes(self) -> NonZeroU64 {
        self.max_document_source_bytes
    }

    /// Selects the same finite token bound for build, compaction and reanalysis.
    pub const fn with_max_document_tokens(mut self, limit: NonZeroUsize) -> Self {
        self.max_document_tokens = limit;
        self
    }

    pub const fn max_document_tokens(self) -> NonZeroUsize {
        self.max_document_tokens
    }
}

impl Default for SearchLexicalSourcePolicy {
    fn default() -> Self {
        Self {
            max_document_source_bytes: NonZeroU64::new(4 * 1024 * 1024).unwrap(),
            max_document_tokens: NonZeroUsize::new(1_000_000).unwrap(),
        }
    }
}
