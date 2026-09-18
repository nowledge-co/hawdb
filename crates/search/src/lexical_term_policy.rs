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

/// Host-selected admission for the UTF-8 bytes of one analyzed lexical term.
///
/// This does not change tokenization or the separate document, block, build,
/// spill, and query budgets. Increasing it permits longer terms only when those
/// budgets also admit the operation. It is never inferred from an index file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchLexicalTermPolicy {
    max_term_bytes: NonZeroU64,
}

impl SearchLexicalTermPolicy {
    /// Creates a finite limit representable by the v1 term-length encoding.
    pub fn new(max_term_bytes: NonZeroU64) -> Result<Self> {
        if max_term_bytes.get() > u64::from(u32::MAX) {
            return Err(HawDBError::Storage(
                "lexical term policy exceeds the v1 u32 length encoding".into(),
            ));
        }
        Ok(Self { max_term_bytes })
    }

    pub const fn max_term_bytes(self) -> NonZeroU64 {
        self.max_term_bytes
    }
}

impl Default for SearchLexicalTermPolicy {
    fn default() -> Self {
        Self {
            max_term_bytes: NonZeroU64::new(4096).unwrap(),
        }
    }
}
