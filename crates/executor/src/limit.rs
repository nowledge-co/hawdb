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

use hawdb_core::{HawDBError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionLimit {
    pub output_rows: Option<usize>,
}

impl ExecutionLimit {
    pub const fn unlimited() -> Self {
        Self { output_rows: None }
    }

    pub fn from_user_max_rows(max_rows: Option<usize>) -> Result<Self> {
        let Some(max_rows) = max_rows else {
            return Ok(Self::unlimited());
        };
        let output_rows = max_rows.checked_add(1).ok_or_else(|| {
            HawDBError::Execution("read query row limit is too large".to_string())
        })?;
        Ok(Self {
            output_rows: Some(output_rows),
        })
    }

    pub fn child_for_limit(self, offset: usize, limit: Option<usize>) -> Self {
        let output_rows = match (self.output_rows, limit) {
            (Some(cap), Some(limit)) => Some(offset.saturating_add(cap.min(limit))),
            (Some(cap), None) => Some(offset.saturating_add(cap)),
            (None, Some(limit)) => Some(offset.saturating_add(limit)),
            (None, None) => None,
        };
        Self { output_rows }
    }

    pub fn is_reached(self, len: usize) -> bool {
        self.output_rows.is_some_and(|cap| len >= cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_limit_keeps_one_extra_detection_row() {
        let limit = ExecutionLimit::from_user_max_rows(Some(10)).unwrap();
        assert_eq!(limit.output_rows, Some(11));
        assert!(limit.is_reached(11));
        assert!(!limit.is_reached(10));
    }

    #[test]
    fn nested_limit_keeps_parent_detection_cap() {
        let limit = ExecutionLimit {
            output_rows: Some(11),
        };
        assert_eq!(
            limit.child_for_limit(5, Some(3)),
            ExecutionLimit {
                output_rows: Some(8)
            }
        );
    }
}
