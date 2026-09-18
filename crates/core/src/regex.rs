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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::{HawDBError, Result};

#[derive(Clone)]
pub struct ValidatedRegex {
    source: String,
    compiled: Arc<regex::Regex>,
}

impl ValidatedRegex {
    pub fn new(source: impl Into<String>) -> Result<Self> {
        let source = source.into();
        let compiled = regex::Regex::new(&source)
            .map_err(|error| HawDBError::Semantic(format!("invalid regex pattern: {error}")))?;
        Ok(Self {
            source,
            compiled: Arc::new(compiled),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub fn is_match(&self, value: &str) -> bool {
        self.compiled.is_match(value)
    }
}

impl Debug for ValidatedRegex {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("ValidatedRegex")
            .field(&self.source)
            .finish()
    }
}

impl PartialEq for ValidatedRegex {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for ValidatedRegex {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ValidatedRegex;

    #[test]
    fn clone_reuses_compiled_regex() {
        let pattern = ValidatedRegex::new("^memory-[0-9]+$").expect("valid regex");
        let cloned = pattern.clone();

        assert!(Arc::ptr_eq(&pattern.compiled, &cloned.compiled));
        assert!(cloned.is_match("memory-42"));
        assert!(!cloned.is_match("thread-42"));
    }

    #[test]
    fn equality_and_debug_use_the_source_pattern() {
        let left = ValidatedRegex::new("memory.*").expect("valid regex");
        let right = ValidatedRegex::new("memory.*").expect("valid regex");

        assert_eq!(left, right);
        assert_eq!(left.as_str(), "memory.*");
        assert_eq!(format!("{left:?}"), "ValidatedRegex(\"memory.*\")");
    }

    #[test]
    fn invalid_patterns_fail_during_construction() {
        assert!(ValidatedRegex::new("[").is_err());
    }
}
