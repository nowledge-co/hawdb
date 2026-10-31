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
    compiled: Option<Arc<regex::Regex>>,
}

impl ValidatedRegex {
    pub fn new(source: impl Into<String>) -> Result<Self> {
        let source = source.into();
        let compiled = regex::Regex::new(&source)
            .map_err(|error| HawDBError::Semantic(format!("invalid regex pattern: {error}")))?;
        Ok(Self {
            source,
            compiled: Some(Arc::new(compiled)),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.source
    }

    pub fn is_match(&self, value: &str) -> bool {
        if let Some(compiled) = &self.compiled {
            return compiled.is_match(value);
        }
        // A source-only template is normally prepared once during execution
        // binding. Keep direct use correct without populating retained state.
        regex::Regex::new(&self.source)
            .expect("source was validated with the same regex compiler limits")
            .is_match(value)
    }

    /// Discard opaque execution state before retaining a plan template.
    /// Clones preserve this source-only state until execution binding prepares it.
    #[doc(hidden)]
    pub fn retain_source_only(&mut self) {
        self.compiled = None;
    }

    /// Compile source-only template state into this execution-owned clone.
    #[doc(hidden)]
    pub fn prepare_for_execution(&mut self) -> Result<()> {
        if self.compiled.is_none() {
            *self = Self::new(self.source.clone())?;
        }
        Ok(())
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

impl crate::RetainedHeapBytes for ValidatedRegex {
    fn retained_heap_bytes(&self) -> Option<usize> {
        // Executable regex programs own opaque automata and growing caches.
        // Only immutable source-only templates have a bounded retained charge.
        let Self { source, compiled } = self;
        compiled.is_none().then_some(source.capacity())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ValidatedRegex;

    #[test]
    fn clone_reuses_compiled_regex() {
        let pattern = ValidatedRegex::new("^memory-[0-9]+$").expect("valid regex");
        let cloned = pattern.clone();

        assert!(Arc::ptr_eq(
            pattern.compiled.as_ref().unwrap(),
            cloned.compiled.as_ref().unwrap()
        ));
        assert!(cloned.is_match("memory-42"));
        assert!(!cloned.is_match("thread-42"));
    }

    #[test]
    fn retained_source_and_execution_programs_have_separate_ownership() {
        use crate::RetainedHeapBytes;

        let mut template = ValidatedRegex::new("^(memory|thread)-[0-9]+$").unwrap();
        assert_eq!(template.retained_heap_bytes(), None);
        template.retain_source_only();
        let charge = template.retained_heap_bytes().unwrap();
        assert!(charge >= template.as_str().len());
        let mut execution = template.clone();
        execution.prepare_for_execution().unwrap();
        assert!(execution.is_match("memory-42"));
        assert!(!execution.is_match("source-42"));
        assert_eq!(execution.retained_heap_bytes(), None);
        assert!(template.compiled.is_none());
        assert_eq!(template.retained_heap_bytes(), Some(charge));
        assert!(template.is_match("thread-1"));
        assert!(
            template.compiled.is_none(),
            "direct matching must not grow the cache template"
        );
        let mut another_execution = template.clone();
        another_execution.prepare_for_execution().unwrap();
        assert!(!Arc::ptr_eq(
            execution.compiled.as_ref().unwrap(),
            another_execution.compiled.as_ref().unwrap()
        ));
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
