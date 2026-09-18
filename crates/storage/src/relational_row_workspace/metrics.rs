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

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

#[derive(Debug, Default)]
pub struct RelationalMonotonicAppendMetrics {
    attempts: AtomicU64,
    hits: AtomicU64,
    fallbacks: AtomicU64,
    proven_absent_primary_keys: AtomicU64,
}

impl RelationalMonotonicAppendMetrics {
    pub fn attempts(&self) -> u64 {
        self.attempts.load(AtomicOrdering::Relaxed)
    }

    pub fn hits(&self) -> u64 {
        self.hits.load(AtomicOrdering::Relaxed)
    }

    pub fn fallbacks(&self) -> u64 {
        self.fallbacks.load(AtomicOrdering::Relaxed)
    }

    pub fn proven_absent_primary_keys(&self) -> u64 {
        self.proven_absent_primary_keys
            .load(AtomicOrdering::Relaxed)
    }

    pub(super) fn record_hit(&self, proven_absent_primary_keys: usize) {
        saturating_add_atomic(&self.attempts, 1);
        saturating_add_atomic(&self.hits, 1);
        saturating_add_atomic(
            &self.proven_absent_primary_keys,
            u64::try_from(proven_absent_primary_keys).unwrap_or(u64::MAX),
        );
    }

    pub(super) fn record_fallback(&self) {
        saturating_add_atomic(&self.attempts, 1);
        saturating_add_atomic(&self.fallbacks, 1);
    }
}

fn saturating_add_atomic(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(
        AtomicOrdering::Relaxed,
        AtomicOrdering::Relaxed,
        |current| Some(current.saturating_add(value)),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_monotonic_append_metrics_saturate_without_wrapping() {
        let metrics = std::sync::Arc::new(RelationalMonotonicAppendMetrics {
            attempts: AtomicU64::new(u64::MAX - 1),
            hits: AtomicU64::new(u64::MAX - 1),
            fallbacks: AtomicU64::new(u64::MAX - 1),
            proven_absent_primary_keys: AtomicU64::new(u64::MAX - 1),
        });
        let shared = std::sync::Arc::clone(&metrics);
        shared.record_hit(2);
        shared.record_hit(usize::MAX);
        shared.record_fallback();
        shared.record_fallback();
        assert_eq!(metrics.attempts(), u64::MAX);
        assert_eq!(metrics.hits(), u64::MAX);
        assert_eq!(metrics.fallbacks(), u64::MAX);
        assert_eq!(metrics.proven_absent_primary_keys(), u64::MAX);
    }
}
