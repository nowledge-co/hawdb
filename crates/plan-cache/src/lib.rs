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

use std::collections::HashMap;
use std::hash::Hash;

mod binding_cache;
mod schema;
mod template;

#[doc(hidden)]
pub use binding_cache::{
    ParameterBindingAdmission, ParameterBindingCache, ParameterBindingCacheConfig,
};
#[doc(hidden)]
pub use schema::OptimizerSchemaKey;

pub use template::{
    bind_physical_plan_parameters, parameter_binding_shape_fingerprint, parameter_scalar_types,
    parameterize_logical_plan, parameterize_value_list, resolve_plan_template_value,
    ParameterBindingTemplate, ParameterizedLogicalPlan, PlanParameterCacheKey,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheLookup {
    Hit,
    Miss,
    Bypass(PlanCacheBypassReason),
}

impl PlanCacheLookup {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Bypass(_) => "bypass",
        }
    }

    pub const fn bypass_reason(self) -> Option<PlanCacheBypassReason> {
        match self {
            Self::Bypass(reason) => Some(reason),
            Self::Hit | Self::Miss => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanCacheBypassReason {
    MutationPlanning,
    OptimizerDirective,
    StatementNotCacheable,
    ParameterizedCaseTemplate,
}

impl PlanCacheBypassReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::OptimizerDirective => "optimizer_directive",
            Self::StatementNotCacheable => "statement_not_cacheable",
            Self::ParameterizedCaseTemplate => "parameterized_case_template",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCacheStats {
    pub max_entries: Option<usize>,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub disabled_misses: u64,
    pub bypasses: u64,
    pub evictions: u64,
    pub memory_pressure_events: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct LfuCacheEntry<V> {
    value: V,
    frequency: u64,
    last_access_tick: u64,
    heap_charge: usize,
}

#[derive(Debug, Clone)]
pub struct LfuCache<K, V> {
    max_entries: Option<usize>,
    max_bytes: Option<usize>,
    entry_heap_charge: usize,
    table_capacity_high_water: usize,
    entries: HashMap<K, LfuCacheEntry<V>>,
    access_tick: u64,
    hits: u64,
    misses: u64,
    admissions: u64,
    disabled_misses: u64,
    bypasses: u64,
    evictions: u64,
    memory_pressure_events: u64,
}

impl<K, V> LfuCache<K, V>
where
    K: Eq + Hash + Ord + Clone,
    V: Clone,
{
    pub fn new(max_entries: Option<usize>) -> Self {
        Self::with_byte_limit(max_entries, None)
    }

    /// A byte-limited cache requires `insert_weighted` with conservative heap
    /// charges for both key and value. Table storage is charged separately.
    pub fn with_byte_limit(max_entries: Option<usize>, max_bytes: Option<usize>) -> Self {
        Self {
            max_entries,
            max_bytes,
            entry_heap_charge: 0,
            table_capacity_high_water: 0,
            entries: HashMap::new(),
            access_tick: 0,
            hits: 0,
            misses: 0,
            admissions: 0,
            disabled_misses: 0,
            bypasses: 0,
            evictions: 0,
            memory_pressure_events: 0,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<V> {
        if self.max_entries == Some(0) || self.max_bytes == Some(0) {
            self.misses += 1;
            self.disabled_misses += 1;
            return None;
        }
        self.access_tick = self.access_tick.saturating_add(1);
        let Some(entry) = self.entries.get_mut(key) else {
            self.misses += 1;
            return None;
        };
        self.hits += 1;
        entry.frequency = entry.frequency.saturating_add(1);
        entry.last_access_tick = self.access_tick;
        Some(entry.value.clone())
    }

    pub fn record_bypass(&mut self) {
        self.bypasses = self.bypasses.saturating_add(1);
    }

    /// Legacy count-only admission. Byte-limited caches refuse this path so
    /// callers cannot accidentally retain an uncharged key/value payload.
    pub fn insert(&mut self, key: K, value: V) {
        if self.max_bytes.is_none() {
            self.insert_weighted(key, value, 0);
        }
    }

    /// Charge owned heap retained through the key/value, excluding their inline
    /// bucket layout. Rejected admissions leave an existing entry unchanged.
    /// In-flight cloned values are owned by callers, outside this cache bound.
    pub fn insert_weighted(&mut self, key: K, value: V, heap_charge: usize) {
        if self.max_entries == Some(0)
            || self
                .max_bytes
                .is_some_and(|limit| limit == 0 || heap_charge > limit)
        {
            return;
        }
        let previous = self.entries.get(&key).map_or(0, |entry| entry.heap_charge);
        let Some(total) = self
            .entry_heap_charge
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(heap_charge))
        else {
            return;
        };
        self.entry_heap_charge = total;
        self.access_tick = self.access_tick.saturating_add(1);
        // Replace the key too: an equal old key may retain more spare heap
        // capacity than the incoming key whose charge the caller supplied.
        let frequency = self
            .entries
            .remove(&key)
            .map_or(1, |entry| entry.frequency.saturating_add(1));
        self.entries.insert(
            key,
            LfuCacheEntry {
                value,
                heap_charge,
                frequency,
                last_access_tick: self.access_tick,
            },
        );
        self.table_capacity_high_water =
            self.table_capacity_high_water.max(self.entries.capacity());
        self.admissions = self.admissions.saturating_add(1);
        if self.over_entry_limit() || self.over_byte_limit() {
            self.memory_pressure_events = self.memory_pressure_events.saturating_add(1);
        }
        while self.over_entry_limit() || self.over_byte_limit() {
            let Some(victim) = self.lfu_victim_key() else {
                // An empty map must not retain an over-budget bucket buffer.
                self.entries = HashMap::new();
                self.table_capacity_high_water = 0;
                break;
            };
            let removed = self
                .entries
                .remove(&victim)
                .expect("selected victim exists");
            self.entry_heap_charge -= removed.heap_charge;
            self.evictions = self.evictions.saturating_add(1);
            if self.entries.is_empty() {
                self.entries = HashMap::new();
                self.table_capacity_high_water = 0;
            } else if self.over_byte_limit() {
                self.entries.shrink_to_fit();
            }
        }
    }

    pub fn max_bytes(&self) -> Option<usize> {
        self.max_bytes
    }

    /// Conservative requested-heap charge, including spare hash-table capacity.
    /// The pinned hashbrown table uses at most twice its original advertised
    /// capacity in buckets plus control bytes/alignment. Preserve the high-water
    /// capacity: tombstones can lower `HashMap::capacity()` without freeing its
    /// allocation. This excludes allocator rounding,
    /// the fixed cache object and heap owned by callers after cloning a value.
    pub fn charged_bytes(&self) -> Option<usize> {
        let capacity = self.table_capacity_high_water;
        if capacity == 0 {
            return Some(self.entry_heap_charge);
        }
        let buckets = capacity.checked_mul(2)?;
        let bucket = std::mem::size_of::<(K, LfuCacheEntry<V>)>().checked_add(1)?;
        let table = buckets
            .checked_mul(bucket)?
            .checked_add(32)?
            .checked_add(std::mem::align_of::<(K, LfuCacheEntry<V>)>())?;
        self.entry_heap_charge.checked_add(table)
    }

    fn over_entry_limit(&self) -> bool {
        self.max_entries
            .is_some_and(|limit| self.entries.len() > limit)
    }

    fn over_byte_limit(&self) -> bool {
        self.max_bytes
            .is_some_and(|limit| self.charged_bytes().is_none_or(|bytes| bytes > limit))
    }

    fn lfu_victim_key(&self) -> Option<K> {
        self.entries
            .iter()
            .min_by(|(left_key, left), (right_key, right)| {
                (left.frequency, left.last_access_tick, *left_key).cmp(&(
                    right.frequency,
                    right.last_access_tick,
                    *right_key,
                ))
            })
            .map(|(key, _)| key.clone())
    }

    pub fn stats(&self) -> PlanCacheStats {
        PlanCacheStats {
            max_entries: self.max_entries,
            entries: self.entries.len(),
            hits: self.hits,
            misses: self.misses,
            admissions: self.admissions,
            disabled_misses: self.disabled_misses,
            bypasses: self.bypasses,
            evictions: self.evictions,
            memory_pressure_events: self.memory_pressure_events,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LfuCache, PlanCacheBypassReason, PlanCacheLookup};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[test]
    fn weighted_replacement_retains_the_charged_key_capacity() {
        let mut cache = LfuCache::with_byte_limit(Some(8), Some(8192));
        let mut key = String::with_capacity(4096);
        key.push('a');
        cache.insert_weighted(key, (), 4096);
        let smaller_key = "a".to_owned();
        let charge = smaller_key.capacity();
        cache.insert_weighted(smaller_key, (), charge);
        assert_eq!(cache.entries.keys().next().unwrap().capacity(), charge);
        assert_eq!(cache.entry_heap_charge, charge);
    }

    #[test]
    fn byte_limited_lfu_bounds_a_burst_and_its_table_capacity() {
        let mut cache = LfuCache::with_byte_limit(Some(128), Some(4096));
        for key in 0..256 {
            cache.insert_weighted(key, vec![key as u8; 1024], 1024);
            assert!(cache.charged_bytes().unwrap() <= 4096);
            for (key, entry) in &cache.entries {
                assert_eq!(entry.value[0], *key as u8);
            }
        }
        assert!(cache.stats().evictions > 0);
        assert!(cache.stats().entries <= 3);
        assert!(cache.clone().charged_bytes().unwrap() <= 4096);
    }

    #[test]
    fn byte_limited_lfu_refuses_uncharged_admission() {
        let mut cache = LfuCache::with_byte_limit(Some(8), Some(4096));
        cache.insert(1, vec![0; 8192]);
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.charged_bytes(), Some(0));
        cache.insert_weighted(1, vec![1; 16], 16);
        assert_eq!(cache.get(&1), Some(vec![1; 16]));
    }

    #[test]
    fn byte_limited_lfu_replacement_and_oversize_admission_are_atomic() {
        let mut cache = LfuCache::with_byte_limit(Some(8), Some(4096));
        cache.insert_weighted(1, vec![1; 1024], 1024);
        cache.insert_weighted(1, vec![2; 2048], 2048);
        assert_eq!(cache.entry_heap_charge, 2048);
        let before = cache.charged_bytes();
        cache.insert_weighted(1, vec![3; 8192], 8192);
        assert_eq!(cache.charged_bytes(), before);
        assert_eq!(cache.get(&1), Some(vec![2; 2048]));
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn zero_and_overflowing_byte_budgets_cannot_leave_retained_entries() {
        let mut disabled = LfuCache::with_byte_limit(None, Some(0));
        disabled.insert_weighted(1, 1, 0);
        assert_eq!(disabled.get(&1), None);
        assert_eq!(disabled.stats().disabled_misses, 1);
        assert_eq!(disabled.charged_bytes(), Some(0));
        let mut overflow = LfuCache::with_byte_limit(None, Some(usize::MAX));
        overflow.insert_weighted(1, 1, usize::MAX);
        assert_eq!(overflow.stats().entries, 0);
        assert_eq!(overflow.charged_bytes(), Some(0));
    }

    #[test]
    fn lfu_cache_evicts_least_frequently_used_entry() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("hot", 1);
        cache.insert("cold", 2);

        assert_eq!(cache.get(&"hot"), Some(1));
        cache.insert("new", 3);

        assert_eq!(cache.get(&"hot"), Some(1));
        assert_eq!(cache.get(&"new"), Some(3));
        assert_eq!(cache.get(&"cold"), None);
        assert_eq!(cache.stats().admissions, 3);
        assert_eq!(cache.stats().evictions, 1);
        assert_eq!(cache.stats().memory_pressure_events, 1);
    }

    #[test]
    fn lfu_cache_uses_oldest_access_as_tie_breaker() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("older", 1);
        cache.insert("newer", 2);
        cache.insert("third", 3);

        assert_eq!(cache.get(&"older"), None);
        assert_eq!(cache.get(&"newer"), Some(2));
        assert_eq!(cache.get(&"third"), Some(3));
    }

    #[test]
    fn zero_capacity_cache_records_misses_without_entries() {
        let mut cache = LfuCache::new(Some(0));
        cache.insert("ignored", 1);

        assert_eq!(cache.get(&"ignored"), None);
        assert_eq!(cache.stats().entries, 0);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().disabled_misses, 1);
        assert_eq!(cache.stats().admissions, 0);
        assert_eq!(cache.stats().memory_pressure_events, 0);
    }

    #[test]
    fn lookup_reports_cacheability_and_bypass_reason() {
        let bypass = PlanCacheLookup::Bypass(PlanCacheBypassReason::OptimizerDirective);

        assert_eq!(PlanCacheLookup::Hit.as_str(), "hit");
        assert_eq!(bypass.as_str(), "bypass");
        assert_eq!(
            bypass.bypass_reason(),
            Some(PlanCacheBypassReason::OptimizerDirective)
        );
        assert_eq!(
            PlanCacheBypassReason::StatementNotCacheable.as_str(),
            "statement_not_cacheable"
        );
    }

    #[test]
    fn hash_lookup_updates_lfu_accounting_once() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("query-a", 1);
        cache.insert("query-b", 2);

        assert_eq!(cache.get(&"query-b"), Some(2));
        assert_eq!(cache.get(&"query-c"), None);
        assert_eq!(cache.stats().hits, 1);
        assert_eq!(cache.stats().misses, 1);
    }

    #[test]
    fn duplicate_hash_keys_update_one_entry() {
        let mut cache = LfuCache::new(Some(2));
        cache.insert("query", 1);
        cache.insert("query", 2);

        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.get(&"query"), Some(2));
        assert_eq!(cache.stats().entries, 1);
    }

    #[test]
    fn cache_records_bypassed_plans_separately_from_misses() {
        let mut cache: LfuCache<&str, i32> = LfuCache::new(Some(2));

        cache.record_bypass();

        assert_eq!(cache.stats().bypasses, 1);
        assert_eq!(cache.stats().misses, 0);
        assert_eq!(cache.stats().hits, 0);
        assert_eq!(cache.stats().admissions, 0);
    }

    #[test]
    fn unbounded_cache_records_admissions_without_pressure() {
        let mut cache = LfuCache::new(None);

        cache.insert("first", 1);
        cache.insert("second", 2);

        assert_eq!(cache.stats().entries, 2);
        assert_eq!(cache.stats().admissions, 2);
        assert_eq!(cache.stats().memory_pressure_events, 0);
        assert_eq!(cache.stats().evictions, 0);
    }

    #[test]
    fn cache_remains_bounded_under_mutex_protected_concurrent_access() {
        let cache = Arc::new(Mutex::new(LfuCache::new(Some(8))));
        let start = Arc::new(Barrier::new(4));

        let handles = (0..4)
            .map(|worker| {
                let cache = Arc::clone(&cache);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    for offset in 0..64 {
                        let key = format!("key-{}", (worker + offset) % 16);
                        let mut cache = cache.lock().unwrap();
                        cache.insert(key.clone(), worker * 100 + offset);
                        let _ = cache.get(&key);
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = cache.lock().unwrap().stats();
        assert_eq!(stats.entries, 8);
        assert_eq!(stats.admissions, 256);
        assert!(stats.hits > 0);
        assert!(stats.evictions > 0);
        assert!(stats.memory_pressure_events > 0);
    }
}

#[cfg(all(test, feature = "loom-tests"))]
mod loom_tests {
    use super::LfuCache;
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    #[test]
    fn lfu_cache_preserves_bounds_under_modeled_concurrent_access() {
        loom::model(|| {
            let cache = Arc::new(Mutex::new(LfuCache::new(Some(2))));

            let writer = {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    let mut cache = cache.lock().unwrap();
                    cache.insert("first", 1);
                    cache.insert("second", 2);
                })
            };

            let reader_writer = {
                let cache = Arc::clone(&cache);
                thread::spawn(move || {
                    let mut cache = cache.lock().unwrap();
                    let _ = cache.get(&"first");
                    cache.insert("third", 3);
                })
            };

            writer.join().unwrap();
            reader_writer.join().unwrap();

            let stats = cache.lock().unwrap().stats();
            assert!(stats.entries <= 2);
            assert!(stats.admissions <= 3);
            assert!(stats.evictions <= 1);
            assert_eq!(stats.max_entries, Some(2));
        });
    }
}
