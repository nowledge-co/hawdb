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

mod template;

pub use template::{
    bind_physical_plan_parameters, parameterize_logical_plan, parameterize_value_list,
    ParameterizedLogicalPlan, PlanParameterCacheKey,
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
}

impl PlanCacheBypassReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MutationPlanning => "mutation_planning",
            Self::OptimizerDirective => "optimizer_directive",
            Self::StatementNotCacheable => "statement_not_cacheable",
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
}

#[derive(Debug, Clone)]
pub struct LfuCache<K, V> {
    max_entries: Option<usize>,
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
        Self {
            max_entries,
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
        if self.max_entries == Some(0) {
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

    pub fn insert(&mut self, key: K, value: V) {
        let Some(max_entries) = self.max_entries else {
            self.insert_entry(key, value);
            self.admissions = self.admissions.saturating_add(1);
            return;
        };
        if max_entries == 0 {
            return;
        }
        let new_key = !self.entries.contains_key(&key);
        if new_key && self.entries.len() >= max_entries {
            self.memory_pressure_events = self.memory_pressure_events.saturating_add(1);
        }
        self.insert_entry(key, value);
        self.admissions = self.admissions.saturating_add(1);
        while self.entries.len() > max_entries {
            let Some(evicted) = self.lfu_victim_key() else {
                break;
            };
            if self.entries.remove(&evicted).is_some() {
                self.evictions += 1;
            }
        }
    }

    fn insert_entry(&mut self, key: K, value: V) {
        self.access_tick = self.access_tick.saturating_add(1);
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.value = value;
            entry.frequency = entry.frequency.saturating_add(1);
            entry.last_access_tick = self.access_tick;
            return;
        }
        self.entries.insert(
            key,
            LfuCacheEntry {
                value,
                frequency: 1,
                last_access_tick: self.access_tick,
            },
        );
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
