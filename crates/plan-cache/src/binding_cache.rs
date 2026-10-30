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

//! Bounded parameter-template retention, independent of database configuration
//! and host locking. Lookups return shared templates so expensive validation,
//! planning and admission preparation can run outside a caller's lock.

use crate::{
    parameter_binding_shape_fingerprint, LfuCache, ParameterBindingTemplate,
    ParameterizedLogicalPlan, PlanCacheStats,
};
use hawdb_core::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParameterBindingCacheConfig {
    pub max_entries: Option<usize>,
    pub max_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ParameterBindingCache {
    config: ParameterBindingCacheConfig,
    templates: LfuCache<(String, u64), Arc<ParameterBindingTemplate>>,
}

/// Only the cache owner can construct a conservatively charged admission.
pub struct ParameterBindingAdmission {
    key: (String, u64),
    template: Arc<ParameterBindingTemplate>,
    retained_bytes: usize,
}

impl ParameterBindingCache {
    pub fn new(config: ParameterBindingCacheConfig) -> Self {
        Self {
            config,
            templates: LfuCache::with_byte_limit(config.max_entries, Some(config.max_bytes)),
        }
    }

    pub fn config(&self) -> ParameterBindingCacheConfig {
        self.config
    }

    pub fn key(query: &str, parameters: &BTreeMap<String, Value>) -> (String, u64) {
        (
            query.to_owned(),
            parameter_binding_shape_fingerprint(parameters),
        )
    }

    /// The fingerprint selects a candidate only. Callers must validate the full
    /// shape and exact guards with `ParameterBindingTemplate::reuse`.
    pub fn lookup(&mut self, key: &(String, u64)) -> Option<Arc<ParameterBindingTemplate>> {
        self.templates.get(key)
    }

    pub fn prepare_admission(
        key: (String, u64),
        plan: &ParameterizedLogicalPlan,
        parameters: &BTreeMap<String, Value>,
    ) -> Option<ParameterBindingAdmission> {
        let template = ParameterBindingTemplate::new(plan, parameters)?;
        let retained_bytes = template
            .retained_heap_bytes()?
            .checked_add(key.0.capacity())?;
        Some(ParameterBindingAdmission {
            key,
            template: Arc::new(template),
            retained_bytes,
        })
    }

    pub fn admit(&mut self, admission: ParameterBindingAdmission) {
        self.templates
            .insert_weighted(admission.key, admission.template, admission.retained_bytes);
    }

    pub fn stats(&self) -> PlanCacheStats {
        self.templates.stats()
    }
    pub fn charged_bytes(&self) -> Option<usize> {
        self.templates.charged_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parameterize_logical_plan;

    #[test]
    fn shared_template_can_be_bound_after_the_cache_is_dropped() {
        let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
        let statement = hawdb_cypher::parse(query).unwrap();
        let values = |id| BTreeMap::from([("ids".to_owned(), Value::List(vec![Value::Int(id)]))]);
        let parameters = values(1);
        let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
        let config = ParameterBindingCacheConfig {
            max_entries: Some(2),
            max_bytes: 1024 * 1024,
        };
        let mut cache = ParameterBindingCache::new(config);
        let key = ParameterBindingCache::key(query, &parameters);
        assert!(cache.lookup(&key).is_none());
        cache.admit(
            ParameterBindingCache::prepare_admission(key.clone(), &plan, &parameters).unwrap(),
        );
        let template = cache.lookup(&key).unwrap();
        assert!(cache.charged_bytes().unwrap() <= config.max_bytes);
        drop(cache);
        let rebound = values(9);
        assert_eq!(
            template.reuse(&rebound).unwrap(),
            parameterize_logical_plan(&statement, &rebound).unwrap()
        );
    }

    #[test]
    fn prepared_admissions_are_rechecked_against_the_receiving_cache_budget() {
        let query = "MATCH (n:Item) WHERE n.id IN $ids RETURN n.id";
        let statement = hawdb_cypher::parse(query).unwrap();
        let parameters = BTreeMap::from([("ids".to_owned(), Value::List(vec![Value::Int(1)]))]);
        let plan = parameterize_logical_plan(&statement, &parameters).unwrap();
        for max_bytes in [0, 1] {
            let key = ParameterBindingCache::key(query, &parameters);
            let candidate =
                ParameterBindingCache::prepare_admission(key.clone(), &plan, &parameters).unwrap();
            let mut cache = ParameterBindingCache::new(ParameterBindingCacheConfig {
                max_entries: Some(128),
                max_bytes,
            });
            cache.admit(candidate);
            assert!(cache.lookup(&key).is_none());
            assert_eq!(cache.stats().entries, 0);
            assert_eq!(cache.charged_bytes(), Some(0));
        }
    }
}
