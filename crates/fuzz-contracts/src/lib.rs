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

//! Portable contracts for deterministic Nowledge query fuzz harnesses.

use hawdb_core::{HawDBError, Value};
use std::collections::BTreeMap;

pub const NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL: &str = "hawdb-nowledge-query-fuzz-harness-v1";
pub const NOWLEDGE_QUERY_FUZZ_FIXTURE_MEMORY_COUNT: usize = 8;
const DEFAULT_CASE_COUNT: usize = 64;
const MAX_CASE_COUNT: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeQueryFuzzHarnessOptions {
    pub seed: u64,
    pub case_count: usize,
    pub capture_physical_plan: bool,
}

impl Default for NowledgeQueryFuzzHarnessOptions {
    fn default() -> Self {
        Self {
            seed: 0x9e37_79b9_7f4a_7c15,
            case_count: DEFAULT_CASE_COUNT,
            capture_physical_plan: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryFuzzCaseReport {
    pub index: usize,
    pub seed: u64,
    pub template: &'static str,
    pub query_family: &'static str,
    pub success: bool,
    pub row_count: usize,
    pub execution_path: Option<String>,
    pub fast_path_reason: Option<String>,
    pub plan_cache_lookup: Option<String>,
    pub scan_pruning_report_count: usize,
    pub error_class: Option<String>,
    pub blocker_codes: Vec<String>,
}

impl NowledgeQueryFuzzCaseReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "index": self.index,
            "seed": self.seed,
            "template": self.template,
            "query_family": self.query_family,
            "success": self.success,
            "row_count": self.row_count,
            "execution_path": self.execution_path,
            "fast_path_reason": self.fast_path_reason,
            "plan_cache_lookup": self.plan_cache_lookup,
            "scan_pruning_report_count": self.scan_pruning_report_count,
            "error_class": self.error_class,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryFuzzHarnessReport {
    pub protocol: String,
    pub ready: bool,
    pub seed: u64,
    pub requested_case_count: usize,
    pub executed_case_count: usize,
    pub failed_case_count: usize,
    pub blocker_codes: Vec<String>,
    pub cases: Vec<NowledgeQueryFuzzCaseReport>,
}

impl NowledgeQueryFuzzHarnessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "seed": self.seed,
            "requested_case_count": self.requested_case_count,
            "executed_case_count": self.executed_case_count,
            "failed_case_count": self.failed_case_count,
            "blocker_codes": self.blocker_codes,
            "cases": self.cases.iter().map(NowledgeQueryFuzzCaseReport::json).collect::<Vec<_>>(),
        })
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeQueryFuzzCase {
    pub seed: u64,
    pub template: &'static str,
    pub query_family: &'static str,
    pub cypher: &'static str,
    pub parameters: BTreeMap<String, Value>,
}

#[doc(hidden)]
pub fn nowledge_query_fuzz_cases(
    options: NowledgeQueryFuzzHarnessOptions,
) -> Vec<NowledgeQueryFuzzCase> {
    let mut rng = DeterministicRng::new(options.seed);
    (0..options.case_count.min(MAX_CASE_COUNT))
        .map(|_| NowledgeQueryFuzzCase::from_seed(rng.next_u64()))
        .collect()
}

#[doc(hidden)]
pub fn nowledge_query_fuzz_harness_report(
    options: NowledgeQueryFuzzHarnessOptions,
    cases: Vec<NowledgeQueryFuzzCaseReport>,
) -> NowledgeQueryFuzzHarnessReport {
    let failed_case_count = cases.iter().filter(|case| !case.success).count();
    let mut blocker_codes = Vec::new();
    if options.case_count == 0 {
        blocker_codes.push("fuzz_cases_missing".to_string());
    }
    if failed_case_count > 0 {
        blocker_codes.push("fuzz_case_failed".to_string());
    }
    if options.case_count > MAX_CASE_COUNT {
        blocker_codes.push("fuzz_case_count_capped".to_string());
    }
    NowledgeQueryFuzzHarnessReport {
        protocol: NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL.to_string(),
        ready: blocker_codes.is_empty(),
        seed: options.seed,
        requested_case_count: options.case_count,
        executed_case_count: cases.len(),
        failed_case_count,
        blocker_codes,
        cases,
    }
}

#[doc(hidden)]
pub const fn nowledge_query_fuzz_error_class(error: &HawDBError) -> &'static str {
    match error {
        HawDBError::Parse(_) => "parse",
        HawDBError::Semantic(_) => "semantic",
        HawDBError::Execution(_) => "execution",
        HawDBError::Storage(_)
        | HawDBError::StorageIntegrity(_)
        | HawDBError::AppendSequenceExhausted { .. } => "storage",
        HawDBError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

impl NowledgeQueryFuzzCase {
    fn from_seed(seed: u64) -> Self {
        let memory_index = (seed as usize) % NOWLEDGE_QUERY_FUZZ_FIXTURE_MEMORY_COUNT;
        let alternate_index = ((seed >> 8) as usize) % NOWLEDGE_QUERY_FUZZ_FIXTURE_MEMORY_COUNT;
        let mut parameters = BTreeMap::new();
        match seed % 5 {
            0 => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
                    seed,
                    template: "node_pattern_lookup",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory {id: $id}) RETURN m.id AS id",
                    parameters,
                }
            }
            1 => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
                    seed,
                    template: "where_equality_lookup",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
                    parameters,
                }
            }
            2 => {
                let kind = if seed & 1 == 0 { "note" } else { "thread" };
                parameters.insert("kind".to_string(), Value::String(kind.to_string()));
                Self {
                    seed,
                    template: "property_pruned_kind_scan",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.id AS id ORDER BY m.id LIMIT 4",
                    parameters,
                }
            }
            3 => {
                parameters.insert(
                    "ids".to_string(),
                    Value::List(vec![memory_id(memory_index), memory_id(alternate_index)]),
                );
                Self {
                    seed,
                    template: "in_list_lookup",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id AS id ORDER BY m.id LIMIT 4",
                    parameters,
                }
            }
            _ => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
                    seed,
                    template: "system_hint_lookup",
                    query_family: "memory_lookup",
                    cypher: "CYPHER system.work_priority = 'foreground' system.work_class = 'query' MATCH (m:Memory {id: $id}) RETURN m.title AS title",
                    parameters,
                }
            }
        }
    }
}

fn memory_id(index: usize) -> Value {
    Value::String(format!("mem-fuzz-{index}"))
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_generation_is_deterministic_and_bounded() {
        let options = NowledgeQueryFuzzHarnessOptions {
            seed: 7,
            case_count: MAX_CASE_COUNT + 1,
            capture_physical_plan: false,
        };
        let first = nowledge_query_fuzz_cases(options);
        let second = nowledge_query_fuzz_cases(options);
        assert_eq!(first, second);
        assert_eq!(first.len(), MAX_CASE_COUNT);
    }

    #[test]
    fn harness_report_fails_closed_for_missing_and_capped_cases() {
        let missing = nowledge_query_fuzz_harness_report(
            NowledgeQueryFuzzHarnessOptions {
                case_count: 0,
                ..NowledgeQueryFuzzHarnessOptions::default()
            },
            Vec::new(),
        );
        assert_eq!(missing.blocker_codes, vec!["fuzz_cases_missing"]);

        let capped = nowledge_query_fuzz_harness_report(
            NowledgeQueryFuzzHarnessOptions {
                case_count: MAX_CASE_COUNT + 1,
                ..NowledgeQueryFuzzHarnessOptions::default()
            },
            Vec::new(),
        );
        assert_eq!(capped.blocker_codes, vec!["fuzz_case_count_capped"]);
    }
}
