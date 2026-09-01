use crate::{
    Database, NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemQueryReportOptions, Result,
    SkeinError, Value,
};
use std::collections::BTreeMap;

pub const NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL: &str = "skein-nowledge-query-fuzz-harness-v1";
const DEFAULT_CASE_COUNT: usize = 64;
const MAX_CASE_COUNT: usize = 512;
const FIXTURE_MEMORY_COUNT: usize = 8;

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

pub fn nowledge_query_fuzz_harness(
    options: NowledgeQueryFuzzHarnessOptions,
) -> Result<NowledgeQueryFuzzHarnessReport> {
    let requested_case_count = options.case_count;
    let case_count = options.case_count.min(MAX_CASE_COUNT);
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    seed_fixture_graph(&mut graph)?;

    let mut rng = DeterministicRng::new(options.seed);
    let mut cases = Vec::with_capacity(case_count);
    for index in 0..case_count {
        let case_seed = rng.next_u64();
        let case = FuzzCase::from_seed(case_seed);
        cases.push(run_case(&mut graph, index, case_seed, case, options)?);
    }

    let failed_case_count = cases.iter().filter(|case| !case.success).count();
    let mut blocker_codes = Vec::new();
    if requested_case_count == 0 {
        blocker_codes.push("fuzz_cases_missing".to_string());
    }
    if failed_case_count > 0 {
        blocker_codes.push("fuzz_case_failed".to_string());
    }
    if requested_case_count > MAX_CASE_COUNT {
        blocker_codes.push("fuzz_case_count_capped".to_string());
    }
    let ready = blocker_codes.is_empty();

    Ok(NowledgeQueryFuzzHarnessReport {
        protocol: NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL.to_string(),
        ready,
        seed: options.seed,
        requested_case_count,
        executed_case_count: case_count,
        failed_case_count,
        blocker_codes,
        cases,
    })
}

fn seed_fixture_graph(graph: &mut NowledgeMemGraph) -> Result<()> {
    for index in 0..FIXTURE_MEMORY_COUNT {
        let kind = if index % 2 == 0 { "note" } else { "thread" };
        graph.query(&format!(
            "CREATE (:Memory {{id: 'mem-fuzz-{index}', kind: '{kind}', title: 'Fuzz {index}'}})"
        ))?;
    }
    Ok(())
}

fn run_case(
    graph: &mut NowledgeMemGraph,
    index: usize,
    seed: u64,
    case: FuzzCase,
    options: NowledgeQueryFuzzHarnessOptions,
) -> Result<NowledgeQueryFuzzCaseReport> {
    let query_options = NowledgeMemQueryReportOptions {
        capture_physical_plan: options.capture_physical_plan,
        slow_log_threshold_micros: None,
    };
    match graph.query_with_params_with_report_options(case.cypher, &case.parameters, query_options)
    {
        Ok(output) => Ok(NowledgeQueryFuzzCaseReport {
            index,
            seed,
            template: case.template,
            query_family: case.query_family,
            success: true,
            row_count: output.output.rows.len(),
            execution_path: Some(output.report.execution_path.as_str().to_string()),
            fast_path_reason: output.report.fast_path_reason,
            plan_cache_lookup: output.report.plan_cache_lookup,
            scan_pruning_report_count: output.report.scan_pruning_reports.len(),
            error_class: None,
            blocker_codes: Vec::new(),
        }),
        Err(error) => Ok(NowledgeQueryFuzzCaseReport {
            index,
            seed,
            template: case.template,
            query_family: case.query_family,
            success: false,
            row_count: 0,
            execution_path: None,
            fast_path_reason: None,
            plan_cache_lookup: None,
            scan_pruning_report_count: 0,
            error_class: Some(skein_error_class(&error).to_string()),
            blocker_codes: vec!["query_fuzz_case_failed".to_string()],
        }),
    }
}

#[derive(Debug, Clone)]
struct FuzzCase {
    template: &'static str,
    query_family: &'static str,
    cypher: &'static str,
    parameters: BTreeMap<String, Value>,
}

impl FuzzCase {
    fn from_seed(seed: u64) -> Self {
        let memory_index = (seed as usize) % FIXTURE_MEMORY_COUNT;
        let alternate_index = ((seed >> 8) as usize) % FIXTURE_MEMORY_COUNT;
        let mut parameters = BTreeMap::new();
        match seed % 5 {
            0 => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
                    template: "node_pattern_lookup",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory {id: $id}) RETURN m.id AS id",
                    parameters,
                }
            }
            1 => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
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
                    template: "in_list_lookup",
                    query_family: "memory_lookup",
                    cypher: "MATCH (m:Memory) WHERE m.id IN $ids RETURN m.id AS id ORDER BY m.id LIMIT 4",
                    parameters,
                }
            }
            _ => {
                parameters.insert("id".to_string(), memory_id(memory_index));
                Self {
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
    fn new(seed: u64) -> Self {
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

fn skein_error_class(error: &SkeinError) -> &'static str {
    match error {
        SkeinError::Parse(_) => "parse",
        SkeinError::Semantic(_) => "semantic",
        SkeinError::Execution(_) => "execution",
        SkeinError::Storage(_)
        | SkeinError::StorageIntegrity(_)
        | SkeinError::AppendSequenceExhausted { .. } => "storage",
        SkeinError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        nowledge_query_fuzz_harness, NowledgeQueryFuzzHarnessOptions,
        NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL,
    };

    #[test]
    fn nowledge_query_fuzz_harness_runs_deterministic_library_cases() {
        let report = nowledge_query_fuzz_harness(NowledgeQueryFuzzHarnessOptions {
            seed: 7,
            case_count: 25,
            capture_physical_plan: false,
        })
        .unwrap();

        assert!(report.ready);
        assert_eq!(report.protocol, NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL);
        assert_eq!(report.executed_case_count, 25);
        assert_eq!(report.failed_case_count, 0);
        assert!(report.cases.iter().all(|case| case.success));
        assert!(report
            .cases
            .iter()
            .any(|case| case.template == "property_pruned_kind_scan"
                && case.scan_pruning_report_count > 0));
        assert_eq!(report.json()["ready"], true);
    }

    #[test]
    fn nowledge_query_fuzz_harness_fails_closed_without_cases() {
        let report = nowledge_query_fuzz_harness(NowledgeQueryFuzzHarnessOptions {
            seed: 7,
            case_count: 0,
            capture_physical_plan: false,
        })
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.executed_case_count, 0);
        assert_eq!(report.blocker_codes, vec!["fuzz_cases_missing"]);
    }

    #[test]
    fn nowledge_query_fuzz_harness_caps_case_count() {
        let report = nowledge_query_fuzz_harness(NowledgeQueryFuzzHarnessOptions {
            seed: 7,
            case_count: 513,
            capture_physical_plan: false,
        })
        .unwrap();

        assert!(!report.ready);
        assert_eq!(report.executed_case_count, 512);
        assert_eq!(report.blocker_codes, vec!["fuzz_case_count_capped"]);
    }
}
