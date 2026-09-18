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

use crate::{
    Database, NowledgeMemGraph, NowledgeMemGraphMode, NowledgeMemQueryReportOptions, Result,
};
use hawdb_fuzz_contracts::{
    nowledge_query_fuzz_cases, nowledge_query_fuzz_error_class, nowledge_query_fuzz_harness_report,
    NowledgeQueryFuzzCase, NOWLEDGE_QUERY_FUZZ_FIXTURE_MEMORY_COUNT,
};
pub use hawdb_fuzz_contracts::{
    NowledgeQueryFuzzCaseReport, NowledgeQueryFuzzHarnessOptions, NowledgeQueryFuzzHarnessReport,
    NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL,
};

pub fn nowledge_query_fuzz_harness(
    options: NowledgeQueryFuzzHarnessOptions,
) -> Result<NowledgeQueryFuzzHarnessReport> {
    let mut graph =
        NowledgeMemGraph::from_database(Database::new(), NowledgeMemGraphMode::WritableCutover);
    seed_fixture_graph(&mut graph)?;

    let fuzz_cases = nowledge_query_fuzz_cases(options);
    let mut case_reports = Vec::with_capacity(fuzz_cases.len());
    for (index, case) in fuzz_cases.into_iter().enumerate() {
        case_reports.push(run_case(&mut graph, index, case, options)?);
    }

    Ok(nowledge_query_fuzz_harness_report(options, case_reports))
}

fn seed_fixture_graph(graph: &mut NowledgeMemGraph) -> Result<()> {
    for index in 0..NOWLEDGE_QUERY_FUZZ_FIXTURE_MEMORY_COUNT {
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
    case: NowledgeQueryFuzzCase,
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
            seed: case.seed,
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
            seed: case.seed,
            template: case.template,
            query_family: case.query_family,
            success: false,
            row_count: 0,
            execution_path: None,
            fast_path_reason: None,
            plan_cache_lookup: None,
            scan_pruning_report_count: 0,
            error_class: Some(nowledge_query_fuzz_error_class(&error).to_string()),
            blocker_codes: vec!["query_fuzz_case_failed".to_string()],
        }),
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
