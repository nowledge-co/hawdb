// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Same-execution graph cost and CE observations. Qualification bypasses the
//! physical cache and retains the actual chosen optimizer trace on failure.

use crate::manifest::Manifest;
use crate::runner::{Observation, Status};
use hawdb::optimizer::{OptimizerQualificationOptions, OptimizerSearchDirective};
use hawdb::{
    DatabaseReadTransaction, QueryRequest, QueryStreamOptions, RuntimeMemoryReservation,
    RuntimeTaskContext,
};
use query_workloads::smith::Case;
use serde_json::{json, Value as Json};
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

pub fn observe(
    read: &mut DatabaseReadTransaction,
    case: &Case,
    m: &Manifest,
    remaining: Duration,
) -> Observation {
    let parameters = case
        .statement
        .values()
        .into_iter()
        .enumerate()
        .map(|(index, value)| (format!("p{index}"), value))
        .collect::<BTreeMap<_, _>>();
    let task = RuntimeTaskContext::with_timeout(
        Duration::from_millis(m.attempt_timeout_ms).min(remaining),
    )
    .with_work_limit(m.work_limit)
    .with_memory_reservation(RuntimeMemoryReservation::new(
        m.memory_bytes,
        m.limits.max_output_bytes as u64,
    ));
    let start = Instant::now();
    let result = catch_unwind(AssertUnwindSafe(|| {
        read.qualify_optimizer_query(
            QueryRequest::new(&case.statement.sql)
                .with_params(&parameters)
                .with_task_context(&task)
                .with_output_limits(QueryStreamOptions {
                    max_rows: Some(m.limits.max_output_rows),
                    max_payload_bytes: Some(m.limits.max_output_bytes),
                }),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::Auto,
        )
    }));
    let mut observation = Observation {
        status: Status::Completed,
        error: None,
        snapshot_epoch: read.commit_epoch(),
        end_to_end_ns: start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        full_result: None,
        evidence: None,
    };
    let qualified = match result {
        Ok(Ok(qualified)) => qualified,
        Ok(Err(error)) => {
            observation.status = crate::runner::error_status(&error);
            observation.error = Some(error.to_string());
            return observation;
        }
        Err(_) => {
            observation.status = Status::Panic;
            observation.error = Some("graph qualification panicked".into());
            return observation;
        }
    };
    let trace = qualified.optimizer_trace;
    let cost = trace.selected_plan_cost_breakdown;
    observation.evidence = Some(json!({
        "cost":{"estimated_rows":cost.estimated_rows,"total":cost.cost,"cpu":cost.cpu,
            "random_access":cost.random_io,"sequential_access":cost.sequential_io,"output":cost.output_rows},
        "cost_scope":"selected_graph_physical_plan",
        "coefficient_policy":"engine_default_logical_weights","cost_policy_qualified":false,
        "physical_cache_policy":"qualification_bypass",
        "complete_physical_fingerprint":trace.selected_plan_fingerprint,
        "identity_limit":"physical tree shape excludes bound values; typed parameters are frozen separately",
        "executed_access_shape":[trace.selected_plan_fingerprint],
        "statistics_decisions":trace.decisions,
        "parse_ns":null,"bind_ns":null,"plan_ns":null,"execute_ns":null,
        "complete_output_scope":"owned rows from the same qualified execution; host serialization excluded",
        "physical_bytes_are_device_io":false,
    }));
    let output = match qualified.result {
        Ok(output) => output,
        Err(error) => {
            observation.status = crate::runner::error_status(&error);
            observation.error = Some(error.to_string());
            return observation;
        }
    };
    let mut rows = output
        .output
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|(key, value)| (key.to_owned(), query_workloads::typed_value_json(value)))
                .collect::<BTreeMap<_, _>>()
        })
        .collect::<Vec<_>>();
    if !case.ordered {
        rows.sort_by_cached_key(|row| serde_json::to_string(row).expect("typed row"));
    }
    observation.full_result =
        Some(json!({"columns":output.output.rows.schema().columns(),"rows":rows}));
    let profile = output.execution_profile;
    let complete = query_workloads::compressed_statistics::complete_query(&case.statement.sql);
    let operators = trace
        .selected_plan_cardinality_estimates
        .iter()
        .map(|estimate| {
            let actual = profile.operator_cardinality_profiles.iter().find(|actual| {
                actual.operator_id == estimate.operator_id && actual.operator == estimate.operator
            });
            json!({"id":estimate.operator_id.ordinal() + 1,"native_ordinal":estimate.operator_id.ordinal(),"operator":estimate.operator.as_str(),
            "table":"","access":estimate.operator.as_str(),"estimated_rows":estimate.estimated_rows,
            "actual_rows":actual.and_then(|actual| actual.actual_rows),
            "fully_consumed":complete && actual.is_some_and(|actual| actual.actual_rows.is_some()),
            "counter_scope":"total_operator_output"})
        })
        .collect::<Vec<_>>();
    let memory = profile.pipeline_memory_report;
    let evidence = observation
        .evidence
        .as_mut()
        .expect("qualified graph evidence");
    evidence["operator_cardinalities"] = json!(operators);
    evidence["tracked_query_memory_peak_bytes"] = json!(memory.query_memory_peak_bytes);
    evidence["rss_bytes"] = json!(memory.steady_resident_bytes);
    evidence["process_lifetime_peak_rss_bytes"] = json!(memory.peak_resident_bytes);
    evidence["allocator_peak_bytes"] = Json::Null;
    evidence["intermediate_rows"] = json!(memory.intermediate_rows);
    observation
}

#[cfg(test)]
mod tests {
    use crate::{cardinality, manifest::Frozen, runner};

    #[test]
    fn directed_graph_distribution_cases_keep_same_execution_evidence_and_truth() {
        let frozen = Frozen::graph_statistics(42, 128).unwrap();
        let run = runner::run(&frozen, false).unwrap();
        let report = cardinality::analyze(&frozen, &run).unwrap();
        assert_eq!(report.cases.len(), 20);
        for case in &report.cases {
            assert_eq!(case.state, cardinality::CaseState::Observed, "{case:?}");
            assert_eq!(case.actual_result_rows, case.expected_result_rows);
        }
        assert!(run.attempts.iter().all(|attempt| attempt
            .observation
            .evidence
            .as_ref()
            .is_some_and(|evidence| evidence["physical_cache_policy"] == "qualification_bypass")));
        let replay = runner::replay(&frozen, 0).unwrap();
        assert_eq!(replay.full_result, run.attempts[0].observation.full_result);
        let mut manifest = frozen.manifest;
        manifest.cells[0]
            .case
            .as_mut()
            .unwrap()
            .statement
            .sql
            .push_str(" LIMIT 1");
        assert!(Frozen::seal(manifest).is_err());
    }
}
