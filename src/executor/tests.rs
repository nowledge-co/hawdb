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

//! Executor admission, streaming, spill, and graph operator regressions.

#![allow(deprecated)]

use super::*;

#[path = "tests/clause_mutations.rs"]
mod clause_mutations;
#[path = "tests/clause_normalization.rs"]
mod clause_normalization;
#[path = "tests/clause_procedures.rs"]
mod clause_procedures;

#[path = "tests/shortest_path.rs"]
mod shortest_path;
use crate::planner::{
    AggregateFunction, AggregateTarget, ProjectionExpression, ShortestPathProjection,
    ShortestPathProjectionExpression, SortDirection, SortKey,
};
use crate::store::{DurabilityPolicy, ScanPruningStrategy, StorageResidencyMode, WalReplayConfig};

#[test]
fn vector_seed_receives_resolved_runtime_resource_contract() {
    #[derive(Default)]
    struct RecordingExternalRead {
        observed: Option<(u8, usize, usize, usize, usize, bool)>,
    }

    impl ExternalReadOperator for RecordingExternalRead {
        fn execute_vector_seed(
            &mut self,
            request: VectorSeedExecutionRequest<'_>,
        ) -> Result<VectorSeedExecutionOutput> {
            self.observed = Some((
                request.resources.priority,
                request.resources.max_parallelism.get(),
                request.resources.max_working_memory_bytes.get(),
                request.resources.result.max_rows,
                request.resources.result.max_memory_bytes.get(),
                request.resources.task_context.is_some(),
            ));
            Err(HawDBError::Execution(
                "recorded external read contract".to_string(),
            ))
        }
    }

    let plan = PhysicalPlan::VectorSeedScan {
        embedding_parameter: "embedding".to_string(),
        output_external_id: false,
        metadata_filters: BTreeMap::new(),
        resource_profile: hawdb_plan::VectorExecutionResourceProfile {
            priority: 200,
            max_parallelism: 4,
            max_working_memory_bytes: Some(2048),
        },
        vector_plan: hawdb_plan::VectorPhysicalPlan::TopK {
            limit: 3,
            input: Box::new(hawdb_plan::VectorPhysicalPlan::RawVectorRerank {
                embedding_dimension: 2,
                input: Box::new(hawdb_plan::VectorPhysicalPlan::VectorCandidateScan {
                    source: hawdb_plan::VectorCandidateSource::Scalar,
                    embedding_dimension: 2,
                    candidate_limit: 3,
                    input: Box::new(hawdb_plan::VectorPhysicalPlan::Filter { fields: Vec::new() }),
                }),
            }),
        },
    };
    let parameters = BTreeMap::from([(
        "embedding".to_string(),
        Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
    )]);
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let task_context =
        RuntimeTaskContext::default().with_admitted_parallelism(NonZeroUsize::new(2).unwrap());
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let mut external = RecordingExternalRead::default();

    let error = execute_with_output_limits_profile_and_external_and_context_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &parameters,
        &mut external,
        Some(1),
        None,
        &task_context,
        &memory,
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("recorded external read contract"));
    assert_eq!(external.observed, Some((200, 2, 2048, 2, 4096, true)));
}

fn spill_test_config(name: &str) -> ExecutionMemoryConfig {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(256 * 1024 * 1024).unwrap(),
        batch_rows: NonZeroUsize::new(2).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        max_spill_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        max_spill_runs: NonZeroUsize::new(64).unwrap(),
        max_total_spill_bytes: NonZeroU64::new(256 * 1024 * 1024).unwrap(),
        max_total_spill_runs: NonZeroUsize::new(256).unwrap(),
        min_spill_free_bytes: NonZeroU64::new(1).unwrap(),
        spill_free_space_probe_interval_bytes: NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        spill_orphan_grace_period: std::time::Duration::ZERO,
        spill_directory: std::env::temp_dir().join(format!("hawdb-{name}-{nonce}")),
    }
}

#[test]
fn empty_exec_returns_without_scanning_or_buffering_rows() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(&mut catalog, "Item", properties([("rank", Value::Int(1))]))
        .unwrap();

    let output =
        execute_with_row_limit_profile(&PhysicalPlan::EmptyExec, &mut catalog, &mut store, None)
            .unwrap();

    assert!(output.rows.is_empty());
    assert!(output.profile.scan_pruning_reports.is_empty());
    assert_eq!(output.profile.operator_cardinality_profiles.len(), 1);
    assert_eq!(
        output.profile.operator_cardinality_profiles[0].actual_rows,
        Some(0)
    );
    assert_eq!(output.profile.pipeline_memory_report.intermediate_rows, 0);
    assert_eq!(output.profile.pipeline_memory_report.output_rows, 0);
}

#[test]
fn adjacency_exists_exec_filters_bound_pairs_and_reports_its_operator() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    let matching_target = store
        .create_node(&mut catalog, "Target", BTreeMap::new())
        .unwrap();
    store
        .create_node(&mut catalog, "Target", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(
            &mut catalog,
            source,
            matching_target,
            "LINKS_TO",
            BTreeMap::new(),
        )
        .unwrap();
    let plan = PhysicalPlan::AdjacencyExistsExec {
        source_variable: "source".to_string(),
        rel_type: "LINKS_TO".to_string(),
        direction: crate::cypher::RelationshipDirection::Outgoing,
        target_variable: "target".to_string(),
        input: Box::new(PhysicalPlan::NodeCartesianProductExec {
            left: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "source".to_string(),
                label: "Source".to_string(),
            }),
            right: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "target".to_string(),
                label: "Target".to_string(),
            }),
        }),
    };

    let output = execute_with_row_limit_profile(&plan, &mut catalog, &mut store, None).unwrap();

    assert_eq!(output.rows.len(), 1);
    assert!(output
        .profile
        .operator_cardinality_profiles
        .iter()
        .any(|profile| {
            profile.operator.as_str() == "AdjacencyExistsExec" && profile.actual_rows == Some(1)
        }));
}

#[test]
fn node_count_exec_uses_label_count_without_scanning() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in 0..3 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    store
        .create_node(&mut catalog, "Other", properties([]))
        .unwrap();

    let output = execute_with_row_limit_profile(
        &PhysicalPlan::NodeCountExec {
            label: "Item".to_string(),
            output: "item_count".to_string(),
        },
        &mut catalog,
        &mut store,
        None,
    )
    .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("item_count".to_string(), Value::Int(3))])]
    );
    assert_eq!(output.profile.scan_pruning_reports.len(), 1);
    let count_report = &output.profile.scan_pruning_reports[0];
    assert_eq!(count_report.strategy, ScanPruningStrategy::ExactCount);
    assert!(count_report.pruned);
    assert_eq!(count_report.candidate_count_before_pruning, 3);
    assert_eq!(count_report.pruned_candidate_count, 3);
    assert_eq!(count_report.output_count, 1);
    assert_eq!(output.profile.pipeline_memory_report.output_rows, 1);
}

#[test]
fn relationship_count_exec_uses_type_count_without_expansion() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let first = store
        .create_node(&mut catalog, "Item", properties([]))
        .unwrap();
    let second = store
        .create_node(&mut catalog, "Item", properties([]))
        .unwrap();
    let third = store
        .create_node(&mut catalog, "Item", properties([]))
        .unwrap();
    store
        .create_relationship(&mut catalog, first, second, "LINK", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(&mut catalog, second, third, "LINK", BTreeMap::new())
        .unwrap();

    let output = execute_with_row_limit_profile(
        &PhysicalPlan::RelationshipCountExec {
            rel_type: "LINK".to_string(),
            output: "link_count".to_string(),
        },
        &mut catalog,
        &mut store,
        None,
    )
    .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([("link_count".to_string(), Value::Int(2))])]
    );
    assert_eq!(output.profile.scan_pruning_reports.len(), 1);
    let count_report = &output.profile.scan_pruning_reports[0];
    assert_eq!(
        count_report.target_kind,
        crate::store::ScanPruningTargetKind::Relationship
    );
    assert_eq!(count_report.strategy, ScanPruningStrategy::ExactCount);
    assert!(count_report.pruned);
    assert_eq!(count_report.candidate_count_before_pruning, 2);
    assert_eq!(count_report.pruned_candidate_count, 2);
    assert_eq!(count_report.output_count, 1);
}

#[test]
fn node_projection_scan_omits_unrequested_large_properties() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in 0..2 {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("rank", Value::Int(rank)),
                    ("title", Value::String(format!("memory-{rank}"))),
                    ("content", Value::String("x".repeat(128 * 1024))),
                ]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "m".to_string(),
        label: "Memory".to_string(),
        access: hawdb_plan::NodeProjectionAccess::LabelScan,
        required_properties: vec!["rank".to_string(), "title".to_string()],
        predicate: Some(Predicate::PropertyCompare {
            variable: "m".to_string(),
            property: "rank".to_string(),
            op: crate::planner::ComparisonOp::Gte,
            value: Value::Int(1),
        }),
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
    };
    let memory = ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;

    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([(
            "title".to_string(),
            Value::String("memory-1".to_string()),
        )])]
    );
    assert_eq!(output.profile.pipeline_memory_report.output_rows, 1);
}

#[test]
fn indexed_node_projection_keeps_large_properties_out_of_pipeline_batches() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in 0..2 {
        store
            .create_node(
                &mut catalog,
                "Memory",
                properties([
                    ("stable_id", Value::String(format!("memory:{rank}"))),
                    ("title", Value::String(format!("memory-{rank}"))),
                    ("content", Value::String("x".repeat(128 * 1024))),
                ]),
            )
            .unwrap();
    }
    store
        .create_property_index(&mut catalog, "Memory", "stable_id")
        .unwrap();
    let plan = PhysicalPlan::NodeProjectionScanExec {
        variable: "m".to_string(),
        label: "Memory".to_string(),
        access: hawdb_plan::NodeProjectionAccess::PropertyValues {
            property: "stable_id".to_string(),
            values: vec![Value::String("memory:1".to_string())],
        },
        required_properties: vec!["stable_id".to_string(), "title".to_string()],
        predicate: Some(Predicate::PropertyEq {
            variable: "m".to_string(),
            property: "stable_id".to_string(),
            value: Value::String("memory:1".to_string()),
        }),
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "m".to_string(),
                property: "title".to_string(),
            },
            name: "title".to_string(),
        }],
    };
    let memory = ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;

    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output.rows,
        vec![BTreeMap::from([(
            "title".to_string(),
            Value::String("memory-1".to_string()),
        )])]
    );
    let report = output
        .profile
        .scan_pruning_reports
        .iter()
        .find(|report| {
            report.strategy
                == ScanPruningStrategy::PropertyEq {
                    property: "stable_id".to_string(),
                }
        })
        .expect("projected index seek should report property pruning");
    assert!(report.pruned);
    assert_eq!(report.candidate_count_before_pruning, 2);
    assert_eq!(report.candidate_count_before_filter, 1);
    assert_eq!(report.output_count, 1);
}

#[test]
fn sort_pipeline_spills_runs_under_a_tight_memory_budget() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..12).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            name: "rank".to_string(),
        }],
        input: Box::new(PhysicalPlan::SortExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = spill_test_config("sort-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["rank"].clone())
            .collect::<Vec<_>>(),
        (0..12).map(Value::Int).collect::<Vec<_>>()
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "SortExec")
        .unwrap();
    assert_eq!(report.input_rows, 12);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 12);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    let pipeline = &output.profile.pipeline_memory_report;
    assert_eq!(pipeline.intermediate_rows, 36);
    assert!(pipeline.intermediate_payload_bytes >= pipeline.output_payload_bytes);
    assert_eq!(pipeline.peak_batch_rows, 2);
    assert_eq!(pipeline.output_rows, 12);
    assert!(pipeline.output_payload_bytes > 0);
    assert_eq!(
        pipeline.query_memory_budget_bytes,
        memory.query_memory_bytes.get()
    );
    assert!(pipeline.query_memory_peak_bytes > 0);
    assert!(pipeline.query_memory_peak_bytes <= pipeline.query_memory_budget_bytes);
    assert!(pipeline.query_memory_completion_bytes > 0);
    assert!(pipeline.query_memory_account_count >= 4);
    assert!(pipeline.start_resident_bytes.is_some());
    assert!(pipeline.steady_resident_bytes.is_some());
    assert!(pipeline.peak_resident_bytes.is_some());
    assert!(pipeline.total_page_faults.is_some());
    assert_eq!(pipeline.minor_page_faults.is_some(), cfg!(unix));
    assert_eq!(pipeline.major_page_faults.is_some(), cfg!(unix));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn streaming_consumer_releases_query_memory_before_completion() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in 0..3 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            name: "rank".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    let mut rows = 0usize;

    let output = execute_with_row_consumer_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &mut |_| {
            rows += 1;
            Ok(())
        },
        &memory,
    )
    .unwrap();

    assert_eq!(rows, 3);
    let pipeline = &output.profile.pipeline_memory_report;
    assert!(pipeline.query_memory_peak_bytes > 0);
    assert_eq!(pipeline.query_memory_completion_bytes, 0);
}

#[test]
fn grouped_aggregate_pipeline_spills_and_merges_groups() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("group", Value::Int(value % 8))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![Aggregation {
            function: AggregateFunction::Count,
            target: AggregateTarget::All,
            distinct: false,
            name: "count".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = spill_test_config("aggregate-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| (row["group"].clone(), row["count"].clone()))
            .collect::<Vec<_>>(),
        vec![
            (Value::Int(0), Value::Int(3)),
            (Value::Int(1), Value::Int(3)),
            (Value::Int(2), Value::Int(3)),
            (Value::Int(3), Value::Int(3)),
            (Value::Int(4), Value::Int(2)),
            (Value::Int(5), Value::Int(2)),
            (Value::Int(6), Value::Int(2)),
            (Value::Int(7), Value::Int(2)),
        ]
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert_eq!(report.input_rows, 20);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn grouped_partial_aggregate_spill_does_not_write_unused_binding_payloads() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let payload = "x".repeat(4096);
    for value in 0..24 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("group", Value::Int(value % 8)),
                    ("value", Value::Int(value)),
                    ("payload", Value::String(payload.clone())),
                ]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![
            Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::All,
                distinct: false,
                name: "count".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Min,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "min".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Max,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "max".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Avg,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "avg".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let mut memory = spill_test_config("aggregate-partial-spill");
    memory.blocking_operator_bytes = NonZeroUsize::new(2048).unwrap();
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(output.rows.len(), 8);
    for (group, row) in output.rows.iter().enumerate() {
        assert_eq!(row["group"], Value::Int(group as i64));
        assert_eq!(row["count"], Value::Int(3));
        assert_eq!(row["min"], Value::Int(group as i64));
        assert_eq!(row["max"], Value::Int(group as i64 + 16));
        assert_eq!(row["avg"], Value::Float(group as f64 + 8.0));
    }
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 24);
    assert!(report.spilled_bytes < 24 * payload.len() as u64);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn top_n_pipeline_spills_without_changing_order_or_offset() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..50).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            name: "rank".to_string(),
        }],
        input: Box::new(PhysicalPlan::TopNExec {
            items: vec![SortItem {
                key: SortKey::Property {
                    variable: "n".to_string(),
                    property: "rank".to_string(),
                },
                direction: SortDirection::Asc,
            }],
            offset: 7,
            limit: 5,
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = spill_test_config("topn-spill");
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(
        output
            .rows
            .iter()
            .map(|row| row["rank"].clone())
            .collect::<Vec<_>>(),
        (7..12).map(Value::Int).collect::<Vec<_>>()
    );
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "TopNExec")
        .unwrap();
    assert!(report.spill_run_count > 1);
    assert!(report.spilled_rows > 0);
    assert!(report.spilled_rows <= report.input_rows);
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes <= report.max_spill_bytes);
    assert!(report.spill_run_count <= report.max_spill_runs);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn distinct_spills_and_deduplicates_across_memory_bounded_runs() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([(
                    "value",
                    Value::String(format!("{}-{}", value % 5, "x".repeat(96))),
                )]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::DistinctExec {
        input: Box::new(PhysicalPlan::ProjectExec {
            items: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                name: "value".to_string(),
            }],
            input: Box::new(PhysicalPlan::SeqNodeScan {
                variable: "n".to_string(),
                label: "Item".to_string(),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(2048).unwrap(),
        ..spill_test_config("distinct-admission")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    assert_eq!(output.rows.len(), 5);
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "DistinctExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spill_run_count > 1);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn collect_aggregate_rejects_unbounded_group_state() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([(
                    "value",
                    Value::String(format!("{value}-{}", "x".repeat(64))),
                )]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: Vec::new(),
        items: vec![Aggregation {
            function: AggregateFunction::Collect,
            target: AggregateTarget::Property {
                variable: "n".to_string(),
                property: "value".to_string(),
            },
            distinct: false,
            name: "values".to_string(),
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        ..spill_test_config("collect-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("AggregateExec state exceeds"));
}

#[test]
fn grouped_mixed_aggregate_spills_only_required_operands() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..64i64 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("group", Value::Int(value % 4)),
                    ("value", Value::Int(value)),
                    ("payload", Value::String("x".repeat(16 * 1024))),
                ]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AggregateExec {
        group_keys: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "group".to_string(),
            },
            name: "group".to_string(),
        }],
        items: vec![
            Aggregation {
                function: AggregateFunction::Collect,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: false,
                name: "values".to_string(),
            },
            Aggregation {
                function: AggregateFunction::Count,
                target: AggregateTarget::Property {
                    variable: "n".to_string(),
                    property: "value".to_string(),
                },
                distinct: true,
                name: "distinct_values".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4 * 1024).unwrap(),
        ..spill_test_config("aggregate-compact-operands")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(output.rows.len(), 4);
    for row in &output.rows {
        assert_eq!(row["distinct_values"], Value::Int(16));
        let Value::List(values) = &row["values"] else {
            panic!("collect must return a list");
        };
        assert_eq!(values.len(), 16);
    }
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "AggregateExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spilled_bytes < 64 * 16 * 1024);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn cartesian_product_spills_an_oversized_build_side() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for value in 0..20 {
        store
            .create_node(
                &mut catalog,
                "Right",
                properties([("value", Value::Int(value))]),
            )
            .unwrap();
    }
    store
        .create_node(&mut catalog, "Left", BTreeMap::new())
        .unwrap();
    let plan = PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "left".to_string(),
            label: "Left".to_string(),
        }),
        right: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "right".to_string(),
            label: "Right".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(1024).unwrap(),
        ..spill_test_config("cartesian-admission")
    };
    let mut external = NoExternalReadOperator;
    let output = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    assert_eq!(output.rows.len(), 20);
    let report = output
        .profile
        .blocking_operator_memory_reports
        .iter()
        .find(|report| report.operator == "NodeCartesianProductExec")
        .unwrap();
    assert!(report.spilled_bytes > 0);
    assert!(report.spill_run_count > 0);
    assert_eq!(report.spilled_rows, 20);
    assert!(report.peak_tracked_bytes <= report.budget_bytes);
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn shortest_path_rejects_an_oversized_frontier() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Node", BTreeMap::new())
        .unwrap();
    for _ in 0..32 {
        let middle = store
            .create_node(&mut catalog, "Node", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, source, middle, "LINK", BTreeMap::new())
            .unwrap();
        store
            .create_relationship(&mut catalog, middle, target, "LINK", BTreeMap::new())
            .unwrap();
    }
    let error = all_shortest_paths(
        &store,
        ShortestPathSearch {
            source,
            target,
            rel_type_id: catalog.rel_type_id("LINK"),
            direction: RelationshipDirection::Outgoing,
            min_hops: 1,
            max_hops: 2,
            path_node_visibility_filter: None,
        },
        NonZeroUsize::new(512).unwrap(),
        usize::MAX,
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("blocking_operator_bytes"));
}

#[test]
fn shortest_path_stream_transfers_accounted_results_to_the_pipeline() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(
            &mut catalog,
            "Node",
            properties([("id", Value::String("source".to_string()))]),
        )
        .unwrap();
    let middle = store
        .create_node(
            &mut catalog,
            "Node",
            properties([("id", Value::String("middle".to_string()))]),
        )
        .unwrap();
    let target = store
        .create_node(
            &mut catalog,
            "Node",
            properties([("id", Value::String("target".to_string()))]),
        )
        .unwrap();
    store
        .create_relationship(&mut catalog, source, middle, "LINK", BTreeMap::new())
        .unwrap();
    store
        .create_relationship(&mut catalog, middle, target, "LINK", BTreeMap::new())
        .unwrap();
    let plan = PhysicalPlan::ShortestPathExec {
        source_variable: "source".to_string(),
        source_label: "Node".to_string(),
        source_id: Value::String("source".to_string()),
        source_visibility_predicate: None,
        rel_type: "LINK".to_string(),
        direction: RelationshipDirection::Outgoing,
        target_variable: "target".to_string(),
        target_label: "Node".to_string(),
        target_id: Value::String("target".to_string()),
        target_visibility_predicate: None,
        min_hops: 1,
        max_hops: 2,
        returns: vec![ShortestPathProjection {
            expression: ShortestPathProjectionExpression::Length,
            name: "length".to_string(),
        }],
    };
    let memory = ExecutionMemoryConfig::default();
    let mut external = NoExternalReadOperator;
    let mut rows = Vec::new();

    let output = execute_with_row_consumer_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
        &memory,
    )
    .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["length"], Value::Int(2));
    assert_eq!(
        output
            .profile
            .pipeline_memory_report
            .query_memory_completion_bytes,
        0
    );
}

#[test]
fn untyped_adjacency_ordering_is_rejected_by_the_query_root_before_collection() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Source", BTreeMap::new())
        .unwrap();
    for ordinal in 0..128 {
        let target = store
            .create_node(
                &mut catalog,
                "Target",
                properties([("id", Value::Int(ordinal))]),
            )
            .unwrap();
        let rel_type = if ordinal & 1 == 0 { "FIRST" } else { "SECOND" };
        store
            .create_relationship(
                &mut catalog,
                source,
                target,
                rel_type,
                properties([("keep", Value::Bool(true))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::AdjacencyExpandExec {
        source_variable: "source".to_string(),
        source_label: "Source".to_string(),
        rel_variable: Some("relationship".to_string()),
        rel_type: String::new(),
        rel_properties: BTreeMap::new(),
        direction: RelationshipDirection::Outgoing,
        target_variable: "target".to_string(),
        target_label: "Target".to_string(),
        min_hops: 1,
        max_hops: 1,
        optional: false,
        graph_budget: None,
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "source".to_string(),
            label: "Source".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(1_024).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(16 * 1_024).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(16 * 1_024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;

    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();

    assert!(error.to_string().contains("query memory ledger"));
    assert!(error.to_string().contains("AdjacencyExpandExec"));

    let filtered_plan = PhysicalPlan::FilterExec {
        predicate: Predicate::PropertyEq {
            variable: "relationship".to_string(),
            property: "keep".to_string(),
            value: Value::Bool(true),
        },
        input: Box::new(plan),
    };
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &filtered_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("query memory ledger"));
    assert!(error.to_string().contains("AdjacencyExpandExec"));
}

#[test]
fn sort_rejects_spill_run_count_over_budget() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..50).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            direction: SortDirection::Asc,
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        max_spill_runs: NonZeroUsize::new(1).unwrap(),
        ..spill_test_config("sort-run-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeded max_spill_runs 1"));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

#[test]
fn sort_rejects_spill_bytes_over_budget_and_removes_partial_run() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    for rank in (0..20).rev() {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([("rank", Value::Int(rank))]),
            )
            .unwrap();
    }
    let plan = PhysicalPlan::SortExec {
        items: vec![SortItem {
            key: SortKey::Property {
                variable: "n".to_string(),
                property: "rank".to_string(),
            },
            direction: SortDirection::Asc,
        }],
        input: Box::new(PhysicalPlan::SeqNodeScan {
            variable: "n".to_string(),
            label: "Item".to_string(),
        }),
    };
    let memory = ExecutionMemoryConfig {
        max_spill_bytes: NonZeroU64::new(32).unwrap(),
        ..spill_test_config("sort-byte-admission")
    };
    let mut external = NoExternalReadOperator;
    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeded max_spill_bytes 32"));
    assert!(std::fs::read_dir(&memory.spill_directory)
        .unwrap()
        .next()
        .is_none());
    std::fs::remove_dir(memory.spill_directory).unwrap();
}

pub(super) fn graph_algorithm_fixture() -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    let source = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(1))]))
        .unwrap();
    let target = store
        .create_node(&mut catalog, "Memory", properties([("id", Value::Int(2))]))
        .unwrap();
    store
        .create_relationship(&mut catalog, source, target, "MENTIONS", BTreeMap::new())
        .unwrap();
    store
        .register_projected_graph(
            "MemoryGraph",
            ProjectedGraphDefinition {
                node_labels: vec!["Memory".to_string()],
                rel_types: vec!["MENTIONS".to_string()],
            },
        )
        .unwrap();
    (catalog, store)
}

pub(super) fn graph_algorithm_plan(algorithm: GraphAlgorithmKind) -> PhysicalPlan {
    PhysicalPlan::GraphAlgorithm {
        algorithm,
        graph_name: "MemoryGraph".to_string(),
        options: crate::planner::GraphAlgorithmOptions {
            damping: None,
            max_iterations: Some(2),
            max_levels: Some(1),
        },
        score_column: "score".to_string(),
        node_visibility_predicate: None,
    }
}

#[test]
fn graph_algorithms_admit_direction_specific_projections() {
    let (mut catalog, mut store) = graph_algorithm_fixture();
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(4096).unwrap(),
        ..spill_test_config("algorithm-admission")
    };

    for algorithm in [GraphAlgorithmKind::PageRank, GraphAlgorithmKind::Louvain] {
        let plan = graph_algorithm_plan(algorithm);
        assert!(BatchPlanRef::try_new(&plan).is_some());
        let mut external = NoExternalReadOperator;
        let output = execute_with_row_limit_profile_and_external_and_memory(
            &plan,
            &mut catalog,
            &mut store,
            &BTreeMap::new(),
            &mut external,
            None,
            &memory,
        )
        .unwrap();
        assert_eq!(output.rows.len(), 2);
        let report = output
            .profile
            .blocking_operator_memory_reports
            .iter()
            .find(|report| report.operator == "GraphAlgorithm")
            .unwrap();
        assert_eq!(report.budget_bytes, 4096);
        assert_eq!(report.input_rows, 2);
        assert!(report.peak_tracked_bytes > 0);
        assert!(report.peak_tracked_bytes <= report.budget_bytes);
        assert_eq!(report.spilled_bytes, 0);
    }
}

#[test]
fn graph_algorithm_rejects_scratch_before_allocation() {
    let (mut catalog, mut store) = graph_algorithm_fixture();
    let plan = graph_algorithm_plan(GraphAlgorithmKind::PageRank);
    let memory = ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(150).unwrap(),
        ..spill_test_config("algorithm-scratch-rejection")
    };
    let mut external = NoExternalReadOperator;

    let error = execute_with_row_limit_profile_and_external_and_memory(
        &plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();

    assert!(error
        .to_string()
        .contains("GraphAlgorithm PageRank scratch and result state"));
    assert!(error
        .to_string()
        .contains("exceeding blocking_operator_bytes 150"));
}

#[test]
fn batch_plan_ref_rejects_an_unsupported_descendant() {
    let plan = PhysicalPlan::FilterExec {
        predicate: Predicate::ConstantBool(true),
        input: Box::new(PhysicalPlan::CreateNode {
            label: "Item".to_string(),
            properties: BTreeMap::new(),
        }),
    };

    assert!(BatchPlanRef::try_new(&plan).is_none());
}

#[test]
fn prepared_physical_plan_separates_streaming_and_materialized_execution() {
    let streaming = PhysicalPlan::SeqNodeScan {
        variable: "node".to_string(),
        label: "Item".to_string(),
    };
    let mutation = PhysicalPlan::CreateNode {
        label: "Item".to_string(),
        properties: BTreeMap::new(),
    };

    let store = GraphStore::in_memory();
    let memory = ExecutionMemoryConfig::default();
    let streaming = PreparedPhysicalPlan::prepare(&streaming, &store, &memory);
    let mutation = PreparedPhysicalPlan::prepare(&mutation, &store, &memory);

    assert_eq!(streaming.execution_mode(), PreparedExecutionMode::Batch);
    assert_eq!(
        mutation.execution_mode(),
        PreparedExecutionMode::Materialized
    );
    assert_eq!(
        streaming.storage_capability(),
        PreparedStorageCapability::InMemory
    );
    assert!(streaming.required_memory().total_bytes > 0);
}

#[test]
fn columnar_numeric_fragment_matches_row_pipeline_and_reports_morsels() {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Int, true);
    let mut store = GraphStore::in_memory();
    for row in 0..513i64 {
        let values = if row % 10 == 0 {
            properties([("name", Value::String(format!("item-{row}")))])
        } else {
            properties([
                ("score", Value::Int(row)),
                ("name", Value::String(format!("item-{row}"))),
            ])
        };
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Float(480.0),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::LimitExec {
        offset: 3,
        limit: Some(17),
        input: Box::new(PhysicalPlan::ProjectExec {
            items: items.clone(),
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: compare.clone(),
                input: Box::new(scan.clone()),
            }),
        }),
    };
    let row_plan = PhysicalPlan::LimitExec {
        offset: 3,
        limit: Some(17),
        input: Box::new(PhysicalPlan::ProjectExec {
            items,
            input: Box::new(PhysicalPlan::FilterExec {
                predicate: Predicate::And(vec![compare]),
                input: Box::new(scan),
            }),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(4).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let morsel_rows = 4 * 16;
    let morsel_count = 513usize.div_ceil(morsel_rows);
    let executor_thread_limit = NonZeroUsize::new(2).unwrap();
    let expected_workers =
        hawdb_executor::SharedExecutorPool::shared_bounded(executor_thread_limit)
            .map(|pool| pool.worker_count())
            .unwrap_or(1)
            .min(MAX_MORSEL_PARALLELISM)
            .min(morsel_count / 4)
            .max(1);
    let task_context = RuntimeTaskContext::default()
        .with_admitted_parallelism(
            NonZeroUsize::new(MAX_MORSEL_PARALLELISM)
                .expect("default morsel parallelism is non-zero"),
        )
        .with_executor_thread_limit(executor_thread_limit);
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_output_limits_profile_and_external_and_context_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &task_context,
        &memory,
    )
    .unwrap();
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert_eq!(columnar.rows.len(), 17);
    let report = &columnar.profile.pipeline_memory_report;
    assert!(report.columnar_batches > 0);
    assert!(report.columnar_input_rows >= report.columnar_selected_rows);
    assert_eq!(report.columnar_batches, report.morsel_count);
    assert_eq!(report.morsel_max_admitted_workers, expected_workers);
    assert_eq!(report.morsel_peak_active_workers, 1);
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);

    let columnar_scan_plan = match &columnar_plan {
        PhysicalPlan::LimitExec { input, .. } => input.as_ref(),
        _ => unreachable!("test plan has a limit root"),
    };
    let row_scan_plan = match &row_plan {
        PhysicalPlan::LimitExec { input, .. } => input.as_ref(),
        _ => unreachable!("test plan has a limit root"),
    };
    let parallel = execute_with_output_limits_profile_and_external_and_context_and_memory(
        columnar_scan_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &task_context,
        &memory,
    )
    .unwrap();
    let sequential = execute_with_row_limit_profile_and_external_and_memory(
        row_scan_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(parallel.rows, sequential.rows);
    assert_eq!(
        parallel
            .profile
            .operator_cardinality_profiles
            .iter()
            .map(|cardinality| (
                cardinality.operator_id.ordinal(),
                cardinality.operator,
                cardinality.actual_rows,
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                0,
                hawdb_plan::PhysicalPlanKind::ProjectExec,
                Some(parallel.rows.len()),
            ),
            (
                1,
                hawdb_plan::PhysicalPlanKind::FilterExec,
                Some(parallel.rows.len()),
            ),
            (2, hawdb_plan::PhysicalPlanKind::SeqNodeScan, Some(513)),
        ]
    );
    assert_eq!(
        parallel
            .profile
            .pipeline_memory_report
            .morsel_max_admitted_workers,
        expected_workers
    );
    assert_eq!(
        parallel
            .profile
            .pipeline_memory_report
            .morsel_peak_active_workers,
        expected_workers
    );
    let parallel_report = &parallel.profile.pipeline_memory_report;
    if expected_workers > 1 {
        assert!((1..=expected_workers).contains(&parallel_report.morsel_peak_buffered_outputs));
        assert!(parallel_report.morsel_peak_buffered_output_bytes > 0);
    }
    assert!(parallel_report.morsel_peak_reorder_entries <= expected_workers);
    assert!(parallel_report.query_memory_peak_bytes < memory.batch_payload_bytes.get());
}

#[test]
fn columnar_numeric_equality_matches_row_pipeline_and_parallelizes() {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Int, true);
    let mut store = GraphStore::in_memory();
    for row in 0..513i64 {
        let values = if row % 11 == 0 {
            BTreeMap::new()
        } else {
            properties([("score", Value::Int(row % 17))])
        };
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    let equality = Predicate::PropertyEq {
        variable: "n".to_string(),
        property: "score".to_string(),
        value: Value::Int(5),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::ProjectExec {
        items: items.clone(),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: equality.clone(),
            input: Box::new(scan.clone()),
        }),
    };
    let row_plan = PhysicalPlan::ProjectExec {
        items,
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::And(vec![equality]),
            input: Box::new(scan),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(4).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let morsel_rows = 4 * 16;
    let morsel_count = 513usize.div_ceil(morsel_rows);
    let memory_workers = memory.query_memory_bytes.get()
        / (memory.batch_payload_bytes.get() + morsel_rows * std::mem::size_of::<&NodeRecord>());
    let expected_workers = hawdb_executor::SharedExecutorPool::shared_default()
        .map(|pool| pool.worker_count())
        .unwrap_or(1)
        .min(MAX_MORSEL_PARALLELISM)
        .min(memory_workers)
        .min(morsel_count / 4)
        .max(1);
    let task_context = RuntimeTaskContext::default().with_admitted_parallelism(
        NonZeroUsize::new(MAX_MORSEL_PARALLELISM).expect("default morsel parallelism is non-zero"),
    );
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_output_limits_profile_and_external_and_context_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        None,
        &task_context,
        &memory,
    )
    .unwrap();
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert!(!columnar.rows.is_empty());
    assert!(columnar
        .rows
        .iter()
        .all(|binding| binding.get("score") == Some(&Value::Int(5))));
    let report = &columnar.profile.pipeline_memory_report;
    assert!(report.columnar_batches > 0);
    assert!(report.columnar_batches > report.morsel_count);
    assert_eq!(report.morsel_count, morsel_count);
    assert_eq!(report.morsel_max_admitted_workers, expected_workers);
    assert_eq!(report.morsel_peak_active_workers, expected_workers);
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);
}

#[test]
fn columnar_lending_fragment_matches_row_for_narrow_numeric_projection() {
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Float, true);
    let mut store = GraphStore::in_memory();
    for row in 0..65i64 {
        let values = if row % 9 == 0 {
            BTreeMap::new()
        } else {
            properties([("score", Value::Float(row as f64 + 0.5))])
        };
        store.create_node(&mut catalog, "Item", values).unwrap();
    }
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Float(48.5),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Literal(Value::String("item".to_string())),
            name: "kind".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::ProjectExec {
        items: items.clone(),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: compare.clone(),
            input: Box::new(scan.clone()),
        }),
    };
    let row_plan = PhysicalPlan::ProjectExec {
        items,
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::And(vec![compare]),
            input: Box::new(scan),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_row_limit_profile_and_external_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert!(columnar.profile.pipeline_memory_report.columnar_batches > 1);
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);
}

#[test]
fn out_of_core_columnar_scan_drops_full_records_before_batching() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hawdb-columnar-owned-{nonce}"));
    let mut catalog = Catalog::default();
    let table = catalog.get_or_create_table(crate::schema::TableKind::Node, "Item");
    catalog.get_or_create_property(table, "score", crate::schema::PropertyType::Int, true);
    let replay_config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    for row in 0..32i64 {
        store
            .create_node(
                &mut catalog,
                "Item",
                properties([
                    ("score", Value::Int(row)),
                    ("payload", Value::String("x".repeat(4096))),
                ]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();
    drop(store);

    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        replay_config,
    )
    .unwrap();
    assert!(store.is_out_of_core());
    let compare = Predicate::PropertyCompare {
        variable: "n".to_string(),
        property: "score".to_string(),
        op: crate::planner::ComparisonOp::Gte,
        value: Value::Int(24),
    };
    let items = vec![
        Projection {
            expression: ProjectionExpression::Id {
                variable: "n".to_string(),
            },
            name: "node_id".to_string(),
        },
        Projection {
            expression: ProjectionExpression::Property {
                variable: "n".to_string(),
                property: "score".to_string(),
            },
            name: "score".to_string(),
        },
    ];
    let scan = PhysicalPlan::SeqNodeScan {
        variable: "n".to_string(),
        label: "Item".to_string(),
    };
    let columnar_plan = PhysicalPlan::ProjectExec {
        items: items.clone(),
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: compare.clone(),
            input: Box::new(scan.clone()),
        }),
    };
    let row_plan = PhysicalPlan::ProjectExec {
        items,
        input: Box::new(PhysicalPlan::FilterExec {
            predicate: Predicate::And(vec![compare]),
            input: Box::new(scan),
        }),
    };
    let memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        batch_payload_bytes: NonZeroUsize::new(1024).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let mut external = NoExternalReadOperator;
    let columnar = execute_with_row_limit_profile_and_external_and_memory(
        &columnar_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap();
    let row_error = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &memory,
    )
    .unwrap_err();
    assert!(row_error.to_string().contains("intermediate row uses"));
    let row_memory = ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::new(8).unwrap(),
        ..ExecutionMemoryConfig::default()
    };
    let row = execute_with_row_limit_profile_and_external_and_memory(
        &row_plan,
        &mut catalog,
        &mut store,
        &BTreeMap::new(),
        &mut external,
        None,
        &row_memory,
    )
    .unwrap();

    assert_eq!(columnar.rows, row.rows);
    assert_eq!(columnar.rows.len(), 8);
    assert_eq!(columnar.profile.pipeline_memory_report.columnar_batches, 4);
    assert_eq!(
        columnar.profile.pipeline_memory_report.columnar_input_rows,
        32
    );
    assert_eq!(row.profile.pipeline_memory_report.columnar_batches, 0);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn node_column_lookup_uses_property_index_pruning_for_exact_label() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::in_memory();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:1".to_string())),
                ("title", Value::String("Graph foundations".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:2".to_string())),
                ("title", Value::String("Storage notes".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            properties([
                ("stable_id", Value::String("memory:3".to_string())),
                ("title", Value::String("Runtime notes".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Seed",
            properties([("target_stable_id", Value::String("memory:2".to_string()))]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Seed",
            properties([("target_stable_id", Value::String("memory:4".to_string()))]),
        )
        .unwrap();

    let plan = PhysicalPlan::ProjectExec {
        items: vec![
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "stable_id".to_string(),
                },
                name: "stable_id".to_string(),
            },
            Projection {
                expression: ProjectionExpression::Property {
                    variable: "m".to_string(),
                    property: "title".to_string(),
                },
                name: "title".to_string(),
            },
        ],
        input: Box::new(PhysicalPlan::NodeColumnLookupExec {
            variable: "m".to_string(),
            label: "Memory".to_string(),
            property: "stable_id".to_string(),
            column: "lookup_id".to_string(),
            optional: true,
            input: Box::new(PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    expression: ProjectionExpression::Property {
                        variable: "s".to_string(),
                        property: "target_stable_id".to_string(),
                    },
                    name: "lookup_id".to_string(),
                }],
                input: Box::new(PhysicalPlan::SeqNodeScan {
                    variable: "s".to_string(),
                    label: "Seed".to_string(),
                }),
            }),
        }),
    };

    let output = execute_with_row_limit_profile(&plan, &mut catalog, &mut store, None).unwrap();

    assert_eq!(output.rows.len(), 2);
    assert_eq!(
        output.rows[0].get("stable_id"),
        Some(&Value::String("memory:2".to_string()))
    );
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("Storage notes".to_string()))
    );
    assert_eq!(output.rows[1].get("stable_id"), Some(&Value::Null));
    assert_eq!(output.rows[1].get("title"), Some(&Value::Null));
    let lookup_scan = output
        .profile
        .scan_pruning_reports
        .iter()
        .find(|report| {
            report.strategy
                == ScanPruningStrategy::PropertyIn {
                    property: "stable_id".to_string(),
                }
        })
        .expect("node column lookup should emit property-in pruning evidence");
    assert_eq!(
        lookup_scan.target_kind,
        crate::store::ScanPruningTargetKind::Node
    );
    assert!(lookup_scan.pruned);
    assert!(!lookup_scan.exact_empty);
    assert_eq!(lookup_scan.candidate_count_before_pruning, 3);
    assert_eq!(lookup_scan.candidate_count_before_filter, 1);
    assert_eq!(lookup_scan.pruned_candidate_count, 2);
    assert_eq!(lookup_scan.output_count, 2);
}

#[test]
fn source_segment_scan_uses_checkpoint_sidecar_and_keeps_filter_semantics() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hawdb-source-segment-executor-{nonce}"));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([
                ("id".to_string(), Value::String("source-a".to_string())),
                ("space_id".to_string(), Value::String("alpha".to_string())),
            ]),
        )
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Source",
            BTreeMap::from([
                ("id".to_string(), Value::String("source-b".to_string())),
                ("space_id".to_string(), Value::String("beta".to_string())),
            ]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();

    let predicate = Predicate::PropertyEq {
        variable: "s".to_string(),
        property: "space_id".to_string(),
        value: Value::String("alpha".to_string()),
    };
    let plan = PhysicalPlan::FilterExec {
        predicate: predicate.clone(),
        input: Box::new(PhysicalPlan::SourceSegmentScan {
            variable: "s".to_string(),
            predicate,
        }),
    };
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let memory = ExecutionMemoryConfig::default();
    let memory_ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let observer = QueryExecutionObserver::default();
    let mut context = ExecutionContext {
        parameters: &parameters,
        external: &mut external,
        memory: &memory,
        memory_ledger: &memory_ledger,
        task_context: None,
        observer: &observer,
    };
    let bindings = execute_bindings_with_limit(
        &plan,
        &mut catalog,
        &mut store,
        &mut context,
        ExecutionLimit::unlimited(),
    )
    .unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(
        bindings[0].nodes["s"].properties["id"],
        Value::String("source-a".to_string())
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn source_segment_scan_limit_reports_planned_candidates_without_false_pruning() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("hawdb-source-segment-limit-{nonce}"));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&path, &mut catalog).unwrap();
    for id in 0..129 {
        store
            .create_node(
                &mut catalog,
                "Source",
                BTreeMap::from([
                    ("id".to_string(), Value::String(format!("source-{id}"))),
                    ("space_id".to_string(), Value::String("alpha".to_string())),
                ]),
            )
            .unwrap();
    }
    store.checkpoint(&catalog).unwrap();

    let predicate = Predicate::PropertyEq {
        variable: "s".to_string(),
        property: "space_id".to_string(),
        value: Value::String("alpha".to_string()),
    };
    let plan = PhysicalPlan::ProjectExec {
        items: vec![Projection {
            expression: ProjectionExpression::Property {
                variable: "s".to_string(),
                property: "id".to_string(),
            },
            name: "id".to_string(),
        }],
        input: Box::new(PhysicalPlan::LimitExec {
            offset: 0,
            limit: Some(1),
            input: Box::new(PhysicalPlan::SourceSegmentScan {
                variable: "s".to_string(),
                predicate,
            }),
        }),
    };

    let output = execute_with_row_limit_profile(&plan, &mut catalog, &mut store, None).unwrap();

    assert_eq!(output.rows.len(), 1);
    let scan = output
        .profile
        .scan_pruning_reports
        .iter()
        .find(|report| {
            report.strategy
                == ScanPruningStrategy::PropertyEq {
                    property: "space_id".to_string(),
                }
        })
        .expect("source segment scan should report pruning");
    assert!(!scan.pruned);
    assert_eq!(scan.candidate_count_before_pruning, 129);
    assert_eq!(scan.candidate_count_before_filter, 129);
    assert_eq!(scan.pruned_candidate_count, 0);
    assert_eq!(scan.output_count, 1);
    std::fs::remove_dir_all(path).unwrap();
}

fn properties(items: impl IntoIterator<Item = (&'static str, Value)>) -> BTreeMap<String, Value> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}
