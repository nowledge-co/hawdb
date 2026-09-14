use super::*;
use skein_sql::SqlStatement;
use std::num::NonZeroUsize;

mod fixtures;
use fixtures::*;

#[test]
fn physical_join_plan_rejects_output_schema_drift() {
    let full_scan = || RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::FullScan,
        name: "__full_scan".to_string(),
        index_columns: Vec::new(),
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: false,
        estimated_rows: 1,
    };
    let join_predicate = match skein_sql::prepare_postgres_sql(
            "SELECT left_table.id FROM left_table INNER JOIN right_table ON left_table.id = right_table.id",
        )
        .expect("parse join predicate")
        .statement
        {
            SqlStatement::Select(select) => select
                .joins
                .into_iter()
                .next()
                .expect("join")
                .on,
            _ => unreachable!("join test must parse as a SELECT"),
        };
    let left = RelationalPhysicalJoinNode::relation(
        BindingId::new(0),
        "left_table".to_string(),
        "left_table".to_string(),
        RelationalPhysicalAccess::Base(RelationalAccessCandidate {
            descriptor: full_scan(),
            access: RelationalBaseAccess::FullScan,
        }),
    );
    let right = RelationalPhysicalJoinNode::relation(
        BindingId::new(1),
        "right_table".to_string(),
        "right_table".to_string(),
        RelationalPhysicalAccess::Probe(RelationalJoinAccessCandidate {
            descriptor: full_scan(),
            access: RelationalJoinAccess::FullScan,
        }),
    );
    let mut plan = RelationalPhysicalJoinPlan::new(
        RelationalPhysicalJoinNode::join(
            RelationalOperatorId::from_plan_index(1),
            SqlJoinKind::Inner,
            vec![join_predicate],
            left,
            right,
        )
        .expect("build physical join"),
        estimate_relational_access_cost(1),
    );
    let RelationalPhysicalJoinNode::Join { output_schema, .. } = &mut plan.root else {
        panic!("expected physical join root");
    };
    *output_schema =
        RelationalPhysicalOutputSchema::relation(BindingId::new(0), "left_table", "left_table");

    let error = plan
        .validate()
        .expect_err("schema drift must fail closed before execution");
    assert!(error.to_string().contains("inconsistent output schema"));
}

fn error<T>(result: Result<T>) -> String {
    result.err().expect("expected rejection").to_string()
}

fn sample(algorithm: usize) -> RelationalPhysicalJoinPlan {
    let input = model(&mut Rng(31), algorithm, 1, &mut 0, &mut 0);
    let (rows, cpu, ..) = input.reference();
    RelationalPhysicalJoinPlan::new(
        input.construct(&statement().joins[0].on),
        PlanCostBreakdown::new(rows, cpu, 0, 0, 0),
    )
}

#[test]
fn borrowed_schema_bindings_preserve_identity_order_and_error_precedence() {
    let schema = RelationalPhysicalOutputSchema::join(
        &RelationalPhysicalOutputSchema::relation(BindingId::new(0), "l", "left"),
        &RelationalPhysicalOutputSchema::relation(BindingId::new(1), "r", "right"),
    )
    .unwrap();
    let binding = |id, table, qualifier| RelationalPhysicalOutputBindingRef {
        binding: BindingId::new(id),
        table,
        qualifier,
    };
    schema
        .ensure_matches([binding(0, "l", "left"), binding(1, "r", "right")].into_iter())
        .unwrap();
    assert!(
        error(schema.ensure_matches([binding(9, "wrong", "wrong")].into_iter()))
            .contains("has 2 bindings but executor produced 1")
    );
    for bindings in [
        [binding(1, "r", "right"), binding(0, "l", "left")],
        [binding(0, "other", "left"), binding(1, "r", "right")],
        [binding(0, "l", "alias"), binding(1, "r", "right")],
    ] {
        assert!(error(schema.ensure_matches(bindings.into_iter())).contains("schema binding"));
    }
    let duplicate = RelationalPhysicalOutputSchema::relation(BindingId::new(0), "another", "alias");
    assert!(
        error(RelationalPhysicalOutputSchema::join(&schema, &duplicate))
            .contains("repeats binding 0")
    );
}

#[test]
fn algorithm_validation_rejects_changed_roles_keys_and_join_kinds() {
    for algorithm in 0..5 {
        let valid = sample(algorithm);
        valid.validate().unwrap();
        validate_prepared_physical_join_plan_accesses(&valid.root, true).unwrap();
        let mut bad = valid.clone();
        let RelationalPhysicalJoinNode::Join {
            equi_join_keys,
            kind,
            ..
        } = &mut bad.root
        else {
            unreachable!()
        };
        if matches!(algorithm, 2 | 3) {
            *equi_join_keys = None;
        } else {
            *equi_join_keys = Some(RelationalEquiJoinKeys {
                columns: Vec::new(),
            });
        }
        if algorithm == 2 {
            *kind = SqlJoinKind::Left;
        }
        assert!(bad.validate().is_err(), "algorithm={algorithm}");
        let mut missing = valid.clone();
        let RelationalPhysicalJoinNode::Join { predicates, .. } = &mut missing.root else {
            unreachable!()
        };
        predicates.clear();
        assert!(error(validate_prepared_physical_join_plan_accesses(
            &missing.root,
            true
        ))
        .contains("has no predicate"));
    }
    let candidate = probe(2, true);
    let wrong_base = RelationalPhysicalJoinNode::relation(
        BindingId::new(0),
        "l".into(),
        "l".into(),
        RelationalPhysicalAccess::Probe(candidate),
    );
    assert!(error(validate_prepared_physical_join_plan_accesses(
        &wrong_base,
        true
    ))
    .contains("invalid access role"));
}

#[test]
fn profiles_reject_invalid_ids_duplicates_and_cost_drift() {
    let valid = sample(4);
    planned_tree_operator_cardinality_profiles(&valid).unwrap();
    let mut cost = valid.clone();
    cost.cost_breakdown.cpu ^= 1;
    assert!(error(planned_tree_operator_cardinality_profiles(&cost)).contains("estimates diverge"));
    let mut outside = valid.clone();
    let RelationalPhysicalJoinNode::Join { operator_id, .. } = &mut outside.root else {
        unreachable!()
    };
    *operator_id = RelationalOperatorId::from_plan_index(usize::MAX);
    assert!(error(planned_tree_operator_cardinality_profiles(&outside))
        .contains("outside the plan profile"));
    let mut duplicate = valid.clone();
    let RelationalPhysicalJoinNode::Join { operator_id, .. } = &mut duplicate.root else {
        unreachable!()
    };
    *operator_id = RelationalOperatorId::from_plan_index(0);
    assert!(
        error(planned_tree_operator_cardinality_profiles(&duplicate))
            .contains("repeats operator 1")
    );
}

#[test]
fn access_descriptor_validation_preserves_primary_index_and_full_scan_rules() {
    for candidate in [base(0, false), base(9, true), primary_key()] {
        assert!(base_access_matches_descriptor(&candidate));
        let mut mismatch = candidate.clone();
        mismatch.descriptor.equality_prefix_len += 1;
        assert!(!base_access_matches_descriptor(&mismatch));
    }
    for candidate in [probe(2, false), probe(3, true)] {
        assert!(join_access_matches_descriptor(&candidate));
        let mut mismatch = candidate.clone();
        mismatch.descriptor.equality_prefix_len += 1;
        assert!(!join_access_matches_descriptor(&mismatch));
    }
}

#[test]
fn finalization_preserves_mode_specific_merge_gate_and_hash_fallback() {
    let state = state();
    let reader = Reader::default();
    for (index, mode) in modes(&reader).into_iter().enumerate() {
        for indexed in [false, true] {
            let mut plan = access_plan(indexed);
            plan.finalize_physical_join_plan(&statement(), &state, mode)
                .unwrap();
            let tree = plan.physical_join_plan().unwrap();
            let RelationalPhysicalJoinNode::Join { algorithm, .. } = tree.root else {
                unreachable!()
            };
            let expected = if !indexed {
                RelationalPhysicalJoinAlgorithm::Hash
            } else if index < 2 {
                RelationalPhysicalJoinAlgorithm::Merge
            } else {
                RelationalPhysicalJoinAlgorithm::BatchedIndex
            };
            assert_eq!(algorithm, expected, "mode={index} indexed={indexed}");
            let before = format!("{tree:?}");
            plan.finalize_physical_join_plan(&statement(), &RelationalState::default(), mode)
                .unwrap();
            assert_eq!(format!("{:?}", plan.physical_join_plan().unwrap()), before);
            prepared(statement(), plan).validate().unwrap();
        }
    }
}

#[test]
fn merge_eligibility_checks_direction_prefix_and_complete_order_keys() {
    let state = state();
    let plan = access_plan(true);
    assert!(merge_join_inputs(&state, &plan.base_access, &plan.join_accesses[0], "r").is_some());
    for field in 0..4 {
        let mut candidate = plan.base_access.clone();
        match field {
            0 => candidate.descriptor.reverse_order = true,
            1 => candidate.descriptor.index_columns = vec!["other".into()],
            2 => candidate.descriptor.equality_prefix_len = 1,
            3 => {
                let RelationalBaseAccess::Index { scan, .. } = &mut candidate.access else {
                    unreachable!()
                };
                scan.direction = RelationalIndexScanDirection::Backward;
            }
            _ => unreachable!(),
        }
        assert!(merge_join_inputs(&state, &candidate, &plan.join_accesses[0], "r").is_none());
    }
    let mut right = plan.join_accesses[0].clone();
    right.descriptor.index_columns.clear();
    assert!(merge_join_inputs(&state, &plan.base_access, &right, "r").is_none());
}

#[test]
fn ndv_uses_fresh_metadata_or_complete_non_null_unique_keys_without_scanning() {
    let state = state();
    let reader = Reader::default();
    let relation = RelationalPhysicalRelation::new(
        BindingId::new(0),
        "l".into(),
        "l".into(),
        RelationalPhysicalAccess::Base(base(3, false)),
    );
    for mode in modes(&reader) {
        assert_eq!(
            relational_join_distinct_values(&state, mode, &relation, &["id".into()]),
            Some(3)
        );
        assert_eq!(
            relational_join_distinct_values(&state, mode, &relation, &["n".into()]),
            None
        );
        assert_eq!(
            relational_join_distinct_values(&state, mode, &relation, &[]),
            None
        );
        assert_eq!(
            relational_join_distinct_values(&state, mode, &relation, &["unknown".into()]),
            None
        );
    }
    let reader = Reader {
        statistics: Some(
            skein_storage::relational_index_view::RelationalIndexProbeStatistics {
                source_commit_epoch: 4,
                distinct_non_null_values: 2,
                non_null_rows: 3,
                fanout: 2,
            },
        ),
        ..Default::default()
    };
    for mode in [
        Mode::Shadow(&reader),
        Mode::DemandPaged(&reader),
        Mode::Authoritative(&reader),
    ] {
        assert_eq!(
            relational_join_distinct_values(&state, mode, &relation, &["k".into()]),
            Some(2)
        );
    }
    assert_eq!(
        *reader.calls.borrow(),
        vec![("l".into(), "l_k".into(), 1); 3]
    );
}

#[test]
fn index_coverage_tracks_required_fields_and_rejects_missing_schema() {
    let state = state();
    for (sql, expected) in [("SELECT k FROM l", true), ("SELECT n FROM l", false)] {
        let fields = crate::field_plan::plan_relational_field_plan(&select(sql), &state).unwrap();
        let mut node = RelationalPhysicalJoinNode::relation(
            BindingId::new(0),
            "l".into(),
            "l".into(),
            RelationalPhysicalAccess::Base(base(3, true)),
        );
        node.apply_index_coverage(&state, &fields).unwrap();
        let descriptor = node.first_relation().access.descriptor();
        assert_eq!(descriptor.covering, expected);
        assert_eq!(descriptor.requires_row_fetch, !expected);
        assert!(
            error(node.apply_index_coverage(&RelationalState::default(), &fields))
                .contains("unknown relational table l")
        );
    }
}

#[test]
fn execution_mode_and_memory_shape_preserve_order_aggregate_and_distinct_boundaries() {
    for (sql, expected, blocking) in [
        (
            "SELECT k FROM l",
            PreparedRelationalExecutionMode::StreamingProjection,
            0,
        ),
        (
            "SELECT k FROM l ORDER BY k",
            PreparedRelationalExecutionMode::OrderedIndexProjection,
            0,
        ),
        (
            "SELECT k FROM l WHERE n > 1 ORDER BY k",
            PreparedRelationalExecutionMode::BlockingProjection,
            1,
        ),
        (
            "SELECT DISTINCT k FROM l ORDER BY k",
            PreparedRelationalExecutionMode::BlockingProjection,
            2,
        ),
        (
            "SELECT COUNT(*) FROM l",
            PreparedRelationalExecutionMode::Aggregate,
            1,
        ),
        (
            "SELECT k, COUNT(*) FROM l GROUP BY k",
            PreparedRelationalExecutionMode::Aggregate,
            2,
        ),
    ] {
        let plan = PreparedRelationalAccessPlan {
            join_accesses: Vec::new(),
            ..access_plan(true)
        };
        let execution =
            PreparedRelationalExecutionDescriptor::prepare(&select(sql), &plan).unwrap();
        assert_eq!(execution.mode, expected, "{sql}");
        assert_eq!(
            execution.memory_shape,
            RelationalExecutionMemoryShape {
                pipeline_batch_count: 1,
                blocking_operator_count: blocking
            }
        );
    }
    let shape = RelationalExecutionMemoryShape {
        pipeline_batch_count: usize::MAX,
        blocking_operator_count: usize::MAX,
    };
    assert_eq!(shape.estimated_bytes(&Default::default()), usize::MAX);
    let memory = skein_executor::ExecutionMemoryConfig {
        batch_payload_bytes: NonZeroUsize::new(7).unwrap(),
        blocking_operator_bytes: NonZeroUsize::new(11).unwrap(),
        ..Default::default()
    };
    assert_eq!(
        RelationalExecutionMemoryShape {
            pipeline_batch_count: 2,
            blocking_operator_count: 3
        }
        .estimated_bytes(&memory),
        47
    );
}

#[test]
fn prepared_validation_rejects_divergent_statement_access_and_execution_before_running() {
    let state = state();
    let make = || {
        let mut plan = access_plan(false);
        plan.finalize_physical_join_plan(&statement(), &state, Mode::Materialized)
            .unwrap();
        prepared(statement(), plan)
    };
    make().validate().unwrap();
    let mut bad = make();
    bad.access_plan.join_accesses.clear();
    assert!(error(bad.validate()).contains("1 joins but 0 join access paths"));
    let mut bad = make();
    bad.access_plan.base_access.descriptor.equality_prefix_len = 1;
    assert!(error(bad.validate()).contains("inconsistent base access path"));
    let mut bad = make();
    bad.execution.memory_shape.pipeline_batch_count += 1;
    assert!(error(bad.validate()).contains("inconsistent execution descriptor"));
    let mut bad = make();
    bad.access_plan.physical_join_plan = None;
    assert!(error(bad.validate()).contains("no finalized physical join plan"));
}

#[test]
fn authoritative_transaction_statistics_do_not_enable_materialized_merge() {
    use skein_storage::relational_index_view::{
        RelationalIndexReadView, RelationalTransactionIndexView,
    };
    use skein_storage::{RelationalIndexShadowReader, RelationalIndexShadowWriter};
    let state = state();
    let directory = std::env::temp_dir().join(format!(
        "skein-physical-plan-{}",
        skein_core::generate_uuidv7().unwrap(),
    ));
    std::fs::create_dir(&directory).unwrap();
    RelationalIndexShadowWriter::new(Default::default())
        .publish(&directory, &state, 1, 4, None)
        .unwrap();
    let view = Arc::new(RelationalIndexReadView::from_base(
        RelationalIndexShadowReader::open(&directory, 1, 4, Default::default()).unwrap(),
    ));
    let transaction =
        RelationalTransactionIndexView::new(view, Default::default(), Default::default());
    let mode = Mode::AuthoritativeTransaction(&transaction);
    let mut plan = access_plan(true);
    plan.finalize_physical_join_plan(&statement(), &state, mode)
        .unwrap();
    let RelationalPhysicalJoinNode::Join { algorithm, .. } =
        plan.physical_join_plan().unwrap().root
    else {
        unreachable!()
    };
    assert_eq!(algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
    let relation = RelationalPhysicalRelation::new(
        BindingId::new(0),
        "l".into(),
        "l".into(),
        RelationalPhysicalAccess::Base(base(3, false)),
    );
    assert_eq!(
        relational_join_distinct_values(&state, mode, &relation, &["k".into()]),
        Some(2)
    );
    prepared(statement(), plan).validate().unwrap();
    drop(transaction);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn planning_helpers_preserve_alias_errors_and_do_not_invent_subtree_statistics() {
    let resolved = crate::field_plan::resolved_access_order_by(&select(
        "SELECT k AS key FROM l ORDER BY key DESC",
    ))
    .unwrap();
    assert_eq!(resolved[0].expression.require_column().unwrap().name, "k");
    assert!(crate::field_plan::resolved_access_order_by(&select(
        "SELECT COALESCE(k, 0) AS key FROM l ORDER BY key"
    ))
    .unwrap()
    .is_empty());
    assert!(error(crate::field_plan::resolved_access_order_by(&select(
        "SELECT k AS key, id AS key FROM l ORDER BY key"
    )))
    .contains("ambiguous relational ORDER BY alias key"));
    let subtree = sample(4);
    let keys = RelationalEquiJoinKeys {
        columns: vec![(
            "k".into(),
            SqlColumnRef {
                qualifier: Some("l".into()),
                name: "k".into(),
            },
        )],
    };
    let reader = Reader::default();
    assert_eq!(
        materialized_equi_join_selectivity(
            &state(),
            Mode::Shadow(&reader),
            &subtree.root,
            &subtree.root,
            &keys
        ),
        RelationalJoinSelectivity::Unknown,
    );
    assert!(reader.calls.borrow().is_empty());
}

#[test]
fn selected_binding_and_cost_invariants_remain_fail_closed() {
    let state = state();
    let make = || {
        let mut plan = access_plan(false);
        plan.finalize_physical_join_plan(&statement(), &state, Mode::Materialized)
            .unwrap();
        let cost = plan.physical_join_plan().unwrap().cost_breakdown;
        plan.join_selection = Some(PreparedRelationalJoinSelection {
            base_binding: BindingId::new(0),
            join_bindings: vec![BindingId::new(1)],
            cost_breakdown: cost,
        });
        prepared(statement(), plan)
    };
    make().validate().unwrap();
    let mut missing = make();
    missing
        .access_plan
        .join_selection
        .as_mut()
        .unwrap()
        .join_bindings
        .clear();
    assert!(error(missing.validate()).contains("0 bindings for 1 joins"));
    let mut duplicate = make();
    duplicate
        .access_plan
        .join_selection
        .as_mut()
        .unwrap()
        .join_bindings[0] = BindingId::new(0);
    assert!(error(duplicate.validate()).contains("duplicate bindings"));
    let mut bad_cost = make();
    bad_cost
        .access_plan
        .join_selection
        .as_mut()
        .unwrap()
        .cost_breakdown
        .cost ^= 1;
    assert!(error(bad_cost.validate()).contains("invalid cost breakdown"));
}

#[test]
fn composite_unique_key_statistics_require_the_complete_non_null_key() {
    let mut state = state();
    for sql in [
        "CREATE TABLE composite (a BIGINT, b BIGINT, c BIGINT, PRIMARY KEY (a, b))",
        "INSERT INTO composite (a, b, c) VALUES (1, 2, NULL), (1, 3, 4)",
    ] {
        let tx = crate::compile_relational_statement_sql(sql, &[], &state).unwrap();
        state = state
            .stage_transaction(tx, Default::default(), Default::default())
            .unwrap();
    }
    let relation = RelationalPhysicalRelation::new(
        BindingId::new(0),
        "composite".into(),
        "c".into(),
        RelationalPhysicalAccess::Base(base(2, false)),
    );
    assert_eq!(
        relational_join_distinct_values(&state, Mode::Materialized, &relation, &["a".into()]),
        None
    );
    for columns in [vec!["a".into(), "b".into()], vec!["b".into(), "a".into()]] {
        assert_eq!(
            relational_join_distinct_values(&state, Mode::Materialized, &relation, &columns),
            Some(2)
        );
    }
}

fn campaign(seeds: u64, cases: usize) -> usize {
    let predicate = statement().joins.remove(0).on;
    let mut checks = 0;
    for seed in 1..=seeds {
        let mut rng = Rng(seed.wrapping_mul(0x9e3779b97f4a7c15));
        for case in 0..cases {
            for algorithm in 0..5 {
                let input = model(&mut rng, algorithm, case % 4, &mut 0, &mut 0);
                let (rows, cpu, batches, materialized, ids) = input.reference();
                let tree = RelationalPhysicalJoinPlan::new(
                    input.construct(&predicate),
                    PlanCostBreakdown::new(rows, cpu, 0, 0, 0),
                );
                tree.validate().unwrap();
                validate_prepared_physical_join_plan_accesses(&tree.root, true).unwrap();
                let actual = planned_tree_operator_cardinality_profiles(&tree).unwrap();
                let mut expected = vec![None; ids.len()];
                let (binding, estimated_rows, indexed, _) = input.first();
                expected[0] = Some(RelationalOperatorCardinalityProfile {
                    operator_id: RelationalOperatorId::from_plan_index(0),
                    operator: if indexed {
                        RelationalOperatorKind::IndexRangeScan
                    } else {
                        RelationalOperatorKind::TableFullScan
                    },
                    table: format!("t{binding}"),
                    access_path: base(estimated_rows, indexed).descriptor,
                    estimated_rows,
                    actual_rows: None,
                    fully_consumed: false,
                });
                input.expected_profiles(&mut expected);
                assert_eq!(
                    actual,
                    expected.into_iter().map(Option::unwrap).collect::<Vec<_>>(),
                    "seed={seed} case={case} algorithm={algorithm}"
                );
                assert_eq!(tree.root.relation_count(), ids.len());
                assert_eq!(
                    tree.output_schema
                        .bindings
                        .iter()
                        .map(|b| b.binding.get())
                        .collect::<Vec<_>>(),
                    ids
                );
                assert_eq!(tree.root.batched_probe_depth(), batches);
                assert_eq!(tree.root.materialized_right_count(), materialized);
                let plan = PreparedRelationalAccessPlan {
                    physical_join_plan: Some(tree),
                    ..access_plan(false)
                };
                let descriptor =
                    PreparedRelationalExecutionDescriptor::prepare(&statement(), &plan).unwrap();
                assert_eq!(
                    descriptor.memory_shape,
                    RelationalExecutionMemoryShape {
                        pipeline_batch_count: 1 + batches,
                        blocking_operator_count: materialized
                    }
                );
                checks += 1;
            }
        }
    }
    checks
}

#[test]
fn physical_plan_differential_smoke() {
    assert_eq!(campaign(4, 8), 160);
}

#[test]
#[ignore = "explicit deterministic physical-plan campaign"]
fn physical_plan_differential_campaign() {
    let checks = campaign(128, 64);
    assert_eq!(checks, 40_960);
    println!("skein-relational-physical-plan-fuzz-v1: 128 seeds, 8192 cases, {checks} complete plan outcomes");
}
