// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::optimizer::{
    OptimizerBoundedOrderAlternative, OptimizerCostPerturbation, OptimizerQualificationOptions,
    OptimizerRuleAssertion, OptimizerSearchDirective, OptionalOptimizerRule,
    DEFAULT_PLAN_COST_POLICY_ID, DEFAULT_PLAN_COST_WEIGHTS,
};

fn fixture(rows: i64) -> Database {
    fixture_with_config(rows, DatabaseConfig::default())
}
fn fixture_with_config(rows: i64, config: DatabaseConfig) -> Database {
    let mut db = Database::new_with_config(config);
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(external) TYPE INT NOT NULL")
        .unwrap();
    db.query("CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE")
        .unwrap();
    let values = (0..rows)
        .map(|value| Value::Map(BTreeMap::from([("value".into(), Value::Int(value))])))
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {external:r.value})",
        &BTreeMap::from([("rows".into(), Value::List(values))]),
    )
    .unwrap();
    db
}

#[test]
fn optimizer_qualification_compares_real_plans_on_the_same_statement_and_snapshot() {
    let mut db = fixture(40);
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id";
    let base = read
        .qualify_optimizer_query(
            crate::QueryRequest::new(query),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    base.verify_rules(&[
        OptimizerRuleAssertion::Required(OptionalOptimizerRule::RedundantDistinct),
        OptimizerRuleAssertion::Forbidden(OptionalOptimizerRule::SingletonGroup),
    ])
    .unwrap();
    let disabled = read
        .qualify_optimizer_query(
            crate::QueryRequest::new(query),
            OptimizerQualificationOptions::default()
                .without_rule(OptionalOptimizerRule::RedundantDistinct),
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    disabled
        .verify_rules(&[OptimizerRuleAssertion::Forbidden(
            OptionalOptimizerRule::RedundantDistinct,
        )])
        .unwrap();
    assert!(disabled
        .verify_rules(&[OptimizerRuleAssertion::Required(
            OptionalOptimizerRule::RedundantDistinct
        )])
        .is_err());
    assert_ne!(
        base.optimizer_trace.selected_plan_fingerprint,
        disabled.optimizer_trace.selected_plan_fingerprint
    );
    assert_eq!(
        base.optimizer_trace.query_digest,
        disabled.optimizer_trace.query_digest
    );
    assert_eq!(base.snapshot_epoch, disabled.snapshot_epoch);
    let expected = (0..40)
        .map(|value| BTreeMap::from([("id".into(), Value::Int(value))]))
        .collect::<Vec<_>>();
    for result in [base.result.unwrap(), disabled.result.unwrap()] {
        assert_eq!(result.output.rows, expected);
        // Owned output remains charged until its returned rows are dropped.
        assert!(
            result
                .execution_profile
                .pipeline_memory_report
                .query_memory_completion_bytes
                > 0
        );
    }
    db.query("CREATE (:Item {external:999})").unwrap();
    let pinned = read
        .qualify_optimizer_query(
            crate::QueryRequest::new(query),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::DirectFallback,
        )
        .unwrap();
    assert_eq!(pinned.result.unwrap().output.rows, expected);
    let before = read.plan_cache_stats();
    read.query(query).unwrap();
    read.query(query).unwrap();
    assert!(
        read.plan_cache_stats().hits > before.hits,
        "ordinary cache path remains usable after qualification"
    );
}

#[test]
fn optimizer_qualification_preserves_runtime_errors_and_refuses_mutations() {
    let db = fixture(12);
    let mut read = db.begin_read_transaction().unwrap();
    let query="MATCH (n:Item) RETURN DISTINCT n.external AS id, 1 / (n.external - 7) AS error ORDER BY id";
    let mut errors = Vec::new();
    for options in [
        OptimizerQualificationOptions::default(),
        OptimizerQualificationOptions::default()
            .without_rule(OptionalOptimizerRule::RedundantDistinct),
    ] {
        let result = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Auto,
            )
            .unwrap();
        result
            .verify_rules(&[OptimizerRuleAssertion::Forbidden(
                OptionalOptimizerRule::RedundantDistinct,
            )])
            .unwrap();
        let error = result.result.unwrap_err().to_string();
        assert!(error.contains("division by zero"), "{error}");
        errors.push(error);
    }
    assert_eq!(errors[0], errors[1]);
    assert!(read
        .qualify_optimizer_query(
            crate::QueryRequest::new("CREATE (:Item {external:1000})"),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::Auto
        )
        .is_err());
    assert_eq!(
        read.query("MATCH (n:Item) RETURN count(n) AS total")
            .unwrap()
            .rows[0]["total"],
        Value::Int(12)
    );
}

#[test]
fn semantic_proof_cache_tracks_publication_state_and_keeps_old_reader_proofs() {
    let mut db = Database::new_with_config(DatabaseConfig {
        optimizer_statistics_inline_commit_lag: Some(u64::MAX),
        ..Default::default()
    });
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(external) TYPE STRING NOT NULL")
        .unwrap();
    db.query("CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE")
        .unwrap();
    db.query("CREATE (:Item {external:'a'})").unwrap();
    db.query("CREATE (:Item {external:'b'})").unwrap();
    let query = "MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id";
    let mut old = db.begin_read_transaction().unwrap();
    assert!(!old
        .explain_query(query)
        .unwrap()
        .trace
        .selected_plan
        .contains("DistinctExec"));
    assert_eq!(old.query(query).unwrap().rows.len(), 2);
    db.query("ALTER PROPERTY ON NODE TABLE Item(external) SET STATE BACKFILL")
        .unwrap();
    db.query("CREATE (:Item {})").unwrap();
    db.query("CREATE (:Item {})").unwrap();
    // An old reader may publish its valid old-schema plan after the DDL.
    assert_eq!(old.query(query).unwrap().rows.len(), 2);
    let mut current = db.begin_read_transaction().unwrap();
    assert!(current
        .explain_query(query)
        .unwrap()
        .trace
        .selected_plan
        .contains("DistinctExec"));
    let output = current.query(query).unwrap();
    assert_eq!(output.rows.len(), 3);
    assert_eq!(
        output
            .rows
            .iter()
            .filter(|row| row["id"] == Value::Null)
            .count(),
        1
    );
}

#[test]
fn seeded_ranking_changes_legal_bounded_plans_without_changing_reported_costs() {
    let db = fixture(100);
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) RETURN n.external AS id ORDER BY id LIMIT 30";
    let expected = (0..30)
        .map(|value| BTreeMap::from([("id".into(), Value::Int(value))]))
        .collect::<Vec<_>>();
    let mut fingerprints = BTreeSet::new();
    let mut raw_costs = BTreeMap::new();
    for seed in 0..24 {
        let options = OptimizerQualificationOptions::default()
            .with_cost_perturbation(OptimizerCostPerturbation::new(seed, 25).unwrap());
        let result = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        let fingerprint = result.optimizer_trace.selected_plan_fingerprint.clone();
        let raw_cost = result.optimizer_trace.selected_plan_cost;
        if let Some(previous) = raw_costs.insert(fingerprint.clone(), raw_cost) {
            assert_eq!(previous, raw_cost, "ranking noise must not alter raw costs");
        }
        assert_eq!(result.result.unwrap().output.rows, expected);
        let replay = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        assert_eq!(
            replay.optimizer_trace.selected_plan_fingerprint,
            fingerprint
        );
        assert_eq!(replay.optimizer_trace.selected_plan_cost, raw_cost);
        assert_eq!(replay.result.unwrap().output.rows, expected);
        fingerprints.insert(fingerprint);
    }
    assert!(
        fingerprints.len() >= 2,
        "perturbation must exercise distinct executable alternatives"
    );
}

#[test]
fn bounded_order_candidate_inventory_executes_distinct_legal_plans_on_one_snapshot() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(id) TYPE INT NOT NULL")
        .unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(sort_key) TYPE INT NOT NULL")
        .unwrap();
    let rows = (0..64)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("sort_key".into(), Value::Int(id % 7)),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {id:r.id, sort_key:r.sort_key})",
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .unwrap();
    let mut read = db.begin_read_transaction().unwrap();
    let query =
        "MATCH (n:Item) RETURN n.sort_key AS sort_key, n.id AS id ORDER BY sort_key, id LIMIT 30";
    let campaign = crate::RuntimeTaskContext::with_timeout(std::time::Duration::from_secs(10))
        .with_work_limit(192);
    let attempt_limits =
        crate::OptimizerQualificationAttemptLimits::new(std::time::Duration::from_secs(5), 64)
            .unwrap();
    let candidates = read
        .qualify_optimizer_bounded_order_candidates(
            crate::QueryRequest::new(query).with_task_context(&campaign),
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap();

    assert_eq!(candidates.snapshot_epoch, read.commit_epoch());
    assert_eq!(candidates.candidate_limit, 8);
    assert_eq!(campaign.work_units(), 192);
    assert_eq!(candidates.candidates.len(), 3);
    assert_eq!(candidates.distinct_candidate_count, 2);
    assert_eq!(
        candidates.retained_result_bytes,
        candidates
            .candidates
            .iter()
            .filter_map(|candidate| candidate.output.result.as_ref().ok())
            .map(|result| result.output.rows.retained_memory_bytes())
            .sum::<usize>()
    );
    assert!(candidates.retained_result_bytes > 0);
    assert!(candidates.retained_result_bytes <= candidates.result_memory_limit);
    assert_eq!(candidates.candidates[0].same_plan_as, None);
    assert!(candidates.candidates[1..]
        .iter()
        .any(|candidate| candidate.same_plan_as == Some(0)));
    assert_eq!(
        candidates.candidates[1].kind,
        crate::OptimizerQualificationCandidateKind::BoundedOrder(
            OptimizerBoundedOrderAlternative::TopN
        )
    );
    assert_eq!(
        candidates.candidates[2].kind,
        crate::OptimizerQualificationCandidateKind::BoundedOrder(
            OptimizerBoundedOrderAlternative::SortThenLimit
        )
    );
    assert!(candidates.candidates[1]
        .output
        .selected_plan_instance_fingerprint
        .contains("TopNExec"));
    assert!(candidates.candidates[2]
        .output
        .selected_plan_instance_fingerprint
        .contains("SortExec"));
    for candidate in &candidates.candidates {
        assert_eq!(candidate.output.snapshot_epoch, candidates.snapshot_epoch);
        assert_eq!(candidate.output.work_units, 64);
        assert_eq!(candidate.output.cost_policy_id, DEFAULT_PLAN_COST_POLICY_ID);
        assert_eq!(candidate.output.cost_weights, DEFAULT_PLAN_COST_WEIGHTS);
        assert_eq!(
            candidate
                .output
                .optimizer_trace
                .selected_plan_cost_breakdown
                .score_with_weights(candidate.output.cost_weights),
            candidate.output.optimizer_trace.selected_plan_cost
        );
    }
    candidates.verify_complete_results().unwrap();
    let expected = (0..7)
        .flat_map(|sort_key| {
            (0..64).filter(move |id| id % 7 == sort_key).map(move |id| {
                BTreeMap::from([
                    ("sort_key".into(), Value::Int(sort_key)),
                    ("id".into(), Value::Int(id)),
                ])
            })
        })
        .take(30)
        .collect::<Vec<_>>();
    assert_eq!(
        candidates.candidates[0]
            .output
            .result
            .as_ref()
            .unwrap()
            .output
            .rows,
        expected
    );

    let reordered_campaign =
        crate::RuntimeTaskContext::with_timeout(std::time::Duration::from_secs(10))
            .with_work_limit(192);
    let reordered = read
        .qualify_optimizer_bounded_order_candidate_requests(
            crate::QueryRequest::new(query).with_task_context(&reordered_campaign),
            &[
                crate::OptimizerQualificationCandidateRequest::new(
                    crate::OptimizerQualificationCandidateKind::BoundedOrder(
                        OptimizerBoundedOrderAlternative::SortThenLimit,
                    ),
                ),
                crate::OptimizerQualificationCandidateRequest::new(
                    crate::OptimizerQualificationCandidateKind::Ordinary,
                ),
                crate::OptimizerQualificationCandidateRequest::new(
                    crate::OptimizerQualificationCandidateKind::BoundedOrder(
                        OptimizerBoundedOrderAlternative::TopN,
                    ),
                ),
            ],
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    assert_eq!(reordered_campaign.work_units(), 192);
    assert!(reordered
        .candidates
        .iter()
        .all(|candidate| candidate.output.work_units == 64));
    assert_eq!(
        reordered.retained_result_bytes,
        candidates.retained_result_bytes
    );
    reordered.verify_complete_results().unwrap();
}

#[test]
fn bounded_order_candidate_inventory_enforces_one_cumulative_result_budget() {
    let db = fixture(64);
    let mut read = db.begin_read_transaction().unwrap();
    let query = "MATCH (n:Item) RETURN n.external AS id ORDER BY id LIMIT 30";
    let probe_task = crate::RuntimeTaskContext::default()
        .with_memory_reservation(crate::RuntimeMemoryReservation::new(64 << 20, 1 << 20));
    let probe = read
        .qualify_optimizer_query(
            crate::QueryRequest::new(query).with_task_context(&probe_task),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    let one_result_bytes = probe
        .result
        .as_ref()
        .unwrap()
        .output
        .rows
        .retained_memory_bytes();
    assert!(one_result_bytes > 0);
    drop(probe);

    let mut minimum_attempt_result_bytes = one_result_bytes;
    let mut upper = 1usize << 20;
    while minimum_attempt_result_bytes < upper {
        let candidate = minimum_attempt_result_bytes + (upper - minimum_attempt_result_bytes) / 2;
        let task = crate::RuntimeTaskContext::default().with_memory_reservation(
            crate::RuntimeMemoryReservation::new(64 << 20, u64::try_from(candidate).unwrap()),
        );
        let output = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query).with_task_context(&task),
                OptimizerQualificationOptions::default(),
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        let completed = output.result.is_ok();
        drop(output);
        if completed {
            upper = candidate;
        } else {
            minimum_attempt_result_bytes = candidate + 1;
        }
    }
    let campaign_result_bytes = minimum_attempt_result_bytes
        .checked_add(one_result_bytes)
        .unwrap();
    let campaign = crate::RuntimeTaskContext::with_timeout(std::time::Duration::from_secs(10))
        .with_work_limit(30_000_000)
        .with_memory_reservation(crate::RuntimeMemoryReservation::new(
            64 << 20,
            u64::try_from(campaign_result_bytes).unwrap(),
        ));
    let candidates = read
        .qualify_optimizer_bounded_order_candidates(
            crate::QueryRequest::new(query).with_task_context(&campaign),
            crate::OptimizerQualificationAttemptLimits::new(
                std::time::Duration::from_secs(5),
                10_000_000,
            )
            .unwrap(),
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    assert_eq!(candidates.result_memory_limit, campaign_result_bytes);
    assert_eq!(
        candidates.retained_result_bytes,
        one_result_bytes.checked_mul(2).unwrap()
    );
    assert!(candidates.candidates[0].output.result.is_ok());
    assert!(candidates.candidates[1].output.result.is_ok());
    let error = candidates.candidates[2].output.result.as_ref().unwrap_err();
    assert!(error.to_string().contains("result_materialization"));
    assert!(candidates.verify_complete_results().is_err());
}

#[test]
fn explicit_bounded_order_candidate_refuses_missing_and_ambiguous_controls() {
    let db = fixture(12);
    let mut read = db.begin_read_transaction().unwrap();
    let request = crate::QueryRequest::new("MATCH (n:Item) RETURN n.external AS id");
    let missing = read
        .qualify_optimizer_query(
            request,
            OptimizerQualificationOptions::default()
                .with_bounded_order_alternative(OptimizerBoundedOrderAlternative::TopN),
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(missing
        .to_string()
        .contains("found 0 legal selection sites"));

    let conflicting = read
        .qualify_optimizer_query(
            request,
            OptimizerQualificationOptions::default()
                .with_cost_perturbation(OptimizerCostPerturbation::new(7, 25).unwrap())
                .with_bounded_order_alternative(OptimizerBoundedOrderAlternative::TopN),
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(conflicting
        .to_string()
        .contains("cannot combine cost perturbation"));

    let ordinary = crate::OptimizerQualificationCandidateRequest::new(
        crate::OptimizerQualificationCandidateKind::Ordinary,
    );
    let top_n = crate::OptimizerQualificationCandidateRequest::new(
        crate::OptimizerQualificationCandidateKind::BoundedOrder(
            OptimizerBoundedOrderAlternative::TopN,
        ),
    );
    let sort_then_limit = crate::OptimizerQualificationCandidateRequest::new(
        crate::OptimizerQualificationCandidateKind::BoundedOrder(
            OptimizerBoundedOrderAlternative::SortThenLimit,
        ),
    );
    let attempt_limits = crate::OptimizerQualificationAttemptLimits::new(
        std::time::Duration::from_secs(5),
        10_000_000,
    )
    .unwrap();
    let timeout_error =
        crate::OptimizerQualificationAttemptLimits::new(std::time::Duration::MAX, 1).unwrap_err();
    assert!(timeout_error
        .to_string()
        .contains("monotonic deadline range"));
    let duplicate = read
        .qualify_optimizer_bounded_order_candidate_requests(
            request,
            &[ordinary, ordinary, top_n],
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(duplicate.to_string().contains("duplicate kind ordinary"));
    let missing_ordinary = read
        .qualify_optimizer_bounded_order_candidate_requests(
            request,
            &[top_n, sort_then_limit],
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(missing_ordinary
        .to_string()
        .contains("missing the ordinary plan"));
    let over_limit = read
        .qualify_optimizer_bounded_order_candidate_requests(
            request,
            &[ordinary; crate::OPTIMIZER_QUALIFICATION_CANDIDATE_LIMIT + 1],
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(over_limit.to_string().contains("limit is 8"));

    let db = fixture(12);
    let mut read = db.begin_read_transaction().unwrap();
    let bounded_request =
        crate::QueryRequest::new("MATCH (n:Item) RETURN n.external AS id ORDER BY id LIMIT 5");
    let stale = read
        .qualify_optimizer_bounded_order_candidate_requests(
            bounded_request,
            &[
                ordinary.with_expected_plan_instance_fingerprint("stale-plan"),
                top_n,
                sort_then_limit,
            ],
            attempt_limits,
            OptimizerSearchDirective::Memo,
        )
        .unwrap_err();
    assert!(stale.to_string().contains("candidate ordinary is stale"));
}

#[test]
fn proved_parameter_templates_rebind_values_nulls_and_limits_without_stale_constants() {
    let db = fixture(20);
    let mut read = db.begin_read_transaction().unwrap();
    let query =
        "MATCH (n:Item) WHERE n.external = $value RETURN DISTINCT n.external AS id ORDER BY id";
    for value in [
        Value::Int(2),
        Value::Int(17),
        Value::Null,
        Value::Int(2),
        Value::Int(99),
    ] {
        let parameters = BTreeMap::from([("value".into(), value.clone())]);
        let expected = if matches!(value, Value::Int(0..=19)) {
            vec![BTreeMap::from([("id".into(), value)])]
        } else {
            Vec::new()
        };
        assert_eq!(
            read.query_with_params(query, &parameters).unwrap().rows,
            expected
        );
        let qualified = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query).with_params(&parameters),
                OptimizerQualificationOptions::default()
                    .without_rule(OptionalOptimizerRule::RedundantOrdering),
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        assert_eq!(qualified.result.unwrap().output.rows, expected);
    }
    let limited = read
        .qualify_optimizer_query(
            crate::QueryRequest::new("MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id")
                .with_output_limits(crate::QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                }),
            OptimizerQualificationOptions::default(),
            OptimizerSearchDirective::Memo,
        )
        .unwrap();
    assert!(limited
        .result
        .unwrap_err()
        .to_string()
        .contains("max_read_result_rows 1"));
}

#[test]
fn constant_ordering_elision_cannot_hide_a_late_expression_error_behind_limit() {
    let db = fixture(20);
    let mut read = db.begin_read_transaction().unwrap();
    let query="MATCH (n:Item) RETURN 1 AS constant, 1 / (n.external - 17) AS error ORDER BY constant LIMIT 1";
    for options in [
        OptimizerQualificationOptions::default(),
        OptimizerQualificationOptions::default()
            .without_rule(OptionalOptimizerRule::RedundantOrdering),
    ] {
        let qualified = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        qualified
            .verify_rules(&[OptimizerRuleAssertion::Forbidden(
                OptionalOptimizerRule::RedundantOrdering,
            )])
            .unwrap();
        assert!(qualified
            .result
            .unwrap_err()
            .to_string()
            .contains("division by zero"));
    }
}

#[test]
fn exists_proofs_refresh_independently_of_statistics_and_remain_snapshot_local() {
    let mut db = Database::new_with_config(DatabaseConfig {
        optimizer_statistics_inline_commit_lag: Some(u64::MAX),
        ..Default::default()
    });
    for query in [
        "CREATE NODE TABLE Item",
        "CREATE PROPERTY ON NODE TABLE Item(external) TYPE INT",
        "CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE",
        "CREATE (:Item {external:1})",
        "CREATE (:Item {external:2})",
    ] {
        db.query(query).unwrap();
    }
    let query = "MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id";
    let mut old = db.begin_read_transaction().unwrap();
    assert!(old
        .explain_query(query)
        .unwrap()
        .trace
        .selected_plan
        .contains("DistinctExec"));
    db.query("CREATE CONSTRAINT ON :Item(external) ASSERT EXISTS")
        .unwrap();
    let mut current = db.begin_read_transaction().unwrap();
    assert!(!current
        .explain_query(query)
        .unwrap()
        .trace
        .selected_plan
        .contains("DistinctExec"));
    assert!(old
        .explain_query(query)
        .unwrap()
        .trace
        .selected_plan
        .contains("DistinctExec"));
    assert_eq!(
        current.query(query).unwrap().rows,
        old.query(query).unwrap().rows
    );
}

#[test]
fn singleton_group_proof_runs_only_after_values_only_grouping() {
    let db = fixture_with_config(
        20,
        DatabaseConfig {
            execution_memory: crate::executor::ExecutionMemoryConfig {
                query_memory_bytes: std::num::NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let mut read = db.begin_read_transaction().unwrap();
    let query="MATCH (n:Item) WITH n.external AS id, count(*) AS original RETURN id, count(*) AS total ORDER BY id";
    let mut outputs = Vec::new();
    let mut fingerprints = BTreeSet::new();
    for enabled in [true, false] {
        let options = if enabled {
            OptimizerQualificationOptions::default()
        } else {
            OptimizerQualificationOptions::default()
                .without_rule(OptionalOptimizerRule::SingletonGroup)
        };
        let qualified = read
            .qualify_optimizer_query(
                crate::QueryRequest::new(query),
                options,
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        let rule = OptionalOptimizerRule::SingletonGroup;
        qualified
            .verify_rules(&[if enabled {
                OptimizerRuleAssertion::Required(rule)
            } else {
                OptimizerRuleAssertion::Forbidden(rule)
            }])
            .unwrap();
        fingerprints.insert(qualified.optimizer_trace.selected_plan_fingerprint);
        outputs.push(qualified.result.unwrap().output.rows.into_rows());
    }
    let expected = (0..20)
        .map(|value| {
            BTreeMap::from([
                ("id".into(), Value::Int(value)),
                ("total".into(), Value::Int(1)),
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs, vec![expected.clone(), expected]);
    assert_eq!(fingerprints.len(), 2);
}
