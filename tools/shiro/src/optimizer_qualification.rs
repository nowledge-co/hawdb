// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Opt-in rule/ranking DQP. Every variant uses one statement and one snapshot.

use super::*;
use hawdb::optimizer::{
    OptimizerBoundedOrderAlternative, OptimizerQualificationOptions, OptimizerRuleAssertion,
    OptimizerSearchDirective, RuleOutcome,
};
use hawdb::QueryRequest;

#[derive(Debug, Clone)]
pub struct OptimizerQualificationVariant {
    pub options: OptimizerQualificationOptions,
    pub search: OptimizerSearchDirective,
    pub assertions: Vec<OptimizerRuleAssertion>,
}

#[derive(Debug)]
pub struct OptimizerQualificationReport {
    pub statement_clock: String,
    pub baseline: ExecutionObservation,
    pub comparisons: Vec<OptimizerQualificationComparison>,
    pub different_plan_agreements: usize,
    pub same_plan_agreements: usize,
    pub failures: usize,
}
impl OptimizerQualificationReport {
    pub fn success(&self) -> bool {
        self.failures == 0 && !self.comparisons.is_empty()
    }
    pub fn json(&self) -> JsonValue {
        json!({
            "protocol": "shiro-optimizer-qualification-v1",
            "statement_clock": self.statement_clock,
            "success": self.success(),
            "baseline": self.baseline.json(),
            "different_plan_agreements": self.different_plan_agreements,
            "same_plan_agreements": self.same_plan_agreements,
            "failures": self.failures,
            "comparisons": self.comparisons.iter().map(OptimizerQualificationComparison::json).collect::<Vec<_>>(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanAgreement {
    DifferentPlan,
    SamePlan,
    Failure,
}
impl PlanAgreement {
    fn as_str(self) -> &'static str {
        match self {
            Self::DifferentPlan => "different_plan_agreement",
            Self::SamePlan => "same_plan_agreement",
            Self::Failure => "failure",
        }
    }
}
#[derive(Debug)]
pub struct OptimizerQualificationComparison {
    pub configuration: OptimizerQualificationVariant,
    pub comparison: PlanAgreement,
    pub applied_rules: Vec<String>,
    pub failure: Option<String>,
    pub execution: ExecutionObservation,
}
impl OptimizerQualificationComparison {
    pub fn json(&self) -> JsonValue {
        json!({"configuration":variant_json(&self.configuration),"comparison":self.comparison.as_str(),
            "applied_rules":self.applied_rules,"failure":self.failure,"execution":self.execution.json()})
    }
}
#[derive(Debug)]
pub struct GeneratedOptimizerQualificationReport {
    pub seed: u64,
    pub case_index: usize,
    pub replay: ReplayBundle,
    pub qualification: OptimizerQualificationReport,
}
impl GeneratedOptimizerQualificationReport {
    pub fn json(&self) -> JsonValue {
        let mut json = self.qualification.json();
        json["seed"] = json!(self.seed);
        json["case_index"] = json!(self.case_index);
        json["replay"] = self.replay.json();
        json
    }
}

/// Read-only qualification over a fixture loaded through ordinary query APIs.
/// Matching arbitrary errors are failures, never differential coverage.
pub fn qualify_optimizer_read(
    mutations: &[Mutation],
    query: &QueryInvocation,
    variants: &[OptimizerQualificationVariant],
) -> Result<OptimizerQualificationReport, FuzzError> {
    with_qualification_clock(|| {
        qualify_optimizer_read_at_statement_clock(mutations, query, variants)
    })
}

fn qualify_optimizer_read_at_statement_clock(
    mutations: &[Mutation],
    query: &QueryInvocation,
    variants: &[OptimizerQualificationVariant],
) -> Result<OptimizerQualificationReport, FuzzError> {
    if variants.is_empty() || variants.len() > 8 {
        return Err(FuzzError::new(
            "qualification requires one to eight variants",
        ));
    }
    if variants.iter().any(|variant| variant.assertions.len() > 6) {
        return Err(FuzzError::new(
            "qualification accepts at most six rule assertions",
        ));
    }
    let mut db = Database::new();
    for mutation in mutations {
        db.query_with_params(&mutation.cypher, &mutation.parameters)
            .map_err(|error| FuzzError::new(format!("qualification fixture: {error}")))?;
    }
    let mut snapshot = db
        .begin_read_transaction()
        .map_err(|error| FuzzError::new(error.to_string()))?;
    let epoch = snapshot.commit_epoch();
    let baseline = execute_qualified(
        &mut snapshot,
        epoch,
        query,
        OptimizerQualificationOptions::default(),
        OptimizerSearchDirective::Memo,
        &[],
    )
    .0;
    let mut report = OptimizerQualificationReport {
        statement_clock: hawdb::temporal::format_datetime(qualification_clock()),
        baseline,
        comparisons: Vec::with_capacity(variants.len()),
        different_plan_agreements: 0,
        same_plan_agreements: 0,
        failures: 0,
    };
    for variant in variants {
        let (observation, assertion_error, applied) = execute_qualified(
            &mut snapshot,
            epoch,
            query,
            variant.options,
            variant.search,
            &variant.assertions,
        );
        let mut failure = assertion_error;
        if report.baseline.snapshot_epoch != observation.snapshot_epoch
            || report.baseline.query_digest.is_none()
            || report.baseline.query_digest != observation.query_digest
        {
            failure.get_or_insert_with(|| "statement or pinned snapshot mismatch".into());
        }
        if let Some(detected) = classify_plan_failure(
            &report.baseline.outcome,
            &observation.outcome,
            query.result_semantics,
        ) {
            failure.get_or_insert(detected.reason);
        }
        let comparison = if failure.is_some() {
            report.failures += 1;
            PlanAgreement::Failure
        } else if report.baseline.plan_fingerprint.is_none()
            || observation.plan_fingerprint.is_none()
        {
            report.failures += 1;
            failure = Some("missing executable plan fingerprint".into());
            PlanAgreement::Failure
        } else if report.baseline.plan_fingerprint == observation.plan_fingerprint {
            report.same_plan_agreements += 1;
            PlanAgreement::SamePlan
        } else {
            report.different_plan_agreements += 1;
            PlanAgreement::DifferentPlan
        };
        report.comparisons.push(OptimizerQualificationComparison {
            configuration: variant.clone(),
            comparison,
            applied_rules: applied,
            failure,
            execution: observation,
        });
    }
    Ok(report)
}

pub fn qualify_generated_optimizer_case(
    seed: u64,
    index: usize,
    variants: &[OptimizerQualificationVariant],
) -> Result<GeneratedOptimizerQualificationReport, FuzzError> {
    if index >= MAX_CASE_COUNT {
        return Err(FuzzError::new(
            "qualification case index exceeds safety limit",
        ));
    }
    let mut generator = StateAwareCaseGenerator::new(seed);
    let mut case = generator.case(0);
    for next in 1..=index {
        case = generator.case(next);
    }
    Ok(GeneratedOptimizerQualificationReport {
        seed,
        case_index: index,
        replay: ReplayBundle::from_case(&case),
        qualification: qualify_optimizer_read(&case.mutations, &case.query, variants)?,
    })
}

/// Generate a statement shape that admits two executable pagination strategies.
pub fn qualify_bounded_order_case(
    seed: u64,
    index: usize,
    additional: &[OptimizerQualificationVariant],
) -> Result<JsonValue, FuzzError> {
    if index >= MAX_CASE_COUNT || additional.len() > 6 {
        return Err(FuzzError::new(
            "bounded-order qualification limits exceeded",
        ));
    }
    let input = seed.wrapping_add((index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let row_count = 4 + (input % 13) as i64;
    let mut mutations = [
        "CREATE NODE TABLE ShiroOrder",
        "CREATE PROPERTY ON NODE TABLE ShiroOrder(id) TYPE INT NOT NULL",
        "CREATE PROPERTY ON NODE TABLE ShiroOrder(sort_key) TYPE INT",
    ]
    .into_iter()
    .map(Mutation::new)
    .collect::<Vec<_>>();
    for id in 0..row_count {
        let key = (input ^ (id as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)) % 7;
        mutations.push(Mutation {
            cypher: "CREATE (:ShiroOrder {id: $id, sort_key: $key})".into(),
            parameters: Parameters::from([
                ("id".into(), Value::Int(id)),
                (
                    "key".into(),
                    if key == 0 {
                        Value::Null
                    } else {
                        Value::Int((key % 3) as i64)
                    },
                ),
            ]),
        });
    }
    let minimum = if index.is_multiple_of(4) {
        row_count
    } else {
        (input % 3) as i64
    };
    let query = QueryInvocation {
        cypher: "MATCH (n:ShiroOrder) WHERE n.id >= $min RETURN n.id AS id, n.sort_key AS key, datetime() AS observed ORDER BY key ASC, id DESC SKIP $offset LIMIT $limit".into(),
        parameters: Parameters::from([
            ("min".into(), Value::Int(minimum)),
            ("offset".into(), Value::Int(((input >> 8) % 4) as i64)),
            ("limit".into(), Value::Int(1 + ((input >> 16) % 6) as i64)),
        ]),
        result_semantics: ResultSemantics::Ordered,
    };
    let mut variants = additional.to_vec();
    variants.extend(
        [
            OptimizerBoundedOrderAlternative::TopN,
            OptimizerBoundedOrderAlternative::SortThenLimit,
        ]
        .map(|alternative| OptimizerQualificationVariant {
            options: OptimizerQualificationOptions::default()
                .with_bounded_order_alternative(alternative),
            search: OptimizerSearchDirective::Memo,
            assertions: Vec::new(),
        }),
    );
    let report = qualify_optimizer_read(&mutations, &query, &variants)?;
    let mut output = bounded_order_json(&report);
    output["seed"] = json!(seed);
    output["case_index"] = json!(index);
    output["family"] = json!("bounded_order");
    output["replay"] = json!({
        "mutations": mutations.iter().map(mutation_json).collect::<Vec<_>>(),
        "query": query_invocation_json(&query),
        "statement_clock": report.statement_clock,
        "variants": variants.iter().map(variant_json).collect::<Vec<_>>(),
    });
    Ok(output)
}

fn bounded_order_json(report: &OptimizerQualificationReport) -> JsonValue {
    let pair = verify_bounded_order_pair(&report.comparisons);
    let verified = pair.is_ok();
    let mut output = report.json();
    output["different_plan_observed"] = json!(verified);
    output["bounded_order_failure"] = json!(pair.err());
    output["success"] = json!(report.success() && verified);
    output
}

fn verify_bounded_order_pair(
    comparisons: &[OptimizerQualificationComparison],
) -> Result<(), &'static str> {
    // The generated family appends these two required runs after any extra variants.
    let [.., top_n, sort_then_limit] = comparisons else {
        return Err("missing forced bounded-order executions");
    };
    if [top_n, sort_then_limit]
        .iter()
        .any(|comparison| comparison.comparison == PlanAgreement::Failure)
    {
        return Err("forced bounded-order execution failed qualification");
    }
    let (Some(top_n), Some(sort_then_limit)) = (
        top_n.execution.plan_fingerprint.as_deref(),
        sort_then_limit.execution.plan_fingerprint.as_deref(),
    ) else {
        return Err("missing forced bounded-order plan fingerprint");
    };
    if top_n == sort_then_limit {
        return Err("forced bounded-order executions used the same plan");
    }
    // The generated query has a single pagination site at its physical root.
    if !top_n.starts_with("TopNExec(") {
        return Err("forced TopN execution did not select TopNExec");
    }
    if !sort_then_limit.starts_with("LimitExec(SortExec(") {
        return Err("forced Sort-plus-Limit execution did not select LimitExec over SortExec");
    }
    Ok(())
}

fn variant_json(variant: &OptimizerQualificationVariant) -> JsonValue {
    use hawdb::optimizer::OptionalOptimizerRule;
    json!({
        "search": variant.search.as_str(),
        "disabled_rules": OptionalOptimizerRule::ALL.into_iter()
            .filter(|rule| !variant.options.rule_enabled(*rule)).map(|rule| rule.as_str()).collect::<Vec<_>>(),
        "cost_perturbation": variant.options.cost_perturbation().map(|cost|
            json!({"seed":cost.seed(),"percent":cost.percent()})),
        "bounded_order_alternative": variant.options.bounded_order_alternative().map(|choice| choice.as_str()),
        "assertions": variant.assertions.iter().map(|assertion| match assertion {
            OptimizerRuleAssertion::Required(rule) => json!({"required":rule.as_str()}),
            OptimizerRuleAssertion::Forbidden(rule) => json!({"forbidden":rule.as_str()}),
        }).collect::<Vec<_>>(),
    })
}

pub(super) fn execute_qualified(
    snapshot: &mut DatabaseReadTransaction,
    epoch: u64,
    query: &QueryInvocation,
    options: OptimizerQualificationOptions,
    search: OptimizerSearchDirective,
    assertions: &[OptimizerRuleAssertion],
) -> (ExecutionObservation, Option<String>, Vec<String>) {
    with_qualification_clock(|| {
        execute_qualified_at_statement_clock(snapshot, epoch, query, options, search, assertions)
    })
}

pub(super) fn qualification_clock() -> hawdb::temporal::OffsetDateTime {
    hawdb::temporal::pinned_statement_clock().unwrap_or_else(|| {
        hawdb::temporal::OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(1_793_361_600) * 1_000_000_000,
        )
        .expect("fixed fuzz statement clock")
    })
}

fn with_qualification_clock<R>(run: impl FnOnce() -> R) -> R {
    hawdb::temporal::with_statement_clock(qualification_clock(), run)
}

fn execute_qualified_at_statement_clock(
    snapshot: &mut DatabaseReadTransaction,
    epoch: u64,
    query: &QueryInvocation,
    options: OptimizerQualificationOptions,
    search: OptimizerSearchDirective,
    assertions: &[OptimizerRuleAssertion],
) -> (ExecutionObservation, Option<String>, Vec<String>) {
    let result = match snapshot.qualify_optimizer_query(
        QueryRequest::new(&query.cypher).with_params(&query.parameters),
        options,
        search,
    ) {
        Ok(result) => result,
        Err(error) => {
            return (
                snapshot_error_observation(
                    Some(epoch),
                    Some(search.as_str().into()),
                    "explain",
                    error,
                ),
                None,
                Vec::new(),
            )
        }
    };
    let assertion_error = result
        .verify_rules(assertions)
        .err()
        .map(|error| error.to_string());
    let trace = result.optimizer_trace;
    let applied = trace
        .rule_events
        .iter()
        .filter(|event| event.outcome() == RuleOutcome::Applied)
        .map(|event| event.rule().to_owned())
        .collect();
    let optimizer_stages = trace
        .stage_events
        .iter()
        .map(|stage| {
            let stats = stage.stats();
            format!(
                "{}:{}:{}:{}:{}:{}",
                stage.name(),
                stage.apply_order().as_str(),
                stats.input_count,
                stats.output_count,
                stats.applied_rules,
                stats.skipped_rules
            )
        })
        .collect();
    let outcome = match result.result {
        Ok(output) => ExecutionOutcome::Rows(output.output.rows.into_rows()),
        Err(error) => error_outcome("execute", error),
    };
    (
        ExecutionObservation {
            snapshot_epoch: Some(result.snapshot_epoch),
            optimizer_search: Some(search.as_str().into()),
            search_mode: Some(trace.search_mode.as_str().into()),
            plan_fingerprint: Some(trace.selected_plan_fingerprint),
            query_digest: trace.query_digest,
            optimizer_stages,
            outcome,
        },
        assertion_error,
        applied,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb::optimizer::{OptimizerCostPerturbation, OptionalOptimizerRule};

    fn variant(options: OptimizerQualificationOptions) -> OptimizerQualificationVariant {
        OptimizerQualificationVariant {
            options,
            search: OptimizerSearchDirective::Memo,
            assertions: Vec::new(),
        }
    }
    fn fixture() -> Vec<Mutation> {
        let mut mutations = [
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(external) TYPE INT NOT NULL",
            "CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE",
        ]
        .into_iter()
        .map(Mutation::new)
        .collect::<Vec<_>>();
        mutations.push(Mutation {
            cypher: "UNWIND $rows AS r CREATE (:Item {external:r.value})".into(),
            parameters: Parameters::from([(
                "rows".into(),
                Value::List(
                    (0..100)
                        .map(|value| {
                            Value::Map(BTreeMap::from([("value".into(), Value::Int(value))]))
                        })
                        .collect(),
                ),
            )]),
        });
        mutations
    }
    fn query(cypher: &str) -> QueryInvocation {
        QueryInvocation {
            cypher: cypher.into(),
            parameters: Parameters::new(),
            result_semantics: ResultSemantics::Ordered,
        }
    }
    #[test]
    fn real_alternatives_and_same_plan_agreement_are_counted_separately() {
        let normal = variant(OptimizerQualificationOptions::default());
        let mut disabled = variant(
            normal
                .options
                .without_rule(OptionalOptimizerRule::RedundantDistinct),
        );
        disabled.assertions.push(OptimizerRuleAssertion::Forbidden(
            OptionalOptimizerRule::RedundantDistinct,
        ));
        let report = qualify_optimizer_read(
            &fixture(),
            &query("MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id"),
            &[normal, disabled],
        )
        .unwrap();
        assert!(report.success(), "{}", report.json());
        assert_eq!(report.same_plan_agreements, 1);
        assert_eq!(report.different_plan_agreements, 1);
        assert_eq!(
            report.json()["comparisons"][1]["configuration"]["disabled_rules"][0],
            OptionalOptimizerRule::RedundantDistinct.as_str()
        );
    }
    #[test]
    fn ranking_variants_replay_the_same_statement_parameters_and_ordered_rows() {
        let variants = (0..8)
            .map(|seed| {
                variant(
                    OptimizerQualificationOptions::default()
                        .with_cost_perturbation(OptimizerCostPerturbation::new(seed, 25).unwrap()),
                )
            })
            .collect::<Vec<_>>();
        let mut query = query(
            "MATCH (n:Item) WHERE n.external >= $low RETURN n.external AS id ORDER BY id LIMIT 30",
        );
        query.parameters.insert("low".into(), Value::Int(0));
        let report = qualify_optimizer_read(&fixture(), &query, &variants).unwrap();
        assert!(report.success(), "{}", report.json());
        assert_eq!(
            report.same_plan_agreements + report.different_plan_agreements,
            8
        );
        let replay = qualify_optimizer_read(&fixture(), &query, &variants).unwrap();
        assert_eq!(report.json(), replay.json());
    }
    #[test]
    fn generated_bounded_order_compares_real_plans_and_replays_exactly() {
        for index in 0..8 {
            let report = qualify_bounded_order_case(7, index, &[]).unwrap();
            assert_eq!(report["success"], true, "{report:#}");
            assert_eq!(report["different_plan_observed"], true);
            assert!(report["bounded_order_failure"].is_null());
            assert_eq!(report["different_plan_agreements"], 1);
            assert_eq!(report["same_plan_agreements"], 1);
            assert_eq!(report, qualify_bounded_order_case(7, index, &[]).unwrap());
        }
    }

    #[test]
    fn generated_bounded_order_rejects_duplicate_and_swapped_forced_plans() {
        use OptimizerBoundedOrderAlternative::{SortThenLimit, TopN};
        let fixture = fixture();
        let query = query("MATCH (n:Item) RETURN n.external AS id ORDER BY id LIMIT 30");
        for (choices, expected_failure) in [
            (
                [TopN, SortThenLimit, SortThenLimit],
                "forced bounded-order executions used the same plan",
            ),
            (
                [SortThenLimit, TopN, TopN],
                "forced bounded-order executions used the same plan",
            ),
            (
                [TopN, SortThenLimit, TopN],
                "forced TopN execution did not select TopNExec",
            ),
        ] {
            let variants = choices.map(|choice| {
                variant(
                    OptimizerQualificationOptions::default().with_bounded_order_alternative(choice),
                )
            });
            let report = qualify_optimizer_read(&fixture, &query, &variants).unwrap();
            assert!(report.success());
            assert!(report.different_plan_agreements > 0);
            let [_, first, second] = report.comparisons.as_slice() else {
                panic!("expected one additional variant and two forced executions");
            };
            assert_eq!(first.execution.outcome, second.execution.outcome);
            assert_eq!(
                first.execution.plan_fingerprint == second.execution.plan_fingerprint,
                choices[1] == choices[2],
            );
            let output = bounded_order_json(&report);
            assert_eq!(output["success"], false, "{output:#}");
            assert_eq!(output["different_plan_observed"], false);
            assert_eq!(output["bounded_order_failure"], expected_failure);
            assert_eq!(
                output["different_plan_agreements"],
                report.different_plan_agreements,
            );
        }
    }

    #[test]
    fn candidate_batches_share_an_automatic_clock() {
        let mut db = Database::new();
        for mutation in fixture() {
            db.query_with_params(&mutation.cypher, &mutation.parameters)
                .unwrap();
        }
        let previous = hawdb::temporal::pinned_statement_clock();
        assert!(previous.is_none());
        let output = db
            .begin_read_transaction()
            .unwrap()
            .qualify_optimizer_bounded_order_candidates(
                QueryRequest::new(
                    "MATCH (n:Item) RETURN n.external AS id, datetime() AS observed ORDER BY id LIMIT 30",
                ),
                hawdb::OptimizerQualificationAttemptLimits::new(
                    std::time::Duration::from_secs(5),
                    100_000,
                )
                .unwrap(),
                OptimizerSearchDirective::Memo,
            )
            .unwrap();
        output.verify_complete_results().unwrap();
        assert_eq!(output.distinct_candidate_count, 2);
        assert_eq!(hawdb::temporal::pinned_statement_clock(), previous);
    }

    #[test]
    fn clock_reading_variants_replay_without_false_plan_diversity() {
        use hawdb::optimizer::OptimizerBoundedOrderAlternative;

        let variants = [
            variant(OptimizerQualificationOptions::default()),
            variant(
                OptimizerQualificationOptions::default()
                    .with_bounded_order_alternative(OptimizerBoundedOrderAlternative::TopN),
            ),
            variant(
                OptimizerQualificationOptions::default().with_bounded_order_alternative(
                    OptimizerBoundedOrderAlternative::SortThenLimit,
                ),
            ),
        ];
        let invocation = query(
            "MATCH (n:Item) RETURN n.external AS id, datetime() AS observed ORDER BY id LIMIT 30",
        );
        let report = qualify_optimizer_read(&fixture(), &invocation, &variants).unwrap();
        assert!(report.success(), "{}", report.json());
        assert_eq!(report.same_plan_agreements, 2);
        assert_eq!(report.different_plan_agreements, 1);
        let replay = qualify_optimizer_read(&fixture(), &invocation, &variants).unwrap();
        assert_eq!(report.json(), replay.json());
        assert_eq!(
            report.json()["comparisons"][2]["configuration"]["bounded_order_alternative"],
            "sort_then_limit"
        );

        let failure = qualify_optimizer_read(
            &fixture(),
            &query("MATCH (n:Item) RETURN datetime() AS observed, 1 / (n.external - 77) AS invalid ORDER BY invalid LIMIT 30"),
            &variants,
        )
        .unwrap();
        assert!(!failure.success());
        assert_eq!(failure.failures, variants.len());
        assert_eq!(
            failure.same_plan_agreements + failure.different_plan_agreements,
            0
        );
    }

    #[test]
    fn duplicates_nulls_and_optional_fanout_do_not_acquire_keys() {
        let fixtures = [
            "CREATE NODE TABLE Item",
            "CREATE PROPERTY ON NODE TABLE Item(external) TYPE INT",
            "CREATE CONSTRAINT ON :Item(external) ASSERT UNIQUE",
            "CREATE (:Item {external:1})",
            "CREATE (:Item {})",
            "CREATE (:Item {})",
            "CREATE (:Other {})",
            "CREATE (:Other {})",
        ]
        .map(Mutation::new);
        let mut forbidden = variant(OptimizerQualificationOptions::default());
        forbidden.assertions.push(OptimizerRuleAssertion::Forbidden(
            OptionalOptimizerRule::RedundantDistinct,
        ));
        for cypher in ["MATCH (n:Item) RETURN DISTINCT n.external AS id ORDER BY id",
            "MATCH (n:Item) OPTIONAL MATCH (n)-[:LINK]->(m:Other) RETURN DISTINCT n.external AS id ORDER BY id",
            "MATCH (n:Item), (m:Other) RETURN DISTINCT n.external AS id ORDER BY id"] {
            let report=qualify_optimizer_read(&fixtures,&query(cypher),&[forbidden.clone()]).unwrap();
            assert!(report.success(),"{}",report.json());
            let ExecutionOutcome::Rows(rows)=&report.baseline.outcome else {panic!("expected rows")};
            assert_eq!(rows.len(),2);
            assert_eq!(rows.iter().filter(|row|row["id"]==Value::Null).count(),1);
        }
    }
    #[test]
    fn runtime_errors_and_failed_rule_assertions_are_failures_not_coverage() {
        let mut required = variant(
            OptimizerQualificationOptions::default()
                .without_rule(OptionalOptimizerRule::RedundantDistinct),
        );
        required.assertions.push(OptimizerRuleAssertion::Required(
            OptionalOptimizerRule::RedundantDistinct,
        ));
        let report = qualify_optimizer_read(
            &fixture(),
            &query("MATCH (n:Item) RETURN DISTINCT n.external AS id"),
            &[required],
        )
        .unwrap();
        assert!(!report.success());
        assert_eq!(report.failures, 1);
        assert_eq!(
            report.same_plan_agreements + report.different_plan_agreements,
            0
        );
        let report = qualify_optimizer_read(
            &fixture(),
            &query("MATCH (n:Item) RETURN DISTINCT 1 / (n.external - 77) AS error"),
            &[variant(OptimizerQualificationOptions::default())],
        )
        .unwrap();
        assert!(!report.success());
        assert!(report.json().to_string().contains("division by zero"));
        assert!(
            report.baseline.plan_fingerprint.is_some(),
            "actual error retains executed plan"
        );
        assert_eq!(
            report.same_plan_agreements + report.different_plan_agreements,
            0
        );
    }
    #[test]
    fn qualification_variant_and_assertion_storage_is_bounded() {
        let item = variant(OptimizerQualificationOptions::default());
        assert!(qualify_optimizer_read(&[], &query("RETURN 1 AS value"), &[]).is_err());
        assert!(qualify_optimizer_read(&[], &query("RETURN 1 AS value"), &vec![item; 9]).is_err());
    }
}
