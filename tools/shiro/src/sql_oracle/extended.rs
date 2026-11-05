// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Additional relations over supported SQL operators. No production executor hooks.
use super::*;
use crate::typed_value_json;

pub(crate) const PROTOCOL: &str = "shiro-sql-operators-v1";

fn fingerprint(text: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hash);
    format!("{:016x}", hash.finish())
}

fn summary(observation: &SqlExecutionObservation) -> JsonValue {
    let mut rows = match &observation.outcome {
        ExecutionOutcome::Rows(rows) => rows
            .iter()
            .map(|row| row_json(row).to_string())
            .collect::<Vec<_>>(),
        ExecutionOutcome::Error { .. } => vec![],
    };
    rows.sort();
    json!({ "snapshot_epoch": observation.snapshot_epoch,
        "row_count": rows.len(), "rows_fingerprint": fingerprint(&rows.join("\n")),
        "plan_fingerprint": observation.plan.as_deref().map(sql_plan_signature).map(|plan| fingerprint(&plan)),
        "operators": observation.plan.as_deref().unwrap_or_default().iter().filter_map(|row| row.get("id")).map(typed_value_json).collect::<Vec<_>>(),
        "join_planning": observation.join_planning.as_deref().map(join_planning_json) })
}

pub(super) fn coverage(cases: &[JsonValue]) -> JsonValue {
    let mut counters = BTreeMap::<String, [u64; 5]>::new();
    for case in cases {
        for relation in case["sql"]["extended"]["cases"]
            .as_array()
            .into_iter()
            .flatten()
        {
            let Some(name) = relation["oracle"].as_str() else {
                continue;
            };
            let counts = counters.entry(name.to_string()).or_default();
            counts[0] += 1;
            counts[if relation["success"] == true { 1 } else { 2 }] += 1;
            if let Some(distinct) = relation["distinct_plans"].as_bool() {
                counts[if distinct { 3 } else { 4 }] += 1;
            }
        }
    }
    json!(counters
        .into_iter()
        .map(|(oracle, counts)| (
            oracle,
            json!({
                "attempted": counts[0], "passed": counts[1], "failed": counts[2],
                "distinct_plan_pairs": counts[3], "same_plan_pairs": counts[4]
            })
        ))
        .collect::<BTreeMap<_, _>>())
}

#[derive(Clone, Copy)]
enum Relation {
    Equivalent,
    BagPartition,
    SetPartition,
    SumPartition,
    DistinctCount,
    OrderedSlice { offset: usize, limit: usize },
    NoRecFilter,
    Dqp,
}

struct Definition {
    name: &'static str,
    relation: Relation,
    queries: Vec<SqlQueryInvocation>,
}

fn query(sql: impl Into<String>, parameters: Vec<Value>) -> SqlQueryInvocation {
    SqlQueryInvocation {
        sql: sql.into(),
        parameters,
        result_semantics: ResultSemantics::Bag,
    }
}

fn definitions(case: &SqlFuzzCase) -> Vec<Definition> {
    let threshold = Value::Int((case.seed % 12) as i64);
    let row_predicates = [
        None,
        Some("r.score >= $1"),
        Some("NOT (r.score >= $1)"),
        Some("r.score IS NULL"),
    ];
    let partition = |projection: &str| {
        row_predicates
            .iter()
            .enumerate()
            .map(|(index, predicate)| {
                query(
                    format!(
                        "SELECT {projection} FROM sql_fuzz_rows AS r{}",
                        predicate.map_or(String::new(), |p| format!(" WHERE {p}"))
                    ),
                    if matches!(index, 1 | 2) {
                        vec![threshold.clone()]
                    } else {
                        vec![]
                    },
                )
            })
            .collect()
    };
    let group_projection = "r.bucket AS bucket, COUNT(*) AS count, COUNT(DISTINCT r.score) AS distinct_scores, SUM(r.score) AS total, MIN(r.score) AS minimum, MAX(r.score) AS maximum";
    let having = [
        "",
        " HAVING SUM(r.score) >= $1",
        " HAVING NOT (SUM(r.score) >= $1)",
        " HAVING SUM(r.score) IS NULL",
    ];
    let offset = (case.seed % 4) as usize;
    let limit = (case.seed % 5 + 1) as usize;
    let ordered = "SELECT r.id AS id, r.score AS score FROM sql_fuzz_rows AS r ORDER BY r.score ASC NULLS FIRST, r.id ASC";
    vec![
        Definition { name: "sql_norec_filter", relation: Relation::NoRecFilter,
            queries: vec![case.norec.original.clone(), case.norec.rewritten.clone()] },
        Definition { name: "sql_dqp", relation: Relation::Dqp,
            queries: vec![case.join_rewrite.optimized.clone(), case.join_rewrite.optimized.clone()] },
        Definition { name: "sql_having_tlp", relation: Relation::BagPartition,
            queries: having.iter().enumerate().map(|(index, having)| query(format!("SELECT {group_projection} FROM sql_fuzz_rows AS r GROUP BY r.bucket{having}"),
                if matches!(index, 1 | 2) { vec![threshold.clone()] } else { vec![] })).collect() },
        Definition { name: "sql_distinct_tlp", relation: Relation::SetPartition, queries: partition("DISTINCT r.bucket AS bucket") },
        Definition { name: "sql_sum_tlp", relation: Relation::SumPartition, queries: partition("SUM(r.score) AS total") },
        Definition { name: "sql_distinct_count", relation: Relation::DistinctCount, queries: vec![
            query("SELECT COUNT(DISTINCT r.score) AS count FROM sql_fuzz_rows AS r", vec![]),
            query("SELECT r.score AS value FROM sql_fuzz_rows AS r", vec![])] },
        Definition { name: "sql_cross_join", relation: Relation::Equivalent, queries: vec![
            query("SELECT r.bucket AS bucket, g.priority AS value FROM sql_fuzz_rows AS r CROSS JOIN sql_fuzz_groups AS g WHERE r.group_id = g.id", vec![]),
            query("SELECT r.bucket AS bucket, g.priority AS value FROM sql_fuzz_rows AS r, sql_fuzz_groups AS g WHERE r.group_id = g.id", vec![]),
            query("SELECT r.bucket AS bucket, g.priority AS value FROM sql_fuzz_rows AS r INNER JOIN sql_fuzz_groups AS g ON r.group_id = g.id", vec![])] },
        Definition { name: "sql_ordered_limit", relation: Relation::OrderedSlice { offset, limit }, queries: vec![
            query(format!("{ordered} LIMIT {limit} OFFSET {offset}"), vec![]), query(ordered, vec![])] },
    ]
}

struct Verdict {
    signature: Option<String>,
    reason: Option<String>,
    observations: Vec<SqlExecutionObservation>,
    distinct_plans: Option<bool>,
}

fn check(snapshot: &DatabaseReadTransaction, epoch: u64, definition: &Definition) -> Verdict {
    let observations: Vec<_> = definition
        .queries
        .iter()
        .enumerate()
        .map(|(index, query)| {
            execute_sql_query_with_join_planning(
                snapshot,
                epoch,
                query,
                if matches!(definition.relation, Relation::Dqp) && index == 1 {
                    RelationalJoinPlanningDirective::SyntaxOrder
                } else {
                    RelationalJoinPlanningDirective::Auto
                },
            )
        })
        .collect();
    classify(definition, observations, epoch)
}

fn classify(
    definition: &Definition,
    observations: Vec<SqlExecutionObservation>,
    epoch: u64,
) -> Verdict {
    let mut verdict = Verdict {
        signature: None,
        reason: None,
        observations,
        distinct_plans: None,
    };
    let result = (|| {
        for (index, observation) in verdict.observations.iter().enumerate() {
            if let ExecutionOutcome::Error {
                phase,
                class,
                message,
            } = &observation.outcome
            {
                return Err((format!("variant_{index}_{phase}_{class}"), message.clone()));
            }
            if observation.snapshot_epoch != Some(epoch) || observation.plan.is_none() {
                return Err((
                    "snapshot_or_plan".into(),
                    "missing plan or different snapshot".into(),
                ));
            }
        }
        let rows: Vec<_> = verdict
            .observations
            .iter()
            .map(|observation| match &observation.outcome {
                ExecutionOutcome::Rows(rows) => rows.as_slice(),
                ExecutionOutcome::Error { .. } => &[],
            })
            .collect();
        if matches!(definition.relation, Relation::NoRecFilter) {
            let plan = verdict.observations[1].plan.as_deref().unwrap_or_default();
            let has_filter = plan.iter().any(|row| row.get("operator info").is_some_and(|value| {
                matches!(value, Value::String(info) if info.to_ascii_lowercase().contains("filter ("))
            }));
            if !has_filter {
                return Err((
                    "reference_plan".into(),
                    "reference must retain aggregate FILTER in its plan".into(),
                ));
            }
        }
        if matches!(definition.relation, Relation::Dqp) {
            let auto = verdict.observations[0].join_planning.as_deref();
            let syntax = verdict.observations[1].join_planning.as_deref();
            if !auto.is_some_and(|p| {
                p.strategy == RelationalJoinPlanningStrategy::CsgCmpMemo
                    && p.status == RelationalJoinPlanningStatus::Selected
                    && p.cost.is_some()
            }) || !syntax.is_some_and(|p| {
                p.strategy == RelationalJoinPlanningStrategy::SyntaxOrder
                    && p.reason == RelationalJoinPlanningReason::ExplicitSyntaxOrder
                    && p.status == RelationalJoinPlanningStatus::Selected
            }) {
                return Err((
                    "planning_directive".into(),
                    "DQP requires selected Auto memo and explicit SyntaxOrder evidence".into(),
                ));
            }
            verdict.distinct_plans = Some(
                sql_plan_signature(verdict.observations[0].plan.as_deref().unwrap_or_default())
                    != sql_plan_signature(
                        verdict.observations[1].plan.as_deref().unwrap_or_default(),
                    ),
            );
        }
        compare(definition.relation, &rows).map_err(|reason| ("result_mismatch".into(), reason))
    })();
    if let Err((signature, reason)) = result {
        verdict.signature = Some(format!("{}_{signature}", definition.name));
        verdict.reason = Some(reason);
    }
    verdict
}

fn scalar(rows: &[hawdb::executor::Row], column: &str) -> Result<Option<i64>, String> {
    let [row] = rows else {
        return Err("expected exactly one aggregate row".into());
    };
    if row.len() != 1 {
        return Err("expected one aggregate column".into());
    }
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(Value::Int(value)) => Ok(Some(*value)),
        _ => Err("expected typed BIGINT or NULL".into()),
    }
}

fn compare(relation: Relation, rows: &[&[hawdb::executor::Row]]) -> Result<(), String> {
    match relation {
        Relation::Equivalent | Relation::NoRecFilter | Relation::Dqp => {
            if matches!(relation, Relation::NoRecFilter) {
                for rows in rows {
                    if !scalar(rows, "count")?.is_some_and(|count| count >= 0) {
                        return Err("NoREC count must be a nonnegative BIGINT".into());
                    }
                }
            }
            for other in &rows[1..] {
                compare_rows(rows[0], other, ResultSemantics::Bag)?;
            }
            Ok(())
        }
        Relation::BagPartition | Relation::SetPartition => {
            let mut combined: Vec<_> = rows[1..]
                .iter()
                .flat_map(|rows| rows.iter().cloned())
                .collect();
            if matches!(relation, Relation::SetPartition) {
                let mut seen = std::collections::BTreeSet::new();
                combined.retain(|row| seen.insert(row_json(row).to_string()));
            }
            compare_rows(rows[0], &combined, ResultSemantics::Bag)
        }
        Relation::SumPartition => {
            let original = scalar(rows[0], "total")?;
            let mut sum = None;
            for partition in &rows[1..] {
                if let Some(value) = scalar(partition, "total")? {
                    sum = Some(
                        sum.unwrap_or(0_i64)
                            .checked_add(value)
                            .ok_or("reference sum overflow")?,
                    );
                }
            }
            if original == sum {
                Ok(())
            } else {
                Err(format!("SUM: expected {sum:?}, got {original:?}"))
            }
        }
        Relation::DistinctCount => {
            let mut values = std::collections::BTreeSet::new();
            for row in rows[1] {
                if let Some(value) = row.get("value")
                    && !matches!(value, Value::Null)
                {
                    values.insert(typed_value_json(value).to_string());
                }
            }
            let expected = i64::try_from(values.len()).map_err(|_| "distinct count overflow")?;
            if scalar(rows[0], "count")? == Some(expected) {
                Ok(())
            } else {
                Err(format!("COUNT DISTINCT: expected {expected}"))
            }
        }
        Relation::OrderedSlice { offset, limit } => {
            let expected: Vec<_> = rows[1].iter().skip(offset).take(limit).cloned().collect();
            compare_rows(rows[0], &expected, ResultSemantics::Ordered)
        }
    }
}

fn replay(case: &SqlFuzzCase, definition: &Definition) -> JsonValue {
    json!({ "seed": case.seed, "oracle": definition.name,
        "setup": case.setup.iter().map(SqlMutation::json).collect::<Vec<_>>(),
        "queries": definition.queries.iter().enumerate().map(|(index, query)| json!({
            "query": query.json(), "join_directive": if matches!(definition.relation, Relation::Dqp) && index == 1 { "syntax_order" } else { "auto" }
        })).collect::<Vec<_>>() })
}

fn reduce(case: &SqlFuzzCase, definition: &Definition, signature: &str) -> JsonValue {
    let mut reduced = case.clone();
    let mut attempts = 0;
    loop {
        let mut accepted = None;
        for index in 0..reduced.setup.len() {
            if attempts == MAX_SQL_REDUCTION_ATTEMPTS {
                break;
            }
            if !reduced.setup[index].reducible {
                continue;
            }
            attempts += 1;
            let mut candidate = reduced.clone();
            candidate.setup.remove(index);
            if let Ok((snapshot, epoch)) = prepare_sql_case(&candidate)
                && check(&snapshot, epoch, definition).signature.as_deref() == Some(signature)
            {
                accepted = Some(candidate);
                break;
            }
        }
        if let Some(candidate) = accepted {
            reduced = candidate;
        } else {
            break;
        }
    }
    json!({ "attempts": attempts, "original_setup_count": case.setup.len(),
        "reduced_setup_count": reduced.setup.len(), "replay": replay(&reduced, definition) })
}

pub(super) fn evaluate(case: &SqlFuzzCase) -> JsonValue {
    let definitions = definitions(case);
    let (snapshot, epoch) = match prepare_sql_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            return json!({ "protocol": PROTOCOL, "success": false, "setup_failure": observation.json(),
            "replay": definitions.iter().map(|definition| replay(case, definition)).collect::<Vec<_>>() })
        }
    };
    let reports: Vec<_> = definitions.iter().map(|definition| {
        let verdict = check(&snapshot, epoch, definition);
        json!({ "oracle": definition.name, "success": verdict.signature.is_none(),
            "signature": verdict.signature, "reason": verdict.reason,
            "distinct_plans": verdict.distinct_plans,
            "observations": verdict.observations.iter().map(|observation| if verdict.signature.is_none() { summary(observation) } else { observation.json() }).collect::<Vec<_>>(),
            "replay": verdict.signature.as_ref().map(|_| replay(case, definition)),
            "reduction": verdict.signature.as_deref().map(|signature| reduce(case, definition, signature)) })
    }).collect();
    json!({ "protocol": PROTOCOL, "success": reports.iter().all(|report| report["success"] == true),
        "norec_reference": "sum_filter_not_case_when", "cases": reports })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn row(column: &str, value: Value) -> hawdb::executor::Row {
        BTreeMap::from([(column.into(), value)])
    }

    #[test]
    fn partitions_preserve_duplicates_nulls_and_group_results() {
        let one = row("value", Value::Int(1));
        let null = row("value", Value::Null);
        let original = vec![one.clone(), one.clone(), null.clone()];
        assert!(compare(
            Relation::BagPartition,
            &[
                &original,
                std::slice::from_ref(&one),
                std::slice::from_ref(&one),
                std::slice::from_ref(&null)
            ]
        )
        .is_ok());
        assert!(compare(
            Relation::BagPartition,
            &[
                &original,
                std::slice::from_ref(&one),
                &[],
                std::slice::from_ref(&null)
            ]
        )
        .is_err());
        assert!(compare(
            Relation::SetPartition,
            &[
                &[one.clone(), null.clone()],
                std::slice::from_ref(&one),
                std::slice::from_ref(&one),
                std::slice::from_ref(&null)
            ]
        )
        .is_ok());
    }

    #[test]
    fn scalar_relations_reject_wrong_counts_and_handle_empty_sums() {
        let null_sum = vec![row("total", Value::Null)];
        assert!(compare(
            Relation::SumPartition,
            &[&null_sum, &null_sum, &null_sum, &null_sum]
        )
        .is_ok());
        let wrong = vec![row("total", Value::Int(0))];
        assert!(compare(
            Relation::SumPartition,
            &[&wrong, &null_sum, &null_sum, &null_sum]
        )
        .is_err());
        let values = vec![
            row("value", Value::Int(1)),
            row("value", Value::Int(1)),
            row("value", Value::Null),
        ];
        assert!(compare(
            Relation::DistinctCount,
            &[&[row("count", Value::Int(1))], &values]
        )
        .is_ok());
        assert!(compare(
            Relation::DistinctCount,
            &[&[row("count", Value::Int(2))], &values]
        )
        .is_err());
    }

    #[test]
    fn generated_operator_oracles_execute_and_capture_plan_evidence() {
        for index in 0..12 {
            let case =
                generator::generate_sql_case(211 + index as u64, index, index.is_multiple_of(2));
            let report = evaluate(&case);
            assert_eq!(report["success"], true, "{report}");
            assert_eq!(report["cases"].as_array().unwrap().len(), 8);
            assert_eq!(case.norec.name, "sum_filter_reference");
            let dqp = &report["cases"][1];
            assert!(dqp["distinct_plans"].is_boolean());
            let definitions = definitions(&case);
            assert_eq!(definitions[1].queries[0], definitions[1].queries[1]);
        }
    }

    #[test]
    fn norec_empty_input_and_invalid_count_are_distinct() {
        let mut case = generator::generate_sql_case(211, 0, false);
        case.setup.retain(|mutation| !mutation.reducible);
        let (snapshot, epoch) = prepare_sql_case(&case).unwrap();
        let definitions = definitions(&case);
        let verdict = check(&snapshot, epoch, &definitions[0]);
        assert!(verdict.signature.is_none(), "{:?}", verdict.reason);
        let invalid = vec![row("count", Value::Int(-1))];
        assert!(compare(Relation::NoRecFilter, &[&invalid, &invalid]).is_err());
    }

    #[test]
    fn planning_and_reference_negative_controls_fail_closed() {
        let case = generator::generate_sql_case(211, 0, false);
        let (snapshot, epoch) = prepare_sql_case(&case).unwrap();
        let definitions = definitions(&case);
        let mut norec = check(&snapshot, epoch, &definitions[0]).observations;
        for row in norec[1].plan.as_mut().unwrap() {
            row.insert(
                "operator info".into(),
                Value::String("removed aggregate predicate".into()),
            );
        }
        assert_eq!(
            classify(&definitions[0], norec, epoch).signature.as_deref(),
            Some("sql_norec_filter_reference_plan")
        );
        let mut dqp = check(&snapshot, epoch, &definitions[1]).observations;
        dqp[1].join_planning = dqp[0].join_planning.clone();
        assert_eq!(
            classify(&definitions[1], dqp, epoch).signature.as_deref(),
            Some("sql_dqp_planning_directive")
        );
    }

    #[test]
    fn incorrect_norec_relation_is_detected_and_reduced_without_becoming_an_error() {
        let case = generator::generate_sql_case(211, 0, true);
        let mut definitions = definitions(&case);
        let definition = &mut definitions[0];
        definition.queries[1] = query(
            "SELECT COALESCE(SUM(1) FILTER (WHERE r.id < 0), 0) AS count FROM sql_fuzz_rows AS r",
            vec![],
        );
        let (snapshot, epoch) = prepare_sql_case(&case).unwrap();
        let verdict = check(&snapshot, epoch, definition);
        assert_eq!(
            verdict.signature.as_deref(),
            Some("sql_norec_filter_result_mismatch")
        );
        let reduced = reduce(&case, definition, verdict.signature.as_deref().unwrap());
        assert!(reduced["attempts"].as_u64().unwrap() > 0);
        assert!(reduced["reduced_setup_count"].as_u64() < reduced["original_setup_count"].as_u64());
        assert_eq!(reduced["replay"]["queries"].as_array().unwrap().len(), 2);
    }
}
