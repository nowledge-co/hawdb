// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::graph::pattern_start::tests::catalog;
use crate::{CascadesOptimizer, OptimizerTrace};
use hawdb_plan_cypher::{plan_with_params, PhysicalPlan};
use std::collections::BTreeMap;

fn logical(query: &str) -> LogicalPlan {
    let statement = hawdb_cypher::parse(query).unwrap();
    plan_with_params(&statement, &BTreeMap::new()).unwrap()
}

fn optimize(query: &str) -> (PhysicalPlan, OptimizerTrace) {
    CascadesOptimizer::default().optimize_with_catalog(&logical(query), &catalog())
}

fn rule_events(trace: &OptimizerTrace) -> Vec<String> {
    trace
        .rule_events
        .iter()
        .filter(|event| event.rule() == OptionalOptimizerRule::CostBasedPatternStart.as_str())
        .map(|event| event.clone().into_decision())
        .collect()
}

const EMPLOYEES: &str = "MATCH (f:Person)-[w:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name: 'Country3'}) \
     RETURN f, w, c";

#[test]
fn a_chain_ending_at_a_seekable_node_starts_from_its_seek() {
    let (plan, trace) = optimize(EMPLOYEES);
    let explain = plan.explain(0);
    let lines = explain.lines().map(str::trim).collect::<Vec<_>>();
    let seek = lines
        .iter()
        .position(|line| line.starts_with("IndexNodeSeek variable=p label=Place property=name"))
        .unwrap_or_else(|| panic!("{explain}"));
    // The walk back flips each direction; the relationship variable stays.
    let located = lines
        .iter()
        .position(|line| {
            line.starts_with(
                "AdjacencyExpandExec source=p:Place rel_type=IS_LOCATED_IN direction=<-",
            ) && line.ends_with("target=c:Organisation")
        })
        .unwrap_or_else(|| panic!("{explain}"));
    let works = lines
        .iter()
        .position(|line| {
            line.starts_with("AdjacencyExpandExec source=c:Organisation rel=w:WORK_AT direction=<-")
                && line.ends_with("target=f:Person")
        })
        .unwrap_or_else(|| panic!("{explain}"));
    assert!(works < located && located < seek, "{explain}");
    let events = rule_events(&trace);
    assert!(
        events.iter().any(|event| event.starts_with(
            "apply transformation:cost_based_pattern_start: start Expand chain at p with an index seek on Place.name"
        )),
        "{events:?}"
    );
}

#[test]
fn a_disabled_rule_or_an_optimistic_catalog_keeps_the_chain() {
    let plan = logical(EMPLOYEES);
    let mut events = Vec::new();
    let disabled = OptimizerQualificationOptions::default()
        .without_rule(OptionalOptimizerRule::CostBasedPatternStart);
    assert_eq!(
        rewrite(plan.clone(), &catalog(), disabled, &mut events),
        plan
    );
    assert!(
        matches!(events.as_slice(), [event] if event.outcome() == crate::RuleOutcome::Skipped),
        "{events:?}"
    );
    events.clear();
    assert_eq!(
        rewrite(
            plan.clone(),
            &OptimizerCatalog::optimistic(),
            OptimizerQualificationOptions::default(),
            &mut events
        ),
        plan
    );
    assert!(events.is_empty(), "{events:?}");
}

#[test]
fn a_selective_text_start_keeps_the_chain() {
    // Both ends seek; walking KNOWS*1..2 back from every employee is not
    // cheaper than the person seek.
    let (plan, trace) = optimize(
        "MATCH (a:Person {id: 7})-[:KNOWS*1..2]-(f:Person)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(:Place {name: 'Country3'}) \
         RETURN f",
    );
    assert!(
        plan.explain(0)
            .contains("IndexNodeSeek variable=a label=Person property=id"),
        "{}",
        plan.explain(0)
    );
    assert!(
        rule_events(&trace).iter().any(|event| event.starts_with(
            "estimate transformation:cost_based_pattern_start: keep Expand chain start a"
        )),
        "{:?}",
        rule_events(&trace)
    );
}
