// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::graph::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};
use crate::{CascadesOptimizer, OptimizerQualificationOptions, OptimizerTrace};
use hawdb_core::RelationshipDirection;
use hawdb_plan_cypher::{plan_with_params, PlanChildren};

const RULE: OptionalOptimizerRule = OptionalOptimizerRule::DisjunctiveSeedUnion;

const IC12: &str = "MATCH (tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(base:TagClass) \
     WHERE tag.name = $name OR base.name = $name RETURN tag.id AS tag, base.id AS base";

fn catalog() -> OptimizerCatalog {
    let indexes = [
        ("Tag", "name"),
        ("TagClass", "name"),
        ("Person", "id"),
        ("Person", "nick"),
    ]
    .map(|(label, property)| (label.to_string(), property.to_string()));
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(indexes, [], [], []),
        OptimizerCatalogStatistics::new(
            [("Tag", 16_000), ("TagClass", 70), ("Person", 1_000)]
                .map(|(label, count)| (label.to_string(), count)),
            [],
            [],
            [],
            [],
            [
                (("Tag", "name"), 16_000),
                (("TagClass", "name"), 70),
                (("Person", "id"), 1_000),
                (("Person", "nick"), 1_000),
            ]
            .map(|((label, property), count)| ((label.to_string(), property.to_string()), count)),
            [],
        ),
    )
}

fn logical(query: &str) -> LogicalPlan {
    let statement = hawdb_cypher::parse(query).unwrap();
    let parameters = BTreeMap::from([("name".to_string(), Value::String("Golf".into()))]);
    plan_with_params(&statement, &parameters).unwrap()
}

fn optimize(query: &str, catalog: &OptimizerCatalog) -> (PhysicalPlan, OptimizerTrace) {
    CascadesOptimizer::default().optimize_with_catalog(&logical(query), catalog)
}

/// Every `GraphMatchExec` program, from the root down.
fn programs(plan: &PhysicalPlan) -> Vec<&GraphMatchProgram> {
    let mut found = Vec::new();
    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        if let PhysicalPlan::GraphMatchExec { program, .. } = plan {
            found.push(program);
        }
        match plan.children() {
            PlanChildren::None => {}
            PlanChildren::Unary(input) => pending.push(input),
            PlanChildren::Binary(left, right) => pending.extend([left, right]),
        }
    }
    found
}

fn branches(query: &str) -> Vec<GraphMatchProgram> {
    let (plan, _) = optimize(query, &catalog());
    programs(&plan)
        .into_iter()
        .flat_map(|program| program.seeded_union.clone())
        .collect()
}

fn start(branch: &GraphMatchProgram) -> &hawdb_plan_cypher::GraphMatchNode {
    match branch.steps.first() {
        Some(GraphMatchStep::Node(node)) => node,
        other => panic!("branch starts with {other:?}"),
    }
}

fn guard(variable: &str) -> Predicate {
    let value = Value::String("Golf".into());
    Predicate::Or(vec![
        Predicate::PropertyIsNull {
            variable: variable.into(),
            property: "name".into(),
        },
        Predicate::ExpressionEq {
            expression: ProjectionExpression::IsNull {
                expression: Box::new(ProjectionExpression::Literal(value.clone())),
                negated: false,
            },
            value: ProjectionExpression::Literal(Value::Bool(true)),
        },
        Predicate::PropertyNotEq {
            variable: variable.into(),
            property: "name".into(),
            value,
        },
    ])
}

#[test]
fn ic12_runs_as_two_seeded_branches_that_keep_the_whole_where() {
    let (plan, trace) = optimize(IC12, &catalog());
    let [program] = programs(&plan)[..] else {
        panic!("{}", plan.explain(0));
    };
    // The program itself is unchanged: it still scans Tag.
    assert_eq!(start(program).variable, "tag");
    assert!(start(program).properties.is_empty());
    let [first, second] = program.seeded_union.as_slice() else {
        panic!("{}", plan.explain(0));
    };
    let seek = BTreeMap::from([("name".to_string(), Value::String("Golf".into()))]);
    assert_eq!(start(first).variable, "tag");
    assert_eq!(start(first).properties, seek);
    assert_eq!(first.predicate, program.predicate);
    assert_eq!(start(second).variable, "base");
    assert_eq!(start(second).properties, seek);
    // The variable-length relationship is walked against its direction.
    assert!(matches!(
        &second.steps[1],
        GraphMatchStep::Expand { source, direction: RelationshipDirection::Incoming, min_hops: 0, target, .. }
            if source == "base" && target.variable == "tag"
    ));
    // The second branch drops the rows the first one produces.
    assert_eq!(
        second.predicate,
        Some(Predicate::And(vec![
            program.predicate.clone().unwrap(),
            guard("tag")
        ]))
    );
    assert!(plan.explain(0).contains(" seeded_union=[branch 0: "));
    assert!(trace.decisions.iter().any(|decision| decision.starts_with(
        "apply transformation:disjunctive_seed_union: run GraphMatchExec as a union of 2 branches seeded by index on [tag:Tag.name, base:TagClass.name]"
    )), "{:?}", trace.decisions);
}

#[test]
fn three_disjuncts_guard_every_earlier_seed() {
    let branches = branches(
        "MATCH (a:Person)-[:KNOWS*1..2]->(t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
         WHERE c.name = $name OR (t.name = $name AND a.id > 3) OR a.nick = $name RETURN a, t, c",
    );
    let starts = branches
        .iter()
        .map(|branch| start(branch).variable.as_str())
        .collect::<Vec<_>>();
    assert_eq!(starts, ["c", "t", "a"]);
    let guards = branches
        .iter()
        .map(|branch| branch.predicate_conjuncts().len() - 1)
        .collect::<Vec<_>>();
    assert_eq!(guards, [0, 1, 2]);
    assert!(branches[2].predicate_conjuncts().contains(&guard("c")));
}

#[test]
fn the_seed_is_the_most_selective_indexed_equality_of_a_disjunct() {
    let query = "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
                 WHERE (t.name = $name AND c.name = $name) OR c.name = 'x' RETURN t, c";
    let seeded = |tag_names: u64, class_names: u64| {
        let mut catalog = catalog();
        catalog
            .property_distinct_counts
            .insert(("Tag".into(), "name".into()), tag_names);
        catalog
            .property_distinct_counts
            .insert(("TagClass".into(), "name".into()), class_names);
        let (plan, _) = optimize(query, &catalog);
        programs(&plan)
            .into_iter()
            .flat_map(|program| program.seeded_union.iter())
            .map(|branch| start(branch).variable.clone())
            .collect::<Vec<_>>()
    };
    // `t.name` finds one Tag and `c.name` two classes: the first disjunct
    // seeds `t`, the second `c`.
    assert_eq!(seeded(16_000, 35), ["t", "c"]);
    // `t.name` finds ten Tags: both disjuncts would seed `c`.
    assert!(seeded(1_600, 70).is_empty());
}

#[test]
fn shapes_without_one_seed_per_node_keep_the_program() {
    for query in [
        // Both disjuncts seed the same node.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) WHERE t.name = 'a' OR t.name = 'b' RETURN t, c",
        // A disjunct without an equality.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) WHERE t.name = 'a' OR c.id > 3 RETURN t, c",
        // An equality without an index.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) WHERE t.name = 'a' OR c.url = 'b' RETURN t, c",
        // No disjunction.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) WHERE t.name = 'a' RETURN t, c",
        // A NULL equality is never true.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) WHERE t.name = null OR c.name = 'b' RETURN t, c",
        // OPTIONAL MATCH pads instead of dropping rows.
        "MATCH (p:Person {id: 1}) OPTIONAL MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
         WHERE t.name = 'a' OR c.name = 'b' RETURN p, t, c",
        // A correlated MATCH runs once per input row.
        "MATCH (p:Person) WITH p MATCH (p)-[:KNOWS*1..2]->(t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
         WHERE t.name = 'a' OR c.name = 'b' RETURN t, c",
        // The start already seeks.
        "MATCH (p:Person {id: 1})-[:KNOWS*1..2]->(t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
         WHERE t.name = 'a' OR c.name = 'b' RETURN t, c",
        // Two paths.
        "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass), (p:Person)-[:KNOWS*1..2]->(t) \
         WHERE t.name = 'a' OR c.name = 'b' RETURN p, t, c",
    ] {
        let (plan, _) = optimize(query, &catalog());
        let programs = programs(&plan);
        assert!(!programs.is_empty(), "{query}: {}", plan.explain(0));
        assert!(
            programs.iter().all(|program| program.seeded_union.is_empty()),
            "{query}"
        );
    }
}

#[test]
fn a_where_that_may_fail_keeps_the_program() {
    let query = "MATCH (t:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF]->(c:TagClass) \
                 WHERE (t.name = 'a' OR c.name = 'b') AND 10 / t.size > 1 RETURN t, c";
    let (plan, trace) = optimize(query, &catalog());
    assert!(programs(&plan)
        .iter()
        .all(|program| program.seeded_union.is_empty()));
    assert!(trace.decisions.iter().any(|decision| decision.contains(
        "transformation:disjunctive_seed_union: keep GraphMatchExec: seeds [t:Tag.name, c:TagClass.name] but the WHERE may fail"
    )), "{:?}", trace.decisions);
}

#[test]
fn seeds_must_be_clearly_cheaper_than_the_start() {
    // Without statistics every label counts one node, so nothing is cheaper.
    let (plan, _) = optimize(IC12, &OptimizerCatalog::optimistic());
    assert!(programs(&plan)
        .iter()
        .all(|program| program.seeded_union.is_empty()));
    // Seeds that each find half the start's nodes are not worth a union.
    let mut flat = catalog();
    flat.property_distinct_counts
        .insert(("Tag".into(), "name".into()), 2);
    let (plan, trace) = optimize(IC12, &flat);
    assert!(programs(&plan)
        .iter()
        .all(|program| program.seeded_union.is_empty()));
    assert!(trace.decisions.iter().any(|decision| decision.starts_with(
        "estimate transformation:disjunctive_seed_union: keep GraphMatchExec; would run"
    )));
}

#[test]
fn a_disabled_rule_records_the_skipped_union() {
    let catalog = catalog();
    let LogicalPlan::Project { input, .. } = logical(IC12) else {
        panic!("expected a projection");
    };
    let LogicalPlan::GraphMatch { program, .. } = *input else {
        panic!("expected a GraphMatch");
    };
    let costing = |options| StartCosting {
        catalog: &catalog,
        options,
        labels: None,
    };
    let mut decisions = Vec::new();
    let options = OptimizerQualificationOptions::default().without_rule(RULE);
    assert!(seeded_union_branches(&program, &program, costing(options), &mut decisions).is_empty());
    assert!(
        matches!(decisions.as_slice(), [decision] if decision.starts_with("skip transformation:disjunctive_seed_union: disabled by statement qualification")),
        "{decisions:?}"
    );
    decisions.clear();
    let optional = GraphMatchProgram {
        optional: true,
        ..program.clone()
    };
    let enabled = costing(OptimizerQualificationOptions::default());
    assert!(seeded_union_branches(&optional, &optional, enabled, &mut decisions).is_empty());
    assert!(decisions.is_empty(), "{decisions:?}");
    let branches = seeded_union_branches(
        &program,
        &program,
        costing(OptimizerQualificationOptions::default()),
        &mut decisions,
    );
    assert_eq!(branches.len(), 2);
}

#[test]
fn a_seeded_path_keeps_named_variable_length_lists_and_conflicting_patterns() {
    let node = |variable: &str, properties: &[(&str, &str)]| hawdb_plan_cypher::GraphMatchNode {
        variable: variable.into(),
        label: "Tag".into(),
        properties: properties
            .iter()
            .map(|(key, value)| (key.to_string(), Value::String(value.to_string())))
            .collect(),
    };
    let steps = |relationship: Option<&str>, max_hops: usize, b: &[(&str, &str)]| {
        vec![
            GraphMatchStep::Node(node("a", &[])),
            GraphMatchStep::Expand {
                source: "a".into(),
                relationship: relationship.map(str::to_string),
                rel_type: "R".into(),
                rel_types: Vec::new(),
                properties: BTreeMap::new(),
                direction: RelationshipDirection::Outgoing,
                min_hops: 1,
                max_hops,
                target: node("b", b),
            },
        ]
    };
    let value = Value::String("x".into());
    let seeded =
        |steps: Vec<GraphMatchStep>| match_order::seeded_path_steps(&steps, "b", "name", &value);
    // A named one-hop relationship is the same relationship either way.
    let reversed = seeded(steps(Some("r"), 1, &[])).unwrap();
    assert_eq!(
        reversed[0],
        GraphMatchStep::Node(node("b", &[("name", "x")]))
    );
    assert!(matches!(
        &reversed[1],
        GraphMatchStep::Expand { direction: RelationshipDirection::Incoming, target, .. }
            if target.variable == "a"
    ));
    // A named list of relationships is bound in text order.
    assert_eq!(seeded(steps(Some("r"), 3, &[])), None);
    assert!(seeded(steps(None, 3, &[])).is_some());
    // The node pattern already requires the same value, or another one.
    assert!(seeded(steps(None, 1, &[("name", "x")])).is_some());
    assert_eq!(seeded(steps(None, 1, &[("name", "y")])), None);
    // Not a node of the path.
    assert_eq!(
        match_order::seeded_path_steps(&steps(None, 1, &[]), "c", "name", &value),
        None
    );
}
