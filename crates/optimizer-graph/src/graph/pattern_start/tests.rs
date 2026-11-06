// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::graph::{OptimizerCatalogIndexes, OptimizerCatalogStatistics};
use RelationshipDirection::{Incoming, Outgoing, Undirected};

/// 3,000 persons, 7,500 organisations and 1,100 places; `Place.name` is
/// unique and indexed, `Person.id` too.
pub(in crate::graph) fn catalog() -> OptimizerCatalog {
    let path = |source: &str, rel_type: &str, target: &str, count: u64| {
        (
            (source.to_string(), rel_type.to_string(), target.to_string()),
            count,
        )
    };
    OptimizerCatalog::new(
        OptimizerCatalogIndexes::new(
            [
                ("Place".to_string(), "name".to_string()),
                ("Person".to_string(), "id".to_string()),
            ],
            [],
            [],
            [],
        ),
        OptimizerCatalogStatistics::new(
            [
                ("Person".to_string(), 3_000),
                ("Organisation".to_string(), 7_500),
                ("Place".to_string(), 1_100),
            ],
            [],
            [],
            [
                path("Person", "KNOWS", "Person", 24_000),
                path("Person", "WORK_AT", "Organisation", 3_000),
                path("Organisation", "IS_LOCATED_IN", "Place", 7_500),
            ],
            [],
            [
                (("Place".to_string(), "name".to_string()), 1_100),
                (("Person".to_string(), "id".to_string()), 3_000),
            ],
            [],
        ),
    )
}

fn node<'a>(variable: &'a str, label: &'a str, equalities: &[&'a str]) -> PathNode<'a> {
    PathNode {
        variable,
        label,
        equalities: equalities.to_vec(),
        values: Default::default(),
        bound: false,
    }
}

fn edge(rel_type: &str, direction: RelationshipDirection) -> PathEdge<'_> {
    PathEdge {
        rel_type: Some(rel_type),
        equalities: Vec::new(),
        values: Default::default(),
        direction,
        min_hops: 1,
        max_hops: 1,
        reversible: true,
    }
}

/// `(f:Person)-[:WORK_AT]->(c:Organisation)-[:IS_LOCATED_IN]->(p:Place {name})`.
fn employees() -> (Vec<PathNode<'static>>, Vec<PathEdge<'static>>) {
    (
        vec![
            node("f", "Person", &[]),
            node("c", "Organisation", &[]),
            node("p", "Place", &["name"]),
        ],
        vec![edge("WORK_AT", Outgoing), edge("IS_LOCATED_IN", Outgoing)],
    )
}

#[test]
fn a_seekable_end_replaces_a_label_scan_start() {
    let catalog = catalog();
    let (nodes, edges) = employees();
    let Some(StartDecision::Chosen(choice)) = choose_start(&catalog, &nodes, &edges, 0) else {
        panic!("the Place seek should win");
    };
    assert_eq!(choice.start, 2);
    assert_eq!(choice.property, "name");
    // Scan 3,000 persons, read one job and one location each.
    assert_eq!(choice.current_work, 9_000.0);
    // Seek one place, read ~6.8 organisations and ~2.7 jobs.
    assert!(choice.work < 12.0, "{choice:?}");
}

#[test]
fn a_selective_current_start_is_kept_within_the_margin() {
    let catalog = catalog();
    // (person:Person {id})-[:KNOWS*1..2]-(friend:Person)-[:WORK_AT]->(c)-[:IS_LOCATED_IN]->(:Place {name})
    let nodes = vec![
        node("person", "Person", &["id"]),
        node("friend", "Person", &[]),
        node("c", "Organisation", &[]),
        node("p", "Place", &["name"]),
    ];
    let mut knows = edge("KNOWS", Undirected);
    knows.max_hops = 2;
    let edges = vec![
        knows,
        edge("WORK_AT", Outgoing),
        edge("IS_LOCATED_IN", Outgoing),
    ];
    let Some(StartDecision::Kept { detail }) = choose_start(&catalog, &nodes, &edges, 0) else {
        panic!("both ends seek; the walk back over KNOWS*1..2 is not cheaper");
    };
    assert!(detail.contains("current start person"), "{detail}");
}

#[test]
fn missing_or_ambiguous_statistics_keep_the_current_start() {
    let (mut nodes, edges) = employees();
    // No published path statistics.
    let without_paths = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("Place".to_string(), "name".to_string())], [], [], []),
        OptimizerCatalogStatistics::default(),
    );
    assert_eq!(choose_start(&without_paths, &nodes, &edges, 0), None);
    let catalog = catalog();
    // An unlabelled or multi-label current start has no estimate.
    for label in ["", "Person:Organisation"] {
        nodes[0].label = label;
        assert_eq!(choose_start(&catalog, &nodes, &edges, 0), None);
    }
    // A type disjunction has no fanout estimate.
    let (nodes, mut edges) = employees();
    edges[0].rel_type = None;
    assert_eq!(choose_start(&catalog, &nodes, &edges, 0), None);
}

#[test]
fn unindexed_bound_or_irreversible_candidates_are_not_seeks() {
    let catalog = catalog();
    let (mut nodes, mut edges) = employees();
    nodes[2].equalities = vec!["type"];
    assert_eq!(choose_start(&catalog, &nodes, &edges, 0), None);
    nodes[2].equalities = vec!["name"];
    nodes[2].bound = true;
    assert_eq!(choose_start(&catalog, &nodes, &edges, 0), None);
    nodes[2].bound = false;
    // Starting at p walks both steps backwards; a named var-length step
    // cannot be reversed.
    edges[0].reversible = false;
    assert!(matches!(
        choose_start(&catalog, &nodes, &edges, 0),
        Some(StartDecision::Kept { detail }) if detail.contains("p=unknown")
    ));
}

#[test]
fn reaching_a_bound_node_filters_and_incoming_steps_use_the_stored_orientation() {
    let catalog = catalog();
    let (mut nodes, edges) = employees();
    // From the place: 7,500 / 1,100 organisations per place, then 3,000 /
    // 7,500 jobs per organisation.
    let work = walk_work(&catalog, &nodes, &edges, 2).unwrap();
    let expected = 2.0 + 7_500.0 / 1_100.0 + 7_500.0 / 1_100.0 * 0.4;
    assert!((work - expected).abs() < 1e-9, "{work} vs {expected}");
    // A bound person start reads one job, then one location.
    nodes[0].bound = true;
    assert_eq!(walk_work(&catalog, &nodes, &edges, 0), Some(3.0));
    let reversed = [node("p", "Place", &[]), node("c", "Organisation", &[])];
    let incoming = [edge("IS_LOCATED_IN", Incoming)];
    assert_eq!(
        walk_work(&catalog, &reversed, &incoming, 1),
        Some(7_500.0 + 7_500.0)
    );
}
