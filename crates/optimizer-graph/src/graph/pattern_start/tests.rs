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
    // Scan 3,000 persons, then probe and read one job and one location each.
    assert_eq!(choice.current_work, 15_000.0);
    // Seek one place, probe its adjacency and the ~6.8 organisations,
    // then read ~6.8 organisations and ~2.7 jobs.
    assert!(choice.work < 20.0, "{choice:?}");
}

#[test]
fn directed_bounded_paths_replace_the_power_of_one_hop_fallback() {
    let mut catalog = OptimizerCatalog::new(
        OptimizerCatalogIndexes::new([("B".into(), "key".into())], [], [], []),
        OptimizerCatalogStatistics {
            label_counts: [("A".into(), 2_000), ("B".into(), 20)].into(),
            path_counts: [(("A".into(), "R".into(), "B".into()), 2_000)].into(),
            property_distinct_counts: [(("B".into(), "key".into()), 20)].into(),
            ..OptimizerCatalogStatistics::default()
        },
    );
    let nodes = [node("a", "A", &[]), node("b", "B", &["key"])];
    let edges = [PathEdge {
        min_hops: 2,
        max_hops: 2,
        ..edge("R", Outgoing)
    }];
    // Squaring the reverse one-hop fanout invents 10,000 paths per B,
    // even though the intermediate nodes have different degrees and labels.
    assert!(matches!(
        choose_start(&catalog, &nodes, &edges, 0),
        Some(StartDecision::Kept { .. })
    ));
    catalog
        .bounded_path_counts
        .insert(("A".into(), "R".into(), "B".into(), 2), 2_000);
    let Some(StartDecision::Chosen(choice)) = choose_start(&catalog, &nodes, &edges, 0) else {
        panic!("the measured reverse fanout should select the B seek");
    };
    assert_eq!(choice.start, 1);
    assert_eq!((choice.current_work, choice.work), (10_000.0, 303.0));
    assert_eq!(walk(&catalog, &nodes, &edges, 1).unwrap().rows, 100.0);

    let reverse_nodes = [nodes[1].clone(), nodes[0].clone()];
    let reverse_edges = [PathEdge {
        direction: Incoming,
        ..edges[0].clone()
    }];
    assert_eq!(
        walk(&catalog, &reverse_nodes, &reverse_edges, 0),
        walk(&catalog, &nodes, &edges, 1)
    );
}

#[test]
fn undirected_mixed_walks_do_not_use_directed_hop_counts() {
    let mut catalog = catalog();
    let nodes = [node("a", "Person", &[]), node("b", "Person", &[])];
    let edges = [PathEdge {
        min_hops: 0,
        max_hops: 2,
        ..edge("KNOWS", Undirected)
    }];
    let fallback = walk(&catalog, &nodes, &edges, 0);
    catalog
        .bounded_path_counts
        .insert(("Person".into(), "KNOWS".into(), "Person".into(), 2), 1);
    assert_eq!(walk(&catalog, &nodes, &edges, 0), fallback);
}

#[test]
fn complete_horizon_distinguishes_absent_paths_from_unknown_and_deeper_hops() {
    let mut catalog = catalog();
    // A non-empty bounded map does not itself establish complete coverage.
    catalog.bounded_path_counts.insert(
        ("Person".into(), "KNOWS".into(), "Person".into(), 1),
        24_000,
    );
    let nodes = [node("a", "Person", &[]), node("b", "Person", &[])];
    let mut edges = [PathEdge {
        min_hops: 2,
        max_hops: 2,
        ..edge("KNOWS", Outgoing)
    }];
    let unknown = walk(&catalog, &nodes, &edges, 0).unwrap();
    assert!(unknown.rows > 0.0);
    catalog.bounded_path_complete_through_hop = Some(1);
    assert_eq!(walk(&catalog, &nodes, &edges, 0).unwrap(), unknown);
    catalog.bounded_path_complete_through_hop = Some(3);
    let complete = walk(&catalog, &nodes, &edges, 0).unwrap();
    assert_eq!(complete.rows, 1.0);
    assert!(complete.work < unknown.work);
    assert!(complete.work >= 2.0 * 3_000.0 + 24_000.0);
    edges[0].direction = Incoming;
    assert_eq!(walk(&catalog, &nodes, &edges, 0).unwrap(), complete);
    edges[0].direction = Undirected;
    assert!(walk(&catalog, &nodes, &edges, 0).unwrap().rows > 0.0);
    edges[0].direction = Outgoing;
    edges[0].min_hops = 4;
    edges[0].max_hops = 4;
    catalog
        .bounded_path_counts
        .insert(("Person".into(), "KNOWS".into(), "Person".into(), 4), 0);
    assert!(walk(&catalog, &nodes, &edges, 0).unwrap().rows > 0.0);
    catalog.bounded_path_complete_through_hop = None;
    edges[0].min_hops = 2;
    edges[0].max_hops = 2;
    assert_eq!(walk(&catalog, &nodes, &edges, 0).unwrap(), unknown);
}

#[test]
fn completed_empty_statistics_keep_positive_planning_and_probe_estimates() {
    let mut catalog = catalog();
    catalog.path_counts.clear();
    catalog.bounded_path_counts.clear();
    let nodes = [node("a", "Person", &[]), node("b", "Person", &[])];
    let edges = [edge("KNOWS", Outgoing)];
    assert!(walk(&catalog, &nodes, &edges, 0).is_none());
    catalog.bounded_path_complete_through_hop = Some(3);
    let estimate = walk(&catalog, &nodes, &edges, 0).unwrap();
    assert!(estimate.rows > 0.0);
    assert!(estimate.work >= 2.0 * 3_000.0);
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
    let expected = 2.0 + 1.0 + 2.0 * 7_500.0 / 1_100.0 + 7_500.0 / 1_100.0 * 0.4;
    assert!((work - expected).abs() < 1e-9, "{work} vs {expected}");
    // A bound person start probes and reads one job, then one location.
    nodes[0].bound = true;
    assert_eq!(walk_work(&catalog, &nodes, &edges, 0), Some(5.0));
    let reversed = [node("p", "Place", &[]), node("c", "Organisation", &[])];
    let incoming = [edge("IS_LOCATED_IN", Incoming)];
    assert_eq!(
        walk_work(&catalog, &reversed, &incoming, 1),
        Some(7_500.0 + 2.0 * 7_500.0)
    );
}

#[test]
fn dead_end_prefix_work_is_not_discounted_by_relationship_filters() {
    let mut catalog = catalog();
    catalog.bounded_path_complete_through_hop = Some(3);
    catalog
        .rel_property_distinct_counts
        .insert(("KNOWS".into(), "flag".into()), 1_000);
    let nodes = [node("a", "Person", &[]), node("b", "Person", &[])];
    let mut edges = [PathEdge {
        min_hops: 2,
        max_hops: 2,
        ..edge("KNOWS", Outgoing)
    }];
    let unfiltered = walk(&catalog, &nodes, &edges, 0).unwrap();
    edges[0].equalities.push("flag");
    let filtered = walk(&catalog, &nodes, &edges, 0).unwrap();
    assert_eq!(filtered.rows, unfiltered.rows);
    assert_eq!(filtered.work, unfiltered.work);
}
