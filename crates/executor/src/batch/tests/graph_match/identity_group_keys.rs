// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Grouping and `WITH DISTINCT` by entity identity (#1615), compared with the
//! map-keyed grouping and map-column planning it replaced on random graphs.
//!
//! The legacy side plans with `with_legacy_graph_group_keys` (grouped graph
//! variables become map columns, `WITH DISTINCT` stays `DistinctExec`) and
//! groups by materialized maps (`with_legacy_map_keys`). Both sides run the
//! same queries over the same graph, in memory and with forced spills, and
//! must return the same rows: as multisets, or in order when the query
//! orders totally.

use super::*;
use crate::expression::with_legacy_map_keys;
use crate::memory_ledger::QueryMemoryLedgerSnapshot;
use hawdb_plan_cypher::GraphGroupKeys;
use hawdb_storage::{RelId, RelRecord};
use std::collections::BTreeSet;

/// A deterministic xorshift generator; the graphs only need variety.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

struct Graph {
    catalog: Catalog,
    nodes: Vec<NodeRecord>,
    relationships: Vec<RelRecord>,
}

/// Nodes draw properties from small domains, so different nodes often have
/// equal properties; some properties are missing and one name sorts before
/// `_id`. Relationships include self-loops and parallel edges.
fn random_graph(seed: u64) -> Graph {
    let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut catalog = Catalog::default();
    let a = catalog.get_or_create_label("A");
    let b = catalog.get_or_create_label("B");
    let r = catalog.get_or_create_rel_type("R");
    let s = catalog.get_or_create_rel_type("S");
    let node_count = 6 + random.below(40);
    let nodes = (0..node_count)
        .map(|id| {
            let mut properties = BTreeMap::new();
            if random.chance(75) {
                let name = ["x", "y", "z"][random.below(3) as usize];
                properties.insert("name".to_string(), Value::String(name.to_string()));
            }
            if random.chance(70) {
                properties.insert("age".to_string(), Value::Int(random.below(4) as i64));
            }
            if random.chance(30) {
                properties.insert("Rank".to_string(), Value::Int(random.below(3) as i64));
            }
            if random.chance(30) {
                properties.insert(
                    "tags".to_string(),
                    Value::List(
                        (0..random.below(3))
                            .map(|tag| Value::Int(tag as i64))
                            .collect(),
                    ),
                );
            }
            if random.chance(20) {
                properties.insert(
                    "meta".to_string(),
                    Value::Map(BTreeMap::from([(
                        "depth".to_string(),
                        Value::Int(random.below(2) as i64),
                    )])),
                );
            }
            let labels = match random.below(4) {
                0 => vec![a, b],
                1 | 2 => vec![a],
                _ => vec![b],
            };
            NodeRecord {
                id: NodeId(id),
                labels: labels.into_iter().collect(),
                properties,
            }
        })
        .collect::<Vec<_>>();
    let relationship_count = node_count + random.below(node_count * 3);
    let relationships = (0..relationship_count)
        .map(|id| {
            let mut properties = BTreeMap::new();
            if random.chance(70) {
                properties.insert("w".to_string(), Value::Int(random.below(3) as i64));
            }
            RelRecord {
                id: RelId(id),
                source: NodeId(random.below(node_count)),
                target: NodeId(random.below(node_count)),
                rel_type: if random.chance(60) { r } else { s },
                properties,
            }
        })
        .collect();
    Graph {
        catalog,
        nodes,
        relationships,
    }
}

/// Queries and whether their rows are totally ordered.
const QUERIES: &[(&str, bool)] = &[
    // Node keys, then property access through the carried record.
    (
        "MATCH (a:A)-[r]->(b) WITH a, count(*) AS c RETURN a.name AS name, a.age AS age, a.Rank AS rank, c",
        false,
    ),
    // Relationship keys.
    (
        "MATCH (a)-[r]->(b) WITH r, count(*) AS c RETURN r.w AS w, type(r) AS t, id(r) AS i, c",
        false,
    ),
    // Entities mixed with scalar keys.
    (
        "MATCH (a)-[r]->(b) WITH a, b.name AS bn, r.w AS w, count(*) AS c RETURN a.name AS an, a.tags AS tags, bn, w, c",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH a, r, b, count(*) AS c RETURN a.age AS aa, r.w AS w, b.name AS bn, b.meta AS meta, c",
        false,
    ),
    // NULL node and relationship keys from OPTIONAL MATCH.
    (
        "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:B) WITH a, b, count(*) AS c RETURN a.name AS an, b.name AS bn, b IS NULL AS missing, c",
        false,
    ),
    (
        "MATCH (a:B) OPTIONAL MATCH (a)-[r:S]->(b) WITH r, count(*) AS c RETURN r.w AS w, r IS NULL AS missing, c",
        false,
    ),
    // WITH DISTINCT over entities, with and without scalars.
    (
        "MATCH (a)-[]->(b) WITH DISTINCT a, b RETURN a.name AS an, b.name AS bn, id(a) AS ai, id(b) AS bi",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH DISTINCT b, r.w AS w RETURN b.age AS age, b.Rank AS rank, w",
        false,
    ),
    (
        "MATCH (a:A) OPTIONAL MATCH (a)-[:S]->(b) WITH DISTINCT b RETURN b.name AS bn, b IS NULL AS missing",
        false,
    ),
    // First-occurrence order survives the in-memory DISTINCT grouping,
    // including LIMIT. A spilled DISTINCT emits in key order on both sides,
    // and maps and identities order differently, so a spilled LIMIT without
    // ORDER BY may select other rows; it is compared by size there.
    (
        "MATCH (a)-[]->(b) WITH DISTINCT a LIMIT 4 RETURN a.name AS an, id(a) AS ai",
        true,
    ),
    // DISTINCT over a grouping.
    (
        "MATCH (a)-[]->(b) WITH a, count(*) AS c WITH DISTINCT a, c RETURN a.name AS an, c",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH DISTINCT a, count(*) AS c RETURN a.name AS an, c",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH a, count(*) AS c RETURN DISTINCT c",
        false,
    ),
    // RETURN keeps entity maps.
    ("MATCH (a)-[]->(b) RETURN a, count(*) AS c", false),
    ("MATCH (a)-[r]->(b) RETURN DISTINCT r, b", false),
    (
        "MATCH (a)-[r]->(b) WITH a, count(DISTINCT b) AS c, collect(DISTINCT b) AS bs RETURN a, c, size(bs) AS n",
        false,
    ),
    // Later clauses read the carried variable natively.
    (
        "MATCH (a)-[]->(b) WITH a, collect(b.name) AS names MATCH (a)-[:R]->(x) RETURN a.name AS an, size(names) AS n, x.name AS xn",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH a, sum(r.w) AS s ORDER BY s DESC, id(a) LIMIT 5 RETURN a.name AS an, s",
        true,
    ),
    (
        "MATCH (a)-[r]->(b) WITH a, count(*) AS c WHERE c > 1 AND a.age IS NOT NULL RETURN a.name AS an, a.age AS age, c",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH a AS x, count(*) AS c RETURN x.name AS xn, x, c",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH a, b, count(r) AS c WITH a, sum(c) AS t RETURN a.name AS an, t",
        false,
    ),
    (
        "MATCH (a) WITH a, count(*) AS c MATCH (a)-[r]->(b) WITH b, sum(c) AS s RETURN b, s",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH a, collect(b) AS bs UNWIND bs AS x RETURN a.name AS an, x.name AS xn",
        false,
    ),
    (
        "MATCH (a)-[]->(b) WITH DISTINCT a RETURN a.name AS an, size([(a)-[:R]->(x) | x.name]) AS k",
        false,
    ),
    (
        "MATCH (a)-[r]->(b) WITH r, count(*) AS c RETURN r, c ORDER BY id(r)",
        true,
    ),
    (
        "MATCH (a)-[r]->(b) WITH b, r.w AS w, count(*) AS c ORDER BY b.name, w, id(b) RETURN b.name AS bn, w, c",
        true,
    ),
];

fn memory_configs(directory: &std::path::Path) -> [(&'static str, ExecutionMemoryConfig); 2] {
    [
        ("in-memory", ExecutionMemoryConfig::default()),
        (
            "spill",
            ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(20 * 1024).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(16 * 1024).unwrap(),
                batch_rows: NonZeroUsize::new(3).unwrap(),
                query_memory_bytes: NonZeroUsize::new(512 * 1024).unwrap(),
                min_spill_free_bytes: std::num::NonZeroU64::MIN,
                spill_directory: directory.to_path_buf(),
                ..ExecutionMemoryConfig::default()
            },
        ),
    ]
}

struct Outcome {
    rows: Result<Vec<BTreeMap<String, Value>>>,
    spilled_aggregates: usize,
    ledger: QueryMemoryLedgerSnapshot,
}

fn run(graph: &Graph, plan: &PhysicalPlan, memory: &ExecutionMemoryConfig) -> Outcome {
    let store = store::ReadFixture {
        nodes: graph.nodes.clone(),
        relationships: graph.relationships.clone(),
        definition: Some(hawdb_storage::projection::ProjectedGraphDefinition {
            node_labels: vec!["A".into(), "B".into()],
            rel_types: vec!["R".into(), "S".into()],
            relationship_predicates: BTreeMap::new(),
        }),
        out_of_core: false,
        ..store::ReadFixture::default()
    };
    let ledger = QueryMemoryLedger::new(memory.query_memory_bytes);
    let parameters = BTreeMap::new();
    let mut external = NoExternalReadOperator;
    let external = BatchExternalReadAdapter::new(&mut external);
    let observer = QueryExecutionObserver::default();
    let mut rows = Vec::new();
    let result = execute_binding_batches(
        plan,
        BatchReadContext {
            catalog: &graph.catalog,
            store: &store,
            memory,
            memory_ledger: &ledger,
            parameters: &parameters,
            external: &external,
            observer: &observer,
            host_scorer: None,
            task_context: None,
        },
        ExecutionLimit::unlimited(),
        &mut |batch| {
            rows.extend(batch.into_iter().map(|binding| binding.values));
            Ok(BatchControl::Continue)
        },
    );
    let spilled_aggregates = observer
        .into_reports()
        .blocking_memory
        .iter()
        .filter(|report| report.operator.starts_with("AggregateExec") && report.spill_run_count > 0)
        .count();
    Outcome {
        rows: result.map(|_| rows),
        spilled_aggregates,
        ledger: ledger.snapshot(),
    }
}

fn plan(query: &str, legacy: bool) -> PhysicalPlan {
    let planned = if legacy {
        hawdb_plan_cypher::with_legacy_graph_group_keys(|| {
            hawdb_plan_cypher::plan_pipeline_query(query, &BTreeMap::new())
        })
    } else {
        hawdb_plan_cypher::plan_pipeline_query(query, &BTreeMap::new())
    };
    lower(planned.unwrap_or_else(|error| panic!("{query}: {error}")))
}

fn graph_key_modes(plan: &PhysicalPlan, modes: &mut Vec<GraphGroupKeys>) {
    hawdb_plan_cypher::visit_plan(plan, &mut |node| {
        if let PhysicalPlan::AggregateExec { graph_keys, .. } = node {
            modes.push(*graph_keys);
        }
    });
}

fn spill_directory(seed: u64) -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-identity-group-keys-{}-{seed}",
        std::process::id()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    directory
}

#[test]
fn identity_grouping_matches_map_grouping_on_random_graphs() {
    let mut slotted_queries = BTreeSet::new();
    let mut spilled_queries = BTreeSet::new();
    for seed in 1..=10u64 {
        let graph = random_graph(seed);
        let directory = spill_directory(seed);
        for (config, memory) in memory_configs(&directory) {
            for (query, ordered) in QUERIES {
                let current = plan(query, false);
                let legacy = plan(query, true);
                let mut modes = Vec::new();
                graph_key_modes(&current, &mut modes);
                if modes.iter().any(|mode| mode.retains_slots()) {
                    slotted_queries.insert(*query);
                }
                let mut legacy_modes = Vec::new();
                graph_key_modes(&legacy, &mut legacy_modes);
                assert!(
                    legacy_modes.iter().all(|mode| !mode.retains_slots()),
                    "{query}"
                );
                let actual = run(&graph, &current, &memory);
                let expected = with_legacy_map_keys(|| run(&graph, &legacy, &memory));
                let context = format!("seed {seed}, {config}: {query}");
                assert_eq!(actual.ledger.used_bytes, 0, "{context}");
                assert_eq!(expected.ledger.used_bytes, 0, "{context}");
                if actual.spilled_aggregates > 0 {
                    spilled_queries.insert(*query);
                }
                let (mut actual, mut expected) = (
                    actual
                        .rows
                        .unwrap_or_else(|error| panic!("{context}: {error}")),
                    expected
                        .rows
                        .unwrap_or_else(|error| panic!("{context}: legacy {error}")),
                );
                let unordered_limit = query.contains("LIMIT") && !query.contains("ORDER BY");
                if unordered_limit && config == "spill" {
                    assert_eq!(actual.len(), expected.len(), "{context}");
                    continue;
                }
                if !ordered {
                    actual.sort();
                    expected.sort();
                }
                assert_eq!(actual, expected, "{context}");
            }
        }
        std::fs::remove_dir_all(&directory).ok();
    }
    // Every WITH grouping or WITH DISTINCT over a bound variable carries slots;
    // RETURN and an aggregating WITH DISTINCT keep map columns.
    let map_columns = [
        "MATCH (a)-[]->(b) WITH DISTINCT a, count(*) AS c RETURN a.name AS an, c",
        "MATCH (a)-[]->(b) RETURN a, count(*) AS c",
        "MATCH (a)-[r]->(b) RETURN DISTINCT r, b",
    ];
    for (query, _) in QUERIES {
        assert_eq!(
            slotted_queries.contains(query),
            !map_columns.contains(query),
            "{query}"
        );
    }
    // Forced spills reach node, relationship and NULL keys, partial (count)
    // and buffered (collect) aggregation, and the DISTINCT grouping.
    assert!(
        spilled_queries.len() * 4 >= QUERIES.len() * 3,
        "too few spilled aggregations: {spilled_queries:?}"
    );
    for query in [
        "MATCH (a:A)-[r]->(b) WITH a, count(*) AS c RETURN a.name AS name, a.age AS age, a.Rank AS rank, c",
        "MATCH (a)-[r]->(b) WITH r, count(*) AS c RETURN r.w AS w, type(r) AS t, id(r) AS i, c",
        "MATCH (a:A) OPTIONAL MATCH (a)-[:R]->(b:B) WITH a, b, count(*) AS c RETURN a.name AS an, b.name AS bn, b IS NULL AS missing, c",
        "MATCH (a)-[]->(b) WITH a, collect(b) AS bs UNWIND bs AS x RETURN a.name AS an, x.name AS xn",
        "MATCH (a)-[]->(b) WITH DISTINCT a, b RETURN a.name AS an, b.name AS bn, id(a) AS ai, id(b) AS bi",
    ] {
        assert!(spilled_queries.contains(query), "{query} never spilled");
    }
}
