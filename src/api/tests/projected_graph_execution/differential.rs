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

use super::*;
use hawdb_analytics::{
    LouvainOptions, PageRankOptions, ProjectedGraph, ProjectionLayout, ProjectionMemoryBudget,
    ProjectionScanControl, ProjectionSource,
};
use hawdb_core::{HawDBError, RelTypeId};
use hawdb_executor::store::{GraphExecutionRead, ScanControl};
use hawdb_storage::{NodeRecord, RelId, RelRecord};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

const NODE_COUNT: u64 = 6;
const SELECTORS: [(&str, &[&str], &[&str]); 5] = [
    ("all", &[], &[]),
    ("selected", &["Memory"], &["LINK"]),
    ("other", &["Memory", "Source"], &["OTHER"]),
    ("no_edges", &["Memory"], &["MissingType"]),
    ("no_nodes", &["MissingLabel"], &["LINK"]),
];

#[derive(Clone)]
struct Edge {
    source: u64,
    target: u64,
    kind: &'static str,
    weight: i64,
}

struct Fixture {
    db: Option<Database>,
    path: PathBuf,
    mode: StorageResidencyMode,
    edges: BTreeMap<u64, Edge>,
    next_edge: u64,
}

impl Fixture {
    fn new(mode: StorageResidencyMode, seed: u64) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = loop {
            let path = std::env::temp_dir().join(format!(
                "hawdb-projection-{mode:?}-{seed}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            ));
            match std::fs::create_dir(&path) {
                Ok(()) => break path,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create projection fixture directory: {error}"),
            }
        };
        let db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let mut fixture = Self {
            db: Some(db),
            path,
            mode,
            edges: BTreeMap::new(),
            next_edge: 0,
        };
        for id in 0..NODE_COUNT {
            fixture
                .db()
                .query(&format!("CREATE (:{} {{id: {id}}})", node_label(id)))
                .unwrap();
        }
        // Preserve a canonical edge, a duplicate, a self-loop, another type,
        // and an edge crossing the selected node-label boundary.
        for (source, target, kind) in [
            (0, 1, "LINK"),
            (1, 2, "LINK"),
            (0, 1, "LINK"),
            (2, 2, "LINK"),
            (3, 4, "OTHER"),
            (2, 5, "LINK"),
        ] {
            fixture.append(source, target, kind);
        }
        for (name, labels, types) in SELECTORS {
            fixture
                .db()
                .query(&format!(
                    "CALL project_graph('{name}', {}, {})",
                    names(labels),
                    names(types)
                ))
                .unwrap();
        }
        fixture.db().checkpoint().unwrap();
        fixture.reopen(false);
        fixture
    }

    fn db(&mut self) -> &mut Database {
        self.db.as_mut().unwrap()
    }

    fn reopen(&mut self, read_only: bool) {
        drop(self.db.take());
        let db = Database::open_with_config(
            &self.path,
            DatabaseConfig {
                storage_residency_mode: self.mode,
                read_only,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_eq!(
            db.storage_residency_report().out_of_core,
            self.mode == StorageResidencyMode::OutOfCore
        );
        self.db = Some(db);
    }

    fn append(&mut self, source: u64, target: u64, kind: &'static str) {
        let id = self.next_edge;
        self.db().query(&format!(
            "MATCH (s:{} {{id: {source}}}), (t:{} {{id: {target}}}) CREATE (s)-[:{kind} {{rid: {id}, weight: 1}}]->(t)",
            node_label(source), node_label(target)
        )).unwrap();
        self.edges.insert(
            id,
            Edge {
                source,
                target,
                kind,
                weight: 1,
            },
        );
        self.next_edge += 1;
    }

    fn update(&mut self, id: u64, weight: i64) {
        let edge = self.edges[&id].clone();
        self.db()
            .query(&format!(
                "MATCH (s:{})-[r:{} {{rid: {id}}}]->(t:{}) SET r.weight = {weight}",
                node_label(edge.source),
                edge.kind,
                node_label(edge.target)
            ))
            .unwrap();
        self.edges.get_mut(&id).unwrap().weight = weight;
    }

    fn delete(&mut self, id: u64) {
        let edge = self.edges[&id].clone();
        self.db()
            .query(&format!(
                "MATCH (s:{})-[r:{} {{rid: {id}}}]->(t:{}) DELETE r",
                node_label(edge.source),
                edge.kind,
                node_label(edge.target)
            ))
            .unwrap();
        self.edges.remove(&id);
    }

    fn records(&self) -> Vec<RelRecord> {
        let catalog = &self.db.as_ref().unwrap().catalog;
        self.edges
            .iter()
            .map(|(&id, edge)| RelRecord {
                id: RelId(id),
                source: NodeId(edge.source),
                target: NodeId(edge.target),
                rel_type: catalog.rel_type_id(edge.kind).unwrap(),
                properties: BTreeMap::from([
                    ("rid".to_string(), Value::Int(id as i64)),
                    ("weight".to_string(), Value::Int(edge.weight)),
                ]),
            })
            .collect()
    }

    fn source(&self, labels: &[&str], types: &[&str]) -> OracleSource {
        let ids: BTreeSet<_> = (0..NODE_COUNT)
            .filter(|&id| labels.is_empty() || labels.contains(&node_label(id)))
            .map(NodeId)
            .collect();
        let relationships = self
            .records()
            .into_iter()
            .filter(|record| {
                ids.contains(&record.source)
                    && ids.contains(&record.target)
                    && (types.is_empty() || types.contains(&self.edges[&record.id.0].kind))
            })
            .collect();
        OracleSource {
            nodes: ids
                .into_iter()
                .map(|id| NodeRecord {
                    id,
                    labels: BTreeSet::new(),
                    properties: BTreeMap::new(),
                })
                .collect(),
            relationships,
        }
    }

    fn verify(&mut self, selector: usize, receipt: &str) {
        let before = frontier(&self.path);
        let epoch = self.db().commit_epoch();
        let expected = self.records();
        let db = self.db.as_ref().unwrap();
        for rel_type in [
            None,
            db.catalog.rel_type_id("LINK"),
            db.catalog.rel_type_id("OTHER"),
            Some(RelTypeId(u32::MAX)),
        ] {
            let selected: Vec<_> = expected
                .iter()
                .filter(|record| rel_type.is_none_or(|kind| kind == record.rel_type))
                .cloned()
                .collect();
            let mut actual = Vec::new();
            let control =
                GraphExecutionRead::visit_relationships_owned(&db.store, rel_type, &mut |record| {
                    actual.push(record);
                    Ok(ScanControl::Continue)
                })
                .unwrap();
            assert_eq!(control, ScanControl::Continue, "{receipt}");
            assert_eq!(actual, selected, "{receipt}");
            for fail in [false, true] {
                let mut prefix = Vec::new();
                let result = GraphExecutionRead::visit_relationships_owned(
                    &db.store,
                    rel_type,
                    &mut |record| {
                        prefix.push(record);
                        if fail {
                            Err(HawDBError::Semantic(
                                "relationship callback sentinel".into(),
                            ))
                        } else {
                            Ok(ScanControl::Stop)
                        }
                    },
                );
                assert_eq!(
                    prefix,
                    selected.iter().take(1).cloned().collect::<Vec<_>>(),
                    "{receipt}"
                );
                match (selected.is_empty(), fail, result) {
                    (true, _, Ok(ScanControl::Continue))
                    | (false, false, Ok(ScanControl::Stop)) => {}
                    (false, true, Err(HawDBError::Semantic(message))) => {
                        assert_eq!(message, "relationship callback sentinel", "{receipt}");
                    }
                    (_, _, result) => panic!("unexpected callback result {result:?}: {receipt}"),
                }
            }
        }

        let (name, labels, types) = SELECTORS[selector % SELECTORS.len()];
        // The oracle selects records directly from the mutation model. It
        // never scans GraphStore or calls the executor's projection adapter.
        let source = self.source(labels, types);
        let outgoing = source.project(ProjectionLayout::Outgoing);
        let undirected = source.project(ProjectionLayout::Undirected);
        let expected_rank: Vec<BTreeMap<String, Value>> = outgoing
            .page_rank(PageRankOptions {
                damping: 0.5,
                iterations: 3,
            })
            .into_iter()
            .map(|score| {
                BTreeMap::from([
                    ("node".into(), Value::Int(score.node.0 as i64)),
                    ("pagerank_score".into(), Value::Float(score.score)),
                ])
            })
            .collect();
        let expected_communities: Vec<BTreeMap<String, Value>> = undirected
            .hierarchical_louvain_communities(LouvainOptions {
                max_iterations: 3,
                max_levels: 2,
            })
            .into_iter()
            .map(|row| {
                BTreeMap::from([
                    ("node".into(), Value::Int(row.node.0 as i64)),
                    ("level".into(), Value::Int(row.level as i64)),
                    ("louvain_id".into(), Value::Int(row.community.0 as i64)),
                ])
            })
            .collect();
        let rank = self.db().query(&format!(
            "CALL page_rank('{name}', dampingFactor := 0.5, maxIterations := 3) RETURN node, pagerank_score"
        )).unwrap().rows.into_rows();
        let communities = self.db().query(&format!(
            "CALL louvain('{name}', maxIterations := 3, maxLevels := 2) RETURN node, level, louvain_id"
        )).unwrap().rows.into_rows();
        assert_eq!(rank, expected_rank, "{receipt}, selector={name}");
        assert_eq!(
            communities, expected_communities,
            "{receipt}, selector={name}"
        );
        assert_eq!(self.db().commit_epoch(), epoch, "{receipt}");
        assert_eq!(frontier(&self.path), before, "{receipt}");
        let out_of_core = self.mode == StorageResidencyMode::OutOfCore;
        assert_eq!(
            self.db().storage_residency_report().out_of_core,
            out_of_core
        );
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        drop(self.db.take());
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn node_label(id: u64) -> &'static str {
    if id < 4 {
        "Memory"
    } else {
        "Source"
    }
}

fn names(names: &[&str]) -> String {
    format!(
        "[{}]",
        names
            .iter()
            .map(|name| format!("'{name}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn frontier(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let result: BTreeMap<_, _> = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            entry.file_name() == "manifest.hawdb"
                || entry.file_name().to_string_lossy().starts_with("wal.")
        })
        .map(|entry| {
            (
                PathBuf::from(entry.file_name()),
                std::fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    assert!(result.contains_key(Path::new("manifest.hawdb")));
    assert!(
        result.len() >= 2,
        "frontier must include an actual WAL generation"
    );
    result
}

struct OracleSource {
    nodes: Vec<NodeRecord>,
    relationships: Vec<RelRecord>,
}

impl OracleSource {
    fn project(&self, layout: ProjectionLayout) -> ProjectedGraph {
        ProjectedGraph::try_from_store_with_node_filter_and_layout(
            self,
            None,
            |_| true,
            layout,
            ProjectionMemoryBudget::unlimited(),
        )
        .unwrap()
    }
}

impl ProjectionSource for OracleSource {
    fn visit_projection_nodes(
        &self,
        visitor: &mut dyn FnMut(NodeRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        for node in &self.nodes {
            if visitor(node.clone()) == ProjectionScanControl::Stop {
                return Ok(ProjectionScanControl::Stop);
            }
        }
        Ok(ProjectionScanControl::Continue)
    }

    fn visit_projection_relationships(
        &self,
        visitor: &mut dyn FnMut(RelRecord) -> ProjectionScanControl,
    ) -> std::result::Result<ProjectionScanControl, String> {
        for relationship in &self.relationships {
            if visitor(relationship.clone()) == ProjectionScanControl::Stop {
                return Ok(ProjectionScanControl::Stop);
            }
        }
        Ok(ProjectionScanControl::Continue)
    }
}

fn next_random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn campaign(seeds: u64) {
    let mut checks = 0;
    for seed in 0..seeds {
        for mode in [
            StorageResidencyMode::Materialized,
            StorageResidencyMode::OutOfCore,
        ] {
            let mut fixture = Fixture::new(mode, seed);
            for selector in 0..SELECTORS.len() {
                fixture.verify(selector, &format!("seed={seed} mode={mode:?} canonical"));
                checks += 1;
            }
            let mut random = seed;
            for step in 0..18 {
                match step % 3 {
                    0 => {
                        let source = next_random(&mut random) % NODE_COUNT;
                        let target = next_random(&mut random) % NODE_COUNT;
                        let kind = if next_random(&mut random) & 1 == 0 {
                            "LINK"
                        } else {
                            "OTHER"
                        };
                        fixture.append(source, target, kind);
                    }
                    1 => {
                        let position = if step == 1 {
                            0
                        } else {
                            next_random(&mut random) as usize % fixture.edges.len()
                        };
                        let id = *fixture.edges.keys().nth(position).unwrap();
                        fixture.update(id, step as i64 + 10);
                    }
                    _ => {
                        let position = if step == 2 {
                            1
                        } else {
                            next_random(&mut random) as usize % fixture.edges.len()
                        };
                        let id = *fixture.edges.keys().nth(position).unwrap();
                        fixture.delete(id);
                    }
                }
                let receipt = format!("seed={seed} mode={mode:?} step={step}");
                fixture.verify(step, &receipt);
                checks += 1;
                if step % 6 == 2 {
                    // Reopen with a nonempty WAL before any checkpoint can
                    // hide an incorrect base/delta merge during replay.
                    fixture.reopen(false);
                    fixture.verify(step, &format!("{receipt} wal-replay"));
                    checks += 1;
                }
                if step % 6 == 5 {
                    fixture.db().checkpoint().unwrap();
                    fixture.reopen(false);
                    fixture.verify(step, &format!("{receipt} checkpoint"));
                    checks += 1;
                }
            }
            fixture.append(0, 2, "LINK");
            for _ in 0..2 {
                fixture.reopen(true);
                fixture.verify(
                    1,
                    &format!("seed={seed} mode={mode:?} read-only-wal-replay"),
                );
                checks += 1;
            }
        }
    }
    eprintln!("graph-projection-residency-v1 seeds={seeds} state_checks={checks}");
}

#[test]
fn projected_graph_residency_differential_smoke() {
    campaign(2);
}

#[test]
#[ignore = "explicit local graph projection residency campaign"]
fn projected_graph_residency_differential_campaign() {
    campaign(32);
}

#[test]
fn projected_graph_registration_and_admission_include_canonical_edges() {
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let mut fixture = Fixture::new(mode, 0);
        let output = fixture
            .db()
            .query("CALL project_graph('fresh', ['Memory'], ['LINK'])")
            .unwrap();
        assert_eq!(output.rows[0].get("node_count"), Some(&Value::Int(4)));
        assert_eq!(output.rows[0].get("edge_count"), Some(&Value::Int(3)));
        for (algorithm, layout) in [
            ("page_rank", ProjectionLayout::Outgoing),
            ("louvain", ProjectionLayout::Undirected),
        ] {
            let bytes = fixture
                .source(&["Memory"], &["LINK"])
                .project(layout)
                .memory_estimate()
                .estimated_bytes;
            drop(fixture.db.take());
            let mut config = DatabaseConfig {
                storage_residency_mode: mode,
                read_only: true,
                ..DatabaseConfig::default()
            };
            config.execution_memory.blocking_operator_bytes =
                std::num::NonZeroUsize::new(bytes - 1).unwrap();
            fixture.db = Some(Database::open_with_config(&fixture.path, config).unwrap());
            let before = frontier(&fixture.path);
            let epoch = fixture.db().commit_epoch();
            let error = fixture
                .db()
                .query(&format!("CALL {algorithm}('selected')"))
                .unwrap_err();
            assert!(
                error.to_string().contains(&format!(
                    "requires an estimated {bytes} bytes for 4 nodes and 4 relationships"
                )),
                "{mode:?} {algorithm}: {error}"
            );
            assert_eq!(fixture.db().commit_epoch(), epoch);
            assert_eq!(frontier(&fixture.path), before);
        }
    }
}

#[test]
fn projected_graph_relationship_corruption_fails_without_partial_results() {
    for algorithm in ["page_rank", "louvain"] {
        let mut fixture = Fixture::new(StorageResidencyMode::OutOfCore, 0);
        // Publish a fresh generation without reopening: startup may have
        // cached verified segments, which must not mask this injected read error.
        fixture.db().checkpoint().unwrap();
        let before = frontier(&fixture.path);
        let epoch = fixture.db().commit_epoch();
        // Canonical relationship segments follow the node segments. Corrupt
        // only the final relationship payload, leaving the header/length intact.
        let generation = fixture
            .db()
            .storage_residency_report()
            .canonical_generation
            .unwrap();
        let canonical = fixture.path.join(format!("canonical.{generation}.hawdb"));
        let mut bytes = std::fs::read(&canonical).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        std::fs::write(canonical, bytes).unwrap();
        let mut nodes = 0;
        GraphExecutionRead::visit_nodes_owned(&fixture.db().store, None, &mut |_| {
            nodes += 1;
            Ok(ScanControl::Continue)
        })
        .unwrap();
        assert_eq!(nodes, NODE_COUNT);
        let error = fixture
            .db()
            .query(&format!("CALL {algorithm}('selected')"))
            .unwrap_err();
        assert!(
            error.to_string().contains("content digest verification"),
            "{error}"
        );
        assert_eq!(
            fixture
                .db()
                .storage_residency_report()
                .segment_cache_digest_mismatch_count,
            1
        );
        assert_eq!(fixture.db().commit_epoch(), epoch);
        assert_eq!(frontier(&fixture.path), before);
    }
}
