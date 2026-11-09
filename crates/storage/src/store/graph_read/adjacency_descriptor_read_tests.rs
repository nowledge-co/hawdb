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

//! #1615: adjacency expansion over demand-paged descriptor trees. Every read
//! path (cold and warm cache, with and without a `DescriptorLeafScope`) must
//! return exactly the adjacency an independent relationship scan implies, for
//! resident, out-of-core and out-of-core-with-overlay stores.

use super::*;
use crate::read_view::{ControlledGraphReadAllocator, GraphReadAllocation};

// Enough (endpoint, direction, type) blocks that the adjacency descriptor
// tree spans several leaves under the default 4096-entry page limit.
const NODES: i64 = 3_600;

struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Permit(usize);
impl GraphReadAllocation for Permit {
    fn bytes(&self) -> usize {
        self.0
    }
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.0 += bytes;
        Ok(())
    }
}

fn label(ordinal: i64) -> &'static str {
    if ordinal % 3 == 0 {
        "Class"
    } else {
        "Tag"
    }
}

/// A resident (never checkpointed) store when `checkpoint` is false.
fn fixture(checkpoint: bool) -> (Fixture, GraphStore, Catalog, Vec<NodeId>) {
    let path = std::env::temp_dir().join(format!(
        "hawdb-adjacency-descriptor-reads-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let directory = Fixture(path.clone());
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::SyncOnCheckpoint,
        WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..WalReplayConfig::default()
        },
    )
    .unwrap();
    let nodes = (0..NODES)
        .map(|ordinal| {
            store
                .create_node(
                    &mut catalog,
                    label(ordinal),
                    BTreeMap::from([("id".into(), Value::Int(ordinal))]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let classes = NODES as usize / 3;
    for (ordinal, source) in nodes.iter().enumerate() {
        if ordinal % 3 == 0 {
            // Classes form a subclass forest with chains of length ~log2 n.
            let class = ordinal / 3;
            if class > 0 {
                store
                    .create_relationship(
                        &mut catalog,
                        *source,
                        nodes[class / 2 * 3],
                        "IS_SUBCLASS_OF",
                        BTreeMap::new(),
                    )
                    .unwrap();
            }
            continue;
        }
        // Every tag has a class; some have a parallel duplicate edge.
        let target = nodes[(ordinal * 7) % classes * 3];
        for _ in 0..if ordinal % 4 == 1 { 2 } else { 1 } {
            store
                .create_relationship(&mut catalog, *source, target, "HAS_TYPE", BTreeMap::new())
                .unwrap();
        }
    }
    if checkpoint {
        store.checkpoint(&catalog).unwrap();
        assert!(store.is_out_of_core());
        assert!(store.nodes.is_empty());
        assert!(store.relationships.is_empty());
    } else {
        assert!(store.canonical_base.is_none());
    }
    (directory, store, catalog, nodes)
}

type Selection = (u64, AdjacencyDirection, Option<u32>);
type Adjacency = Vec<(Selection, Vec<(u64, u64)>)>;

fn selections(catalog: &Catalog) -> Vec<(AdjacencyDirection, Option<RelTypeId>)> {
    let mut selections = Vec::new();
    for direction in [AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming] {
        selections.push((direction, None));
        for rel_type in ["HAS_TYPE", "IS_SUBCLASS_OF"] {
            selections.push((direction, Some(catalog.rel_type_id(rel_type).unwrap())));
        }
    }
    selections
}

fn adjacency(store: &GraphStore, catalog: &Catalog, sources: &[NodeId]) -> Adjacency {
    let mut admit = |bytes| Ok(Some(Box::new(Permit(bytes)) as Box<dyn GraphReadAllocation>));
    let mut rows = Vec::new();
    for &source in sources {
        for (direction, rel_type) in selections(catalog) {
            let mut entries = Vec::new();
            store
                .visit_ordered_adjacent_relationships_with_allocation(
                    source,
                    rel_type,
                    direction,
                    usize::MAX,
                    &mut ControlledGraphReadAllocator::new(&mut admit, None),
                    |relationship| {
                        let relationship = relationship.relationship();
                        let neighbor = match direction {
                            AdjacencyDirection::Outgoing => relationship.target,
                            AdjacencyDirection::Incoming => relationship.source,
                        };
                        entries.push((neighbor.0, relationship.id.0));
                        Ok(GraphScanControl::Continue)
                    },
                )
                .unwrap();
            rows.push(((source.0, direction, rel_type.map(|id| id.0)), entries));
        }
    }
    rows
}

/// The same selections derived from an independent full relationship scan.
fn expected(store: &GraphStore, catalog: &Catalog, sources: &[NodeId]) -> Adjacency {
    let relationships = store
        .relationship_records_owned()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    // (endpoint, direction) -> (rel type, neighbor, relationship).
    let mut index = BTreeMap::<(u64, bool), Vec<(RelTypeId, u64, u64)>>::new();
    for relationship in &relationships {
        index
            .entry((relationship.source.0, true))
            .or_default()
            .push((
                relationship.rel_type,
                relationship.target.0,
                relationship.id.0,
            ));
        index
            .entry((relationship.target.0, false))
            .or_default()
            .push((
                relationship.rel_type,
                relationship.source.0,
                relationship.id.0,
            ));
    }
    let mut rows = Vec::new();
    for &source in sources {
        for (direction, rel_type) in selections(catalog) {
            let outgoing = direction == AdjacencyDirection::Outgoing;
            let mut entries = index
                .get(&(source.0, outgoing))
                .into_iter()
                .flatten()
                .filter(|(candidate, _, _)| rel_type.is_none_or(|id| *candidate == id))
                .map(|(_, neighbor, id)| (*neighbor, *id))
                .collect::<Vec<_>>();
            entries.sort_unstable();
            rows.push(((source.0, direction, rel_type.map(|id| id.0)), entries));
        }
    }
    rows
}

fn sorted(mut rows: Adjacency) -> Adjacency {
    for (_, entries) in &mut rows {
        entries.sort_unstable();
    }
    rows
}

/// `(tag:Tag)-[:HAS_TYPE|IS_SUBCLASS_OF*0..]->(class)` reachability per Tag,
/// expanded one typed adjacency read per hop as the executor does.
fn var_length(store: &GraphStore, catalog: &Catalog) -> Vec<(u64, Vec<u64>)> {
    let tag = catalog.label_id("Tag").unwrap();
    let types = ["HAS_TYPE", "IS_SUBCLASS_OF"].map(|name| catalog.rel_type_id(name).unwrap());
    let mut admit = |bytes| Ok(Some(Box::new(Permit(bytes)) as Box<dyn GraphReadAllocation>));
    let mut sources = Vec::new();
    store
        .visit_projected_nodes_with_allocation(
            Some(tag),
            &BTreeSet::from(["id".to_owned()]),
            &mut ControlledGraphReadAllocator::new(&mut admit, None),
            &mut |node| {
                sources.push(node.into_parts().0.id);
                Ok(GraphScanControl::Continue)
            },
        )
        .unwrap();
    let mut rows = Vec::new();
    for source in sources {
        let mut reached = BTreeSet::from([source.0]);
        let mut frontier = vec![source];
        while let Some(node) = frontier.pop() {
            for rel_type in types {
                store
                    .visit_ordered_adjacency_entries_with_allocation(
                        (node, Some(rel_type), AdjacencyDirection::Outgoing),
                        usize::MAX,
                        &mut ControlledGraphReadAllocator::new(&mut admit, None),
                        |entry| {
                            if reached.insert(entry.neighbor_id.0) {
                                frontier.push(entry.neighbor_id);
                            }
                            Ok(GraphScanControl::Continue)
                        },
                    )
                    .unwrap();
            }
        }
        rows.push((source.0, reached.into_iter().collect()));
    }
    rows
}

fn expected_var_length(store: &GraphStore, catalog: &Catalog) -> Vec<(u64, Vec<u64>)> {
    let tag = catalog.label_id("Tag").unwrap();
    let types = ["HAS_TYPE", "IS_SUBCLASS_OF"].map(|name| catalog.rel_type_id(name).unwrap());
    let mut outgoing = BTreeMap::<u64, Vec<u64>>::new();
    for relationship in store.relationship_records_owned().map(Result::unwrap) {
        if types.contains(&relationship.rel_type) {
            outgoing
                .entry(relationship.source.0)
                .or_default()
                .push(relationship.target.0);
        }
    }
    let mut rows = Vec::new();
    for node in store.node_records_owned().map(Result::unwrap) {
        if !node.labels.contains(&tag) {
            continue;
        }
        let mut reached = BTreeSet::from([node.id.0]);
        let mut frontier = vec![node.id.0];
        while let Some(next) = frontier.pop() {
            for &neighbor in outgoing.get(&next).into_iter().flatten() {
                if reached.insert(neighbor) {
                    frontier.push(neighbor);
                }
            }
        }
        rows.push((node.id.0, reached.into_iter().collect()));
    }
    rows
}

fn apply_overlay(store: &mut GraphStore, catalog: &mut Catalog, nodes: &[NodeId]) {
    // Uncheckpointed additions, a new endpoint and tombstoned base edges.
    let added = store
        .create_node(
            catalog,
            "Tag",
            BTreeMap::from([("id".into(), Value::Int(-1))]),
        )
        .unwrap();
    for (source, target, rel_type) in [
        (nodes[10], nodes[2_001], "HAS_TYPE"),
        (nodes[1_998], nodes[3], "IS_SUBCLASS_OF"),
        (added, nodes[6], "HAS_TYPE"),
        (nodes[6], added, "IS_SUBCLASS_OF"),
    ] {
        store
            .create_relationship(catalog, source, target, rel_type, BTreeMap::new())
            .unwrap();
    }
    let deleted = store
        .delete_relationships(
            catalog,
            RelationshipDeleteRequest {
                source_label: "Tag".into(),
                filter: Some(PropertyFilter::Eq {
                    property: "id".into(),
                    value: Value::Int(5),
                }),
                rel_type: "HAS_TYPE".into(),
                target_label: "Class".into(),
                target_filter: None,
                rel_filter: None,
            },
        )
        .unwrap();
    assert_eq!(deleted.len(), 2, "both parallel base edges are tombstoned");
}

fn probe_nodes(store: &GraphStore) -> Vec<NodeId> {
    store
        .node_records_owned()
        .map(Result::unwrap)
        .map(|node| node.id)
        .collect()
}

#[test]
fn adjacency_reads_match_an_independent_scan_resident_and_out_of_core() {
    let (_resident_directory, resident, resident_catalog, _) = fixture(false);
    let (_directory, store, catalog, _) = fixture(true);
    let sources = probe_nodes(&store);
    assert_eq!(sources, probe_nodes(&resident));

    let reference = expected(&resident, &resident_catalog, &sources);
    assert_eq!(expected(&store, &catalog, &sources), reference);
    assert_eq!(
        sorted(adjacency(&resident, &resident_catalog, &sources)),
        reference
    );
    // Cold, then warm (cached descriptor pages), out-of-core reads.
    let cold = adjacency(&store, &catalog, &sources);
    assert_eq!(sorted(cold.clone()), reference);
    let before = store.segment_cache_snapshot().unwrap();
    let warm = adjacency(&store, &catalog, &sources);
    assert_eq!(warm, cold, "warm reads keep the exact order");
    let after = store.segment_cache_snapshot().unwrap();
    assert_eq!(after.insertion_count, before.insertion_count);
    assert_eq!(after.pinned_bytes, 0);

    let reachability = expected_var_length(&resident, &resident_catalog);
    assert!(reachability.iter().any(|(_, reached)| reached.len() > 4));
    assert_eq!(var_length(&resident, &resident_catalog), reachability);
    assert_eq!(var_length(&store, &catalog), reachability);
}

#[test]
fn out_of_core_adjacency_reads_merge_uncheckpointed_overlay() {
    let (_resident_directory, mut resident, mut resident_catalog, resident_nodes) = fixture(false);
    let (_directory, mut store, mut catalog, nodes) = fixture(true);
    // Warm the descriptor pages before the overlay exists.
    adjacency(&store, &catalog, &nodes);
    apply_overlay(&mut resident, &mut resident_catalog, &resident_nodes);
    apply_overlay(&mut store, &mut catalog, &nodes);
    assert!(!store.relationships.is_empty());
    assert!(!store.relationship_tombstones.is_empty());

    let sources = probe_nodes(&store);
    let reference = expected(&resident, &resident_catalog, &sources);
    assert_eq!(expected(&store, &catalog, &sources), reference);
    assert_eq!(
        sorted(adjacency(&resident, &resident_catalog, &sources)),
        reference
    );
    let first = adjacency(&store, &catalog, &sources);
    assert_eq!(sorted(first.clone()), reference);
    assert_eq!(adjacency(&store, &catalog, &sources), first);
    let reachability = expected_var_length(&resident, &resident_catalog);
    assert_eq!(expected_var_length(&store, &catalog), reachability);
    assert_eq!(var_length(&store, &catalog), reachability);
}

// #1615: inside a `DescriptorLeafScope` consecutive endpoint seeks reuse the
// verified descriptor leaf an earlier seek reached. Every scoped read must
// equal the unscoped read exactly, before and after an uncheckpointed overlay,
// and the scope must release its pins.
#[test]
fn scoped_adjacency_reads_reuse_retained_leaves_and_match_unscoped_reads() {
    use crate::graph_descriptor_tree::{
        descriptor_retained_leaf_hits_for_test, DescriptorLeafScope,
    };
    let (_directory, mut store, mut catalog, nodes) = fixture(true);
    // The adjacency descriptor tree has interior pages: a cold seek reads
    // more than one descriptor page.
    let (report, _) = store
        .canonical_adjacency
        .as_ref()
        .unwrap()
        .scan_endpoint_inputs_control(nodes[1], AdjacencyDirection::Outgoing, None, |_| {
            Ok(CanonicalScanControl::Continue)
        })
        .unwrap();
    assert!(report.descriptor_pages_visited >= 2, "{report:?}");
    assert_eq!(report.descriptor_retained_leaf_hits, 0);

    for overlay in [false, true] {
        if overlay {
            apply_overlay(&mut store, &mut catalog, &nodes);
        }
        let sources = probe_nodes(&store);
        let unscoped = adjacency(&store, &catalog, &sources);
        let unscoped_reachability = var_length(&store, &catalog);
        assert_eq!(
            sorted(unscoped.clone()),
            expected(&store, &catalog, &sources)
        );
        assert_eq!(unscoped_reachability, expected_var_length(&store, &catalog));
        let before = descriptor_retained_leaf_hits_for_test();
        {
            let _scope = DescriptorLeafScope::enter();
            assert_eq!(adjacency(&store, &catalog, &sources), unscoped);
            let hits = descriptor_retained_leaf_hits_for_test() - before;
            // Six endpoint seeks per node; most land inside a retained leaf.
            assert!(
                hits > 3 * sources.len() as u64,
                "{hits} retained hits for {} nodes",
                sources.len()
            );
            assert_eq!(var_length(&store, &catalog), unscoped_reachability);
            assert!(store.segment_cache_snapshot().unwrap().pinned_bytes > 0);
        }
        assert_eq!(store.segment_cache_snapshot().unwrap().pinned_bytes, 0);
    }
}
