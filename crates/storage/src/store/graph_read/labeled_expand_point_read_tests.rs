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

//! Out-of-core labeled expansion point-reads one relationship and one target
//! node per adjacency entry. Each read must stay bounded by the sparse record
//! directory rather than walking the whole canonical segment.

use super::*;
use crate::canonical::point_directory::{point_frames_examined, POINT_DIRECTORY_STRIDE};
use crate::read_view::{ControlledGraphReadAllocator, GraphReadAllocation};

const NODES: i64 = 600;
const DEGREE: i64 = 3;

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

fn fixture() -> (Fixture, GraphStore, Catalog, Vec<NodeId>) {
    let path = std::env::temp_dir().join(format!(
        "hawdb-labeled-expand-point-reads-{}",
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
                    if ordinal % 5 == 0 { "Other" } else { "Item" },
                    BTreeMap::from([("id".into(), Value::Int(ordinal))]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (ordinal, source) in nodes.iter().enumerate() {
        for step in 1..=DEGREE {
            let target = nodes[(ordinal * 7 + step as usize * 131) % nodes.len()];
            store
                .create_relationship(
                    &mut catalog,
                    *source,
                    target,
                    "LINK",
                    BTreeMap::from([("rank".into(), Value::Int(step))]),
                )
                .unwrap();
        }
    }
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    assert!(store.nodes.is_empty());
    assert!(store.relationships.is_empty());
    (directory, store, catalog, nodes)
}

/// `(:Item)-[:LINK]->(:Item)` through the storage calls the executor's
/// adjacency expand uses: ordered adjacency, then a labeled target read.
fn labeled_expand(store: &GraphStore, catalog: &Catalog, sources: &[NodeId]) -> Vec<(u64, u64)> {
    let link = catalog.rel_type_id("LINK").unwrap();
    let item = [catalog.label_id("Item").unwrap()];
    let mut admit = |bytes| Ok(Some(Box::new(Permit(bytes)) as Box<dyn GraphReadAllocation>));
    let mut rows = Vec::new();
    for &source in sources {
        let mut relationships = Vec::new();
        store
            .visit_ordered_adjacent_relationships_with_allocation(
                source,
                Some(link),
                AdjacencyDirection::Outgoing,
                usize::MAX,
                &mut ControlledGraphReadAllocator::new(&mut admit, None),
                |relationship| {
                    relationships.push(relationship.relationship().clone());
                    Ok(GraphScanControl::Continue)
                },
            )
            .unwrap();
        for relationship in relationships {
            match store
                .node_with_allocation(
                    relationship.target,
                    Some(&item),
                    &mut ControlledGraphReadAllocator::new(&mut admit, None),
                )
                .unwrap()
            {
                AdmittedNodeRead::Node(target) => {
                    let (target, _) = target.into_parts();
                    assert!(target.labels.contains(&item[0]));
                    rows.push((relationship.id.0, target.id.0));
                }
                AdmittedNodeRead::Missing => {}
                AdmittedNodeRead::Stopped => panic!("unbounded admission stopped"),
            }
        }
    }
    rows.sort_unstable();
    rows
}

fn expected(store: &GraphStore, catalog: &Catalog, sources: &[NodeId]) -> Vec<(u64, u64)> {
    let link = catalog.rel_type_id("LINK").unwrap();
    let item = catalog.label_id("Item").unwrap();
    let mut rows = store
        .relationship_records_owned()
        .map(Result::unwrap)
        .filter(|relationship| {
            relationship.rel_type == link && sources.contains(&relationship.source)
        })
        .filter(|relationship| {
            store
                .node_owned(relationship.target)
                .unwrap()
                .is_some_and(|node| node.labels.contains(&item))
        })
        .map(|relationship| (relationship.id.0, relationship.target.0))
        .collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

fn item_sources(store: &GraphStore, catalog: &Catalog) -> Vec<NodeId> {
    let item = catalog.label_id("Item").unwrap();
    store
        .node_records_owned()
        .map(Result::unwrap)
        .filter(|node| node.labels.contains(&item))
        .map(|node| node.id)
        .collect()
}

#[test]
fn out_of_core_labeled_expand_point_reads_do_not_walk_whole_segments() {
    let (_directory, store, catalog, _) = fixture();
    let sources = item_sources(&store, &catalog);
    let expected = expected(&store, &catalog, &sources);
    let relationships = sources.len() as u64 * DEGREE as u64;
    // Warm descriptor pages, segments and point directories.
    assert_eq!(labeled_expand(&store, &catalog, &sources), expected);
    let cache_before = store.segment_cache_snapshot().unwrap();
    let frames_before = point_frames_examined();
    assert_eq!(labeled_expand(&store, &catalog, &sources), expected);
    let frames = point_frames_examined() - frames_before;
    let cache_after = store.segment_cache_snapshot().unwrap();
    // One relationship read and one target read per adjacency entry, each
    // walking at most one directory stride. Walking from the start of a
    // segment instead costs about half of its records per read.
    let bound = 2 * relationships * u64::from(POINT_DIRECTORY_STRIDE + 1);
    assert!(
        frames <= bound,
        "labeled expansion examined {frames} segment frames for {relationships} relationships (bound {bound})"
    );
    assert!(
        frames < relationships * NODES as u64 / 4,
        "point reads must not scale with segment size: {frames} frames"
    );
    // Warm point reads reuse cached segments and directories: they neither
    // rebuild a directory nor re-admit a segment per relationship.
    assert_eq!(
        cache_after.insertion_count, cache_before.insertion_count,
        "warm labeled expansion must not admit per-row cache entries"
    );
    assert_eq!(cache_after.miss_count, cache_before.miss_count);
}

#[test]
fn out_of_core_labeled_expand_honours_overlay_records_and_tombstones() {
    let (_directory, mut store, mut catalog, nodes) = fixture();
    // Update, delete and add targets after the checkpoint so the overlay,
    // tombstones and canonical base must all agree with a full scan.
    let id_filter = |id| PropertyFilter::Eq {
        property: "id".into(),
        value: Value::Int(id),
    };
    let updated = nodes[131];
    store
        .set_node_property(
            &mut catalog,
            "Item",
            Some(&id_filter(131)),
            "name",
            Value::String("updated".into()),
        )
        .unwrap();
    let deleted = nodes[262];
    assert_eq!(
        store
            .delete_nodes(&mut catalog, "Item", Some(&id_filter(262)), true)
            .unwrap(),
        vec![deleted]
    );
    let added = store
        .create_node(&mut catalog, "Item", BTreeMap::new())
        .unwrap();
    let added_other = store
        .create_node(&mut catalog, "Other", BTreeMap::new())
        .unwrap();
    for target in [added, added_other] {
        store
            .create_relationship(&mut catalog, nodes[1], target, "LINK", BTreeMap::new())
            .unwrap();
    }
    assert!(store.nodes.contains_key(&updated));
    let sources = item_sources(&store, &catalog);
    let expected = expected(&store, &catalog, &sources);
    let rows = labeled_expand(&store, &catalog, &sources);
    assert_eq!(rows, expected);
    assert!(rows.iter().any(|(_, target)| *target == updated.0));
    assert!(rows.iter().any(|(_, target)| *target == added.0));
    assert!(!rows.iter().any(|(_, target)| *target == added_other.0));
    assert!(!rows.iter().any(|(_, target)| *target == deleted.0));
    // Targets labeled Other in the canonical base are filtered by the
    // encoded-header label check.
    assert!(!rows.iter().any(|(_, target)| *target == nodes[5].0));
    assert!(expected.len() < sources.len() * DEGREE as usize);
}
