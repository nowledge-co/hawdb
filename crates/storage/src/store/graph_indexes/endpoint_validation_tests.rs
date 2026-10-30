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

const NODES: i64 = 128;

/// Removes the store directory even when an assertion fails.
struct Fixture(std::path::PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fixture() -> (Fixture, GraphStore, Vec<NodeId>) {
    let path = std::env::temp_dir().join(format!(
        "hawdb-endpoint-validation-{}",
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
                    "Item",
                    BTreeMap::from([
                        ("id".into(), Value::Int(ordinal)),
                        ("name".into(), Value::String(format!("item-{ordinal}"))),
                    ]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (ordinal, source) in nodes.iter().enumerate() {
        for step in 1..=4 {
            let target = nodes[(ordinal + step) % nodes.len()];
            store
                .create_relationship(
                    &mut catalog,
                    *source,
                    target,
                    "LINK",
                    BTreeMap::from([("rank".into(), Value::Int(step as i64))]),
                )
                .unwrap();
        }
    }
    store.checkpoint(&catalog).unwrap();
    assert!(store.is_out_of_core());
    assert!(store.nodes.is_empty());
    assert!(store.relationships.is_empty());
    (directory, store, nodes)
}

#[test]
fn out_of_core_endpoint_validation_does_not_point_read_each_endpoint() {
    let (_directory, store, _) = fixture();
    let relationships = (NODES * 4) as u64;
    let before = store.segment_cache_snapshot().unwrap();
    store.validate_relationship_endpoints().unwrap();
    let after = store.segment_cache_snapshot().unwrap();
    let lookups = after.hit_count + after.miss_count - before.hit_count - before.miss_count;
    assert!(
        lookups < relationships,
        "endpoint validation must scan, not point-read each endpoint: {lookups} segment lookups for {relationships} relationships"
    );
}

#[test]
fn out_of_core_endpoint_validation_rejects_missing_endpoint() {
    let (_directory, mut store, nodes) = fixture();
    let missing = nodes[7];
    store.node_tombstones.insert(missing);
    let error = store
        .validate_relationship_endpoints()
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(&format!("node {}", missing.0)) && error.contains("references missing"),
        "unexpected error: {error}"
    );
}
