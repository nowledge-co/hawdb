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
use crate::{
    CanonicalSegmentConfig, CanonicalSegmentReader, CanonicalSegmentWriter, ManifestGeneration,
    SegmentCache, StoreId,
};
use hawdb_core::{LabelId, RelTypeId, Value};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

fn node(id: u64, revision: u64) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::from([LabelId((revision % 3) as u32)]),
        properties: properties(id, revision),
    }
}

fn relationship(id: u64, revision: u64) -> RelRecord {
    RelRecord {
        id: RelId(id),
        source: NodeId(revision),
        target: NodeId(id.wrapping_add(revision)),
        rel_type: RelTypeId((revision % 3) as u32),
        properties: properties(id, revision),
    }
}

fn properties(id: u64, revision: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "identity".to_string(),
            Value::Binary(id.to_le_bytes().to_vec()),
        ),
        (
            "revision".to_string(),
            Value::String(format!("v{revision}")),
        ),
        ("optional".to_string(), Value::Null),
    ])
}

#[test]
fn graph_overlay_differential_smoke() {
    campaign(4, 16);
}

#[test]
#[ignore = "explicit local exhaustive and generated graph overlay campaign"]
fn graph_overlay_differential_campaign() {
    campaign(128, 128);
}

fn campaign(seeds: u64, cases_per_seed: usize) {
    let ids = [0, 1, 17, u64::MAX];
    let mut cases = 0;
    for base_mask in 0..16 {
        for delta_mask in 0..16 {
            for deleted_mask in 0..16 {
                let selected = |mask| {
                    ids.iter()
                        .enumerate()
                        .filter(|(bit, _)| mask & (1 << bit) != 0)
                        .map(|(_, id)| *id)
                        .collect::<Vec<_>>()
                };
                check_both(
                    &selected(base_mask),
                    &selected(delta_mask),
                    &selected(deleted_mask),
                );
                cases += 2;
            }
        }
    }
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        for _ in 0..cases_per_seed {
            let mut select = || {
                (0..rng.below(48))
                    .map(|_| match rng.below(3) {
                        0 => rng.below(32),
                        1 => u64::MAX - rng.below(32),
                        _ => 1 << 63 | rng.below(32),
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            };
            check_both(&select(), &select(), &select());
            cases += 2;
        }
    }
    eprintln!("graph-overlay-differential-v1 seeds={seeds} record_cases={cases}");
}

fn check_both(base: &[u64], delta: &[u64], deleted: &[u64]) {
    compare(
        &base.iter().map(|&id| node(id, 1)).collect::<Vec<_>>(),
        &delta.iter().map(|&id| node(id, 2)).collect::<Vec<_>>(),
        deleted,
        |record| record.id.0,
        NodeId,
    );
    compare(
        &base
            .iter()
            .map(|&id| relationship(id, 1))
            .collect::<Vec<_>>(),
        &delta
            .iter()
            .map(|&id| relationship(id, 2))
            .collect::<Vec<_>>(),
        deleted,
        |record| record.id.0,
        RelId,
    );
}

fn reference<R: Clone>(base: &[R], delta: &[R], deleted: &[u64], id: fn(&R) -> u64) -> Vec<R> {
    let mut records = base
        .iter()
        .chain(delta)
        .map(|record| (id(record), record.clone()))
        .collect::<BTreeMap<_, _>>();
    for id in deleted {
        records.remove(id);
    }
    records.into_values().collect()
}

fn compare<R: OverlayRecord + Clone + Debug + Eq>(
    base: &[R],
    delta: &[R],
    deleted: &[u64],
    id: fn(&R) -> u64,
    tombstone: fn(u64) -> R::Id,
) {
    let expected = reference(base, delta, deleted, id);
    let mut actual = OverlayIterator::new(
        Some(base.iter().cloned().map(Ok)),
        delta.to_vec(),
        deleted
            .iter()
            .copied()
            .map(tombstone)
            .collect::<BTreeSet<_>>()
            .into(),
    );
    assert_eq!(
        actual.by_ref().collect::<Result<Vec<_>>>().unwrap(),
        expected,
        "base={base:?} delta={delta:?} deleted={deleted:?}"
    );
    assert!(actual.next().is_none());
    assert!(actual.next().is_none());
    if base.is_empty() {
        let without_base: OverlayIterator<
            R,
            std::iter::Empty<std::result::Result<R, CanonicalSegmentError>>,
        > = OverlayIterator::new(
            None,
            delta.to_vec(),
            deleted
                .iter()
                .copied()
                .map(tombstone)
                .collect::<BTreeSet<_>>()
                .into(),
        );
        assert_eq!(without_base.collect::<Result<Vec<_>>>().unwrap(), expected);
    }
}

#[test]
fn physical_errors_preserve_order_class_and_remaining_delta() {
    // A canonical iterator stops after one physical error. Preserve the overlay's
    // existing contract: emit that error first, then its remaining captured delta
    // if the caller deliberately continues instead of collecting Result<Vec<_>>.
    for prefix in 0..=3 {
        for deleted_mask in 0..128 {
            let deleted = (0..7)
                .filter(|id| deleted_mask & (1 << id) != 0)
                .collect::<Vec<_>>();
            check_failure(prefix, &deleted, node, |r| r.id.0, NodeId);
            check_failure(prefix, &deleted, relationship, |r| r.id.0, RelId);
        }
    }
    eprintln!("graph-overlay-error-matrix-v1 record_cases=1024");
}

fn check_failure<R: OverlayRecord + Clone + Debug + Eq>(
    prefix: usize,
    deleted: &[u64],
    make: fn(u64, u64) -> R,
    id: fn(&R) -> u64,
    tombstone: fn(u64) -> R::Id,
) {
    let base = [1, 3, 5][..prefix]
        .iter()
        .map(|&id| make(id, 1))
        .collect::<Vec<_>>();
    let delta = (0..7).map(|id| make(id, 2)).collect::<Vec<_>>();
    let last = base.last().map(id);
    let (before, after): (Vec<_>, Vec<_>) = delta
        .iter()
        .cloned()
        .partition(|record| last.is_some_and(|last| id(record) <= last));
    let before = reference(&base, &before, deleted, id);
    let after = reference(&[], &after, deleted, id);
    let fault = "injected canonical fault";
    let input =
        base.into_iter()
            .map(Ok)
            .chain(std::iter::once(Err(CanonicalSegmentError::Corrupt(
                fault.to_string(),
            ))));
    let mut actual = OverlayIterator::new(
        Some(input),
        delta,
        deleted
            .iter()
            .copied()
            .map(tombstone)
            .collect::<BTreeSet<_>>()
            .into(),
    );
    for expected in before {
        assert_eq!(actual.next().unwrap().unwrap(), expected);
    }
    let error = actual
        .next()
        .expect("physical error must not disappear")
        .unwrap_err();
    assert!(matches!(&error, HawDBError::StorageIntegrity(_)));
    assert_eq!(
        error.to_string(),
        HawDBError::StorageIntegrity(CanonicalSegmentError::Corrupt(fault.to_string()).to_string())
            .to_string()
    );
    assert_eq!(actual.collect::<Result<Vec<_>>>().unwrap(), after);
}

#[test]
fn canonical_factories_preserve_overlay_and_tombstone_snapshot() {
    let fixture = Fixture::new();
    let reader = fixture.reader();
    let mut node_deleted = CowSegment::from(BTreeSet::from([NodeId(1), NodeId(5)]));
    let mut rel_deleted = CowSegment::from(BTreeSet::from([RelId(1), RelId(5)]));
    let nodes = node_records(
        Some(reader.node_records()),
        vec![node(3, 2), node(5, 2)],
        node_deleted.clone(),
    );
    let relationships = relationship_records(
        Some(reader.relationship_records()),
        vec![relationship(3, 2), relationship(5, 2)],
        rel_deleted.clone(),
    );
    node_deleted.clear();
    node_deleted.insert(NodeId(3));
    rel_deleted.clear();
    rel_deleted.insert(RelId(3));
    assert_eq!(nodes.collect::<Result<Vec<_>>>().unwrap(), vec![node(3, 2)]);
    assert_eq!(
        relationships.collect::<Result<Vec<_>>>().unwrap(),
        vec![relationship(3, 2)]
    );
}

#[test]
fn corrupt_canonical_file_is_not_hidden_by_delta_or_tombstones() {
    let fixture = Fixture::new();
    let reader = fixture.reader();
    let relationship_reader = fixture.reader();
    let mut nodes = node_records(
        Some(reader.node_records()),
        vec![node(0, 2), node(1, 2)],
        BTreeSet::from([NodeId(1), NodeId(3)]).into(),
    );
    let mut relationships = relationship_records(
        Some(relationship_reader.relationship_records()),
        vec![relationship(0, 2), relationship(1, 2)],
        BTreeSet::from([RelId(1), RelId(3)]).into(),
    );
    // Truncate only this test's owned artifact, after opening but before reading.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&fixture.path)
        .unwrap()
        .set_len(16)
        .unwrap();
    assert!(matches!(
        nodes.next(),
        Some(Err(HawDBError::StorageIntegrity(_)))
    ));
    assert!(matches!(
        relationships.next(),
        Some(Err(HawDBError::StorageIntegrity(_)))
    ));
    assert!(reader.is_poisoned());
    assert!(relationship_reader.is_poisoned());
}

#[test]
fn construction_and_early_drop_do_not_drain_checkpoint() {
    let reads = std::cell::Cell::new(0);
    let base = (10..100).map(|id| {
        reads.set(reads.get() + 1);
        Ok(node(id, 1))
    });
    let mut iterator = OverlayIterator::new(Some(base), vec![node(0, 2)], CowSegment::default());
    assert_eq!(reads.get(), 0);
    assert_eq!(iterator.next().unwrap().unwrap(), node(0, 2));
    assert_eq!(reads.get(), 1);
    drop(iterator);
    assert_eq!(reads.get(), 1);
}

struct Fixture {
    directory: PathBuf,
    path: PathBuf,
    manifest: crate::CanonicalSegmentManifest,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "hawdb-overlay-{}-{time}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(1),
                [node(1, 1), node(3, 1)],
                [relationship(1, 1), relationship(3, 1)],
            )
            .unwrap();
        Self {
            directory,
            path,
            manifest,
        }
    }

    fn reader(&self) -> CanonicalSegmentReader {
        CanonicalSegmentReader::open(
            &self.path,
            self.manifest.clone(),
            Arc::new(SegmentCache::new(0)),
            StoreId(1),
            NonZeroU64::new(4 * 1024 * 1024).unwrap(),
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

struct Generator(u64);

impl Generator {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}
