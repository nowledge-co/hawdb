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

//! Retain ordered immutable sources and an unaccepted row across work denials.
use super::*;
use crate::background::CheckpointWorkContext;
use crate::canonical::{
    CanonicalSegmentReader, CheckpointCanonicalCursor, CheckpointCanonicalRecord, CheckpointRecord,
};
use crate::cow::CowSegmentedMap;

enum Selected<I> {
    Base,
    Delta { id: I, replaces_base: bool },
}

pub(crate) struct Cursor<
    R: OverlayRecord + CheckpointCanonicalRecord,
    D = CowSegmentedMap<<R as OverlayRecord>::Id, R>,
> {
    reader: Option<CanonicalSegmentReader>,
    base: Option<CheckpointCanonicalCursor<R>>,
    // One admitted heap slot keeps the current record address stable even
    // when the containing preparation is moved between attempts.
    base_row: Option<Box<Option<CheckpointRecord<R>>>>,
    base_memory: Option<Box<dyn hawdb_core::RuntimeMemoryPermit>>,
    base_ended: bool,
    delta: D,
    tombstones: CowSegment<BTreeSet<R::Id>>,
    last_delta: Option<R::Id>,
    selected: Option<Selected<R::Id>>,
}

impl<R: OverlayRecord + CheckpointCanonicalRecord, D: Delta<R>> Cursor<R, D> {
    pub(crate) fn new(
        reader: Option<&CanonicalSegmentReader>,
        delta: &D,
        tombstones: &CowSegment<BTreeSet<R::Id>>,
    ) -> Self {
        Self {
            reader: reader.cloned(),
            base: reader.map(CheckpointCanonicalCursor::new),
            base_row: None,
            base_memory: None,
            base_ended: reader.is_none(),
            delta: delta.clone(),
            tombstones: tombstones.clone(),
            last_delta: None,
            selected: None,
        }
    }

    pub(crate) fn peek(
        &mut self,
        work: &CheckpointWorkContext,
    ) -> std::result::Result<Option<&R>, CanonicalSegmentError> {
        loop {
            work.checkpoint()?;
            if self.selected.is_some() {
                break;
            }
            if self.base_record().is_none() && !self.base_ended {
                if self.base_row.is_none() {
                    let unit = work.start_unit()?;
                    let memory =
                        work.reserve_memory(std::mem::size_of::<Option<CheckpointRecord<R>>>())?;
                    self.base_row = Some(Box::new(None));
                    self.base_memory = memory;
                    unit.finish();
                }
                let next = self
                    .base
                    .as_mut()
                    .expect("base cursor")
                    .next_record(self.reader.as_ref().expect("base reader"), work)?;
                self.base_ended = next.is_none();
                **self.base_row.as_mut().expect("admitted record slot") = next;
                if self.base_ended {
                    self.base_row = None;
                    self.base_memory = None;
                }
            }
            let unit = work.start_unit()?;
            let delta_id = self
                .delta
                .first_after(self.last_delta.as_ref())
                .map(|(id, _)| *id);
            let base_id = self.base_record().map(|row| row.id());
            self.selected = match (base_id, delta_id) {
                (None, None) => {
                    unit.finish();
                    return Ok(None);
                }
                (Some(base), Some(delta)) if base == delta => Some(Selected::Delta {
                    id: delta,
                    replaces_base: true,
                }),
                (None, Some(id)) => Some(Selected::Delta {
                    id,
                    replaces_base: false,
                }),
                (Some(base), Some(delta)) if delta < base => Some(Selected::Delta {
                    id: delta,
                    replaces_base: false,
                }),
                _ => Some(Selected::Base),
            };
            let id = match self.selected.as_ref().expect("selected record") {
                Selected::Base => base_id.expect("selected base"),
                Selected::Delta { id, .. } => *id,
            };
            let deleted = self.tombstones.contains(&id);
            if deleted {
                self.accept();
            }
            unit.finish();
            work.checkpoint()?;
            if !deleted {
                break;
            }
        }
        Ok(Some(
            match self.selected.as_ref().expect("unaccepted selected record") {
                Selected::Base => self.base_record().expect("selected base"),
                Selected::Delta { id, .. } => self.delta.get(id).expect("captured delta record"),
            },
        ))
    }

    fn base_record(&self) -> Option<&CheckpointRecord<R>> {
        self.base_row.as_deref().and_then(Option::as_ref)
    }

    fn consume_base(&mut self) {
        self.base_row.as_mut().expect("admitted record slot").take();
    }

    pub(crate) fn accept(&mut self) {
        match self
            .selected
            .take()
            .expect("selected record must precede acceptance")
        {
            Selected::Base => self.consume_base(),
            Selected::Delta { id, replaces_base } => {
                self.last_delta = Some(id);
                if replaces_base {
                    self.consume_base();
                }
            }
        }
    }
}

pub(crate) trait Delta<R: OverlayRecord>: Clone {
    fn first_after(&self, id: Option<&R::Id>) -> Option<(&R::Id, &R)>;
    fn get(&self, id: &R::Id) -> Option<&R>;
}
impl<R: OverlayRecord> Delta<R> for CowSegmentedMap<R::Id, R> {
    fn first_after(&self, id: Option<&R::Id>) -> Option<(&R::Id, &R)> {
        self.first_after(id)
    }
    fn get(&self, id: &R::Id) -> Option<&R> {
        self.get(id)
    }
}
impl Delta<NodeRecord> for crate::cow::CowNodeMap {
    fn first_after(&self, id: Option<&NodeId>) -> Option<(&NodeId, &NodeRecord)> {
        self.first_after(id)
    }
    fn get(&self, id: &NodeId) -> Option<&NodeRecord> {
        self.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::background::{CheckpointWorkError, CheckpointWorkProbe};
    use hawdb_core::Value;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::collections::BTreeMap;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[test]
    fn retained_overlay_cursor_preserves_cow_source_and_order_with_one_unit_attempts() {
        let mut delta = CowSegmentedMap::from(
            (0..256u64)
                .map(|id| {
                    (
                        NodeId(id),
                        NodeRecord {
                            id: NodeId(id),
                            labels: BTreeSet::new(),
                            properties: BTreeMap::from([
                                ("id".into(), Value::Int(id as i64)),
                                ("payload".into(), Value::Binary(vec![0x53; 1024])),
                            ]),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        );
        let mut tombstones = CowSegment::from(
            (0..256u64)
                .filter(|id| id % 2 == 0)
                .map(NodeId)
                .collect::<BTreeSet<_>>(),
        );
        let mut cursor = Cursor::new(None, &delta, &tombstones);
        // The captured COW roots remain authoritative after the host mutates.
        delta.insert(
            NodeId(3),
            NodeRecord {
                id: NodeId(3),
                labels: BTreeSet::new(),
                properties: BTreeMap::from([("id".into(), Value::Int(999))]),
            },
        );
        tombstones.insert(NodeId(3));
        let scheduler = LocalQosScheduler::new(LocalQosPolicy {
            max_background_operations: Some(1),
            max_total_background_operations: Some(1),
            ..Default::default()
        });
        let mut observed = Vec::new();
        let mut finished = false;
        let mut denials = 0;
        for _ in 0..512 {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(1, Ordering::SeqCst);
            let work = probe.context(scheduler.clone());
            match cursor.peek(&work) {
                Ok(Some(node)) => {
                    let id = node.id.0;
                    assert_eq!(node.properties["id"], Value::Int(id as i64));
                    let address = node as *const NodeRecord;
                    assert_eq!(
                        cursor
                            .peek(&CheckpointWorkContext::default())
                            .unwrap()
                            .unwrap() as *const NodeRecord,
                        address
                    );
                    observed.push(id);
                    cursor.accept();
                }
                Ok(None) => {
                    finished = true;
                }
                Err(CanonicalSegmentError::Work(CheckpointWorkError::Stopped(_))) => {
                    denials += 1;
                    assert!(probe.cancellation.is_cancelled());
                }
                other => panic!("unexpected cursor result: {other:?}"),
            }
            probe.assert_released(&scheduler);
            assert!(probe.completed.load(Ordering::SeqCst) <= 1);
            if finished {
                break;
            }
        }
        assert!(finished);
        assert!(denials >= 256);
        assert_eq!(
            observed,
            (0..256u64).filter(|id| id % 2 == 1).collect::<Vec<_>>()
        );
        assert!(cursor
            .peek(&CheckpointWorkContext::default())
            .unwrap()
            .is_none());
    }
    #[test]
    fn retained_base_row_address_survives_moving_cursor_and_merges_replacements() {
        use crate::cache::{ManifestGeneration, SegmentCache, StoreId};
        use crate::canonical::{CanonicalSegmentConfig, CanonicalSegmentWriter};
        use std::num::NonZeroU64;
        let directory = std::env::temp_dir().join(format!(
            "hawdb-base-cursor-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("canonical.hawdb");
        let node = |id, revision| NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("revision".into(), Value::Int(revision))]),
        };
        let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig::default())
            .write(
                &path,
                ManifestGeneration(47),
                (0..8).map(|id| node(id * 2, 0)),
                std::iter::empty::<RelRecord>(),
            )
            .unwrap();
        let reader = CanonicalSegmentReader::open(
            &path,
            manifest,
            Arc::new(SegmentCache::new(0)),
            StoreId(47),
            NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let delta = CowSegmentedMap::from(BTreeMap::from([
            (NodeId(2), node(2, 1)),
            (NodeId(5), node(5, 1)),
            (NodeId(12), node(12, 1)),
        ]));
        let tombstones = CowSegment::from(BTreeSet::from([NodeId(4), NodeId(12)]));
        let mut cursor = Cursor::new(Some(&reader), &delta, &tombstones);
        let work = CheckpointWorkContext::default();
        let first = cursor.peek(&work).unwrap().unwrap() as *const NodeRecord;
        // Force ownership through a separately allocated container and back.
        let moved = Box::new(cursor);
        cursor = *moved;
        assert_eq!(
            cursor.peek(&work).unwrap().unwrap() as *const NodeRecord,
            first
        );
        let mut observed = Vec::new();
        while let Some(record) = cursor.peek(&work).unwrap() {
            observed.push((record.id.0, record.properties["revision"].clone()));
            cursor.accept();
        }
        assert_eq!(
            observed,
            [(0, 0), (2, 1), (5, 1), (6, 0), (8, 0), (10, 0), (14, 0)]
                .map(|(id, revision)| (id, Value::Int(revision)))
        );
        assert!(cursor.base_row.is_none());
        assert!(cursor.base_memory.is_none());
        drop(cursor);
        drop(reader);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
