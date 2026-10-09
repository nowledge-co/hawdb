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
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

pub(super) struct Fixture {
    pub(super) directory: PathBuf,
    pub(super) kind: CanonicalSegmentKind,
    pub(super) node: NodeRecord,
    pub(super) relationship: RelRecord,
    pub(super) expected: Vec<u8>,
}

impl Fixture {
    pub(super) fn new(kind: CanonicalSegmentKind) -> Self {
        let directory = std::env::temp_dir().join(format!(
            "hawdb-canonical-flush-memory-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let properties =
            BTreeMap::from([("payload".into(), Value::Binary(vec![0x9f; 257 * 1024 + 3]))]);
        let node = NodeRecord {
            id: NodeId(17),
            labels: BTreeSet::from([LabelId(7)]),
            properties: properties.clone(),
        };
        let relationship = RelRecord {
            id: RelId(17),
            source: NodeId(23),
            target: NodeId(29),
            rel_type: RelTypeId(11),
            properties,
        };
        let payload = match kind {
            CanonicalSegmentKind::Nodes => {
                encode_node_with_property_spills(&node, None, None).unwrap()
            }
            CanonicalSegmentKind::Relationships => encode_relationship(&relationship).unwrap(),
        };
        // Independent established framing, prepared outside observation.
        let mut expected = b"HAWDBCANONICAL01".to_vec();
        expected.extend_from_slice(&31u64.to_le_bytes());
        expected.extend_from_slice(b"SKNSEG01");
        expected.push(kind.tag());
        expected.extend_from_slice(&31u64.to_le_bytes());
        expected.extend_from_slice(&5u64.to_le_bytes());
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&17u64.to_le_bytes());
        expected.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        expected.extend_from_slice(&payload);
        Self {
            directory,
            kind,
            node,
            relationship,
            expected,
        }
    }

    pub(super) fn accumulator(&self) -> SegmentAccumulator {
        let mut output = SegmentAccumulator::new(
            self.kind,
            ManifestGeneration(31),
            5,
            CanonicalSegmentConfig::default(),
        );
        match self.kind {
            CanonicalSegmentKind::Nodes => {
                let payload = encode_node_with_property_spills(&self.node, None, None).unwrap();
                output.push(self.node.id.0, &payload, None).unwrap();
                output.add_node_properties(&self.node, None).unwrap();
            }
            CanonicalSegmentKind::Relationships => {
                let payload = encode_relationship(&self.relationship).unwrap();
                output
                    .push(
                        self.relationship.id.0,
                        &payload,
                        Some((self.relationship.source.0, self.relationship.target.0)),
                    )
                    .unwrap();
            }
        }
        output
    }

    pub(super) fn assert_result(
        &self,
        path: &Path,
        descriptor: &CanonicalSegmentDescriptor,
        integrity: hawdb_integrity::IntegrityDigest,
    ) {
        let actual = std::fs::read(path).unwrap();
        assert_eq!(actual, self.expected);
        let mut expected = IntegrityHasher::new();
        expected.update(&self.expected);
        assert_eq!(integrity, expected.finish());
        assert_eq!(descriptor.offset, 24);
        assert_eq!(descriptor.length.get(), self.expected.len() as u64 - 24);
        assert_eq!(descriptor.content_digest, content_digest(&actual[24..]));
        assert_eq!(descriptor.record_count, 1);
        assert_eq!(descriptor.min_record_id, 17);
        assert_eq!(descriptor.max_record_id, 17);
        let mut count = 0;
        decode_segment_records(
            &actual[24..],
            ManifestGeneration(31),
            descriptor,
            |id, payload| {
                match self.kind {
                    CanonicalSegmentKind::Nodes => {
                        assert_eq!(
                            decode_node_with_property_spills(id, payload, None, None)?,
                            self.node
                        );
                        let key = node_property_bloom_key(
                            LabelId(7),
                            "payload",
                            &self.node.properties["payload"],
                        )?;
                        assert!(descriptor.node_property_bloom.might_contain(key));
                    }
                    CanonicalSegmentKind::Relationships => {
                        assert_eq!(decode_relationship(id, payload)?, self.relationship);
                        assert!(descriptor.source_endpoint_bloom.might_contain(23));
                        assert!(descriptor.target_endpoint_bloom.might_contain(29));
                    }
                }
                count += 1;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 1);
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn assert_flush_without_large_copy(kind: CanonicalSegmentKind) {
    let fixture = Fixture::new(kind);
    let source = fixture.accumulator();
    let path = fixture.directory.join("canonical.hawdb");
    let mut file = File::create(&path).unwrap();
    file.write_all(&fixture.expected[..24]).unwrap();
    let mut digest = IntegrityHasher::new();
    digest.update(&fixture.expected[..24]);
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let unit = work.start_unit().unwrap();
    let wave = work.io_wave().unwrap();
    let observation = crate::test_allocator::AllocationObservation::start();
    let descriptor = source.flush(&mut file, &mut digest, 24).unwrap();
    let allocations = observation.finish();
    drop(wave);
    unit.finish();
    drop(file);
    fixture.assert_result(&path, &descriptor, digest.finish());
    assert_eq!(allocations, 0, "segment flush must borrow the captured records rather than duplicating the complete segment");
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn checkpoint_units_canonical_flush_memory_node_records_do_not_duplicate_segment() {
    assert_flush_without_large_copy(CanonicalSegmentKind::Nodes);
}

#[test]
fn checkpoint_units_canonical_flush_memory_relationship_records_do_not_duplicate_segment() {
    assert_flush_without_large_copy(CanonicalSegmentKind::Relationships);
}
