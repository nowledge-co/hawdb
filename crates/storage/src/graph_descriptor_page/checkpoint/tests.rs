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
use hawdb_core::RuntimeMemoryError;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
    RuntimeWorkRequest,
};

fn fixture(leaf: bool) -> (Vec<u8>, GraphDescriptorPageRef) {
    let body = if leaf {
        ImmutableGraphDescriptorPageBody::Leaf(vec![
            GraphDescriptorLeafEntry {
                key: b"a".to_vec(),
                value: b"one".to_vec(),
            },
            GraphDescriptorLeafEntry {
                key: b"z".to_vec(),
                value: b"two".to_vec(),
            },
        ])
    } else {
        ImmutableGraphDescriptorPageBody::Interior(
            [(b"a", b"m"), (b"n", b"z")]
                .into_iter()
                .enumerate()
                .map(|(index, (lower, upper))| {
                    let (_, child) = ImmutableGraphDescriptorPage {
                        kind: GraphDescriptorKind::CanonicalSegment,
                        physical_generation: 1,
                        source_commit_epoch: 19,
                        page_id: GraphDescriptorPageId::new(
                            NonZeroU64::new(index as u64 + 2).unwrap(),
                        ),
                        body: ImmutableGraphDescriptorPageBody::Leaf(vec![
                            GraphDescriptorLeafEntry {
                                key: lower.to_vec(),
                                value: vec![1],
                            },
                            GraphDescriptorLeafEntry {
                                key: upper.to_vec(),
                                value: vec![2],
                            },
                        ]),
                    }
                    .encode_with_ref(3, index as u64 * 1024, GraphDescriptorPageLimits::default())
                    .unwrap();
                    GraphDescriptorInteriorEntry { child }
                })
                .collect(),
        )
    };
    ImmutableGraphDescriptorPage {
        kind: GraphDescriptorKind::CanonicalSegment,
        physical_generation: 1,
        source_commit_epoch: 19,
        page_id: GraphDescriptorPageId::new(NonZeroU64::MIN),
        body,
    }
    .encode_with_ref(3, 0, GraphDescriptorPageLimits::default())
    .unwrap()
}

fn admitted(bytes: &[u8], work: &CheckpointWorkContext) -> CheckpointBytes {
    let mut output = CheckpointBytes::new(bytes.len(), work).unwrap();
    output.append(bytes, work).unwrap();
    output
}

fn rebind(bytes: &mut [u8], reference: &mut GraphDescriptorPageRef) {
    let length = bytes.len();
    bytes[40..48].copy_from_slice(&((length - PAGE_HEADER_BYTES) as u64).to_le_bytes());
    let mut hasher = IntegrityHasher::new();
    hasher.update(&bytes[..48]);
    hasher.update(&bytes[PAGE_HEADER_BYTES..]);
    let digest = hasher.finish();
    bytes[48..52].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    bytes[52..84].copy_from_slice(digest.sha256.as_bytes());
    reference.length = NonZeroU64::new(length as u64).unwrap();
    reference.content_crc32c = digest.crc32c;
    reference.content_sha256 = digest.sha256;
}

#[test]
fn checkpoint_units_metadata_codec_matches_independent_leaf_child_and_extension_decoding() {
    let work = CheckpointWorkContext::default();
    let limits = GraphDescriptorPageLimits::default();
    for leaf in [false, true] {
        let (mut bytes, mut reference) = fixture(leaf);
        bytes.extend_from_slice(&99u16.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[0x9a, 2, 3]);
        rebind(&mut bytes, &mut reference);
        let ordinary = ImmutableGraphDescriptorPage::decode_bound(
            &reference,
            GraphDescriptorKind::CanonicalSegment,
            19,
            &bytes,
            limits,
        )
        .unwrap();
        let page = CheckpointDescriptorPage::decode_bound(
            &reference,
            GraphDescriptorKind::CanonicalSegment,
            19,
            admitted(&bytes, &work),
            limits,
            &work,
        )
        .unwrap();
        assert_eq!(page.is_leaf(), leaf);
        assert_eq!(page.len(), 2);
        match ordinary.body {
            ImmutableGraphDescriptorPageBody::Leaf(entries) => {
                for (index, entry) in entries.iter().enumerate() {
                    assert_eq!(
                        page.leaf_entry(index).unwrap(),
                        (entry.key.as_slice(), entry.value.as_slice())
                    );
                }
                assert_eq!(page.lower_bound(b"b", &work).unwrap().0, 1);
            }
            ImmutableGraphDescriptorPageBody::Interior(entries) => {
                for (index, entry) in entries.iter().enumerate() {
                    assert_eq!(
                        page.child(index, limits, &work).unwrap().reference,
                        entry.child
                    );
                }
                assert_eq!(page.lower_bound(b"n", &work).unwrap().0, 1);
            }
        }
    }
}

#[test]
fn checkpoint_units_metadata_codec_rejects_independent_authenticated_framing_and_order_corruption()
{
    let work = CheckpointWorkContext::default();
    let limits = GraphDescriptorPageLimits::default();
    for leaf in [false, true] {
        let (base, reference) = fixture(leaf);
        let mut cases = (0..base.len())
            .map(|end| (base[..end].to_vec(), reference.clone()))
            .collect::<Vec<_>>();
        for count in [0u32, 1, 3] {
            let mut bytes = base.clone();
            bytes[36..40].copy_from_slice(&count.to_le_bytes());
            let mut reference = reference.clone();
            rebind(&mut bytes, &mut reference);
            cases.push((bytes, reference));
        }
        let mut wrong_tag = base.clone();
        wrong_tag[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 2].copy_from_slice(&99u16.to_le_bytes());
        let mut changed = reference.clone();
        rebind(&mut wrong_tag, &mut changed);
        cases.push((wrong_tag, changed));
        let mut unordered = base.clone();
        let second_field = PAGE_HEADER_BYTES
            + FIELD_HEADER_BYTES
            + read_u32(&base[PAGE_HEADER_BYTES + 2..PAGE_HEADER_BYTES + 6]) as usize;
        let second_key = second_field + FIELD_HEADER_BYTES + if leaf { 4 } else { 80 };
        unordered[second_key] = b'a';
        let mut changed = reference.clone();
        rebind(&mut unordered, &mut changed);
        cases.push((unordered, changed));
        for (bytes, reference) in cases {
            assert!(ImmutableGraphDescriptorPage::decode_bound(
                &reference,
                GraphDescriptorKind::CanonicalSegment,
                19,
                &bytes,
                limits
            )
            .is_err());
            assert!(matches!(
                CheckpointDescriptorPage::decode_bound(
                    &reference,
                    GraphDescriptorKind::CanonicalSegment,
                    19,
                    admitted(&bytes, &work),
                    limits,
                    &work
                ),
                Err(GraphDescriptorTreeError::Page(
                    GraphDescriptorPageError::Corrupt(_)
                ))
            ));
        }
    }
}

#[test]
fn checkpoint_units_metadata_codec_retains_page_and_index_capacity_after_execution_closes() {
    let ceiling = 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let (bytes, reference) = fixture(true);
    let page = CheckpointDescriptorPage::decode_bound(
        &reference,
        GraphDescriptorKind::CanonicalSegment,
        19,
        admitted(&bytes, &work),
        GraphDescriptorPageLimits::default(),
        &work,
    )
    .unwrap();
    drop(work);
    drop(task);
    drop(permit);
    assert!(
        governor.snapshot().admitted_memory_bytes
            >= bytes.len() as u64 + 2 * std::mem::size_of::<Range<usize>>() as u64
    );
    assert_eq!(
        page.leaf_entry(1).unwrap(),
        (b"z".as_slice(), b"two".as_slice())
    );
    drop(page);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_metadata_codec_input_and_index_match_actual_capacity_differences() {
    let ceiling = 4 * 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let available = || match task.reserve_working_memory(ceiling) {
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes, ..
        }) => available_bytes,
        _ => panic!("exact-ceiling probe must expose real working-memory remainder"),
    };
    let idle = available();
    let work = CheckpointWorkContext::new(task.clone());
    let mut retained = Vec::new();
    for (count, value_len, key_len) in [(2, 33, 1), (4, 9, 1), (2, 33, 1025)] {
        let (bytes, reference) = ImmutableGraphDescriptorPage {
            kind: GraphDescriptorKind::CanonicalSegment,
            physical_generation: 1,
            source_commit_epoch: 19,
            page_id: GraphDescriptorPageId::new(NonZeroU64::MIN),
            body: ImmutableGraphDescriptorPageBody::Leaf(
                (0..count)
                    .map(|index| {
                        let mut key = vec![b'x'; key_len];
                        key[0] = index as u8 + 1;
                        GraphDescriptorLeafEntry {
                            key,
                            value: vec![0xa5; value_len],
                        }
                    })
                    .collect(),
            ),
        }
        .encode_with_ref(3, 0, GraphDescriptorPageLimits::default())
        .unwrap();
        let page = CheckpointDescriptorPage::decode_bound(
            &reference,
            GraphDescriptorKind::CanonicalSegment,
            19,
            admitted(&bytes, &work),
            GraphDescriptorPageLimits::default(),
            &work,
        )
        .unwrap();
        retained.push((bytes.len(), idle - available()));
        assert_eq!(page.len(), count);
        drop(page);
        assert_eq!(available(), idle);
    }
    // The first two images have identical byte capacity and the same two
    // concrete permits. Only their entry-index capacities differ.
    assert_eq!(retained[0].0, retained[1].0);
    assert_eq!(
        retained[1].1 - retained[0].1,
        2 * std::mem::size_of::<Range<usize>>() as u64
    );
    // Equal entry counts keep the index and permit inventories fixed; only
    // the two actual input-key capacities grow by 1024 bytes each.
    assert_eq!(retained[2].1 - retained[0].1, 2048);
    assert_eq!(governor.snapshot().admissions, 1);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
