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
use crate::property_projection::memory_test_support::{admitted, definition, OWNER};

#[test]
fn composite_names_deny_before_allocating_decoded_hex_or_strings() {
    let properties = vec!["索引".repeat(16 * 1024), "rank".into()];
    let definitions = vec![definition(
        PersistentPropertyProjectionKind::CompositeEquality,
        persistent_composite_property_identity(&properties).unwrap(),
    )];
    let original = definitions.clone();
    let (governor, admission, work) = admitted(4096);
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = prepare(&definitions, &work);
    let large_allocations = observer.finish();
    assert!(
        matches!(
            result,
            Err(PersistentPropertyProjectionError::Work(
                CheckpointWorkError::Memory(_)
            ))
        ),
        "decoded composite names must be admitted before allocation"
    );
    assert_eq!(large_allocations, 0);
    assert_eq!(definitions, original);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn grouped_array_denies_before_unadmitted_growth_and_refunds_partial_plan() {
    let definitions = (0..4096)
        .map(|i| {
            definition(
                PersistentPropertyProjectionKind::Equality,
                format!("property-{i}"),
            )
        })
        .collect::<Vec<_>>();
    let original = definitions.clone();
    let (governor, admission, work) = admitted(4096);
    let observer = crate::test_allocator::AllocationObservation::start();
    let result = prepare(&definitions, &work);
    let large_allocations = observer.finish();
    assert!(
        matches!(
            result,
            Err(PersistentPropertyProjectionError::Work(
                CheckpointWorkError::Memory(_)
            ))
        ),
        "grouped arrays must be admitted before growing"
    );
    assert_eq!(large_allocations, 0);
    assert!(admission.memory_report().peak_accounted_bytes > 0);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    assert_eq!(definitions, original);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn subject_tree_denies_before_first_node_allocation() {
    let definitions = vec![definition(
        PersistentPropertyProjectionKind::Equality,
        "id".into(),
    )];
    let (governor, admission, work) = admitted(512);
    let result = prepare(&definitions, &work);
    assert!(
        matches!(
            result,
            Err(PersistentPropertyProjectionError::Work(
                CheckpointWorkError::Memory(_)
            ))
        ),
        "the subject tree must be admitted before insertion"
    );
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn grouped_subjects_and_composite_capacity_remain_admitted_while_paused() {
    let properties = vec!["索引".repeat(12 * 1024), "name:\0x".into()];
    let definitions = vec![
        definition(PersistentPropertyProjectionKind::Equality, "id".into()),
        definition(
            PersistentPropertyProjectionKind::CompositeEquality,
            persistent_composite_property_identity(&properties).unwrap(),
        ),
        definition(
            PersistentPropertyProjectionKind::RelationshipEquality,
            "id".into(),
        ),
        definition(
            PersistentPropertyProjectionKind::RelationshipRange,
            "rank".into(),
        ),
    ];
    let original = definitions.clone();
    let (governor, mut admission, work) = admitted(1024 * 1024);
    let plan = prepare(&definitions, &work).unwrap();
    assert_eq!(plan.len(), 2);
    let node = plan.get(&ProjectionSubject::Node(LabelId(7))).unwrap();
    let relationship = plan
        .get(&ProjectionSubject::Relationship(RelTypeId(7)))
        .unwrap();
    assert_eq!(
        node.iter()
            .map(|item| item.definition_index)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    assert_eq!(
        relationship
            .iter()
            .map(|item| item.definition_index)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    let ProjectionValueSource::Composite(decoded) = &node[1].value_source else {
        panic!("composite properties must retain their source kind");
    };
    assert_eq!(decoded, &properties);
    let capacity_bytes = (node.capacity() + relationship.capacity())
        * std::mem::size_of::<PreparedProjectionDefinition>()
        + decoded.capacity() * std::mem::size_of::<String>()
        + decoded.iter().map(String::capacity).sum::<usize>();
    assert!(admission.memory_report().live_accounted_bytes >= capacity_bytes as u64);
    assert_eq!(definitions, original);
    admission.pause();
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert!(governor.snapshot().admitted_memory_bytes > OWNER);
    assert_eq!(decoded, &properties);
    drop(plan);
    assert_eq!(admission.memory_report().live_accounted_bytes, 0);
    drop(work);
    drop(admission);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_composite_decode_preserves_fixed_corruption_diagnostics() {
    for (identity, message) in [
        ("bad:00:00", "composite property projection has an invalid identity prefix"),
        ("hawdb-composite-property-v1:00", "composite property projection identity has fewer than two properties"),
        ("hawdb-composite-property-v1:0:00", "property projection composite property identity has an invalid hexadecimal length"),
        ("hawdb-composite-property-v1:gg:00", "property projection composite property identity has invalid hexadecimal data"),
        ("hawdb-composite-property-v1:ff:00", "property projection composite property identity is not UTF-8: invalid utf-8 sequence of 1 bytes from index 0"),
    ] {
        let ordinary = decode_composite_property_identity(identity).err().unwrap();
        assert!(matches!(ordinary, PersistentPropertyProjectionError::Corrupt(ref text) if text == message));
        let definitions = vec![definition(PersistentPropertyProjectionKind::CompositeEquality, identity.into())];
        let (governor, admission, work) = admitted(1024 * 1024);
        let controlled = prepare(&definitions, &work).err().unwrap();
        assert!(matches!(controlled, PersistentPropertyProjectionError::Corrupt(ref text) if text == message));
        assert_eq!(admission.memory_report().live_accounted_bytes, 0);
        drop(work);
        drop(admission);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
