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

//! Point-read scopes reuse selected segments across reads, and projected reads
//! skip unselected values of segments validated by their point directory
//! (#1482). Results must equal full reads, scopes must not cross readers or
//! generations, pins must be released, and corruption must still fail closed.

use super::point_read_scope::retained_segments;
use super::tests::{collect_descriptors, replace_descriptor_tree, unique_path};
use super::*;
use hawdb_core::temporal;

const NODES: u64 = 400;
const FIRST_ID: u64 = 1_000;
/// Every fourth id is missing so lookups also land between records.
const ID_STEP: u64 = 4;
/// Segments of more than one point-directory stride of records.
const LARGE_SEGMENT: u64 = 16 * 1024;
/// Segments of at most one stride, read without a directory.
const SMALL_SEGMENT: u64 = 2048;

fn node(id: u64, generation: u64) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::from([LabelId(1), LabelId(u32::try_from(id % 3).unwrap() + 2)]),
        properties: BTreeMap::from([
            (
                "rank".to_string(),
                Value::Int((id * 10 + generation) as i64),
            ),
            ("score".to_string(), Value::Float(id as f64 / 3.0)),
            ("flag".to_string(), Value::Bool(id.is_multiple_of(2))),
            (
                "content".to_string(),
                Value::String(format!("été {id} {}", "x".repeat((id % 7) as usize * 16))),
            ),
            (
                "tags".to_string(),
                Value::List(vec![
                    Value::String(format!("t{id}")),
                    Value::List(vec![Value::Int(id as i64), Value::Null]),
                ]),
            ),
            (
                "meta".to_string(),
                Value::Map(BTreeMap::from([
                    ("k".to_string(), Value::String(format!("v{id}"))),
                    ("n".to_string(), Value::Int(-(id as i64))),
                ])),
            ),
            (
                "created".to_string(),
                Value::DateTime(temporal::parse_datetime("2026-10-30T12:00:00+02:00").unwrap()),
            ),
            (
                "day".to_string(),
                Value::Date(temporal::parse_date("2010-02-14").unwrap()),
            ),
            (
                "blob".to_string(),
                Value::Binary(vec![0xff, 0xfe, id as u8]),
            ),
            (
                "uid".to_string(),
                Value::Uuid(hawdb_core::Uuid::from_u128(u128::from(id))),
            ),
            (
                "embedding".to_string(),
                Value::Vector(hawdb_core::Vector::new(&[id as f32, 0.5]).unwrap()),
            ),
        ]),
    }
}

fn nodes(generation: u64) -> Vec<NodeRecord> {
    (0..NODES)
        .map(|index| node(FIRST_ID + index * ID_STEP, generation))
        .collect()
}

fn write(
    name: &str,
    generation: u64,
    target_segment_bytes: u64,
) -> (impl AsRef<Path>, CanonicalSegmentManifest) {
    let path = unique_path(name);
    let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
        target_segment_bytes: NonZeroU64::new(target_segment_bytes).unwrap(),
        max_record_bytes: NonZeroU64::new(64 * 1024).unwrap(),
    })
    .write(
        &path,
        ManifestGeneration(generation),
        nodes(generation),
        std::iter::empty::<RelRecord>(),
    )
    .unwrap();
    (path, manifest)
}

fn open(
    path: &Path,
    manifest: &CanonicalSegmentManifest,
    cache: &Arc<SegmentCache>,
) -> CanonicalSegmentReader {
    CanonicalSegmentReader::open(
        path,
        manifest.clone(),
        Arc::clone(cache),
        StoreId(1482),
        NonZeroU64::new(1024 * 1024).unwrap(),
    )
    .unwrap()
}

fn cache() -> Arc<SegmentCache> {
    Arc::new(SegmentCache::new(64 * 1024 * 1024))
}

fn selected() -> BTreeSet<String> {
    BTreeSet::from(["rank".to_string(), "tags".to_string(), "day".to_string()])
}

/// The full record projected to `properties`, as a projected read returns it.
fn project(node: &NodeRecord, properties: &BTreeSet<String>) -> ProjectedNodeRecord {
    ProjectedNodeRecord {
        id: node.id,
        labels: node.labels.clone(),
        properties: node
            .properties
            .iter()
            .filter(|(key, _)| properties.contains(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    }
}

/// A projected read through the admitted node input, as the store performs it.
fn admitted_projection(
    reader: &CanonicalSegmentReader,
    id: u64,
    properties: &BTreeSet<String>,
) -> Option<(ProjectedNodeRecord, usize)> {
    reader
        .with_node_input(NodeId(id), |input| {
            let mut admitted = 0;
            input
                .decode_selected(properties, |bytes| {
                    admitted = bytes;
                    Ok(())
                })
                .map(|(node, ())| (node, admitted))
        })
        .unwrap()
}

fn all_ids() -> impl DoubleEndedIterator<Item = u64> + Clone {
    // Includes ids between records, below the first and past the last.
    (FIRST_ID - 2)..(FIRST_ID + NODES * ID_STEP + 2)
}

#[test]
fn scoped_reads_seek_each_segment_once_and_equal_full_reads() {
    let (path, manifest) = write("scope_once", 7, LARGE_SEGMENT);
    let cache = cache();
    let reader = open(path.as_ref(), &manifest, &cache);
    let segments = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
    assert!(segments.len() >= 4, "{} segments", segments.len());
    // Only the final remainder segment may be small enough to have no directory.
    assert!(
        segments[..segments.len() - 1]
            .iter()
            .all(|segment| segment.record_count > point_directory::POINT_DIRECTORY_STRIDE),
        "every full segment must use a validated point directory"
    );
    let expected = nodes(7)
        .into_iter()
        .map(|node| (node.id.0, node))
        .collect::<BTreeMap<_, _>>();
    let properties = selected();
    // Unscoped reference: one descriptor seek per read, as before #1482.
    let mut unscoped = Vec::new();
    let seeks = point_descriptor_seeks_for_test();
    for id in all_ids() {
        unscoped.push(admitted_projection(&reader, id, &properties));
    }
    assert_eq!(
        point_descriptor_seeks_for_test() - seeks,
        all_ids().count() as u64
    );
    for order in [
        all_ids().collect::<Vec<_>>(),
        all_ids().rev().collect::<Vec<_>>(),
    ] {
        let scope = CanonicalPointReadScope::enter();
        let seeks = point_descriptor_seeks_for_test();
        let mut segment_reads = 0;
        let mut previous_segment = None;
        for &id in &order {
            let read = admitted_projection(&reader, id, &properties);
            let reference = &unscoped[(id - (FIRST_ID - 2)) as usize];
            assert_eq!(&read, reference, "id {id}");
            assert_eq!(
                read.as_ref().map(|(node, _)| node),
                expected
                    .get(&id)
                    .map(|node| project(node, &properties))
                    .as_ref(),
                "id {id}"
            );
            let segment = segments
                .iter()
                .position(|segment| segment.min_record_id <= id && id <= segment.max_record_id);
            if segment.is_some() && segment != previous_segment {
                segment_reads += 1;
                previous_segment = segment;
            }
        }
        assert_eq!(segment_reads, segments.len());
        // Ids outside every segment always consult the descriptor tree; the
        // reads inside segments seek once per segment.
        let outside = order
            .iter()
            .filter(|id| {
                !segments
                    .iter()
                    .any(|segment| segment.min_record_id <= **id && **id <= segment.max_record_id)
            })
            .count();
        assert_eq!(
            point_descriptor_seeks_for_test() - seeks,
            (segments.len() + outside) as u64
        );
        assert!(cache.snapshot().pinned_bytes > 0);
        drop(scope);
        assert_eq!(retained_segments(), 0);
        assert_eq!(
            cache.snapshot().pinned_bytes,
            0,
            "a closed scope keeps no pins"
        );
    }
    // Full, unprojected reads agree as well, inside and outside a scope.
    let scope = CanonicalPointReadScope::enter();
    for id in all_ids() {
        assert_eq!(
            reader.get_node(NodeId(id)).unwrap().as_ref(),
            expected.get(&id)
        );
        assert_eq!(
            reader.get_projected_node(NodeId(id), &properties).unwrap(),
            expected.get(&id).map(|node| project(node, &properties))
        );
    }
    drop(scope);
    assert!(!reader.is_poisoned());
}

#[test]
fn projected_reads_match_full_reads_for_every_property_selection() {
    // Small segments have no directory: their reads still validate the
    // unselected values on every read.
    for (target, expect_directory) in [(LARGE_SEGMENT, true), (SMALL_SEGMENT, false)] {
        let (path, manifest) = write("scope_selection", 3, target);
        let cache = cache();
        let reader = open(path.as_ref(), &manifest, &cache);
        let segments = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
        assert!(segments.len() > 1);
        // The final segment holds the remainder and may be small either way.
        assert!(segments[..segments.len() - 1].iter().all(|segment| {
            (segment.record_count > point_directory::POINT_DIRECTORY_STRIDE) == expect_directory
        }));
        let keys = node(FIRST_ID, 3).properties.into_keys().collect::<Vec<_>>();
        let mut selections = vec![BTreeSet::new(), keys.iter().cloned().collect()];
        selections.extend(keys.iter().map(|key| BTreeSet::from([key.clone()])));
        selections.push(BTreeSet::from([
            "missing".to_string(),
            "content".to_string(),
        ]));
        let expected = nodes(3);
        // Skipping by length and validating agree on every record and selection.
        for segment in &segments {
            let bytes = reader.read_segment(segment).unwrap();
            decode_segment_records(&bytes, manifest.generation, segment, |id, payload| {
                let node = expected.iter().find(|node| node.id.0 == id).unwrap();
                for properties in &selections {
                    let decode = |validated| {
                        decode_projected_node_with_property_spills(
                            id,
                            payload,
                            None,
                            Some(reader.property_keys()),
                            properties,
                            validated,
                        )
                        .unwrap()
                    };
                    assert_eq!(decode(true), project(node, properties));
                    assert_eq!(decode(false), project(node, properties));
                    let measure = |validated| {
                        node_payload_allocation_bytes(
                            payload,
                            reader.property_keys(),
                            None,
                            Some(properties),
                            validated,
                        )
                        .unwrap()
                    };
                    assert_eq!(measure(true), measure(false), "{properties:?}");
                }
                Ok(())
            })
            .unwrap();
        }
        let scope = CanonicalPointReadScope::enter();
        for properties in &selections {
            for node in &expected {
                let (projected, admitted) =
                    admitted_projection(&reader, node.id.0, properties).unwrap();
                assert_eq!(projected, project(node, properties), "{properties:?}");
                assert_eq!(
                    Some(admitted),
                    reader
                        .projected_node_allocation_bytes(node.id, properties)
                        .unwrap(),
                );
            }
        }
        drop(scope);
        assert_eq!(cache.snapshot().pinned_bytes, 0);
    }
}

#[test]
fn scopes_never_mix_readers_or_generations() {
    // Two generations with the same ids, extents and store share one cache.
    let (old_path, old_manifest) = write("scope_generation_old", 11, LARGE_SEGMENT);
    let (new_path, new_manifest) = write("scope_generation_new", 12, LARGE_SEGMENT);
    let cache = cache();
    let old = open(old_path.as_ref(), &old_manifest, &cache);
    let new = open(new_path.as_ref(), &new_manifest, &cache);
    let old_clone = old.clone();
    let properties = BTreeSet::from(["rank".to_string()]);
    let rank = |reader: &CanonicalSegmentReader, id: u64| {
        reader
            .get_projected_node(NodeId(id), &properties)
            .unwrap()
            .map(|node| node.properties["rank"].clone())
    };
    let _scope = CanonicalPointReadScope::enter();
    let seeks = point_descriptor_seeks_for_test();
    for index in 0..NODES {
        let id = FIRST_ID + index * ID_STEP;
        assert_eq!(rank(&old, id), Some(Value::Int((id * 10 + 11) as i64)));
        assert_eq!(rank(&new, id), Some(Value::Int((id * 10 + 12) as i64)));
        // A clone reads the same immutable artifact and shares its segments.
        assert_eq!(
            rank(&old_clone, id),
            Some(Value::Int((id * 10 + 11) as i64))
        );
    }
    let segments = collect_descriptors(&old, CanonicalSegmentKind::Nodes).len()
        + collect_descriptors(&new, CanonicalSegmentKind::Nodes).len();
    assert_eq!(point_descriptor_seeks_for_test() - seeks, segments as u64);
    // A reader opened again for the same generation is a different snapshot
    // handle and selects its own segments.
    let reopened = open(old_path.as_ref(), &old_manifest, &cache);
    let seeks = point_descriptor_seeks_for_test();
    assert_eq!(
        rank(&reopened, FIRST_ID),
        Some(Value::Int((FIRST_ID * 10 + 11) as i64))
    );
    assert_eq!(point_descriptor_seeks_for_test() - seeks, 1);
}

#[test]
fn scoped_reads_are_per_thread_and_bounded_by_cache_capacity() {
    let (path, manifest) = write("scope_threads", 5, LARGE_SEGMENT);
    let cache = cache();
    let reader = open(path.as_ref(), &manifest, &cache);
    let segment_count = collect_descriptors(&reader, CanonicalSegmentKind::Nodes).len() as u64;
    let properties = selected();
    let expected = nodes(5);
    std::thread::scope(|threads| {
        for thread in 0..4u64 {
            let (reader, properties, expected) = (reader.clone(), &properties, &expected);
            threads.spawn(move || {
                let _scope = CanonicalPointReadScope::enter();
                let seeks = point_descriptor_seeks_for_test();
                for round in 0..3 {
                    // Each thread walks the ids in its own rotation.
                    let offset = (thread as usize * 97 + round) % expected.len();
                    for node in expected[offset..].iter().chain(&expected[..offset]) {
                        assert_eq!(
                            reader.get_projected_node(node.id, properties).unwrap(),
                            Some(project(node, properties))
                        );
                    }
                }
                // At most the four retained segments are reused per rotation.
                assert!(point_descriptor_seeks_for_test() - seeks <= 3 * (segment_count + 1));
            });
        }
    });
    assert_eq!(cache.snapshot().pinned_bytes, 0);

    // A cache too small to spare a quarter of its capacity for one segment
    // never retains it, so scoped reads fall back to the ordinary path.
    let segment_bytes = collect_descriptors(&reader, CanonicalSegmentKind::Nodes)[0]
        .length
        .get();
    let small = Arc::new(SegmentCache::new(segment_bytes * 2));
    let reader = open(path.as_ref(), &manifest, &small);
    let _scope = CanonicalPointReadScope::enter();
    let seeks = point_descriptor_seeks_for_test();
    for node in &expected[..40] {
        assert_eq!(
            reader.get_projected_node(node.id, &properties).unwrap(),
            Some(project(node, &properties))
        );
    }
    assert_eq!(point_descriptor_seeks_for_test() - seeks, 40);
    assert_eq!(retained_segments(), 0);
}

/// Replaces the bytes of the node segment holding `id` with `corrupt(bytes)` and binds the
/// descriptor tree to the corrupted bytes' digest, as an undetected physical
/// corruption that the segment checksum cannot catch would look.
fn corrupt_segment_rebinding_digest(
    path: &Path,
    manifest: &mut CanonicalSegmentManifest,
    id: u64,
    corrupt: impl FnOnce(&mut [u8]),
) {
    let reader = open(path, manifest, &cache());
    let mut descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
    drop(reader);
    let first = descriptors
        .iter_mut()
        .find(|segment| segment.min_record_id <= id && id <= segment.max_record_id)
        .unwrap();
    let mut artifact = std::fs::read(path).unwrap();
    let range = first.offset as usize..(first.offset + first.length.get()) as usize;
    corrupt(&mut artifact[range.clone()]);
    first.content_digest = crate::cache::content_digest(&artifact[range]);
    std::fs::write(path, artifact).unwrap();
    replace_descriptor_tree(path, manifest, descriptors);
}

/// Overwrites the last byte of `content` in record `id` with invalid UTF-8.
fn corrupt_content_of(id: u64) -> impl FnOnce(&mut [u8]) {
    move |segment: &mut [u8]| {
        let needle = format!("été {id} ");
        let start = segment
            .windows(needle.len())
            .position(|window| window == needle.as_bytes())
            .expect("record content is stored inline");
        segment[start + needle.len() - 1] = 0xff;
    }
}

#[test]
fn unselected_invalid_utf8_still_fails_closed() {
    let victim = FIRST_ID + ID_STEP;
    for (target, scoped) in [
        (LARGE_SEGMENT, true),
        (LARGE_SEGMENT, false),
        (SMALL_SEGMENT, true),
        (SMALL_SEGMENT, false),
    ] {
        let (path, mut manifest) = write("scope_utf8", 9, target);
        corrupt_segment_rebinding_digest(
            path.as_ref(),
            &mut manifest,
            victim,
            corrupt_content_of(victim),
        );
        let reader = open(path.as_ref(), &manifest, &cache());
        let _scope = scoped.then(CanonicalPointReadScope::enter);
        let properties = BTreeSet::from(["rank".to_string()]);
        // A large segment's directory build validates every record, so even a
        // projection of another record of the segment fails; a small segment
        // validates the unselected values of the record it reads.
        let read = if target == LARGE_SEGMENT {
            FIRST_ID
        } else {
            victim
        };
        let result = reader.get_projected_node(NodeId(read), &properties);
        assert!(
            matches!(&result, Err(CanonicalSegmentError::Corrupt(reason)) if reason.contains("UTF-8")),
            "target {target}, scoped {scoped}: {result:?}"
        );
        assert!(reader.is_poisoned());
        assert!(reader
            .get_projected_node(NodeId(read), &properties)
            .is_err());
    }
}

#[test]
fn unselected_malformed_values_fail_closed() {
    // An unknown value tag in an unselected property of another record.
    let (path, mut manifest) = write("scope_tag", 13, LARGE_SEGMENT);
    let victim = FIRST_ID + ID_STEP;
    corrupt_segment_rebinding_digest(path.as_ref(), &mut manifest, victim, |segment| {
        let needle = format!("été {victim} ").into_bytes();
        let start = segment
            .windows(needle.len())
            .position(|window| window == needle.as_slice())
            .unwrap();
        // Tag byte and u32 length precede the string bytes.
        segment[start - 5] = 0xee;
    });
    let reader = open(path.as_ref(), &manifest, &cache());
    let _scope = CanonicalPointReadScope::enter();
    let result = reader.get_projected_node(NodeId(FIRST_ID), &BTreeSet::from(["rank".to_string()]));
    assert!(
        matches!(&result, Err(CanonicalSegmentError::Corrupt(reason)) if reason.contains("tag")),
        "{result:?}"
    );
    assert!(reader.is_poisoned());
}

#[test]
fn retained_segments_do_not_outlive_a_failure() {
    let (path, manifest) = write("scope_digest", 15, LARGE_SEGMENT);
    let cache = cache();
    let segments = collect_descriptors(
        &open(path.as_ref(), &manifest, &cache),
        CanonicalSegmentKind::Nodes,
    );
    // Corrupt the second segment on disk without rebinding its digest.
    let mut artifact = std::fs::read(path.as_ref()).unwrap();
    artifact[segments[1].offset as usize + 40] ^= 0x55;
    std::fs::write(path.as_ref(), artifact).unwrap();
    let reader = open(path.as_ref(), &manifest, &cache);
    let properties = BTreeSet::from(["rank".to_string()]);
    let _scope = CanonicalPointReadScope::enter();
    let first = segments[0].min_record_id;
    assert!(reader
        .get_projected_node(NodeId(first), &properties)
        .unwrap()
        .is_some());
    assert_eq!(retained_segments(), 1);
    assert!(matches!(
        reader.get_projected_node(NodeId(segments[1].min_record_id), &properties),
        Err(CanonicalSegmentError::Read(
            SegmentReadError::DigestMismatch { .. }
        ))
    ));
    assert!(reader.is_poisoned());
    // The retained, still valid first segment must not serve a poisoned reader.
    assert!(matches!(
        reader.get_projected_node(NodeId(first), &properties),
        Err(CanonicalSegmentError::Corrupt(reason)) if reason.contains("poisoned")
    ));
    assert!(reader
        .with_node_input(NodeId(first), |_| -> Result<(), CanonicalSegmentError> {
            panic!("a poisoned reader must not decode")
        })
        .is_err());
}

#[test]
fn descriptor_corruption_fails_closed_inside_a_scope() {
    let (path, mut manifest) = write("scope_descriptor", 17, LARGE_SEGMENT);
    let reader = open(path.as_ref(), &manifest, &cache());
    let mut descriptors = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
    drop(reader);
    // The tree now claims a digest the segment bytes do not have.
    descriptors[0].content_digest = ContentDigest(descriptors[0].content_digest.0 ^ 1);
    replace_descriptor_tree(path.as_ref(), &mut manifest, descriptors);
    let reader = open(path.as_ref(), &manifest, &cache());
    let _scope = CanonicalPointReadScope::enter();
    assert!(reader
        .get_projected_node(NodeId(FIRST_ID), &BTreeSet::from(["rank".to_string()]))
        .is_err());
    assert!(reader.is_poisoned());
    assert_eq!(retained_segments(), 0);
}

#[test]
fn single_walk_projection_equals_the_two_walk_reference() {
    let (path, manifest) = write("scope_single_walk", 4, LARGE_SEGMENT);
    let reader = open(path.as_ref(), &manifest, &cache());
    let keys = node(FIRST_ID, 4).properties.into_keys().collect::<Vec<_>>();
    assert!(keys.len() > LOCATED_SELECTION_CAPACITY);
    // A bounded table of named selections: no key, each key alone (one per
    // encoded value type of the fixture), exactly the located capacity, one
    // past it (the two-walk fallback), every key, and keys a record does not
    // hold. Selection capacity and admission do not depend on which record
    // is read, so a few representative records suffice (#1662 review).
    let capacity = LOCATED_SELECTION_CAPACITY;
    let mut selections: Vec<(String, BTreeSet<String>)> =
        vec![("empty".to_string(), BTreeSet::new())];
    for key in &keys {
        selections.push((format!("only {key}"), BTreeSet::from([key.clone()])));
    }
    selections.push((
        format!("exactly {capacity}"),
        keys[..capacity].iter().cloned().collect(),
    ));
    selections.push((
        format!("{} (fallback)", capacity + 1),
        keys[..capacity + 1].iter().cloned().collect(),
    ));
    selections.push(("all".to_string(), keys.iter().cloned().collect()));
    selections.push((
        "absent and present".to_string(),
        BTreeSet::from(["missing".to_string(), "content".to_string()]),
    ));
    const RECORDS: usize = 3;
    let expected = nodes(4);
    let (mut located, mut fallback) = (0, 0);
    // Only payloads of directory-validated segments take the single walk.
    let segment = collect_descriptors(&reader, CanonicalSegmentKind::Nodes)
        .into_iter()
        .find(|segment| segment.record_count > point_directory::POINT_DIRECTORY_STRIDE)
        .expect("a directory-backed node segment");
    let bytes = reader.read_segment(&segment).unwrap();
    let mut records = 0;
    decode_segment_records(&bytes, manifest.generation, &segment, |id, payload| {
        if records == RECORDS {
            return Ok(());
        }
        records += 1;
        {
            let node = expected.iter().find(|node| node.id.0 == id).unwrap();
            let keys = reader.property_keys();
            for (case, properties) in &selections {
                let reference_bytes =
                    node_payload_allocation_bytes(payload, keys, None, Some(properties), true)
                        .unwrap();
                let reference = decode_projected_node_with_property_spills(
                    id,
                    payload,
                    None,
                    Some(keys),
                    properties,
                    true,
                )
                .unwrap();
                assert_eq!(reference, project(node, properties));
                match SelectedNodeValues::locate(payload, keys, None, properties).unwrap() {
                    Some(selection) => {
                        located += 1;
                        assert_eq!(selection.allocation_bytes, reference_bytes);
                        assert_eq!(
                            selection.decode(NodeId(id), payload, None).unwrap(),
                            reference
                        );
                    }
                    None => {
                        fallback += 1;
                        assert!(properties.len() > LOCATED_SELECTION_CAPACITY, "{case}");
                    }
                }
                // As the store reads: admission sees exactly the reference
                // bytes before decoding, and a refusal decodes nothing.
                let input = CanonicalNodeInput {
                    id: NodeId(id),
                    payload,
                    property_keys: keys,
                    property_spills: None,
                    values_validated: true,
                };
                let mut admitted = None;
                let (read, ()) = input
                    .decode_selected(properties, |bytes| {
                        admitted = Some(bytes);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(admitted, Some(reference_bytes), "{case}");
                assert_eq!(read, reference);
                let refused = input.decode_selected(properties, |_| {
                    Err::<(), _>(CanonicalSegmentError::Source("refused".into()))
                });
                assert!(
                    matches!(refused, Err(CanonicalSegmentError::Source(_))),
                    "{case}"
                );
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(records, RECORDS);
    // Exactly the capacity is located; one past it and every key fall back.
    assert_eq!(
        fallback,
        2 * RECORDS,
        "{located} located, {fallback} fallback"
    );
    assert_eq!(located, (selections.len() - 2) * RECORDS);
}
