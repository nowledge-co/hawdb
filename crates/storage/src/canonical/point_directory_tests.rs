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

//! Point directories adapt their stride to the segment's frame size, and a
//! segment of dense ids computes its directory entry instead of searching it.
//! Reads must equal the records written, examine one directory entry and at
//! most one stride of frames per dense read, keep the directory within its
//! memory bound, and fail closed when the directory disagrees with the bytes.

use super::*;
use crate::canonical::tests::{collect_descriptors, unique_path};

const NODES: u64 = 600;
const FIRST_ID: u64 = 10_000;
const TARGET_SEGMENT_BYTES: u64 = 64 * 1024;
/// Records of minimum frames fill several segments of this size.
const SMALL_TARGET_SEGMENT_BYTES: u64 = 4 * 1024;

/// A record of about `content_bytes` of payload.
fn node(id: u64, content_bytes: usize) -> NodeRecord {
    let properties = if content_bytes == 0 {
        BTreeMap::new()
    } else {
        BTreeMap::from([
            ("rank".to_string(), Value::Int(id as i64)),
            (
                "content".to_string(),
                Value::String(format!("{id} {}", "é".repeat(content_bytes / 2))),
            ),
        ])
    };
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::new(),
        properties,
    }
}

fn nodes(id_step: u64, content_bytes: usize) -> Vec<NodeRecord> {
    (0..NODES)
        .map(|index| node(FIRST_ID + index * id_step, content_bytes))
        .collect()
}

struct Written {
    _path: crate::canonical::tests::CanonicalFixturePath,
    reader: CanonicalSegmentReader,
    expected: BTreeMap<u64, NodeRecord>,
    segments: Vec<CanonicalSegmentDescriptor>,
}

fn write(name: &str, records: Vec<NodeRecord>, target_segment_bytes: u64) -> Written {
    let path = unique_path(name);
    let manifest = CanonicalSegmentWriter::new(CanonicalSegmentConfig {
        target_segment_bytes: NonZeroU64::new(target_segment_bytes).unwrap(),
        max_record_bytes: NonZeroU64::new(64 * 1024).unwrap(),
    })
    .write(
        &path,
        ManifestGeneration(5),
        records.clone(),
        std::iter::empty::<RelRecord>(),
    )
    .unwrap();
    let reader = CanonicalSegmentReader::open(
        &path,
        manifest,
        Arc::new(SegmentCache::new(64 * 1024 * 1024)),
        StoreId(1615),
        NonZeroU64::new(1024 * 1024).unwrap(),
    )
    .unwrap();
    let segments = collect_descriptors(&reader, CanonicalSegmentKind::Nodes);
    Written {
        _path: path,
        reader,
        expected: records.into_iter().map(|node| (node.id.0, node)).collect(),
        segments,
    }
}

/// The most frames a read of `id` may examine: one stride and the first
/// frame past it with a directory, every frame without one.
fn frame_bound(segments: &[CanonicalSegmentDescriptor], id: u64) -> u64 {
    segments
        .iter()
        .find(|segment| segment.min_record_id <= id && id <= segment.max_record_id)
        .map_or(0, |segment| {
            if segment.record_count > POINT_DIRECTORY_STRIDE {
                u64::from(directory_stride(segment)) + 1
            } else {
                u64::from(segment.record_count)
            }
        })
}

/// Reads, directory entries and frames examined while reading every id from
/// just below the first record to just past the last, checking each result
/// and that no read examines more than its bound of frames.
fn read_all(written: &Written) -> (u64, u64, u64) {
    let (first, last) = (
        *written.expected.keys().next().unwrap(),
        *written.expected.keys().next_back().unwrap(),
    );
    let properties = BTreeSet::from(["rank".to_string()]);
    let _scope = CanonicalPointReadScope::enter();
    let (entries, frames) = (point_directory_entries_examined(), point_frames_examined());
    let mut reads = 0;
    let mut bound = 0;
    for id in (first - 2)..=(last + 2) {
        bound += 2 * frame_bound(&written.segments, id);
        let expected = written.expected.get(&id);
        assert_eq!(
            written.reader.get_node(NodeId(id)).unwrap().as_ref(),
            expected,
            "id {id}"
        );
        assert_eq!(
            written
                .reader
                .get_projected_node(NodeId(id), &properties)
                .unwrap()
                .map(|node| node.properties),
            expected.map(|node| {
                node.properties
                    .iter()
                    .filter(|(key, _)| properties.contains(*key))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>()
            }),
            "id {id}"
        );
        reads += 2;
    }
    let frames = point_frames_examined() - frames;
    assert!(frames <= bound, "{frames} frames, bound {bound}");
    (reads, point_directory_entries_examined() - entries, frames)
}

/// Segments of more than one maximum stride of records, which have a
/// directory.
fn directed(written: &Written) -> Vec<&CanonicalSegmentDescriptor> {
    assert!(
        written.segments.len() > 2,
        "{} segments",
        written.segments.len()
    );
    written
        .segments
        .iter()
        .filter(|segment| segment.record_count > POINT_DIRECTORY_STRIDE)
        .collect()
}

fn directory_bytes(segment: &CanonicalSegmentDescriptor) -> u64 {
    (directory_entries(segment, directory_stride(segment)) * ENTRY_BYTES) as u64
}

/// The directory of the fixed maximum stride used before strides adapted.
fn maximum_stride_directory_bytes(segment: &CanonicalSegmentDescriptor) -> u64 {
    (directory_entries(segment, POINT_DIRECTORY_STRIDE) * ENTRY_BYTES) as u64
}

#[test]
fn dense_large_records_examine_one_entry_and_at_most_a_short_stride() {
    let written = write("directory_dense_large", nodes(1, 300), TARGET_SEGMENT_BYTES);
    let directed = directed(&written);
    assert!(directed.len() + 1 >= written.segments.len());
    for segment in &directed {
        assert!(dense_ids(segment));
        // About 330-byte frames take a stride of one or two records, and the
        // directory stays within its fraction of the segment.
        assert!(directory_stride(segment) <= 2, "{segment:?}");
        assert!(directory_bytes(segment) * DIRECTORY_SEGMENT_FRACTION <= segment.length.get());
    }
    let directed_records = directed
        .iter()
        .map(|segment| u64::from(segment.record_count))
        .sum::<u64>();
    // Warm the cached segments and directories, then measure warm reads.
    read_all(&written);
    let (reads, entries, frames) = read_all(&written);
    // Each read of a directed segment examines exactly one entry: no search.
    assert_eq!(entries, 2 * directed_records, "{reads} reads");
    // A dense read walks only within its stride, so reads of directed
    // segments examine at most two frames each; before the stride adapted,
    // a read walked about half of 32 frames on average.
    let undirected = written
        .segments
        .iter()
        .filter(|segment| segment.record_count <= POINT_DIRECTORY_STRIDE)
        .map(|segment| u64::from(segment.record_count).pow(2))
        .sum::<u64>();
    assert!(
        frames <= 2 * (2 * directed_records + undirected),
        "{frames} frames for {reads} reads"
    );
}

#[test]
fn minimum_frames_keep_the_maximum_stride_and_its_memory_bound() {
    // Records without labels or properties have the smallest frames, so a
    // finer stride would exceed the directory's memory bound.
    let written = write(
        "directory_dense_small",
        nodes(1, 0),
        SMALL_TARGET_SEGMENT_BYTES,
    );
    assert!(directed(&written).len() > 2);
    for segment in &written.segments {
        if segment.record_count > POINT_DIRECTORY_STRIDE {
            assert_eq!(directory_stride(segment), POINT_DIRECTORY_STRIDE);
        }
        assert!(directory_bytes(segment) <= maximum_stride_directory_bytes(segment));
    }
    let (reads, entries, _) = read_all(&written);
    assert!(entries <= reads, "{entries} entries for {reads} reads");
}

#[test]
fn gapped_ids_still_binary_search_and_equal_written_records() {
    // Every third id is used, so no segment is dense and reads search their
    // directory: more than one entry per read, still at most a stride.
    let written = write("directory_gapped", nodes(3, 300), TARGET_SEGMENT_BYTES);
    let directed = directed(&written);
    assert!(directed.len() + 1 >= written.segments.len());
    assert!(directed.iter().all(|segment| !dense_ids(segment)
        && directory_bytes(segment) * DIRECTORY_SEGMENT_FRACTION <= segment.length.get()));
    read_all(&written);
    let (reads, entries, _) = read_all(&written);
    assert!(entries > reads, "{entries} entries for {reads} reads");
}

#[test]
fn every_directory_memory_stays_within_its_previous_bound() {
    // For every frame size: never larger than the maximum-stride directory
    // or a twenty-fourth of the segment, whichever is larger.
    for content in [0, 8, 24, 60, 120, 300, 1200] {
        let target = if content < 60 {
            SMALL_TARGET_SEGMENT_BYTES
        } else {
            TARGET_SEGMENT_BYTES
        };
        let written = write("directory_bound", nodes(1, content), target);
        for segment in &written.segments {
            let bound = maximum_stride_directory_bytes(segment)
                .max(segment.length.get() / DIRECTORY_SEGMENT_FRACTION);
            assert!(
                directory_bytes(segment) <= bound,
                "content {content}: {segment:?}"
            );
            let stride = directory_stride(segment);
            assert!(stride.is_power_of_two() && stride <= POINT_DIRECTORY_STRIDE);
        }
    }
}

#[test]
fn dense_seek_fails_closed_when_the_directory_disagrees() {
    let written = write("directory_disagrees", nodes(1, 300), TARGET_SEGMENT_BYTES);
    let segment = &written.segments[0];
    assert!(dense_ids(segment) && segment.record_count > POINT_DIRECTORY_STRIDE);
    let bytes = written.reader.read_segment(segment).unwrap();
    let generation = written.reader.manifest.generation;
    let directory = build_point_directory(
        &bytes,
        generation,
        segment,
        RecordValidation {
            property_keys: written.reader.property_keys(),
            property_spill_count: None,
        },
    )
    .unwrap();
    let id = segment.min_record_id + u64::from(segment.record_count) / 2;
    let found = locate_point_frame(generation, segment, &bytes, Some(&directory), id)
        .unwrap()
        .unwrap();
    assert_eq!(found.id, id);
    let entry = (id - segment.min_record_id) as usize / directory_stride(segment) as usize;
    let start = entry * ENTRY_BYTES;
    // An entry claiming another id, an entry pointing at another frame, and
    // a directory of the wrong length are all corruption.
    let mut wrong_id = directory.clone();
    wrong_id[start..start + 8].copy_from_slice(&(id + 1).to_le_bytes());
    let mut wrong_offset = directory.clone();
    let next = u64::from_le_bytes(
        directory[start + ENTRY_BYTES + 8..start + 2 * ENTRY_BYTES]
            .try_into()
            .unwrap(),
    );
    wrong_offset[start + 8..start + ENTRY_BYTES].copy_from_slice(&next.to_le_bytes());
    let short = &directory[..directory.len() - ENTRY_BYTES];
    for directory in [&wrong_id[..], &wrong_offset[..], short] {
        let result = locate_point_frame(generation, segment, &bytes, Some(directory), id);
        assert!(
            matches!(result, Err(CanonicalSegmentError::Corrupt(_))),
            "{:?}",
            result.map(|frame| frame.map(|frame| frame.id))
        );
    }
}
