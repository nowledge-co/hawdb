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

//! Reader-resident segment value summaries stay bounded (#1746).

use super::*;

/// A projection row shaped like `projection_row_from_node`: every node
/// property, including the body, is also document metadata.
fn memory(number: usize, space: &str, body_bytes: usize) -> SearchDocument {
    let mut body = format!("graph memory {number} ");
    body.extend(std::iter::repeat_n('b', body_bytes - body.len()));
    SearchDocument {
        id: format!("memory:{number:03}"),
        title: format!("Title {number}"),
        content: body.clone(),
        embedding: None,
        metadata: BTreeMap::from([
            ("content".into(), body),
            ("title".into(), format!("Title {number}")),
            ("space_id".into(), space.into()),
        ]),
    }
}

fn spaces(number: usize) -> &'static str {
    if number < 4 {
        "team"
    } else {
        "ops"
    }
}

fn build(root: &Path, body_bytes: usize) {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
    for number in 0..6 {
        writer
            .push(memory(number, spaces(number), body_bytes))
            .unwrap();
    }
    writer.finish().unwrap();
}

fn filtered(
    reader: &SearchOutOfCoreReader,
    field: &str,
    value: &str,
) -> (BTreeSet<String>, SearchPredicatePushdownReport) {
    let mut options = options(10, None);
    options
        .metadata_filters
        .insert(field.to_string(), value.to_string());
    let output = reader
        .search_with_options("graph", None, SearchMode::Text, options)
        .unwrap();
    (
        output.result.hits.into_iter().map(|hit| hit.id).collect(),
        output.result.candidate_set.metadata_predicate_pushdown,
    )
}

/// Distinct value bytes the reader keeps in segment value summaries.
fn retained_value_bytes(reader: &SearchOutOfCoreReader) -> usize {
    reader
        .segments
        .iter()
        .flat_map(|artifact| &artifact.descriptor.segments)
        .flat_map(|segment| segment.metadata.values())
        .flat_map(|summary| &summary.values)
        .map(String::len)
        .sum()
}

#[test]
#[cfg(feature = "full-text-search")]
fn body_value_summaries_are_omitted_and_filters_keep_their_results() {
    let root = test_dir("value-summary-bounds");
    build(&root, 1024);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    // The descriptor uses the V4 grammar and the selector requires a reader
    // that understands it.
    assert!(reader
        .manifest
        .segments
        .iter()
        .all(|segment| segment.descriptor_version == Some(VALUE_SUMMARY_DESCRIPTOR_VERSION)));
    assert_eq!(
        reader.manifest.min_reader_version,
        manifest_version::VALUE_SUMMARY_VERSION
    );
    let descriptor_text =
        fs::read_to_string(root.join(&reader.manifest.segments[0].descriptor_file)).unwrap();
    assert!(descriptor_text.starts_with("HAWDB_SEARCH_SEGMENTS_V4\n"));
    for segment in reader
        .segments
        .iter()
        .flat_map(|artifact| &artifact.descriptor.segments)
    {
        let content = &segment.metadata["content"];
        assert!(content.values_omitted && content.values.is_empty());
        assert_eq!(content.present_count, segment.document_count);
        assert!(!segment.metadata["space_id"].values_omitted);
        assert!(!segment.metadata["title"].values_omitted);
    }

    // An equality filter on the omitted body field cannot skip any segment
    // and still finds the one document that matches.
    let body = memory(3, spaces(3), 1024).content;
    let (hits, report) = filtered(&reader, "content", &body);
    assert_eq!(hits, BTreeSet::from(["memory:003".to_string()]));
    assert_eq!(report.pruned_segment_count, 0);
    let (hits, report) = filtered(&reader, "content", "no such body");
    assert!(hits.is_empty());
    assert_eq!(report.pruned_segment_count, 0);
    // Kept short-field summaries still skip segments.
    let (hits, report) = filtered(&reader, "space_id", "ops");
    assert_eq!(
        hits,
        BTreeSet::from(["memory:004".to_string(), "memory:005".to_string()])
    );
    assert!(report.pruned_segment_count > 0, "{report:?}");
    let (hits, report) = filtered(&reader, "title", "title 1");
    assert_eq!(hits, BTreeSet::from(["memory:001".to_string()]));
    assert!(report.pruned_segment_count > 0, "{report:?}");
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_value_summaries_do_not_grow_with_body_length() {
    let mut retained = Vec::new();
    for body_bytes in [300, 4096] {
        let root = test_dir("value-summary-retention");
        build(&root, body_bytes);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let bytes = retained_value_bytes(&reader);
        // Six documents keep their IDs, titles and space IDs, not bodies.
        assert!(bytes < 6 * 64, "body_bytes={body_bytes} retained={bytes}");
        retained.push(bytes);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
    assert_eq!(retained[0], retained[1]);
}

#[test]
#[cfg(feature = "full-text-search")]
fn legacy_full_value_descriptor_reopens_with_bounded_summaries() {
    // A resident checkpoint writes the V3 descriptor grammar with every
    // value, as out-of-core generations did before #1746.
    let root = test_dir("value-summary-legacy");
    let mut index = SearchIndex::open(&root).unwrap();
    for number in 0..6 {
        index.upsert(memory(number, spaces(number), 1024)).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let manifest = SearchOutOfCoreManifestBody::decode(
        &fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest.min_reader_version, manifest_version::BASE_VERSION);
    assert!(manifest
        .segments
        .iter()
        .all(|segment| segment.descriptor_version.is_none()));
    let text = fs::read_to_string(root.join(&manifest.segments[0].descriptor_file)).unwrap();
    assert!(text.starts_with("HAWDB_SEARCH_SEGMENTS_V3\n"));
    let legacy = crate::decode_search_segment_descriptor_text(&text).unwrap();
    assert!(legacy
        .segments
        .iter()
        .all(|segment| segment.metadata["content"].values.len() == segment.document_count));

    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    for segment in reader
        .segments
        .iter()
        .flat_map(|artifact| &artifact.descriptor.segments)
    {
        assert!(segment.metadata["content"].values_omitted);
        assert!(!segment.metadata["space_id"].values_omitted);
    }
    assert!(retained_value_bytes(&reader) < 6 * 64);
    let body = memory(2, spaces(2), 1024).content;
    let (hits, _) = filtered(&reader, "content", &body);
    assert_eq!(hits, BTreeSet::from(["memory:002".to_string()]));
    let (hits, report) = filtered(&reader, "space_id", "ops");
    assert_eq!(
        hits,
        BTreeSet::from(["memory:004".to_string(), "memory:005".to_string()])
    );
    assert!(report.pruned_segment_count > 0, "{report:?}");
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn value_summary_selector_is_refused_by_older_readers_and_checked_against_descriptors() {
    let root = test_dir("value-summary-version");
    build(&root, 1024);
    let path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    let bytes = fs::read(&path).unwrap();
    // A reader that predates the V4 descriptor refuses the selector with the
    // typed error instead of failing on the descriptor as `Storage`.
    for supported in 1..=3 {
        let error = SearchOutOfCoreManifestBody::decode_for_reader_version(&bytes, None, supported)
            .unwrap_err();
        let HawDBError::UnsupportedStorageFormat(refusal) = &error else {
            panic!("{error:?}");
        };
        assert_eq!(
            refusal.format(),
            hawdb_core::SEARCH_PROJECTION_MANIFEST_FORMAT
        );
        assert_eq!(
            error.to_string(),
            format!("unsupported search projection manifest version 4; this reader supports version {supported}")
        );
    }

    // A selector whose descriptor version disagrees with the descriptor
    // grammar is a storage failure.
    let mut manifest = SearchOutOfCoreManifestBody::decode(&bytes).unwrap();
    for segment in &mut manifest.segments {
        segment.descriptor_version = None;
    }
    fs::write(&path, manifest.encode().unwrap()).unwrap();
    let error = SearchOutOfCoreReader::open(&root).err().unwrap();
    assert!(
        matches!(&error, HawDBError::Storage(message) if message.contains("descriptor grammar")),
        "{error:?}"
    );
    fs::write(&path, &bytes).unwrap();
    SearchOutOfCoreReader::open(&root).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn omitted_value_marker_is_rejected_outside_a_v4_descriptor() {
    let root = test_dir("value-summary-marker");
    build(&root, 1024);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let text = fs::read_to_string(root.join(&reader.manifest.segments[0].descriptor_file)).unwrap();
    let (body, _) = crate::split_checksum(&text).unwrap();
    let body = body.replacen("HAWDB_SEARCH_SEGMENTS_V4", "HAWDB_SEARCH_SEGMENTS_V3", 1);
    let mut hasher = hawdb_integrity::Crc32cHasher::new();
    hasher.update(body.as_bytes());
    let forged = format!("{body}checksum\t{}\n", hasher.finish());
    let error = crate::decode_search_segment_descriptor_text(&forged).unwrap_err();
    assert!(error.to_string().contains("before V4"), "{error}");
    assert!(crate::decode_search_segment_descriptor_text(&text).is_ok());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

fn assert_refused(bytes: &[u8], found: u16, supported: u16) {
    let error =
        SearchOutOfCoreManifestBody::decode_for_reader_version(bytes, None, supported).unwrap_err();
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(hawdb_core::StorageFormatError::UnsupportedVersion {
            format: hawdb_core::SEARCH_PROJECTION_MANIFEST_FORMAT,
            found,
            supported,
        })
    );
}

/// Body bytes inside and above the value bound: V3 and V4 descriptors.
const KEPT: usize = 64;
const OMITTED: usize = 1024;

/// Runs `write` as a release that frames content as `stream1` or `blocks1`.
fn framed<T>(stream1: bool, write: impl FnOnce() -> T) -> T {
    use crate::out_of_core::content_blocks::tests::WRITE_STREAM1;
    WRITE_STREAM1.set(stream1);
    let output = write();
    WRITE_STREAM1.set(false);
    output
}

fn build_bodies(root: &Path, bodies: &[usize], max_content_documents: usize) {
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        root,
        SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(1),
            max_content_documents: NonZeroUsize::new(max_content_documents).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    for (number, body_bytes) in bodies.iter().enumerate() {
        writer.push(memory(number, "team", *body_bytes)).unwrap();
    }
    writer.finish().unwrap();
}

fn append(root: &Path, epoch: u64, bodies: &[(usize, usize)], max_content_documents: usize) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    let upserts = bodies
        .iter()
        .map(|(number, body_bytes)| {
            let document = memory(*number, "team", *body_bytes);
            crate::SearchProjectionRow {
                kind: crate::SearchProjectionKind::Memory,
                external_id: format!("{number:03}"),
                title: document.title,
                body: document.content,
                embedding: None,
                source_id: None,
                metadata: document.metadata,
            }
        })
        .collect();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        crate::SearchProjectionDelta {
            upserts,
            source_graph_commit_epoch: Some(epoch),
            ..Default::default()
        },
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(max_content_documents).unwrap(),
            ..Default::default()
        },
    )
    .unwrap()
    .finish()
    .unwrap();
}

/// `(blocks1, V4 descriptor)` of each selected segment, after checking that
/// the selector requires exactly `required`, reopens with every document and
/// is refused by every older reader with the gate.
fn assert_selector(root: &Path, required: u16, documents: usize) -> Vec<(bool, bool)> {
    let bytes = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let manifest = SearchOutOfCoreManifestBody::decode(&bytes).unwrap();
    assert_eq!(manifest.min_reader_version, required);
    assert_eq!(
        manifest.format_version,
        if required == manifest_version::BASE_VERSION {
            manifest_version::BASE_VERSION
        } else {
            manifest_version::READER_VERSION
        }
    );
    // A reader of the required version passes the gate; each older one
    // refuses with the typed error.
    SearchOutOfCoreManifestBody::decode_for_reader_version(&bytes, None, required).unwrap();
    for supported in manifest_version::BASE_VERSION..required {
        assert_refused(&bytes, required, supported);
    }
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    assert_eq!(reader.document_count(), documents);
    let ids = (0..documents)
        .map(|number| format!("memory:{number:03}"))
        .collect::<Vec<_>>();
    let output = reader.hydrate_documents(&ids).unwrap();
    assert_eq!(
        output
            .documents
            .iter()
            .map(|document| document.id.clone())
            .collect::<Vec<_>>(),
        ids
    );
    for document in &output.documents {
        assert_eq!(document.metadata["content"], document.content);
    }
    let report = reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(report.documents, documents as u64);
    manifest
        .segments
        .iter()
        .map(|segment| {
            let text = fs::read_to_string(root.join(&segment.descriptor_file)).unwrap();
            let v4 = segment.descriptor_version.is_some();
            assert_eq!(text.starts_with("HAWDB_SEARCH_SEGMENTS_V4\n"), v4);
            (!segment.content_encoding.is_stream1(), v4)
        })
        .collect()
}

/// Builds one segment per `(stream1, body bytes)` entry: a base generation of
/// one document and one single-document delta for each later entry.
fn build_segments(root: &Path, segments: &[(bool, usize)]) {
    framed(segments[0].0, || build_bodies(root, &[segments[0].1], 100));
    for (number, (stream1, body_bytes)) in segments.iter().enumerate().skip(1) {
        framed(*stream1, || {
            append(root, 1 + number as u64, &[(number, *body_bytes)], 100)
        });
    }
}

fn features((stream1, body_bytes): (bool, usize)) -> (bool, bool) {
    (!stream1, body_bytes == OMITTED)
}

#[test]
fn mixed_framing_closures_require_every_selected_segment_feature() {
    // Neither feature hides the other, in either segment order.
    for (segments, required) in [
        ([(true, KEPT), (true, KEPT)], 1),
        ([(true, KEPT), (false, KEPT)], 3),
        ([(false, KEPT), (true, KEPT)], 3),
        ([(true, OMITTED), (false, KEPT)], 4),
        ([(false, KEPT), (true, OMITTED)], 4),
        ([(true, KEPT), (false, OMITTED)], 4),
        ([(false, OMITTED), (true, KEPT)], 4),
    ] {
        let root = test_dir("value-summary-mixed");
        build_segments(&root, &segments);
        assert_eq!(
            assert_selector(&root, required, 2),
            segments.map(features).to_vec()
        );
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn compaction_requires_the_rewritten_and_carried_segment_features() {
    // Compaction rewrites the first two segments as one `blocks1` segment and
    // carries the third.
    for (segments, before, after) in [
        ([(true, KEPT), (true, KEPT), (true, KEPT)], 1, 3),
        ([(false, KEPT), (false, KEPT), (true, KEPT)], 3, 3),
        // The rewritten body omits its set again, in either framing.
        ([(true, OMITTED), (true, KEPT), (true, KEPT)], 4, 4),
        ([(true, KEPT), (false, OMITTED), (false, KEPT)], 4, 4),
        // Only the carried segment has a V4 descriptor.
        ([(false, KEPT), (true, KEPT), (true, OMITTED)], 4, 4),
    ] {
        let root = test_dir("value-summary-compaction");
        build_segments(&root, &segments);
        assert_eq!(
            assert_selector(&root, before, 3),
            segments.map(features).to_vec()
        );
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        SearchOutOfCoreGenerationWriter::compact_segments(
            &reader,
            crate::SearchOutOfCoreSegmentCompactionPolicy::new(
                NonZeroUsize::new(2).unwrap(),
                std::num::NonZeroU64::new(256 * 1024 * 1024).unwrap(),
            )
            .unwrap(),
            SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(3),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
        drop(reader);
        let rewritten = segments[..2]
            .iter()
            .any(|(_, body_bytes)| *body_bytes == OMITTED);
        assert_eq!(
            assert_selector(&root, after, 3),
            vec![(true, rewritten), features(segments[2])]
        );
        // A replacement without omitted sets needs no V4 descriptor.
        build_bodies(&root, &[KEPT; 2], 100);
        assert_eq!(assert_selector(&root, 3, 2), vec![(true, false)]);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn partitioned_publication_requires_the_joined_closure_version() {
    for (bodies, required) in [
        ([KEPT, KEPT, KEPT, KEPT], 3),
        ([OMITTED, OMITTED, KEPT, KEPT], 4),
        ([KEPT, KEPT, OMITTED, OMITTED], 4),
    ] {
        let root = test_dir("value-summary-partitioned");
        build_bodies(&root, &bodies, 2);
        assert_eq!(
            assert_selector(&root, required, 4),
            vec![(true, bodies[0] == OMITTED), (true, bodies[2] == OMITTED)]
        );
        fs::remove_dir_all(root).unwrap();
    }
    // New partitions join an active closure with the other feature.
    for (base, delta, required) in [
        ((true, KEPT), (false, KEPT), 3),
        ((true, OMITTED), (false, KEPT), 4),
        ((false, KEPT), (true, OMITTED), 4),
    ] {
        let root = test_dir("value-summary-partitioned-delta");
        framed(base.0, || build_bodies(&root, &[base.1; 2], 100));
        let rows = [2, 3, 4, 5].map(|number| (number, delta.1));
        framed(delta.0, || append(&root, 2, &rows, 2));
        let new = (!delta.0, delta.1 == OMITTED);
        assert_eq!(
            assert_selector(&root, required, 6),
            vec![(!base.0, base.1 == OMITTED), new, new]
        );
        fs::remove_dir_all(root).unwrap();
    }
}
