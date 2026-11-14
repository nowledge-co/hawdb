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
    let error = SearchOutOfCoreManifestBody::decode_for_reader_version(
        &bytes,
        None,
        manifest_version::CONTENT_EPOCH_VERSION,
    )
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
        "unsupported search projection manifest version 3; this reader supports version 2"
    );

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
