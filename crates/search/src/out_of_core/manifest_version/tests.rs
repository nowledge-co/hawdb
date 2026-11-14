// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! #1763: a selector that requires a newer reader is refused with a typed
//! error before field decode; base selectors keep their pre-#1763 bytes.

use super::super::{
    SearchOutOfCoreManifestBody, SearchOutOfCoreReader, MAX_OUT_OF_CORE_MANIFEST_BYTES,
    OUT_OF_CORE_MANIFEST_FILE,
};
use super::*;
use crate::{
    SearchIndex, SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        Self(
            std::env::var_os("TEST_TMPDIR")
                .map_or_else(std::env::temp_dir, PathBuf::from)
                .join(format!(
                    "hawdb-manifest-version-{name}-{}",
                    hawdb_core::generate_uuidv7().unwrap()
                )),
        )
    }

    fn search(&self) -> PathBuf {
        self.0.join("search")
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn row(id: &str, body: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: format!("title {id}"),
        body: body.into(),
        embedding: Some(vec![1.0, 0.5]),
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn delta(epoch: u64, upserts: Vec<SearchProjectionRow>) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts,
        deletes: Vec::new(),
        max_operations: None,
        source_graph_commit_epoch: Some(epoch),
    }
}

fn resident(path: &Path) {
    let mut index = SearchIndex::open(path).unwrap();
    index
        .apply_projection_delta(delta(3, vec![row("a", "version alpha")]))
        .unwrap();
    index.checkpoint().unwrap();
}

fn out_of_core(path: &Path) {
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(4),
        ..Default::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, options).unwrap();
    for id in ["a", "b"] {
        writer
            .push(row(id, "version base").into_document())
            .unwrap();
    }
    writer.finish().unwrap();
}

fn selector(root: &Path) -> Vec<u8> {
    std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap()
}

/// What a reader of selector version `supported` makes of `bytes`.
fn read_as(bytes: &[u8], supported: u16) -> Result<SearchOutOfCoreManifestBody> {
    SearchOutOfCoreManifestBody::decode_for_reader_version(bytes, None, supported)
}

/// A selector body predating #1763 carries neither version field, and a
/// release predating the version fields could decode it.
fn assert_base_selector(bytes: &[u8]) {
    let text = std::str::from_utf8(bytes).unwrap();
    for field in [
        "format_version",
        "min_reader_version",
        "content_source_graph_commit_epoch",
    ] {
        assert!(!text.contains(field), "{field} in {text}");
    }
    let manifest = read_as(bytes, BASE_VERSION).unwrap();
    assert_eq!(manifest.format_version, BASE_VERSION);
    assert_eq!(manifest.min_reader_version, BASE_VERSION);
}

/// Rewrite the selector body as a future writer would, with a valid
/// checksum: `edit` gets the body as JSON.
fn future_selector(
    bytes: &[u8],
    edit: impl FnOnce(&mut serde_json::Map<String, serde_json::Value>),
) -> Vec<u8> {
    let mut envelope: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    edit(envelope["body"].as_object_mut().unwrap());
    let body = serde_json::to_vec(&envelope["body"]).unwrap();
    let mut out = BODY_PREFIX.to_vec();
    out.extend_from_slice(&body);
    out.extend_from_slice(CHECKSUM_MARKER);
    out.extend_from_slice(checksum_bytes(&body).to_string().as_bytes());
    out.push(b'}');
    out
}

fn version_three(body: &mut serde_json::Map<String, serde_json::Value>) {
    body.insert("format_version".into(), 3.into());
    body.insert("min_reader_version".into(), 3.into());
    body.insert("field_from_the_future".into(), true.into());
}

fn assert_refused(error: HawDBError, found: u16, supported: u16) {
    assert!(!matches!(error, HawDBError::Storage(_)), "{error:?}");
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: FORMAT_NAME,
            found,
            supported,
        })
    );
}

#[test]
fn newer_selector_is_refused_with_a_typed_error_before_field_decode() {
    let root = Dir::new("newer");
    let path = root.search();
    resident(&path);
    let future = future_selector(&selector(&path), version_three);
    std::fs::write(path.join(OUT_OF_CORE_MANIFEST_FILE), &future).unwrap();

    // The unknown field would fail the strict decode as `Storage`; the
    // version gate refuses first, on every open path.
    assert_refused(
        read_as(&future, READER_VERSION).unwrap_err(),
        3,
        READER_VERSION,
    );
    assert_refused(
        SearchOutOfCoreReader::open(&path).err().unwrap(),
        3,
        READER_VERSION,
    );
    assert_refused(SearchIndex::open(&path).err().unwrap(), 3, READER_VERSION);
    assert_eq!(
        SearchIndex::open(&path).err().unwrap().to_string(),
        "unsupported search projection manifest version 3; this reader supports version 2"
    );
    // A version that does not fit the error's field saturates.
    let huge = future_selector(&selector(&path), |body| {
        body.insert("min_reader_version".into(), 70_000.into());
    });
    assert_refused(
        read_as(&huge, READER_VERSION).unwrap_err(),
        u16::MAX,
        READER_VERSION,
    );
}

#[test]
fn damaged_or_inconsistent_selectors_stay_storage_failures() {
    let root = Dir::new("damaged");
    let path = root.search();
    out_of_core(&path);
    let future = future_selector(&selector(&path), version_three);
    // A checksum mismatch is corruption, not a newer format.
    let mut damaged = future.clone();
    let digit = damaged.len() - 2;
    damaged[digit] = if damaged[digit] == b'0' { b'1' } else { b'0' };
    assert!(matches!(
        read_as(&damaged, READER_VERSION),
        Err(HawDBError::Storage(_))
    ));
    // So is a selector that is not a canonical envelope.
    let mut padded = future.clone();
    padded.push(b' ');
    assert!(matches!(
        read_as(&padded, READER_VERSION),
        Err(HawDBError::Storage(_))
    ));
    // An intact selector whose versions contradict each other.
    let manifest = read_as(&selector(&path), READER_VERSION).unwrap();
    for (format_version, min_reader_version) in [(1, 2), (2, 0), (2, 3)] {
        let mut body = manifest.clone();
        body.format_version = format_version;
        body.min_reader_version = min_reader_version;
        let checksum = checksum_bytes(&serde_json::to_vec(&body).unwrap());
        let bytes =
            serde_json::to_vec(&super::super::SearchOutOfCoreManifestEnvelope { body, checksum })
                .unwrap();
        // Version 3 passes the gate of a version-3 reader and is still
        // inconsistent with the fields this reader knows.
        let supported = min_reader_version.max(READER_VERSION);
        let error = read_as(&bytes, supported).unwrap_err();
        assert_eq!(
            error,
            HawDBError::Storage(
                "search out-of-core manifest header or segment set is invalid".into()
            ),
            "{format_version} {min_reader_version}"
        );
    }
    // A supported version still rejects unknown fields strictly.
    let unknown = future_selector(&selector(&path), |body| {
        body.insert("field_from_the_future".into(), true.into());
    });
    assert!(matches!(
        read_as(&unknown, READER_VERSION),
        Err(HawDBError::Storage(_))
    ));
}

#[test]
fn unversioned_selectors_stay_readable_and_byte_identical() {
    let root = Dir::new("unversioned");
    let resident_path = root.0.join("resident");
    resident(&resident_path);
    assert_base_selector(&selector(&resident_path));
    SearchIndex::open(&resident_path).unwrap();

    let path = root.search();
    out_of_core(&path);
    let bytes = selector(&path);
    assert_base_selector(&bytes);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reader.document_count(), 2);
    // Re-encoding a base selector reproduces its bytes exactly.
    let manifest = read_as(&bytes, READER_VERSION).unwrap();
    assert_eq!(manifest.encode().unwrap(), bytes);
}

#[test]
fn epoch_advanced_selector_requires_version_two_and_round_trips() {
    let root = Dir::new("advanced");
    let path = root.search();
    out_of_core(&path);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    reader.advance_source_graph_commit_epoch(9).unwrap();
    drop(reader);

    let bytes = selector(&path);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(
        text.contains("\"format_version\":2,\"min_reader_version\":2"),
        "{text}"
    );
    let manifest = read_as(&bytes, READER_VERSION).unwrap();
    assert_eq!(manifest.min_reader_version, CONTENT_EPOCH_VERSION);
    assert_eq!(manifest.format_version, READER_VERSION);
    assert_eq!(manifest.content_source_graph_commit_epoch, Some(4));
    assert_eq!(manifest.source_graph_commit_epoch, Some(9));
    assert_eq!(manifest.encode().unwrap(), bytes);
    assert_eq!(
        read_as(&manifest.encode().unwrap(), READER_VERSION).unwrap(),
        manifest
    );
    // A version-1 reader with the gate refuses it with the typed error
    // instead of failing on the unknown content epoch.
    assert_refused(read_as(&bytes, BASE_VERSION).unwrap_err(), 2, BASE_VERSION);
    let reopened = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reopened.source_graph_commit_epoch(), Some(9));

    // An advance stamps the version even from a selector decoded without one,
    // and a chained advance keeps it.
    reopened.advance_source_graph_commit_epoch(11).unwrap();
    let chained = read_as(&selector(&path), READER_VERSION).unwrap();
    assert_eq!(chained.min_reader_version, CONTENT_EPOCH_VERSION);
    assert_eq!(chained.content_source_graph_commit_epoch, Some(4));
}

#[test]
fn content_publication_after_an_advance_returns_to_the_base_version() {
    let root = Dir::new("after-advance");
    // Resident: a checkpoint after an advance.
    let resident_path = root.0.join("resident");
    resident(&resident_path);
    let mut index = SearchIndex::open(&resident_path).unwrap();
    index.advance_source_graph_commit_epoch(6).unwrap();
    assert!(read_as(&selector(&resident_path), BASE_VERSION).is_err());
    index
        .apply_projection_delta(delta(8, vec![row("b", "version beta")]))
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    assert_base_selector(&selector(&resident_path));
    assert_eq!(
        SearchIndex::open(&resident_path).unwrap().document_count(),
        2
    );

    // Out-of-core: an incremental delta after an advance.
    let path = root.search();
    out_of_core(&path);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    reader.advance_source_graph_commit_epoch(9).unwrap();
    drop(reader);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        delta(12, vec![row("c", "after advance")]),
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    drop(reader);
    let bytes = selector(&path);
    assert_base_selector(&bytes);
    assert!(bytes.len() as u64 <= MAX_OUT_OF_CORE_MANIFEST_BYTES);
    assert_eq!(
        SearchOutOfCoreReader::open(&path).unwrap().document_count(),
        3
    );
}
