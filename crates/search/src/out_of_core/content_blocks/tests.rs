// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Block-framed content segments (#1917): format, selective hydration,
//! corruption scope, deep verification, compatibility and cache accounting.

use super::*;
use crate::SearchSegmentPayloadRange;
use std::cell::Cell;
use std::io::Seek;

thread_local! {
    static BLOCK_INFLATED: Cell<u64> = const { Cell::new(0) };
    pub(in crate::out_of_core) static WRITE_STREAM1: Cell<bool> = const { Cell::new(false) };
}

pub(in crate::out_of_core) fn record_block_inflated(bytes: u64) {
    BLOCK_INFLATED.set(BLOCK_INFLATED.get() + bytes);
}

fn take_block_inflated() -> u64 {
    BLOCK_INFLATED.replace(0)
}

const DOCUMENTS: usize = 40;
/// Content bytes of a large document: one record per block (two binary
/// records exceed `BLOCK_TARGET_BYTES`).
const LARGE: usize = 9000;
/// Content bytes of a small document: several records per block.
const SMALL: usize = 600;

fn document_sized(number: usize, size: usize) -> SearchDocument {
    // Varied words keep the hex records from compressing to nothing.
    let mut content = format!("uniqword{number} shared ");
    let mut state = (number as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    while content.len() < size {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        content.push_str(&format!("w{:x} ", state >> 44));
    }
    SearchDocument {
        id: format!("document:{number:04}"),
        title: format!("title {number}"),
        content,
        embedding: Some(vec![number as f32, 1.0, 0.5]),
        metadata: BTreeMap::from([("kind".to_string(), "note".to_string())]),
    }
}

fn document(number: usize) -> SearchDocument {
    document_sized(number, LARGE)
}

fn build(stream1: bool) -> PathBuf {
    build_sized(stream1, LARGE)
}

fn build_sized(stream1: bool, size: usize) -> PathBuf {
    let sequence = CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let root = crate::test_temp_dir().join(format!(
        "hawdb-content-blocks-{}-{sequence}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    WRITE_STREAM1.set(stream1);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in 0..DOCUMENTS {
        writer.push(document_sized(number, size)).unwrap();
    }
    let result = writer.finish();
    WRITE_STREAM1.set(false);
    result.unwrap();
    root
}

fn open(root: &Path, cache: u64) -> SearchOutOfCoreReader {
    SearchOutOfCoreReader::open_with_config(
        root,
        SearchOutOfCoreConfig {
            content_cache_bytes: cache,
            ..Default::default()
        },
    )
    .unwrap()
}

fn selector(root: &Path) -> Vec<u8> {
    std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap()
}

/// The segment holding `id`, its range and its verified index.
fn locate(
    reader: &SearchOutOfCoreReader,
    id: &str,
) -> (usize, SearchSegmentPayloadRange, Index<Vec<u8>>) {
    let route = reader.segment_for_document(id).unwrap().unwrap();
    let range = route.segment.payload_range.unwrap();
    let artifact = &reader.segments[route.artifact_index];
    let mut bytes = vec![0; range.length as usize];
    read_search_range(&artifact.payload, range.offset, &mut bytes).unwrap();
    let index_len = Header::declared_index_len(&bytes).unwrap() as usize;
    bytes.truncate(HEADER_BYTES + index_len);
    let index = Index::parse(
        bytes,
        Expect {
            length: range.length,
            checksum: range.checksum,
            max_uncompressed_bytes: u64::MAX,
            descriptor: Some((
                route.segment.document_count,
                &route.segment.first_document_id,
                &route.segment.last_document_id,
            )),
        },
        None,
    )
    .unwrap();
    (route.artifact_index, range, index)
}

fn corrupt(root: &Path, reader: &SearchOutOfCoreReader, artifact: usize, offset: u64) {
    let path = root.join(&reader.manifest.segments[artifact].payload_file);
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0];
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    std::io::Read::read_exact(&mut file, &mut byte).unwrap();
    file.seek(std::io::SeekFrom::Start(offset)).unwrap();
    file.write_all(&[byte[0] ^ 0x5a]).unwrap();
    file.sync_all().unwrap();
}

fn is_storage(error: &HawDBError) -> bool {
    matches!(error, HawDBError::Storage(_))
}

#[test]
fn new_generations_write_blocks_behind_a_version_three_selector() {
    let root = build(false);
    let bytes = selector(&root);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("\"content_encoding\":\"blocks1\""));
    assert!(text.contains("\"min_reader_version\":3"));
    let reader = open(&root, 0);
    let (_, range, index) = locate(&reader, "document:0007");
    // Unit-test segments hold two documents: one large record per block.
    assert_eq!(index.entries().len(), 2);
    assert!(index
        .entries()
        .iter()
        .all(|entry| u64::from(entry.uncompressed_len) <= BLOCK_TARGET_BYTES));
    let last = index.entries().last().unwrap();
    assert_eq!(last.offset + u64::from(last.compressed_len), range.length);
    // Round trip: every document reads back exactly.
    let ids = (0..DOCUMENTS)
        .map(|number| format!("document:{number:04}"))
        .collect::<Vec<_>>();
    let output = reader.hydrate_documents(&ids).unwrap();
    assert_eq!(
        output.documents,
        (0..DOCUMENTS).map(document).collect::<Vec<_>>()
    );
    let report = reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(report.documents, DOCUMENTS as u64);
    assert_eq!(report.segments, report.block_segments);
    // Small records share a block, which is scanned against its own bounds.
    let small = build_sized(false, SMALL);
    let small_reader = open(&small, 0);
    let (_, _, small_index) = locate(&small_reader, "document:0007");
    assert_eq!(small_index.entries().len(), 1);
    assert_eq!(small_index.entries()[0].document_count, 2);
    let output = small_reader.hydrate_documents(&ids).unwrap();
    assert_eq!(
        output.documents,
        (0..DOCUMENTS)
            .map(|number| document_sized(number, SMALL))
            .collect::<Vec<_>>()
    );
    let report = small_reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(report.documents, DOCUMENTS as u64);
    fs::remove_dir_all(small).unwrap();
    // A version-2 reader refuses the selector with the typed error.
    let error =
        SearchOutOfCoreManifestBody::decode_for_reader_version(&bytes, None, 2).unwrap_err();
    assert!(matches!(
        error,
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: SEARCH_PROJECTION_MANIFEST_FORMAT,
            found: 3,
            supported: 2,
        })
    ));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn hydrating_k_documents_reads_k_blocks_not_the_segment() {
    let root = build(false);
    let reader = open(&root, 0);
    let (_, range, index) = locate(&reader, "document:0007");
    take_block_inflated();
    let output = reader
        .hydrate_documents(&["document:0007".to_string()])
        .unwrap();
    assert_eq!(output.documents, vec![document(7)]);
    let metrics = output.metrics;
    assert_eq!(metrics.content_index_reads, 1);
    assert_eq!(metrics.content_blocks_read, 1);
    let block = index.entries()[index.find("document:0007").unwrap()];
    assert_eq!(
        metrics.content_block_bytes_inflated,
        u64::from(block.uncompressed_len)
    );
    assert_eq!(take_block_inflated(), u64::from(block.uncompressed_len));
    // The header and index prefetch plus one block, never the other block.
    assert!(
        metrics.segment_bytes_read
            <= HEADER_BYTES as u64 + INDEX_PREFETCH_BYTES + u64::from(block.compressed_len)
    );
    assert!(u64::from(block.uncompressed_len) < index.header.uncompressed_len);
    let _ = range;
    assert_eq!(metrics.segment_range_reads, 2);
    // Three ids spread over three blocks read three blocks.
    let output = reader
        .hydrate_documents(&[
            "document:0001".to_string(),
            "document:0020".to_string(),
            "document:0030".to_string(),
        ])
        .unwrap();
    assert_eq!(output.metrics.content_blocks_read, 3);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "full-text-search")]
#[test]
fn search_hits_hydrate_only_their_blocks() {
    let root = build(false);
    let reader = open(&root, 0);
    let options = SearchQueryOptions {
        limit: 3,
        offset: 0,
        rank_window: None,
        fusion_weights: Default::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let output = reader
        .search_with_options("uniqword23", None, SearchMode::Text, options.clone())
        .unwrap();
    assert_eq!(output.result.hits[0].id, "document:0023");
    assert_eq!(output.result.hits[0].kind.as_deref(), Some("note"));
    let metrics = output.metrics;
    assert!(metrics.content_blocks_read >= 1);
    assert!(metrics.content_blocks_read <= output.result.hits.len() as u64);
    assert!(metrics.content_block_bytes_inflated <= 3 * BLOCK_TARGET_BYTES);
    // Repeating the query with a cache serves the blocks verified.
    let cached = open(&root, 1 << 20);
    let first = cached
        .search_with_options("uniqword23", None, SearchMode::Text, options.clone())
        .unwrap();
    let second = cached
        .search_with_options("uniqword23", None, SearchMode::Text, options)
        .unwrap();
    assert_eq!(first.result.hits, second.result.hits);
    assert_eq!(second.metrics.content_blocks_read, 0);
    assert_eq!(
        second.metrics.content_block_cache_hits,
        first.metrics.content_blocks_read
    );
    assert_eq!(
        second.metrics.content_index_cache_hits,
        first.metrics.content_index_reads
    );
    assert_eq!(second.metrics.segment_bytes_read, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_damaged_block_fails_only_requests_that_read_it_and_deep_verification() {
    let root = build(false);
    let reader = open(&root, 0);
    let (artifact, range, index) = locate(&reader, "document:0007");
    let damaged = index.find("document:0007").unwrap();
    let entry = index.entries()[damaged];
    corrupt(
        &root,
        &reader,
        artifact,
        range.offset + entry.offset + u64::from(entry.compressed_len) / 2,
    );
    let healthy = index.first(&index.entries()[1 - damaged]).to_string();
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&healthy))
            .unwrap()
            .documents
            .len(),
        1
    );
    let error = reader
        .hydrate_documents(&["document:0007".to_string()])
        .unwrap_err();
    assert!(is_storage(&error), "{error:?}");
    let error = reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap_err();
    assert!(is_storage(&error), "{error:?}");
    // Whole-segment consumers keep refusing the damaged segment.
    let error = reader
        .visit_projection_documents(
            crate::SearchProjectionScanBudget {
                max_rows: 1 << 20,
                max_payload_bytes: 1 << 30,
                max_encoded_record_bytes: 1 << 20,
            },
            &hawdb_core::RuntimeTaskContext::default(),
            &mut |_| Ok(()),
        )
        .unwrap_err();
    assert!(is_storage(&error), "{error:?}");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_damaged_index_fails_every_request_in_its_segment() {
    let root = build(false);
    let reader = open(&root, 0);
    let (artifact, range, index) = locate(&reader, "document:0007");
    // A byte inside the index (the second entry's compressed CRC).
    corrupt(
        &root,
        &reader,
        artifact,
        range.offset + HEADER_BYTES as u64 + 8,
    );
    let other = index
        .first(&index.entries()[index.entries().len() - 1])
        .to_string();
    for id in ["document:0007".to_string(), other] {
        let error = reader.hydrate_documents(&[id]).unwrap_err();
        assert_eq!(
            error,
            HawDBError::Storage("search content blocks payload checksum mismatch".into())
        );
    }
    assert!(reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .is_err());
    fs::remove_dir_all(root).unwrap();
}

/// A `header || index` image whose digest is recomputed after `edit`.
fn reframe(index: &Index<Vec<u8>>, edit: impl FnOnce(&mut Vec<u8>)) -> (Vec<u8>, u64) {
    let mut bytes = index.bytes.clone();
    edit(&mut bytes);
    let mut digest = Crc32cHasher::new();
    digest.update(&bytes);
    (bytes, digest.finish())
}

#[test]
fn index_structure_and_reserved_fields_are_refused_typed_or_as_storage() {
    let root = build(false);
    let reader = open(&root, 0);
    let (_, range, index) = locate(&reader, "document:0007");
    let parse = |bytes: Vec<u8>, checksum: u64| {
        Index::parse(
            bytes,
            Expect {
                length: range.length,
                checksum,
                max_uncompressed_bytes: u64::MAX,
                descriptor: None,
            },
            None,
        )
        .map(|_| ())
    };
    let typed = |error: HawDBError| matches!(error, HawDBError::UnsupportedStorageFormat(_));
    // Newer layout, other codec, dictionary, flags and reserved bytes.
    for (at, value) in [(8, 3u8), (10, 2), (12, 1), (11, 1), (40, 1)] {
        let (bytes, checksum) = reframe(&index, |bytes| bytes[at] = value);
        let error = parse(bytes, checksum).unwrap_err();
        assert!(typed(error.clone()), "offset {at}: {error:?}");
    }
    let (bytes, checksum) = reframe(&index, |bytes| bytes[8] = 3);
    assert_eq!(
        parse(bytes, checksum).unwrap_err(),
        HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
            format: SEARCH_PROJECTION_MANIFEST_FORMAT,
            found: 3,
            supported: LAYOUT_VERSION,
        })
    );
    // The unreleased text-record layout 1 is refused, not misread.
    let (bytes, checksum) = reframe(&index, |bytes| bytes[8] = 1);
    let error = parse(bytes, checksum).unwrap_err();
    assert!(is_storage(&error) && error.to_string().contains("never released"));
    // An intact-looking header over a bad digest is never decoded.
    let (bytes, checksum) = reframe(&index, |bytes| bytes[8] = 3);
    assert!(is_storage(&parse(bytes, checksum ^ 1).unwrap_err()));
    // Truncated, garbled and over-long indexes are storage errors.
    type Edit = Box<dyn FnOnce(&mut Vec<u8>)>;
    let structural: [Edit; 5] = [
        Box::new(|bytes| {
            bytes.pop();
        }),
        Box::new(|bytes| bytes.push(0)),
        // Block count above what the index holds.
        Box::new(|bytes| bytes[16..20].copy_from_slice(&u32::MAX.to_le_bytes())),
        // First entry's compressed length: blocks no longer tile the range.
        Box::new(|bytes| bytes[HEADER_BYTES] ^= 1),
        // First entry's document count.
        Box::new(|bytes| bytes[HEADER_BYTES + 16] ^= 1),
    ];
    for edit in structural {
        let (bytes, checksum) = reframe(&index, edit);
        assert!(is_storage(&parse(bytes, checksum).unwrap_err()));
    }
    // The reader bounds the index read before trusting it.
    assert!(Header::declared_index_len(&index.bytes[..HEADER_BYTES - 1]).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn envelope_artifacts_from_earlier_releases_still_read() {
    let root = build(true);
    let bytes = selector(&root);
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(!text.contains("content_encoding"));
    assert!(!text.contains("min_reader_version"));
    let reader = open(&root, 1 << 20);
    assert!(reader
        .segments
        .iter()
        .all(|artifact| artifact.content_encoding == ContentEncoding::Stream1));
    let output = reader
        .hydrate_documents(&["document:0007".to_string()])
        .unwrap();
    assert_eq!(output.documents, vec![document(7)]);
    assert_eq!(output.metrics.content_blocks_read, 0);
    let route = reader
        .segment_for_document("document:0007")
        .unwrap()
        .unwrap();
    assert_eq!(
        output.metrics.segment_bytes_read,
        route.segment.payload_range.unwrap().length
    );
    let report = reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(
        (report.documents, report.block_segments),
        (DOCUMENTS as u64, 0)
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn the_content_cache_is_bounded_and_admits_only_verified_bytes() {
    let root = build(false);
    let ids = ["document:0007".to_string(), "document:0031".to_string()];
    let reader = open(&root, 1 << 20);
    let first = reader.hydrate_documents(&ids).unwrap();
    let snapshot = reader.content_cache_snapshot().unwrap();
    assert_eq!(snapshot.capacity_bytes, 1 << 20);
    assert!(snapshot.resident_bytes > 0 && snapshot.resident_bytes <= 1 << 20);
    assert_eq!(snapshot.pinned_bytes, 0, "requests release their leases");
    assert_eq!(snapshot.entry_count, 4, "two indexes and two blocks");
    let second = reader.hydrate_documents(&ids).unwrap();
    assert_eq!(first.documents, second.documents);
    assert_eq!(second.metrics.content_block_cache_hits, 2);
    assert_eq!(second.metrics.content_index_cache_hits, 2);
    assert_eq!(second.metrics.segment_bytes_read, 0);
    // A cache too small for any block refuses admission; reads stay correct.
    let tiny = open(&root, 64);
    for _ in 0..2 {
        let output = tiny.hydrate_documents(&ids).unwrap();
        assert_eq!(output.documents, first.documents);
        assert_eq!(output.metrics.content_blocks_read, 2);
    }
    let snapshot = tiny.content_cache_snapshot().unwrap();
    assert!(snapshot.admission_rejection_count > 0);
    assert!(snapshot.resident_bytes <= 64);
    // Disabled cache.
    assert!(open(&root, 0).content_cache_snapshot().is_none());
    // A damaged block is never admitted, so it keeps failing.
    let fresh = open(&root, 1 << 20);
    let (artifact, range, index) = locate(&fresh, "document:0007");
    let entry = index.entries()[index.find("document:0007").unwrap()];
    corrupt(&root, &fresh, artifact, range.offset + entry.offset + 3);
    for _ in 0..2 {
        assert!(fresh.hydrate_documents(&ids[..1]).is_err());
    }
    assert_eq!(fresh.content_cache_snapshot().unwrap().entry_count, 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn records_above_the_buffered_block_limit_stream_through_text_reencoding() {
    let root = crate::test_temp_dir().join(format!(
        "hawdb-content-blocks-large-{}-{}",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let mut documents = (0..4)
        .map(|number| document_sized(number, SMALL))
        .collect::<Vec<_>>();
    // Long words keep the large record within the lexical token limit.
    let mut content = String::new();
    while content.len() <= MAX_BUFFERED_BLOCK_BYTES as usize {
        content.push_str(&format!("{}{} ", "w".repeat(60), content.len() % 977));
    }
    documents[2].content = content;
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for document in &documents {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    let reader = open(&root, 1 << 20);
    let (_, _, index) = locate(&reader, "document:0002");
    let block = index.entries()[index.find("document:0002").unwrap()];
    assert!(u64::from(block.uncompressed_len) > MAX_BUFFERED_BLOCK_BYTES);
    assert_eq!(block.document_count, 1);
    let ids = documents
        .iter()
        .map(|document| document.id.clone())
        .collect::<Vec<_>>();
    // By-id hydration streams the large block and buffers the others.
    let output = reader.hydrate_documents(&ids).unwrap();
    assert_eq!(output.documents, documents);
    // Whole-segment consumers read the re-encoded text records.
    let report = reader
        .verify_content_segments(&hawdb_core::RuntimeTaskContext::default())
        .unwrap();
    assert_eq!(report.documents, documents.len() as u64);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

/// Positioned reads over an in-memory segment.
struct Bytes<'a>(&'a [u8]);

impl SegmentSource for Bytes<'_> {
    fn read_exact_at(&self, offset: u64, output: &mut [u8]) -> Result<()> {
        let start = offset as usize;
        output.copy_from_slice(&self.0[start..start + output.len()]);
        Ok(())
    }
}

#[test]
fn streamed_runs_and_single_block_reads_agree_on_frame_boundaries() {
    let root = build(false);
    let reader = open(&root, 0);
    let (artifact, range, index) = locate(&reader, "document:0007");
    let mut segment = vec![0; range.length as usize];
    read_search_range(
        &reader.segments[artifact].payload,
        range.offset,
        &mut segment,
    )
    .unwrap();
    let entries = index.entries().to_vec();
    assert_eq!(entries.len(), 2);
    // Verdicts of a streamed run (deep verification and whole-segment
    // consumers) and of each sparse block read, for one index.
    let verdicts = |index: &Index<Vec<u8>>| {
        let start = index.entries()[0].offset as usize;
        let streamed = stream_blocks(
            &segment[start..],
            index,
            0..index.entries().len(),
            None,
            |_| Ok(()),
        )
        .is_ok();
        let mut decoder = crate::build_memory::decoder::BlockDecoder::new(None, None).unwrap();
        let sparse = index
            .entries()
            .iter()
            .map(|entry| {
                load_block(
                    &Bytes(&segment),
                    None,
                    0,
                    entry,
                    &mut decoder,
                    None,
                    &mut Reads::default(),
                )
                .is_ok()
            })
            .collect::<Vec<_>>();
        (streamed, sparse)
    };
    assert_eq!(verdicts(&index), (true, vec![true, true]));
    // Move the boundary between the two frames by `shift` bytes, keeping the
    // spans tiling the range, their compressed CRCs and every inflated length
    // and CRC valid: only the frame boundaries are wrong.
    for shift in [-7i64, -1, 1, 5] {
        let first = (entries[0].compressed_len as i64 + shift) as u32;
        let second = (entries[1].compressed_len as i64 - shift) as u32;
        let crc = |offset: u64, length: u32| {
            let mut digest = Crc32cHasher::new();
            digest.update(&segment[offset as usize..(offset + u64::from(length)) as usize]);
            digest.finish_u32()
        };
        let first_crc = crc(entries[0].offset, first);
        let second_crc = crc(entries[0].offset + u64::from(first), second);
        let second_at = HEADER_BYTES
            + ENTRY_FIXED_BYTES
            + index.first(&entries[0]).len()
            + index.last(&entries[0]).len();
        let (bytes, checksum) = reframe(&index, |bytes| {
            for (at, length, crc) in [
                (HEADER_BYTES, first, first_crc),
                (second_at, second, second_crc),
            ] {
                bytes[at..at + 4].copy_from_slice(&length.to_le_bytes());
                bytes[at + 8..at + 12].copy_from_slice(&crc.to_le_bytes());
            }
        });
        let shifted = Index::parse(
            bytes,
            Expect {
                length: range.length,
                checksum,
                max_uncompressed_bytes: u64::MAX,
                descriptor: None,
            },
            None,
        )
        .unwrap();
        assert_eq!(
            verdicts(&shifted),
            (false, vec![false, false]),
            "shift {shift}"
        );
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

/// A task admitted with `bytes` of working memory.
fn task_with_memory(
    governor: &hawdb_qos::RuntimeGovernor,
    bytes: u64,
) -> (hawdb_qos::RuntimePermit, hawdb_core::RuntimeTaskContext) {
    let permit = governor
        .try_admit(hawdb_qos::RuntimeWorkRequest::background_maintenance(bytes))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    (permit, task)
}

#[test]
fn deep_verification_of_a_large_document_stays_within_task_memory() {
    let _serial = crate::test_allocation::serial();
    let root = crate::test_temp_dir().join(format!(
        "hawdb-content-blocks-verify-{}-{}",
        std::process::id(),
        CANDIDATE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir(&root).unwrap();
    let mut documents = (0..3)
        .map(|number| document_sized(number, SMALL))
        .collect::<Vec<_>>();
    // Long words keep the large record within the lexical token limit.
    let mut content = String::new();
    while content.len() < (4 << 20) - (64 << 10) {
        content.push_str(&format!("{}{} ", "v".repeat(60), content.len() % 991));
    }
    documents[1].content = content;
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for document in &documents {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    let reader = open(&root, 0);
    let governor = hawdb_qos::RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig::shared_host(),
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(
                NonZeroUsize::new(2).unwrap(),
                None,
                None,
            ),
            hawdb_qos::RuntimeMemorySnapshot::from_limits(
                Some(4 << 30),
                Some(4 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    );
    // Less task memory than the ~4 MiB document (~8 MiB as a text line): the
    // record streams through the taps and is never materialized.
    let allowance = 3584u64 << 10;
    let (permit, task) = task_with_memory(&governor, allowance);
    let (result, peak) = crate::test_allocation::measure(|| reader.verify_content_segments(&task));
    let report = result.unwrap();
    assert_eq!(report.documents, documents.len() as u64);
    assert!(peak < allowance as usize, "peak {peak}");
    drop((task, permit));
    // Far less memory than one decoder needs is a typed refusal, not a
    // storage fault or an unaccounted allocation.
    let (_permit, task) = task_with_memory(&governor, 64 << 10);
    let (result, peak) = crate::test_allocation::measure(|| reader.verify_content_segments(&task));
    assert!(
        matches!(result, Err(HawDBError::ResourceBudgetExceeded(ref error))
            if error.resource == hawdb_core::ResourceBudgetKind::MemoryBytes),
        "{result:?}"
    );
    assert!(peak < 1 << 20, "peak {peak}");
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
