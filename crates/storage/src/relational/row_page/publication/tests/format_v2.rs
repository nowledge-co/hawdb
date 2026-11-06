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

//! Row-page format v2 (#1521): exact-length pages packed back to back and
//! addressed by byte offset, length and the page's own digest.

use super::*;
use crate::relational::RelationalRowPageMutationPlanner;
use hawdb_integrity::IntegrityHasher;

/// Rewrites descriptor `ordinal` of generation 1 with `edit` and re-signs its
/// binding, so the page-level checks rather than the binding reject it.
fn tamper_descriptor(
    directory: &std::path::Path,
    ordinal: u64,
    edit: impl FnOnce(&mut [u8; root::ROOT_DESCRIPTOR_BYTES]),
) {
    let path = directory.join(relational_row_page_root_descriptor_file(1));
    let keys = fs::read(directory.join(relational_row_page_root_key_file(1))).unwrap();
    let mut encoded = fs::read(&path).unwrap();
    let start = ordinal as usize * root::ROOT_DESCRIPTOR_BYTES;
    let entry: &mut [u8; root::ROOT_DESCRIPTOR_BYTES] = (&mut encoded
        [start..start + root::ROOT_DESCRIPTOR_BYTES])
        .try_into()
        .unwrap();
    edit(entry);
    let field = |range: std::ops::Range<usize>| -> u64 {
        let mut bytes = [0u8; 8];
        bytes[..range.len()].copy_from_slice(&entry[range]);
        u64::from_le_bytes(bytes)
    };
    let (lower_offset, lower_len) = (field(40..48) as usize, field(48..52) as usize);
    let (upper_offset, upper_len) = (field(52..60) as usize, field(60..64) as usize);
    let mut hasher = IntegrityHasher::new();
    hasher.update(&1u64.to_le_bytes());
    hasher.update(&ordinal.to_le_bytes());
    hasher.update(&entry[..100]);
    hasher.update(&keys[lower_offset..lower_offset + lower_len]);
    hasher.update(&keys[upper_offset..upper_offset + upper_len]);
    let digest = hasher.finish();
    entry[100..104].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    entry[104..136].copy_from_slice(digest.sha256.as_bytes());
    fs::write(path, encoded).unwrap();
}

fn publish_two_pages(label: &str) -> (PathBuf, RelationalRowPagePublicationConfig) {
    let directory = unique_test_dir(label);
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 9)],
            )],
        )
        .unwrap();
    (directory, config)
}

fn read_error(
    directory: &std::path::Path,
    config: RelationalRowPagePublicationConfig,
    ordinal: u64,
) -> String {
    let reader = RelationalRowPageRootReader::open_latest(directory, config)
        .unwrap()
        .unwrap();
    match reader
        .read_table_page_descriptor("documents", ordinal)
        .and_then(|descriptor| reader.read_page(&descriptor))
    {
        Err(RelationalRowPagePublicationError::Corrupt(message)) => message,
        other => panic!("tampered page was not rejected as corrupt: {other:?}"),
    }
}

#[test]
fn pages_are_packed_by_exact_length_and_read_back_by_offset() {
    let (directory, config) = publish_two_pages("v2-packed");
    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptors = collect_descriptors(&reader, "documents");
    let artifact = fs::read(directory.join(relational_row_page_artifact_file(1))).unwrap();
    let mut offset = 0u64;
    for (descriptor, expected) in descriptors
        .iter()
        .zip([page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 9)])
    {
        let encoded = expected.encode(config.page_limits).unwrap();
        assert_eq!(descriptor.physical_offset, offset);
        assert_eq!(
            descriptor.page_integrity.encoded_len as usize,
            encoded.len()
        );
        let stored = &artifact
            [offset as usize..offset as usize + descriptor.page_integrity.encoded_len as usize];
        assert_eq!(stored, encoded.as_slice());
        assert_eq!(
            (
                descriptor.page_integrity.page_crc32c,
                descriptor.page_integrity.page_sha256
            ),
            crate::relational::row_page::encoded_page_digest(&encoded)
        );
        assert_eq!(reader.read_page(descriptor).unwrap(), expected);
        offset += encoded.len() as u64;
    }
    // No padding: the artifact is exactly the packed pages.
    assert_eq!(artifact.len() as u64, offset);
    assert_eq!(reader.manifest().page_artifact.encoded_len, offset);
    assert_eq!(
        reader.manifest().physical_generations[0].allocated_bytes,
        offset
    );
    assert_eq!(
        reader.manifest().target_page_bytes,
        crate::relational::DEFAULT_RELATIONAL_ROW_PAGE_TARGET_BYTES as u64
    );
    assert_eq!(
        reader.manifest().max_page_bytes,
        config.page_limits.max_page_bytes.get() as u64
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tampered_offset_length_and_digest_are_rejected_at_read() {
    // An offset into the middle of a page.
    let (directory, config) = publish_two_pages("v2-offset");
    tamper_descriptor(&directory, 0, |entry| {
        entry[16..24].copy_from_slice(&1u64.to_le_bytes());
    });
    read_error(&directory, config, 0);
    fs::remove_dir_all(directory).unwrap();

    // An offset that selects a different, valid page.
    let (directory, config) = publish_two_pages("v2-swapped-offset");
    let second_offset = {
        let reader = RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap();
        reader
            .read_table_page_descriptor("documents", 1)
            .unwrap()
            .physical_offset
    };
    tamper_descriptor(&directory, 0, |entry| {
        entry[16..24].copy_from_slice(&second_offset.to_le_bytes());
    });
    read_error(&directory, config, 0);
    fs::remove_dir_all(directory).unwrap();

    // A length one byte short of the page.
    let (directory, config) = publish_two_pages("v2-short-length");
    tamper_descriptor(&directory, 0, |entry| {
        let len = u32::from_le_bytes(entry[36..40].try_into().unwrap());
        entry[36..40].copy_from_slice(&(len - 1).to_le_bytes());
    });
    read_error(&directory, config, 0);
    fs::remove_dir_all(directory).unwrap();

    // A range past the generation's allocation fails before any page I/O.
    let (directory, config) = publish_two_pages("v2-range");
    tamper_descriptor(&directory, 1, |entry| {
        let len = u32::from_le_bytes(entry[36..40].try_into().unwrap());
        entry[36..40].copy_from_slice(&(len + 1).to_le_bytes());
    });
    let message = read_error(&directory, config, 1);
    assert!(message.contains("outside generation"), "{message}");
    fs::remove_dir_all(directory).unwrap();

    // A descriptor digest that does not match the page's own.
    let (directory, config) = publish_two_pages("v2-digest");
    tamper_descriptor(&directory, 0, |entry| entry[68] ^= 1);
    let message = read_error(&directory, config, 0);
    assert!(message.contains("digest does not match"), "{message}");
    fs::remove_dir_all(directory).unwrap();

    // A page byte flipped on disk fails the page's own checksum.
    let (directory, config) = publish_two_pages("v2-page-byte");
    let artifact = directory.join(relational_row_page_artifact_file(1));
    let mut bytes = fs::read(&artifact).unwrap();
    bytes[ROW_PAGE_TEST_PAYLOAD_OFFSET] ^= 1;
    fs::write(&artifact, bytes).unwrap();
    let message = read_error(&directory, config, 0);
    assert!(message.contains("checksum mismatch"), "{message}");
    fs::remove_dir_all(directory).unwrap();
}

/// A payload byte of the first page, past its 140-byte header.
const ROW_PAGE_TEST_PAYLOAD_OFFSET: usize = 145;

#[test]
fn packer_closes_pages_at_the_target_and_keeps_a_wide_row_whole() {
    let directory = unique_test_dir("v2-target");
    let config = RelationalRowPagePublicationConfig {
        target_page_bytes: std::num::NonZeroUsize::new(1024).unwrap(),
        ..RelationalRowPagePublicationConfig::default()
    };
    let rows = (0..200)
        .map(|id| {
            let body = if id == 100 {
                // One row larger than the target makes a page of its own.
                "w".repeat(3000)
            } else {
                format!("row-{id}")
            };
            crate::relational::RelationalRowChange {
                table: "documents".to_string(),
                primary_key: RelationalKey(vec![RelationalValue::BigInt(id)]),
                row: Some(RelationalRow::new(vec![
                    RelationalValue::BigInt(id),
                    RelationalValue::Text(body),
                ])),
            }
        })
        .collect::<Vec<_>>();
    let plan = RelationalRowPageMutationPlanner::new(None, 1, 10, config)
        .unwrap()
        .plan_table("documents", schema_digest(), 2, rows)
        .unwrap();
    let pages = &plan.delta.dirty_pages;
    assert!(pages.len() > 2);
    for page in pages.iter().filter(|page| page.rows.len() > 1) {
        let encoded = page.encode(config.page_limits).unwrap();
        assert!(encoded.len() <= 1024, "{} bytes", encoded.len());
    }
    let wide = pages
        .iter()
        .find(|page| {
            page.rows
                .iter()
                .any(|entry| entry.primary_key == RelationalKey(vec![RelationalValue::BigInt(100)]))
        })
        .unwrap();
    assert_eq!(wide.rows.len(), 1);
    assert!(wide.encode(config.page_limits).unwrap().len() > 3000);
    assert_eq!(pages.iter().map(|page| page.rows.len()).sum::<usize>(), 200);
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn replacement_rewinding_the_hidden_row_id_allocator_is_rejected() {
    let directory = unique_test_dir("replace-rewind");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    let mut base = table_delta("documents", vec![page(1, 1, 10, 1, 2)]);
    let mut keyless = crate::relational::row_page::test_row_page_schema("documents", 2);
    keyless.primary_key.clear();
    base.schema = Some(keyless.clone());
    base.schema_digest =
        crate::relational::index_shadow::relational_schema_digest(&keyless).unwrap();
    base.last_row_id = 5;
    let mut base_pages = vec![page(1, 1, 10, 1, 2)];
    for page in &mut base_pages {
        page.schema_digest = base.schema_digest;
    }
    base.dirty_pages = base_pages;
    publisher
        .publish(&directory, 1, 10, None, vec![base.clone()])
        .unwrap();

    let mut rewound = base.clone();
    rewound.replaces_base = true;
    rewound.last_row_id = 4;
    rewound.next_page_id = NonZeroU64::new(3).unwrap();
    let mut replacement = page(2, 2, 11, 1, 2);
    replacement.schema_digest = base.schema_digest;
    rewound.dirty_pages = vec![replacement];
    let error = publisher
        .publish(&directory, 2, 11, Some(1), vec![rewound])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("rewinds its hidden row ID allocator"),
        "{error}"
    );
    assert!(!directory
        .join(relational_row_page_manifest_generation_file(2))
        .exists());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn torn_packed_page_artifact_fails_closed_without_breaking_a_pinned_root() {
    let directory = unique_test_dir("v2-torn");
    let config = RelationalRowPagePublicationConfig::default();
    let publisher = RelationalRowPagePublisher::new(config);
    publisher
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta(
                "documents",
                vec![page(1, 1, 10, 1, 2), page(2, 1, 10, 3, 4)],
            )],
        )
        .unwrap();
    let pinned = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    publisher
        .publish(
            &directory,
            2,
            11,
            Some(1),
            vec![{
                let mut delta = table_delta("documents", vec![page(1, 2, 11, 1, 2)]);
                delta.next_page_id = NonZeroU64::new(3).unwrap();
                delta
            }],
        )
        .unwrap();
    // Lose the tail of the newest packed page file, as a medium error or an
    // unsynchronized write after a crash would. The selected root refuses it
    // at open by its exact recorded length.
    let artifact = directory.join(relational_row_page_artifact_file(2));
    let length = fs::metadata(&artifact).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&artifact)
        .unwrap()
        .set_len(length - 7)
        .unwrap();
    assert!(matches!(
        RelationalRowPageRootReader::open_latest(&directory, config),
        Err(RelationalRowPagePublicationError::Corrupt(_))
    ));
    // The pinned generation's pages live in their own immutable file.
    for descriptor in collect_descriptors(&pinned, "documents") {
        pinned.read_page(&descriptor).unwrap();
    }
    fs::remove_dir_all(directory).unwrap();
}

/// A page whose bytes, own digest and descriptor are all consistent but whose
/// row directory is out of order must be rejected by every read path,
/// including the cooperative checkpoint path (`read_checkpoint_page_slot`),
/// which verifies the digest itself and skips only the codec's hash (#1602
/// review).
#[test]
fn consistently_signed_unordered_directory_is_rejected_on_every_read_path() {
    use crate::cache::{SegmentCache, StoreId};

    let directory = unique_test_dir("v2-unordered-directory");
    let config = RelationalRowPagePublicationConfig::default();
    RelationalRowPagePublisher::new(config)
        .publish(
            &directory,
            1,
            10,
            None,
            vec![table_delta("documents", vec![page(1, 1, 10, 1, 4)])],
        )
        .unwrap();
    // Swap the 16-byte row slots of rows 2 and 3. Both key bounds still
    // match, but the keys are no longer strictly ascending.
    let artifact = directory.join(relational_row_page_artifact_file(1));
    let mut bytes = fs::read(&artifact).unwrap();
    let header = crate::relational::row_page::decode_header(&bytes, config.page_limits).unwrap();
    let directory_start = crate::relational::row_page::ROW_PAGE_HEADER_BYTES
        + header.lower_bound_len
        + header.upper_bound_len;
    let (first, second) = (directory_start + 16, directory_start + 32);
    let slot = bytes[first..first + 16].to_vec();
    bytes.copy_within(second..second + 16, first);
    bytes[second..second + 16].copy_from_slice(&slot);
    crate::relational::row_page::write_integrity(&mut bytes);
    fs::write(&artifact, &bytes).unwrap();
    let (crc32c, sha256) = crate::relational::row_page::encoded_page_digest(&bytes);
    tamper_descriptor(&directory, 0, |entry| {
        entry[64..68].copy_from_slice(&crc32c.to_le_bytes());
        entry[68..100].copy_from_slice(sha256.as_bytes());
    });
    // The artifact digest in the manifest is not consulted by page reads, so
    // only the page-level checks stand between the bytes and the caller.

    let reader = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let descriptor = reader.read_table_page_descriptor("documents", 0).unwrap();
    // Ordinary reads (mutation planning and relocation use `read_page`).
    assert!(matches!(
        reader.read_page(&descriptor),
        Err(RelationalRowPagePublicationError::Corrupt(_))
    ));
    // The cooperative checkpoint read.
    let work = crate::background::CheckpointWorkContext::default();
    let result = reader.read_page_slot_accounted_with_work_context(
        &descriptor,
        &SegmentCache::new(1024 * 1024),
        StoreId(1),
        Some(&work),
    );
    assert!(
        matches!(result, Err(RelationalRowPagePublicationError::Corrupt(_))),
        "{result:?}"
    );
    // The mutation planner reading the page as an incremental base.
    let change = crate::relational::RelationalRowChange {
        table: "documents".to_string(),
        primary_key: RelationalKey(vec![RelationalValue::BigInt(2)]),
        row: None,
    };
    assert!(
        RelationalRowPageMutationPlanner::new(Some(&reader), 2, 11, config)
            .unwrap()
            .plan_table("documents", schema_digest(), 2, vec![change])
            .is_err()
    );
    fs::remove_dir_all(directory).unwrap();
}
