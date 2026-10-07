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
use crate::cache::{SegmentCache, StoreId};
use crate::index_page::ImmutableIndexPageLimits;
use crate::relational::{
    relational_index_shadow_artifact_file, RelationalColumnSchema, RelationalIndexSchema,
    RelationalIndexShadowConfig, RelationalIndexShadowWriter, RelationalInsertMode, RelationalKey,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalRow, RelationalScalarType,
    RelationalState, RelationalTableSchema, RelationalTransaction, RelationalValue,
    RelationalWrite,
};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

fn key(value: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(value)])
}

fn fixture() -> (
    PathBuf,
    RelationalState,
    RelationalIndexShadowReader,
    RelationalIndexShadowConfig,
) {
    let state = RelationalState::default()
        .stage_transaction(
            RelationalTransaction {
                writes: vec![
                    RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "counts".into(),
                        columns: ["id", "bucket"]
                            .into_iter()
                            .map(|name| RelationalColumnSchema {
                                name: name.into(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            })
                            .collect(),
                        primary_key: vec!["id".into()],
                        unique_constraints: Vec::new(),
                        foreign_keys: Vec::new(),
                        indexes: vec![
                            RelationalIndexSchema {
                                name: "bucket_idx".into(),
                                columns: vec!["bucket".into()],
                                unique: false,
                            },
                            RelationalIndexSchema {
                                name: "bucket_id_idx".into(),
                                columns: vec!["bucket".into(), "id".into()],
                                unique: false,
                            },
                        ],
                    }),
                    RelationalWrite::Insert {
                        table: "counts".into(),
                        rows: (0..48)
                            .map(|id| {
                                let bucket = if id == 0 {
                                    0
                                } else if id <= 10 {
                                    1
                                } else {
                                    2
                                };
                                RelationalRow::new(vec![
                                    RelationalValue::BigInt(id),
                                    RelationalValue::BigInt(bucket),
                                ])
                            })
                            .collect(),
                        mode: RelationalInsertMode::Error,
                    },
                ],
            },
            RelationalMutationLimits::default(),
            RelationalOverflowConfig::default(),
        )
        .unwrap();
    let directory = std::env::temp_dir().join(format!(
        "hawdb-posting-count-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let config = RelationalIndexShadowConfig {
        page_limits: ImmutableIndexPageLimits {
            max_page_bytes: NonZeroUsize::new(1024).unwrap(),
            max_entries: NonZeroUsize::new(2).unwrap(),
            max_inline_postings: NonZeroUsize::new(2).unwrap(),
            ..ImmutableIndexPageLimits::default()
        },
        ..RelationalIndexShadowConfig::default()
    };
    RelationalIndexShadowWriter::new(config)
        .publish(&directory, &state, 1, 10, None)
        .unwrap();
    let reader = RelationalIndexShadowReader::open(&directory, 1, 10, config).unwrap();
    (directory, state, reader, config)
}

#[test]
fn exact_posting_counts_match_skewed_values_without_visiting_rows_or_chains() {
    let (directory, state, reader, _) = fixture();
    let descriptor = reader.manifest().root("counts", "bucket_idx").unwrap();
    assert!(descriptor.height > 1);
    for (bucket, expected) in [(0, 1), (1, 10), (2, 37), (99, 0)] {
        let lookup = key(bucket);
        let (rows, report) = reader
            .count_exact_postings(
                "counts",
                "bucket_idx",
                &lookup,
                RelationalIndexReadLimits {
                    max_rows: NonZeroUsize::new(1).unwrap(),
                    ..RelationalIndexReadLimits::default()
                },
            )
            .unwrap();
        assert_eq!(rows, expected);
        let resident_count = state
            .index_lookup("counts", "bucket_idx", &lookup)
            .map_or(0, |postings| postings.len());
        assert_eq!(rows, u64::try_from(resident_count).unwrap());
        assert_eq!(report.rows_visited, 0);
        assert_eq!(report.matched_index_keys, usize::from(expected != 0));
        assert!(report.pages_read <= usize::try_from(descriptor.height).unwrap() + 1);
        if expected != 0 {
            assert_eq!(
                report.pages_read,
                usize::try_from(descriptor.height).unwrap() + 1
            );
        }
        assert_eq!(report.bytes_read, report.pages_read * 1024);
        assert!(!report.stopped_early);
    }
    let composite = RelationalKey(vec![
        RelationalValue::BigInt(2),
        RelationalValue::BigInt(20),
    ]);
    let (rows, report) = reader
        .count_exact_postings("counts", "bucket_id_idx", &composite, Default::default())
        .unwrap();
    assert_eq!(rows, 1);
    assert_eq!(report.rows_visited, 0);
    assert!(matches!(
        reader.count_exact_postings("counts", "bucket_id_idx", &key(2), Default::default()),
        Err(RelationalIndexShadowError::Admission(message)) if message.contains("2 complete key components")
    ));
    drop(reader);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn exact_posting_counts_keep_page_byte_height_file_budgets_and_known_poison() {
    let (directory, _, reader, config) = fixture();
    for limits in [
        RelationalIndexReadLimits {
            max_pages: NonZeroUsize::new(1).unwrap(),
            ..RelationalIndexReadLimits::default()
        },
        RelationalIndexReadLimits {
            max_bytes: NonZeroUsize::new(1023).unwrap(),
            ..RelationalIndexReadLimits::default()
        },
        RelationalIndexReadLimits {
            max_tree_height: NonZeroU32::new(1).unwrap(),
            ..RelationalIndexReadLimits::default()
        },
    ] {
        assert!(matches!(
            reader.count_exact_postings("counts", "bucket_idx", &key(2), limits),
            Err(RelationalIndexShadowError::Admission(_))
        ));
        assert!(!reader.is_poisoned());
    }
    let cold = RelationalIndexShadowReader::open_latest_with_cache(
        &directory,
        config,
        Arc::new(SegmentCache::new(64 * 1024)),
        StoreId(216),
    )
    .unwrap();
    let cache_only = RelationalIndexReadLimits {
        max_file_bytes: 0,
        ..RelationalIndexReadLimits::default()
    };
    assert!(matches!(
        cold.count_exact_postings("counts", "bucket_idx", &key(2), cache_only),
        Err(RelationalIndexShadowError::Admission(_))
    ));
    assert!(!cold.is_poisoned());
    let (rows, first) = cold
        .count_exact_postings("counts", "bucket_idx", &key(2), Default::default())
        .unwrap();
    assert_eq!(rows, 37);
    assert!(first.file_bytes_read > 0);
    let (rows, warm) = cold
        .count_exact_postings("counts", "bucket_idx", &key(2), cache_only)
        .unwrap();
    assert_eq!(rows, 37);
    assert_eq!(warm.pages_read, first.pages_read);
    assert_eq!(warm.cache_hits, warm.pages_read);
    assert_eq!(warm.file_bytes_read, 0);
    assert_eq!(warm.rows_visited, 0);

    let descriptor = cold.manifest().root("counts", "bucket_idx").unwrap();
    let mut context = ReadContext::new(&cold, Default::default());
    let encoded = cold.encode_lookup_key(&key(2)).unwrap();
    let Some(IndexLeafPosting::Page { first, .. }) =
        context.find_exact_posting(descriptor, &encoded).unwrap()
    else {
        panic!("broad value must use an unread posting chain");
    };
    let offset = (first.get() - 1) * cold.manifest().page_bytes;
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join(relational_index_shadow_artifact_file(1)))
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&byte).unwrap();
    drop(file);
    // Metadata still describes 37 rows; it did not certify the unread chain.
    assert_eq!(
        cold.count_exact_postings("counts", "bucket_idx", &key(2), cache_only)
            .unwrap()
            .0,
        37
    );
    assert!(matches!(
        cold.visit_exact_postings("counts", "bucket_idx", &key(2), Default::default(), |_| {
            true
        }),
        Err(RelationalIndexShadowError::Corrupt(_))
    ));
    assert!(cold.is_poisoned());
    assert!(matches!(
        cold.count_exact_postings("counts", "bucket_idx", &key(2), Default::default()),
        Err(RelationalIndexShadowError::Corrupt(message)) if message.contains("poisoned")
    ));
    drop(cold);
    drop(reader);
    std::fs::remove_dir_all(directory).unwrap();
}
