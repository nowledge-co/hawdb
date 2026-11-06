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
use crate::relational::{RelationalRow, RelationalRowPagePublisher, RelationalValue};
use std::fs;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[test]
fn mutation_planner_reads_only_affected_leaves_and_splits_deterministically() {
    let directory = unique_test_dir("split");
    let config = two_row_config();
    publish_base(&directory, config);
    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let planner = RelationalRowPageMutationPlanner::new(Some(&base), 2, 2, config).unwrap();
    let changes = vec![change(2, Some("two"))];
    let first = planner
        .plan_table("documents", schema_digest(), 2, changes.clone())
        .unwrap();
    let second = planner
        .plan_table("documents", schema_digest(), 2, changes)
        .unwrap();

    assert_eq!(first, second);
    assert_eq!(first.distinct_changes, 1);
    assert_eq!(first.base_pages_read, 1);
    assert_eq!(first.dirty_pages, 2);
    assert_eq!(first.split_pages, 1);
    assert_eq!(first.deleted_pages, 0);
    assert_eq!(first.delta.next_page_id.get(), 4);
    assert_eq!(page_ids(&first.delta.dirty_pages), vec![1, 3]);
    assert_eq!(row_keys(&first.delta.dirty_pages[0]), vec![1, 2]);
    assert_eq!(row_keys(&first.delta.dirty_pages[1]), vec![3]);

    RelationalRowPagePublisher::new(config)
        .publish(&directory, 2, 2, Some(1), vec![first.delta])
        .unwrap();
    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        current.table_root("documents").unwrap().next_page_id.get(),
        4
    );
    let key_two = key(2);
    let descriptor = current
        .find_table_page_descriptor("documents", &key_two)
        .unwrap()
        .unwrap();
    assert_eq!(descriptor.logical_page_id.get(), 1);
    let page = current.read_page(&descriptor).unwrap();
    assert_eq!(row_keys(&page), vec![1, 2]);
    assert_eq!(
        page.rows[1].row,
        RelationalRow::new(vec![
            RelationalValue::BigInt(2),
            RelationalValue::Text("two".to_string()),
        ])
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn mutation_planner_updates_deletes_and_never_reuses_page_ids() {
    let directory = unique_test_dir("delete");
    let config = two_row_config();
    publish_base(&directory, config);
    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let first = RelationalRowPageMutationPlanner::new(Some(&base), 2, 2, config)
        .unwrap()
        .plan_table(
            "documents",
            schema_digest(),
            2,
            vec![change(2, Some("two"))],
        )
        .unwrap();
    RelationalRowPagePublisher::new(config)
        .publish(&directory, 2, 2, Some(1), vec![first.delta])
        .unwrap();

    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let plan = RelationalRowPageMutationPlanner::new(Some(&base), 3, 3, config)
        .unwrap()
        .plan_table(
            "documents",
            schema_digest(),
            2,
            vec![change(2, Some("two-updated")), change(3, None)],
        )
        .unwrap();
    assert_eq!(plan.base_pages_read, 2);
    assert_eq!(plan.dirty_pages, 1);
    assert_eq!(plan.deleted_pages, 1);
    assert_eq!(plan.delta.deleted_page_ids[0].get(), 3);
    assert_eq!(plan.delta.dirty_pages[0].page_id.get(), 1);
    assert_eq!(plan.delta.next_page_id.get(), 4);

    RelationalRowPagePublisher::new(config)
        .publish(&directory, 3, 3, Some(2), vec![plan.delta])
        .unwrap();
    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        current.table_root("documents").unwrap().next_page_id.get(),
        4
    );
    let descriptors = collect_descriptors(&current, "documents");
    assert_eq!(
        descriptors
            .iter()
            .map(|descriptor| descriptor.logical_page_id.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let plan = RelationalRowPageMutationPlanner::new(Some(&current), 4, 4, config)
        .unwrap()
        .plan_table(
            "documents",
            schema_digest(),
            2,
            vec![change(12, Some("twelve"))],
        )
        .unwrap();
    assert_eq!(plan.base_pages_read, 1);
    assert_eq!(plan.split_pages, 1);
    assert_eq!(page_ids(&plan.delta.dirty_pages), vec![2, 4]);
    assert_eq!(plan.delta.next_page_id.get(), 5);
    RelationalRowPagePublisher::new(config)
        .publish(&directory, 4, 4, Some(3), vec![plan.delta])
        .unwrap();
    let current = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    assert_eq!(
        collect_descriptors(&current, "documents")
            .iter()
            .map(|descriptor| descriptor.logical_page_id.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn streaming_bootstrap_keeps_one_page_plus_one_candidate_row() {
    let config = two_row_config();
    let mut bootstrap = RelationalRowPageBootstrap::new(
        1,
        1,
        schema_digest(),
        2,
        NonZeroU64::new(1).unwrap(),
        config,
    )
    .unwrap();
    let mut pages = Vec::new();
    for value in 1..=5 {
        bootstrap
            .push(entry(value, &format!("row-{value}")), |page| {
                pages.push(page);
                Ok(())
            })
            .unwrap();
    }
    let report = bootstrap
        .finish(|page| {
            pages.push(page);
            Ok(())
        })
        .unwrap();

    assert_eq!(report.rows, 5);
    assert_eq!(report.pages, 3);
    assert_eq!(report.peak_buffered_rows, 3);
    assert_eq!(report.next_page_id.get(), 4);
    assert_eq!(page_ids(&pages), vec![1, 2, 3]);
    assert_eq!(
        pages.iter().map(|page| page.rows.len()).collect::<Vec<_>>(),
        vec![2, 2, 1]
    );
}

#[test]
fn allocator_exhaustion_is_atomic() {
    let mut allocator = RelationalRowPageIdAllocator::new(NonZeroU64::new(u64::MAX).unwrap());
    let error = allocator.allocate().unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageMutationError::Admission(message)
            if message.contains("allocator is exhausted")
    ));
    assert_eq!(allocator.next_page_id().get(), u64::MAX);
}

#[test]
fn streaming_bootstrap_fails_closed_after_emit_error() {
    let mut config = two_row_config();
    config.page_limits.max_rows = NonZeroUsize::new(1).unwrap();
    let mut bootstrap = RelationalRowPageBootstrap::new(
        1,
        1,
        schema_digest(),
        2,
        NonZeroU64::new(1).unwrap(),
        config,
    )
    .unwrap();
    bootstrap.push(entry(1, "one"), |_| Ok(())).unwrap();
    let error = bootstrap
        .push(entry(2, "two"), |_| {
            Err(RelationalRowPageMutationError::Admission(
                "bootstrap sink failed".to_string(),
            ))
        })
        .unwrap_err();
    assert!(error.to_string().contains("bootstrap sink failed"));

    let error = bootstrap.push(entry(3, "three"), |_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("cannot continue"));
    let error = bootstrap.finish(|_| Ok(())).unwrap_err();
    assert!(error.to_string().contains("cannot finish"));
}

#[test]
fn mutation_planner_rejects_the_global_dirty_page_budget() {
    let directory = unique_test_dir("dirty-budget");
    let base_config = two_row_config();
    publish_base(&directory, base_config);
    RelationalRowPagePublisher::new(base_config)
        .publish(&directory, 2, 2, Some(1), Vec::new())
        .unwrap();
    let mut config = base_config;
    config.max_dirty_pages = NonZeroUsize::new(1).unwrap();
    let base = RelationalRowPageRootReader::open_latest(&directory, config)
        .unwrap()
        .unwrap();
    let error = RelationalRowPageMutationPlanner::new(Some(&base), 3, 3, config)
        .unwrap()
        .plan_table(
            "documents",
            schema_digest(),
            2,
            vec![
                change(1, Some("one-updated")),
                change(10, Some("ten-updated")),
            ],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageMutationError::Admission(message)
            if message.contains("emits 2 dirty pages")
    ));

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn change_admission_replaces_duplicate_key_charges_before_rejecting_growth() {
    let config = two_row_config();
    let limits = RelationalRowChangeCaptureLimits {
        max_entries: NonZeroUsize::new(2).unwrap(),
        max_bytes: NonZeroUsize::new(2 * 1024).unwrap(),
    };
    let first = "a".repeat(1_500);
    let replacement = "b".repeat(1_500);
    let changes = collect_changes_with_limits(
        "documents",
        vec![change(1, Some(&first)), change(1, Some(&replacement))],
        config,
        limits,
    )
    .unwrap();
    assert_eq!(changes.len(), 1);

    let error = collect_changes_with_limits(
        "documents",
        vec![change(1, Some(&first)), change(2, Some(&replacement))],
        config,
        limits,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        RelationalRowPageMutationError::Admission(message)
            if message.contains("exceeding limit 2048")
    ));
}

fn publish_base(directory: &Path, config: RelationalRowPagePublicationConfig) {
    RelationalRowPagePublisher::new(config)
        .publish(
            directory,
            1,
            1,
            None,
            vec![RelationalRowPageTableDelta {
                replaces_base: false,
                last_row_id: 0,
                table: "documents".to_string(),
                schema: Some(crate::relational::row_page::test_row_page_schema(
                    "documents",
                    2,
                )),
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                next_page_id: NonZeroU64::new(3).unwrap(),
                dirty_pages: vec![page(1, 1, 1, &[1, 3]), page(2, 1, 1, &[10, 11])],
                deleted_page_ids: Vec::new(),
            }],
        )
        .unwrap();
}

fn two_row_config() -> RelationalRowPagePublicationConfig {
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_rows = NonZeroUsize::new(2).unwrap();
    config
}

fn page(page_id: u64, generation: u64, epoch: u64, keys: &[i64]) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation,
        source_commit_epoch: epoch,
        page_id: RelationalRowPageId::new(NonZeroU64::new(page_id).unwrap()),
        schema_digest: schema_digest(),
        column_count: 2,
        rows: keys
            .iter()
            .map(|value| entry(*value, &format!("row-{value}")))
            .collect(),
    }
}

fn entry(value: i64, text: &str) -> RelationalRowPageEntry {
    RelationalRowPageEntry {
        primary_key: key(value),
        row: RelationalRow::new(vec![
            RelationalValue::BigInt(value),
            RelationalValue::Text(text.to_string()),
        ]),
    }
}

fn change(value: i64, text: Option<&str>) -> RelationalRowChange {
    RelationalRowChange {
        table: "documents".to_string(),
        primary_key: key(value),
        row: text.map(|text| entry(value, text).row),
    }
}

fn key(value: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(value)])
}

fn schema_digest() -> Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 2)
}

fn row_keys(page: &ImmutableRelationalRowPage) -> Vec<i64> {
    page.rows
        .iter()
        .map(|entry| match entry.primary_key.0[0] {
            RelationalValue::BigInt(value) => value,
            _ => panic!("test primary key is not an integer"),
        })
        .collect()
}

fn page_ids(pages: &[ImmutableRelationalRowPage]) -> Vec<u64> {
    pages.iter().map(|page| page.page_id.get()).collect()
}

fn collect_descriptors(
    reader: &RelationalRowPageRootReader,
    table: &str,
) -> Vec<RelationalRowPageRootDescriptor> {
    let mut descriptors = Vec::new();
    reader
        .visit_table_pages(table, |descriptor| {
            descriptors.push(descriptor.clone());
            Ok(())
        })
        .unwrap();
    descriptors
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-row-page-mutation-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

/// The packer before linear sizing: it encoded the whole candidate page after
/// every row and split at the first row the codec rejected.
fn reference_pack(
    column_count: usize,
    digest: Sha256Digest,
    config: RelationalRowPagePublicationConfig,
    rows: &[RelationalRowPageEntry],
) -> (
    Vec<ImmutableRelationalRowPage>,
    Option<RelationalRowPageMutationError>,
) {
    let limits = config.page_limits;
    let new_page = |id: u64| ImmutableRelationalRowPage {
        generation: 1,
        source_commit_epoch: 1,
        page_id: RelationalRowPageId::new(NonZeroU64::new(id).unwrap()),
        schema_digest: digest,
        column_count,
        rows: Vec::new(),
    };
    let mut pages = Vec::new();
    let mut next_id = 1;
    let mut pending: Option<ImmutableRelationalRowPage> = None;
    for row in rows {
        let page = pending.get_or_insert_with(|| new_page(next_id));
        page.rows.push(row.clone());
        if page.encode(limits).is_ok() {
            continue;
        }
        let row = page.rows.pop().unwrap();
        if page.rows.is_empty() {
            let mut single = new_page(next_id);
            single.rows.push(row);
            return (pages, Some(single.encode(limits).unwrap_err().into()));
        }
        page.encode(limits).unwrap();
        pages.push(pending.take().unwrap());
        next_id += 1;
        let mut next = new_page(next_id);
        next.rows.push(row);
        if let Err(error) = next.encode(limits) {
            return (pages, Some(error.into()));
        }
        pending = Some(next);
    }
    pages.extend(pending);
    (pages, None)
}

fn linear_pack(
    column_count: usize,
    digest: Sha256Digest,
    config: RelationalRowPagePublicationConfig,
    rows: &[RelationalRowPageEntry],
) -> (
    Vec<ImmutableRelationalRowPage>,
    Option<RelationalRowPageMutationError>,
) {
    let mut pages = Vec::new();
    let mut bootstrap = RelationalRowPageBootstrap::new(
        1,
        1,
        digest,
        column_count,
        NonZeroU64::new(1).unwrap(),
        config,
    )
    .unwrap();
    for row in rows {
        if let Err(error) = bootstrap.push(row.clone(), |page| {
            pages.push(page);
            Ok(())
        }) {
            return (pages, Some(error));
        }
    }
    let result = bootstrap.finish(|page| {
        pages.push(page);
        Ok(())
    });
    (pages, result.err())
}

fn assert_same_packing(
    label: &str,
    column_count: usize,
    config: RelationalRowPagePublicationConfig,
    rows: &[RelationalRowPageEntry],
) {
    let digest = crate::relational::row_page::test_row_page_schema_digest("packing", column_count);
    let (expected, expected_error) = reference_pack(column_count, digest, config, rows);
    let (actual, actual_error) = linear_pack(column_count, digest, config, rows);
    assert_eq!(
        actual_error.as_ref().map(ToString::to_string),
        expected_error.as_ref().map(ToString::to_string),
        "{label}: packing outcome differs"
    );
    assert_eq!(actual, expected, "{label}: page partition differs");
    for (actual, expected) in actual.iter().zip(&expected) {
        let encoded = actual.encode(config.page_limits).unwrap();
        assert_eq!(
            encoded,
            expected.encode(config.page_limits).unwrap(),
            "{label}: encoded page bytes differ"
        );
        // The dirty budget charges `encoded_len` (#1593).
        assert_eq!(
            actual.encoded_len(config.page_limits).unwrap(),
            encoded.len() as u64,
            "{label}: encoded_len differs from the encoded page"
        );
    }
}

fn varied_row(index: usize, column_count: usize) -> RelationalRow {
    let mut values = Vec::with_capacity(column_count);
    values.push(RelationalValue::BigInt(index as i64));
    for column in 1..column_count {
        values.push(match (index * 7 + column * 3) % 8 {
            0 => RelationalValue::Null,
            1 => RelationalValue::Boolean(index.is_multiple_of(2)),
            2 => RelationalValue::DoublePrecision(index as f64 / 3.0),
            3 => RelationalValue::Bytea(vec![column as u8; (index * 13) % 97]),
            4 => RelationalValue::Overflow(crate::relational::RelationalOverflowRef {
                digest: Sha256Digest::from_bytes([index as u8; 32]),
                scalar_type: crate::relational::RelationalScalarType::Text,
                compressed_bytes: 5_000 + index as u64,
                uncompressed_bytes: 9_000 + index as u64,
            }),
            5 => RelationalValue::Text(String::new()),
            _ => RelationalValue::Text("w".repeat((index * 37 + column) % 300)),
        });
    }
    RelationalRow::new(values)
}

fn integer_rows(count: usize, column_count: usize) -> Vec<RelationalRowPageEntry> {
    (0..count)
        .map(|index| RelationalRowPageEntry {
            primary_key: key(index as i64),
            row: varied_row(index, column_count),
        })
        .collect()
}

/// Strictly increasing text keys whose length shrinks after long keys, so a
/// later row's per-row size projection can be smaller than an earlier one.
fn text_key_rows(count: usize) -> Vec<RelationalRowPageEntry> {
    (0..count)
        .map(|index| {
            let length = if index.is_multiple_of(3) {
                400
            } else {
                1 + index % 5
            };
            let text = format!("{:04}{}", index, "k".repeat(length));
            RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::Text(text.clone())]),
                row: RelationalRow::new(vec![
                    RelationalValue::Text(text),
                    RelationalValue::Text("v".repeat(index % 11)),
                ]),
            }
        })
        .collect()
}

fn limits_config(page_bytes: usize, rows: usize) -> RelationalRowPagePublicationConfig {
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_page_bytes = NonZeroUsize::new(page_bytes).unwrap();
    config.page_limits.max_rows = NonZeroUsize::new(rows).unwrap();
    config
}

#[test]
fn linear_packing_matches_whole_page_encoding_at_default_limits() {
    let config = RelationalRowPagePublicationConfig::default();
    // Narrow rows fill pages to exactly the 256-row cap; one more row splits.
    for count in [1, 255, 256, 257, 513, 1_000] {
        assert_same_packing(
            &format!("narrow {count}"),
            2,
            config,
            &integer_rows(count, 2),
        );
    }
    assert_same_packing("wide", 24, config, &integer_rows(600, 24));
    assert_same_packing("text keys", 2, config, &text_key_rows(700));
}

#[test]
fn linear_packing_matches_whole_page_encoding_at_every_byte_cap() {
    let wide = integer_rows(60, 6);
    let keyed = text_key_rows(60);
    // Every byte limit in the sweep lands some page exactly at its cap and
    // the next row one byte or more over it, including single rows that no
    // page can hold.
    for page_bytes in (ROW_PAGE_TEST_HEADER_BYTES..6_000).step_by(3) {
        for max_rows in [1, 2, 3, 7, 256] {
            let config = limits_config(page_bytes, max_rows);
            assert_same_packing(&format!("wide {page_bytes}/{max_rows}"), 6, config, &wide);
            assert_same_packing(&format!("keyed {page_bytes}/{max_rows}"), 2, config, &keyed);
        }
    }
}

#[test]
fn linear_packing_reports_the_codec_error_for_unpackable_rows() {
    let mut rows = integer_rows(20, 3);
    let config = limits_config(64 * 1024, 256);

    // A row wider than its row limit, after rows that already fill a page.
    let mut row_limited = config;
    row_limited.page_limits.max_row_bytes = NonZeroUsize::new(200).unwrap();
    rows[10].row = RelationalRow::new(vec![
        RelationalValue::BigInt(10),
        RelationalValue::Text("x".repeat(400)),
        RelationalValue::Null,
    ]);
    assert_same_packing("row over row limit", 3, row_limited, &rows);

    // A single row larger than a whole page.
    let page_limited = limits_config(600, 256);
    assert_same_packing("row over page limit", 3, page_limited, &rows);

    // A row with the wrong number of values.
    let mut short = integer_rows(20, 3);
    short[5].row = RelationalRow::new(vec![RelationalValue::BigInt(5)]);
    assert_same_packing("wrong column count", 3, config, &short);

    // A key over the key limit.
    let mut key_limited = config;
    key_limited.page_limits.max_key_bytes = NonZeroUsize::new(64).unwrap();
    assert_same_packing("key over key limit", 2, key_limited, &text_key_rows(10));
}

const ROW_PAGE_TEST_HEADER_BYTES: usize = 140;
