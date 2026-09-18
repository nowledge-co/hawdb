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
