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
use crate::relational::{
    relational_row_page_artifact_file, ImmutableRelationalRowPage, RelationalKey,
    RelationalOverflowConfig, RelationalOverflowExtentInput, RelationalOverflowPublicationConfig,
    RelationalOverflowPublisher, RelationalOverflowRef, RelationalOverflowRootReader,
    RelationalRow, RelationalRowPageEntry, RelationalRowPageId, RelationalRowPagePublicationConfig,
    RelationalRowPagePublisher, RelationalRowPageRootReader, RelationalRowPageTableDelta,
    RelationalScalarType, RelationalValue,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_integrity::{integrity_digest, Sha256Digest};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const PAGE_BYTES: usize = 4096;

#[path = "tests/admission.rs"]
mod admission;

#[test]
fn descriptor_exhaustion_preserves_demand_reader_for_retry() {
    use crate::file_descriptors::ProjectFileDescriptors;
    use hawdb_core::error::{file_descriptor_error, FileDescriptorError, HawDBError};

    let fixture = DemandFixture::new("descriptor-retry");
    let project =
        ProjectFileDescriptors::acquire_with_capacity_limit(&fixture.directory, 16, true).unwrap();
    let path = fixture.directory.join("held-descriptor");
    fs::write(&path, b"held").unwrap();
    let held = (0..16)
        .map(|_| crate::file_io::File::open(&path).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(project.metrics().open, 16);
    let initial = RelationalHydrationBudget::default();
    let mut hydration = initial;
    let error = fixture
        .reader
        .point_projected(
            "documents",
            &key(1),
            &[1],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err();
    let expected = FileDescriptorError::BudgetExceeded {
        requested: 1,
        available: 0,
        limit: 16,
    };
    assert_eq!(file_descriptor_error(&error), Some(expected.clone()));
    assert_eq!(
        HawDBError::from_storage_error(error),
        HawDBError::FileDescriptors(expected)
    );
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(hydration, initial);
    assert_eq!(project.metrics().open, 16);
    assert_eq!(project.metrics().reserved, 0);
    drop(held);
    let (row, _) = fixture
        .reader
        .point_projected(
            "documents",
            &key(1),
            &[1],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    assert_eq!(
        row.unwrap().fields[0].value,
        RelationalValue::Text("alpha overflow payload".into())
    );
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(project.metrics().open, 0);
    fixture.remove();
}

#[test]
fn point_projection_hydrates_only_selected_overflow_and_reuses_cache() {
    let fixture = DemandFixture::new("point-projection");
    let primary_key = key(1);
    let task = RuntimeTaskContext::default();

    let mut inline_budget = RelationalHydrationBudget::default();
    let (inline, cold_report) = fixture
        .reader
        .point_projected(
            "documents",
            &primary_key,
            &[3],
            RelationalRowPageDemandReadLimits::default(),
            &mut inline_budget,
            &task,
        )
        .expect("cold point projection");
    let inline = inline.expect("row one exists");
    assert_eq!(inline.primary_key, primary_key);
    assert_eq!(inline.fields.len(), 1);
    assert_eq!(inline.fields[0].ordinal, 3);
    assert_eq!(
        inline.fields[0].value,
        RelationalValue::Text("inline-1".to_string())
    );
    assert_eq!(cold_report.pages_read, 1);
    assert_eq!(cold_report.cache_misses, 1);
    assert_eq!(cold_report.cache_hits, 0);
    assert_eq!(cold_report.file_pages_read, 1);
    assert_eq!(cold_report.hydrated_values, 0);
    assert_eq!(inline_budget.hydrated_rows, 0);
    assert_eq!(inline_budget.compressed_bytes, 0);
    let cached = fixture.cache.snapshot();
    let encoded_page_bytes = fixture
        .row_root
        .read_table_page_descriptor("documents", 0)
        .expect("read cached page descriptor")
        .slot_integrity
        .encoded_len as u64;
    assert_eq!(cached.pinned_bytes, 0);
    assert_eq!(cached.resident_bytes, encoded_page_bytes);
    assert!(cached.resident_bytes < PAGE_BYTES as u64);

    let mut overflow_budget = RelationalHydrationBudget::default();
    let (projected, warm_report) = fixture
        .reader
        .point_projected(
            "documents",
            &key(1),
            &[1],
            RelationalRowPageDemandReadLimits::default(),
            &mut overflow_budget,
            &task,
        )
        .expect("warm point projection");
    let projected = projected.expect("row one exists");
    assert_eq!(projected.fields.len(), 1);
    assert_eq!(projected.fields[0].ordinal, 1);
    assert_eq!(
        projected.fields[0].value,
        RelationalValue::Text("alpha overflow payload".to_string())
    );
    assert_eq!(warm_report.cache_hits, 1);
    assert_eq!(warm_report.cache_misses, 0);
    assert_eq!(warm_report.file_pages_read, 0);
    assert_eq!(warm_report.hydrated_values, 1);
    assert!(warm_report.compressed_hydration_bytes > 0);
    assert_eq!(overflow_budget.hydrated_rows, 1);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    let mut selective_budget = RelationalHydrationBudget::default();
    let (selective, selective_report) = fixture
        .reader
        .point_projected_fields(
            "documents",
            &key(1),
            RelationalRowPageProjectedFields {
                requested_fields: &[1, 2],
                hydration_fields: &[2],
            },
            RelationalRowPageDemandReadLimits::default(),
            &mut selective_budget,
            &task,
        )
        .expect("selective overflow hydration");
    let selective = selective.expect("row one exists");
    assert!(matches!(
        selective.fields[0].value,
        RelationalValue::Overflow(reference) if reference == fixture.alpha
    ));
    assert_eq!(
        selective.fields[1].value,
        RelationalValue::Bytea(b"beta overflow payload".to_vec())
    );
    assert_eq!(selective_report.hydrated_values, 1);
    assert_eq!(selective_budget.hydrated_rows, 1);

    let initial = RelationalHydrationBudget {
        max_decompressed_bytes: fixture.alpha.uncompressed_bytes as usize,
        ..RelationalHydrationBudget::default()
    };
    let mut atomic_budget = initial;
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(1),
            &[1, 2],
            RelationalRowPageDemandReadLimits::default(),
            &mut atomic_budget,
            &task,
        ),
        Err(RelationalRowPageDemandReadError::Admission(_))
    ));
    assert_eq!(atomic_budget, initial);
    assert!(!fixture.reader.is_poisoned());

    fixture.remove();
}

#[test]
fn multi_point_projection_groups_keys_by_row_page() {
    let fixture = DemandFixture::new("multi-point-projection");
    let task = RuntimeTaskContext::default();
    let mut hydration = RelationalHydrationBudget::default();
    let (rows, report) = fixture
        .reader
        .points_projected_fields(
            "documents",
            &[key(1), key(2), key(2), key(4), key(9)],
            RelationalRowPageProjectedFields {
                requested_fields: &[0, 3],
                hydration_fields: &[],
            },
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &task,
        )
        .expect("multi-point projection");

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[&key(1)].fields[1].value,
        RelationalValue::Text("inline-1".to_string())
    );
    assert_eq!(
        rows[&key(2)].fields[1].value,
        RelationalValue::Text("inline-2".to_string())
    );
    assert_eq!(
        rows[&key(4)].fields[1].value,
        RelationalValue::Text("inline-4".to_string())
    );
    assert!(!rows.contains_key(&key(9)));
    assert_eq!(report.pages_read, 2);
    assert_eq!(report.rows_decoded, 3);
    assert_eq!(report.rows_emitted, 3);
    assert_eq!(report.owned_rows_emitted, 3);
    assert_eq!(report.hydrated_values, 0);
    assert_eq!(hydration.hydrated_rows, 0);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    fixture.remove();
}

#[test]
fn range_cursor_is_ordered_bounded_and_applies_lower_bound_once() {
    let fixture = DemandFixture::new("range");
    let mut hydration = RelationalHydrationBudget::default();
    let mut visited = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Excluded(&key(1)), Bound::Included(&key(3)), &[0, 3]),
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                visited.push(row.primary_key);
                true
            },
        )
        .expect("bounded range read");
    assert_eq!(visited, vec![key(2), key(3)]);
    assert_eq!(report.pages_read, 2);
    assert_eq!(report.rows_emitted, 2);
    assert_eq!(report.borrowed_rows_emitted, 0);
    assert_eq!(report.owned_rows_emitted, 2);
    assert!(!report.stopped_early);

    let mut hydration = RelationalHydrationBudget::default();
    let mut after_page_boundary = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Excluded(&key(2)), Bound::Unbounded, &[0]),
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                after_page_boundary.push(row.primary_key);
                true
            },
        )
        .expect("range after exact page boundary");
    assert_eq!(after_page_boundary, vec![key(3), key(4)]);
    assert_eq!(report.pages_read, 1);

    let mut hydration = RelationalHydrationBudget::default();
    let (missing, report) = fixture
        .reader
        .point_projected(
            "documents",
            &key(9),
            &[0],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .expect("point miss above the final page");
    assert!(missing.is_none());
    assert_eq!(report.pages_read, 0);

    let mut hydration = RelationalHydrationBudget::default();
    let mut early = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded, &[0]),
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                early.push(row.primary_key);
                false
            },
        )
        .expect("early range stop");
    assert_eq!(early, vec![key(1)]);
    assert_eq!(report.pages_read, 1);
    assert_eq!(report.rows_emitted, 1);
    assert!(report.stopped_early);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    fixture.remove();
}

#[test]
fn lending_range_keeps_base_rows_borrowed_and_overlay_rows_owned() {
    let fixture = DemandFixture::new("lending-range");
    let requested = [0, 3];
    let mut overlay = BTreeMap::new();
    overlay.insert(
        key(2),
        RelationalRowPageProjectedOverlayValue::Present {
            fields: vec![
                RelationalProjectedField {
                    ordinal: 0,
                    value: RelationalValue::BigInt(2),
                },
                RelationalProjectedField {
                    ordinal: 3,
                    value: RelationalValue::Text("overlay-2".to_string()),
                },
            ]
            .into_boxed_slice(),
            binds_overlay_overflow: false,
        },
    );
    let mut hydration = RelationalHydrationBudget::default();
    let mut resolve = |_: &mut RelationalProjectedRow,
                       _: &mut RelationalHydrationBudget,
                       _: &RuntimeTaskContext| { Ok(()) };
    let mut visited = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range_with_overlay_ref(
            RelationalRowPageOverlayRead {
                owned_callback: false,
                range: projected_range(Bound::Unbounded, Bound::Unbounded, &requested),
                limits: RelationalRowPageDemandReadLimits::default(),
                overlay: RelationalRowPageOverlayRange {
                    cursor: overlay.into_iter().peekable(),
                    overflow_root: None,
                },
            },
            &mut hydration,
            &RuntimeTaskContext::default(),
            Some(&requested),
            &mut resolve,
            |row, _| {
                let value = row
                    .value(3)
                    .expect("projected inline value")
                    .to_owned_value();
                visited.push((row.is_borrowed(), row.primary_key().clone(), value));
                true
            },
        )
        .expect("lending range read");

    assert_eq!(
        visited,
        vec![
            (true, key(1), RelationalValue::Text("inline-1".to_string())),
            (
                false,
                key(2),
                RelationalValue::Text("overlay-2".to_string())
            ),
            (true, key(3), RelationalValue::Text("inline-3".to_string())),
            (true, key(4), RelationalValue::Text("inline-4".to_string())),
        ]
    );
    assert_eq!(report.borrowed_rows_emitted, 3);
    assert_eq!(report.owned_rows_emitted, 1);
    assert_eq!(report.rows_emitted, 4);
    assert_eq!(report.peak_pins, 1);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    let mut hydration = RelationalHydrationBudget::default();
    let mut resolve = |_: &mut RelationalProjectedRow,
                       _: &mut RelationalHydrationBudget,
                       _: &RuntimeTaskContext| { Ok(()) };
    let report = fixture
        .reader
        .visit_projected_range_with_overlay_ref(
            RelationalRowPageOverlayRead {
                owned_callback: false,
                range: projected_range(Bound::Unbounded, Bound::Unbounded, &requested),
                limits: RelationalRowPageDemandReadLimits::default(),
                overlay: RelationalRowPageOverlayRange {
                    cursor: BTreeMap::new().into_iter().peekable(),
                    overflow_root: None,
                },
            },
            &mut hydration,
            &RuntimeTaskContext::default(),
            Some(&requested),
            &mut resolve,
            |row, _| {
                assert!(row.is_borrowed());
                false
            },
        )
        .expect("early lending range stop");
    assert!(report.stopped_early);
    assert_eq!(report.borrowed_rows_emitted, 1);
    assert_eq!(report.owned_rows_emitted, 0);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    fixture.remove();
}

#[test]
fn admission_rejects_before_unbounded_io_without_poisoning() {
    let fixture = DemandFixture::new("admission");
    let task = RuntimeTaskContext::default();

    let low_height = RelationalRowPageDemandReadLimits {
        max_tree_height: NonZeroU32::new(1).unwrap(),
        ..RelationalRowPageDemandReadLimits::default()
    };
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(1),
            &[0],
            low_height,
            &mut hydration,
            &task,
        ),
        Err(RelationalRowPageDemandReadError::Admission(message))
            if message.contains("descriptor search height")
    ));
    assert_eq!(fixture.cache.snapshot().resident_bytes, 0);

    let low_bytes = RelationalRowPageDemandReadLimits {
        max_bytes: NonZeroUsize::new(PAGE_BYTES - 1).unwrap(),
        ..RelationalRowPageDemandReadLimits::default()
    };
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(1),
            &[0],
            low_bytes,
            &mut hydration,
            &task,
        ),
        Err(RelationalRowPageDemandReadError::Admission(message))
            if message.contains("byte limit")
    ));
    assert_eq!(fixture.cache.snapshot().resident_bytes, 0);

    let one_page = RelationalRowPageDemandReadLimits {
        max_pages: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowPageDemandReadLimits::default()
    };
    let mut provisional = Vec::new();
    assert!(matches!(
        fixture.reader.visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded, &[0]),
            one_page,
            &mut hydration,
            &task,
            |row| {
                provisional.push(row.primary_key);
                true
            },
        ),
        Err(RelationalRowPageDemandReadError::Admission(message))
            if message.contains("page limit")
    ));
    assert_eq!(provisional, vec![key(1), key(2)]);
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);

    fixture.remove();
}

#[test]
fn cancellation_and_callback_panic_release_page_pins() {
    let fixture = DemandFixture::new("pin-release");
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(1),
            &[0],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::without_deadline(token),
        ),
        Err(RelationalRowPageDemandReadError::Stopped(_))
    ));
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);
    assert!(!fixture.reader.is_poisoned());

    let token = RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(token.clone());
    let mut provisional = Vec::new();
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded, &[0]),
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &context,
            |row| {
                provisional.push(row.primary_key);
                token.cancel();
                true
            },
        ),
        Err(RelationalRowPageDemandReadError::Stopped(_))
    ));
    assert_eq!(provisional, vec![key(1)]);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);
    assert!(!fixture.reader.is_poisoned());

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut hydration = RelationalHydrationBudget::default();
        let _ = fixture.reader.visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded, &[0]),
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |_| panic!("stop demand cursor"),
        );
    }));
    assert!(panic.is_err());
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);
    assert!(!fixture.reader.is_poisoned());

    fixture.remove();
}

#[test]
fn corrupted_page_poison_is_sticky_but_admission_is_not() {
    let fixture = DemandFixture::new("corruption");
    flip_byte(
        &fixture.directory.join(relational_row_page_artifact_file(1)),
        0,
    );
    let cold = RelationalRowPageDemandReader::new(
        Arc::clone(&fixture.row_root),
        Arc::clone(&fixture.overflow_root),
        Arc::new(SegmentCache::new((PAGE_BYTES * 2) as u64)),
        StoreId(902),
    )
    .expect("open cold demand reader");
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        cold.point_projected(
            "documents",
            &key(1),
            &[0],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(RelationalRowPageDemandReadError::Corrupt(message))
            if message.contains("checksum mismatch")
    ));
    assert!(cold.is_poisoned());
    assert!(matches!(
        cold.point_projected(
            "documents",
            &key(3),
            &[0],
            RelationalRowPageDemandReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(RelationalRowPageDemandReadError::Corrupt(message))
            if message.contains("poisoned")
    ));

    fixture.remove();
}

#[test]
fn table_root_column_count_must_match_every_demand_loaded_page() {
    let alpha = RelationalOverflowRef {
        digest: integrity_digest(b"unused-alpha").sha256,
        scalar_type: RelationalScalarType::Text,
        compressed_bytes: 1,
        uncompressed_bytes: 1,
    };
    let beta = RelationalOverflowRef {
        digest: integrity_digest(b"unused-beta").sha256,
        scalar_type: RelationalScalarType::Bytea,
        compressed_bytes: 1,
        uncompressed_bytes: 1,
    };
    let encoded = page(1, 1, 1, alpha, beta)
        .encode(row_publication_config().page_limits)
        .expect("encode page with four columns");
    let page = RelationalRowPageView::open(&encoded, row_publication_config().page_limits)
        .expect("open encoded page");
    let table = super::super::RelationalRowPageTableRoot {
        last_row_id: 0,
        table: "documents".to_string(),
        schema: crate::relational::row_page::test_row_page_schema("documents", 3),
        schema_digest: schema_digest(),
        column_count: NonZeroU32::new(3).unwrap(),
        row_count: 1,
        next_page_id: NonZeroU64::new(2).unwrap(),
        first_descriptor: 0,
        page_count: 1,
        lower_bound: vec![1],
        upper_bound: vec![2],
    };
    assert!(matches!(
        validate_table_column_count(&table, &page),
        Err(RelationalRowPageDemandReadError::Corrupt(message))
            if message.contains("binds 3 columns")
    ));
}

struct DemandFixture {
    directory: PathBuf,
    row_root: Arc<RelationalRowPageRootReader>,
    overflow_root: Arc<RelationalOverflowRootReader>,
    cache: Arc<SegmentCache>,
    reader: RelationalRowPageDemandReader,
    alpha: RelationalOverflowRef,
}

impl DemandFixture {
    fn new(label: &str) -> Self {
        let directory = unique_test_dir(label);
        let overflow_config = RelationalOverflowPublicationConfig::default();
        let alpha = RelationalOverflowExtentInput::encode(
            RelationalScalarType::Text,
            b"alpha overflow payload",
            RelationalOverflowConfig::default(),
        )
        .expect("encode alpha overflow input");
        let alpha_reference = *alpha.reference();
        let beta = RelationalOverflowExtentInput::encode(
            RelationalScalarType::Bytea,
            b"beta overflow payload",
            RelationalOverflowConfig::default(),
        )
        .expect("encode beta overflow input");
        let beta_reference = *beta.reference();
        RelationalOverflowPublisher::new(overflow_config)
            .publish(&directory, 1, 10, None, vec![alpha, beta])
            .expect("publish overflow fixture");
        let overflow_root = Arc::new(
            RelationalOverflowRootReader::open_latest(&directory, overflow_config)
                .expect("open overflow root")
                .expect("overflow root exists"),
        );

        let row_config = row_publication_config();
        RelationalRowPagePublisher::new(row_config)
            .publish_with_overflow_root(
                &directory,
                1,
                10,
                None,
                vec![table_delta(vec![
                    page(1, 1, 2, alpha_reference, beta_reference),
                    page(2, 3, 4, alpha_reference, beta_reference),
                ])],
                &overflow_root,
            )
            .expect("publish row-page fixture");
        let row_root = Arc::new(
            RelationalRowPageRootReader::open_latest(&directory, row_config)
                .expect("open row root")
                .expect("row root exists"),
        );
        let cache = Arc::new(SegmentCache::new((PAGE_BYTES * 2) as u64));
        let reader = RelationalRowPageDemandReader::new(
            Arc::clone(&row_root),
            Arc::clone(&overflow_root),
            Arc::clone(&cache),
            StoreId(901),
        )
        .expect("open row demand reader");
        Self {
            directory,
            row_root,
            overflow_root,
            cache,
            reader,
            alpha: alpha_reference,
        }
    }

    fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        fs::remove_dir_all(directory).expect("remove demand-read fixture");
    }
}

fn row_publication_config() -> RelationalRowPagePublicationConfig {
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_page_bytes = NonZeroUsize::new(PAGE_BYTES).unwrap();
    config
}

fn table_delta(dirty_pages: Vec<ImmutableRelationalRowPage>) -> RelationalRowPageTableDelta {
    RelationalRowPageTableDelta {
        replaces_base: false,
        last_row_id: 0,
        table: "documents".to_string(),
        schema: Some(crate::relational::row_page::test_row_page_schema(
            "documents",
            4,
        )),
        schema_digest: schema_digest(),
        column_count: NonZeroU32::new(4).unwrap(),
        next_page_id: NonZeroU64::new(3).unwrap(),
        dirty_pages,
        deleted_page_ids: Vec::new(),
    }
}

fn page(
    page_id: u64,
    lower: i64,
    upper: i64,
    alpha: RelationalOverflowRef,
    beta: RelationalOverflowRef,
) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation: 1,
        source_commit_epoch: 10,
        page_id: RelationalRowPageId::new(NonZeroU64::new(page_id).unwrap()),
        schema_digest: schema_digest(),
        column_count: 4,
        rows: (lower..=upper)
            .map(|value| RelationalRowPageEntry {
                primary_key: key(value),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(value),
                    RelationalValue::Overflow(alpha),
                    RelationalValue::Overflow(beta),
                    RelationalValue::Text(format!("inline-{value}")),
                ]),
            })
            .collect(),
    }
}

fn key(value: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(value)])
}

fn projected_range<'a>(
    lower: Bound<&'a RelationalKey>,
    upper: Bound<&'a RelationalKey>,
    requested_fields: &'a [usize],
) -> RelationalRowPageProjectedRange<'a> {
    RelationalRowPageProjectedRange {
        table: "documents",
        lower,
        upper,
        requested_fields,
    }
}

fn schema_digest() -> Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 4)
}

fn flip_byte(path: &std::path::Path, offset: u64) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open row-page artifact");
    file.seek(SeekFrom::Start(offset))
        .expect("seek row-page artifact");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read row-page byte");
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(offset))
        .expect("rewind row-page artifact");
    file.write_all(&byte)
        .expect("write corrupted row-page byte");
    file.sync_all().expect("sync corrupted row-page artifact");
}

fn unique_test_dir(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hawdb-row-demand-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

#[test]
fn controlled_point_read_admits_page_and_selected_payload_then_retries_without_poison() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    let directory = unique_test_dir("controlled-demand-point");
    let overflow_config = RelationalOverflowPublicationConfig::default();
    RelationalOverflowPublisher::new(overflow_config)
        .publish(&directory, 1, 10, None, Vec::new())
        .unwrap();
    let overflow = Arc::new(
        RelationalOverflowRootReader::open_latest(&directory, overflow_config)
            .unwrap()
            .unwrap(),
    );
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let page = ImmutableRelationalRowPage {
        generation: 1,
        source_commit_epoch: 10,
        page_id: RelationalRowPageId::new(NonZeroU64::MIN),
        schema_digest: schema_digest(),
        column_count: 4,
        rows: vec![RelationalRowPageEntry {
            primary_key: key(1),
            row: RelationalRow::new(vec![
                RelationalValue::BigInt(1),
                RelationalValue::Text("v".repeat(128 * 1024)),
                RelationalValue::Bytea(vec![7; 128 * 1024]),
                RelationalValue::Text("small".into()),
            ]),
        }],
    };
    RelationalRowPagePublisher::new(config)
        .publish(&directory, 1, 10, None, vec![table_delta(vec![page])])
        .unwrap();
    let root = Arc::new(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap(),
    );
    let cache = Arc::new(SegmentCache::new(8 * 1024 * 1024));
    let reader = RelationalRowPageDemandReader::new(root, overflow, cache, StoreId(1)).unwrap();
    let target = key(1);
    let ordinary = reader
        .point_projected_unhydrated(
            "documents",
            &target,
            &[1],
            RelationalRowPageDemandReadLimits::default(),
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let mut budget = RelationalHydrationBudget::default();
        let before = budget;
        let result = work.classify(|scoped| {
            let allocations = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let row = reader.point_projected_with_hydration_mode(ProjectedPointReadRequest {
                table: "documents",
                primary_key: &target,
                requested_fields: &[1],
                limits: RelationalRowPageDemandReadLimits::default(),
                hydration: &mut budget,
                task: scoped.task_context(),
                hydration_fields: None,
                allocations: Some(&allocations),
            })?;
            Ok::<_, RelationalRowPageDemandReadError>((row, allocations.memory.into_inner()))
        });
        if succeeds {
            let ((row, report), memory) = result.unwrap();
            assert_eq!(row, ordinary.0);
            assert_eq!(report.rows_decoded, 1);
            assert_eq!(report.pages_read, 1);
            assert_eq!(report.file_pages_read, 1);
            assert_eq!(report.cache_hits, 0);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes <= ceiling - 128 * 1024
                        && available_bytes > ceiling - config.page_limits.max_page_bytes.get() as u64)
            );
            drop(row);
            drop(memory);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
        }
        assert!(!reader.is_poisoned());
        assert_eq!(budget, before);
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    drop(reader);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn controlled_point_hydration_matches_bound_overflow_values_and_budget() {
    use crate::background::{CheckpointDecodeContext, CheckpointWorkContext};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    let fixture = DemandFixture::new("controlled-bound-overflow");
    let target = key(1);
    let mut expected_budget = RelationalHydrationBudget::default();
    let expected = fixture
        .reader
        .point_projected(
            "documents",
            &target,
            &[1],
            RelationalRowPageDemandReadLimits::default(),
            &mut expected_budget,
            &RuntimeTaskContext::default(),
        )
        .unwrap();
    let ceiling = 16 * 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let context = CheckpointDecodeContext {
        work: CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default())),
        memory: Default::default(),
    };
    let mut budget = RelationalHydrationBudget::default();
    let (row, report) = fixture
        .reader
        .point_projected_with_hydration_mode(ProjectedPointReadRequest {
            table: "documents",
            primary_key: &target,
            requested_fields: &[1],
            limits: RelationalRowPageDemandReadLimits::default(),
            hydration: &mut budget,
            task: context.task_context(),
            hydration_fields: Some(&[1]),
            allocations: Some(&context),
        })
        .unwrap();
    assert_eq!(row, expected.0);
    assert_eq!(budget, expected_budget);
    assert_eq!(report.hydrated_values, 1);
    assert!(!fixture.reader.is_poisoned());
    drop(row);
    drop(context);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_overlay_output_copy_denies_before_wide_copy_and_retries() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    let fixture = DemandFixture::new("controlled-overlay-copy");
    let make_point = || RelationalRowPageOverlayPoint {
        table: "documents",
        primary_key: key(1),
        overflow_root: None,
        value: RelationalRowPageProjectedOverlayValue::Present {
            fields: vec![RelationalProjectedField {
                ordinal: 1,
                value: RelationalValue::Text("v".repeat(128 * 1024)),
            }]
            .into_boxed_slice(),
            binds_overlay_overflow: false,
        },
    };
    let mut ordinary_budget = RelationalHydrationBudget::default();
    let ordinary = fixture
        .reader
        .point_projected_overlay(
            make_point(),
            RelationalRowPageDemandReadLimits::default(),
            &mut ordinary_budget,
            &RuntimeTaskContext::default(),
            None,
        )
        .unwrap();
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let mut budget = RelationalHydrationBudget::default();
        let initial = budget;
        let point = make_point();
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let output = fixture.reader.point_projected_overlay_with_allocations(
                point,
                RelationalRowPageDemandReadLimits::default(),
                &mut budget,
                scoped.task_context(),
                None,
                Some(&context),
            )?;
            Ok::<_, RelationalRowPageDemandReadError>((output, context.memory.into_inner()))
        });
        let wide_allocations = observation.finish();
        if succeeds {
            let ((row, report), memory) = result.unwrap();
            assert_eq!(row, ordinary.0);
            assert_eq!(report.rows_decoded, ordinary.1.rows_decoded);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes <= ceiling - 128 * 1024)
            );
            drop(row);
            drop(memory);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(wide_allocations, 0);
        }
        assert_eq!(budget, initial);
        assert!(!fixture.reader.is_poisoned());
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn failed_overlay_return_copy_restores_budget_after_successful_hydration() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit};
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    #[derive(Debug)]
    struct Controller {
        base: RuntimeTaskContext,
        payload_bytes: u64,
        calls: AtomicUsize,
        deny: AtomicBool,
    }
    impl RuntimeMemoryController for Controller {
        fn reserve(
            &self,
            bytes: u64,
            _: u64,
        ) -> Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
            if bytes == self.payload_bytes
                && self.calls.fetch_add(1, Ordering::SeqCst) == 1
                && self.deny.load(Ordering::SeqCst)
            {
                return Err(RuntimeMemoryError::ReservationExceeded {
                    requested_bytes: bytes,
                    available_bytes: 0,
                });
            }
            self.base
                .reserve_working_memory(bytes)?
                .ok_or(RuntimeMemoryError::Closed)
        }
    }
    let fixture = DemandFixture::new("overlay-return-copy-failure");
    let ceiling = 16 * 1024 * 1024;
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let controller = Arc::new(Controller {
        base: permit.bind_task_context(RuntimeTaskContext::default()),
        payload_bytes: fixture.alpha.uncompressed_bytes,
        calls: AtomicUsize::new(0),
        deny: AtomicBool::new(true),
    });
    let work = CheckpointWorkContext::new(
        controller
            .base
            .clone()
            .with_memory_controller(controller.clone()),
    );
    let initial = RelationalHydrationBudget::default();
    let mut budget = initial;
    for succeeds in [false, true] {
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let point = RelationalRowPageOverlayPoint {
                table: "documents",
                primary_key: key(1),
                overflow_root: Some(&fixture.overflow_root),
                value: RelationalRowPageProjectedOverlayValue::Present {
                    fields: vec![RelationalProjectedField {
                        ordinal: 1,
                        value: RelationalValue::Overflow(fixture.alpha),
                    }]
                    .into_boxed_slice(),
                    binds_overlay_overflow: true,
                },
            };
            let output = fixture.reader.point_projected_overlay_with_allocations(
                point,
                RelationalRowPageDemandReadLimits::default(),
                &mut budget,
                scoped.task_context(),
                Some(&[1]),
                Some(&context),
            )?;
            Ok::<_, RelationalRowPageDemandReadError>((output, context.memory.into_inner()))
        });
        if succeeds {
            let ((row, _), memory) = result.unwrap();
            assert!(row.is_some());
            assert_eq!(budget.hydrated_rows, 1);
            drop(row);
            drop(memory);
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(controller.calls.load(Ordering::SeqCst), 2);
            assert_eq!(budget, initial);
            controller.deny.store(false, Ordering::SeqCst);
        }
        assert!(!fixture.reader.is_poisoned());
        assert!(matches!(controller.base.reserve_working_memory(ceiling),
            Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling));
    }
    drop(work);
    drop(controller);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn controlled_point_with_wide_table_bounds_denies_without_cloning_root_metadata() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    let directory = unique_test_dir("borrowed-wide-table");
    let overflow_config = RelationalOverflowPublicationConfig::default();
    RelationalOverflowPublisher::new(overflow_config)
        .publish(&directory, 1, 10, None, Vec::new())
        .unwrap();
    let overflow = Arc::new(
        RelationalOverflowRootReader::open_latest(&directory, overflow_config)
            .unwrap()
            .unwrap(),
    );
    let mut config = RelationalRowPagePublicationConfig::default();
    config.page_limits.max_key_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    config.page_limits.max_inline_value_bytes = NonZeroUsize::new(256 * 1024).unwrap();
    let mut schema = crate::relational::row_page::test_row_page_schema("documents", 1);
    schema.columns[0].scalar_type = RelationalScalarType::Text;
    let digest = crate::relational::index_shadow::relational_schema_digest(&schema).unwrap();
    let value = "k".repeat(128 * 1024);
    let page = ImmutableRelationalRowPage {
        generation: 1,
        source_commit_epoch: 10,
        page_id: RelationalRowPageId::new(NonZeroU64::MIN),
        schema_digest: digest,
        column_count: 1,
        rows: vec![RelationalRowPageEntry {
            primary_key: RelationalKey(vec![RelationalValue::Text(value.clone())]),
            row: RelationalRow::new(vec![RelationalValue::Text(value)]),
        }],
    };
    RelationalRowPagePublisher::new(config)
        .publish_with_overflow_root(
            &directory,
            1,
            10,
            None,
            vec![RelationalRowPageTableDelta {
                replaces_base: false,
                last_row_id: 0,
                table: "documents".into(),
                schema: Some(schema),
                schema_digest: digest,
                column_count: NonZeroU32::MIN,
                next_page_id: NonZeroU64::new(2).unwrap(),
                dirty_pages: vec![page],
                deleted_page_ids: Vec::new(),
            }],
            &overflow,
        )
        .unwrap();
    let root = Arc::new(
        RelationalRowPageRootReader::open_latest(&directory, config)
            .unwrap()
            .unwrap(),
    );
    let reader = RelationalRowPageDemandReader::new(
        root,
        overflow,
        Arc::new(SegmentCache::new(64 * 1024)),
        StoreId(923),
    )
    .unwrap();
    let target = RelationalKey(vec![RelationalValue::Text("!".into())]);
    for (ceiling, succeeds) in [(4096, false), (16 * 1024 * 1024, true)] {
        let governor = RuntimeGovernor::detect(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(ceiling),
                ..RuntimeGovernorConfig::shared_host()
            },
            IoConcurrencyBudget::new(2, 1),
        );
        governor.pin_resources();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
            .unwrap();
        let work =
            CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
        let mut budget = RelationalHydrationBudget::default();
        let initial = budget;
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            reader.point_projected_with_hydration_mode(ProjectedPointReadRequest {
                table: "documents",
                primary_key: &target,
                requested_fields: &[0],
                limits: RelationalRowPageDemandReadLimits::default(),
                hydration: &mut budget,
                task: scoped.task_context(),
                hydration_fields: None,
                allocations: Some(&context),
            })
        });
        let wide_allocations = observation.finish();
        if succeeds {
            assert!(result.unwrap().0.is_none());
        } else {
            assert!(
                matches!(
                    result,
                    Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                        _
                    )))
                ),
                "{result:?}"
            );
            assert_eq!(
                wide_allocations, 0,
                "root metadata must be borrowed before descriptor admission"
            );
        }
        assert_eq!(budget, initial);
        assert!(!reader.is_poisoned());
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
    drop(reader);
    fs::remove_dir_all(directory).unwrap();
}
