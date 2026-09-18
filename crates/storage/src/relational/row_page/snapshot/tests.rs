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
    estimated_row_change_encoding_bytes, ImmutableRelationalRowPage,
    RelationalOverflowPublicationConfig, RelationalOverflowPublisher, RelationalOverflowRef,
    RelationalRow, RelationalRowChange, RelationalRowChangeCapture,
    RelationalRowChangeCaptureLimits, RelationalRowDeltaBuilder, RelationalRowDeltaConfig,
    RelationalRowDeltaReader, RelationalRowDeltaTableMetadata, RelationalRowPageEntry,
    RelationalRowPageId, RelationalRowPagePublicationConfig, RelationalRowPagePublisher,
    RelationalRowPageRootReader, RelationalRowPageTableDelta, RelationalValue,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_integrity::{integrity_digest, Sha256Digest};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[test]
fn point_reads_select_live_recovery_checkpoint_and_tombstones() {
    let fixture = SnapshotFixture::new("point-precedence");
    let expected = [
        (
            1,
            Some("one-recovery"),
            RelationalRowPageSnapshotRowSource::Recovery,
        ),
        (
            2,
            Some("two-live"),
            RelationalRowPageSnapshotRowSource::Live,
        ),
        (3, None, RelationalRowPageSnapshotRowSource::Deleted),
        (
            4,
            Some("four-live"),
            RelationalRowPageSnapshotRowSource::Live,
        ),
        (5, None, RelationalRowPageSnapshotRowSource::Deleted),
        (
            6,
            Some("six-live"),
            RelationalRowPageSnapshotRowSource::Live,
        ),
        (9, None, RelationalRowPageSnapshotRowSource::Missing),
    ];

    for (id, body, source) in expected {
        let mut hydration = RelationalHydrationBudget::default();
        let (row, report) = fixture
            .reader
            .point_projected(
                "documents",
                &key(id),
                &[1],
                RelationalRowPageSnapshotReadLimits::default(),
                &mut hydration,
                &RuntimeTaskContext::default(),
            )
            .expect("read snapshot point");
        assert_eq!(projected_body(row.as_ref()), body);
        assert_eq!(report.source, source);
        assert_eq!(report.identity.visible_commit_epoch, 15);
    }
    assert!(!fixture.reader.is_poisoned());
    fixture.remove();
}

#[test]
fn multi_point_reads_preserve_snapshot_precedence_and_share_checkpoint_pages() {
    let fixture = SnapshotFixture::new("multi-point-precedence");
    let mut hydration = RelationalHydrationBudget::default();
    let (rows, report) = fixture
        .reader
        .points_projected_fields(
            "documents",
            &[
                key(0),
                key(0),
                key(1),
                key(2),
                key(3),
                key(4),
                key(5),
                key(6),
                key(9),
            ],
            RelationalRowPageProjectedFields {
                requested_fields: &[1],
                hydration_fields: &[1],
            },
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .expect("read snapshot points");

    let bodies = rows
        .iter()
        .map(|(primary_key, row)| {
            (
                primary_key.clone(),
                projected_body(Some(row))
                    .expect("projected body")
                    .to_string(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        bodies,
        vec![
            (key(0), "zero".to_string()),
            (key(1), "one-recovery".to_string()),
            (key(2), "two-live".to_string()),
            (key(4), "four-live".to_string()),
            (key(6), "six-live".to_string()),
        ]
    );
    assert_eq!(report.identity.visible_commit_epoch, 15);
    assert_eq!(report.overlay_entries, 6);
    assert_eq!(report.demand.pages_read, 1);
    assert_eq!(report.demand.rows_emitted, 5);
    assert!(report.live_batches_examined > 0);
    fixture.remove();
}

#[test]
fn range_reads_merge_ordered_rows_and_keep_overlay_after_the_base_tail() {
    let fixture = SnapshotFixture::new("range-merge");
    let mut hydration = RelationalHydrationBudget::default();
    let mut rows = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Included(&key(1)), Bound::Included(&key(6))),
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                let body = projected_body(Some(&row)).unwrap().to_string();
                rows.push((row.primary_key, body));
                true
            },
        )
        .expect("merge snapshot range");
    assert_eq!(
        rows,
        vec![
            (key(1), "one-recovery".to_string()),
            (key(2), "two-live".to_string()),
            (key(4), "four-live".to_string()),
            (key(6), "six-live".to_string()),
        ]
    );
    assert_eq!(report.overlay_entries, 6);
    assert_eq!(report.overlay_replacements, 1);
    assert_eq!(report.overlay_merge_sources, 5);
    assert_eq!(report.recovery.peak_open_files, 3);
    assert_eq!(report.recovery.range_file_opens, 3);
    assert_eq!(report.recovery.range_file_pool_misses, 3);
    assert_eq!(report.recovery.range_file_pool_hits, 0);
    assert!(report.overlay_peak_buffered_entries <= report.overlay_merge_sources + 1);
    assert_eq!(report.demand.rows_emitted, 4);
    assert_eq!(report.demand.borrowed_rows_emitted, 0);
    assert_eq!(report.demand.owned_rows_emitted, 4);

    let mut hydration = RelationalHydrationBudget::default();
    let mut tail = Vec::new();
    fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Excluded(&key(5)), Bound::Unbounded),
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                tail.push(row.primary_key);
                true
            },
        )
        .expect("read overlay beyond base tail");
    assert_eq!(tail, vec![key(6)]);
    fixture.remove();
}

#[test]
fn range_callback_starts_before_the_complete_overlay_is_consumed() {
    let fixture = SnapshotFixture::new("range-streaming-start");
    let mut hydration = RelationalHydrationBudget::default();
    let mut rows = Vec::new();
    let report = fixture
        .reader
        .visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded),
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
            |row| {
                rows.push(row.primary_key);
                false
            },
        )
        .expect("stop the streaming merge after its first row");

    assert_eq!(rows, vec![key(0)]);
    assert_eq!(report.overlay_entries, 0);
    assert_eq!(report.overlay_merge_sources, 5);
    assert_eq!(report.live_entries_visited, 2);
    assert!(report.recovery.stopped_early);
    assert!(report.overlay_peak_buffered_entries <= report.overlay_merge_sources + 1);
    fixture.remove();
}

#[test]
fn overlay_admission_and_cancellation_do_not_poison_the_reader() {
    let fixture = SnapshotFixture::new("admission-cancellation");
    let point_limits = RelationalRowPageSnapshotReadLimits {
        max_overlay_bytes: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowPageSnapshotReadLimits::default()
    };
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(2),
            &[1],
            point_limits,
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(RelationalRowPageSnapshotReadError::Admission(message))
            if message.contains("overlay point")
    ));
    assert!(!fixture.reader.is_poisoned());

    let limits = RelationalRowPageSnapshotReadLimits {
        max_overlay_entries: NonZeroUsize::new(1).unwrap(),
        ..RelationalRowPageSnapshotReadLimits::default()
    };
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.visit_projected_range(
            projected_range(Bound::Unbounded, Bound::Unbounded),
            limits,
            &mut hydration,
            &RuntimeTaskContext::default(),
            |_| true,
        ),
        Err(RelationalRowPageSnapshotReadError::Admission(_))
    ));
    assert!(!fixture.reader.is_poisoned());

    let cancellation = RuntimeCancellationToken::new();
    cancellation.cancel();
    let task = RuntimeTaskContext::new(cancellation, None);
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        fixture.reader.point_projected(
            "documents",
            &key(0),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &task,
        ),
        Err(RelationalRowPageSnapshotReadError::Stopped(_))
    ));
    assert!(!fixture.reader.is_poisoned());

    let mut hydration = RelationalHydrationBudget::default();
    assert!(fixture
        .reader
        .point_projected(
            "documents",
            &key(1),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .is_ok());
    fixture.remove();
}

#[test]
fn overlay_projection_drops_unrequested_large_values_before_residency() {
    let large = vec![7u8; 1024 * 1024];
    let value = RelationalRowPageRecoveredValue::Present(RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Text("selected".to_string()),
        RelationalValue::Bytea(large),
    ]));
    validate_overlay_row(&value, 3).unwrap();
    let resident_bytes = projected_overlay_resident_bytes(&value, &[1]).unwrap();
    assert!(resident_bytes < 1024);
    let projected = project_overlay_value(&value, &[1], true);
    let RelationalRowPageProjectedOverlayValue::Present {
        fields,
        binds_overlay_overflow,
    } = projected
    else {
        panic!("present overlay row must remain present");
    };
    assert!(binds_overlay_overflow);
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].ordinal, 1);
    assert_eq!(
        fields[0].value,
        RelationalValue::Text("selected".to_string())
    );
}

#[test]
fn live_overflow_stays_unresolved_until_the_pinned_state_resolves_it() {
    let fixture = SnapshotFixture::new("live-overflow");
    let reference = RelationalOverflowRef {
        digest: integrity_digest(b"live-overflow").sha256,
        scalar_type: crate::relational::RelationalScalarType::Text,
        compressed_bytes: 128,
        uncompressed_bytes: 1024,
    };
    let change = RelationalRowChange {
        table: "documents".to_string(),
        primary_key: key(7),
        row: Some(RelationalRow::new(vec![
            RelationalValue::BigInt(7),
            RelationalValue::Overflow(reference),
        ])),
    };
    let encoded_bytes = estimated_row_change_encoding_bytes(&change).unwrap();
    let view = Arc::new(
        fixture
            .view
            .advance(
                16,
                Some(RelationalRowChangeCapture::Captured {
                    changes: vec![change],
                    encoded_bytes,
                }),
                RelationalRowChangeCaptureLimits::default(),
            )
            .expect("advance live view with an overflow reference"),
    );
    let reader = RelationalRowPageSnapshotReader::new(
        view,
        Arc::clone(&fixture.overflow_root),
        None,
        Arc::new(SegmentCache::new(64 * 1024)),
        StoreId(905),
    )
    .expect("open snapshot reader without an overlay overflow root");
    let mut hydration = RelationalHydrationBudget::default();
    let (row, report) = reader
        .point_projected(
            "documents",
            &key(7),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .expect("read unresolved live overflow reference");
    let row = row.expect("live row");
    assert_eq!(row.fields[0].value, RelationalValue::Overflow(reference));
    assert_eq!(report.source, RelationalRowPageSnapshotRowSource::Live);
    assert_eq!(report.demand.hydrated_values, 0);
    assert_eq!(hydration.hydrated_rows, 0);
    assert!(!reader.is_poisoned());
    fixture.remove();
}

#[test]
fn pinned_reader_does_not_observe_a_later_live_view() {
    let fixture = SnapshotFixture::new_at_epoch("pinned-reader", 14);
    let pinned_identity = fixture.reader.identity();
    assert_eq!(pinned_identity.visible_commit_epoch, 14);
    let mut hydration = RelationalHydrationBudget::default();
    let (row, report) = fixture
        .reader
        .point_projected(
            "documents",
            &key(5),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        )
        .expect("read pinned base row");
    assert_eq!(projected_body(row.as_ref()), Some("five"));
    assert_eq!(
        report.source,
        RelationalRowPageSnapshotRowSource::Checkpoint
    );
    fixture.remove();
}

#[test]
fn checkpoint_corruption_poison_is_sticky_at_the_composite_reader() {
    let fixture = SnapshotFixture::new("sticky-corruption");
    flip_byte(
        &fixture
            .directory
            .join(crate::relational::relational_row_page_artifact_file(1)),
        0,
    );
    let cold = RelationalRowPageSnapshotReader::new(
        Arc::clone(&fixture.view),
        Arc::clone(&fixture.overflow_root),
        None,
        Arc::new(SegmentCache::new(64 * 1024)),
        StoreId(904),
    )
    .expect("open cold snapshot reader");
    let mut hydration = RelationalHydrationBudget::default();
    assert!(matches!(
        cold.point_projected(
            "documents",
            &key(0),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(RelationalRowPageSnapshotReadError::Corrupt(message))
            if message.contains("checksum mismatch")
    ));
    assert!(cold.is_poisoned());
    assert!(matches!(
        cold.point_projected(
            "documents",
            &key(2),
            &[1],
            RelationalRowPageSnapshotReadLimits::default(),
            &mut hydration,
            &RuntimeTaskContext::default(),
        ),
        Err(RelationalRowPageSnapshotReadError::Corrupt(message))
            if message.contains("poisoned")
    ));
    fixture.remove();
}

struct SnapshotFixture {
    directory: PathBuf,
    view: Arc<RelationalRowPageReadView>,
    overflow_root: Arc<RelationalOverflowRootReader>,
    reader: RelationalRowPageSnapshotReader,
}

impl SnapshotFixture {
    fn new(label: &str) -> Self {
        Self::new_at_epoch(label, 15)
    }

    fn new_at_epoch(label: &str, visible_epoch: u64) -> Self {
        let directory = unique_test_dir(label);
        let overflow_config = RelationalOverflowPublicationConfig::default();
        RelationalOverflowPublisher::new(overflow_config)
            .publish(&directory, 1, 10, None, Vec::new())
            .expect("publish empty overflow root");
        let overflow_root = Arc::new(
            RelationalOverflowRootReader::open_latest(&directory, overflow_config)
                .expect("open overflow root")
                .expect("overflow root exists"),
        );
        let row_config = RelationalRowPagePublicationConfig::default();
        RelationalRowPagePublisher::new(row_config)
            .publish_with_overflow_root(
                &directory,
                1,
                10,
                None,
                vec![RelationalRowPageTableDelta {
                    table: "documents".to_string(),
                    schema: Some(crate::relational::row_page::test_row_page_schema(
                        "documents",
                        2,
                    )),
                    schema_digest: schema_digest(),
                    column_count: NonZeroU32::new(2).unwrap(),
                    next_page_id: NonZeroU64::new(2).unwrap(),
                    dirty_pages: vec![ImmutableRelationalRowPage {
                        generation: 1,
                        source_commit_epoch: 10,
                        page_id: RelationalRowPageId::new(NonZeroU64::new(1).unwrap()),
                        schema_digest: schema_digest(),
                        column_count: 2,
                        rows: vec![
                            entry(0, "zero"),
                            entry(1, "one"),
                            entry(3, "three"),
                            entry(5, "five"),
                        ],
                    }],
                    deleted_page_ids: Vec::new(),
                }],
                &overflow_root,
            )
            .expect("publish row root");
        let row_root = Arc::new(
            RelationalRowPageRootReader::open_latest(&directory, row_config)
                .expect("open row root")
                .expect("row root exists"),
        );
        let delta_config = RelationalRowDeltaConfig {
            max_dirty_entries: NonZeroUsize::new(1).unwrap(),
            ..RelationalRowDeltaConfig::default()
        };
        let mut builder = RelationalRowDeltaBuilder::new(
            &directory,
            &row_root,
            1,
            None,
            vec![RelationalRowDeltaTableMetadata {
                table: "documents".to_string(),
                schema_digest: schema_digest(),
                column_count: NonZeroU32::new(2).unwrap(),
                row_count: 3,
            }],
            delta_config,
        )
        .expect("create recovery delta");
        builder
            .record(11, capture(vec![change(1, Some("one-recovery"))]))
            .unwrap();
        builder
            .record(12, capture(vec![change(2, Some("two-recovery"))]))
            .unwrap();
        builder.record(13, capture(vec![change(3, None)])).unwrap();
        builder.finish(13, None).expect("publish recovery delta");
        let delta = Arc::new(
            RelationalRowDeltaReader::open_latest(&directory, &row_root, 13, delta_config)
                .expect("open recovery delta")
                .expect("recovery delta exists"),
        );
        let recovered =
            RelationalRowPageReadView::from_recovery_delta(Arc::clone(&row_root), delta)
                .expect("bind recovery view");
        let live_limits = RelationalRowChangeCaptureLimits {
            max_entries: NonZeroUsize::new(32).unwrap(),
            max_bytes: NonZeroUsize::new(64 * 1024).unwrap(),
        };
        let epoch_14 = recovered
            .advance(
                14,
                Some(capture(vec![
                    change(2, Some("two-live")),
                    change(4, Some("four-live")),
                ])),
                live_limits,
            )
            .expect("publish first live view");
        let selected = if visible_epoch == 14 {
            epoch_14
        } else {
            assert_eq!(visible_epoch, 15);
            epoch_14
                .advance(
                    15,
                    Some(capture(vec![change(5, None), change(6, Some("six-live"))])),
                    live_limits,
                )
                .expect("publish second live view")
        };
        let view = Arc::new(selected);
        let reader = RelationalRowPageSnapshotReader::new(
            Arc::clone(&view),
            Arc::clone(&overflow_root),
            None,
            Arc::new(SegmentCache::new(64 * 1024)),
            StoreId(903),
        )
        .expect("open snapshot reader");
        Self {
            directory,
            view,
            overflow_root,
            reader,
        }
    }

    fn remove(self) {
        let directory = self.directory.clone();
        drop(self);
        fs::remove_dir_all(directory).expect("remove snapshot fixture");
    }
}

fn entry(id: i64, body: &str) -> RelationalRowPageEntry {
    RelationalRowPageEntry {
        primary_key: key(id),
        row: row(id, body),
    }
}

fn change(id: i64, body: Option<&str>) -> RelationalRowChange {
    RelationalRowChange {
        table: "documents".to_string(),
        primary_key: key(id),
        row: body.map(|body| row(id, body)),
    }
}

fn capture(changes: Vec<RelationalRowChange>) -> RelationalRowChangeCapture {
    let encoded_bytes = changes
        .iter()
        .map(|change| estimated_row_change_encoding_bytes(change).unwrap())
        .sum();
    RelationalRowChangeCapture::Captured {
        changes,
        encoded_bytes,
    }
}

fn row(id: i64, body: &str) -> RelationalRow {
    RelationalRow::new(vec![
        RelationalValue::BigInt(id),
        RelationalValue::Text(body.to_string()),
    ])
}

fn key(id: i64) -> RelationalKey {
    RelationalKey(vec![RelationalValue::BigInt(id)])
}

fn projected_range<'a>(
    lower: Bound<&'a RelationalKey>,
    upper: Bound<&'a RelationalKey>,
) -> RelationalRowPageProjectedRange<'a> {
    RelationalRowPageProjectedRange {
        table: "documents",
        lower,
        upper,
        requested_fields: &[1],
    }
}

fn projected_body(row: Option<&RelationalProjectedRow>) -> Option<&str> {
    match row?.fields.first()?.value {
        RelationalValue::Text(ref body) => Some(body),
        _ => None,
    }
}

fn schema_digest() -> Sha256Digest {
    crate::relational::row_page::test_row_page_schema_digest("documents", 2)
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
        "hawdb-row-snapshot-{label}-{}-{}",
        std::process::id(),
        TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}
