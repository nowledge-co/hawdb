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
use crate::relational::row_page::PROJECTED_ROW_DECODES;
use crate::relational::RelationalRowPageSnapshotReadLimits;

fn decode_admission_guard(operation: &str) {
    let fixture = DemandFixture::new(operation);
    let task = RuntimeTaskContext::default();
    let mut hydration = RelationalHydrationBudget::default();
    let mut statement = RelationalRowPageSnapshotReadLimits::default();
    statement.demand.max_rows = NonZeroUsize::new(1).unwrap();
    fixture.reader.cumulative.restrict(statement).unwrap();
    PROJECTED_ROW_DECODES.with(|count| count.set(0));
    let error = match operation {
        "point" => {
            fixture
                .reader
                .point_projected_unhydrated("documents", &key(1), &[3], Default::default(), &task)
                .unwrap();
            fixture
                .reader
                .point_projected_unhydrated("documents", &key(2), &[3], Default::default(), &task)
                .unwrap_err()
        }
        "batch" => fixture
            .reader
            .points_projected_fields(
                "documents",
                &[key(1), key(2)],
                RelationalRowPageProjectedFields {
                    requested_fields: &[3],
                    hydration_fields: &[],
                },
                Default::default(),
                &mut hydration,
                &task,
            )
            .unwrap_err(),
        "range" => {
            let mut emitted = Vec::new();
            let error = fixture
                .reader
                .visit_projected_range(
                    projected_range(Bound::Unbounded, Bound::Unbounded, &[3]),
                    Default::default(),
                    &mut hydration,
                    &task,
                    |row| {
                        emitted.push(row.primary_key);
                        true
                    },
                )
                .unwrap_err();
            assert_eq!(emitted, vec![key(1)]);
            error
        }
        _ => panic!("unsupported operation"),
    };
    assert!(
        matches!(error, RelationalRowPageDemandReadError::Admission(ref message) if message.contains("row"))
    );
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(fixture.reader.cumulative.report().unwrap().admitted_rows, 1);
    PROJECTED_ROW_DECODES
        .with(|count| assert_eq!(count.get(), 1, "{operation} decoded a row before admission"));
    fixture.remove();
}

#[test]
fn point_admits_before_projected_decode() {
    decode_admission_guard("point");
}

#[test]
fn batch_admits_before_projected_decode() {
    decode_admission_guard("batch");
}

#[test]
fn range_admits_before_projected_decode() {
    decode_admission_guard("range");
}

#[test]
fn failed_page_attempt_retains_admission_before_retry() {
    use crate::file_descriptors::ProjectFileDescriptors;
    use hawdb_core::error::file_descriptor_error;

    let fixture = DemandFixture::new("failed-page-admission");
    let descriptor = fixture
        .row_root
        .read_table_page_descriptor("documents", 0)
        .unwrap();
    let mut statement = RelationalRowPageSnapshotReadLimits::default();
    statement.demand.max_pages = NonZeroUsize::new(1).unwrap();
    statement.demand.max_bytes = NonZeroUsize::new(PAGE_BYTES).unwrap();
    fixture.reader.cumulative.restrict(statement).unwrap();
    let project = ProjectFileDescriptors::acquire(&fixture.directory, 16).unwrap();
    let path = fixture.directory.join("held-descriptor");
    fs::write(&path, b"held").unwrap();
    let held = (0..16)
        .map(|_| crate::file_io::File::open(&path).unwrap())
        .collect::<Vec<_>>();
    let task = RuntimeTaskContext::default();
    let mut hydration = RelationalHydrationBudget::default();
    let mut context =
        DemandReadContext::new(&fixture.reader, statement.demand, &mut hydration, &task).unwrap();
    let error = match context.read_page(&descriptor) {
        Err(error) => error,
        Ok(_) => panic!("page read succeeded with no descriptor allowance"),
    };
    assert!(file_descriptor_error(&error).is_some(), "{error}");
    assert!(!fixture.reader.is_poisoned());
    let report = fixture.reader.cumulative.report().unwrap();
    assert_eq!(report.demand.pages_read, 1);
    assert_eq!(report.demand.bytes_read, PAGE_BYTES);
    assert_eq!(report.demand.file_pages_read, 0);
    assert_eq!(report.demand.file_bytes_read, 0);
    assert_eq!(report.demand.cache_hits, 0);
    assert_eq!(report.demand.cache_misses, 0);
    assert_eq!(report.admitted_rows, 0);
    drop(held);
    assert_eq!(project.metrics().open, 0);
    assert_eq!(project.metrics().reserved, 0);
    // Repairing I/O and attaching a larger cap cannot refund the failed work.
    fixture
        .reader
        .cumulative
        .restrict(Default::default())
        .unwrap();
    let error = match context.read_page(&descriptor) {
        Err(error) => error,
        Ok(_) => panic!("failed I/O refunded its statement page allowance"),
    };
    assert!(
        matches!(error, RelationalRowPageDemandReadError::Admission(ref message) if message.contains("page"))
    );
    assert_eq!(fixture.reader.cumulative.report().unwrap(), report);
    assert!(!fixture.reader.is_poisoned());
    fixture.remove();
}

fn failed_hydration_decode_report_guard(operation: &str) {
    let fixture = DemandFixture::new(operation);
    let task = RuntimeTaskContext::default();
    let mut hydration = RelationalHydrationBudget {
        max_decompressed_bytes: 1,
        ..Default::default()
    };
    let mut statement = RelationalRowPageSnapshotReadLimits::default();
    statement.demand.max_rows = NonZeroUsize::new(1).unwrap();
    fixture.reader.cumulative.restrict(statement).unwrap();
    PROJECTED_ROW_DECODES.with(|count| count.set(0));
    let error = match operation {
        "point" => fixture
            .reader
            .point_projected_fields(
                "documents",
                &key(1),
                RelationalRowPageProjectedFields {
                    requested_fields: &[1],
                    hydration_fields: &[1],
                },
                Default::default(),
                &mut hydration,
                &task,
            )
            .unwrap_err(),
        "batch" => fixture
            .reader
            .points_projected_fields(
                "documents",
                &[key(1)],
                RelationalRowPageProjectedFields {
                    requested_fields: &[1],
                    hydration_fields: &[1],
                },
                Default::default(),
                &mut hydration,
                &task,
            )
            .unwrap_err(),
        "range" => {
            let mut callbacks = 0;
            let error = fixture
                .reader
                .visit_projected_range(
                    projected_range(Bound::Unbounded, Bound::Unbounded, &[1]),
                    Default::default(),
                    &mut hydration,
                    &task,
                    |_| {
                        callbacks += 1;
                        true
                    },
                )
                .unwrap_err();
            assert_eq!(callbacks, 0);
            error
        }
        "overlay" => fixture
            .reader
            .point_projected_overlay(
                RelationalRowPageOverlayPoint {
                    table: "documents",
                    primary_key: key(1),
                    value: RelationalRowPageProjectedOverlayValue::Present {
                        fields: vec![RelationalProjectedField {
                            ordinal: 1,
                            value: RelationalValue::Overflow(fixture.alpha),
                        }]
                        .into_boxed_slice(),
                        binds_overlay_overflow: true,
                    },
                    overflow_root: Some(&fixture.overflow_root),
                },
                Default::default(),
                &mut hydration,
                &task,
                Some(&[1]),
            )
            .unwrap_err(),
        _ => panic!("unsupported operation"),
    };
    assert!(
        matches!(error, RelationalRowPageDemandReadError::Admission(_)),
        "{error}"
    );
    assert!(!fixture.reader.is_poisoned());
    assert_eq!(hydration.hydrated_rows, 0);
    assert_eq!(hydration.compressed_bytes, 0);
    assert_eq!(hydration.decompressed_bytes, 0);
    PROJECTED_ROW_DECODES
        .with(|count| assert_eq!(count.get(), usize::from(operation != "overlay")));
    let report = fixture.reader.cumulative.report().unwrap();
    assert_eq!(report.admitted_rows, 1);
    assert_eq!(report.demand.rows_emitted, 0);
    assert_eq!(report.demand.owned_rows_emitted, 0);
    assert_eq!(report.demand.borrowed_rows_emitted, 0);
    assert_eq!(
        report.demand.rows_decoded, 1,
        "{operation} lost successful decode evidence on hydration failure"
    );
    fixture.remove();
}

#[test]
fn point_retains_decoded_evidence_on_failed_hydration() {
    failed_hydration_decode_report_guard("point");
}

#[test]
fn batch_retains_decoded_evidence_on_failed_hydration() {
    failed_hydration_decode_report_guard("batch");
}

#[test]
fn range_retains_decoded_evidence_on_failed_hydration() {
    failed_hydration_decode_report_guard("range");
}

#[test]
fn overlay_retains_decoded_evidence_on_failed_hydration() {
    failed_hydration_decode_report_guard("overlay");
}

#[test]
fn malformed_selected_value_retains_admission_without_successful_decode_or_emission() {
    use crate::relational::row_page::{
        decode_header, write_integrity, RowSlot, ROW_PAGE_HEADER_BYTES, ROW_SLOT_BYTES,
        VALUE_SLOT_BYTES,
    };

    let fixture = DemandFixture::new("malformed-selected-value-report");
    fixture
        .reader
        .cumulative
        .restrict(Default::default())
        .unwrap();
    let task = RuntimeTaskContext::default();
    let mut hydration = RelationalHydrationBudget::default();
    {
        let mut context =
            DemandReadContext::new(&fixture.reader, Default::default(), &mut hydration, &task)
                .unwrap();
        let descriptor = fixture
            .row_root
            .read_table_page_descriptor("documents", 0)
            .unwrap();
        let page = context.read_page(&descriptor).unwrap();
        let mut encoded = page.bytes.to_vec();
        let limits = row_publication_config().page_limits;
        let header = decode_header(&encoded, limits).unwrap();
        let directory_start =
            ROW_PAGE_HEADER_BYTES + header.lower_bound_len + header.upper_bound_len;
        let row_start = directory_start + header.directory_len + header.key_payload_len;
        let first_slot =
            RowSlot::decode(&encoded[directory_start..directory_start + ROW_SLOT_BYTES]).unwrap();
        let first_value_start =
            row_start + first_slot.row_offset as usize + 4 + header.column_count * VALUE_SLOT_BYTES;
        assert_eq!(
            encoded[first_value_start], 2,
            "the real fixture's selected BigInt tag"
        );
        encoded[first_value_start] = 99;
        write_integrity(&mut encoded);
        let view = RelationalRowPageView::open(&encoded, limits).unwrap();
        let mut cursor = ProjectedRowPageCursor::new(view, 0, &[0]).unwrap();
        assert!(cursor.peek_encoded_primary_key().unwrap().is_some());
        let mut callbacks = 0;
        let error = (|| {
            let row = context.decode_row(|| {
                cursor.next_row()?.ok_or_else(|| {
                    RelationalRowPageError::Corrupt("row cursor lost an admitted row".to_string())
                })
            })?;
            emit_base_row(&mut context, row, None, &mut |_, _| {
                callbacks += 1;
                true
            })
        })()
        .unwrap_err();
        assert!(
            matches!(error, RelationalRowPageDemandReadError::Corrupt(ref message)
            if message.contains("invalid relational row value tag")),
            "{error}"
        );
        assert!(fixture.reader.is_poisoned());
        assert_eq!(callbacks, 0);
        assert_eq!(context.report.rows_decoded, 0);
        assert_eq!(context.report.rows_emitted, 0);
        let report = fixture.reader.cumulative.report().unwrap();
        assert_eq!(report.admitted_rows, 1);
        assert_eq!(report.demand.rows_decoded, 0);
        assert_eq!(report.demand.rows_emitted, 0);
        assert_eq!(report.demand.owned_rows_emitted, 0);
        assert_eq!(report.demand.borrowed_rows_emitted, 0);
        assert_eq!(report.demand.pages_read, 1);
        assert_eq!(report.demand.bytes_read, PAGE_BYTES);
        assert_eq!(report.demand.file_pages_read, 1);
        assert_eq!(report.demand.file_bytes_read, PAGE_BYTES);
        assert_eq!(report.demand.cache_misses, 1);
        assert_eq!(report.demand.cache_hits, 0);
    }
    assert_eq!(hydration.hydrated_rows, 0);
    assert_eq!(fixture.cache.snapshot().pinned_bytes, 0);
    fixture.remove();
}
