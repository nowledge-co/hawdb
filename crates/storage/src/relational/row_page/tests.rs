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
use hawdb_integrity::integrity_digest;

fn page_id(value: u64) -> RelationalRowPageId {
    RelationalRowPageId::new(NonZeroU64::new(value).unwrap())
}

fn overflow_reference() -> RelationalOverflowRef {
    RelationalOverflowRef {
        digest: integrity_digest(b"overflow payload").sha256,
        scalar_type: RelationalScalarType::Text,
        compressed_bytes: 120,
        uncompressed_bytes: 4096,
    }
}

fn page() -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation: 9,
        source_commit_epoch: 400,
        page_id: page_id(2),
        schema_digest: integrity_digest(b"documents schema").sha256,
        column_count: 7,
        rows: vec![
            RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::BigInt(1)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(1),
                    RelationalValue::Text("first".to_string()),
                    RelationalValue::Boolean(true),
                    RelationalValue::DoublePrecision(-0.0),
                    RelationalValue::Bytea(vec![0, 1, 2]),
                    RelationalValue::Null,
                    RelationalValue::Overflow(overflow_reference()),
                ]),
            },
            RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::BigInt(3)]),
                row: RelationalRow::new(vec![
                    RelationalValue::BigInt(3),
                    RelationalValue::Text("third".to_string()),
                    RelationalValue::Boolean(false),
                    RelationalValue::DoublePrecision(f64::NAN),
                    RelationalValue::Bytea(vec![9]),
                    RelationalValue::Null,
                    RelationalValue::Overflow(overflow_reference()),
                ]),
            },
        ],
    }
}

#[test]
fn row_page_round_trip_is_exact() {
    let page = page();
    let limits = RelationalRowPageLimits::default();
    let encoded = page.encode(limits).unwrap();
    assert_eq!(
        integrity_digest(&encoded).sha256.to_string(),
        "186ba67d354fc8a0b9a5a5d9d08c86ce1aa0d2252516181faedb02d52fe23987"
    );
    assert_eq!(
        ImmutableRelationalRowPage::decode(&encoded, limits).unwrap(),
        page
    );

    let slot = page.encode_slot(limits).unwrap();
    assert_eq!(slot.len(), limits.max_page_bytes.get());
    assert_eq!(
        ImmutableRelationalRowPage::decode_slot(&slot, limits).unwrap(),
        page
    );
}

#[test]
fn decimal_row_page_preserves_raw_scale_and_uses_numeric_key_identity() {
    let decimal = "9007199254740993.0100"
        .parse::<hawdb_core::Decimal>()
        .unwrap();
    let page = ImmutableRelationalRowPage {
        generation: 1,
        source_commit_epoch: 1,
        page_id: page_id(1),
        schema_digest: integrity_digest(b"decimal schema").sha256,
        column_count: 2,
        rows: vec![RelationalRowPageEntry {
            primary_key: RelationalKey(vec![RelationalValue::Decimal(decimal)]),
            row: RelationalRow::new(vec![
                RelationalValue::Decimal(decimal),
                RelationalValue::Decimal("0.0000".parse().unwrap()),
            ]),
        }],
    };
    let limits = RelationalRowPageLimits::default();
    let encoded = page.encode(limits).unwrap();
    let decoded = ImmutableRelationalRowPage::decode(&encoded, limits).unwrap();
    let RelationalValue::Decimal(value) = decoded.rows[0].row.values()[0] else {
        panic!("native Decimal");
    };
    assert_eq!(value.to_le_bytes(), decimal.to_le_bytes());
    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    let key = RelationalKey(vec![RelationalValue::Decimal(
        "9007199254740993.01".parse().unwrap(),
    )]);
    assert_eq!(view.find_row(&key).unwrap(), Some(0));
    let projected = view.find_projected_row(&key, &[1]).unwrap().unwrap();
    let RelationalValue::Decimal(zero) = projected.fields[0].value else {
        panic!("native Decimal");
    };
    assert_eq!(zero.to_string(), "0.0000");
}

#[test]
fn view_binary_searches_keys_and_decodes_only_requested_fields() {
    let page = page();
    let limits = RelationalRowPageLimits::default();
    let encoded = page.encode(limits).unwrap();
    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    assert_eq!(view.generation(), 9);
    assert_eq!(view.source_commit_epoch(), 400);
    assert_eq!(view.page_id(), page_id(2));
    assert_eq!(view.schema_digest(), page.schema_digest);
    assert_eq!(view.row_count(), 2);
    assert_eq!(view.column_count(), 7);

    let projected = view
        .find_projected_row(&RelationalKey(vec![RelationalValue::BigInt(3)]), &[0, 6])
        .unwrap()
        .unwrap();
    assert_eq!(
        projected.primary_key,
        RelationalKey(vec![RelationalValue::BigInt(3)])
    );
    assert_eq!(
        projected.fields,
        vec![
            RelationalProjectedField {
                ordinal: 0,
                value: RelationalValue::BigInt(3),
            },
            RelationalProjectedField {
                ordinal: 6,
                value: RelationalValue::Overflow(overflow_reference()),
            },
        ]
    );
    assert!(view
        .find_projected_row(&RelationalKey(vec![RelationalValue::BigInt(2)]), &[0])
        .unwrap()
        .is_none());
}

#[test]
fn projected_decode_does_not_materialize_unrequested_large_inline_values() {
    let page = ImmutableRelationalRowPage {
        generation: 2,
        source_commit_epoch: 900,
        page_id: page_id(1),
        schema_digest: integrity_digest(b"large value schema").sha256,
        column_count: 2,
        rows: vec![RelationalRowPageEntry {
            primary_key: RelationalKey(vec![RelationalValue::BigInt(1)]),
            row: RelationalRow::new(vec![
                RelationalValue::BigInt(1),
                RelationalValue::Text("x".repeat(60 * 1024)),
            ]),
        }],
    };
    let limits = RelationalRowPageLimits::default();
    let encoded = page.encode(limits).unwrap();
    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    let projected = view.decode_projected_row(0, &[0]).unwrap();
    assert_eq!(
        projected.fields,
        vec![RelationalProjectedField {
            ordinal: 0,
            value: RelationalValue::BigInt(1),
        }]
    );
}

#[test]
fn lending_projected_cursor_borrows_variable_values_and_matches_owned_decode() {
    let page = page();
    let limits = RelationalRowPageLimits::default();
    let encoded = page.encode(limits).unwrap();
    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    let requested = [0, 1, 2, 3, 4, 5, 6];
    let expected = (0..view.row_count())
        .map(|ordinal| view.decode_projected_row(ordinal, &requested).unwrap())
        .collect::<Vec<_>>();
    let mut cursor = ProjectedRowPageCursor::new(view, 0, &requested).unwrap();

    let first_fields_ptr = {
        let row = cursor.next_row().unwrap().unwrap();
        assert_eq!(row.primary_key(), &page.rows[0].primary_key);
        assert_eq!(row.value(0), Some(RelationalValueRef::BigInt(1)));
        assert_eq!(row.value(1), Some(RelationalValueRef::Text("first")));
        assert_eq!(row.value(4), Some(RelationalValueRef::Bytea(&[0, 1, 2])));
        assert_eq!(row.value(5), Some(RelationalValueRef::Null));
        assert_eq!(
            row.value(6),
            Some(RelationalValueRef::Overflow(overflow_reference()))
        );
        assert_eq!(row.to_owned_row(), expected[0]);
        let text = match row.value(1).unwrap() {
            RelationalValueRef::Text(value) => value,
            value => panic!("expected borrowed TEXT, got {value:?}"),
        };
        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();
        assert!((encoded_start..encoded_end).contains(&(text.as_ptr() as usize)));
        row.fields().as_ptr() as usize
    };

    {
        let row = cursor.next_row().unwrap().unwrap();
        assert_eq!(row.primary_key(), &page.rows[1].primary_key);
        assert_eq!(row.value(1), Some(RelationalValueRef::Text("third")));
        assert_eq!(row.fields().as_ptr() as usize, first_fields_ptr);
        assert_eq!(row.to_owned_row(), expected[1]);
    }
    assert!(cursor.next_row().unwrap().is_none());
}

#[test]
fn encoder_rejects_invalid_shape_order_and_budget() {
    let limits = RelationalRowPageLimits::default();
    let mut empty = page();
    empty.rows.clear();
    assert!(matches!(
        empty.encode(limits),
        Err(RelationalRowPageError::Admission(_))
    ));

    let mut unordered = page();
    unordered.rows.swap(0, 1);
    assert!(matches!(
        unordered.encode(limits),
        Err(RelationalRowPageError::Admission(message))
            if message.contains("strictly increasing")
    ));

    let mut wrong_columns = page();
    wrong_columns.rows[0].row = RelationalRow::new(vec![RelationalValue::BigInt(1)]);
    assert!(matches!(
        wrong_columns.encode(limits),
        Err(RelationalRowPageError::Admission(message))
            if message.contains("expected 7")
    ));

    let small = RelationalRowPageLimits {
        max_page_bytes: NonZeroUsize::new(256).unwrap(),
        ..limits
    };
    assert!(matches!(
        page().encode(small),
        Err(RelationalRowPageError::Admission(message))
            if message.contains("exceeding limit")
    ));
}

#[test]
fn decoder_rejects_truncation_checksum_and_slot_corruption() {
    let limits = RelationalRowPageLimits::default();
    let mut encoded = page().encode(limits).unwrap();
    encoded.pop();
    assert!(ImmutableRelationalRowPage::decode(&encoded, limits).is_err());

    let mut encoded = page().encode(limits).unwrap();
    *encoded.last_mut().unwrap() ^= 0x80;
    assert!(matches!(
        RelationalRowPageView::open(&encoded, limits),
        Err(RelationalRowPageError::Corrupt(message))
            if message == "row page checksum mismatch"
    ));

    let mut encoded = page().encode(limits).unwrap();
    let lower_len = read_u32(&encoded[44..48]) as usize;
    let upper_len = read_u32(&encoded[48..52]) as usize;
    let directory_start = ROW_PAGE_HEADER_BYTES + lower_len + upper_len;
    encoded[directory_start..directory_start + 4].copy_from_slice(&1u32.to_le_bytes());
    write_integrity(&mut encoded);
    assert!(matches!(
        RelationalRowPageView::open(&encoded, limits),
        Err(RelationalRowPageError::Corrupt(message))
            if message.contains("not contiguous")
    ));
}

#[test]
fn decoder_rejects_false_key_bounds_and_non_zero_slot_tail() {
    let limits = RelationalRowPageLimits::default();
    let mut encoded = page().encode(limits).unwrap();
    encoded[ROW_PAGE_HEADER_BYTES] ^= 1;
    write_integrity(&mut encoded);
    assert!(matches!(
        RelationalRowPageView::open(&encoded, limits),
        Err(RelationalRowPageError::Corrupt(message))
            if message.contains("bounds")
    ));

    let mut slot = page().encode_slot(limits).unwrap();
    *slot.last_mut().unwrap() = 1;
    assert!(matches!(
        RelationalRowPageView::open_slot(&slot, limits),
        Err(RelationalRowPageError::Corrupt(message))
            if message.contains("non-zero trailing bytes")
    ));
}

#[test]
fn full_and_projected_decode_reject_invalid_selected_row_values() {
    let limits = RelationalRowPageLimits::default();
    let mut encoded = page().encode(limits).unwrap();
    let header = decode_header(&encoded, limits).unwrap();
    let directory_start = ROW_PAGE_HEADER_BYTES + header.lower_bound_len + header.upper_bound_len;
    let key_start = directory_start + header.directory_len;
    let row_start = key_start + header.key_payload_len;
    let first_slot =
        RowSlot::decode(&encoded[directory_start..directory_start + ROW_SLOT_BYTES]).unwrap();
    let first_row_start = row_start + first_slot.row_offset as usize;
    let first_value_payload_start = first_row_start + 4 + 7 * VALUE_SLOT_BYTES;
    encoded[first_value_payload_start] = 99;
    write_integrity(&mut encoded);

    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    assert!(matches!(
        view.decode_projected_row(0, &[6]),
        Err(RelationalRowPageError::Corrupt(message))
            if message.contains("invalid relational row value tag")
    ));
    assert!(ImmutableRelationalRowPage::decode(&encoded, limits).is_err());
}

#[test]
fn requested_field_contract_is_bounded_and_unambiguous() {
    let limits = RelationalRowPageLimits {
        max_requested_fields: NonZeroUsize::new(2).unwrap(),
        ..RelationalRowPageLimits::default()
    };
    let encoded = page().encode(limits).unwrap();
    let view = RelationalRowPageView::open(&encoded, limits).unwrap();
    for fields in [&[0, 0][..], &[3, 2][..], &[0, 1, 2][..], &[7][..]] {
        assert!(matches!(
            view.decode_projected_row(0, fields),
            Err(RelationalRowPageError::Admission(_))
        ));
    }
}

#[test]
fn overflow_descriptor_validation_is_symmetric() {
    let limits = RelationalRowPageLimits::default();
    let mut invalid_length = page();
    invalid_length.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Text("first".to_string()),
        RelationalValue::Boolean(true),
        RelationalValue::DoublePrecision(-0.0),
        RelationalValue::Bytea(vec![]),
        RelationalValue::Null,
        RelationalValue::Overflow(RelationalOverflowRef {
            compressed_bytes: 0,
            ..overflow_reference()
        }),
    ]);
    assert!(matches!(
        invalid_length.encode(limits),
        Err(RelationalRowPageError::Admission(message))
            if message.contains("compressed")
    ));

    let mut invalid_type = page();
    invalid_type.rows[0].row = RelationalRow::new(vec![
        RelationalValue::BigInt(1),
        RelationalValue::Text("first".to_string()),
        RelationalValue::Boolean(true),
        RelationalValue::DoublePrecision(-0.0),
        RelationalValue::Bytea(vec![]),
        RelationalValue::Null,
        RelationalValue::Overflow(RelationalOverflowRef {
            scalar_type: RelationalScalarType::BigInt,
            ..overflow_reference()
        }),
    ]);
    assert!(matches!(
        invalid_type.encode(limits),
        Err(RelationalRowPageError::Admission(message))
            if message.contains("non-payload")
    ));
}

#[test]
fn controlled_projected_decode_admits_selected_values_and_primary_keys_before_copy() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    for (primary_key, payload, retained_bytes) in [
        (
            RelationalKey(vec![RelationalValue::BigInt(1)]),
            "v".repeat(128 * 1024),
            128 * 1024,
        ),
        (
            RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]),
            "small".into(),
            128 * 1024,
        ),
        (
            RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]),
            "v".repeat(128 * 1024),
            256 * 1024,
        ),
    ] {
        let limits = RelationalRowPageLimits {
            max_key_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
            max_inline_value_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
            ..Default::default()
        };
        let page = ImmutableRelationalRowPage {
            generation: 2,
            source_commit_epoch: 900,
            page_id: page_id(1),
            schema_digest: integrity_digest(b"controlled projected row").sha256,
            column_count: 2,
            rows: vec![RelationalRowPageEntry {
                primary_key,
                row: RelationalRow::new(vec![
                    RelationalValue::Text(payload),
                    RelationalValue::Bytea(vec![7; 128 * 1024]),
                ]),
            }],
        };
        let encoded = page.encode(limits).unwrap();
        let view = RelationalRowPageView::open(&encoded, limits).unwrap();
        let ordinary = view.decode_projected_row(0, &[0]).unwrap();
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
            let observation = crate::test_allocator::AllocationObservation::start();
            let result = work.classify(|scoped| {
                let context = CheckpointDecodeContext {
                    work: scoped.clone(),
                    memory: Default::default(),
                };
                let row = view.decode_projected_row_with_allocations(0, &[0], Some(&context))?;
                Ok::<_, RelationalRowPageError>((row, context.memory.into_inner()))
            });
            let wide_allocations = observation.finish();
            if succeeds {
                let (row, memory) = result.unwrap();
                assert_eq!(row, ordinary);
                assert_eq!(
                    wide_allocations,
                    retained_bytes / (128 * 1024),
                    "unrequested BYTEA must not be copied"
                );
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling),
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                        if available_bytes <= ceiling - retained_bytes as u64)
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
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            drop(work);
            drop(permit);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }
}

#[test]
fn admitted_owned_and_borrowed_row_copies_preserve_all_scalar_variants_and_leases() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let row = RelationalProjectedRow {
        primary_key: RelationalKey(vec![RelationalValue::Text("k".repeat(128 * 1024))]),
        fields: vec![
            RelationalValue::Null,
            RelationalValue::Boolean(true),
            RelationalValue::BigInt(-7),
            RelationalValue::DoublePrecision(1.5),
            RelationalValue::Decimal("-1.2300".parse().unwrap()),
            RelationalValue::Text("中".repeat(64 * 1024)),
            RelationalValue::Bytea(vec![7; 128 * 1024]),
            RelationalValue::Uuid(hawdb_core::Uuid::from_bytes([3; 16])),
            RelationalValue::Overflow(overflow_reference()),
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, value)| RelationalProjectedField { ordinal, value })
        .collect(),
    };
    let fields = row
        .fields
        .iter()
        .map(|field| RelationalProjectedFieldRef {
            ordinal: field.ordinal,
            value: field.value.as_ref(),
        })
        .collect::<Vec<_>>();
    for view in [
        RelationalProjectedRowView::Owned(&row),
        RelationalProjectedRowView::Borrowed(RelationalProjectedRowRef {
            primary_key: &row.primary_key,
            fields: &fields,
        }),
    ] {
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
            let observation = crate::test_allocator::AllocationObservation::start();
            let result = work.classify(|scoped| {
                let context = CheckpointDecodeContext {
                    work: scoped.clone(),
                    memory: Default::default(),
                };
                let copied = view.to_owned_row_with_allocations(&context)?;
                Ok::<_, super::super::RelationalError>((copied, context.memory.into_inner()))
            });
            let wide_allocations = observation.finish();
            if succeeds {
                let (copied, memory) = result.unwrap();
                assert_eq!(copied, row);
                assert!(
                    matches!(work.task_context().reserve_working_memory(ceiling),
                    Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                        if available_bytes <= ceiling - 448 * 1024)
                );
                drop(copied);
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
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
            );
            drop(work);
            drop(permit);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }
}

#[test]
fn projected_row_conversion_admits_value_vector_and_shared_owner_before_allocation() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let make_row = || RelationalProjectedRow {
        primary_key: RelationalKey(vec![RelationalValue::BigInt(1)]),
        fields: (0..4096)
            .map(|ordinal| RelationalProjectedField {
                ordinal,
                value: RelationalValue::BigInt(ordinal as i64),
            })
            .collect(),
    };
    let expected = make_row()
        .into_relational_row_with_allocations(None)
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
        let projected = make_row();
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let row = projected.into_relational_row_with_allocations(Some(&context))?;
            Ok::<_, super::super::RelationalError>((row, context.memory.into_inner()))
        });
        let wide_allocations = observation.finish();
        if succeeds {
            let (row, memory) = result.unwrap();
            assert_eq!(row, expected);
            let backing_bytes = (4096 * std::mem::size_of::<RelationalValue>()) as u64;
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes <= ceiling - backing_bytes)
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
fn admitted_row_encoding_matches_wire_and_releases_slots_and_payload_scratch() {
    use crate::background::{
        CheckpointDecodeContext, CheckpointOperationError, CheckpointWorkContext,
        CheckpointWorkError,
    };
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeTaskContext,
        RuntimeWorkRequest,
    };
    let row = RelationalRow::new(vec![
        RelationalValue::Null,
        RelationalValue::Boolean(true),
        RelationalValue::BigInt(-9),
        RelationalValue::DoublePrecision(1.25),
        RelationalValue::Text("中".repeat(64 * 1024)),
        RelationalValue::Bytea(vec![7; 128 * 1024]),
        RelationalValue::Uuid(hawdb_core::Uuid::from_bytes([4; 16])),
        RelationalValue::Overflow(overflow_reference()),
    ]);
    let limits = RelationalRowPageLimits {
        max_inline_value_bytes: NonZeroUsize::new(256 * 1024).unwrap(),
        ..Default::default()
    };
    let expected =
        value::encode_row_with_allocations(&row, row.values().len(), limits, None).unwrap();
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
        let observation = crate::test_allocator::AllocationObservation::start();
        let result = work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let bytes = value::encode_row_with_allocations(
                &row,
                row.values().len(),
                limits,
                Some(&context),
            )?;
            Ok::<_, RelationalRowPageError>((bytes, context.memory.into_inner()))
        });
        let wide_allocations = observation.finish();
        if succeeds {
            let (bytes, memory) = result.unwrap();
            assert_eq!(bytes, expected);
            assert!(
                matches!(work.task_context().reserve_working_memory(ceiling),
                Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. })
                    if available_bytes <= ceiling - bytes.len() as u64 && available_bytes > ceiling - bytes.len() as u64 - 16 * 1024)
            );
            drop(bytes);
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
        assert!(
            matches!(work.task_context().reserve_working_memory(ceiling),
            Err(hawdb_core::RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
