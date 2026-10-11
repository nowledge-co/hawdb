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
use crate::background::CheckpointWorkProbe;
use crate::relational::RelationalHydrationBudget;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

fn stopped<T: std::fmt::Debug>(result: Result<T, AppendTableError>) {
    assert!(
        matches!(&result, Err(AppendTableError::Admission(message))
        if message.contains("checkpoint build stopped") && message.contains("cancelled")),
        "{result:?}"
    );
}

fn rows(count: i64) -> Vec<AppendTableRow> {
    let mut rows = (0..count)
        .map(|sequence| {
            let table = if sequence % 2 == 0 { "a🦀" } else { "z" };
            let partition = format!("分区{}\0", sequence % 7);
            let payload = (0..1025)
                .map(|offset| ((offset * 31 + sequence * 17 + offset / 7) % 251) as u8)
                .collect();
            AppendTableRow {
                table: table.into(),
                partition_key: RelationalKey(vec![RelationalValue::Text(partition.clone())]),
                order_key: RelationalKey(vec![RelationalValue::BigInt(sequence)]),
                row: RelationalRow::new(vec![
                    RelationalValue::Text(partition),
                    RelationalValue::BigInt(sequence),
                    RelationalValue::Bytea(payload),
                    RelationalValue::Text("x🦀".repeat(2048)),
                ]),
            }
        })
        .collect::<Vec<_>>();
    rows.sort_by(compare_rows);
    rows
}

#[test]
fn checkpoint_units_append_segment_matches_complete_legacy_bytes_and_reopen() {
    let rows = rows(2049);
    let config = AppendSegmentConfig {
        target_decoded_block_bytes: 16 * 1024,
        ..AppendSegmentConfig::default()
    };
    let expected = AppendSegmentWriter::encode(9, 77, &rows, config).unwrap();
    assert!(expected.descriptors.len() > 128);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = encode(9, 77, &rows, config, &probe.context(scheduler.clone())).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    assert!(probe.completed.load(Ordering::SeqCst) > rows.len() * 10);
    probe.assert_released(&scheduler);
    let reader = AppendSegmentReader::open(actual.encoded, config).unwrap();
    let mut reopened = Vec::new();
    for table in ["a🦀", "z"] {
        for partition in 0..7 {
            let key = RelationalKey(vec![RelationalValue::Text(format!("分区{partition}\0"))]);
            let result = reader
                .read_partition_bounded(table, &key, None, rows.len(), 64 * 1024 * 1024)
                .unwrap();
            reopened.extend(result.rows);
        }
    }
    reopened.sort_by(compare_rows);
    assert_eq!(reopened, rows);
    let total = probe.completed.load(Ordering::SeqCst);
    for boundary in [
        17,
        rows.len() + 17,
        total / 4,
        total / 2,
        total * 3 / 4,
        total - 1,
        total,
    ] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        stopped(encode(
            9,
            77,
            &rows,
            config,
            &probe.context(scheduler.clone()),
        ));
        assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
        probe.assert_released(&scheduler);
    }
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        encode(9, 77, &rows, config, &retry.context(scheduler.clone())).unwrap(),
        expected
    );
    retry.assert_released(&scheduler);
}

#[test]
fn checkpoint_units_append_row_and_ordered_keys_preserve_large_values_and_errors() {
    let text = format!("{}🦀\0{}", "a".repeat(65535), "z".repeat(131073));
    let bytes = (0..4 * 64 * 1024 + 1)
        .map(|offset| (offset % 256) as u8)
        .collect::<Vec<_>>();
    let values = vec![
        RelationalValue::Null,
        RelationalValue::Boolean(true),
        RelationalValue::BigInt(i64::MIN),
        RelationalValue::DoublePrecision(-0.0),
        RelationalValue::DoublePrecision(f64::INFINITY),
        RelationalValue::Text(text),
        RelationalValue::Bytea(bytes),
        RelationalValue::Uuid(hawdb_core::Uuid::from_bytes([7; 16])),
    ];
    let row = RelationalRow::new(values.clone());
    let source_key = RelationalKey(values);
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    assert_eq!(
        crate::relational::encode_relational_row_payload_with_work_context(&row, &work).unwrap(),
        encode_relational_row_payload(&row).unwrap()
    );
    assert_eq!(
        key(&source_key, 4 * 1024 * 1024, &work).unwrap(),
        encode_append_key(&source_key, 4 * 1024 * 1024).unwrap()
    );
    assert_eq!(
        key(&source_key, 10, &work).unwrap_err(),
        encode_append_key(&source_key, 10).unwrap_err()
    );
    assert_eq!(
        key(&RelationalKey(Vec::new()), 1, &work).unwrap(),
        vec![GLOBAL_PARTITION_TAG]
    );
    probe.assert_released(&scheduler);
    let variable = RelationalRow::new(vec![RelationalValue::Bytea(vec![7; 8 * 64 * 1024])]);
    for boundary in [2, 3, 5] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        stopped(
            crate::relational::encode_relational_row_payload_with_work_context(
                &variable,
                &probe.context(scheduler.clone()),
            )
            .map_err(map_relational),
        );
        probe.assert_released(&scheduler);
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(boundary, Ordering::SeqCst);
        stopped(key(
            &RelationalKey(vec![RelationalValue::Text("x".repeat(8 * 64 * 1024))]),
            1024 * 1024,
            &probe.context(scheduler.clone()),
        ));
        probe.assert_released(&scheduler);
    }
    let empty = RelationalKey(Vec::new());
    let work = CheckpointWorkContext::default();
    assert_eq!(
        crate::relational::encode_relational_primary_key_with_work_context(&empty, &work)
            .unwrap_err(),
        encode_relational_primary_key(&empty).unwrap_err()
    );
}

#[test]
fn checkpoint_units_append_overflow_preserves_raw_compressed_closure_and_each_cancel() {
    let mut seed = 0x1357_2468_u64;
    let random = (0..8 * 64 * 1024 + 1)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect::<Vec<_>>();
    let fixtures = [
        (RelationalScalarType::Bytea, random),
        (
            RelationalScalarType::Text,
            "🦀a".repeat(110000).into_bytes(),
        ),
        (RelationalScalarType::Bytea, vec![0; 4096]),
    ];
    let scheduler = scheduler();
    for (scalar, raw) in fixtures {
        let config = RelationalOverflowConfig::default();
        let expected = encode_overflow_envelope(scalar, &raw, config).unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let work = probe.context(scheduler.clone());
        let actual = crate::relational::overflow::encode_overflow_envelope_with_work_context(
            scalar, &raw, config, &work,
        )
        .unwrap();
        assert_eq!(actual, expected);
        let decoded = decode_overflow_envelope(
            &actual.reference,
            &actual.bytes,
            &mut RelationalHydrationBudget::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            decoded,
            match scalar {
                RelationalScalarType::Text =>
                    RelationalValue::Text(String::from_utf8(raw.clone()).unwrap()),
                _ => RelationalValue::Bytea(raw.clone()),
            }
        );
        let total = probe.completed.load(Ordering::SeqCst);
        assert!(total > 5);
        probe.assert_released(&scheduler);
        for boundary in 1..=total {
            let probe = Arc::new(CheckpointWorkProbe::default());
            probe.cancel_after.store(boundary, Ordering::SeqCst);
            stopped(
                crate::relational::overflow::encode_overflow_envelope_with_work_context(
                    scalar,
                    &raw,
                    config,
                    &probe.context(scheduler.clone()),
                )
                .map_err(map_relational),
            );
            assert_eq!(probe.completed.load(Ordering::SeqCst), boundary);
            probe.assert_released(&scheduler);
        }
    }
    for (scalar, raw, config) in [
        (
            RelationalScalarType::Boolean,
            vec![1],
            RelationalOverflowConfig::default(),
        ),
        (
            RelationalScalarType::Bytea,
            Vec::new(),
            RelationalOverflowConfig::default(),
        ),
        (
            RelationalScalarType::Bytea,
            vec![1; 17],
            RelationalOverflowConfig {
                max_value_bytes: 16,
                ..RelationalOverflowConfig::default()
            },
        ),
    ] {
        assert_eq!(
            crate::relational::overflow::encode_overflow_envelope_with_work_context(
                scalar,
                &raw,
                config,
                &CheckpointWorkContext::default()
            )
            .unwrap_err(),
            encode_overflow_envelope(scalar, &raw, config).unwrap_err()
        );
    }
}

#[test]
fn checkpoint_units_append_block_directory_compression_and_copy_cancel_then_retry() {
    let rows = rows(97);
    let config = AppendSegmentConfig::default();
    let scheduler = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(scheduler.clone());
    let partition_end = rows
        .iter()
        .position(|row| row.table != rows[0].table || row.partition_key != rows[0].partition_key)
        .unwrap();
    assert_eq!(
        encode_block(&rows, 0, partition_end, config, &work).unwrap(),
        encode_next_block(&rows, 0, partition_end, config).unwrap()
    );
    let output = AppendSegmentWriter::encode(9, 77, &rows, config).unwrap();
    assert_eq!(
        directory(&output.descriptors, config, &work).unwrap(),
        encode_directory(&output.descriptors, config).unwrap()
    );
    let large = vec![7; 8 * 64 * 1024 + 1];
    assert_eq!(
        compress(&large, 3, &work).unwrap(),
        zstd::stream::encode_all(large.as_slice(), 3).unwrap()
    );
    assert!(bytes_equal(&large, &large, &work).unwrap());
    let mut altered = large.clone();
    *altered.last_mut().unwrap() = 8;
    assert!(!bytes_equal(&large, &altered, &work).unwrap());
    probe.assert_released(&scheduler);
    for phase in 0..5 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(2, Ordering::SeqCst);
        let work = probe.context(scheduler.clone());
        let result = match phase {
            0 => compress(&large, 3, &work).map(|_| ()),
            1 => copy_bytes(&mut Vec::new(), &large, &work),
            2 => integrity_slices(&[&large], &work).map(|_| ()),
            3 => directory(&output.descriptors, config, &work).map(|_| ()),
            _ => bytes_equal(&large, &large, &work).map(|_| ()),
        };
        stopped(result);
        assert_eq!(probe.completed.load(Ordering::SeqCst), 2);
        probe.assert_released(&scheduler);
    }
    for limit in [0, 1] {
        let bad = AppendSegmentConfig {
            max_rows: limit,
            ..config
        };
        assert_eq!(
            encode(9, 77, &rows, bad, &CheckpointWorkContext::default()).unwrap_err(),
            AppendSegmentWriter::encode(9, 77, &rows, bad).unwrap_err()
        );
    }
    assert_eq!(
        encode(9, 77, &[], config, &CheckpointWorkContext::default()).unwrap(),
        AppendSegmentWriter::encode(9, 77, &[], config).unwrap()
    );
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(1, Ordering::SeqCst);
    stopped(key(
        &RelationalKey(Vec::new()),
        1,
        &probe.context(scheduler.clone()),
    ));
    probe.assert_released(&scheduler);
    let probe = Arc::new(CheckpointWorkProbe::default());
    probe.cancel_after.store(1, Ordering::SeqCst);
    stopped(bytes_equal(&[0], &[1], &probe.context(scheduler.clone())));
    probe.assert_released(&scheduler);
    let unordered = vec![rows[1].clone(), rows[0].clone()];
    assert_eq!(
        encode(9, 77, &unordered, config, &CheckpointWorkContext::default()).unwrap_err(),
        AppendSegmentWriter::encode(9, 77, &unordered, config).unwrap_err()
    );
}
