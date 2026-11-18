// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::{sql_timestamp, TimestampPrecision};

#[test]
fn timestamp_row_codec_round_trips_edges_and_rejects_out_of_range_payloads() {
    let (min, max) = (sql_timestamp::min_micros(), sql_timestamp::max_micros());
    for value in [
        RelationalValue::Timestamp(min.into()),
        RelationalValue::Timestamp((-1).into()),
        RelationalValue::Timestamp(max.into()),
        RelationalValue::TimestampTz(min.into()),
        RelationalValue::TimestampTz(0.into()),
        RelationalValue::TimestampTz(max.into()),
    ] {
        let row = RelationalRow::new(vec![value.clone()]);
        let encoded = encode_relational_row_payload(&row).unwrap();
        let decoded = decode_relational_row_payload(&encoded, 1, 64).unwrap();
        assert_eq!(decoded.values()[0], value);
        for length in 0..encoded.len() {
            assert!(decode_relational_row_payload(&encoded[..length], 1, 64).is_err());
        }
        let mut corrupt = encoded.clone();
        if encoded.len() >= 16 && value.estimated_payload_bytes() == 16 {
            let length = corrupt.len();
            corrupt[length - 8..].copy_from_slice(&u64::MAX.to_le_bytes());
        } else {
            let length = corrupt.len();
            corrupt[length - 8..].copy_from_slice(&i64::MIN.to_le_bytes());
        }
        assert!(matches!(
            decode_relational_row_payload(&corrupt, 1, 64),
            Err(RelationalError::Corruption(_))
        ));
    }
}

#[test]
fn timestamp_schema_codec_preserves_precision_and_rejects_invalid_precision() {
    let column = |name: &str, scalar_type| RelationalColumnSchema {
        name: name.into(),
        scalar_type,
        nullable: true,
        default: None,
    };
    let schema = RelationalTableSchema {
        name: "events".into(),
        columns: vec![
            column(
                "at",
                RelationalScalarType::Timestamp(TimestampPrecision::new(0).unwrap()),
            ),
            column(
                "seen",
                RelationalScalarType::TimestampTz(TimestampPrecision::DEFAULT),
            ),
            RelationalColumnSchema {
                default: Some(RelationalColumnDefault::Literal(
                    RelationalValue::Timestamp(1_000.into()),
                )),
                ..column(
                    "local",
                    RelationalScalarType::Timestamp(TimestampPrecision::new(3).unwrap()),
                )
            },
        ],
        primary_key: vec!["at".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: Vec::new(),
        materialized_view: None,
    };
    let mut encoder = Encoder::default();
    encoder.table_schema(&schema).unwrap();
    let encoded = encoder.finish();
    let mut decoder = Decoder::from_slice(&encoded, RelationalDecodeLimits::wal(), false);
    assert_eq!(decoder.table_schema().unwrap(), schema);
    decoder.finish().unwrap();

    // The first column's type is tag 9 followed by its precision byte.
    let position = encoded
        .windows(3)
        .position(|window| window == [b't', 9, 0])
        .expect("TIMESTAMP(0) type encoding")
        + 2;
    let mut corrupt = encoded;
    corrupt[position] = 7;
    let mut decoder = Decoder::from_slice(&corrupt, RelationalDecodeLimits::wal(), false);
    assert!(decoder
        .table_schema()
        .unwrap_err()
        .to_string()
        .contains("TIMESTAMP precision"));
}

#[test]
fn timestamp_carrier_keeps_relational_values_at_eight_byte_alignment() {
    assert!(std::mem::align_of::<RelationalValue>() <= std::mem::align_of::<u64>());
    assert!(
        std::mem::align_of::<crate::relational::RelationalValueRef<'static>>()
            <= std::mem::align_of::<u64>()
    );
}
