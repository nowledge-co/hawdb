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
use hawdb_core::VectorDimension;

fn schema(dimension: u32) -> RelationalTableSchema {
    RelationalTableSchema {
        materialized_view: None,
        name: "memories".into(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "embedding".into(),
                scalar_type: RelationalScalarType::Vector(VectorDimension::new(dimension).unwrap()),
                nullable: true,
                default: Some(RelationalColumnDefault::Literal(RelationalValue::Null)),
            },
        ],
        primary_key: vec!["id".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
    }
}

#[test]
fn scalar_tags_stay_stable_and_vector_dimensions_are_checked_on_decode() {
    for (kind, tag) in [
        (RelationalScalarType::Boolean, 1),
        (RelationalScalarType::BigInt, 2),
        (RelationalScalarType::DoublePrecision, 3),
        (RelationalScalarType::Text, 4),
        (RelationalScalarType::Bytea, 5),
        (RelationalScalarType::Uuid, 6),
    ] {
        let mut encoder = Encoder::default();
        encoder.scalar_type(kind);
        assert_eq!(encoder.finish(), [tag]);
    }
    let encoded = [7, 0, 4, 0, 0];
    let mut decoder = Decoder::from_slice(&encoded, RelationalDecodeLimits::wal(), true);
    assert_eq!(
        decoder.scalar_type().unwrap(),
        schema(1024).columns[1].scalar_type
    );
    decoder.finish().unwrap();
    for end in 0..encoded.len() {
        assert!(
            Decoder::from_slice(&encoded[..end], RelationalDecodeLimits::wal(), true)
                .scalar_type()
                .is_err()
        );
    }
    assert!(matches!(
        Decoder::from_slice(&[7, 0, 0, 0, 0], RelationalDecodeLimits::wal(), true).scalar_type(),
        Err(RelationalError::Corruption(_))
    ));
}

#[test]
fn schema_round_trip_preserves_maximum_dimension_without_payload_allocation() {
    for dimension in [1, 1024, u32::MAX] {
        let expected = schema(dimension);
        let encoded = encode_relational_table_schema(&expected).unwrap();
        assert!(encoded.len() < 256);
        assert_eq!(
            decode_relational_table_schema(&encoded, 256, 16).unwrap(),
            expected
        );
        for end in 0..encoded.len() {
            assert!(decode_relational_table_schema(&encoded[..end], 256, 16).is_err());
        }
    }
}

#[test]
fn shared_vector_value_codec_round_trip_and_malformed_payloads() {
    let value = RelationalValue::Vector(hawdb_core::Vector::new(&[1.0, -0.0]).unwrap());
    let mut encoder = Encoder::default();
    encoder.value(&value).unwrap();
    let encoded = encoder.finish();
    assert_eq!(
        encoded,
        [8, 12, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 128, 63, 0, 0, 0, 128]
    );
    let mut decoder = Decoder::from_slice(&encoded, RelationalDecodeLimits::wal(), true);
    assert_eq!(decoder.value().unwrap(), value);
    decoder.finish().unwrap();
    for end in 0..encoded.len() {
        assert!(
            Decoder::from_slice(&encoded[..end], RelationalDecodeLimits::wal(), true)
                .value()
                .is_err()
        );
    }
    for replacement in [0u32, u32::MAX] {
        let mut corrupt = encoded.clone();
        corrupt[9..13].copy_from_slice(&replacement.to_le_bytes());
        assert!(
            Decoder::from_slice(&corrupt, RelationalDecodeLimits::wal(), true)
                .value()
                .is_err()
        );
    }
    let mut corrupt = encoded;
    corrupt[13..17].copy_from_slice(&f32::NAN.to_le_bytes());
    assert!(
        Decoder::from_slice(&corrupt, RelationalDecodeLimits::wal(), true)
            .value()
            .is_err()
    );
}

#[test]
fn shared_vector_codec_validation_cancellation_is_typed_and_retryable() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
    use std::sync::{atomic::Ordering, Arc};
    let vector = hawdb_core::Vector::new(&vec![1.0; 32769]).unwrap();
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    for cut in 1..=3 {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        let result = decode_vector(
            vector.encoded().to_vec(),
            Some(&probe.context(local.clone())),
        );
        assert!(
            matches!(
                result,
                Err(RelationalError::Stopped(
                    hawdb_core::RuntimeCancellationReason::Cancelled
                ))
            ),
            "{result:?}"
        );
        probe.assert_released(&local);
    }
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        decode_vector(
            vector.encoded().to_vec(),
            Some(&retry.context(local.clone()))
        )
        .unwrap(),
        vector
    );
    retry.assert_released(&local);
}

#[test]
fn shared_vector_rejects_overflow_rows_defaults_and_durable_values() {
    let mut schema = schema(2);
    let reference = RelationalOverflowRef {
        digest: hawdb_integrity::integrity_digest(b"invalid vector overflow").sha256,
        scalar_type: schema.columns[1].scalar_type,
        compressed_bytes: 12,
        uncompressed_bytes: 12,
    };
    let value = RelationalValue::Overflow(reference);
    assert!(super::super::validate_value_type(&schema.columns[1], &value).is_err());
    assert!(Encoder::default().value(&value).is_err());
    schema.columns[1].default = Some(RelationalColumnDefault::Literal(value.clone()));
    assert!(encode_relational_table_schema(&schema).is_err());
    let work = crate::background::CheckpointWorkContext::default();
    assert!(checkpoint::encode_relational_table_schema_with_work_context(&schema, &work).is_err());
    assert!(checkpoint::encode_relational_row_payload_with_work_context(
        &RelationalRow::new(vec![value]),
        &work
    )
    .is_err());
    let mut malformed = vec![6];
    malformed.extend_from_slice(reference.digest.as_bytes());
    malformed.push(7);
    malformed.extend_from_slice(&2u32.to_le_bytes());
    malformed.extend_from_slice(&12u64.to_le_bytes());
    malformed.extend_from_slice(&12u64.to_le_bytes());
    assert!(matches!(
        Decoder::from_slice(&malformed, RelationalDecodeLimits::wal(), true).value(),
        Err(RelationalError::Corruption(_))
    ));
}
