// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn decimal_values_round_trip_through_every_canonical_path() {
    let values = [
        "0.0000",
        "-9007199254740993.0100",
        "99999999999999999999999999999999999999",
        "0.00000000000000000000000000000000000001",
    ]
    .map(|value| Value::Decimal(value.parse().unwrap()));
    for value in values.iter().cloned().chain([
        Value::List(values.to_vec()),
        Value::Map(BTreeMap::from([
            ("amounts".into(), Value::List(values.to_vec())),
            (
                "date".into(),
                Value::Date(hawdb_core::temporal::parse_date("2026-11-01").unwrap()),
            ),
        ])),
    ]) {
        let encoded = encode_standalone_value(&value).unwrap();
        let decoded = decode_standalone_value(&encoded).unwrap();
        assert_eq!(decoded, value);
        // Numeric equality alone would not detect a lost trailing scale.
        assert_eq!(encode_standalone_value(&decoded).unwrap(), encoded);
        assert_eq!(
            encoded_value_len_with_work(&value, 0, None).unwrap(),
            encoded.len() as u64
        );
        let mut streamed = Vec::new();
        write_value_streaming(&mut streamed, &value, 0).unwrap();
        assert_eq!(streamed, encoded);
        standalone_value_allocation_bytes(&encoded).unwrap();

        let mut followed = encoded;
        encode_value(&Value::Int(7), &mut followed, 0).unwrap();
        let mut cursor = SliceCursor::new(&followed);
        validate_encoded_value(&mut cursor, 0, None).unwrap();
        assert_eq!(decode_value(&mut cursor, 0).unwrap(), Value::Int(7));
        assert!(cursor.is_empty());
        let mut cursor = SliceCursor::new(&followed);
        encoded_value_allocation_bytes(&mut cursor, 0, None).unwrap();
        assert_eq!(decode_value(&mut cursor, 0).unwrap(), Value::Int(7));
        assert!(cursor.is_empty());
    }
}

#[test]
fn decimal_canonical_payloads_fail_closed_on_every_read_path() {
    let value = Value::Decimal("9007199254740993.0100".parse().unwrap());
    let encoded = encode_standalone_value(&value).unwrap();
    assert_eq!(encoded[0], 16);
    let mut corrupt = (0..encoded.len())
        .map(|end| encoded[..end].to_vec())
        .collect::<Vec<_>>();
    let mut invalid_scale = encoded.clone();
    invalid_scale[17] = 39;
    corrupt.push(invalid_scale);
    let mut invalid_coefficient = encoded;
    invalid_coefficient[1..17].copy_from_slice(&i128::MAX.to_le_bytes());
    corrupt.push(invalid_coefficient);
    for bytes in corrupt {
        assert!(decode_standalone_value(&bytes).is_err(), "{bytes:?}");
        assert!(
            standalone_value_allocation_bytes(&bytes).is_err(),
            "{bytes:?}"
        );
        assert!(
            validate_encoded_value(&mut SliceCursor::new(&bytes), 0, None).is_err(),
            "{bytes:?}"
        );
    }
}
