// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn decimal_row_codec_preserves_scale_and_rejects_malformed_scalars() {
    for text in [
        "0.0000",
        "-1.2300",
        "9007199254740993.01",
        "99999999999999999999999999999999999999",
    ] {
        let decimal: hawdb_core::Decimal = text.parse().unwrap();
        let row = RelationalRow::new(vec![RelationalValue::Decimal(decimal)]);
        let encoded = encode_relational_row_payload(&row).unwrap();
        let decoded = decode_relational_row_payload(&encoded, 1, 64).unwrap();
        let RelationalValue::Decimal(actual) = decoded.values()[0] else {
            panic!("Decimal row");
        };
        assert_eq!(actual.to_le_bytes(), decimal.to_le_bytes());
        let mut corrupt = encoded.clone();
        *corrupt.last_mut().unwrap() = 39;
        assert!(decode_relational_row_payload(&corrupt, 1, 64).is_err());
        let mut corrupt = encoded.clone();
        let coefficient = corrupt.len() - 17;
        corrupt[coefficient..coefficient + 16].copy_from_slice(&i128::MAX.to_le_bytes());
        assert!(decode_relational_row_payload(&corrupt, 1, 64).is_err());
        for length in 0..encoded.len() {
            assert!(decode_relational_row_payload(&encoded[..length], 1, 64).is_err());
        }
    }
}

#[test]
fn decimal_numeric_wal_expression_is_bounded_and_preserves_cast_metadata() {
    use super::super::RelationalNumericExpression as N;
    use hawdb_core::numeric::{NumericBinaryOp, NumericType};
    let expression = N::Cast {
        expression: Box::new(N::Binary {
            left: Box::new(N::Column("amount".into())),
            op: NumericBinaryOp::Multiply,
            right: Box::new(N::Value(RelationalValue::Decimal("1.200".parse().unwrap()))),
        }),
        target: NumericType::Decimal(hawdb_core::DecimalType::new(20, 4).unwrap()),
    };
    let mut encoder = Encoder::default();
    numeric::encode(&mut encoder, &expression).unwrap();
    let encoded = encoder.finish();
    let mut decoder = Decoder::from_slice(&encoded, RelationalDecodeLimits::wal(), false);
    assert_eq!(decoder.numeric_expression().unwrap(), expression);
    decoder.finish().unwrap();
    let invalid = [3_u8; 66];
    let mut decoder = Decoder::from_slice(&invalid, RelationalDecodeLimits::wal(), false);
    assert!(decoder
        .numeric_expression()
        .unwrap_err()
        .to_string()
        .contains("64 levels"));
    let mut invalid_cast = encoded;
    invalid_cast[2] = 0;
    let mut decoder = Decoder::from_slice(&invalid_cast, RelationalDecodeLimits::wal(), false);
    assert!(decoder.numeric_expression().is_err());
}
