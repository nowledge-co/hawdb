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

use super::{Decimal, DecimalError, DecimalType};
use num_bigint::BigInt;
use num_traits::Signed;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn d(value: &str) -> Decimal {
    value.parse().unwrap()
}

#[test]
fn exact_literals_preserve_digits_and_bound_work() {
    for text in [
        "0.00",
        "-0.001",
        "9007199254740993.01",
        "99999999999999999999999999999999999999",
        "0.00000000000000000000000000000000000001",
    ] {
        assert_eq!(d(text).to_string(), text);
    }
    for (text, expected) in [
        (".1", "0.1"),
        ("1.", "1"),
        ("+01.00e+1", "10.0"),
        ("1e37", "10000000000000000000000000000000000000"),
        ("100e-40", "0.00000000000000000000000000000000000001"),
    ] {
        assert_eq!(d(text).to_string(), expected);
    }
    for text in [
        "", ".", "+", "-", " 1", "1 ", "NaN", "Inf", "1e", "1e1e1", "1.2.3", "1e38", "1e-39",
        "1e32768", "1e-32768",
    ] {
        assert!(text.parse::<Decimal>().is_err(), "{text}");
    }
    assert!("0".repeat(257).parse::<Decimal>().is_err());
    assert_eq!(d("-0.0"), Decimal::ZERO);
    assert_eq!(d("000.00100").to_string(), "0.00100");
}

#[test]
fn arithmetic_rejects_loss_but_allows_large_intermediate_cancellation() {
    assert_eq!(d("0.1").checked_add(d("0.2")).unwrap(), d("0.3"));
    assert_eq!(
        d("9007199254740993.01").checked_sub(d("0.01")).unwrap(),
        d("9007199254740993")
    );
    assert_eq!(d("-7.5").checked_rem(d("2")).unwrap(), d("-1.5"));
    let one = Decimal::new(10_i128.pow(37), 37).unwrap();
    assert_eq!(one.checked_mul(one).unwrap(), d("1"));
    let max = Decimal::new(Decimal::MAX_COEFFICIENT, 0).unwrap();
    assert_eq!(max.checked_sub(max).unwrap(), Decimal::ZERO);
    assert_eq!(max.checked_add(d("0.1")), Err(DecimalError::Overflow));
    assert_eq!(
        d("1e-38").checked_mul(d("1e-38")),
        Err(DecimalError::Overflow)
    );
    assert_eq!(
        d("1").checked_rem(Decimal::ZERO),
        Err(DecimalError::DivisionByZero)
    );
}

#[test]
fn division_rounds_once_at_precision_or_scale_boundary() {
    assert_eq!(
        d("1").checked_div(d("3")).unwrap().to_string(),
        "0.33333333333333333333333333333333333333"
    );
    assert_eq!(
        d("2").checked_div(d("3")).unwrap().to_string(),
        "0.66666666666666666666666666666666666667"
    );
    for sign in [-1, 1] {
        assert_eq!(
            Decimal::new(sign * 5, 38)
                .unwrap()
                .checked_div(d("10"))
                .unwrap(),
            Decimal::new(sign, 38).unwrap()
        );
        assert_eq!(
            Decimal::new(sign, 38)
                .unwrap()
                .checked_div(d("10"))
                .unwrap(),
            Decimal::ZERO
        );
    }
    let max = Decimal::new(Decimal::MAX_COEFFICIENT, 0).unwrap();
    assert_eq!(max.checked_div(d("1")).unwrap(), max);
    assert_eq!(max.checked_div(d("1e-38")), Err(DecimalError::Overflow));
    assert_eq!(
        Decimal::ZERO.checked_div(Decimal::ZERO),
        Err(DecimalError::DivisionByZero)
    );
    // Independent rational residual bound for both signs and scale extremes.
    for left in ["-123456.789", "-1e-38", "0", "1e-38", "99999999999999999"] {
        for right in ["-7", "0.03", "2", "99999999999"] {
            let a = d(left);
            let b = d(right);
            let q = a.checked_div(b).unwrap();
            let n = BigInt::from(a.coefficient())
                * BigInt::from(10_u8).pow(u32::from(b.scale() + q.scale()));
            let denominator =
                BigInt::from(b.coefficient()) * BigInt::from(10_u8).pow(u32::from(a.scale()));
            assert!(
                (n - BigInt::from(q.coefficient()) * &denominator).abs() * 2 <= denominator.abs()
            );
        }
    }
}

#[test]
fn schema_rounding_checks_precision_after_carry() {
    let kind = DecimalType::new(4, 2).unwrap();
    for (input, expected) in [
        ("1.234", "1.23"),
        ("1.235", "1.24"),
        ("-1.235", "-1.24"),
        ("0", "0.00"),
    ] {
        let value = kind.coerce(d(input)).unwrap();
        assert_eq!(value.to_string(), expected);
        assert!(kind.accepts(value));
    }
    assert_eq!(kind.coerce(d("99.995")), Err(DecimalError::Overflow));
    assert_eq!(kind.coerce(d("-99.995")), Err(DecimalError::Overflow));
    for (precision, scale) in [(0, 0), (39, 0), (2, 3), (38, 39)] {
        assert_eq!(
            DecimalType::new(precision, scale),
            Err(DecimalError::InvalidType)
        );
    }
    assert_eq!(
        Decimal::new(Decimal::MAX_COEFFICIENT, 38)
            .unwrap()
            .rescale(0)
            .unwrap(),
        d("1")
    );
    assert_eq!(d("42.00").to_i64().unwrap(), 42);
    assert!(d("42.01").to_i64().is_err());
    assert_eq!(d("-9223372036854775808").to_i64().unwrap(), i64::MIN);
    assert!(d("9223372036854775808").to_i64().is_err());
}

#[test]
fn value_identity_order_and_encodings_agree_across_scales() {
    fn hash(value: Decimal) -> u64 {
        let mut state = DefaultHasher::new();
        value.hash(&mut state);
        state.finish()
    }
    let mut values = Vec::new();
    for scale in [0, 1, 2, 18, 37, 38] {
        for coefficient in [
            -Decimal::MAX_COEFFICIENT,
            -999,
            -100,
            -1,
            0,
            1,
            10,
            100,
            999,
            Decimal::MAX_COEFFICIENT,
        ] {
            values.push(Decimal::new(coefficient, scale).unwrap());
        }
    }
    for left in &values {
        let raw = Decimal::from_le_bytes(left.to_le_bytes()).unwrap();
        assert_eq!(raw.coefficient(), left.coefficient());
        assert_eq!(raw.scale(), left.scale());
        assert_eq!(
            Decimal::from_ordered_bytes(left.to_ordered_bytes()).unwrap(),
            *left
        );
        for right in &values {
            let lhs =
                BigInt::from(left.coefficient()) * BigInt::from(10_u8).pow(right.scale().into());
            let rhs =
                BigInt::from(right.coefficient()) * BigInt::from(10_u8).pow(left.scale().into());
            assert_eq!(left.cmp(right), lhs.cmp(&rhs));
            assert_eq!(left == right, lhs == rhs);
            if left == right {
                assert_eq!(hash(*left), hash(*right));
            }
        }
    }
    assert_eq!(d("1.0"), d("1.00"));
    assert_eq!(hash(d("1.0")), hash(d("1.00")));
    assert_eq!(std::mem::size_of::<Decimal>(), 17);
}

#[test]
fn corrupt_encodings_never_normalize_invalid_values() {
    let mut bytes = Decimal::ZERO.to_le_bytes();
    bytes[16] = 39;
    assert_eq!(
        Decimal::from_le_bytes(bytes),
        Err(DecimalError::InvalidEncoding)
    );
    bytes[..16].copy_from_slice(&i128::MIN.to_le_bytes());
    bytes[16] = 0;
    assert_eq!(
        Decimal::from_le_bytes(bytes),
        Err(DecimalError::InvalidEncoding)
    );
    for (offset, byte) in [(0, 3), (1, 0), (1, 77), (2, 0), (3, 10)] {
        let mut bytes = d("1").to_ordered_bytes();
        bytes[offset] = byte;
        assert_eq!(
            Decimal::from_ordered_bytes(bytes),
            Err(DecimalError::InvalidEncoding)
        );
    }
    let mut bytes = Decimal::ZERO.to_ordered_bytes();
    bytes[39] = 1;
    assert_eq!(
        Decimal::from_ordered_bytes(bytes),
        Err(DecimalError::InvalidEncoding)
    );
    let mut unrepresentable = d("1e-38").to_ordered_bytes();
    unrepresentable[39] = 1;
    assert_eq!(
        Decimal::from_ordered_bytes(unrepresentable),
        Err(DecimalError::InvalidEncoding)
    );
}

#[test]
fn successor_preserves_dense_boundaries_and_extremes() {
    let max = Decimal::MAX_COEFFICIENT;
    assert_eq!(d("-1").next_up().unwrap(), Decimal::new(-max, 38).unwrap());
    assert_eq!(Decimal::ZERO.next_up().unwrap(), d("1e-38"));
    assert_eq!(
        d("1").next_up().unwrap(),
        Decimal::new(10_i128.pow(37) + 1, 37).unwrap()
    );
    assert_eq!(Decimal::new(max, 0).unwrap().next_up(), None);
    assert_eq!(
        Decimal::new(-max, 0).unwrap().next_up().unwrap(),
        Decimal::new(-max + 1, 0).unwrap()
    );
}
