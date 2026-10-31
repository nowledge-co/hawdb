// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn decimal_json_parameters_preserve_digits_and_scale() {
    let value = Value::Decimal("9007199254740993.0100".parse().unwrap());
    let json = value_to_json(&value);
    assert_eq!(json, json!({"$decimal": "9007199254740993.0100"}));
    let Value::Decimal(decoded) = json_to_value(&json).unwrap() else {
        panic!("native Decimal");
    };
    assert_eq!(decoded.to_string(), "9007199254740993.0100");
    for invalid in [
        json!({"$decimal": 1.5}),
        json!({"$decimal": null}),
        json!({"$decimal": "NaN"}),
        json!({"$decimal": "1e1000"}),
    ] {
        assert!(json_to_value(&invalid).is_err(), "{invalid}");
    }
}
