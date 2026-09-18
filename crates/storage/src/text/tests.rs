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

#[test]
fn fixed_value_encodings_preserve_tags_bits_and_container_framing() {
    for (value, encoded) in [
        (Value::Null, "n"),
        (Value::Bool(false), "b0"),
        (Value::Bool(true), "b1"),
        (Value::Int(i64::MIN), "i-9223372036854775808"),
        (Value::Int(i64::MAX), "i9223372036854775807"),
        (Value::Float(0.0), "f0"),
        (Value::Float(-0.0), "f9223372036854775808"),
        (Value::Float(f64::INFINITY), "f9218868437227405312"),
        (
            Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
            "f9221120237041090561",
        ),
        (Value::String(String::new()), "s"),
        (Value::String("\u{e9};=,\0".into()), "sc3a93b3d2c00"),
        (
            Value::Uuid(
                hawdb_core::Uuid::parse_str("01890f3e-0e3c-7f9e-9b23-1bcdef012345").unwrap(),
            ),
            "u01890f3e-0e3c-7f9e-9b23-1bcdef012345",
        ),
        (Value::Binary(vec![]), "x"),
        (Value::Binary(vec![0, 255]), "x00ff"),
        (Value::List(vec![]), "l"),
        (Value::List(vec![Value::Null, Value::Int(1)]), "l6e,6931"),
        (Value::Map(BTreeMap::new()), "m"),
        (
            Value::Map(BTreeMap::from([
                ("z".into(), Value::Bool(false)),
                ("a".into(), Value::Int(1)),
            ])),
            "m61=6931;7a=6230",
        ),
    ] {
        assert_eq!(encode_value(&value), encoded, "{value:?}");
        assert_eq!(decode_value(encoded).unwrap(), value, "{encoded}");
    }
    let properties = BTreeMap::from([
        ("z".into(), Value::Bool(false)),
        ("a".into(), Value::Int(1)),
    ]);
    assert_eq!(encode_properties(&properties), "61=i1;7a=b0");
    assert_eq!(decode_properties("61=i1;7a=b0").unwrap(), properties);
}

#[test]
fn decoding_preserves_legacy_tolerance_without_changing_binary_hex_rules() {
    assert_eq!(decode_bytes("+fAF").unwrap(), vec![15, 175]);
    assert_eq!(decode_value("xAF").unwrap(), Value::Binary(vec![175]));
    assert!(decode_value("x+f").is_err());
    assert_eq!(decode_value("i+1").unwrap(), Value::Int(1));
    assert_eq!(decode_value("f+0").unwrap(), Value::Float(0.0));
    assert_eq!(
        decode_value("m61=6931;61=6932").unwrap(),
        Value::Map(BTreeMap::from([("a".into(), Value::Int(2))]))
    );
    assert_eq!(
        decode_properties("61=i1;61=i2").unwrap(),
        BTreeMap::from([("a".into(), Value::Int(2))])
    );
    for encoded in [
        "i9223372036854775808",
        "f18446744073709551616",
        "b2",
        "n0",
        "uinvalid",
    ] {
        assert!(matches!(decode_value(encoded), Err(HawDBError::Storage(_))));
    }
}

// Independent byte oracle, including the legacy radix parser's leading plus.
fn reference(input: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if !input.len().is_multiple_of(2) {
        return None;
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = if pair[0] == b'+' { 0 } else { nibble(pair[0])? };
            Some(high * 16 + nibble(pair[1])?)
        })
        .collect()
}

#[test]
fn hex_preserves_all_ascii_pairs_and_valid_byte_round_trips() {
    for first in 0..128 {
        for second in 0..128 {
            let input = String::from_utf8(vec![first, second]).unwrap();
            assert_eq!(decode_bytes(&input).ok(), reference(&input));
        }
    }
    let bytes = (0..=255).collect::<Vec<u8>>();
    assert_eq!(decode_bytes(&encode_bytes(&bytes)).unwrap(), bytes);
    for text in ["", "\0\t\n", "ASCII \u{e9}\u{4e2d}\u{1f980}"] {
        assert_eq!(decode_string(&encode_string(text)).unwrap(), text);
    }
}

#[test]
fn malformed_hex_returns_storage_errors_without_panicking() {
    for input in ["a\u{e9}a", "\u{1f980}", "00a\u{e9}a", "f", "gg"] {
        let result = std::panic::catch_unwind(|| decode_bytes(input));
        assert!(result.is_ok(), "hex decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(HawDBError::Storage(_))));
    }
}

#[test]
fn bad_hex_diagnostics_do_not_copy_the_field() {
    for input in [
        format!("gg{}", "00".repeat(128 * 1024)),
        format!("{}gg", "00".repeat(128 * 1024)),
    ] {
        let error = decode_bytes(&input).unwrap_err().to_string();
        assert!(error.contains("invalid hex"), "{error}");
        assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
    }
}

#[test]
fn malformed_value_tags_are_rejected_without_unbounded_diagnostics() {
    for input in ["\u{e9}", "\u{4e2d}", "\u{1f980}", "", "?", "nextra"] {
        let result = std::panic::catch_unwind(|| decode_value(input));
        assert!(result.is_ok(), "value decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(HawDBError::Storage(_))));
    }
    let input = format!("?{}", "x".repeat(256 * 1024));
    let error = decode_value(&input).unwrap_err().to_string();
    assert!(error.len() < 128, "diagnostic copied {} bytes", error.len());
}

#[test]
fn nested_value_paths_propagate_corruption_and_preserve_valid_values() {
    let bad_hex = "a\u{e9}a";
    let bad_value = encode_string("\u{e9}");
    for input in [
        format!("s{bad_hex}"),
        format!("l{bad_hex}"),
        format!("m{bad_hex}=6931"),
        format!("m6964={bad_hex}"),
        format!("l{bad_value}"),
        format!("m6964={bad_value}"),
    ] {
        let result = std::panic::catch_unwind(|| decode_value(&input));
        assert!(result.is_ok(), "nested decoder panicked for {input:?}");
        assert!(matches!(result.unwrap(), Err(HawDBError::Storage(_))));
    }
    assert!(decode_properties("6964=sa\u{e9}a").is_err());
    for value in [
        Value::Null,
        Value::Bool(true),
        Value::Int(-7),
        Value::Float(1.25),
        Value::Uuid(hawdb_core::Uuid::parse_str("01890f3e-0e3c-7f9e-9b23-1bcdef012345").unwrap()),
        Value::String("\u{4e2d}".into()),
        Value::Binary(vec![0, 255]),
        Value::List(vec![Value::String("\u{e9}".into()), Value::Null]),
        Value::Map(BTreeMap::from([(
            "\u{1f980}".into(),
            Value::List(vec![Value::Int(8)]),
        )])),
    ] {
        assert_eq!(decode_value(&encode_value(&value)).unwrap(), value);
    }
}

fn campaign(cases: usize) {
    let mut seed = 0x0404_5afe_u64;
    let mut next = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        seed
    };
    for case in 0..cases {
        let mut input = (0..next() % 128)
            .map(|_| format!("{:02x}", (next() >> 32) as u8))
            .collect::<String>();
        match case % 8 {
            1 => input.make_ascii_uppercase(),
            2 => input.push_str("gg"),
            3 => input.push('f'),
            4 => input.insert_str(0, "a\u{e9}a"),
            5 => input.push_str("+0+f"),
            6 => input.truncate(input.len() / 2),
            7 => input.push('\u{1f980}'),
            _ => {}
        }
        let expected = reference(&input);
        let decoded = decode_bytes(&input);
        if let Err(error) = &decoded {
            assert!(matches!(error, HawDBError::Storage(_)));
            assert!(error.to_string().len() < 128, "case {case}");
        }
        assert_eq!(decoded.ok(), expected, "case {case}");
        let expected = expected.and_then(|bytes| String::from_utf8(bytes).ok());
        assert_eq!(decode_string(&input).ok(), expected, "case {case}");
        assert_eq!(
            decode_value(&format!("s{input}")).ok(),
            expected.map(Value::String),
            "case {case}"
        );
        let bad_tag = ["\u{e9}", "\u{4e2d}", "\u{1f980}"][case % 3];
        assert!(decode_value(&format!("{bad_tag}{input}")).is_err());
        let error = decode_value(&format!("?{input}")).unwrap_err();
        assert!(matches!(error, HawDBError::Storage(_)));
        assert!(error.to_string().len() < 128, "case {case}");
    }
}

#[test]
fn recovery_text_differential_smoke() {
    campaign(32);
}

#[test]
#[ignore = "explicit local recovery text differential campaign"]
fn recovery_text_differential_campaign() {
    campaign(1024);
}
