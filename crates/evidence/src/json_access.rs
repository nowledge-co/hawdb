//! Strict JSON accessors shared by evidence validators.
//!
//! Missing or malformed fields remain absent; values are never coerced. In
//! particular, string arrays reject mixed element types instead of filtering them.

pub fn evidence_string<'a>(evidence: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    evidence.get(field).and_then(serde_json::Value::as_str)
}

pub fn evidence_bool(evidence: &serde_json::Value, field: &str) -> Option<bool> {
    evidence.get(field).and_then(serde_json::Value::as_bool)
}

pub fn evidence_u64(evidence: &serde_json::Value, field: &str) -> Option<u64> {
    evidence.get(field).and_then(serde_json::Value::as_u64)
}

pub fn nested_value<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

pub fn nested_bool(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    nested_value(value, path).and_then(serde_json::Value::as_bool)
}

pub fn nested_u64(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    nested_value(value, path).and_then(serde_json::Value::as_u64)
}

pub fn string_array_at(value: &serde_json::Value, path: &[&str]) -> Option<Vec<String>> {
    nested_value(value, path)?
        .as_array()?
        .iter()
        .map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn scalar_accessors_preserve_types_without_coercion() {
        for (value, string, boolean, integer) in [
            (Value::Null, None, None, None),
            (json!(true), None, Some(true), None),
            (json!(false), None, Some(false), None),
            (json!(0), None, None, Some(0)),
            (json!(u64::MAX), None, None, Some(u64::MAX)),
            (json!(-1), None, None, None),
            (json!(1.0), None, None, None),
            (json!("1"), Some("1"), None, None),
            (json!(""), Some(""), None, None),
            (json!([]), None, None, None),
            (json!({}), None, None, None),
        ] {
            let evidence = json!({"field": value});
            assert_eq!(evidence_string(&evidence, "field"), string);
            assert_eq!(evidence_bool(&evidence, "field"), boolean);
            assert_eq!(evidence_u64(&evidence, "field"), integer);
            assert_eq!(nested_bool(&evidence, &["field"]), boolean);
            assert_eq!(nested_u64(&evidence, &["field"]), integer);
        }
    }

    #[test]
    fn missing_fields_and_non_objects_stay_absent() {
        for value in [Value::Null, json!([]), json!(1), json!({"other": true})] {
            assert_eq!(evidence_string(&value, "field"), None);
            assert_eq!(evidence_bool(&value, "field"), None);
            assert_eq!(evidence_u64(&value, "field"), None);
            assert_eq!(nested_value(&value, &["field"]), None);
            assert_eq!(nested_bool(&value, &["field"]), None);
            assert_eq!(nested_u64(&value, &["field"]), None);
            assert_eq!(string_array_at(&value, &["field"]), None);
        }
    }

    #[test]
    fn nested_paths_are_literal_object_keys_and_preserve_null() {
        let evidence = json!({
            "outer": {"flag": false, "count": u64::MAX, "null": null},
            "a/b": {"0": true}, "array": [true]
        });
        assert!(std::ptr::eq(
            nested_value(&evidence, &[]).unwrap(),
            &evidence
        ));
        assert_eq!(nested_bool(&evidence, &["outer", "flag"]), Some(false));
        assert_eq!(nested_u64(&evidence, &["outer", "count"]), Some(u64::MAX));
        assert_eq!(
            nested_value(&evidence, &["outer", "null"]),
            Some(&Value::Null)
        );
        assert_eq!(nested_value(&evidence, &["outer", "missing"]), None);
        assert_eq!(nested_value(&evidence, &["outer", "flag", "child"]), None);
        assert_eq!(nested_bool(&evidence, &["a/b", "0"]), Some(true));
        assert_eq!(nested_bool(&evidence, &["array", "0"]), None);
    }

    #[test]
    fn string_arrays_reject_mixed_types_without_losing_order_or_duplicates() {
        for invalid in [
            json!(null),
            json!("code"),
            json!({}),
            json!([null]),
            json!(["code", 1]),
        ] {
            assert_eq!(string_array_at(&invalid, &[]), None);
        }
        assert_eq!(string_array_at(&json!([]), &[]), Some(Vec::new()));
        let strings = vec![
            "z".to_string(),
            " a ".to_string(),
            "z".to_string(),
            "\u{96ea}".to_string(),
        ];
        assert_eq!(
            string_array_at(&json!({"codes": strings}), &["codes"]),
            Some(strings)
        );
    }
}
