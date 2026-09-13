//! Shared JSON evidence accessors preserving missing, null, and wrong-type semantics.
pub fn json_get_string_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> Vec<String> {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_string_array_path(value, &path)
}

pub fn json_get_array_path_from_dynamic(
    value: &serde_json::Value,
    base_path: &[&str],
    field: &str,
) -> serde_json::Value {
    let mut path = base_path.to_vec();
    path.push(field);
    json_get_array_path(value, &path)
}
pub fn json_get_path<'a>(
    value: &'a serde_json::Value,
    path: &[&str],
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    Some(current)
}

pub fn json_get_u64_path(value: &serde_json::Value, path: &[&str]) -> Option<u64> {
    json_get_path(value, path).and_then(serde_json::Value::as_u64)
}

pub fn json_get_bool_path(value: &serde_json::Value, path: &[&str]) -> Option<bool> {
    json_get_path(value, path).and_then(serde_json::Value::as_bool)
}

pub fn json_get_str_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    json_get_path(value, path).and_then(serde_json::Value::as_str)
}

pub fn json_get_bool_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<bool> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_bool)
}

pub fn json_get_str_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a str> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_str)
}

pub fn json_get_u64_path_from_dynamic(
    value: &serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<u64> {
    json_get_path_from_dynamic(value, prefix, field).and_then(serde_json::Value::as_u64)
}

pub fn json_get_path_from_dynamic<'a>(
    value: &'a serde_json::Value,
    prefix: &[&str],
    field: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for key in prefix {
        current = current.get(*key)?;
    }
    current.get(field)
}

pub fn json_get_array_path(value: &serde_json::Value, path: &[&str]) -> serde_json::Value {
    json_get_path(value, path)
        .filter(|value| value.is_array())
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

pub fn json_get_string_array_path(value: &serde_json::Value, path: &[&str]) -> Vec<String> {
    json_get_path(value, path)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.as_str().map(str::to_string))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn accessors_match_direct_json_access_without_coercion() {
        let values = [
            Value::Null,
            json!(false),
            json!(true),
            json!(0),
            json!(1),
            json!(u64::MAX),
            json!(-1),
            json!(1.5),
            json!("true"),
            json!("1"),
            json!(""),
            json!([]),
            json!({}),
            json!(["b", null, "a", 1, "b"]),
        ];
        for value in values {
            let nested = json!({"outer": {"field": value}});
            for (base, path) in [(&value, &[][..]), (&nested, &["outer", "field"][..])] {
                assert_eq!(json_get_path(base, path), Some(&value));
                assert_eq!(json_get_bool_path(base, path), value.as_bool());
                assert_eq!(json_get_u64_path(base, path), value.as_u64());
                assert_eq!(json_get_str_path(base, path), value.as_str());
                assert_eq!(
                    json_get_array_path(base, path),
                    if value.is_array() {
                        value.clone()
                    } else {
                        Value::Null
                    }
                );
                let expected: Vec<String> = value
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|item| item.as_str().map(str::to_owned))
                    .collect();
                assert_eq!(json_get_string_array_path(base, path), expected);
                assert_eq!(
                    json_get_string_array_path_from_dynamic(&nested, &["outer"], "field"),
                    expected
                );
            }
            assert_eq!(
                json_get_path_from_dynamic(&nested, &["outer"], "field"),
                Some(&value)
            );
            assert_eq!(
                json_get_bool_path_from_dynamic(&nested, &["outer"], "field"),
                value.as_bool()
            );
            assert_eq!(
                json_get_u64_path_from_dynamic(&nested, &["outer"], "field"),
                value.as_u64()
            );
            assert_eq!(
                json_get_str_path_from_dynamic(&nested, &["outer"], "field"),
                value.as_str()
            );
            assert_eq!(
                json_get_array_path_from_dynamic(&nested, &["outer"], "field"),
                json_get_array_path(&nested, &["outer", "field"])
            );
            assert_eq!(json_get_path(&nested, &["missing"]), None);
            assert_eq!(
                json_get_path_from_dynamic(&nested, &["missing"], "field"),
                None
            );
            assert_eq!(json_get_path(&value, &["field"]), None);
        }
    }
}
