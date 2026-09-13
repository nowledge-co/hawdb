//! Test-only wire and bulk-summary oracle, independent of sidecar implementation.

use super::*;
use crate::{NumericMinMax, ScanScalar};

pub(super) fn crc(bytes: &[u8]) -> u64 {
    let mut state = u32::MAX;
    for byte in bytes {
        state ^= u32::from(*byte);
        for _ in 0..8 {
            state = (state >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(state & 1));
        }
    }
    u64::from(!state)
}

pub(super) fn hex(value: &str) -> String {
    value.bytes().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn value(value: &Value) -> String {
    match value {
        Value::Null => "n".into(),
        Value::Bool(value) => format!("b{}", u8::from(*value)),
        Value::Int(value) => format!("i{value}"),
        Value::Float(value) => format!("f{}", value.to_bits()),
        Value::String(value) => format!("s{}", hex(value)),
        Value::Binary(value) => format!(
            "x{}",
            value.iter().map(|b| format!("{b:02x}")).collect::<String>()
        ),
        Value::Uuid(value) => format!("u{value}"),
        Value::List(values) => format!(
            "l{}",
            values
                .iter()
                .map(|item| hex(&self::value(item)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Map(values) => format!(
            "m{}",
            values
                .iter()
                .map(|(key, item)| format!("{}={}", hex(key), hex(&self::value(item))))
                .collect::<Vec<_>>()
                .join(";")
        ),
    }
}

pub(super) fn payload(rows: &[SourceScanRow]) -> String {
    let mut lines = vec!["SKEIN_SOURCE_SCAN_SEGMENT_V1".to_string()];
    for row in rows {
        let properties = row
            .properties
            .iter()
            .map(|(key, item)| format!("{}={}", hex(key), value(item)))
            .collect::<Vec<_>>()
            .join(";");
        lines.push(format!("row\t{}\t{properties}", row.node_id));
    }
    lines.join("\n") + "\n"
}

// Single raw zstd block; no production compression/envelope encoder is used.
pub(super) fn envelope(text: &str) -> Vec<u8> {
    let raw = text.as_bytes();
    assert!(raw.len() < 131_072);
    let mut frame = vec![0x28, 0xb5, 0x2f, 0xfd, 0xa0];
    frame.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    frame.extend_from_slice(&(((raw.len() as u32) << 3) | 1).to_le_bytes()[..3]);
    frame.extend_from_slice(raw);
    let mut result = format!("SKEIN_COMPRESSED_V1\ncodec\tzstd\nuncompressed_checksum\t{}\ncompressed_checksum\t{}\nuncompressed_len\t{}\ncompressed_len\t{}\n\n", crc(raw), crc(&frame), raw.len(), frame.len()).into_bytes();
    result.extend(frame);
    result
}

pub(super) fn scalar(value: &Value) -> Option<ScanScalar> {
    // Source summaries intentionally omit binary and nested values.
    if matches!(value, Value::Binary(_)) {
        return None;
    }
    ScanScalar::from_value(value)
}

pub(super) fn summary(segment_id: u64, rows: &[SourceScanRow]) -> SegmentSummary {
    let mut fields = BTreeMap::new();
    let names = rows
        .iter()
        .flat_map(|row| row.properties.keys())
        .collect::<BTreeSet<_>>();
    for name in names {
        let observed = rows
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.properties.get(name).map(|value| (index, value)))
            .collect::<Vec<_>>();
        let mut numbers = observed
            .iter()
            .filter_map(|(_, value)| match value {
                Value::Int(number)
                    if (-9_007_199_254_740_992..=9_007_199_254_740_992).contains(number) =>
                {
                    Some(*number as f64)
                }
                Value::Float(number) if number.is_finite() => Some(*number),
                _ => None,
            })
            .collect::<Vec<_>>();
        numbers.sort_by(f64::total_cmp);
        let dates = observed
            .iter()
            .filter_map(|(_, value)| value.as_ref().as_str())
            .filter_map(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.timestamp_millis())
            .collect::<BTreeSet<_>>();
        let dictionary = observed
            .iter()
            .filter_map(|(_, value)| scalar(value))
            .collect::<BTreeSet<_>>();
        let exact_values = if name == "id" {
            dictionary
                .iter()
                .map(|key| {
                    let positions = observed
                        .iter()
                        .filter(|(_, value)| scalar(value).as_ref() == Some(key))
                        .map(|(index, _)| *index as u64)
                        .collect();
                    (key.clone(), positions)
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        fields.insert(
            name.clone(),
            FieldSummary {
                row_count: rows.len() as u64,
                present_count: observed.len() as u64,
                null_count: observed
                    .iter()
                    .filter(|(_, value)| matches!(value, Value::Null))
                    .count() as u64,
                missing_count: (rows.len() - observed.len()) as u64,
                numeric_min_max: numbers
                    .first()
                    .zip(numbers.last())
                    .map(|(&min, &max)| NumericMinMax { min, max }),
                datetime_min_max: dates.first().zip(dates.last()).map(
                    |(&min_epoch_millis, &max_epoch_millis)| DateTimeMinMax {
                        min_epoch_millis,
                        max_epoch_millis,
                    },
                ),
                enum_dictionary: (!dictionary.is_empty()).then_some(EnumDictionaryStats {
                    values: dictionary,
                    complete: true,
                }),
                membership: None,
                exact_values,
            },
        );
    }
    SegmentSummary {
        segment_id,
        row_count: rows.len() as u64,
        fields,
    }
}

pub(super) fn validate_signed_zero_ties(
    expected: &mut SegmentSummary,
    actual: &SegmentSummary,
    rows: &[SourceScanRow],
) {
    assert_eq!(actual, expected);
    for (name, field) in &mut expected.fields {
        let Some(bounds) = &mut field.numeric_min_max else {
            continue;
        };
        let actual_bounds = actual.fields[name].numeric_min_max.unwrap();
        for (bound, actual) in [
            (&mut bounds.min, actual_bounds.min),
            (&mut bounds.max, actual_bounds.max),
        ] {
            if bound.to_bits() == actual.to_bits() {
                continue;
            }
            // Rust min/max may return either input for equal signed zeros.
            // Accept only a zero sign present in this field's source values;
            // all other summary fields and numeric bounds remain exact.
            assert_eq!(*bound, 0.0);
            assert_eq!(actual, 0.0);
            assert!(
                rows.iter().any(|row| match row.properties.get(name) {
                    Some(Value::Float(value)) => value.to_bits() == actual.to_bits(),
                    Some(Value::Int(0)) => actual.to_bits() == 0,
                    _ => false,
                }),
                "numeric zero sign must occur in source field {name}"
            );
            *bound = actual;
        }
    }
}

fn scalar_text(value: &ScanScalar) -> String {
    match value {
        ScanScalar::Bool(v) => self::value(&Value::Bool(*v)),
        ScanScalar::Int(v) => self::value(&Value::Int(*v)),
        ScanScalar::Float(v) => self::value(&Value::Float(f64::from_bits(*v))),
        ScanScalar::String(v) => self::value(&Value::String(v.clone())),
        ScanScalar::Binary(v) => self::value(&Value::Binary(v.clone())),
        ScanScalar::Uuid(v) => self::value(&Value::Uuid(*v)),
    }
}

pub(super) fn descriptor(
    epoch: u64,
    summaries: &[SegmentSummary],
    ranges: &[SegmentPayloadRange],
) -> String {
    let mut lines = vec![
        "SKEIN_SOURCE_SCAN_SEGMENTS_V1".into(),
        format!("graph_epoch\t{epoch}"),
    ];
    for (summary, range) in summaries.iter().zip(ranges) {
        lines.push(format!(
            "segment\t{}\t{}\t{}\t{}\t{}\t{}",
            summary.segment_id,
            range.artifact_id,
            range.offset,
            range.length,
            range.checksum,
            summary.row_count
        ));
        for (name, field) in &summary.fields {
            let numeric = field
                .numeric_min_max
                .map(|value| {
                    [
                        value.min.to_bits().to_string(),
                        value.max.to_bits().to_string(),
                    ]
                })
                .unwrap_or_default();
            let datetime = field
                .datetime_min_max
                .map(|value| {
                    [
                        value.min_epoch_millis.to_string(),
                        value.max_epoch_millis.to_string(),
                    ]
                })
                .unwrap_or_default();
            let dictionary = field
                .enum_dictionary
                .as_ref()
                .map(|value| {
                    value
                        .values
                        .iter()
                        .map(|item| hex(&scalar_text(item)))
                        .collect::<Vec<_>>()
                        .join(":")
                })
                .unwrap_or_default();
            lines.push(format!(
                "field\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                hex(name),
                field.present_count,
                field.null_count,
                field.missing_count,
                numeric[0],
                numeric[1],
                datetime[0],
                datetime[1],
                dictionary
            ));
            for (key, ids) in &field.exact_values {
                lines.push(format!(
                    "exact\t{}\t{}\t{}",
                    hex(name),
                    hex(&scalar_text(key)),
                    ids.iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
        }
    }
    lines.join("\n") + "\n"
}

pub(super) fn descriptor_file(body: &str) -> (String, u64) {
    let checksum = crc(body.as_bytes());
    (format!("{body}checksum\t{checksum}\n"), checksum)
}
