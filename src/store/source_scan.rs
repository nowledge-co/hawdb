//! Durable, rebuildable Source scan sidecar.
//!
//! The graph checkpoint remains authoritative. This sidecar is eligible only
//! when the checkpoint manifest publishes the same graph epoch.

use super::{
    checksum_bytes, decode_properties, decode_string, decode_value, encode_durable_text,
    encode_properties, encode_string, encode_value, parse_i64, parse_u64, DurableCompression,
    Result, SkeinError, Value,
};
use skein_storage::{
    durable_replace_file, DateTimeMinMax, EnumDictionaryStats, FieldSummary, NodeRecord,
    PersistedScanSegment, ScanSegmentManifest, SegmentPayloadRange, SegmentSummary,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::Path;

pub(super) const SOURCE_SCAN_DESCRIPTOR_FILE: &str = "source_scan_segments.skein";
pub(super) const SOURCE_SCAN_PAYLOAD_FILE: &str = "source_scan_segment_payloads.skein";

pub(super) const SOURCE_SCAN_ARTIFACT_ID: u64 = 1;
pub(super) const SOURCE_SCAN_TARGET_ROWS: usize = 128;
const SOURCE_SCAN_DESCRIPTOR_HEADER: &str = "SKEIN_SOURCE_SCAN_SEGMENTS_V1";
const SOURCE_SCAN_SEGMENT_HEADER: &str = "SKEIN_SOURCE_SCAN_SEGMENT_V1";
const UNIQUE_KEY_FIELDS: &[&str] = &["id"];

#[derive(Debug, Clone)]
pub(super) struct SourceScanProjection {
    graph_epoch: u64,
    segments: Vec<SourceScanSegment>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SourceScanPublication {
    graph_epoch: u64,
    descriptor_checksum: u64,
}

impl SourceScanPublication {
    pub(super) const fn graph_epoch(self) -> u64 {
        self.graph_epoch
    }

    pub(super) const fn descriptor_checksum(self) -> u64 {
        self.descriptor_checksum
    }
}

#[derive(Debug, Clone)]
struct SourceScanSegment {
    summary: SegmentSummary,
    rows: Vec<SourceScanRow>,
    payload_range: Option<SegmentPayloadRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceScanRow {
    pub node_id: u64,
    pub properties: BTreeMap<String, Value>,
}

pub(super) fn build<'a>(
    graph_epoch: u64,
    source_label_id: Option<crate::schema::LabelId>,
    nodes: impl Iterator<Item = &'a NodeRecord>,
) -> SourceScanProjection {
    let rows = source_label_id.map_or_else(Vec::new, |source_label_id| {
        nodes
            .filter(|node| node.labels.contains(&source_label_id))
            .map(|node| SourceScanRow {
                node_id: node.id.0,
                properties: node.properties.clone(),
            })
            .collect()
    });
    let segments = rows
        .chunks(SOURCE_SCAN_TARGET_ROWS)
        .enumerate()
        .map(|(segment_id, rows)| SourceScanSegment::from_rows(segment_id as u64, rows))
        .collect();
    SourceScanProjection {
        graph_epoch,
        segments,
    }
}

pub(super) fn write(
    path: &Path,
    projection: &mut SourceScanProjection,
) -> Result<SourceScanPublication> {
    let payload_path = path.join(SOURCE_SCAN_PAYLOAD_FILE);
    let payload_tmp_path = payload_path.with_extension("skein.tmp");
    let mut offset = 0u64;
    {
        let mut file = File::create(&payload_tmp_path)?;
        for segment in &mut projection.segments {
            let payload = encode_segment_payload(&segment.rows)?;
            let length = u64::try_from(payload.len()).map_err(|_| {
                SkeinError::Storage(format!(
                    "source scan segment {} payload exceeds supported range length",
                    segment.summary.segment_id
                ))
            })?;
            segment.payload_range = Some(SegmentPayloadRange {
                artifact_id: SOURCE_SCAN_ARTIFACT_ID,
                offset,
                length: NonZeroU64::new(length).ok_or_else(|| {
                    SkeinError::Storage("source scan segment payload is empty".to_string())
                })?,
                checksum: checksum_bytes(&payload),
            });
            file.write_all(&payload)?;
            offset = offset.checked_add(length).ok_or_else(|| {
                SkeinError::Storage("source scan payload artifact length overflow".to_string())
            })?;
        }
        file.sync_all()?;
    }
    durable_replace_file(&payload_tmp_path, &payload_path)?;

    let descriptor_path = path.join(SOURCE_SCAN_DESCRIPTOR_FILE);
    let descriptor_tmp_path = descriptor_path.with_extension("skein.tmp");
    let body = encode_descriptor(projection)?;
    let descriptor_checksum = checksum_bytes(body.as_bytes());
    let data = format!("{body}checksum\t{descriptor_checksum}\n");
    {
        let mut file = File::create(&descriptor_tmp_path)?;
        file.write_all(data.as_bytes())?;
        file.sync_all()?;
    }
    durable_replace_file(&descriptor_tmp_path, &descriptor_path)?;
    Ok(SourceScanPublication {
        graph_epoch: projection.graph_epoch,
        descriptor_checksum,
    })
}

pub(super) fn load(
    path: &Path,
    expected_graph_epoch: u64,
    expected_descriptor_checksum: u64,
) -> Result<Option<ScanSegmentManifest>> {
    let descriptor_path = path.join(SOURCE_SCAN_DESCRIPTOR_FILE);
    if !descriptor_path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&descriptor_path)?;
    let mut projection = decode_descriptor(&text, expected_descriptor_checksum)?;
    if projection.graph_epoch != expected_graph_epoch {
        return Ok(None);
    }

    let payload_path = path.join(SOURCE_SCAN_PAYLOAD_FILE);
    validate_payload_ranges(&payload_path, &projection.segments)?;
    let segments = projection
        .segments
        .drain(..)
        .map(|segment| {
            Ok(PersistedScanSegment {
                summary: segment.summary,
                payload_range: segment.payload_range.ok_or_else(|| {
                    SkeinError::Storage("source scan descriptor missing payload range".to_string())
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ScanSegmentManifest::new(expected_graph_epoch, segments)
        .map(Some)
        .map_err(|error| SkeinError::Storage(error.to_string()))
}

pub(super) fn decode_payload(payload: &[u8]) -> Result<Vec<SourceScanRow>> {
    let text = super::read_durable_text_bytes(payload, "source scan segment")?;
    let mut rows = Vec::new();
    for line in text.lines() {
        if line == SOURCE_SCAN_SEGMENT_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["row", raw_node_id, raw_properties] => rows.push(SourceScanRow {
                node_id: parse_u64(raw_node_id, "source scan node id")?,
                properties: decode_properties(raw_properties)?,
            }),
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid source scan segment line: {line}"
                )));
            }
        }
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].node_id >= pair[1].node_id)
    {
        return Err(SkeinError::Storage(
            "source scan segment rows are not strictly ordered".to_string(),
        ));
    }
    Ok(rows)
}

impl SourceScanSegment {
    fn from_rows(segment_id: u64, rows: &[SourceScanRow]) -> Self {
        let mut summary = SegmentSummary::new(segment_id, rows.len() as u64);
        let fields = rows
            .iter()
            .flat_map(|row| row.properties.keys().cloned())
            .collect::<BTreeSet<_>>();
        for field in fields {
            let (field_summary, exact_values) = build_field_summary(rows, &field);
            summary.insert_field(field.clone(), field_summary);
            if !exact_values.is_empty() {
                // Exact row cursors are bounded by one immutable segment.
                for (value, row_ids) in &exact_values {
                    let field_summary = summary
                        .fields
                        .get_mut(&field)
                        .expect("source scan field summary was inserted");
                    *field_summary =
                        std::mem::replace(field_summary, FieldSummary::new(rows.len() as u64))
                            .with_exact_row_ids(value.clone(), row_ids.iter().copied());
                }
            }
        }
        Self {
            summary,
            rows: rows.to_vec(),
            payload_range: None,
        }
    }
}

fn build_field_summary(
    rows: &[SourceScanRow],
    field: &str,
) -> (FieldSummary, Vec<(Value, Vec<u64>)>) {
    let mut present_count = 0u64;
    let mut null_count = 0u64;
    let mut scalar_values = Vec::new();
    let mut exact_values = BTreeMap::<String, (Value, Vec<u64>)>::new();
    let mut numeric_min = None::<f64>;
    let mut numeric_max = None::<f64>;
    let mut datetime_min = None::<i64>;
    let mut datetime_max = None::<i64>;

    for (row_id, row) in rows.iter().enumerate() {
        let Some(value) = row.properties.get(field) else {
            continue;
        };
        present_count += 1;
        if matches!(value, Value::Null) {
            null_count += 1;
            continue;
        }
        if is_scalar(value) {
            scalar_values.push(value.clone());
            if UNIQUE_KEY_FIELDS.contains(&field) {
                exact_values
                    .entry(encode_value(value))
                    .or_insert_with(|| (value.clone(), Vec::new()))
                    .1
                    .push(row_id as u64);
            }
        }
        if let Some(value) = precise_numeric_value(value) {
            numeric_min = Some(numeric_min.map_or(value, |min| min.min(value)));
            numeric_max = Some(numeric_max.map_or(value, |max| max.max(value)));
        }
        if let Value::String(value) = value
            && let Some(value) = DateTimeMinMax::parse_rfc3339(value)
        {
            datetime_min = Some(datetime_min.map_or(value, |min| min.min(value)));
            datetime_max = Some(datetime_max.map_or(value, |max| max.max(value)));
        }
    }

    let row_count = rows.len() as u64;
    let mut summary = FieldSummary::new(row_count)
        .with_presence_counts(present_count, null_count, row_count - present_count)
        .expect("source scan field counts must match rows");
    if let (Some(min), Some(max)) = (numeric_min, numeric_max) {
        summary = summary
            .with_numeric_min_max(min, max)
            .expect("source scan numeric range is valid");
    }
    if let (Some(min), Some(max)) = (datetime_min, datetime_max) {
        summary = summary
            .with_datetime_min_max(min, max)
            .expect("source scan timestamp range is valid");
    }
    if !scalar_values.is_empty() {
        summary = summary.with_enum_dictionary(EnumDictionaryStats::complete(scalar_values));
    }
    (summary, exact_values.into_values().collect())
}

fn is_scalar(value: &Value) -> bool {
    match value {
        Value::Bool(_) | Value::Int(_) | Value::String(_) => true,
        Value::Float(value) => value.is_finite(),
        Value::Null | Value::Binary(_) | Value::List(_) | Value::Map(_) => false,
    }
}

fn precise_numeric_value(value: &Value) -> Option<f64> {
    match value {
        Value::Float(value) if value.is_finite() => Some(*value),
        Value::Int(value) if value.unsigned_abs() <= (1_u64 << 53) => Some(*value as f64),
        _ => None,
    }
}

fn encode_segment_payload(rows: &[SourceScanRow]) -> Result<Vec<u8>> {
    let mut body = String::from(SOURCE_SCAN_SEGMENT_HEADER);
    body.push('\n');
    for row in rows {
        body.push_str(&format!(
            "row\t{}\t{}\n",
            row.node_id,
            encode_properties(&row.properties)
        ));
    }
    encode_durable_text(&body, DurableCompression::default())
}

fn encode_descriptor(projection: &SourceScanProjection) -> Result<String> {
    let mut body = format!(
        "{SOURCE_SCAN_DESCRIPTOR_HEADER}\ngraph_epoch\t{}\n",
        projection.graph_epoch
    );
    for segment in &projection.segments {
        let range = segment.payload_range.ok_or_else(|| {
            SkeinError::Storage("source scan segment has no payload range".to_string())
        })?;
        body.push_str(&format!(
            "segment\t{}\t{}\t{}\t{}\t{}\t{}\n",
            segment.summary.segment_id,
            range.artifact_id,
            range.offset,
            range.length,
            range.checksum,
            segment.summary.row_count,
        ));
        for (field, summary) in &segment.summary.fields {
            body.push_str(&format!(
                "field\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                encode_string(field),
                summary.present_count,
                summary.null_count,
                summary.missing_count,
                encode_optional_f64(summary.numeric_min_max.map(|range| range.min)),
                encode_optional_f64(summary.numeric_min_max.map(|range| range.max)),
                encode_optional_i64(summary.datetime_min_max.map(|range| range.min_epoch_millis)),
                encode_optional_i64(summary.datetime_min_max.map(|range| range.max_epoch_millis)),
                encode_enum_dictionary(summary.enum_dictionary.as_ref()),
            ));
            for (value, row_ids) in &summary.exact_values {
                let encoded_value = match value {
                    skein_storage::ScanScalar::Bool(value) => encode_value(&Value::Bool(*value)),
                    skein_storage::ScanScalar::Int(value) => encode_value(&Value::Int(*value)),
                    skein_storage::ScanScalar::Float(value) => {
                        encode_value(&Value::Float(f64::from_bits(*value)))
                    }
                    skein_storage::ScanScalar::String(value) => {
                        encode_value(&Value::String(value.clone()))
                    }
                    skein_storage::ScanScalar::Binary(value) => {
                        encode_value(&Value::Binary(value.clone()))
                    }
                };
                body.push_str(&format!(
                    "exact\t{}\t{}\t{}\n",
                    encode_string(field),
                    encode_string(&encoded_value),
                    row_ids
                        .iter()
                        .map(|row_id| row_id.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                ));
            }
        }
    }
    Ok(body)
}

fn decode_descriptor(text: &str, expected_checksum: u64) -> Result<SourceScanProjection> {
    let (body, checksum) = text.rsplit_once("checksum\t").ok_or_else(|| {
        SkeinError::Storage("source scan descriptor missing checksum footer".to_string())
    })?;
    let checksum = parse_u64(checksum.trim(), "source scan checksum")?;
    if checksum != expected_checksum || checksum_bytes(body.as_bytes()) != checksum {
        return Err(SkeinError::Storage(
            "source scan descriptor checksum mismatch".to_string(),
        ));
    }
    let mut graph_epoch = None;
    let mut segments = Vec::<SourceScanSegment>::new();
    let mut current = None::<SourceScanSegment>;
    for line in body.lines() {
        if line == SOURCE_SCAN_DESCRIPTOR_HEADER {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["graph_epoch", raw_epoch] => {
                graph_epoch = Some(parse_u64(raw_epoch, "source scan epoch")?)
            }
            ["segment", raw_id, raw_artifact, raw_offset, raw_length, raw_checksum, raw_rows] => {
                if let Some(segment) = current.take() {
                    segments.push(segment);
                }
                let length = NonZeroU64::new(parse_u64(raw_length, "source scan payload length")?)
                    .ok_or_else(|| {
                        SkeinError::Storage("source scan payload length is zero".to_string())
                    })?;
                current = Some(SourceScanSegment {
                    summary: SegmentSummary::new(
                        parse_u64(raw_id, "source scan segment id")?,
                        parse_u64(raw_rows, "source scan row count")?,
                    ),
                    rows: Vec::new(),
                    payload_range: Some(SegmentPayloadRange {
                        artifact_id: parse_u64(raw_artifact, "source scan artifact id")?,
                        offset: parse_u64(raw_offset, "source scan payload offset")?,
                        length,
                        checksum: parse_u64(raw_checksum, "source scan payload checksum")?,
                    }),
                });
            }
            ["field", raw_field, raw_present, raw_null, raw_missing, raw_numeric_min, raw_numeric_max, raw_datetime_min, raw_datetime_max, raw_values] =>
            {
                let segment = current.as_mut().ok_or_else(|| {
                    SkeinError::Storage("source scan field appears before a segment".to_string())
                })?;
                let row_count = segment.summary.row_count;
                let mut summary = FieldSummary::new(row_count)
                    .with_presence_counts(
                        parse_u64(raw_present, "source scan field present count")?,
                        parse_u64(raw_null, "source scan field null count")?,
                        parse_u64(raw_missing, "source scan field missing count")?,
                    )
                    .ok_or_else(|| {
                        SkeinError::Storage("invalid source scan field counts".to_string())
                    })?;
                if let (Some(min), Some(max)) = (
                    decode_optional_f64(raw_numeric_min)?,
                    decode_optional_f64(raw_numeric_max)?,
                ) {
                    summary = summary.with_numeric_min_max(min, max).ok_or_else(|| {
                        SkeinError::Storage("invalid source scan numeric range".to_string())
                    })?;
                }
                if let (Some(min), Some(max)) = (
                    decode_optional_i64(raw_datetime_min)?,
                    decode_optional_i64(raw_datetime_max)?,
                ) {
                    summary = summary.with_datetime_min_max(min, max).ok_or_else(|| {
                        SkeinError::Storage("invalid source scan datetime range".to_string())
                    })?;
                }
                let values = decode_enum_dictionary(raw_values)?;
                if !values.is_empty() {
                    summary = summary.with_enum_dictionary(EnumDictionaryStats::complete(values));
                }
                segment
                    .summary
                    .insert_field(decode_string(raw_field)?, summary);
            }
            ["exact", raw_field, raw_value, raw_row_ids] => {
                let segment = current.as_mut().ok_or_else(|| {
                    SkeinError::Storage(
                        "source scan exact cursor appears before a segment".to_string(),
                    )
                })?;
                let field = decode_string(raw_field)?;
                let value = decode_value(&decode_string(raw_value)?)?;
                let row_ids = decode_row_ids(raw_row_ids)?;
                let summary = segment.summary.fields.remove(&field).ok_or_else(|| {
                    SkeinError::Storage(
                        "source scan exact cursor references unknown field".to_string(),
                    )
                })?;
                segment
                    .summary
                    .insert_field(field, summary.with_exact_row_ids(value, row_ids));
            }
            [""] => {}
            _ => {
                return Err(SkeinError::Storage(format!(
                    "invalid source scan descriptor line: {line}"
                )))
            }
        }
    }
    if let Some(segment) = current.take() {
        segments.push(segment);
    }
    Ok(SourceScanProjection {
        graph_epoch: graph_epoch.ok_or_else(|| {
            SkeinError::Storage("source scan descriptor missing graph epoch".to_string())
        })?,
        segments,
    })
}

fn validate_payload_ranges(path: &Path, segments: &[SourceScanSegment]) -> Result<()> {
    let mut file = File::open(path)?;
    let artifact_len = file.metadata()?.len();
    for segment in segments {
        let range = segment.payload_range.ok_or_else(|| {
            SkeinError::Storage("source scan descriptor missing payload range".to_string())
        })?;
        if range.artifact_id != SOURCE_SCAN_ARTIFACT_ID {
            return Err(SkeinError::Storage(
                "source scan descriptor has unsupported artifact".to_string(),
            ));
        }
        let end = range
            .offset
            .checked_add(range.length.get())
            .ok_or_else(|| SkeinError::Storage("source scan payload range overflow".to_string()))?;
        if end > artifact_len {
            return Err(SkeinError::Storage(
                "source scan payload range exceeds artifact".to_string(),
            ));
        }
        let mut payload = vec![
            0;
            usize::try_from(range.length.get()).map_err(|_| {
                SkeinError::Storage("source scan payload range exceeds address space".to_string())
            })?
        ];
        file.seek(SeekFrom::Start(range.offset))?;
        file.read_exact(&mut payload)?;
        if checksum_bytes(&payload) != range.checksum {
            return Err(SkeinError::Storage(
                "source scan payload checksum mismatch".to_string(),
            ));
        }
        let rows = decode_payload(&payload)?;
        if rows.len() as u64 != segment.summary.row_count {
            return Err(SkeinError::Storage(
                "source scan payload row count mismatch".to_string(),
            ));
        }
    }
    Ok(())
}

fn encode_optional_f64(value: Option<f64>) -> String {
    value
        .map(|value| value.to_bits().to_string())
        .unwrap_or_default()
}

fn decode_optional_f64(value: &str) -> Result<Option<f64>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(f64::from_bits(parse_u64(value, "source scan float")?)))
}

fn encode_optional_i64(value: Option<i64>) -> String {
    value.map(|value| value.to_string()).unwrap_or_default()
}

fn decode_optional_i64(value: &str) -> Result<Option<i64>> {
    if value.is_empty() {
        return Ok(None);
    }
    parse_i64(value, "source scan timestamp").map(Some)
}

fn encode_enum_dictionary(dictionary: Option<&EnumDictionaryStats>) -> String {
    dictionary.map_or_else(String::new, |dictionary| {
        dictionary
            .values
            .iter()
            .map(|value| {
                let value = match value {
                    skein_storage::ScanScalar::Bool(value) => Value::Bool(*value),
                    skein_storage::ScanScalar::Int(value) => Value::Int(*value),
                    skein_storage::ScanScalar::Float(value) => Value::Float(f64::from_bits(*value)),
                    skein_storage::ScanScalar::String(value) => Value::String(value.clone()),
                    skein_storage::ScanScalar::Binary(value) => Value::Binary(value.clone()),
                };
                encode_string(&encode_value(&value))
            })
            .collect::<Vec<_>>()
            .join(":")
    })
}

fn decode_enum_dictionary(value: &str) -> Result<Vec<Value>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(':')
        .map(|value| decode_string(value).and_then(|value| decode_value(&value)))
        .collect()
}

fn decode_row_ids(value: &str) -> Result<Vec<u64>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let row_ids = value
        .split(',')
        .map(|value| parse_u64(value, "source scan row id"))
        .collect::<Result<Vec<_>>>()?;
    if !row_ids.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(SkeinError::Storage(
            "source scan exact row ids are not ordered".to_string(),
        ));
    }
    Ok(row_ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::LabelId;
    use skein_storage::NodeId;

    fn source(id: u64, source_label: LabelId, scope: &str) -> NodeRecord {
        NodeRecord {
            id: NodeId(id),
            labels: BTreeSet::from([source_label]),
            properties: BTreeMap::from([
                ("id".to_string(), Value::String(format!("source-{id}"))),
                ("space_id".to_string(), Value::String(scope.to_string())),
                (
                    "created_at".to_string(),
                    Value::String("2026-08-01T00:00:00Z".to_string()),
                ),
            ]),
        }
    }

    #[test]
    fn sidecar_round_trips_payload_ranges_and_exact_cursors() {
        let directory =
            std::env::temp_dir().join(format!("skein-source-scan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).unwrap();
        let source_label = LabelId(7);
        let nodes = [
            source(1, source_label, "alpha"),
            source(2, source_label, "beta"),
        ];
        let mut projection = build(4, Some(source_label), nodes.iter());
        let publication = write(&directory, &mut projection).unwrap();

        let manifest = load(&directory, 4, publication.descriptor_checksum())
            .unwrap()
            .unwrap();
        assert_eq!(manifest.graph_epoch(), 4);
        assert_eq!(manifest.segments().len(), 1);
        let plan = manifest.plan_scan(
            4,
            &skein_storage::ScanPredicate::Eq {
                property: "id".to_string(),
                value: Value::String("source-1".to_string()),
            },
        );
        let skein_storage::ScanSegmentAccessPlan::Read(plan) = plan else {
            panic!("expected scan");
        };
        assert_eq!(plan.segments.len(), 1);
        assert_eq!(plan.segments[0].candidates.as_ref().unwrap().remaining(), 1);
        let _ = fs::remove_dir_all(&directory);
    }
}
