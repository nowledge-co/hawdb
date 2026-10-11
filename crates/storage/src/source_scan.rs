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

//! Durable, rebuildable Source scan sidecar.
//!
//! The graph checkpoint remains authoritative. This sidecar is eligible only
//! when the checkpoint manifest publishes the same graph epoch.

use crate::background::{CheckpointText, CheckpointValues, CheckpointWorkContext};
use crate::cow::CowSegmentedMap;
use crate::file_io::{self as fs, File};
use crate::text::envelope::{encode_durable_text_with_work_context, read_durable_text_bytes};
use crate::text::{
    decode_properties, decode_string, decode_value, encode_string, encode_value, parse_i64,
    parse_u64,
};
use crate::NodeId;

use crate::{
    config::DurableCompression,
    durability::durable_replace_file,
    scan::{
        DateTimeMinMax, EnumDictionaryStats, FieldSummary, PersistedScanSegment, ScanPredicate,
        ScanSegmentFallback, ScanSegmentManifest, SegmentPayloadRange, SegmentReadExecutionReport,
        SegmentSummary,
    },
    NodeRecord,
};
use hawdb_core::schema::LabelId;
use hawdb_core::{HawDBError, Result, Value};
use hawdb_integrity::checksum_u64 as checksum_bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

pub const SOURCE_SCAN_DESCRIPTOR_FILE: &str = "source_scan_segments.hawdb";
pub const SOURCE_SCAN_PAYLOAD_FILE: &str = "source_scan_segment_payloads.hawdb";

pub const SOURCE_SCAN_ARTIFACT_ID: u64 = 1;
pub const SOURCE_SCAN_TARGET_ROWS: usize = 128;
const SOURCE_SCAN_DESCRIPTOR_HEADER: &str = "HAWDB_SOURCE_SCAN_SEGMENTS_V1";
const SOURCE_SCAN_SEGMENT_HEADER: &str = "HAWDB_SOURCE_SCAN_SEGMENT_V1";
const UNIQUE_KEY_FIELDS: &[&str] = &["id"];

#[derive(Debug, Clone)]
pub struct SourceScanProjection {
    graph_epoch: u64,
    segments: Vec<SourceScanSegment>,
    // One outer COW root preserves the original records without copying their
    // property maps. Segment intervals identify at most 128 matching rows.
    source_nodes: Option<CowSegmentedMap<NodeId, NodeRecord>>,
}

#[derive(Debug, Clone, Copy)]
pub struct SourceScanPublication {
    graph_epoch: u64,
    descriptor_checksum: u64,
}

impl SourceScanPublication {
    pub const fn graph_epoch(self) -> u64 {
        self.graph_epoch
    }

    pub const fn descriptor_checksum(self) -> u64 {
        self.descriptor_checksum
    }
}

#[derive(Debug, Clone)]
struct SourceScanSegment {
    summary: SegmentSummary,
    rows: Vec<SourceScanRow>,
    pinned_bounds: Option<(NodeId, NodeId, LabelId)>,
    payload_range: Option<SegmentPayloadRange>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SourceScanRow {
    pub node_id: u64,
    pub properties: BTreeMap<String, Value>,
}

const MAX_SOURCE_CANDIDATE_ROWS: usize = 10_000;

/// A bounded page request over checkpoint-published Source candidates.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceCandidateScanRequest {
    pub predicate: ScanPredicate,
    pub after: Option<SourceCandidateCursor>,
    pub limit: usize,
    pub max_payload_bytes: usize,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCandidateCursor {
    pub created_at: Option<Value>,
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCandidateRow {
    pub node_id: u64,
    pub source_id: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceCandidateScanOrigin {
    Sidecar {
        graph_epoch: u64,
        skipped_segment_count: usize,
    },
    CanonicalFallback {
        reason: ScanSegmentFallback,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceCandidateScanOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<SourceCandidateRow>,
    pub next_cursor: Option<SourceCandidateCursor>,
    pub origin: SourceCandidateScanOrigin,
    pub read_report: Option<SegmentReadExecutionReport>,
}

#[doc(hidden)]
pub fn validate_source_candidate_scan_request(request: &SourceCandidateScanRequest) -> Result<()> {
    if request.limit == 0 {
        return Err(HawDBError::Semantic(
            "knowledge source candidate scan requires a positive limit".to_string(),
        ));
    }
    if request.limit > MAX_SOURCE_CANDIDATE_ROWS {
        return Err(HawDBError::Semantic(format!(
            "knowledge source candidate scan limit {} exceeds {MAX_SOURCE_CANDIDATE_ROWS}",
            request.limit
        )));
    }
    if request.max_payload_bytes == 0 {
        return Err(HawDBError::Semantic(
            "knowledge source candidate scan requires a positive payload budget".to_string(),
        ));
    }
    if request
        .property_names
        .iter()
        .any(|name| name.trim().is_empty())
    {
        return Err(HawDBError::Semantic(
            "knowledge source candidate scan requires non-empty property names".to_string(),
        ));
    }
    Ok(())
}

/// Adds one canonical node to a bounded Source candidate page.
#[doc(hidden)]
pub fn select_source_candidate(
    nodes: &mut Vec<NodeRecord>,
    mut node: NodeRecord,
    request: &SourceCandidateScanRequest,
) -> Result<()> {
    if request
        .after
        .as_ref()
        .is_some_and(|after| !compare_source_candidate_to_cursor(&node, after).is_gt())
    {
        return Ok(());
    }
    node.properties.retain(|name, _| {
        name == "id" || name == "created_at" || request.property_names.contains(name)
    });
    let insertion = nodes
        .binary_search_by(|existing| compare_source_candidates(existing, &node))
        .unwrap_or_else(|index| index);
    nodes.insert(insertion, node);
    if nodes.len() > request.limit.saturating_add(1) {
        nodes.pop();
    }
    let payload_bytes = nodes
        .iter()
        .map(estimated_source_candidate_payload_bytes)
        .fold(0usize, usize::saturating_add);
    if payload_bytes > request.max_payload_bytes {
        return Err(HawDBError::Execution(format!(
            "knowledge source candidate payload budget exceeded: estimated_payload_bytes={payload_bytes}, max_payload_bytes={}",
            request.max_payload_bytes
        )));
    }
    Ok(())
}

/// Builds the externally visible candidate page from canonical source nodes.
#[doc(hidden)]
pub fn render_source_candidate_page(
    graph_commit_epoch: u64,
    mut nodes: Vec<NodeRecord>,
    request: &SourceCandidateScanRequest,
    origin: SourceCandidateScanOrigin,
    read_report: Option<SegmentReadExecutionReport>,
) -> Result<SourceCandidateScanOutput> {
    let mut bounded = Vec::with_capacity(request.limit.saturating_add(1));
    for node in nodes.drain(..) {
        select_source_candidate(&mut bounded, node, request)?;
    }
    nodes = bounded;
    let has_more = nodes.len() > request.limit;
    nodes.truncate(request.limit);
    let next_cursor = has_more
        .then(|| {
            nodes.last().map(|node| SourceCandidateCursor {
                created_at: node.properties.get("created_at").cloned(),
                node_id: node.id.0,
            })
        })
        .flatten();
    let rows = nodes
        .into_iter()
        .map(|node| SourceCandidateRow {
            node_id: node.id.0,
            source_id: node
                .properties
                .get("id")
                .and_then(|value| matches!(value, Value::String(_)).then(|| value.clone()))
                .and_then(|value| match value {
                    Value::String(value) => Some(value),
                    _ => None,
                }),
            properties: node
                .properties
                .iter()
                .filter(|(name, _)| request.property_names.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    Ok(SourceCandidateScanOutput {
        graph_commit_epoch,
        rows,
        next_cursor,
        origin,
        read_report,
    })
}

fn estimated_source_candidate_payload_bytes(node: &NodeRecord) -> usize {
    node.properties
        .iter()
        .fold(16usize, |bytes, (name, value)| {
            bytes
                .saturating_add(name.len())
                .saturating_add(estimated_value_bytes(value))
        })
}

fn estimated_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Uuid(_) => 16,
        Value::List(values) => values
            .iter()
            .map(estimated_value_bytes)
            .fold(16usize, usize::saturating_add),
        Value::Map(values) => values.iter().fold(16usize, |bytes, (name, value)| {
            bytes
                .saturating_add(name.len())
                .saturating_add(estimated_value_bytes(value))
        }),
    }
}

fn compare_source_candidates(left: &NodeRecord, right: &NodeRecord) -> std::cmp::Ordering {
    compare_source_candidate_keys(
        left.properties.get("created_at"),
        left.id.0,
        right.properties.get("created_at"),
        right.id.0,
    )
}

fn compare_source_candidate_to_cursor(
    node: &NodeRecord,
    cursor: &SourceCandidateCursor,
) -> std::cmp::Ordering {
    compare_source_candidate_keys(
        node.properties.get("created_at"),
        node.id.0,
        cursor.created_at.as_ref(),
        cursor.node_id,
    )
}

fn compare_source_candidate_keys(
    left_created_at: Option<&Value>,
    left_node_id: u64,
    right_created_at: Option<&Value>,
    right_node_id: u64,
) -> std::cmp::Ordering {
    right_created_at
        .cmp(&left_created_at)
        .then_with(|| right_node_id.cmp(&left_node_id))
}

pub fn build<'a>(
    graph_epoch: u64,
    source_label_id: Option<LabelId>,
    nodes: impl Iterator<Item = &'a NodeRecord>,
) -> SourceScanProjection {
    build_with_work_context(
        graph_epoch,
        source_label_id,
        nodes,
        &CheckpointWorkContext::default(),
    )
    .expect("default source scan build context cannot stop")
}

#[doc(hidden)]
pub fn build_with_work_context<'a>(
    graph_epoch: u64,
    source_label_id: Option<LabelId>,
    mut nodes: impl Iterator<Item = &'a NodeRecord>,
    work: &CheckpointWorkContext,
) -> Result<SourceScanProjection> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    let mut segments = Vec::new();
    if let Some(source_label_id) = source_label_id {
        let mut rows = Vec::with_capacity(SOURCE_SCAN_TARGET_ROWS);
        loop {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let Some(node) = nodes.next() else {
                unit.finish();
                break;
            };
            if node.labels.contains(&source_label_id) {
                rows.push(SourceScanRow {
                    node_id: node.id.0,
                    properties: node.properties.clone(),
                });
            }
            unit.finish();
            if rows.len() == SOURCE_SCAN_TARGET_ROWS {
                let chunk =
                    std::mem::replace(&mut rows, Vec::with_capacity(SOURCE_SCAN_TARGET_ROWS));
                segments.push(SourceScanSegment::from_rows(
                    segments.len() as u64,
                    chunk,
                    work,
                )?);
            }
        }
        if !rows.is_empty() {
            segments.push(SourceScanSegment::from_rows(
                segments.len() as u64,
                rows,
                work,
            )?);
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(SourceScanProjection {
        graph_epoch,
        segments,
        source_nodes: None,
    })
}

/// Production preparation pins the captured COW root rather than cloning
/// every Source property map. The temporary record-reference array is admitted
/// before allocation. Summary/descriptor ownership has separate resource gaps.
pub(crate) fn build_with_pinned_nodes(
    graph_epoch: u64,
    source_label_id: Option<LabelId>,
    nodes: &CowSegmentedMap<NodeId, NodeRecord>,
    work: &CheckpointWorkContext,
) -> Result<SourceScanProjection> {
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    let mut segments = Vec::new();
    if let Some(label) = source_label_id {
        let mut rows = CheckpointValues::new(SOURCE_SCAN_TARGET_ROWS, work)
            .map_err(HawDBError::from_storage_error)?;
        for node in nodes.values() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let matches = node.labels.contains(&label);
            unit.finish();
            if !matches {
                continue;
            }
            rows.push(node, work)
                .map_err(HawDBError::from_storage_error)?;
            if rows.as_slice().len() == SOURCE_SCAN_TARGET_ROWS {
                segments.push(SourceScanSegment::from_pinned_rows(
                    segments.len() as u64,
                    rows.as_slice(),
                    label,
                    work,
                )?);
                drop(rows);
                rows = CheckpointValues::new(SOURCE_SCAN_TARGET_ROWS, work)
                    .map_err(HawDBError::from_storage_error)?;
            }
        }
        if !rows.as_slice().is_empty() {
            segments.push(SourceScanSegment::from_pinned_rows(
                segments.len() as u64,
                rows.as_slice(),
                label,
                work,
            )?);
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(SourceScanProjection {
        graph_epoch,
        segments,
        source_nodes: Some(nodes.clone()),
    })
}

struct TemporarySourceScanPath(Option<PathBuf>);

impl Drop for TemporarySourceScanPath {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = fs::remove_file(path);
        }
    }
}

pub fn write(path: &Path, projection: &mut SourceScanProjection) -> Result<SourceScanPublication> {
    write_with_work_context(path, projection, &CheckpointWorkContext::default())
}

#[doc(hidden)]
pub fn write_with_work_context(
    path: &Path,
    projection: &mut SourceScanProjection,
    work: &CheckpointWorkContext,
) -> Result<SourceScanPublication> {
    let payload_path = path.join(SOURCE_SCAN_PAYLOAD_FILE);
    let payload_tmp_path = payload_path.with_extension("hawdb.tmp");
    let mut payload_temporary = TemporarySourceScanPath(None);
    let mut offset = 0u64;
    {
        let mut file = {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
            let file = File::create(&payload_tmp_path)?;
            payload_temporary.0 = Some(payload_tmp_path.clone());
            unit.finish();
            file
        };
        for segment in &mut projection.segments {
            let payload = if let Some((first, last, label)) = segment.pinned_bounds {
                let nodes = projection.source_nodes.as_ref().ok_or_else(|| {
                    HawDBError::StorageIntegrity(
                        "source scan segment lost its pinned records".into(),
                    )
                })?;
                encode_segment_records_with_work_context(
                    nodes.range(&first, &last).map(|(_, node)| {
                        (node.labels.contains(&label), node.id.0, &node.properties)
                    }),
                    work,
                )?
            } else {
                encode_segment_payload_with_work_context(&segment.rows, work)?
            };
            let length = u64::try_from(payload.len()).map_err(|_| {
                HawDBError::Storage(format!(
                    "source scan segment {} payload exceeds supported range length",
                    segment.summary.segment_id
                ))
            })?;
            segment.payload_range = Some(SegmentPayloadRange {
                artifact_id: SOURCE_SCAN_ARTIFACT_ID,
                offset,
                length: NonZeroU64::new(length).ok_or_else(|| {
                    HawDBError::Storage("source scan segment payload is empty".to_string())
                })?,
                checksum: work
                    .checksum(&payload)
                    .map_err(HawDBError::from_storage_error)?,
            });
            for block in payload.chunks(64 * 1024) {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
                file.write_all(block)?;
                unit.finish();
            }
            offset = offset.checked_add(length).ok_or_else(|| {
                HawDBError::Storage("source scan payload artifact length overflow".to_string())
            })?;
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        file.sync_all()?;
        unit.finish();
    }
    {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        durable_replace_file(&payload_tmp_path, &payload_path)?;
        payload_temporary.0 = None;
        unit.finish();
    }

    let descriptor_path = path.join(SOURCE_SCAN_DESCRIPTOR_FILE);
    let descriptor_tmp_path = descriptor_path.with_extension("hawdb.tmp");
    let mut descriptor_temporary = TemporarySourceScanPath(None);
    let body = encode_descriptor_with_work_context(projection, work)?;
    let descriptor_checksum = work
        .checksum(body.as_bytes())
        .map_err(HawDBError::from_storage_error)?;
    let data = format!("{body}checksum\t{descriptor_checksum}\n");
    {
        let mut file = {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
            let file = File::create(&descriptor_tmp_path)?;
            descriptor_temporary.0 = Some(descriptor_tmp_path.clone());
            unit.finish();
            file
        };
        for block in data.as_bytes().chunks(64 * 1024) {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
            file.write_all(block)?;
            unit.finish();
        }
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        file.sync_all()?;
        unit.finish();
    }
    {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let _wave = work.io_wave().map_err(HawDBError::from_storage_error)?;
        durable_replace_file(&descriptor_tmp_path, &descriptor_path)?;
        descriptor_temporary.0 = None;
        unit.finish();
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(SourceScanPublication {
        graph_epoch: projection.graph_epoch,
        descriptor_checksum,
    })
}

pub fn load(
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
                    HawDBError::Storage("source scan descriptor missing payload range".to_string())
                })?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ScanSegmentManifest::new(expected_graph_epoch, segments)
        .map(Some)
        .map_err(HawDBError::from_storage_error)
}

pub fn decode_payload(payload: &[u8]) -> Result<Vec<SourceScanRow>> {
    let text = read_durable_text_bytes(payload, "source scan segment")?;
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
                return Err(HawDBError::Storage(format!(
                    "invalid source scan segment line: {line}"
                )));
            }
        }
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].node_id >= pair[1].node_id)
    {
        return Err(HawDBError::Storage(
            "source scan segment rows are not strictly ordered".to_string(),
        ));
    }
    Ok(rows)
}

impl SourceScanSegment {
    fn from_rows(
        segment_id: u64,
        rows: Vec<SourceScanRow>,
        work: &CheckpointWorkContext,
    ) -> Result<Self> {
        let summary = build_segment_summary(segment_id, &rows, work)?;
        Ok(Self {
            summary,
            rows,
            pinned_bounds: None,
            payload_range: None,
        })
    }

    fn from_pinned_rows(
        segment_id: u64,
        rows: &[&NodeRecord],
        label: LabelId,
        work: &CheckpointWorkContext,
    ) -> Result<Self> {
        let summary = build_segment_summary(segment_id, rows, work)?;
        let first = rows.first().expect("nonempty pinned segment").id;
        let last = rows.last().expect("nonempty pinned segment").id;
        Ok(Self {
            summary,
            rows: Vec::new(),
            pinned_bounds: Some((first, last, label)),
            payload_range: None,
        })
    }
}

trait SourceScanProperties {
    fn properties(&self) -> &BTreeMap<String, Value>;
}

impl SourceScanProperties for SourceScanRow {
    fn properties(&self) -> &BTreeMap<String, Value> {
        &self.properties
    }
}

impl SourceScanProperties for &NodeRecord {
    fn properties(&self) -> &BTreeMap<String, Value> {
        &self.properties
    }
}

fn build_segment_summary<R: SourceScanProperties>(
    segment_id: u64,
    rows: &[R],
    work: &CheckpointWorkContext,
) -> Result<SegmentSummary> {
    let mut summary = SegmentSummary::new(segment_id, rows.len() as u64);
    let mut fields = BTreeSet::new();
    for row in rows {
        for field in row.properties().keys() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            fields.insert(field.clone());
            unit.finish();
        }
    }
    for field in fields {
        let SourceScanFieldSummary {
            summary: field_summary,
            exact_values,
        } = build_field_summary(rows, &field, work)?;
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        summary.insert_field(field.clone(), field_summary);
        unit.finish();
        if !exact_values.is_empty() {
            // Exact row cursors are bounded by one immutable segment.
            for (value, row_ids) in &exact_values {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                let field_summary = summary
                    .fields
                    .get_mut(&field)
                    .expect("source scan field summary was inserted");
                *field_summary =
                    std::mem::replace(field_summary, FieldSummary::new(rows.len() as u64))
                        .with_exact_row_ids(value.clone(), row_ids.iter().copied());
                unit.finish();
            }
        }
    }
    Ok(summary)
}

struct SourceScanFieldSummary {
    summary: FieldSummary,
    exact_values: Vec<(Value, Vec<u64>)>,
}

fn build_field_summary<R: SourceScanProperties>(
    rows: &[R],
    field: &str,
    work: &CheckpointWorkContext,
) -> Result<SourceScanFieldSummary> {
    let mut present_count = 0u64;
    let mut null_count = 0u64;
    let mut scalar_values = Vec::new();
    let mut exact_values = BTreeMap::<String, (Value, Vec<u64>)>::new();
    let mut numeric_min = None::<f64>;
    let mut numeric_max = None::<f64>;
    let mut datetime_min = None::<i64>;
    let mut datetime_max = None::<i64>;

    for (row_id, row) in rows.iter().enumerate() {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let Some(value) = row.properties().get(field) else {
            unit.finish();
            continue;
        };
        present_count += 1;
        if matches!(value, Value::Null) {
            null_count += 1;
            unit.finish();
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
        unit.finish();
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
    Ok(SourceScanFieldSummary {
        summary,
        exact_values: exact_values.into_values().collect(),
    })
}

fn is_scalar(value: &Value) -> bool {
    match value {
        Value::Bool(_) | Value::Int(_) | Value::String(_) | Value::Uuid(_) => true,
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

#[cfg(test)]
fn encode_segment_payload(rows: &[SourceScanRow]) -> Result<Vec<u8>> {
    encode_segment_payload_with_work_context(rows, &CheckpointWorkContext::default())
}

fn encode_segment_payload_with_work_context(
    rows: &[SourceScanRow],
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>> {
    encode_segment_records_with_work_context(
        rows.iter().map(|row| (true, row.node_id, &row.properties)),
        work,
    )
}

fn encode_segment_records_with_work_context<'a>(
    rows: impl Iterator<Item = (bool, u64, &'a BTreeMap<String, Value>)>,
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>> {
    let body = encode_segment_text_with_work_context(rows, work)?;
    encode_durable_text_with_work_context(&body, DurableCompression::default(), work)
}

fn encode_segment_text_with_work_context<'a>(
    rows: impl Iterator<Item = (bool, u64, &'a BTreeMap<String, Value>)>,
    work: &CheckpointWorkContext,
) -> Result<CheckpointText> {
    let mut body = CheckpointText::new();
    body.append(SOURCE_SCAN_SEGMENT_HEADER, work)?;
    body.append("\n", work)?;
    for (selected, node_id, properties) in rows {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        unit.finish();
        if !selected {
            continue;
        }
        body.fields(format_args!("row\t{node_id}\t"), work)?;
        body.properties(properties, work)?;
        body.append("\n", work)?;
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(body)
}

#[cfg(test)]
fn encode_descriptor(projection: &SourceScanProjection) -> Result<String> {
    encode_descriptor_with_work_context(projection, &CheckpointWorkContext::default())
}

fn encode_descriptor_with_work_context(
    projection: &SourceScanProjection,
    work: &CheckpointWorkContext,
) -> Result<String> {
    let mut body = format!(
        "{SOURCE_SCAN_DESCRIPTOR_HEADER}\ngraph_epoch\t{}\n",
        projection.graph_epoch
    );
    for segment in &projection.segments {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let range = segment.payload_range.ok_or_else(|| {
            HawDBError::Storage("source scan segment has no payload range".to_string())
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
        unit.finish();
        for (field, summary) in &segment.summary.fields {
            let dictionary = encode_enum_dictionary(summary.enum_dictionary.as_ref(), work)?;
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
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
                dictionary,
            ));
            unit.finish();
            for (value, row_ids) in &summary.exact_values {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                let encoded_value = match value {
                    crate::scan::ScanScalar::Bool(value) => encode_value(&Value::Bool(*value)),
                    crate::scan::ScanScalar::Int(value) => encode_value(&Value::Int(*value)),
                    crate::scan::ScanScalar::Float(value) => {
                        encode_value(&Value::Float(f64::from_bits(*value)))
                    }
                    crate::scan::ScanScalar::String(value) => {
                        encode_value(&Value::String(value.clone()))
                    }
                    crate::scan::ScanScalar::Binary(value) => {
                        encode_value(&Value::Binary(value.clone()))
                    }
                    crate::scan::ScanScalar::Uuid(value) => encode_value(&Value::Uuid(*value)),
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
                unit.finish();
            }
        }
    }
    Ok(body)
}

fn decode_descriptor(text: &str, expected_checksum: u64) -> Result<SourceScanProjection> {
    let (body, checksum) = text.rsplit_once("checksum\t").ok_or_else(|| {
        HawDBError::Storage("source scan descriptor missing checksum footer".to_string())
    })?;
    let checksum = parse_u64(checksum.trim(), "source scan checksum")?;
    if checksum != expected_checksum || checksum_bytes(body.as_bytes()) != checksum {
        return Err(HawDBError::Storage(
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
                        HawDBError::Storage("source scan payload length is zero".to_string())
                    })?;
                current = Some(SourceScanSegment {
                    summary: SegmentSummary::new(
                        parse_u64(raw_id, "source scan segment id")?,
                        parse_u64(raw_rows, "source scan row count")?,
                    ),
                    rows: Vec::new(),
                    pinned_bounds: None,
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
                    HawDBError::Storage("source scan field appears before a segment".to_string())
                })?;
                let row_count = segment.summary.row_count;
                let mut summary = FieldSummary::new(row_count)
                    .with_presence_counts(
                        parse_u64(raw_present, "source scan field present count")?,
                        parse_u64(raw_null, "source scan field null count")?,
                        parse_u64(raw_missing, "source scan field missing count")?,
                    )
                    .ok_or_else(|| {
                        HawDBError::Storage("invalid source scan field counts".to_string())
                    })?;
                if let (Some(min), Some(max)) = (
                    decode_optional_f64(raw_numeric_min)?,
                    decode_optional_f64(raw_numeric_max)?,
                ) {
                    summary = summary.with_numeric_min_max(min, max).ok_or_else(|| {
                        HawDBError::Storage("invalid source scan numeric range".to_string())
                    })?;
                }
                if let (Some(min), Some(max)) = (
                    decode_optional_i64(raw_datetime_min)?,
                    decode_optional_i64(raw_datetime_max)?,
                ) {
                    summary = summary.with_datetime_min_max(min, max).ok_or_else(|| {
                        HawDBError::Storage("invalid source scan datetime range".to_string())
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
                    HawDBError::Storage(
                        "source scan exact cursor appears before a segment".to_string(),
                    )
                })?;
                let field = decode_string(raw_field)?;
                let value = decode_value(&decode_string(raw_value)?)?;
                let row_ids = decode_row_ids(raw_row_ids)?;
                let summary = segment.summary.fields.remove(&field).ok_or_else(|| {
                    HawDBError::Storage(
                        "source scan exact cursor references unknown field".to_string(),
                    )
                })?;
                segment
                    .summary
                    .insert_field(field, summary.with_exact_row_ids(value, row_ids));
            }
            [""] => {}
            _ => {
                return Err(HawDBError::Storage(format!(
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
            HawDBError::Storage("source scan descriptor missing graph epoch".to_string())
        })?,
        segments,
        source_nodes: None,
    })
}

fn validate_payload_ranges(path: &Path, segments: &[SourceScanSegment]) -> Result<()> {
    let mut file = File::open(path)?;
    let artifact_len = file.metadata()?.len();
    for segment in segments {
        let range = segment.payload_range.ok_or_else(|| {
            HawDBError::Storage("source scan descriptor missing payload range".to_string())
        })?;
        if range.artifact_id != SOURCE_SCAN_ARTIFACT_ID {
            return Err(HawDBError::Storage(
                "source scan descriptor has unsupported artifact".to_string(),
            ));
        }
        let end = range
            .offset
            .checked_add(range.length.get())
            .ok_or_else(|| HawDBError::Storage("source scan payload range overflow".to_string()))?;
        if end > artifact_len {
            return Err(HawDBError::Storage(
                "source scan payload range exceeds artifact".to_string(),
            ));
        }
        let mut payload = vec![
            0;
            usize::try_from(range.length.get()).map_err(|_| {
                HawDBError::Storage("source scan payload range exceeds address space".to_string())
            })?
        ];
        file.seek(SeekFrom::Start(range.offset))?;
        file.read_exact(&mut payload)?;
        if checksum_bytes(&payload) != range.checksum {
            return Err(HawDBError::Storage(
                "source scan payload checksum mismatch".to_string(),
            ));
        }
        let rows = decode_payload(&payload)?;
        if rows.len() as u64 != segment.summary.row_count {
            return Err(HawDBError::Storage(
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

fn encode_enum_dictionary(
    dictionary: Option<&EnumDictionaryStats>,
    work: &CheckpointWorkContext,
) -> Result<String> {
    let mut encoded = String::new();
    if let Some(dictionary) = dictionary {
        for (index, value) in dictionary.values.iter().enumerate() {
            let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
            if index != 0 {
                encoded.push(':');
            }
            let value = match value {
                crate::scan::ScanScalar::Bool(value) => Value::Bool(*value),
                crate::scan::ScanScalar::Int(value) => Value::Int(*value),
                crate::scan::ScanScalar::Float(value) => Value::Float(f64::from_bits(*value)),
                crate::scan::ScanScalar::String(value) => Value::String(value.clone()),
                crate::scan::ScanScalar::Binary(value) => Value::Binary(value.clone()),
                crate::scan::ScanScalar::Uuid(value) => Value::Uuid(*value),
            };
            encoded.push_str(&encode_string(&encode_value(&value)));
            unit.finish();
        }
    }
    Ok(encoded)
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
        return Err(HawDBError::Storage(
            "source scan exact row ids are not ordered".to_string(),
        ));
    }
    Ok(row_ids)
}

#[cfg(test)]
mod reference;
#[cfg(test)]
mod tests;
