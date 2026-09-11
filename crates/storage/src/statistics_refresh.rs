//! Internal external-sort kernels for optimizer statistics refresh.
//!
//! Root storage owns graph traversal, user-option validation, epoch checks, and
//! publication. These kernels consume facts and return unpublished statistics.

use crate::text::{decode_string, decode_value, encode_string, encode_value};
use crate::{NodeId, NodeRecord};
use skein_core::{
    Catalog, GraphStatistics, IndexId, IndexStatisticsSample, LabelId, RelTypeId, Result,
    SkeinError, Value,
};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Lines, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

mod policy;
pub use policy::{
    adaptive_histogram_sample_limit, node_property_supports_optimizer_statistics,
    relationship_property_supports_optimizer_statistics, sample_histogram_values,
    MAX_BOUNDED_PATH_STAT_HOPS, MAX_PROPERTY_HISTOGRAM_VALUES,
};

static NEXT_REFRESH_ID: AtomicU64 = AtomicU64::new(1);
const INDEX_SAMPLE_OUTPUT_BYTES: usize = 96;

/// The unchanged sort/output limits copied from validated root options.
pub struct StatsRunOptions {
    pub memory_budget_bytes: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub max_generated_facts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StatsRecord {
    NodeProperty {
        label: LabelId,
        property: String,
        value: Value,
    },
    RelProperty {
        rel_type: RelTypeId,
        property: String,
        value: Value,
    },
    IndexEntry {
        index: IndexId,
        key: String,
    },
    RelSource {
        rel_type: RelTypeId,
        node: NodeId,
    },
    RelTarget {
        rel_type: RelTypeId,
        node: NodeId,
    },
    PathCount {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
    },
    PathSource {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        node: NodeId,
    },
    PathTarget {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        node: NodeId,
    },
    BoundedPathCount {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
    },
    BoundedPathSource {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
        node: NodeId,
    },
    BoundedPathTarget {
        source_label: LabelId,
        rel_type: RelTypeId,
        target_label: LabelId,
        hop: usize,
        node: NodeId,
    },
}

impl StatsRecord {
    fn estimated_bytes(&self) -> usize {
        let fixed = 64usize;
        match self {
            Self::NodeProperty {
                property, value, ..
            }
            | Self::RelProperty {
                property, value, ..
            } => fixed
                .saturating_add(property.len())
                .saturating_add(encode_value(value).len()),
            Self::IndexEntry { key, .. } => fixed.saturating_add(key.len()),
            _ => fixed,
        }
    }

    fn encode(&self) -> String {
        match self {
            Self::NodeProperty {
                label,
                property,
                value,
            } => format!(
                "np\t{}\t{}\t{}",
                label.0,
                encode_string(property),
                encode_string(&encode_value(value))
            ),
            Self::RelProperty {
                rel_type,
                property,
                value,
            } => format!(
                "rp\t{}\t{}\t{}",
                rel_type.0,
                encode_string(property),
                encode_string(&encode_value(value))
            ),
            Self::IndexEntry { index, key } => {
                format!("ix\t{}\t{}", index.0, encode_string(key))
            }
            Self::RelSource { rel_type, node } => {
                format!("rs\t{}\t{}", rel_type.0, node.0)
            }
            Self::RelTarget { rel_type, node } => {
                format!("rt\t{}\t{}", rel_type.0, node.0)
            }
            Self::PathCount {
                source_label,
                rel_type,
                target_label,
            } => format!("pc\t{}\t{}\t{}", source_label.0, rel_type.0, target_label.0),
            Self::PathSource {
                source_label,
                rel_type,
                target_label,
                node,
            } => format!(
                "ps\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, node.0
            ),
            Self::PathTarget {
                source_label,
                rel_type,
                target_label,
                node,
            } => format!(
                "pt\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, node.0
            ),
            Self::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            } => format!(
                "bc\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop
            ),
            Self::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            } => format!(
                "bs\t{}\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop, node.0
            ),
            Self::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            } => format!(
                "bt\t{}\t{}\t{}\t{}\t{}",
                source_label.0, rel_type.0, target_label.0, hop, node.0
            ),
        }
    }

    fn decode(line: &str) -> Result<Self> {
        let fields = line.split('\t').collect::<Vec<_>>();
        match fields.as_slice() {
            ["np", label, property, value] => Ok(Self::NodeProperty {
                label: LabelId(parse_u32_field(label, "label id")?),
                property: decode_string(property)?,
                value: decode_value(&decode_string(value)?)?,
            }),
            ["rp", rel_type, property, value] => Ok(Self::RelProperty {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                property: decode_string(property)?,
                value: decode_value(&decode_string(value)?)?,
            }),
            ["ix", index, key] => Ok(Self::IndexEntry {
                index: IndexId(parse_u32_field(index, "index id")?),
                key: decode_string(key)?,
            }),
            ["rs", rel_type, node] => Ok(Self::RelSource {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["rt", rel_type, node] => Ok(Self::RelTarget {
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["pc", source_label, rel_type, target_label] => Ok(Self::PathCount {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
            }),
            ["ps", source_label, rel_type, target_label, node] => Ok(Self::PathSource {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["pt", source_label, rel_type, target_label, node] => Ok(Self::PathTarget {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                node: NodeId(parse_u64_field(node, "node id")?),
            }),
            ["bc", source_label, rel_type, target_label, hop] => Ok(Self::BoundedPathCount {
                source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                hop: parse_usize_field(hop, "path hop")?,
            }),
            ["bs", source_label, rel_type, target_label, hop, node] => {
                Ok(Self::BoundedPathSource {
                    source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                    rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                    target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                    hop: parse_usize_field(hop, "path hop")?,
                    node: NodeId(parse_u64_field(node, "node id")?),
                })
            }
            ["bt", source_label, rel_type, target_label, hop, node] => {
                Ok(Self::BoundedPathTarget {
                    source_label: LabelId(parse_u32_field(source_label, "source label id")?),
                    rel_type: RelTypeId(parse_u32_field(rel_type, "relationship type id")?),
                    target_label: LabelId(parse_u32_field(target_label, "target label id")?),
                    hop: parse_usize_field(hop, "path hop")?,
                    node: NodeId(parse_u64_field(node, "node id")?),
                })
            }
            _ => Err(SkeinError::Storage(
                "invalid optimizer statistics spill record".to_string(),
            )),
        }
    }
}

fn parse_u32_field(raw: &str, name: &str) -> Result<u32> {
    raw.parse::<u32>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

fn parse_u64_field(raw: &str, name: &str) -> Result<u64> {
    raw.parse::<u64>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

fn parse_usize_field(raw: &str, name: &str) -> Result<usize> {
    raw.parse::<usize>()
        .map_err(|error| SkeinError::Storage(format!("invalid statistics {name}: {error}")))
}

// Refresh keys are transient and outer spill framing already escapes them. A
// length-prefixed encoding avoids the durable codec's hex expansion and lets
// admission reject an oversized key before allocating its buffer.
fn index_statistics_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 2,
        Value::Int(value) => 2usize.saturating_add(value.to_string().len()),
        Value::Float(value) => 2usize.saturating_add(value.to_bits().to_string().len()),
        Value::String(value) => 2usize
            .saturating_add(value.len().to_string().len())
            .saturating_add(value.len()),
        Value::Binary(value) => 2usize
            .saturating_add(value.len().to_string().len())
            .saturating_add(value.len().saturating_mul(2)),
        Value::Uuid(_) => 34,
        Value::List(values) => values.iter().fold(
            2usize.saturating_add(values.len().to_string().len()),
            |bytes, value| {
                let value_bytes = index_statistics_value_bytes(value);
                bytes
                    .saturating_add(value_bytes.to_string().len())
                    .saturating_add(1)
                    .saturating_add(value_bytes)
            },
        ),
        Value::Map(values) => values.iter().fold(
            2usize.saturating_add(values.len().to_string().len()),
            |bytes, (key, value)| {
                let value_bytes = index_statistics_value_bytes(value);
                bytes
                    .saturating_add(key.len().to_string().len())
                    .saturating_add(1)
                    .saturating_add(key.len())
                    .saturating_add(value_bytes.to_string().len())
                    .saturating_add(1)
                    .saturating_add(value_bytes)
            },
        ),
    }
}

fn append_index_statistics_value(encoded: &mut String, value: &Value) {
    match value {
        Value::Null => encoded.push('n'),
        Value::Bool(value) => encoded.push_str(if *value { "b1" } else { "b0" }),
        Value::Int(value) => {
            encoded.push('i');
            encoded.push_str(&value.to_string());
            encoded.push(';');
        }
        Value::Float(value) => {
            encoded.push('f');
            encoded.push_str(&value.to_bits().to_string());
            encoded.push(';');
        }
        Value::String(value) => {
            encoded.push('s');
            encoded.push_str(&value.len().to_string());
            encoded.push(':');
            encoded.push_str(value);
        }
        Value::Binary(value) => {
            use std::fmt::Write as _;

            encoded.push('x');
            encoded.push_str(&value.len().to_string());
            encoded.push(':');
            for byte in value {
                write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
            }
        }
        Value::Uuid(value) => {
            encoded.push('u');
            encoded.push_str(&value.to_string());
        }
        Value::List(values) => {
            encoded.push('l');
            encoded.push_str(&values.len().to_string());
            encoded.push(':');
            for value in values {
                let value_bytes = index_statistics_value_bytes(value);
                encoded.push_str(&value_bytes.to_string());
                encoded.push(':');
                append_index_statistics_value(encoded, value);
            }
        }
        Value::Map(values) => {
            encoded.push('m');
            encoded.push_str(&values.len().to_string());
            encoded.push(':');
            for (key, value) in values {
                encoded.push_str(&key.len().to_string());
                encoded.push(':');
                encoded.push_str(key);
                let value_bytes = index_statistics_value_bytes(value);
                encoded.push_str(&value_bytes.to_string());
                encoded.push(':');
                append_index_statistics_value(encoded, value);
            }
        }
    }
}

fn index_statistics_key_bytes(values: &[&Value]) -> usize {
    values.iter().fold(0usize, |bytes, value| {
        let value_bytes = index_statistics_value_bytes(value);
        bytes
            .saturating_add(value_bytes.to_string().len())
            .saturating_add(1)
            .saturating_add(value_bytes)
    })
}

fn encode_index_statistics_key(values: &[&Value], key_bytes: usize) -> String {
    let mut key = String::with_capacity(key_bytes);
    for value in values {
        let value_bytes = index_statistics_value_bytes(value);
        key.push_str(&value_bytes.to_string());
        key.push(':');
        append_index_statistics_value(&mut key, value);
    }
    debug_assert_eq!(key.len(), key_bytes);
    key
}

pub struct StatsRunWriter<'a> {
    directory: &'a Path,
    catalog: &'a Catalog,
    options: &'a StatsRunOptions,
    scalar_indexes_by_label: ScalarIndexesByLabel,
    composite_indexes_by_label: CompositeIndexesByLabel,
    index_ids: Vec<IndexId>,
    chunk: Vec<StatsRecord>,
    chunk_bytes: usize,
    excluded_property_groups: BTreeSet<PropertyGroupKey>,
    excluded_property_group_bytes: usize,
    peak_buffer_bytes: usize,
    generated_facts: u64,
    spilled_bytes: u64,
    runs: Vec<PathBuf>,
}

type ScalarIndexesByLabel = BTreeMap<LabelId, Arc<[(IndexId, String)]>>;
type CompositeIndexesByLabel = BTreeMap<LabelId, Arc<[(IndexId, Vec<String>)]>>;

impl<'a> StatsRunWriter<'a> {
    pub fn new(directory: &'a Path, catalog: &'a Catalog, options: &'a StatsRunOptions) -> Self {
        let mut scalar_indexes_by_label = BTreeMap::<LabelId, Vec<(IndexId, String)>>::new();
        let mut composite_indexes_by_label =
            BTreeMap::<LabelId, Vec<(IndexId, Vec<String>)>>::new();
        let mut index_ids = Vec::new();
        for index in catalog
            .property_indexes()
            .filter(|index| catalog.supports_index_statistics(index.id))
        {
            scalar_indexes_by_label
                .entry(index.label_id)
                .or_default()
                .push((index.id, index.property.clone()));
            index_ids.push(index.id);
        }
        for index in catalog
            .composite_property_indexes()
            .filter(|index| catalog.supports_index_statistics(index.id))
        {
            composite_indexes_by_label
                .entry(index.label_id)
                .or_default()
                .push((index.id, index.properties.clone()));
            index_ids.push(index.id);
        }
        let scalar_indexes_by_label = scalar_indexes_by_label
            .into_iter()
            .map(|(label, indexes)| (label, Arc::from(indexes)))
            .collect();
        let composite_indexes_by_label = composite_indexes_by_label
            .into_iter()
            .map(|(label, indexes)| (label, Arc::from(indexes)))
            .collect();
        index_ids.sort_unstable();
        Self {
            directory,
            catalog,
            options,
            scalar_indexes_by_label,
            composite_indexes_by_label,
            index_ids,
            chunk: Vec::new(),
            chunk_bytes: 0,
            excluded_property_groups: BTreeSet::new(),
            excluded_property_group_bytes: 0,
            peak_buffer_bytes: 0,
            generated_facts: 0,
            spilled_bytes: 0,
            runs: Vec::new(),
        }
    }

    pub fn push_node_property(
        &mut self,
        label: LabelId,
        property: &str,
        value: &Value,
    ) -> Result<()> {
        let key = PropertyGroupKey::Node(label, property.to_string());
        if !node_property_supports_optimizer_statistics(Some(self.catalog), label, property, value)
        {
            return self.exclude_property_group(key);
        }
        if self.excluded_property_groups.contains(&key) {
            return Ok(());
        }
        let PropertyGroupKey::Node(_, property) = key else {
            unreachable!("node property key changed variant")
        };
        self.push(StatsRecord::NodeProperty {
            label,
            property,
            value: value.clone(),
        })
    }

    pub fn push_relationship_property(
        &mut self,
        rel_type: RelTypeId,
        property: &str,
        value: &Value,
    ) -> Result<()> {
        let key = PropertyGroupKey::Relationship(rel_type, property.to_string());
        if !relationship_property_supports_optimizer_statistics(
            Some(self.catalog),
            rel_type,
            property,
            value,
        ) {
            return self.exclude_property_group(key);
        }
        if self.excluded_property_groups.contains(&key) {
            return Ok(());
        }
        let PropertyGroupKey::Relationship(_, property) = key else {
            unreachable!("relationship property key changed variant")
        };
        self.push(StatsRecord::RelProperty {
            rel_type,
            property,
            value: value.clone(),
        })
    }

    pub fn push_node_index_entries(&mut self, node: &NodeRecord) -> Result<()> {
        for label in &node.labels {
            if let Some(indexes) = self.scalar_indexes_by_label.get(label).cloned() {
                for (index, property) in indexes.iter() {
                    let Some(value) = node.properties.get(property) else {
                        continue;
                    };
                    self.push(StatsRecord::IndexEntry {
                        index: *index,
                        key: self.admit_and_encode_index_key(&[value])?,
                    })?;
                }
            }
            if let Some(indexes) = self.composite_indexes_by_label.get(label).cloned() {
                for (index, properties) in indexes.iter() {
                    let Some(values) = properties
                        .iter()
                        .map(|property| node.properties.get(property))
                        .collect::<Option<Vec<_>>>()
                    else {
                        continue;
                    };
                    self.push(StatsRecord::IndexEntry {
                        index: *index,
                        key: self.admit_and_encode_index_key(&values)?,
                    })?;
                }
            }
        }
        Ok(())
    }

    fn admit_and_encode_index_key(&self, values: &[&Value]) -> Result<String> {
        let key_bytes = index_statistics_key_bytes(values);
        let record_bytes = 64usize.saturating_add(key_bytes);
        if record_bytes.saturating_add(self.excluded_property_group_bytes)
            > self.options.memory_budget_bytes
        {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics fact uses {record_bytes} bytes, exceeding memory_budget_bytes {}",
                self.options.memory_budget_bytes
            )));
        }
        Ok(encode_index_statistics_key(values, key_bytes))
    }

    fn exclude_property_group(&mut self, key: PropertyGroupKey) -> Result<()> {
        if self.excluded_property_groups.contains(&key) {
            return Ok(());
        }
        let key_bytes = key.estimated_bytes();
        if !self.chunk.is_empty()
            && self
                .excluded_property_group_bytes
                .saturating_add(key_bytes)
                .saturating_add(self.chunk_bytes)
                > self.options.memory_budget_bytes
        {
            self.flush()?;
        }
        let next_excluded_bytes = self.excluded_property_group_bytes.saturating_add(key_bytes);
        if next_excluded_bytes > self.options.memory_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics excluded-property state exceeds memory_budget_bytes {}",
                self.options.memory_budget_bytes
            )));
        }
        self.excluded_property_groups.insert(key);
        self.excluded_property_group_bytes = next_excluded_bytes;
        self.peak_buffer_bytes = self.peak_buffer_bytes.max(
            self.chunk_bytes
                .saturating_add(self.excluded_property_group_bytes),
        );
        Ok(())
    }

    pub fn push(&mut self, record: StatsRecord) -> Result<()> {
        self.generated_facts = self.generated_facts.saturating_add(1);
        if self.generated_facts > self.options.max_generated_facts {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_generated_facts {}",
                self.options.max_generated_facts
            )));
        }
        let record_bytes = record.estimated_bytes();
        if record_bytes.saturating_add(self.excluded_property_group_bytes)
            > self.options.memory_budget_bytes
        {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics fact uses {record_bytes} bytes, exceeding memory_budget_bytes {}",
                self.options.memory_budget_bytes
            )));
        }
        if !self.chunk.is_empty()
            && self
                .chunk_bytes
                .saturating_add(record_bytes)
                .saturating_add(self.excluded_property_group_bytes)
                > self.options.memory_budget_bytes
        {
            self.flush()?;
        }
        self.chunk.push(record);
        self.chunk_bytes = self.chunk_bytes.saturating_add(record_bytes);
        self.peak_buffer_bytes = self.peak_buffer_bytes.max(
            self.chunk_bytes
                .saturating_add(self.excluded_property_group_bytes),
        );
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.chunk.is_empty() {
            return Ok(());
        }
        if self.runs.len() == self.options.max_spill_runs {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh exceeded max_spill_runs {}",
                self.options.max_spill_runs
            )));
        }
        self.chunk.sort_unstable();
        let path = self
            .directory
            .join(format!("run.{:08}.skein", self.runs.len()));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to create optimizer statistics spill run: {error}"
                ))
            })?;
        let mut output = BufWriter::new(file);
        let mut run_bytes = 0u64;
        for record in &self.chunk {
            let encoded = record.encode();
            let encoded_bytes = encoded.len().saturating_add(1) as u64;
            run_bytes = run_bytes.saturating_add(encoded_bytes);
            if self.spilled_bytes.saturating_add(run_bytes) > self.options.max_spill_bytes {
                return Err(SkeinError::Execution(format!(
                    "optimizer statistics refresh exceeded max_spill_bytes {}",
                    self.options.max_spill_bytes
                )));
            }
            output.write_all(encoded.as_bytes()).map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to write optimizer statistics spill run: {error}"
                ))
            })?;
            output.write_all(b"\n").map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to write optimizer statistics spill run: {error}"
                ))
            })?;
        }
        output.flush().map_err(|error| {
            SkeinError::Storage(format!(
                "failed to flush optimizer statistics spill run: {error}"
            ))
        })?;
        self.spilled_bytes = self.spilled_bytes.saturating_add(run_bytes);
        self.runs.push(path);
        self.chunk.clear();
        self.chunk_bytes = 0;
        Ok(())
    }

    pub fn finish(
        mut self,
        mut statistics: GraphStatistics,
    ) -> Result<(GraphStatistics, StatsMergeReport)> {
        self.flush()?;
        let mut readers = Vec::with_capacity(self.runs.len());
        for path in &self.runs {
            let file = File::open(path).map_err(|error| {
                SkeinError::Storage(format!(
                    "failed to open optimizer statistics spill run: {error}"
                ))
            })?;
            readers.push(BufReader::new(file).lines());
        }
        let mut heap = BinaryHeap::new();
        for (run, reader) in readers.iter_mut().enumerate() {
            if let Some(record) = read_next_record(reader)? {
                heap.push(Reverse((record, run)));
            }
        }
        let excluded_property_group_count = self
            .excluded_property_groups
            .iter()
            .filter(|key| matches!(key, PropertyGroupKey::Node(_, _)))
            .count();
        let excluded_relationship_property_group_count = self
            .excluded_property_groups
            .len()
            .saturating_sub(excluded_property_group_count);
        let accumulator_memory_budget = self
            .options
            .memory_budget_bytes
            .saturating_sub(self.excluded_property_group_bytes);
        let index_sample_output_bytes = self
            .index_ids
            .len()
            .saturating_mul(INDEX_SAMPLE_OUTPUT_BYTES);
        if index_sample_output_bytes > accumulator_memory_budget {
            return Err(SkeinError::Execution(format!(
                "optimizer index statistics output state exceeds memory_budget_bytes {}",
                self.options.memory_budget_bytes
            )));
        }
        for index_id in &self.index_ids {
            statistics
                .index_samples
                .insert(*index_id, IndexStatisticsSample::exact(0, 0));
        }
        let mut accumulator = StatsAccumulator::new(
            statistics,
            accumulator_memory_budget,
            index_sample_output_bytes,
            self.excluded_property_groups,
        );
        while let Some(Reverse((record, run))) = heap.pop() {
            accumulator.consume(record)?;
            if let Some(next) = read_next_record(&mut readers[run])? {
                heap.push(Reverse((next, run)));
            }
        }
        let (statistics, output_statistics_bytes) = accumulator.finish()?;
        Ok((
            statistics,
            StatsMergeReport {
                generated_facts: self.generated_facts,
                spill_run_count: self.runs.len(),
                spilled_bytes: self.spilled_bytes,
                peak_buffer_bytes: self.peak_buffer_bytes.max(
                    accumulator_peak_bytes(output_statistics_bytes)
                        .saturating_add(self.excluded_property_group_bytes),
                ),
                output_statistics_bytes,
                excluded_property_group_count,
                excluded_relationship_property_group_count,
            },
        ))
    }
}

fn accumulator_peak_bytes(output_statistics_bytes: usize) -> usize {
    output_statistics_bytes
}

fn read_next_record(lines: &mut Lines<BufReader<File>>) -> Result<Option<StatsRecord>> {
    let Some(line) = lines.next() else {
        return Ok(None);
    };
    let line = line.map_err(|error| {
        SkeinError::Storage(format!(
            "failed to read optimizer statistics spill run: {error}"
        ))
    })?;
    StatsRecord::decode(&line).map(Some)
}

pub struct StatsMergeReport {
    pub generated_facts: u64,
    pub spill_run_count: usize,
    pub spilled_bytes: u64,
    pub peak_buffer_bytes: usize,
    pub output_statistics_bytes: usize,
    pub excluded_property_group_count: usize,
    pub excluded_relationship_property_group_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum PropertyGroupKey {
    Node(LabelId, String),
    Relationship(RelTypeId, String),
}

impl PropertyGroupKey {
    fn estimated_bytes(&self) -> usize {
        let property_bytes = match self {
            Self::Node(_, property) | Self::Relationship(_, property) => property.len(),
        };
        property_bytes.saturating_add(96)
    }
}

struct PropertyGroup {
    key: PropertyGroupKey,
    last_value: Option<Value>,
    distinct_count: u64,
    samples: BinaryHeap<(u64, Value)>,
    sample_bytes: usize,
}

struct IndexGroup {
    index: IndexId,
    last_key: Option<String>,
    index_size: u64,
    unique_values: u64,
}

struct StatsAccumulator {
    statistics: GraphStatistics,
    memory_budget_bytes: usize,
    output_statistics_bytes: usize,
    excluded_property_groups: BTreeSet<PropertyGroupKey>,
    current_property: Option<PropertyGroup>,
    current_index: Option<IndexGroup>,
    last_distinct_record: Option<StatsRecord>,
}

impl StatsAccumulator {
    fn new(
        statistics: GraphStatistics,
        memory_budget_bytes: usize,
        output_statistics_bytes: usize,
        excluded_property_groups: BTreeSet<PropertyGroupKey>,
    ) -> Self {
        Self {
            statistics,
            memory_budget_bytes,
            output_statistics_bytes,
            excluded_property_groups,
            current_property: None,
            current_index: None,
            last_distinct_record: None,
        }
    }

    fn consume(&mut self, record: StatsRecord) -> Result<()> {
        match record {
            StatsRecord::NodeProperty {
                label,
                property,
                value,
            } => self.consume_property(PropertyGroupKey::Node(label, property), value),
            StatsRecord::RelProperty {
                rel_type,
                property,
                value,
            } => self.consume_property(PropertyGroupKey::Relationship(rel_type, property), value),
            StatsRecord::IndexEntry { index, key } => {
                self.finish_property_group()?;
                self.consume_index(index, key)
            }
            record => {
                self.finish_property_group()?;
                self.finish_index_group();
                self.consume_non_property(record)
            }
        }
    }

    fn consume_property(&mut self, key: PropertyGroupKey, value: Value) -> Result<()> {
        if self.excluded_property_groups.contains(&key) {
            self.finish_property_group()?;
            return Ok(());
        }
        if self
            .current_property
            .as_ref()
            .is_some_and(|group| group.key != key)
        {
            self.finish_property_group()?;
        }
        let output_statistics_bytes = self.output_statistics_bytes;
        let memory_budget_bytes = self.memory_budget_bytes;
        let group = self.current_property.get_or_insert_with(|| PropertyGroup {
            key,
            last_value: None,
            distinct_count: 0,
            samples: BinaryHeap::new(),
            sample_bytes: 0,
        });
        if group.last_value.as_ref() == Some(&value) {
            return Ok(());
        }
        group.last_value = Some(value.clone());
        group.distinct_count = group.distinct_count.saturating_add(1);
        let encoded_bytes = encode_value(&value).len().saturating_add(32);
        let hash = stable_value_hash(&value);
        if group.samples.len() < MAX_PROPERTY_HISTOGRAM_VALUES {
            ensure_statistics_memory(
                output_statistics_bytes,
                group.sample_bytes.saturating_add(encoded_bytes),
                memory_budget_bytes,
            )?;
            group.samples.push((hash, value));
            group.sample_bytes = group.sample_bytes.saturating_add(encoded_bytes);
        } else if group
            .samples
            .peek()
            .is_some_and(|candidate| hash < candidate.0)
        {
            let removed = group.samples.pop().expect("sample heap is non-empty");
            let removed_bytes = encode_value(&removed.1).len().saturating_add(32);
            let next_sample_bytes = group
                .sample_bytes
                .saturating_sub(removed_bytes)
                .saturating_add(encoded_bytes);
            ensure_statistics_memory(
                output_statistics_bytes,
                next_sample_bytes,
                memory_budget_bytes,
            )?;
            group.samples.push((hash, value));
            group.sample_bytes = next_sample_bytes;
        }
        Ok(())
    }

    fn consume_index(&mut self, index: IndexId, key: String) -> Result<()> {
        if self
            .current_index
            .as_ref()
            .is_some_and(|group| group.index != index)
        {
            self.finish_index_group();
        }
        self.ensure_memory(key.len().saturating_add(64))?;
        let group = self.current_index.get_or_insert(IndexGroup {
            index,
            last_key: None,
            index_size: 0,
            unique_values: 0,
        });
        group.index_size = group.index_size.saturating_add(1);
        if group.last_key.as_ref() != Some(&key) {
            group.last_key = Some(key);
            group.unique_values = group.unique_values.saturating_add(1);
        }
        Ok(())
    }

    fn finish_index_group(&mut self) {
        let Some(group) = self.current_index.take() else {
            return;
        };
        self.statistics.index_samples.insert(
            group.index,
            IndexStatisticsSample::exact(group.index_size, group.unique_values),
        );
    }

    fn consume_non_property(&mut self, record: StatsRecord) -> Result<()> {
        let duplicate = self.last_distinct_record.as_ref() == Some(&record);
        match &record {
            StatsRecord::RelSource { rel_type, .. } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.rel_type_source_counts,
                    *rel_type,
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::RelTarget { rel_type, .. } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.rel_type_target_counts,
                    *rel_type,
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathCount {
                source_label,
                rel_type,
                target_label,
            } => {
                reserve_counter_entry(
                    &mut self.statistics.path_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathSource {
                source_label,
                rel_type,
                target_label,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.path_source_distinct_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::PathTarget {
                source_label,
                rel_type,
                target_label,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.path_target_distinct_counts,
                    (*source_label, *rel_type, *target_label),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            } => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_source_distinct_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            StatsRecord::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } if !duplicate => {
                reserve_counter_entry(
                    &mut self.statistics.bounded_path_target_distinct_counts,
                    (*source_label, *rel_type, *target_label, *hop),
                    &mut self.output_statistics_bytes,
                    self.memory_budget_bytes,
                )?;
            }
            _ => {}
        }
        self.last_distinct_record = Some(record);
        Ok(())
    }

    fn finish_property_group(&mut self) -> Result<()> {
        let Some(group) = self.current_property.take() else {
            return Ok(());
        };
        let distinct_count = usize::try_from(group.distinct_count).unwrap_or(usize::MAX);
        let sample_limit = adaptive_histogram_sample_limit(distinct_count);
        let mut samples = group
            .samples
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        samples.sort_unstable();
        if samples.len() > sample_limit {
            let len = samples.len();
            samples = (0..sample_limit)
                .map(|sample_index| {
                    let value_index = sample_index * (len - 1) / (sample_limit - 1);
                    samples[value_index].clone()
                })
                .collect();
        }
        let sampled = distinct_count > sample_limit;
        let histogram_bytes = samples.iter().fold(0usize, |total, value| {
            total.saturating_add(encode_value(value).len().saturating_add(32))
        });
        let key_bytes = match &group.key {
            PropertyGroupKey::Node(_, property) | PropertyGroupKey::Relationship(_, property) => {
                property.len().saturating_add(96)
            }
        };
        self.reserve_output(key_bytes.saturating_add(histogram_bytes))?;
        match group.key {
            PropertyGroupKey::Node(label, property) => {
                let key = (label, property);
                self.statistics
                    .property_distinct_counts
                    .insert(key.clone(), group.distinct_count);
                self.statistics
                    .property_histograms
                    .insert(key.clone(), samples);
                self.statistics
                    .sampled_property_histograms
                    .insert(key, sampled);
            }
            PropertyGroupKey::Relationship(rel_type, property) => {
                let key = (rel_type, property);
                self.statistics
                    .rel_property_distinct_counts
                    .insert(key.clone(), group.distinct_count);
                self.statistics
                    .rel_property_histograms
                    .insert(key.clone(), samples);
                self.statistics
                    .sampled_rel_property_histograms
                    .insert(key, sampled);
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<(GraphStatistics, usize)> {
        self.finish_property_group()?;
        self.finish_index_group();
        Ok((self.statistics, self.output_statistics_bytes))
    }

    fn ensure_memory(&self, temporary_bytes: usize) -> Result<()> {
        if self.output_statistics_bytes.saturating_add(temporary_bytes) > self.memory_budget_bytes {
            return Err(SkeinError::Execution(format!(
                "optimizer statistics refresh output state exceeds memory_budget_bytes {}",
                self.memory_budget_bytes
            )));
        }
        Ok(())
    }

    fn reserve_output(&mut self, bytes: usize) -> Result<()> {
        self.ensure_memory(bytes)?;
        self.output_statistics_bytes = self.output_statistics_bytes.saturating_add(bytes);
        Ok(())
    }
}

fn reserve_counter_entry<K: Ord + Clone>(
    counts: &mut BTreeMap<K, u64>,
    key: K,
    output_bytes: &mut usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    if let Some(count) = counts.get_mut(&key) {
        *count = count.saturating_add(1);
        return Ok(());
    }
    let entry_bytes = std::mem::size_of::<K>().saturating_add(48);
    if output_bytes.saturating_add(entry_bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "optimizer statistics refresh output state exceeds memory_budget_bytes {memory_budget_bytes}"
        )));
    }
    counts.insert(key, 1);
    *output_bytes = output_bytes.saturating_add(entry_bytes);
    Ok(())
}

fn ensure_statistics_memory(
    output_bytes: usize,
    temporary_bytes: usize,
    memory_budget_bytes: usize,
) -> Result<()> {
    if output_bytes.saturating_add(temporary_bytes) > memory_budget_bytes {
        return Err(SkeinError::Execution(format!(
            "optimizer statistics refresh output state exceeds memory_budget_bytes {memory_budget_bytes}"
        )));
    }
    Ok(())
}

fn stable_value_hash(value: &Value) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in encode_value(value).as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub struct RefreshSpillDirectory {
    path: PathBuf,
}

impl RefreshSpillDirectory {
    pub fn create(root: &Path) -> Result<Self> {
        fs::create_dir_all(root).map_err(|error| {
            SkeinError::Storage(format!(
                "failed to create optimizer statistics spill root: {error}"
            ))
        })?;
        let id = NEXT_REFRESH_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let path = root.join(format!(
            "skein-statistics-refresh-{}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).map_err(|error| {
            SkeinError::Storage(format!(
                "failed to create optimizer statistics spill directory: {error}"
            ))
        })?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RefreshSpillDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests;
