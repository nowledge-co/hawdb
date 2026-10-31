// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Source-bound width estimates. Distinct-value samples and type proxies are
//! not frequency-weighted means, allocation bounds, or semantic proofs.

use super::super::OptimizerCatalog;
use hawdb_core::{PropertyType, SchemaObjectState, TableKind, Value};
use std::collections::BTreeMap;

const MAX_WIDTH_SAMPLES: usize = 32;
const MAX_RECORD_FIELDS: usize = 64;
pub(super) const UNKNOWN_PAYLOAD_BYTES: u64 = 64;
const UNKNOWN_RECORD_BYTES: u64 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WidthSource {
    Literal,
    DeclaredScalar,
    DeclaredVector,
    DistinctSample,
    TypeProxy,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FieldWidth {
    /// Owned variable payload; the value/container cells are accounted separately.
    pub(super) payload_bytes: u64,
    pub(super) source: WidthSource,
    pub(super) samples: usize,
}

impl FieldWidth {
    pub(super) fn copy_units(self) -> Option<u64> {
        (self.source != WidthSource::Unknown)
            .then_some(1u64.saturating_add(self.payload_bytes.div_ceil(64)))
    }

    pub(super) fn fixed(payload_bytes: u64, source: WidthSource) -> Self {
        Self {
            payload_bytes,
            source,
            samples: 0,
        }
    }
}

pub(super) fn literal_width(value: &Value) -> FieldWidth {
    observed_payload(value).map_or_else(
        || FieldWidth::fixed(256, WidthSource::Unknown),
        |bytes| FieldWidth::fixed(bytes, WidthSource::Literal),
    )
}

pub(super) fn unknown_width() -> FieldWidth {
    FieldWidth::fixed(UNKNOWN_PAYLOAD_BYTES, WidthSource::Unknown)
}

fn declared_type(
    catalog: &OptimizerCatalog,
    kind: TableKind,
    label: &str,
    property: &str,
) -> Option<PropertyType> {
    let schema = catalog.source_schema.as_ref()?;
    let table = schema.table_id(kind, label)?;
    if schema.table_descriptor(table)?.state != SchemaObjectState::Public {
        return None;
    }
    let descriptor = schema.property_descriptor(schema.property_descriptor_id(table, property)?)?;
    (descriptor.state == SchemaObjectState::Public).then_some(descriptor.value_type)
}

fn observed_payload(value: &Value) -> Option<u64> {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => Some(0),
        Value::Uuid(_) => Some(16),
        Value::String(value) => Some(value.len() as u64),
        Value::Binary(value) => Some(value.len() as u64),
        Value::Vector(value) => Some(value.encoded().len() as u64),
        // Containers and value kinds without a width model use the existing
        // unknown proxy. New core value variants must not be assumed scalar.
        _ => None,
    }
}

fn sample_width(values: &[Value], strings_only: bool) -> Option<FieldWidth> {
    let samples = values.len().min(MAX_WIDTH_SAMPLES);
    if samples == 0 {
        return None;
    }
    let mut bytes = 0;
    for position in 0..samples {
        let index = if samples == 1 {
            0
        } else {
            (position as u128 * (values.len() - 1) as u128 / (samples - 1) as u128) as usize
        };
        let value = &values[index];
        if strings_only && !matches!(value, Value::Null | Value::String(_)) {
            return None;
        }
        bytes = bytes.max(observed_payload(value)?);
    }
    Some(FieldWidth {
        payload_bytes: bytes,
        source: WidthSource::DistinctSample,
        samples,
    })
}

pub(super) fn field_width(
    catalog: &OptimizerCatalog,
    kind: TableKind,
    label: &str,
    property: &str,
) -> FieldWidth {
    let value_type = declared_type(catalog, kind, label, property);
    match value_type {
        Some(PropertyType::Bool | PropertyType::Int | PropertyType::Float) => {
            return FieldWidth::fixed(0, WidthSource::DeclaredScalar)
        }
        Some(PropertyType::Vector(dimension)) => {
            return FieldWidth::fixed(
                u64::from(dimension.get())
                    .saturating_mul(4)
                    .saturating_add(4),
                WidthSource::DeclaredVector,
            )
        }
        // These payload types deliberately have no value histograms.
        Some(PropertyType::Text) => return FieldWidth::fixed(4096, WidthSource::TypeProxy),
        Some(PropertyType::List) => return FieldWidth::fixed(256, WidthSource::TypeProxy),
        Some(PropertyType::Any | PropertyType::String) | None => {}
    }
    let histograms = match kind {
        TableKind::Node => &catalog.property_histograms,
        TableKind::Relationship => &catalog.rel_property_histograms,
    };
    if let Some(width) = histograms
        .get(&(label.to_owned(), property.to_owned()))
        .and_then(|values| sample_width(values, value_type == Some(PropertyType::String)))
    {
        return width;
    }
    FieldWidth::fixed(
        UNKNOWN_PAYLOAD_BYTES,
        if value_type.is_some() {
            WidthSource::TypeProxy
        } else {
            WidthSource::Unknown
        },
    )
}

// Match the runtime's logical map-cell estimate, without claiming allocator
// capacity or physical IO. Variable payloads are separate from movable headers.
pub(super) fn field_cell_bytes(name: &str, width: FieldWidth) -> u64 {
    let cells =
        std::mem::size_of::<(String, Value)>() as u64 * 3 + std::mem::size_of::<Value>() as u64;
    cells
        .saturating_add(name.len() as u64)
        .saturating_add(width.payload_bytes)
}

pub(super) fn record_decode_units(
    catalog: &OptimizerCatalog,
    kind: TableKind,
    label: &str,
) -> Option<u64> {
    let schema = catalog.source_schema.as_ref()?;
    let table = schema.table_id(kind, label)?;
    if schema.table_descriptor(table)?.state != SchemaObjectState::Public
        || schema.table_property_descriptors(table).next().is_none()
    {
        return None;
    }
    Some(record_width(catalog, kind, label).bytes.div_ceil(64))
}

pub(super) struct RecordWidth {
    pub(super) bytes: u64,
    pub(super) proxies: usize,
    pub(super) samples: usize,
}

pub(super) fn record_width(
    catalog: &OptimizerCatalog,
    kind: TableKind,
    label: &str,
) -> RecordWidth {
    let opaque = || RecordWidth {
        bytes: UNKNOWN_RECORD_BYTES,
        proxies: 1,
        samples: 0,
    };
    let Some(schema) = catalog.source_schema.as_ref() else {
        return opaque();
    };
    let Some(table) = schema.table_id(kind, label).filter(|table| {
        schema
            .table_descriptor(*table)
            .is_some_and(|table| table.state == SchemaObjectState::Public)
    }) else {
        return opaque();
    };
    let mut properties = schema
        .table_property_descriptors(table)
        .filter(|property| property.state == SchemaObjectState::Public);
    let mut bytes = std::mem::size_of::<BTreeMap<String, Value>>() as u64 + 32;
    let mut count = 0;
    let mut proxies = 0;
    let mut samples = 0;
    for property in properties.by_ref().take(MAX_RECORD_FIELDS) {
        let field = field_width(catalog, kind, label, &property.name);
        bytes = bytes.saturating_add(field_cell_bytes(&property.name, field));
        proxies += usize::from(matches!(
            field.source,
            WidthSource::Unknown | WidthSource::TypeProxy
        ));
        samples += field.samples;
        count += 1;
    }
    if count == 0 || properties.next().is_some() {
        bytes = bytes.saturating_add(UNKNOWN_RECORD_BYTES);
        proxies += 1;
    }
    RecordWidth {
        bytes,
        proxies,
        samples,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::optimizer_catalog_from_graph_statistics;
    use hawdb_core::{Catalog, GraphStatistics, VectorDimension};

    #[test]
    fn declared_shapes_skip_payload_histograms_and_keep_the_old_schema_snapshot() {
        let mut schema = Catalog::default();
        let table = schema.get_or_create_table(TableKind::Node, "Memory");
        schema.get_or_create_property(table, "body", PropertyType::Text, true);
        let old_vector = schema.get_or_create_property(
            table,
            "vector",
            PropertyType::Vector(VectorDimension::new(32).unwrap()),
            true,
        );
        let pinned = optimizer_catalog_from_graph_statistics(&schema, &GraphStatistics::default());
        assert_eq!(
            field_width(&pinned, TableKind::Node, "Memory", "body"),
            FieldWidth::fixed(4096, WidthSource::TypeProxy)
        );
        assert_eq!(
            field_width(&pinned, TableKind::Node, "Memory", "vector"),
            FieldWidth::fixed(132, WidthSource::DeclaredVector)
        );
        schema.remove_property_descriptor(old_vector);
        schema.get_or_create_property(
            table,
            "vector",
            PropertyType::Vector(VectorDimension::new(64).unwrap()),
            true,
        );
        let current = optimizer_catalog_from_graph_statistics(&schema, &GraphStatistics::default());
        assert_eq!(
            field_width(&pinned, TableKind::Node, "Memory", "vector").payload_bytes,
            132
        );
        assert_eq!(
            field_width(&current, TableKind::Node, "Memory", "vector").payload_bytes,
            260
        );
        assert!(
            record_width(&current, TableKind::Node, "Memory").bytes
                > record_width(&pinned, TableKind::Node, "Memory").bytes
        );
        assert!(field_width(&current, TableKind::Node, "Missing", "vector").payload_bytes > 0);
    }

    #[test]
    fn distinct_domain_sampling_is_bounded_and_not_a_row_frequency_mean() {
        let values = (0..1000)
            .map(|n| Value::String("x".repeat(n)))
            .collect::<Vec<_>>();
        let width = sample_width(&values, true).unwrap();
        assert_eq!(width.samples, 32);
        assert_eq!(width.payload_bytes, 999);
        assert_eq!(width.source, WidthSource::DistinctSample);
        assert_eq!(sample_width(&[], false), None);
        assert_eq!(sample_width(&[Value::Int(1)], true), None);
        assert_eq!(sample_width(&[Value::Map(BTreeMap::new())], false), None);
    }
}
