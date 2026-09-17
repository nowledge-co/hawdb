//! Row representation kernels for the derived graph columnar shadow.

use crate::{
    residual_row_properties_encoded_len, write_residual_row_properties, ColumnGroupTableKey,
    ColumnGroupTableKind, StreamedBlob,
};
use skein_core::{LabelId, RelTypeId, Result, SkeinError, Value};
use std::collections::BTreeSet;

/// The shadow table key of a node with `labels` (minimum label = primary).
pub fn node_table_key(labels: &BTreeSet<LabelId>) -> ColumnGroupTableKey {
    let table_id = labels
        .first()
        .map_or(0, |label| u64::from(label.0).saturating_add(1));
    ColumnGroupTableKey::new(ColumnGroupTableKind::Node, table_id)
}

pub fn relationship_table_key(rel_type: RelTypeId) -> ColumnGroupTableKey {
    ColumnGroupTableKey::new(ColumnGroupTableKind::Relationship, u64::from(rel_type.0))
}

fn encode_varint_u32(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

pub fn encode_label_set(labels: &BTreeSet<LabelId>) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(labels.len());
    for label in labels {
        encode_varint_u32(label.0, &mut bytes);
    }
    bytes
}

/// Rough resident-byte estimate of one buffered value, mirroring the
/// existing record estimators' spirit: enough to keep the budget honest,
/// never exact.
pub fn estimated_shadow_value_bytes(value: &Value) -> u64 {
    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) => 16,
        Value::String(value) => 16 + value.len() as u64,
        Value::Binary(value) => 16 + value.len() as u64,
        Value::Uuid(_) => 16,
        Value::List(values) => 16 + values.iter().map(estimated_shadow_value_bytes).sum::<u64>(),
        Value::Map(entries) => {
            16 + entries
                .iter()
                .map(|(key, value)| key.len() as u64 + estimated_shadow_value_bytes(value))
                .sum::<u64>()
        }
    }
}

/// Streams a residual row from borrowed entries: exact length up front,
/// value bytes written through the canonical streaming writer — transient
/// memory O(recursion frame), never O(value).
pub struct ResidualRowBlob<'a> {
    entries: &'a [(u32, &'a Value)],
    encoded_len: u64,
}

impl<'a> ResidualRowBlob<'a> {
    pub fn new(entries: &'a [(u32, &'a Value)]) -> Result<Self> {
        let encoded_len = residual_row_properties_encoded_len(entries)
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
        Ok(Self {
            entries,
            encoded_len,
        })
    }
}

impl StreamedBlob for ResidualRowBlob<'_> {
    fn blob_len(&self) -> u64 {
        self.encoded_len
    }

    fn write_blob(&self, out: &mut dyn std::io::Write) -> std::io::Result<()> {
        write_residual_row_properties(out, self.entries)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}
