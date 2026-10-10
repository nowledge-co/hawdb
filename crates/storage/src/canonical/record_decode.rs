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

//! Canonical record, map and value wire semantics shared by ordinary and
//! maintenance reads. Backends own allocation and work-admission mechanics.

use super::*;

pub(super) trait Decoder {
    type Unit;
    fn start_unit(&self) -> Result<Self::Unit, CanonicalSegmentError>;
    fn finish_unit(&self, unit: Self::Unit) -> Result<(), CanonicalSegmentError>;
    fn validate_count(
        &self,
        cursor: &SliceCursor<'_>,
        count: usize,
        minimum: usize,
    ) -> Result<(), CanonicalSegmentError>;
    fn string(&self, bytes: &[u8]) -> Result<String, CanonicalSegmentError>;
    fn key(&self, key: &str) -> Result<String, CanonicalSegmentError>;
    fn bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, CanonicalSegmentError>;
    fn list(&self, count: usize) -> Result<Vec<Value>, CanonicalSegmentError>;
    fn insert_label(
        &self,
        labels: &mut BTreeSet<LabelId>,
        label: LabelId,
        admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError>;
    fn before_map_insert(
        &self,
        len: usize,
        admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError>;
    fn visit_spill<T>(
        &self,
        reader: &PropertySpillReader,
        id: u64,
        visitor: impl FnOnce(Option<&[u8]>) -> Result<T, CanonicalSegmentError>,
    ) -> Result<T, CanonicalSegmentError>;
}

pub(super) fn node<D: Decoder>(
    id: u64,
    payload: &[u8],
    spills: Option<&PropertySpillReader>,
    keys: Option<&[String]>,
    decoder: &D,
) -> Result<NodeRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let unit = decoder.start_unit()?;
    let count = cursor.read_u32()? as usize;
    decoder.validate_count(&cursor, count, 4)?;
    decoder.finish_unit(unit)?;
    let mut labels = BTreeSet::new();
    let mut admitted = 0;
    for _ in 0..count {
        let unit = decoder.start_unit()?;
        let label = LabelId(cursor.read_u32()?);
        decoder.insert_label(&mut labels, label, &mut admitted)?;
        decoder.finish_unit(unit)?;
    }
    let properties = properties(&mut cursor, 1, 1, spills, keys, decoder)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "node record has trailing bytes".into(),
        ));
    }
    Ok(NodeRecord {
        id: NodeId(id),
        labels,
        properties,
    })
}

pub(super) fn relationship<D: Decoder>(
    id: u64,
    payload: &[u8],
    spills: Option<&PropertySpillReader>,
    keys: Option<&[String]>,
    decoder: &D,
) -> Result<RelRecord, CanonicalSegmentError> {
    let mut cursor = SliceCursor::new(payload);
    let unit = decoder.start_unit()?;
    let source = NodeId(cursor.read_u64()?);
    let target = NodeId(cursor.read_u64()?);
    let rel_type = RelTypeId(cursor.read_u32()?);
    decoder.finish_unit(unit)?;
    let properties = properties(&mut cursor, 1, 1, spills, keys, decoder)?;
    if !cursor.is_empty() {
        return Err(CanonicalSegmentError::Corrupt(
            "relationship record has trailing bytes".into(),
        ));
    }
    Ok(RelRecord {
        id: RelId(id),
        source,
        target,
        rel_type,
        properties,
    })
}

fn string_field<D: Decoder>(
    cursor: &mut SliceCursor<'_>,
    decoder: &D,
) -> Result<String, CanonicalSegmentError> {
    let unit = decoder.start_unit()?;
    let length = cursor.read_u32()? as usize;
    let bytes = cursor.read_exact(length)?;
    decoder.finish_unit(unit)?;
    decoder.string(bytes)
}

pub(super) fn properties<D: Decoder>(
    cursor: &mut SliceCursor<'_>,
    map_depth: usize,
    value_depth: usize,
    spills: Option<&PropertySpillReader>,
    keys: Option<&[String]>,
    decoder: &D,
) -> Result<BTreeMap<String, Value>, CanonicalSegmentError> {
    let unit = decoder.start_unit()?;
    ensure_depth(map_depth)?;
    let count = cursor.read_u32()? as usize;
    decoder.validate_count(cursor, count, 5)?;
    decoder.finish_unit(unit)?;
    let mut output = BTreeMap::new();
    let mut admitted = 0;
    for _ in 0..count {
        let key = if let Some(keys) = keys {
            let unit = decoder.start_unit()?;
            let key_id = cursor.read_u32()?;
            let key = keys.get(key_id as usize).ok_or_else(|| {
                CanonicalSegmentError::Corrupt(format!(
                    "canonical record references unknown property key id {key_id}"
                ))
            })?;
            decoder.finish_unit(unit)?;
            decoder.key(key)?
        } else {
            string_field(cursor, decoder)?
        };
        let value = value(cursor, value_depth, spills, decoder)?;
        let unit = decoder.start_unit()?;
        decoder.before_map_insert(output.len(), &mut admitted)?;
        if output.insert(key, value).is_some() {
            return Err(CanonicalSegmentError::Corrupt(
                "canonical property map has duplicate keys".into(),
            ));
        }
        decoder.finish_unit(unit)?;
    }
    Ok(output)
}

pub(super) fn value<D: Decoder>(
    cursor: &mut SliceCursor<'_>,
    depth: usize,
    spills: Option<&PropertySpillReader>,
    decoder: &D,
) -> Result<Value, CanonicalSegmentError> {
    let unit = decoder.start_unit()?;
    ensure_depth(depth)?;
    let tag = cursor.read_u8()?;
    let decoded = match tag {
        0 => Value::Null,
        1 => match cursor.read_u8()? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            value => {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "invalid canonical boolean {value}"
                )))
            }
        },
        2 => Value::Int(cursor.read_i64()?),
        3 => Value::Float(f64::from_bits(cursor.read_u64()?)),
        4 => {
            decoder.finish_unit(unit)?;
            return string_field(cursor, decoder).map(Value::String);
        }
        5 => {
            let count = cursor.read_u32()? as usize;
            decoder.validate_count(cursor, count, 1)?;
            let mut values = decoder.list(count)?;
            decoder.finish_unit(unit)?;
            for _ in 0..count {
                values.push(value(cursor, depth.saturating_add(1), spills, decoder)?);
            }
            return Ok(Value::List(values));
        }
        6 => {
            decoder.finish_unit(unit)?;
            return properties(
                cursor,
                depth.saturating_add(1),
                depth.saturating_add(2),
                spills,
                None,
                decoder,
            )
            .map(Value::Map);
        }
        7 => {
            let id = cursor.read_u64()?;
            let reader = spills.ok_or_else(|| CanonicalSegmentError::Corrupt(format!(
                "canonical value references property spill {id} without a published spill artifact")))?;
            decoder.finish_unit(unit)?;
            return decoder.visit_spill(reader, id, |encoded| {
                let encoded = encoded.ok_or_else(|| {
                    CanonicalSegmentError::Corrupt(format!(
                        "canonical value references missing property spill {id}"
                    ))
                })?;
                let mut spilled = SliceCursor::new(encoded);
                let decoded = value(&mut spilled, depth, None, decoder)?;
                if !spilled.is_empty() {
                    return Err(CanonicalSegmentError::Corrupt(format!(
                        "property spill {id} has trailing bytes"
                    )));
                }
                Ok(decoded)
            });
        }
        8 => {
            let length = cursor.read_u32()? as usize;
            let bytes = cursor.read_exact(length)?;
            decoder.finish_unit(unit)?;
            return decoder.bytes(bytes).map(Value::Binary);
        }
        9 => Value::Uuid(hawdb_core::Uuid::from_bytes(
            cursor
                .read_exact(16)?
                .try_into()
                .expect("fixed UUID length"),
        )),
        tag => {
            return Err(CanonicalSegmentError::Corrupt(format!(
                "unknown canonical value tag {tag}"
            )))
        }
    };
    decoder.finish_unit(unit)?;
    Ok(decoded)
}

pub(super) struct OrdinaryDecoder;

impl Decoder for OrdinaryDecoder {
    type Unit = ();
    #[inline(always)]
    fn start_unit(&self) -> Result<(), CanonicalSegmentError> {
        Ok(())
    }
    #[inline(always)]
    fn finish_unit(&self, (): ()) -> Result<(), CanonicalSegmentError> {
        Ok(())
    }
    #[inline(always)]
    fn validate_count(
        &self,
        _cursor: &SliceCursor<'_>,
        _count: usize,
        _minimum: usize,
    ) -> Result<(), CanonicalSegmentError> {
        Ok(())
    }
    fn string(&self, bytes: &[u8]) -> Result<String, CanonicalSegmentError> {
        String::from_utf8(bytes.to_vec()).map_err(|error| {
            CanonicalSegmentError::Corrupt(format!("canonical string is not UTF-8: {error}"))
        })
    }
    fn key(&self, key: &str) -> Result<String, CanonicalSegmentError> {
        Ok(key.to_owned())
    }
    fn bytes(&self, bytes: &[u8]) -> Result<Vec<u8>, CanonicalSegmentError> {
        Ok(bytes.to_vec())
    }
    fn list(&self, count: usize) -> Result<Vec<Value>, CanonicalSegmentError> {
        Ok(Vec::with_capacity(count.min(1024)))
    }
    fn insert_label(
        &self,
        labels: &mut BTreeSet<LabelId>,
        label: LabelId,
        _admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError> {
        labels.insert(label);
        Ok(())
    }
    fn before_map_insert(
        &self,
        _len: usize,
        _admitted: &mut usize,
    ) -> Result<(), CanonicalSegmentError> {
        Ok(())
    }
    fn visit_spill<T>(
        &self,
        reader: &PropertySpillReader,
        id: u64,
        visitor: impl FnOnce(Option<&[u8]>) -> Result<T, CanonicalSegmentError>,
    ) -> Result<T, CanonicalSegmentError> {
        let encoded = reader.get(id)?;
        visitor(encoded.as_deref())
    }
}
