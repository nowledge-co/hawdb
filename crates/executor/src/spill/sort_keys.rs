// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Select sort keys from an already validated binding without owning its payload.

use super::*;
use hawdb_plan_cypher::{SortDirection, SortItem, SortKey};

#[cfg(test)]
mod tests;

enum Key<'a> {
    Encoded(&'a [u8]),
    Id(u64),
    Null,
}

impl Key<'_> {
    fn retained_bytes(&self) -> Result<usize> {
        match self {
            Self::Encoded(bytes) => estimate_value_memory(&mut Cursor::new(bytes), 1, true),
            Self::Id(_) | Self::Null => Ok(size_of::<Value>()),
        }
    }

    fn decode(self) -> Result<Value> {
        match self {
            Self::Encoded(bytes) => read_value(&mut Cursor::new(bytes), 1),
            Self::Id(id) => Ok(Value::Int(id as i64)),
            Self::Null => Ok(Value::Null),
        }
    }
}

impl SpillBindingRecord {
    pub(crate) fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub(crate) fn ordinal(&self) -> Result<u64> {
        read_u64(&mut Cursor::new(self.payload()))
    }

    /// Complete-record validation and staging admission happened in the reader.
    /// Admit every selected value before allocating the key vector or its values.
    pub(crate) fn sort_keys(
        &self,
        items: &[SortItem],
        tracker: &mut OperatorMemoryTracker,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<Vec<(Value, SortDirection)>> {
        let mut bytes = encoded_len_add(
            size_of::<Vec<(Value, SortDirection)>>(),
            encoded_len_mul(items.len(), size_of::<(Value, SortDirection)>())?,
        )?;
        for item in items {
            crate::pipeline::runtime_checkpoint(task)?;
            bytes = encoded_len_add(
                bytes,
                self.key(&item.key)?
                    .retained_bytes()?
                    .saturating_sub(size_of::<Value>()),
            )?;
        }
        tracker.try_charge(bytes)?;
        let result = (|| {
            let mut keys = Vec::with_capacity(items.len());
            for item in items {
                crate::pipeline::runtime_checkpoint(task)?;
                keys.push((self.key(&item.key)?.decode()?, item.direction));
            }
            Ok(keys)
        })();
        if result.is_err() {
            tracker.release(bytes);
        }
        result
    }

    fn key(&self, key: &SortKey) -> Result<Key<'_>> {
        let mut input = Cursor::new(self.payload());
        read_u64(&mut input)?;
        let column = match key {
            SortKey::Column(name) => Some(name.as_str()),
            SortKey::Expression(_) => {
                return Err(HawDBError::Execution(
                    "encoded sort keys cannot evaluate expressions".into(),
                ));
            }
            _ => None,
        };
        let value = select_map(&mut input, column)?;
        if column.is_some() {
            return Ok(value.map_or(Key::Null, Key::Encoded));
        }
        let (variable, property) = match key {
            SortKey::Property { variable, property } => {
                (variable.as_str(), Some(property.as_str()))
            }
            SortKey::Id { variable } => (variable.as_str(), None),
            _ => unreachable!("column and expression keys handled above"),
        };
        let mut node = None;
        for _ in 0..read_len(&mut input)? {
            let name = string_ref(&mut input)?;
            let id = read_u64(&mut input)?;
            for _ in 0..read_len(&mut input)? {
                read_u32(&mut input)?;
            }
            let value = select_map(&mut input, property)?;
            if name == variable {
                // Match BTreeMap decoding: the last variable entry wins, even
                // when that entry lacks the requested property.
                node = match property {
                    Some(_) => value.map(Key::Encoded),
                    None => Some(Key::Id(id)),
                };
            }
        }
        let mut relationship = None;
        for _ in 0..read_len(&mut input)? {
            let name = string_ref(&mut input)?;
            let id = read_u64(&mut input)?;
            read_u64(&mut input)?;
            read_u64(&mut input)?;
            read_u32(&mut input)?;
            let value = select_map(&mut input, property)?;
            if name == variable {
                relationship = match property {
                    Some(_) => value.map(Key::Encoded),
                    None => Some(Key::Id(id)),
                };
            }
        }
        Ok(node.or(relationship).unwrap_or(Key::Null))
    }
}

fn string_ref<'a>(input: &mut Cursor<&'a [u8]>) -> Result<&'a str> {
    let start = input.position() as usize + 8;
    let len = skip_string(input)?;
    std::str::from_utf8(&input.get_ref()[start..start + len])
        .map_err(|error| HawDBError::Execution(format!("invalid spill string: {error}")))
}

fn select_map<'a>(input: &mut Cursor<&'a [u8]>, wanted: Option<&str>) -> Result<Option<&'a [u8]>> {
    let mut selected = None;
    for _ in 0..read_len(input)? {
        let name = string_ref(input)?;
        let start = input.position() as usize;
        estimate_value_memory(input, 1, true)?;
        if wanted == Some(name) {
            selected = Some(&input.get_ref()[start..input.position() as usize]);
        }
    }
    Ok(selected)
}
