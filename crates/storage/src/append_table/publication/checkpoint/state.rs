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

//! Build the append runtime before mounting any prepared graph state. Its data
//! and concrete allocation inventory stay together until ownership transfers.

use super::*;
use crate::append_table::{AppendState, AppendTableSchema};
use crate::background::{CheckpointDecodeContext, CheckpointWorkError};
use crate::projection::predicate_checkpoint::decode::MapMemory;
use crate::relational::{RelationalColumnDefault, RelationalColumnSchema, RelationalValue};
use hawdb_core::{HawDBError, Result};
use std::cell::RefCell;

#[derive(Default)]
struct Watermarks<'a> {
    values: BTreeMap<&'a RelationalKey, &'a RelationalKey>,
    memory: MapMemory<&'a RelationalKey>,
}

pub(in crate::append_table::publication) fn build(
    reader: &AppendGenerationReader,
    work: &CheckpointWorkContext,
) -> Result<AppendState> {
    let owned = context(work);
    let temporary = context(work);
    let mut schemas = BTreeMap::new();
    let mut schema_memory = MapMemory::<AppendTableSchema>::default();
    // Validation borrows names but allocates two sets and key-position arrays.
    // Retain the largest scratch reservation, rather than summing all tables.
    let mut names_memory = MapMemory::<()>::default();
    let mut keys_memory = MapMemory::<()>::default();
    let mut key_scratch_bytes = 0;
    for (name, schema) in &reader.manifest().schemas {
        schema_memory.before_insert(schemas.len(), &owned)?;
        let name = owned.string(name)?;
        let schema = schema_copy(schema, &owned)?;
        if !schema.columns.is_empty() {
            names_memory.before_insert(schema.columns.len() - 1, &temporary)?;
        }
        let count = schema
            .partition_key
            .len()
            .checked_add(schema.order_key.len())
            .ok_or_else(|| {
                allocation("append key cardinality overflows usize", usize::MAX, &owned)
            })?;
        if count > 0 {
            keys_memory.before_insert(count - 1, &temporary)?;
        }
        let mut bytes = 0usize;
        for keys in [&schema.partition_key, &schema.order_key] {
            if !keys.is_empty() {
                let capacity = keys
                    .len()
                    .checked_next_power_of_two()
                    .ok_or_else(|| {
                        allocation("append key positions overflow usize", usize::MAX, &owned)
                    })?
                    .max(4);
                bytes = capacity
                    .checked_mul(2 * std::mem::size_of::<usize>())
                    .and_then(|size| bytes.checked_add(size))
                    .ok_or_else(|| {
                        allocation("append key positions overflow usize", usize::MAX, &owned)
                    })?;
            }
        }
        if bytes > key_scratch_bytes {
            // Result collection may grow its Vec geometrically; cover the
            // minimum capacity and simultaneous old/new backing arrays.
            temporary.reserve(bytes - key_scratch_bytes)?;
            key_scratch_bytes = bytes;
        }
        let unit = start(work)?;
        schemas.insert(name, schema);
        unit.finish();
    }
    let mut selected = BTreeMap::<&str, Watermarks<'_>>::new();
    let mut selected_memory = MapMemory::<Watermarks<'_>>::default();
    for segment in reader.segments.iter() {
        for descriptor in segment.descriptors() {
            let unit = start(work)?;
            let new_table = !selected.contains_key(descriptor.table.as_str());
            unit.finish();
            if new_table {
                selected_memory.before_insert(selected.len(), &temporary)?;
                let unit = start(work)?;
                selected.insert(&descriptor.table, Watermarks::default());
                unit.finish();
            }
            let table = selected
                .get_mut(descriptor.table.as_str())
                .expect("selected table");
            let unit = start(work)?;
            let new_partition = !table.values.contains_key(&descriptor.partition_key);
            unit.finish();
            if new_partition {
                table.memory.before_insert(table.values.len(), &temporary)?;
            }
            let unit = start(work)?;
            // Ordinary watermarks retain the last descriptor for each partition.
            // Borrow it until collection finishes, then copy each key only once.
            table
                .values
                .insert(&descriptor.partition_key, &descriptor.max_order_key);
            unit.finish();
        }
    }
    let mut watermarks = BTreeMap::new();
    let mut table_memory = MapMemory::<BTreeMap<RelationalKey, RelationalKey>>::default();
    for (name, selected) in selected {
        table_memory.before_insert(watermarks.len(), &owned)?;
        let name = owned.string(name)?;
        let mut values = BTreeMap::new();
        let mut value_memory = MapMemory::<RelationalKey>::default();
        for (partition, order) in selected.values {
            value_memory.before_insert(values.len(), &owned)?;
            let partition = key_copy(partition, &owned)?;
            let order = key_copy(order, &owned)?;
            let unit = start(work)?;
            values.insert(partition, order);
            unit.finish();
        }
        let unit = start(work)?;
        watermarks.insert(name, values);
        unit.finish();
    }
    let mut generated = BTreeMap::new();
    let mut generated_memory = MapMemory::<i64>::default();
    for (name, value) in reader.generated_order_watermarks() {
        generated_memory.before_insert(generated.len(), &owned)?;
        let name = owned.string(name)?;
        let unit = start(work)?;
        generated.insert(name, *value);
        unit.finish();
    }
    let shared_maps = usize::from(!schemas.is_empty())
        + usize::from(!watermarks.is_empty())
        + usize::from(!generated.is_empty());
    owned.reserve(
        shared_maps
            * (std::mem::size_of::<BTreeMap<String, i64>>() + 2 * std::mem::size_of::<usize>()),
    )?;
    let unit = start(work)?;
    let mut state = AppendState::from_checkpoint_with_generated_order_watermarks(
        schemas, watermarks, generated,
    )
    .map_err(HawDBError::from_storage_error)?;
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    state.checkpoint_allocations = owned.memory.into_inner();
    Ok(state)
}

fn context(work: &CheckpointWorkContext) -> CheckpointDecodeContext {
    CheckpointDecodeContext {
        work: work.clone(),
        memory: RefCell::default(),
    }
}

fn start(work: &CheckpointWorkContext) -> Result<crate::background::CheckpointWorkUnit> {
    work.start_unit().map_err(HawDBError::from_storage_error)
}

fn allocation(message: &str, bytes: usize, work: &CheckpointDecodeContext) -> HawDBError {
    HawDBError::from_storage_error(work.record_failure(CheckpointWorkError::Allocation {
        bytes: bytes as u64,
        reason: message.into(),
    }))
}

fn schema_copy(
    schema: &AppendTableSchema,
    work: &CheckpointDecodeContext,
) -> Result<AppendTableSchema> {
    let name = work.string(&schema.name)?;
    let mut columns = Vec::new();
    for column in &schema.columns {
        let name = work.string(&column.name)?;
        let default = match &column.default {
            Some(RelationalColumnDefault::Literal(value)) => {
                Some(RelationalColumnDefault::Literal(value_copy(value, work)?))
            }
            Some(RelationalColumnDefault::UuidV7) => Some(RelationalColumnDefault::UuidV7),
            None => None,
        };
        work.push(
            &mut columns,
            RelationalColumnSchema {
                name,
                scalar_type: column.scalar_type,
                nullable: column.nullable,
                default,
            },
        )?;
    }
    let mut partition_key = Vec::new();
    for key in &schema.partition_key {
        work.push(&mut partition_key, work.string(key)?)?;
    }
    let mut order_key = Vec::new();
    for key in &schema.order_key {
        work.push(&mut order_key, work.string(key)?)?;
    }
    Ok(AppendTableSchema {
        name,
        columns,
        partition_key,
        order_key,
        order_mode: schema.order_mode,
    })
}

pub(super) fn key_copy(
    key: &RelationalKey,
    work: &CheckpointDecodeContext,
) -> Result<RelationalKey> {
    let mut values = Vec::new();
    for value in &key.0 {
        work.push(&mut values, value_copy(value, work)?)?;
    }
    Ok(RelationalKey(values))
}

fn value_copy(value: &RelationalValue, work: &CheckpointDecodeContext) -> Result<RelationalValue> {
    Ok(match value {
        RelationalValue::Text(value) => RelationalValue::Text(work.string(value)?),
        RelationalValue::Bytea(_) | RelationalValue::Vector(_) => {
            let (value, vector) = match value {
                RelationalValue::Bytea(bytes) => (bytes.as_slice(), false),
                RelationalValue::Vector(value) => (value.encoded(), true),
                _ => unreachable!(),
            };
            work.reserve(value.len())?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(value.len())
                .map_err(|_| allocation("append byte allocation failed", value.len(), work))?;
            if bytes.capacity() != value.len() {
                return Err(allocation(
                    "append byte capacity differs from admission",
                    value.len(),
                    work,
                ));
            }
            for chunk in value.chunks(64 * 1024) {
                let unit = start(work)?;
                bytes.extend_from_slice(chunk);
                unit.finish();
            }
            if vector {
                RelationalValue::Vector(crate::background::decode_vector(bytes, Some(&work.work))?)
            } else {
                RelationalValue::Bytea(bytes)
            }
        }
        // All other relational variants own only fixed-width data.
        value @ (RelationalValue::Null
        | RelationalValue::Boolean(_)
        | RelationalValue::BigInt(_)
        | RelationalValue::DoublePrecision(_)
        | RelationalValue::Decimal(_)
        | RelationalValue::Uuid(_)
        | RelationalValue::Overflow(_)) => {
            let unit = start(work)?;
            let value = value.clone();
            unit.finish();
            value
        }
    })
}
