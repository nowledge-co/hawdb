// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Necessary typed leading equalities bound a whole declared index domain,
//! regardless of residual ranges, ordering, LIMIT or the physical access choice.
use super::*;
use hawdb_storage::version::relational_index_prefix_read_key;

pub(super) fn prefix_key(
    select: &SelectStatement,
    schema: &RelationalTableSchema,
    parameters: &[Value],
) -> Result<Option<VersionKey>> {
    let mut descriptors = 0;
    let mut metadata = 0usize;
    for index in &schema.indexes {
        descriptors += 1;
        metadata = metadata
            .saturating_add(index.name.len())
            .saturating_add(std::mem::size_of_val(index));
        if descriptors > 1024 || metadata > MAX_INPUT_BYTES {
            return Ok(None);
        }
        if index.columns.is_empty() || index.columns.len() > MAX_KEY_COLUMNS {
            continue;
        }
        let mut values = Vec::new();
        let mut bytes = std::mem::size_of::<VersionKey>()
            .saturating_add(schema.name.len())
            .saturating_add(index.name.len());
        for name in &index.columns {
            metadata = metadata
                .saturating_add(name.len())
                .saturating_add(std::mem::size_of::<String>());
            if metadata > MAX_INPUT_BYTES {
                return Ok(None);
            }
            let Some(value) = select
                .selection
                .as_ref()
                .and_then(|p| necessary_equality(p, select, name))
            else {
                break;
            };
            let value = match value {
                SqlValue::Literal(value) => value,
                SqlValue::Parameter(position) => {
                    parameters.get(position.saturating_sub(1)).ok_or_else(|| {
                        HawDBError::Semantic(format!("missing PostgreSQL parameter ${position}"))
                    })?
                }
            };
            bytes = bytes
                .saturating_add(scalar_bytes(value)?)
                .saturating_add(std::mem::size_of::<RelationalValue>());
            if bytes > MAX_KEY_BYTES {
                break;
            }
            let position = schema
                .column_position(name)
                .ok_or_else(|| refusal("invalid declared-index schema"))?;
            let scalar_type = schema.columns[position].scalar_type;
            let value = value_to_relational_as(value.clone(), scalar_type)?;
            if matches!(
                value,
                RelationalValue::Null
                    | RelationalValue::DoublePrecision(_)
                    | RelationalValue::Overflow(_)
            ) {
                break;
            }
            if value.scalar_type() != Some(scalar_type) {
                return Err(refusal("indexed comparison with incompatible scalar types"));
            }
            values.push(value);
        }
        // Unique indexes omit rows with any NULL component. An unconstrained
        // nullable suffix would therefore make their membership incomplete.
        if !schema.index_prefix_covers_matches(&index.columns, values.len(), index.unique) {
            continue;
        }
        if let Some(key) =
            relational_index_prefix_read_key(&schema.name, &index.name, values.iter())
        {
            return Ok(Some(key));
        }
    }
    Ok(None)
}
