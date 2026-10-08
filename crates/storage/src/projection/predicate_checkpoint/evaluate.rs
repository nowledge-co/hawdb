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

//! Borrowed predicate evaluation with a permit per tree node or 64 KiB
//! comparison. A sorted property visit avoids unbounded string comparisons
//! inside BTreeMap::get. Source, recursion-stack and producer allocations still
//! require the checkpoint's complete resource ledger.

use super::ProjectedRelationshipPredicate;
use crate::background::CheckpointWorkContext;
use hawdb_core::{HawDBError, Result, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

fn compare(left: &[u8], right: &[u8], work: &CheckpointWorkContext) -> Result<Ordering> {
    for (left, right) in left.chunks(64 * 1024).zip(right.chunks(64 * 1024)) {
        let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
        let ordering = left.cmp(right);
        unit.finish();
        if !ordering.is_eq() {
            work.checkpoint().map_err(HawDBError::from_storage_error)?;
            return Ok(ordering);
        }
    }
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let ordering = left.len().cmp(&right.len());
    unit.finish();
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(ordering)
}

fn property<'a>(
    properties: &'a BTreeMap<String, Value>,
    name: &str,
    work: &CheckpointWorkContext,
) -> Result<Option<&'a Value>> {
    for (key, value) in properties {
        match compare(key.as_bytes(), name.as_bytes(), work)? {
            Ordering::Less => {}
            Ordering::Equal => return Ok(Some(value)),
            Ordering::Greater => break,
        }
    }
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(None)
}

fn equal(left: &Value, right: &Value, work: &CheckpointWorkContext) -> Result<bool> {
    let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
    let scalar = match (left, right) {
        (Value::Null, Value::Null) => Some(true),
        (Value::Bool(left), Value::Bool(right)) => Some(left == right),
        (Value::Int(left), Value::Int(right)) => Some(left == right),
        (Value::Float(left), Value::Float(right)) => Some(left.total_cmp(right).is_eq()),
        (Value::Uuid(left), Value::Uuid(right)) => Some(left == right),
        (Value::String(_), Value::String(_))
        | (Value::Binary(_), Value::Binary(_))
        | (Value::List(_), Value::List(_))
        | (Value::Map(_), Value::Map(_)) => None,
        _ => Some(false),
    };
    unit.finish();
    if let Some(equal) = scalar {
        work.checkpoint().map_err(HawDBError::from_storage_error)?;
        return Ok(equal);
    }
    let equal = match (left, right) {
        (Value::String(left), Value::String(right)) => {
            compare(left.as_bytes(), right.as_bytes(), work)?.is_eq()
        }
        (Value::Binary(left), Value::Binary(right)) => compare(left, right, work)?.is_eq(),
        (Value::List(left), Value::List(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !self::equal(left, right, work)? {
                    return Ok(false);
                }
            }
            true
        }
        (Value::Map(left), Value::Map(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for ((left_key, left), (right_key, right)) in left.iter().zip(right) {
                if !compare(left_key.as_bytes(), right_key.as_bytes(), work)?.is_eq()
                    || !self::equal(left, right, work)?
                {
                    return Ok(false);
                }
            }
            true
        }
        _ => unreachable!("scalar and mismatched variants were compared above"),
    };
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(equal)
}

pub(in crate::projection) fn matches(
    predicate: &ProjectedRelationshipPredicate,
    properties: &BTreeMap<String, Value>,
    work: &CheckpointWorkContext,
) -> Result<bool> {
    work.start_unit()
        .map_err(HawDBError::from_storage_error)?
        .finish();
    let matched = match predicate {
        ProjectedRelationshipPredicate::And(predicates) => {
            for predicate in predicates {
                if !matches(predicate, properties, work)? {
                    return Ok(false);
                }
            }
            true
        }
        ProjectedRelationshipPredicate::Eq {
            property: name,
            value,
        } => match property(properties, name, work)? {
            Some(actual) => equal(actual, value, work)?,
            None => false,
        },
        ProjectedRelationshipPredicate::Gte {
            property: name,
            value,
        } => match property(properties, name, work)? {
            Some(Value::String(actual)) if matches!(value, Value::String(_)) => {
                let Value::String(value) = value else {
                    unreachable!();
                };
                !compare(actual.as_bytes(), value.as_bytes(), work)?.is_lt()
            }
            Some(actual) => {
                let unit = work.start_unit().map_err(HawDBError::from_storage_error)?;
                let matched = crate::predicate::comparable_value_ordering(actual, value)
                    .is_some_and(|ordering| !ordering.is_lt());
                unit.finish();
                matched
            }
            None => false,
        },
    };
    work.checkpoint().map_err(HawDBError::from_storage_error)?;
    Ok(matched)
}

#[cfg(test)]
mod tests;
