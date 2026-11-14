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

//! Temporal functions, postfix property access and map literals.

use super::*;

pub(super) fn function<'a>(
    function: hawdb_core::TemporalFunction,
    arguments: &'a [ProjectionExpression],
    evaluate: impl Fn(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
) -> Result<ProjectedValue<'a>> {
    let mut values = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let value = evaluate(argument)?;
        if value.as_ref() == &Value::Null {
            return Ok(ProjectedValue::scalar(Value::Null));
        }
        values.push(value);
    }
    // Arguments are scalars, strings or small maps; the result is inline
    // apart from a boxed duration, which is charged once the value is owned.
    let owned = values
        .into_iter()
        .map(ProjectedValue::into_owned)
        .collect::<Vec<_>>();
    hawdb_core::temporal::evaluate(function, &owned, hawdb_core::temporal::statement_now())
        .map(ProjectedValue::scalar)
        .map_err(|error| HawDBError::Execution(error.to_string()))
}

/// `value.key`: a temporal component, a map entry, or NULL for NULL.
pub(super) fn property<'a>(
    value: ProjectedValue<'a>,
    key: &str,
    account: Option<&QueryMemoryAccount>,
) -> Result<ProjectedValue<'a>> {
    let ProjectedValue { value, _allocation } = value;
    match value {
        Cow::Borrowed(Value::Map(values)) => Ok(values
            .get(key)
            .map(ProjectedValue::borrowed)
            .unwrap_or_else(|| ProjectedValue::scalar(Value::Null))),
        Cow::Owned(Value::Map(mut values)) => Ok(match values.remove(key) {
            // The entry keeps the map's (larger) charge until it is owned.
            Some(value) => ProjectedValue {
                value: Cow::Owned(value),
                _allocation,
            },
            None => ProjectedValue::scalar(Value::Null),
        }),
        value => scalar_property(&value, key, account).map(ProjectedValue::scalar),
    }
}

/// Property access on a non-map scalar: temporal components and NULL.
pub(super) fn scalar_property(
    value: &Value,
    key: &str,
    account: Option<&QueryMemoryAccount>,
) -> Result<Value> {
    match value {
        Value::Null => Ok(Value::Null),
        value if hawdb_core::temporal::is_temporal(value) => {
            hawdb_core::temporal::component(value, key)
                .map_err(|error| HawDBError::Execution(error.to_string()))
        }
        value => Err(HawDBError::Execution(format!(
            "property access '.{key}' requires a map or temporal value, got {:?}",
            description(value, account)
        ))),
    }
}

pub(super) fn map_literal<'a>(
    entries: &'a [(String, ProjectionExpression)],
    evaluate: impl Fn(&'a ProjectionExpression) -> Result<ProjectedValue<'a>>,
    account: Option<&QueryMemoryAccount>,
    work: &ListWork,
) -> Result<ProjectedValue<'a>> {
    let mut map = std::collections::BTreeMap::new();
    for (key, value) in entries {
        map.insert(key.clone(), evaluate(value)?.into_owned());
    }
    let bytes = map_memory_bytes(&map).saturating_add(std::mem::size_of::<Value>());
    match account {
        Some(account) => {
            let allocation = account.reserve(bytes)?;
            Ok(ProjectedValue {
                value: Cow::Owned(Value::Map(map)),
                _allocation: Some(allocation),
            })
        }
        None => {
            work.charge_unaccounted(bytes)?;
            Ok(ProjectedValue::scalar(Value::Map(map)))
        }
    }
}
