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

use std::collections::BTreeMap;

use hawdb::{Uuid, Value};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::IntoPyObjectExt;

/// Converts a HawDB [`Value`] into an owned Python object.
///
/// `Uuid` values arrive as canonical strings; callers that need `uuid.UUID`
/// can wrap the string on the Python side without paying the import cost for
/// every result set.
pub fn value_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(item) => item.into_py_any(py),
        Value::Int(item) => item.into_py_any(py),
        Value::Float(item) => item.into_py_any(py),
        Value::String(item) => item.as_str().into_py_any(py),
        Value::Binary(item) => Ok(PyBytes::new(py, item).unbind().into()),
        Value::Uuid(item) => item.to_string().into_py_any(py),
        Value::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(value_to_py(py, item)?)?;
            }
            Ok(list.unbind().into())
        }
        Value::Map(items) => {
            let dict = PyDict::new(py);
            for (key, item) in items {
                dict.set_item(key, value_to_py(py, item)?)?;
            }
            Ok(dict.unbind().into())
        }
    }
}

/// Converts a Python object into a HawDB [`Value`] for query parameters.
///
/// `bool` is checked before `int` because Python's `bool` is a subclass of
/// `int`. `uuid.UUID` is accepted via its string form; everything outside the
/// listed types raises `TypeError` rather than guessing.
pub fn py_to_value(object: &Bound<'_, PyAny>) -> PyResult<Value> {
    if object.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(item) = object.extract::<bool>() {
        return Ok(Value::Bool(item));
    }
    if let Ok(item) = object.extract::<i64>() {
        return Ok(Value::Int(item));
    }
    if let Ok(item) = object.extract::<f64>() {
        return Ok(Value::Float(item));
    }
    if let Ok(item) = object.cast::<PyBytes>() {
        return Ok(Value::Binary(item.as_bytes().to_vec()));
    }
    if object.get_type().name()? == "UUID" {
        let text = object.str()?.to_string();
        return Uuid::parse_str(&text)
            .map(Value::Uuid)
            .map_err(|_| PyTypeError::new_err(format!("invalid uuid.UUID value: {text}")));
    }
    if let Ok(item) = object.extract::<String>() {
        return Ok(Value::String(item));
    }
    if let Ok(list) = object.cast::<PyList>() {
        return list
            .iter()
            .map(|item| py_to_value(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::List);
    }
    if let Ok(tuple) = object.cast::<PyTuple>() {
        return tuple
            .iter()
            .map(|item| py_to_value(&item))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::List);
    }
    if let Ok(dict) = object.cast::<PyDict>() {
        let mut map = BTreeMap::new();
        for (key, item) in dict.iter() {
            let key = key
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err("parameter maps require string keys"))?;
            map.insert(key, py_to_value(&item)?);
        }
        return Ok(Value::Map(map));
    }
    Err(PyTypeError::new_err(format!(
        "unsupported parameter type: {}",
        object.get_type().name()?
    )))
}

/// Converts a Python `dict` of query parameters into the HawDB parameter map.
pub fn py_dict_to_params(dict: &Bound<'_, PyDict>) -> PyResult<BTreeMap<String, Value>> {
    let mut params = BTreeMap::new();
    for (key, item) in dict.iter() {
        let key = key
            .extract::<String>()
            .map_err(|_| PyTypeError::new_err("query parameter names must be strings"))?;
        params.insert(key, py_to_value(&item)?);
    }
    Ok(params)
}
