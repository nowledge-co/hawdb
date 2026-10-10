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
use std::fmt::{self, Formatter};

use hawdb::{Uuid, Value};
use pyo3::exceptions::{PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyFloat, PyList, PyString, PyTuple};
use pyo3::IntoPyObjectExt;

/// How deep lists, tuples, and dicts may nest in one parameter: the
/// executor's limit for spilled values. It bounds the conversion's recursion,
/// so a deeper or self-containing value raises instead of overflowing the
/// stack.
const MAX_NESTING: usize = 64;

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
/// `int`. Integers, including objects implementing `__index__`, must fit in
/// `i64` or raise `OverflowError`. Only `float` instances and NumPy's
/// `float16`/`float32` scalars, which widen to `f64` exactly, become floats:
/// `__float__` alone may round, so `Decimal`, `Fraction`, or NumPy
/// `longdouble` raise `TypeError` instead of being stored inexactly.
/// `uuid.UUID` is accepted via its string form; everything outside the listed
/// types raises `TypeError` rather than guessing. Lists, tuples, and dicts
/// nested deeper than [`MAX_NESTING`] raise `ValueError`. These errors start
/// with `path`, the value's place in the parameters, such as `$rows[3].price`.
/// A `str` that is not valid UTF-8 raises Python's `UnicodeEncodeError`.
fn py_to_value(object: &Bound<'_, PyAny>, path: &ParamPath<'_>, depth: usize) -> PyResult<Value> {
    if object.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(item) = object.extract::<bool>() {
        return Ok(Value::Bool(item));
    }
    match object.extract::<i64>() {
        Ok(item) => return Ok(Value::Int(item)),
        Err(error) if error.is_instance_of::<PyOverflowError>(object.py()) => {
            return Err(PyOverflowError::new_err(format!(
                "{path}: integer parameter is outside the signed 64-bit range"
            )));
        }
        Err(_) => {}
    }
    if let Ok(item) = object.cast::<PyFloat>() {
        return Ok(Value::Float(item.value()));
    }
    if let Ok(item) = object.cast::<PyBytes>() {
        return Ok(Value::Binary(item.as_bytes().to_vec()));
    }
    if object.get_type().name()? == "UUID" {
        let text = object.str()?.to_string();
        return Uuid::parse_str(&text)
            .map(Value::Uuid)
            .map_err(|_| PyTypeError::new_err(format!("{path}: invalid uuid.UUID value: {text}")));
    }
    if let Ok(text) = object.cast::<PyString>() {
        return Ok(Value::String(text.to_str()?.to_owned()));
    }
    if let Ok(list) = object.cast::<PyList>() {
        let depth = nest(path, depth)?;
        return list
            .iter()
            .enumerate()
            .map(|(index, item)| py_to_value(&item, &ParamPath::Index(path, index), depth))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::List);
    }
    if let Ok(tuple) = object.cast::<PyTuple>() {
        let depth = nest(path, depth)?;
        return tuple
            .iter()
            .enumerate()
            .map(|(index, item)| py_to_value(&item, &ParamPath::Index(path, index), depth))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::List);
    }
    if let Ok(dict) = object.cast::<PyDict>() {
        let depth = nest(path, depth)?;
        let mut map = BTreeMap::new();
        for (key, item) in dict.iter() {
            let Ok(key_text) = key.cast::<PyString>() else {
                return Err(PyTypeError::new_err(format!(
                    "{path}: parameter maps require string keys, got {}",
                    key.get_type().name()?
                )));
            };
            let key_text = key_text.to_str()?;
            let value = py_to_value(&item, &ParamPath::Key(path, key_text), depth)?;
            map.insert(key_text.to_owned(), value);
        }
        return Ok(Value::Map(map));
    }
    // Checked last so common parameter types skip the type lookup.
    if is_numpy_narrow_float(object)? {
        return object.extract::<f64>().map(Value::Float);
    }
    Err(PyTypeError::new_err(format!(
        "{path}: unsupported parameter type: {}",
        object.get_type().name()?
    )))
}

/// Returns the depth for the items of a list, tuple, or dict at `depth`, or
/// rejects a container nested deeper than [`MAX_NESTING`].
fn nest(path: &ParamPath<'_>, depth: usize) -> PyResult<usize> {
    if depth == MAX_NESTING {
        return Err(PyValueError::new_err(format!(
            "{path}: parameter nests deeper than {MAX_NESTING} levels"
        )));
    }
    Ok(depth + 1)
}

/// NumPy scalars are not `float` subclasses below `float64`, so they are
/// recognized by type, as PyO3 does for `numpy.bool_`.
fn is_numpy_narrow_float(object: &Bound<'_, PyAny>) -> PyResult<bool> {
    let ty = object.get_type();
    // Proxies can define `__module__` as something other than a `str`.
    Ok(ty.module().is_ok_and(|module| module == "numpy") && {
        let name = ty.name()?;
        name == "float16" || name == "float32"
    })
}

/// Converts a Python `dict` of `$name` query parameters into the HawDB
/// parameter map.
pub fn py_dict_to_params(dict: &Bound<'_, PyDict>) -> PyResult<BTreeMap<String, Value>> {
    let mut params = BTreeMap::new();
    for (key, item) in dict.iter() {
        let Ok(name) = key.cast::<PyString>() else {
            return Err(PyTypeError::new_err(format!(
                "query parameter names must be strings, got {}",
                key.get_type().name()?
            )));
        };
        let name = name.to_str()?;
        let value = py_to_value(&item, &ParamPath::Name(name), 0)?;
        params.insert(name.to_owned(), value);
    }
    Ok(params)
}

/// Converts a Python `list` of positional `$1`, `$2`, ... query parameters.
pub fn py_list_to_params(list: &Bound<'_, PyList>) -> PyResult<Vec<Value>> {
    list.iter()
        .enumerate()
        .map(|(index, item)| py_to_value(&item, &ParamPath::Position(index + 1), 0))
        .collect()
}

/// A value's place in the parameters: `$name` or `$1`, then `[index]` into a
/// list and `.key` into a dict. A name or key that is not an identifier is
/// quoted, as in `$["first name"]` or `$m["价格"]`. Each level borrows its
/// parent from the stack, so a successful conversion builds no path.
enum ParamPath<'a> {
    Name(&'a str),
    Position(usize),
    Index(&'a ParamPath<'a>, usize),
    Key(&'a ParamPath<'a>, &'a str),
}

impl fmt::Display for ParamPath<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) if is_identifier(name) => write!(formatter, "${name}"),
            Self::Name(name) => write!(formatter, "$[{name:?}]"),
            Self::Position(position) => write!(formatter, "${position}"),
            Self::Index(parent, index) => write!(formatter, "{parent}[{index}]"),
            Self::Key(parent, key) if is_identifier(key) => write!(formatter, "{parent}.{key}"),
            Self::Key(parent, key) => write!(formatter, "{parent}[{key:?}]"),
        }
    }
}

/// HawDB's Cypher identifier rule: an ASCII letter or `_`, then ASCII
/// letters, digits, or `_`.
fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
}
