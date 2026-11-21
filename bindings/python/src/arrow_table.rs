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

//! `QueryResult.to_arrow()`: a materialized result as a `pyarrow.Table`.
//!
//! Results declare no column types, so each column's Arrow type is inferred
//! from its values. Inference reads the engine's [`Value`] variants, not their
//! Python form, where a UUID and a string are both `str` and temporal values
//! are ISO text. Each value then becomes the object that `pyarrow.array`
//! stores exactly in that type: integer days and nanoseconds, a
//! `(months, days, nanoseconds)` tuple, or a `decimal.Decimal`. PyArrow is
//! imported on first use; the extension does not link it.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

use hawdb::temporal::{OffsetDateTime, UtcOffset};
use hawdb::{QueryRows, TemporalDuration, Value};
use pyo3::exceptions::{PyImportError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::IntoPyObjectExt;

/// The Julian day number of 1970-01-01, the epoch of Arrow's `date32`.
const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;
/// The most digits `decimal128` holds; wider columns use `decimal256`.
const DECIMAL128_DIGITS: u8 = 38;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

/// Imports the optional package `module` for `QueryResult.{method}()`, or
/// raises `ImportError` naming the extra that installs it, chained to the
/// original error.
pub fn import_optional<'py>(
    py: Python<'py>,
    module: &str,
    method: &str,
    extra: &str,
) -> PyResult<Bound<'py, PyModule>> {
    py.import(module).map_err(|error| {
        if !error.is_instance_of::<PyImportError>(py) {
            return error;
        }
        let missing = PyImportError::new_err(format!(
            "QueryResult.{method}() requires {module}; \
             install it with \"pip install 'hawdb[{extra}]'\""
        ));
        missing.set_cause(py, Some(error));
        missing
    })
}

/// Every row of `rows` as a `pyarrow.Table`, one column per result column
/// in result order. `method` and `extra` name the public call in errors.
pub fn table<'py>(
    py: Python<'py>,
    rows: &QueryRows,
    method: &str,
    extra: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let pyarrow = import_optional(py, "pyarrow", method, extra)?;
    let columns = rows.schema().columns();
    let mut converter = Converter { py, decimal: None };
    let mut arrays = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let mut kind = Kind::Null;
        // Only one column's Python values are alive at a time: PyArrow
        // copies them into the array, and the list is dropped after it.
        let values = PyList::empty(py);
        for (row, row_values) in rows.value_rows().enumerate() {
            let value = &row_values[index];
            let place = Place {
                method,
                column,
                row,
            };
            kind.absorb(value)
                .map_err(|conflict| PyTypeError::new_err(format!("{place}{conflict}")))?;
            match converter.convert(value) {
                Ok(item) => values.append(item)?,
                Err(Failure::Python(error)) => return Err(error),
                Err(Failure::Range(range)) => {
                    return Err(PyValueError::new_err(format!("{place}: {range}")));
                }
            }
        }
        arrays.push(pyarrow.call_method1("array", (values, arrow_type(&pyarrow, &kind)?))?);
    }
    pyarrow
        .getattr("Table")?
        .call_method1("from_arrays", (arrays, columns))
}

/// The Arrow type of one column or nested field, refined value by value.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    /// No non-null value yet: Arrow's `null` type.
    Null,
    Bool,
    Int,
    Float,
    /// Wide enough for every value: the most integer digits and the largest
    /// scale seen.
    Decimal {
        integer_digits: u8,
        scale: u8,
    },
    /// Strings, and UUIDs as their canonical text.
    Text,
    Binary,
    /// Float32 coordinates, `Some(dimension)` while every vector has it.
    Vector(Option<u32>),
    Date,
    LocalTime,
    LocalDateTime,
    /// Instants, `Some(offset)` while every value has that UTC offset.
    DateTime(Option<UtcOffset>),
    Duration,
    List(Box<Kind>),
    /// A field for every key of any map, null where a map lacks the key.
    Struct(BTreeMap<String, Kind>),
}

impl Kind {
    /// Widens the type to hold `value` too, or reports two kinds of values
    /// that no single Arrow type holds exactly.
    fn absorb(&mut self, value: &Value) -> Result<(), Conflict> {
        let seen = match value {
            Value::Null => return Ok(()),
            Value::List(items) => {
                if matches!(self, Kind::Null) {
                    *self = Kind::List(Box::new(Kind::Null));
                }
                let Kind::List(item) = self else {
                    return Err(Conflict::new(self, value));
                };
                return items.iter().try_for_each(|value| {
                    item.absorb(value)
                        .map_err(|conflict| conflict.within(Step::Item))
                });
            }
            Value::Map(entries) => {
                if matches!(self, Kind::Null) {
                    *self = Kind::Struct(BTreeMap::new());
                }
                let Kind::Struct(fields) = self else {
                    return Err(Conflict::new(self, value));
                };
                for (key, value) in entries {
                    let absorbed = match fields.get_mut(key) {
                        Some(field) => field.absorb(value),
                        None => {
                            let mut field = Kind::Null;
                            let absorbed = field.absorb(value);
                            fields.insert(key.clone(), field);
                            absorbed
                        }
                    };
                    absorbed.map_err(|conflict| conflict.within(Step::Field(key.clone())))?;
                }
                return Ok(());
            }
            Value::Bool(_) => Kind::Bool,
            Value::Int(_) => Kind::Int,
            Value::Float(_) => Kind::Float,
            Value::Decimal(decimal) => Kind::Decimal {
                integer_digits: decimal.precision().saturating_sub(decimal.scale()),
                scale: decimal.scale(),
            },
            Value::String(_) | Value::Uuid(_) => Kind::Text,
            Value::Binary(_) => Kind::Binary,
            Value::Vector(vector) => Kind::Vector(Some(vector.dimension().get())),
            Value::Date(_) => Kind::Date,
            Value::LocalTime(_) => Kind::LocalTime,
            Value::LocalDateTime(_) => Kind::LocalDateTime,
            Value::DateTime(instant) => Kind::DateTime(Some(instant.offset())),
            Value::Duration(_) => Kind::Duration,
        };
        if matches!(self, Kind::Null) {
            *self = seen;
            return Ok(());
        }
        match (self, seen) {
            (
                Kind::Decimal {
                    integer_digits,
                    scale,
                },
                Kind::Decimal {
                    integer_digits: seen_digits,
                    scale: seen_scale,
                },
            ) => {
                *integer_digits = (*integer_digits).max(seen_digits);
                *scale = (*scale).max(seen_scale);
            }
            (Kind::Vector(dimension), Kind::Vector(seen)) => {
                if *dimension != seen {
                    *dimension = None;
                }
            }
            (Kind::DateTime(offset), Kind::DateTime(seen)) => {
                if *offset != seen {
                    *offset = None;
                }
            }
            (current, seen) if *current == seen => {}
            (current, _) => return Err(Conflict::new(current, value)),
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        match self {
            Kind::Null => "null",
            Kind::Bool => "boolean",
            Kind::Int => "integer",
            Kind::Float => "float",
            Kind::Decimal { .. } => "decimal",
            Kind::Text => "string",
            Kind::Binary => "binary",
            Kind::Vector(_) => "vector",
            Kind::Date => "date",
            Kind::LocalTime => "local time",
            Kind::LocalDateTime => "local datetime",
            Kind::DateTime(_) => "zoned datetime",
            Kind::Duration => "duration",
            Kind::List(_) => "list",
            Kind::Struct(_) => "map",
        }
    }
}

/// The name of a value's kind in conflict messages.
fn value_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Int(_) => "integer",
        Value::Float(_) => "float",
        Value::Decimal(_) => "decimal",
        Value::String(_) => "string",
        Value::Uuid(_) => "uuid",
        Value::Binary(_) => "binary",
        Value::Vector(_) => "vector",
        Value::Date(_) => "date",
        Value::LocalTime(_) => "local time",
        Value::LocalDateTime(_) => "local datetime",
        Value::DateTime(_) => "zoned datetime",
        Value::Duration(_) => "duration",
        Value::List(_) => "list",
        Value::Map(_) => "map",
    }
}

/// The Arrow type for `kind`, built with PyArrow's type factories.
fn arrow_type<'py>(pyarrow: &Bound<'py, PyModule>, kind: &Kind) -> PyResult<Bound<'py, PyAny>> {
    match kind {
        Kind::Null => pyarrow.call_method0("null"),
        Kind::Bool => pyarrow.call_method0("bool_"),
        Kind::Int => pyarrow.call_method0("int64"),
        Kind::Float => pyarrow.call_method0("float64"),
        Kind::Decimal {
            integer_digits,
            scale,
        } => {
            // Both are at most 38, so the sum cannot overflow.
            let precision = (integer_digits + scale).max(1);
            let factory = if precision <= DECIMAL128_DIGITS {
                "decimal128"
            } else {
                "decimal256"
            };
            pyarrow.call_method1(factory, (precision, *scale))
        }
        Kind::Text => pyarrow.call_method0("string"),
        Kind::Binary => pyarrow.call_method0("binary"),
        Kind::Vector(Some(dimension)) => {
            pyarrow.call_method1("list_", (pyarrow.call_method0("float32")?, *dimension))
        }
        Kind::Vector(None) => pyarrow.call_method1("list_", (pyarrow.call_method0("float32")?,)),
        Kind::Date => pyarrow.call_method0("date32"),
        Kind::LocalTime => pyarrow.call_method1("time64", ("ns",)),
        Kind::LocalDateTime => pyarrow.call_method1("timestamp", ("ns",)),
        Kind::DateTime(offset) => pyarrow.call_method1("timestamp", ("ns", time_zone(*offset))),
        Kind::Duration => pyarrow.call_method0("month_day_nano_interval"),
        Kind::List(item) => pyarrow.call_method1("list_", (arrow_type(pyarrow, item)?,)),
        Kind::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, kind)| Ok((name.as_str(), arrow_type(pyarrow, kind)?)))
                .collect::<PyResult<Vec<_>>>()?;
            pyarrow.call_method1("struct", (fields,))
        }
    }
}

/// The time zone of a zoned datetime column: the values' common offset when
/// they share one in whole minutes, which Arrow can name, and UTC otherwise.
/// Arrow stores the UTC instant either way.
fn time_zone(offset: Option<UtcOffset>) -> String {
    match offset {
        Some(offset) if !offset.is_utc() && offset.seconds_past_minute() == 0 => {
            let (hours, minutes, _) = offset.as_hms();
            let sign = if offset.is_negative() { '-' } else { '+' };
            format!(
                "{sign}{:02}:{:02}",
                hours.unsigned_abs(),
                minutes.unsigned_abs()
            )
        }
        _ => "UTC".to_owned(),
    }
}

/// Converts values into the objects `pyarrow.array` stores exactly.
struct Converter<'py> {
    py: Python<'py>,
    /// `decimal.Decimal`, imported on the first decimal value.
    decimal: Option<Bound<'py, PyAny>>,
}

impl<'py> Converter<'py> {
    fn convert(&mut self, value: &Value) -> Result<Bound<'py, PyAny>, Failure> {
        let py = self.py;
        Ok(match value {
            Value::Null => py.None().into_bound(py),
            Value::Bool(item) => item.into_bound_py_any(py)?,
            Value::Int(item) => item.into_bound_py_any(py)?,
            Value::Float(item) => item.into_bound_py_any(py)?,
            Value::Decimal(item) => self.decimal()?.call1((item.to_string(),))?,
            Value::String(item) => item.as_str().into_bound_py_any(py)?,
            Value::Uuid(item) => item.to_string().into_bound_py_any(py)?,
            Value::Binary(item) => PyBytes::new(py, item).into_any(),
            Value::Vector(item) => PyList::new(py, item.as_ref().coordinates())?.into_any(),
            Value::Date(date) => {
                (date.to_julian_day() - UNIX_EPOCH_JULIAN_DAY).into_bound_py_any(py)?
            }
            Value::LocalTime(time) => {
                let (hours, minutes, seconds, nanos) = time.as_hms_nano();
                let seconds =
                    (i64::from(hours) * 60 + i64::from(minutes)) * 60 + i64::from(seconds);
                (seconds * NANOS_PER_SECOND + i64::from(nanos)).into_bound_py_any(py)?
            }
            Value::LocalDateTime(local) => {
                timestamp(local.assume_utc(), value)?.into_bound_py_any(py)?
            }
            Value::DateTime(instant) => timestamp(*instant, value)?.into_bound_py_any(py)?,
            Value::Duration(duration) => interval(duration, value)?.into_bound_py_any(py)?,
            Value::List(items) => {
                let list = PyList::empty(py);
                for item in items {
                    list.append(self.convert(item)?)?;
                }
                list.into_any()
            }
            Value::Map(entries) => {
                let dict = PyDict::new(py);
                for (key, item) in entries {
                    dict.set_item(key, self.convert(item)?)?;
                }
                dict.into_any()
            }
        })
    }

    fn decimal(&mut self) -> PyResult<&Bound<'py, PyAny>> {
        if self.decimal.is_none() {
            self.decimal = Some(self.py.import("decimal")?.getattr("Decimal")?);
        }
        Ok(self.decimal.as_ref().expect("imported above"))
    }
}

/// Nanoseconds since the Unix epoch, Arrow's `timestamp[ns]`.
fn timestamp(instant: OffsetDateTime, value: &Value) -> Result<i64, OutOfRange> {
    i64::try_from(instant.unix_timestamp_nanos()).map_err(|_| OutOfRange {
        value: value.to_string(),
        target: "timestamp[ns], which spans 1677-09-21 to 2262-04-11",
    })
}

/// Arrow's `month_day_nano_interval` parts of a duration.
fn interval(duration: &TemporalDuration, value: &Value) -> Result<(i32, i32, i64), OutOfRange> {
    match (
        i32::try_from(duration.months),
        i32::try_from(duration.days),
        i64::try_from(duration.time_nanos()),
    ) {
        (Ok(months), Ok(days), Ok(nanos)) => Ok((months, days, nanos)),
        _ => Err(OutOfRange {
            value: value.to_string(),
            target: "month_day_nano_interval, which has 32-bit months and days \
                     and 64-bit nanoseconds",
        }),
    }
}

enum Failure {
    Range(OutOfRange),
    Python(PyErr),
}

impl From<PyErr> for Failure {
    fn from(error: PyErr) -> Self {
        Self::Python(error)
    }
}

impl From<OutOfRange> for Failure {
    fn from(range: OutOfRange) -> Self {
        Self::Range(range)
    }
}

/// A value outside the range of its Arrow type.
struct OutOfRange {
    value: String,
    target: &'static str,
}

impl Display for OutOfRange {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} does not fit Arrow's {}",
            self.value, self.target
        )
    }
}

/// Two kinds of values where one Arrow type must hold both.
struct Conflict {
    expected: &'static str,
    found: &'static str,
    /// From the conflicting value out to the column value.
    steps: Vec<Step>,
}

enum Step {
    Field(String),
    Item,
}

impl Conflict {
    fn new(kind: &Kind, value: &Value) -> Self {
        Self {
            expected: kind.name(),
            found: value_name(value),
            steps: Vec::new(),
        }
    }

    fn within(mut self, step: Step) -> Self {
        self.steps.push(step);
        self
    }
}

/// Formats as `, at $.key[*]: <found> value after <expected> values; ...`,
/// to follow a [`Place`]. The path is omitted for a column value itself.
impl Display for Conflict {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        if !self.steps.is_empty() {
            formatter.write_str(", at $")?;
            for step in self.steps.iter().rev() {
                match step {
                    Step::Field(key) if is_identifier(key) => write!(formatter, ".{key}")?,
                    Step::Field(key) => write!(formatter, "[{key:?}]")?,
                    Step::Item => formatter.write_str("[*]")?,
                }
            }
        }
        write!(
            formatter,
            ": {} value after {} values; an Arrow column holds one type, \
             so cast the values in the query",
            self.found, self.expected
        )
    }
}

/// HawDB's Cypher identifier rule, as in parameter paths.
fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
}

/// Where a conversion failed, for error messages.
struct Place<'a> {
    method: &'a str,
    column: &'a str,
    row: usize,
}

impl Display for Place<'_> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "QueryResult.{}(): column {:?}, row {}",
            self.method, self.column, self.row
        )
    }
}
