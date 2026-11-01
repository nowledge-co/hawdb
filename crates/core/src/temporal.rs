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

//! openCypher temporal values: dates, local times, local and zoned datetimes,
//! and durations.
//!
//! Instants are represented with chrono types. Zoned datetimes carry a fixed
//! UTC offset; named time-zone regions are not supported. A [`Duration`]
//! keeps calendar months and days separate from its exact time part, as
//! openCypher requires: one month or one day has no fixed length.

mod arithmetic;
pub mod codec;
mod component;
mod construct;
mod format;
mod parse;

use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

pub use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Utc};

use crate::Value;

use arithmetic::between;
pub use arithmetic::{arithmetic, truncate, TemporalArithmeticOp};
pub use component::component;
pub use construct::{construct, from_epoch, from_epoch_millis};
pub use format::{format_date, format_datetime, format_local_datetime, format_local_time};
pub use parse::{
    parse_date, parse_datetime, parse_duration, parse_local_datetime, parse_local_time,
};

pub(crate) const NANOS_PER_SECOND: i128 = 1_000_000_000;
pub(crate) const SECONDS_PER_DAY: i128 = 86_400;
pub(crate) const NANOS_PER_DAY: i128 = NANOS_PER_SECOND * SECONDS_PER_DAY;
/// The openCypher (and ISO 8601 Gregorian) average month length: 30.436875
/// days, used only to cascade fractional months into days.
pub(crate) const AVERAGE_DAYS_PER_MONTH: f64 = 30.436_875;

/// A temporal evaluation error. Callers map it to the error class of the
/// evaluating layer (semantic during planning, execution at run time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalError(String);

impl TemporalError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

impl Display for TemporalError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TemporalError {}

impl From<TemporalError> for crate::HawDBError {
    fn from(error: TemporalError) -> Self {
        crate::HawDBError::Execution(error.0)
    }
}

pub type TemporalResult<T> = std::result::Result<T, TemporalError>;

/// An openCypher duration.
///
/// `months` and `days` are calendar quantities whose length depends on the
/// instant they are applied to; `time` is an exact length. The derived order
/// (months, days, time) is a total internal order used for sorting, grouping
/// and indexes. Cypher `<`/`>` between durations is undefined (NULL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Duration {
    pub months: i64,
    pub days: i64,
    pub time: TimeDelta,
}

impl Duration {
    pub const fn new(months: i64, days: i64, time: TimeDelta) -> Self {
        Self { months, days, time }
    }

    /// Builds a duration from a nanosecond time part.
    pub fn from_parts(months: i64, days: i64, nanos: i128) -> Option<Self> {
        Some(Self::new(months, days, delta_from_nanos(nanos)?))
    }

    /// The time part in nanoseconds.
    pub fn time_nanos(&self) -> i128 {
        delta_nanos(self.time)
    }

    /// Whole seconds of the time part, rounded toward negative infinity so
    /// that [`Self::nanos_of_second`] is always non-negative.
    pub fn seconds(&self) -> i64 {
        self.time_nanos().div_euclid(NANOS_PER_SECOND) as i64
    }

    /// Nanoseconds of the time part within its second, in `0..1_000_000_000`.
    pub fn nanos_of_second(&self) -> i64 {
        self.time_nanos().rem_euclid(NANOS_PER_SECOND) as i64
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self::new(
            self.months.checked_add(other.months)?,
            self.days.checked_add(other.days)?,
            self.time.checked_add(&other.time)?,
        ))
    }

    pub fn checked_neg(self) -> Option<Self> {
        Some(Self::new(
            self.months.checked_neg()?,
            self.days.checked_neg()?,
            delta_from_nanos(-self.time_nanos())?,
        ))
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_add(other.checked_neg()?)
    }
}

impl Display for Duration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        format::write_duration(f, self)
    }
}

pub(crate) fn delta_nanos(delta: TimeDelta) -> i128 {
    i128::from(delta.num_seconds()) * NANOS_PER_SECOND + i128::from(delta.subsec_nanos())
}

pub(crate) fn delta_from_nanos(nanos: i128) -> Option<TimeDelta> {
    let seconds = i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).ok()?;
    let nanos = u32::try_from(nanos.rem_euclid(NANOS_PER_SECOND)).ok()?;
    TimeDelta::new(seconds, nanos)
}

/// The four instant kinds that openCypher constructors and truncation produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TemporalKind {
    Date,
    LocalTime,
    LocalDateTime,
    DateTime,
}

impl TemporalKind {
    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Date => "date",
            Self::LocalTime => "localtime",
            Self::LocalDateTime => "localdatetime",
            Self::DateTime => "datetime",
        }
    }

    pub const fn has_date(self) -> bool {
        !matches!(self, Self::LocalTime)
    }

    pub const fn has_time(self) -> bool {
        !matches!(self, Self::Date)
    }
}

/// The openCypher temporal functions HawDB evaluates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TemporalFunction {
    /// `date()`, `localtime()`, `localdatetime()` or `datetime()`: the
    /// statement clock without an argument, otherwise a string, map or
    /// temporal conversion.
    Construct(TemporalKind),
    /// `duration(string|map)`.
    Duration,
    /// `duration.between(a, b)`.
    DurationBetween,
    /// `duration.inMonths(a, b)`.
    DurationInMonths,
    /// `duration.inDays(a, b)`.
    DurationInDays,
    /// `duration.inSeconds(a, b)`.
    DurationInSeconds,
    /// `date.truncate(unit, value)` and its siblings.
    Truncate(TemporalKind),
    /// `datetime.fromepoch(seconds, nanoseconds)`.
    DateTimeFromEpoch,
    /// `datetime.fromepochmillis(milliseconds)`.
    DateTimeFromEpochMillis,
}

const KINDS: [TemporalKind; 4] = [
    TemporalKind::Date,
    TemporalKind::LocalTime,
    TemporalKind::LocalDateTime,
    TemporalKind::DateTime,
];

impl TemporalFunction {
    /// Every function, for parser dispatch and exhaustive tests.
    pub const ALL: [Self; 15] = [
        Self::Construct(TemporalKind::Date),
        Self::Construct(TemporalKind::LocalTime),
        Self::Construct(TemporalKind::LocalDateTime),
        Self::Construct(TemporalKind::DateTime),
        Self::Duration,
        Self::DurationBetween,
        Self::DurationInMonths,
        Self::DurationInDays,
        Self::DurationInSeconds,
        Self::Truncate(TemporalKind::Date),
        Self::Truncate(TemporalKind::LocalTime),
        Self::Truncate(TemporalKind::LocalDateTime),
        Self::Truncate(TemporalKind::DateTime),
        Self::DateTimeFromEpoch,
        Self::DateTimeFromEpochMillis,
    ];

    /// Returns whether `name` (case-insensitive) is a temporal namespace that
    /// may start a function call: `date`, `localtime`, `localdatetime`,
    /// `datetime` or `duration`.
    pub fn is_namespace(name: &str) -> bool {
        KINDS
            .iter()
            .any(|kind| name.eq_ignore_ascii_case(kind.function_name()))
            || name.eq_ignore_ascii_case("duration")
    }

    /// Resolves `namespace` or `namespace.member` case-insensitively.
    pub fn from_name(namespace: &str, member: Option<&str>) -> Option<Self> {
        Self::ALL.into_iter().find(|function| {
            let (expected_namespace, expected_member) = function.name_parts();
            namespace.eq_ignore_ascii_case(expected_namespace)
                && match (member, expected_member) {
                    (None, None) => true,
                    (Some(member), Some(expected)) => member.eq_ignore_ascii_case(expected),
                    _ => false,
                }
        })
    }

    const fn name_parts(self) -> (&'static str, Option<&'static str>) {
        match self {
            Self::Construct(kind) => (kind.function_name(), None),
            Self::Duration => ("duration", None),
            Self::DurationBetween => ("duration", Some("between")),
            Self::DurationInMonths => ("duration", Some("inMonths")),
            Self::DurationInDays => ("duration", Some("inDays")),
            Self::DurationInSeconds => ("duration", Some("inSeconds")),
            Self::Truncate(kind) => (kind.function_name(), Some("truncate")),
            Self::DateTimeFromEpoch => ("datetime", Some("fromepoch")),
            Self::DateTimeFromEpochMillis => ("datetime", Some("fromepochmillis")),
        }
    }

    /// The canonical Cypher spelling, such as `duration.between`.
    pub fn function_name(self) -> String {
        match self.name_parts() {
            (namespace, None) => namespace.to_string(),
            (namespace, Some(member)) => format!("{namespace}.{member}"),
        }
    }

    /// Inclusive minimum and maximum argument counts.
    pub const fn arity(self) -> (usize, usize) {
        match self {
            Self::Construct(_) => (0, 1),
            Self::Duration | Self::DateTimeFromEpochMillis => (1, 1),
            Self::DurationBetween
            | Self::DurationInMonths
            | Self::DurationInDays
            | Self::DurationInSeconds
            | Self::Truncate(_)
            | Self::DateTimeFromEpoch => (2, 2),
        }
    }

    /// Whether a call with `argument_count` arguments reads the statement
    /// clock (and therefore must not be cached or folded across statements).
    pub const fn reads_clock(self, argument_count: usize) -> bool {
        matches!(self, Self::Construct(_)) && argument_count == 0
    }
}

/// Evaluates a temporal function. NULL arguments produce NULL, except that
/// a missing argument to a constructor reads `now`.
pub fn evaluate(
    function: TemporalFunction,
    arguments: &[Value],
    now: DateTime<Utc>,
) -> TemporalResult<Value> {
    let (min, max) = function.arity();
    if !(min..=max).contains(&arguments.len()) {
        return Err(TemporalError::new(format!(
            "{}() expects {} argument(s), got {}",
            function.function_name(),
            if min == max {
                min.to_string()
            } else {
                format!("{min} or {max}")
            },
            arguments.len()
        )));
    }
    if arguments.iter().any(|value| matches!(value, Value::Null)) {
        return Ok(Value::Null);
    }
    match function {
        TemporalFunction::Construct(kind) => construct(kind, arguments.first(), now),
        TemporalFunction::Duration => construct::duration(&arguments[0]),
        TemporalFunction::DurationBetween => {
            between(&arguments[0], &arguments[1], arithmetic::BetweenMode::Full)
        }
        TemporalFunction::DurationInMonths => between(
            &arguments[0],
            &arguments[1],
            arithmetic::BetweenMode::Months,
        ),
        TemporalFunction::DurationInDays => {
            between(&arguments[0], &arguments[1], arithmetic::BetweenMode::Days)
        }
        TemporalFunction::DurationInSeconds => between(
            &arguments[0],
            &arguments[1],
            arithmetic::BetweenMode::Seconds,
        ),
        TemporalFunction::Truncate(kind) => {
            let Value::String(unit) = &arguments[0] else {
                return Err(TemporalError::new(format!(
                    "{}() unit must be a string",
                    function.function_name()
                )));
            };
            truncate(kind, unit, &arguments[1])
        }
        TemporalFunction::DateTimeFromEpoch => match (&arguments[0], &arguments[1]) {
            (Value::Int(seconds), Value::Int(nanos)) => from_epoch(*seconds, *nanos),
            _ => Err(TemporalError::new(
                "datetime.fromepoch() expects integer seconds and nanoseconds",
            )),
        },
        TemporalFunction::DateTimeFromEpochMillis => match &arguments[0] {
            Value::Int(millis) => from_epoch_millis(*millis),
            _ => Err(TemporalError::new(
                "datetime.fromepochmillis() expects integer milliseconds",
            )),
        },
    }
}

/// Returns whether `value` is a temporal instant or a duration.
pub const fn is_temporal(value: &Value) -> bool {
    matches!(
        value,
        Value::Date(_)
            | Value::LocalTime(_)
            | Value::LocalDateTime(_)
            | Value::DateTime(_)
            | Value::Duration(_)
    )
}

/// The Cypher comparison order of two temporal values: `Some` for two
/// instants of the same kind, `None` otherwise (including two durations,
/// which openCypher leaves incomparable).
///
/// The result agrees with [`Value`]'s total order, so index range seeks and
/// predicate evaluation select the same rows. Zoned datetimes compare by
/// instant and then by offset.
pub fn comparable_ordering(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Date(left), Value::Date(right)) => Some(left.cmp(right)),
        (Value::LocalTime(left), Value::LocalTime(right)) => Some(left.cmp(right)),
        (Value::LocalDateTime(left), Value::LocalDateTime(right)) => Some(left.cmp(right)),
        (Value::DateTime(left), Value::DateTime(right)) => Some(datetime_cmp(left, right)),
        _ => None,
    }
}

/// Whether `value`, as the argument of a temporal constructor, reads the
/// statement clock: a map without keys other than `timezone`
/// (`datetime({timezone: '+01:00'})`).
pub fn is_clock_reading_map(value: &Value) -> bool {
    matches!(value, Value::Map(map)
        if map.keys().all(|key| key.eq_ignore_ascii_case("timezone")))
}

/// Whether `<`, `<=`, `>` and `>=` are undefined (NULL) for this pair:
/// openCypher durations have no order because months and days have no
/// fixed length. Other incomparable pairs keep HawDB's `false` result.
pub const fn ordering_is_null(left: &Value, right: &Value) -> bool {
    matches!((left, right), (Value::Duration(_), Value::Duration(_)))
}

/// Orders zoned datetimes by UTC instant, then by offset (east of UTC last),
/// so that equal instants in different offsets remain distinct values.
pub fn datetime_cmp(left: &DateTime<FixedOffset>, right: &DateTime<FixedOffset>) -> Ordering {
    left.naive_utc().cmp(&right.naive_utc()).then_with(|| {
        left.offset()
            .local_minus_utc()
            .cmp(&right.offset().local_minus_utc())
    })
}

/// The tagged-JSON object keys used by bindings and developer JSON:
/// `{"$date": "2026-10-30"}`, `{"$localtime": "12:00:00"}`,
/// `{"$localdatetime": "2026-10-30T12:00:00"}`,
/// `{"$datetime": "2026-10-30T12:00:00+02:00"}` and
/// `{"$duration": "P1M2DT3.5S"}`. Payloads are ISO 8601 strings.
pub const JSON_TAGS: [&str; 5] = [
    "$date",
    "$localtime",
    "$localdatetime",
    "$datetime",
    "$duration",
];

/// The tagged-JSON key of a temporal value; its payload is `value.to_string()`.
pub const fn json_tag(value: &Value) -> Option<&'static str> {
    Some(match value {
        Value::Date(_) => JSON_TAGS[0],
        Value::LocalTime(_) => JSON_TAGS[1],
        Value::LocalDateTime(_) => JSON_TAGS[2],
        Value::DateTime(_) => JSON_TAGS[3],
        Value::Duration(_) => JSON_TAGS[4],
        _ => return None,
    })
}

/// Parses the payload of a tagged-JSON temporal value, or returns `None`
/// when `tag` is not a temporal tag.
pub fn from_json_tag(tag: &str, text: &str) -> Option<TemporalResult<Value>> {
    Some(match tag {
        "$date" => parse_date(text).map(Value::Date),
        "$localtime" => parse_local_time(text).map(Value::LocalTime),
        "$localdatetime" => parse_local_datetime(text).map(Value::LocalDateTime),
        "$datetime" => parse_datetime(text).map(Value::DateTime),
        "$duration" => parse_duration(text).map(Value::duration),
        _ => return None,
    })
}

thread_local! {
    static STATEMENT_CLOCK: Cell<Option<DateTime<Utc>>> = const { Cell::new(None) };
}

/// The instant that clock-reading temporal functions observe: the pinned
/// statement clock, or the system clock when none is pinned.
pub fn statement_now() -> DateTime<Utc> {
    STATEMENT_CLOCK.with(Cell::get).unwrap_or_else(system_now)
}

fn system_now() -> DateTime<Utc> {
    let nanos = crate::time::SystemTime::now()
        .duration_since(crate::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0);
    DateTime::from_timestamp_nanos(nanos)
}

/// Runs `f` with the statement clock pinned to `instant` on this thread.
/// The previous pin is restored afterwards, also on unwind.
pub fn with_statement_clock<R>(instant: DateTime<Utc>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<DateTime<Utc>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            STATEMENT_CLOCK.with(|clock| clock.set(self.0));
        }
    }
    let _restore = Restore(STATEMENT_CLOCK.with(|clock| clock.replace(Some(instant))));
    f()
}

/// Runs `f` with one stable statement clock: an existing pin (for example a
/// test clock) is kept, otherwise the system clock is read once and pinned.
pub fn with_stable_statement_clock<R>(f: impl FnOnce() -> R) -> R {
    if STATEMENT_CLOCK.with(Cell::get).is_some() {
        return f();
    }
    with_statement_clock(system_now(), f)
}

/// The statement clock pinned on this thread, if any. Work handed to another
/// thread re-pins it there with [`with_optional_statement_clock`], so that
/// parallel workers observe the statement's instant too.
pub fn pinned_statement_clock() -> Option<DateTime<Utc>> {
    STATEMENT_CLOCK.with(Cell::get)
}

/// Runs `f` with the statement clock pinned to `instant` when it is `Some`,
/// and with this thread's current clock otherwise.
pub fn with_optional_statement_clock<R>(
    instant: Option<DateTime<Utc>>,
    f: impl FnOnce() -> R,
) -> R {
    match instant {
        Some(instant) => with_statement_clock(instant, f),
        None => f(),
    }
}

#[cfg(test)]
mod tests;
