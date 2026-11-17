// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Checked construction and epoch conversions shared by temporal consumers.

use super::{Date, Month, OffsetDateTime, PlainDateTime, SignedDuration, Time, UtcOffset, Weekday};

pub(crate) fn date_from_ymd(year: i32, month: u32, day: u32) -> Option<Date> {
    Date::from_calendar_date(
        year,
        Month::try_from(u8::try_from(month).ok()?).ok()?,
        u8::try_from(day).ok()?,
    )
    .ok()
}

pub(crate) fn date_from_ordinal(year: i32, ordinal: u32) -> Option<Date> {
    Date::from_ordinal_date(year, u16::try_from(ordinal).ok()?).ok()
}

pub(crate) fn date_from_iso_week(year: i32, week: u32, weekday: Weekday) -> Option<Date> {
    Date::from_iso_week_date(year, u8::try_from(week).ok()?, weekday).ok()
}

pub(crate) fn time_from_hms_nano(hour: u32, minute: u32, second: u32, nanos: u32) -> Option<Time> {
    Time::from_hms_nano(
        u8::try_from(hour).ok()?,
        u8::try_from(minute).ok()?,
        u8::try_from(second).ok()?,
        nanos,
    )
    .ok()
}

pub(crate) fn local_datetime(value: OffsetDateTime) -> PlainDateTime {
    PlainDateTime::new(value.date(), value.time())
}

pub(crate) fn utc_datetime(value: OffsetDateTime) -> PlainDateTime {
    local_datetime(value.to_offset(UtcOffset::UTC))
}

pub(crate) fn epoch_millis(value: OffsetDateTime) -> i64 {
    // All supported civil dates fit in i64 milliseconds, including large-dates.
    (value.unix_timestamp_nanos().div_euclid(1_000_000)) as i64
}

pub(crate) fn from_epoch_nanos(nanos: i128) -> Option<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

pub(crate) fn duration_days(days: i64) -> Option<SignedDuration> {
    days.checked_mul(86_400).map(SignedDuration::seconds)
}

pub(crate) fn offset_from_seconds(seconds: i32) -> Option<UtcOffset> {
    UtcOffset::from_whole_seconds(seconds).ok()
}

pub(crate) fn datetime_from_timestamp(seconds: i64, nanos: u32) -> Option<OffsetDateTime> {
    if nanos >= 1_000_000_000 {
        return None;
    }
    from_epoch_nanos(i128::from(seconds) * 1_000_000_000 + i128::from(nanos))
}

pub(crate) fn datetime_from_millis(millis: i64) -> Option<OffsetDateTime> {
    from_epoch_nanos(i128::from(millis) * 1_000_000)
}

pub(crate) fn datetime_from_nanos(nanos: i64) -> OffsetDateTime {
    from_epoch_nanos(i128::from(nanos)).expect("i64 nanoseconds fit the datetime range")
}
