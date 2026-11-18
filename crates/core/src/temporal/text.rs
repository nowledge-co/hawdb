// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Legacy metadata text contracts, separate from Cypher's ISO constructors.

use std::borrow::Cow;

use ::time::format_description::well_known::Rfc3339;

use super::types::{date_from_ymd, epoch_millis, time_from_hms_nano};
use super::OffsetDateTime;

pub fn parse_rfc3339_millis(text: &str) -> Option<i64> {
    let (text, leap) = normalize_leap_second(text);
    OffsetDateTime::parse(&text, &Rfc3339)
        .ok()
        .map(|value| epoch_millis(value) + if leap { 1_000 } else { 0 })
}

pub fn parse_metadata_millis(text: &str) -> Option<i64> {
    let text = text.trim();
    if let Some(millis) = parse_rfc3339_millis(text) {
        return Some(millis);
    }
    // Legacy strftime parsing skips whitespace before numeric fields, permits
    // unpadded years, and permits zero whitespace between the date and clock.
    // Keep that grammar separate from the strict Cypher/SQL constructors.
    let mut rest = text;
    let sign = if let Some(tail) = rest.strip_prefix('-') {
        rest = tail;
        -1
    } else if let Some(tail) = rest.strip_prefix('+') {
        rest = tail;
        1
    } else {
        0
    };
    let year = digits(&mut rest, if sign == 0 { 4 } else { usize::MAX }, false)?;
    let year = i32::try_from(year).ok()? * if sign == -1 { -1 } else { 1 };
    rest = rest.strip_prefix('-')?;
    let month = digits(&mut rest, 2, true)?;
    rest = rest.strip_prefix('-')?;
    let day = digits(&mut rest, 2, true)?;
    let date = date_from_ymd(year, month, day)?;
    if rest.is_empty() {
        return Some(epoch_millis(date.midnight().assume_utc()));
    }
    if let Some(tail) = rest.strip_prefix('T') {
        rest = tail;
    }
    let hour = digits(&mut rest, 2, true)?;
    rest = rest.strip_prefix(':')?;
    let minute = digits(&mut rest, 2, true)?;
    rest = rest.strip_prefix(':')?;
    let second = digits(&mut rest, 2, true)?;
    let nanos = if let Some(tail) = rest.strip_prefix('.') {
        rest = tail;
        let length = rest.bytes().take_while(u8::is_ascii_digit).count();
        if length == 0 {
            return None;
        }
        let retained = length.min(9);
        let fraction = rest[..retained].parse::<u32>().ok()?;
        rest = &rest[length..];
        fraction * 10_u32.pow(9 - retained as u32)
    } else {
        0
    };
    if !rest.is_empty() || second > 60 {
        return None;
    }
    let time = time_from_hms_nano(hour, minute, second.min(59), nanos)?;
    Some(epoch_millis(date.with_time(time).assume_utc()) + if second == 60 { 1_000 } else { 0 })
}

fn digits(rest: &mut &str, width: usize, skip_space: bool) -> Option<u32> {
    if skip_space {
        *rest = rest.trim_start();
    }
    let length = rest
        .bytes()
        .take(width)
        .take_while(u8::is_ascii_digit)
        .count();
    if length == 0 {
        return None;
    }
    let value = rest[..length].parse().ok()?;
    *rest = &rest[length..];
    Some(value)
}

// Chrono's metadata parser counted :60 as the following second, while time's
// RFC 3339 parser clamps it to :59.999999999. Normalize before parsing so the
// migration preserves the old integer milliseconds, including the fraction.
fn normalize_leap_second(text: &str) -> (Cow<'_, str>, bool) {
    let Some(clock) = text.find(['T', 't', ' ']) else {
        return (Cow::Borrowed(text), false);
    };
    let start = clock + 1;
    let Some(first) = text[start..].find(':').map(|i| start + i) else {
        return (Cow::Borrowed(text), false);
    };
    let Some(second) = text[first + 1..].find(':').map(|i| first + 1 + i) else {
        return (Cow::Borrowed(text), false);
    };
    if text.get(second + 1..second + 3) != Some("60") {
        return (Cow::Borrowed(text), false);
    }
    let mut normalized = text.to_owned();
    normalized.replace_range(second + 1..second + 3, "59");
    (Cow::Owned(normalized), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_keeps_pre_epoch_flooring_whitespace_and_leap_seconds() {
        for (text, expected) in [
            ("1969-12-31T23:59:59.999999999Z", -1),
            ("1969-12-31 23:59:59.999999999", -1),
            ("2026-11-05  12:34:56", 1_793_882_096_000),
            ("2026-11-0512:34:56", 1_793_882_096_000),
            ("2026- 1- 5 2: 3: 4", 1_767_578_584_000),
            ("2026-11-05T 12:34:56", 1_793_882_096_000),
            ("1-1-1", -62_135_596_800_000),
            ("2016-12-31T23:59:60Z", 1_483_228_800_000),
            ("2016-12-31T23:59:60.1Z", 1_483_228_800_100),
            ("2016-12-31 23:59:60.5", 1_483_228_800_500),
        ] {
            assert_eq!(parse_metadata_millis(text), Some(expected), "{text}");
        }
        assert_eq!(
            parse_rfc3339_millis("2016-12-31T23:59:60.1Z"),
            Some(1_483_228_800_100)
        );
        assert_eq!(parse_metadata_millis("2026-02-30"), None);
    }
}
