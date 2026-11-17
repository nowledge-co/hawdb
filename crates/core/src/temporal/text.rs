// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Legacy metadata text contracts, separate from Cypher's ISO constructors.

use std::borrow::Cow;

use ::time::format_description::well_known::Rfc3339;
use ::time::macros::format_description;

use super::types::epoch_millis;
use super::{Date, OffsetDateTime, PlainDateTime};

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
    let text = if text.chars().any(|c| c.is_whitespace() && c != ' ')
        || text.as_bytes().windows(2).any(|pair| pair == b"  ")
    {
        Cow::Owned(text.split_whitespace().collect::<Vec<_>>().join(" "))
    } else {
        Cow::Borrowed(text)
    };
    let (text, leap) = normalize_leap_second(&text);
    for format in [
        format_description!("[year]-[month padding:none]-[day padding:none] [hour padding:none]:[minute padding:none]:[second padding:none].[subsecond]"),
        format_description!("[year]-[month padding:none]-[day padding:none]T[hour padding:none]:[minute padding:none]:[second padding:none].[subsecond]"),
        format_description!("[year]-[month padding:none]-[day padding:none] [hour padding:none]:[minute padding:none]:[second padding:none]"),
        format_description!("[year]-[month padding:none]-[day padding:none]T[hour padding:none]:[minute padding:none]:[second padding:none]"),
    ] {
        if let Ok(value) = PlainDateTime::parse(&text, format) {
            return Some(epoch_millis(value.assume_utc()) + if leap { 1_000 } else { 0 });
        }
    }
    Date::parse(
        &text,
        format_description!("[year]-[month padding:none]-[day padding:none]"),
    )
    .ok()
    .map(|date| epoch_millis(date.midnight().assume_utc()))
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
