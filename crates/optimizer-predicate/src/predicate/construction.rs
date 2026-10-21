// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Pre-allocation envelopes for the shared metadata parser. These assumptions
//! follow the pinned Rust Vec growth and serde_json 1.0.154 slice deserializer:
//! Strings copy decoded bytes, the sequence Vec grows geometrically, and one
//! reusable byte Vec decodes escaped strings. Recheck when either pin changes.

use super::{SearchPredicateParseError, IN_SUFFIX, NOT_IN_SUFFIX};

#[derive(Debug, PartialEq, Eq)]
pub enum SearchPredicateConstructionError<E> {
    Parse(SearchPredicateParseError),
    Admission(E),
    SizeOverflow,
}

impl<E> From<SearchPredicateParseError> for SearchPredicateConstructionError<E> {
    fn from(error: SearchPredicateParseError) -> Self {
        Self::Parse(error)
    }
}

// A scalar-set node holds at most eleven scalar values and twelve child
// pointers. Per input value this also covers internal nodes and split overlap.
// This is a B-tree capacity allowance, not a measured allocator size.
const SET_ENTRY_BYTES: usize = 1024;
const MAX_REASON_BYTES: usize = 64;
// Boxed Serde syntax/type errors and their bounded numeric/type descriptions.
// Non-array top-level input is rejected without Serde diagnostic formatting.
const JSON_ERROR_BYTES: usize = 1024;

pub(super) fn filter_capacity(key: &str, value: &str) -> Option<usize> {
    let diagnostic = key
        .len()
        .checked_add(MAX_REASON_BYTES + 2)?
        .checked_mul(3)?;
    // One canonical field, the parse-error key, and Display growth/overlap.
    let common = key
        .len()
        .max(16)
        .checked_add(key.len())?
        .checked_add(diagnostic)?;
    if !key.ends_with(IN_SUFFIX) && !key.ends_with(NOT_IN_SUFFIX) {
        // Owned scalar plus a normalized bool while its source is still live.
        return common.checked_add(value.len())?.checked_add(5);
    }
    let strings = StringCapacity::scan(value)?;
    // Serde's Vec<String> can have capacity < 2*n (minimum four). Include
    // the old buffer during replacement, hence three rather than two.
    let vector = strings
        .count
        .max(4)
        .checked_mul(3)?
        .checked_mul(size_of::<String>())?;
    // Unicode decoding may reserve four bytes for its final code point. The
    // scratch Vec minimum is eight bytes; geometric replacement overlaps its
    // predecessor. Source whitespace never contributes to this capacity.
    let scratch = strings.largest.checked_add(4)?.max(8).checked_mul(3)?;
    let sets = strings.count.checked_mul(SET_ENTRY_BYTES)?;
    let bools = strings.count.checked_mul(5)?;
    common
        .checked_add(vector)?
        .checked_add(scratch)?
        .checked_add(sets)?
        .checked_add(strings.total)?
        .checked_add(bools)?
        .checked_add(JSON_ERROR_BYTES)
}

#[derive(Default)]
struct StringCapacity {
    count: usize,
    total: usize,
    largest: usize,
}

impl StringCapacity {
    // This is only a byte-capacity scan, not another JSON grammar. Serde remains
    // authoritative for escapes, surrogate pairs, syntax and element types.
    // In malformed input, count unfinished and nested strings conservatively.
    fn scan(input: &str) -> Option<Self> {
        let mut result = Self::default();
        let mut start = None;
        let mut escaped = false;
        for (offset, byte) in input.bytes().enumerate() {
            if let Some(begin) = start {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    result.include(offset - begin)?;
                    start = None;
                }
            } else if byte == b'"' {
                result.count = result.count.checked_add(1)?;
                start = Some(offset + 1);
            }
        }
        if let Some(begin) = start {
            result.include(input.len() - begin)?;
        }
        Some(result)
    }

    fn include(&mut self, bytes: usize) -> Option<()> {
        self.total = self.total.checked_add(bytes)?;
        self.largest = self.largest.max(bytes);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SearchPredicate, SearchPredicateOp, SearchPredicateSet};
    use std::collections::BTreeMap;

    #[test]
    fn metadata_admission_refusal_precedes_invalid_json_and_normalization() {
        let filters = BTreeMap::from([("kind__in".into(), "not json".into())]);
        let mut calls = 0;
        let refused = SearchPredicateSet::from_metadata_filters_with_admission(&filters, |_| {
            calls += 1;
            if calls == 2 {
                Err("shared budget")
            } else {
                Ok(())
            }
        });
        assert_eq!(calls, 2);
        assert_eq!(
            refused,
            Err(SearchPredicateConstructionError::Admission("shared budget"))
        );
        let parsed =
            SearchPredicateSet::from_metadata_filters_with_admission(&filters, |_| Ok::<_, ()>(()));
        assert!(
            matches!(parsed, Err(SearchPredicateConstructionError::Parse(error)) if error.reason() == "expected JSON string array")
        );
    }

    #[test]
    fn metadata_capacity_counts_empty_values_and_excludes_json_whitespace() {
        let plain = filter_capacity("labels__in", r#"["value"]"#).unwrap();
        let spaced = filter_capacity(
            "labels__in",
            &format!("[{}\"value\"{}]", " ".repeat(4096), "\t".repeat(4096)),
        )
        .unwrap();
        assert_eq!(plain, spaced);
        let duplicate =
            filter_capacity("labels__in", &format!("[{}]", vec![r#""""#; 100].join(","))).unwrap();
        assert!(duplicate >= 100 * SET_ENTRY_BYTES);
        let scanned = StringCapacity::scan(r#"["a\"b","\\","\uD83D\uDE80"]"#).unwrap();
        assert_eq!(scanned.count, 3);
        assert_eq!(scanned.largest, 12);
        assert_eq!(scanned.total, 18);
    }

    #[test]
    fn enum_normalization_reuses_owned_storage_and_preserves_unicode_trimming() {
        for value in ["\u{2003}Memory\u{2003}", " \t\u{2003}", "Mixed CASE", ""] {
            let owned = value.to_owned();
            let pointer = owned.as_ptr();
            let expected = value.trim().to_ascii_lowercase();
            let predicate = SearchPredicate::eq("kind", owned);
            let SearchPredicateOp::Eq(super::super::SearchScalarValue::Enum(actual)) =
                predicate.op()
            else {
                panic!("kind must be an enum");
            };
            assert_eq!(actual, &expected);
            assert_eq!(actual.as_ptr(), pointer);
        }
    }
}
