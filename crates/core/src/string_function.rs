// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Text scalars shared by query frontends. Callers admit each output increment
//! before allocation; regex compilation retains the regex crate's size limit.

use crate::{HawDBError, Result, ValidatedRegex, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StringFunction {
    Trim,
    RegexpReplace,
}

impl StringFunction {
    pub fn from_function_name(name: &str) -> Option<Self> {
        [Self::Trim, Self::RegexpReplace]
            .into_iter()
            .find(|f| name.eq_ignore_ascii_case(f.function_name()))
    }

    pub const fn function_name(self) -> &'static str {
        match self {
            Self::Trim => "trim",
            Self::RegexpReplace => "regexp_replace",
        }
    }

    pub const fn arity(self) -> (usize, usize) {
        match self {
            Self::Trim => (1, 1),
            Self::RegexpReplace => (3, 4),
        }
    }

    /// Replacement supports first-match/default or global (`g`) replacement,
    /// literal text, `\1`..`\9`, `\&` (whole match) and `\\`.
    /// Unsupported flags/escapes fail explicitly. NULL arguments propagate.
    pub fn apply(self, values: &[&Value], admit: impl FnMut(usize) -> Result<()>) -> Result<Value> {
        self.apply_prepared(values, None, admit)
    }

    /// Evaluate using a validated, execution-owned invariant pattern when bound.
    #[doc(hidden)]
    pub fn apply_prepared(
        self,
        values: &[&Value],
        pattern: Option<&ValidatedRegex>,
        mut admit: impl FnMut(usize) -> Result<()>,
    ) -> Result<Value> {
        let (min, max) = self.arity();
        if !(min..=max).contains(&values.len()) {
            return Err(HawDBError::Execution(format!(
                "{}() expects {min}..={max} arguments",
                self.function_name()
            )));
        }
        if values.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }
        let values = values
            .iter()
            .map(|value| match value {
                Value::String(text) => Ok(text.as_str()),
                _ => Err(HawDBError::Execution(format!(
                    "{}() requires string arguments",
                    self.function_name()
                ))),
            })
            .collect::<Result<Vec<_>>>()?;
        if self == Self::Trim {
            let value = values[0].trim();
            admit(value.len())?;
            return Ok(Value::String(value.to_owned()));
        }
        let global = match values.get(3).copied().unwrap_or("") {
            "" => false,
            "g" => true,
            _ => {
                return Err(HawDBError::Execution(
                    "regexp_replace() supports only the 'g' flag".into(),
                ))
            }
        };
        let mut escapes = values[2].chars();
        while let Some(ch) = escapes.next() {
            if ch == '\\' && !matches!(escapes.next(), Some('\\' | '&' | '1'..='9')) {
                return Err(HawDBError::Execution(
                    "regexp_replace() unsupported replacement escape".into(),
                ));
            }
        }
        let dynamic;
        let pattern = match pattern {
            Some(pattern) if pattern.as_str() == values[1] => pattern,
            Some(_) => {
                return Err(HawDBError::Execution(
                    "regexp_replace() prepared pattern does not match its argument".into(),
                ))
            }
            None => {
                dynamic = ValidatedRegex::new(values[1]).map_err(|error| match error {
                    HawDBError::Semantic(message) => HawDBError::Execution(message),
                    error => error,
                })?;
                &dynamic
            }
        };
        pattern.with_compiled(|regex| {
            // Admit each measured piece before allocating the final buffer.
            // Early refusal also bounds replacement-expansion work. One exact
            // allocation avoids both spare capacity and repeated prefix copies.
            let mut bytes = 0usize;
            replacement_pieces(regex, values[0], values[2], global, |piece| {
                admit(piece.len())?;
                bytes = bytes.checked_add(piece.len()).ok_or_else(|| {
                    HawDBError::Execution("regexp_replace() output size overflow".into())
                })?;
                Ok(())
            })?;
            let mut output = String::new();
            output.try_reserve_exact(bytes).map_err(|_| {
                HawDBError::Execution("regexp_replace() output allocation failed".into())
            })?;
            replacement_pieces(regex, values[0], values[2], global, |piece| {
                output.push_str(piece);
                Ok(())
            })?;
            Ok(Value::String(output))
        })
    }
}

/// Visit borrowed output slices without allocating per match. Replacement
/// escapes were validated once before either the sizing or emission pass.
fn replacement_pieces(
    regex: &regex::Regex,
    input: &str,
    replacement: &str,
    global: bool,
    mut append: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let mut cursor = 0;
    for captures in regex.captures_iter(input) {
        let found = captures.get(0).expect("regex capture zero always exists");
        append(&input[cursor..found.start()])?;
        let mut escaped = replacement.char_indices();
        let mut literal_start = 0;
        while let Some((index, ch)) = escaped.next() {
            if ch != '\\' {
                continue;
            }
            append(&replacement[literal_start..index])?;
            let (offset, escape) = escaped.next().expect("replacement escapes were validated");
            match escape {
                '\\' => append("\\")?,
                '&' => append(found.as_str())?,
                '1'..='9' => {
                    if let Some(group) = captures.get((escape as u8 - b'0') as usize) {
                        append(group.as_str())?;
                    }
                }
                _ => unreachable!("replacement escapes were validated"),
            }
            literal_start = offset + escape.len_utf8();
        }
        append(&replacement[literal_start..])?;
        cursor = found.end();
        if !global {
            break;
        }
    }
    append(&input[cursor..])
}

impl crate::RetainedHeapBytes for StringFunction {
    fn retained_heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replace(input: &str, pattern: &str, replacement: &str, flags: &str) -> Result<Value> {
        let values = [input, pattern, replacement, flags].map(|v| Value::String(v.into()));
        StringFunction::RegexpReplace.apply(&values.iter().collect::<Vec<_>>(), |_| Ok(()))
    }

    #[test]
    fn regexp_replace_keeps_unicode_captures_literal_dollars_and_empty_matches() {
        assert_eq!(
            replace("α-42 β-7", r"(\p{L})-(\d+)", r"\2:\1:$1:\&:\\", "g").unwrap(),
            Value::String("42:α:$1:α-42:\\ 7:β:$1:β-7:\\".into())
        );
        assert_eq!(
            replace("αβ", "", "-", "g").unwrap(),
            Value::String("-α-β-".into())
        );
        assert_eq!(
            replace("a a", "a", "b", "").unwrap(),
            Value::String("b a".into())
        );
    }

    #[test]
    fn invalid_replacement_is_rejected_even_without_a_match() {
        assert!(replace("unmatched", "x", r"\q", "g").is_err());
        assert!(replace("unmatched", "x", "\\", "g").is_err());
    }

    #[test]
    fn output_is_admitted_before_each_allocation_and_refusal_is_terminal() {
        let values = [
            Value::String("aa".into()),
            Value::String("a".into()),
            Value::String("expanded".into()),
            Value::String("g".into()),
        ];
        let mut admitted = 0;
        let result =
            StringFunction::RegexpReplace.apply(&values.iter().collect::<Vec<_>>(), |bytes| {
                admitted += bytes;
                if admitted > 8 {
                    Err(HawDBError::Execution("test output budget exhausted".into()))
                } else {
                    Ok(())
                }
            });
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("test output budget exhausted"));
        assert_eq!(admitted, 16);
    }
}

#[cfg(test)]
mod prepared_tests {
    use super::*;

    #[test]
    fn invariant_regex_is_compiled_once_and_execution_does_not_mutate_cache_owner() {
        use crate::RetainedHeapBytes;
        let before = crate::regex::compilation_count();
        let mut cached = ValidatedRegex::new(" ").unwrap();
        assert_eq!(crate::regex::compilation_count() - before, 1);
        cached.retain_source_only();
        let cached_bytes = cached.retained_heap_bytes();
        let mut execution = cached.clone();
        execution.prepare_for_execution().unwrap();
        let prepared = crate::regex::compilation_count();
        for (input, expected) in [("A B", "A_B"), ("α β", "α_β"), ("none", "none")] {
            let values = [input, " ", "_", "g"].map(|value| Value::String(value.into()));
            let result = StringFunction::RegexpReplace
                .apply_prepared(&values.iter().collect::<Vec<_>>(), Some(&execution), |_| {
                    Ok(())
                })
                .unwrap();
            assert_eq!(result, Value::String(expected.into()));
        }
        assert_eq!(crate::regex::compilation_count(), prepared);
        assert_eq!(cached.retained_heap_bytes(), cached_bytes);
        assert!(cached_bytes.is_some());
        assert!(execution.retained_heap_bytes().is_none());
    }

    #[test]
    fn many_matches_admit_all_output_before_materialization() {
        let values = ["aaaa", "a", "1234", "g"].map(|value| Value::String(value.into()));
        let mut admissions = Vec::new();
        let output = StringFunction::RegexpReplace
            .apply(&values.iter().collect::<Vec<_>>(), |bytes| {
                admissions.push(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(output, Value::String("1234123412341234".into()));
        assert_eq!(admissions.iter().sum::<usize>(), 16);
    }
}
