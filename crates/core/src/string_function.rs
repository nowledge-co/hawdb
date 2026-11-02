// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Text scalars shared by query frontends. Callers admit each output increment
//! before allocation; regex compilation retains the regex crate's size limit.

use crate::{HawDBError, Result, Value};

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
    pub fn apply(
        self,
        values: &[&Value],
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
        let regex = regex::Regex::new(values[1])
            .map_err(|error| HawDBError::Execution(format!("invalid regex pattern: {error}")))?;
        let mut output = String::new();
        let mut append = |text: &str| -> Result<()> {
            admit(text.len())?;
            output.try_reserve_exact(text.len()).map_err(|_| {
                HawDBError::Execution("regexp_replace() output allocation failed".into())
            })?;
            output.push_str(text);
            Ok(())
        };
        let mut cursor = 0;
        for captures in regex.captures_iter(values[0]) {
            let found = captures.get(0).expect("regex capture zero always exists");
            append(&values[0][cursor..found.start()])?;
            let mut replacement = values[2].char_indices();
            let mut literal_start = 0;
            while let Some((index, ch)) = replacement.next() {
                if ch != '\\' {
                    continue;
                }
                append(&values[2][literal_start..index])?;
                let Some((offset, escape)) = replacement.next() else {
                    return Err(HawDBError::Execution(
                        "regexp_replace() trailing replacement escape".into(),
                    ));
                };
                match escape {
                    '\\' => append("\\")?,
                    '&' => append(found.as_str())?,
                    '1'..='9' => {
                        if let Some(group) = captures.get((escape as u8 - b'0') as usize) {
                            append(group.as_str())?;
                        }
                    }
                    _ => {
                        return Err(HawDBError::Execution(
                            "regexp_replace() unsupported replacement escape".into(),
                        ))
                    }
                }
                literal_start = offset + escape.len_utf8();
            }
            append(&values[2][literal_start..])?;
            cursor = found.end();
            if !global {
                break;
            }
        }
        append(&values[0][cursor..])?;
        Ok(Value::String(output))
    }
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
