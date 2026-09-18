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

use super::*;
use serde::de::{DeserializeSeed, Error, SeqAccess, Visitor};
use serde::Deserializer;
use std::fmt;

#[cfg(test)]
pub(super) fn visit(
    document: &SearchDocument,
    field: &str,
    visitor: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let task = RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task)?;
    visit_with_context(document, field, &memory, &task, visitor)
}

pub(super) fn visit_with_context(
    document: &SearchDocument,
    field: &str,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
    visitor: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    checkpoint(task)?;
    if field != "labels" && !field.starts_with("metadata.") {
        if let Some(value) = search_document_field_value(document, field) {
            visitor(value)?;
        }
        return Ok(());
    }
    let Some(value) = document.metadata.get(field) else {
        return Ok(());
    };
    // A valid array must start here. Avoid deserializing a top-level String
    // only to allocate a type-error message containing that entire string.
    if !value
        .trim_start_matches([' ', '\t', '\n', '\r'])
        .starts_with('[')
    {
        return csv_values(value, task, visitor);
    }
    let _scratch = memory
        .retained
        .reserve(checked_add(json_scratch_bytes(value, task)?, 512)?)?;
    // Validate the complete array before emitting anything. Cancellation must
    // propagate separately from syntax errors that select whole-value CSV.
    let mut stopped = None;
    let valid = json_strings(value, &mut |_| {
        #[cfg(test)]
        evidence::validated_value();
        match checkpoint(task) {
            Ok(()) => true,
            Err(error) => {
                stopped = Some(error);
                false
            }
        }
    })
    .is_ok();
    if let Some(error) = stopped {
        return Err(error);
    }
    if !valid {
        return csv_values(value, task, visitor);
    }
    let mut failure = None;
    let result = json_strings(value, &mut |part| {
        let result = checkpoint(task).and_then(|()| {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                Ok(())
            } else {
                visitor(trimmed)
            }
        });
        match result {
            Ok(()) => true,
            Err(error) => {
                failure = Some(error);
                false
            }
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    result.map_err(|error| {
        HawDBError::Storage(format!("search descriptor label traversal failed: {error}"))
    })
}

fn csv_values(
    value: &str,
    task: &RuntimeTaskContext,
    visitor: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for part in value.split(',') {
        checkpoint(task)?;
        let part = part.trim();
        if !part.is_empty() {
            visitor(part)?;
        }
    }
    Ok(())
}

fn json_scratch_bytes(value: &str, task: &RuntimeTaskContext) -> Result<usize> {
    let mut start = None;
    let mut escape = false;
    let mut copied = false;
    let mut longest = 0;
    for (index, byte) in value.bytes().enumerate() {
        if index % 8192 == 0 {
            checkpoint(task)?;
        }
        match start {
            None if byte == b'"' => {
                start = Some(index + 1);
                copied = false;
            }
            None => {}
            Some(_) if escape => {
                escape = false;
            }
            Some(_) if byte == b'\\' => {
                escape = true;
                copied = true;
            }
            Some(begin) if byte == b'"' => {
                if copied {
                    longest = longest.max(index - begin);
                }
                start = None;
            }
            Some(_) => {}
        }
    }
    if let Some(begin) = start
        && copied
    {
        longest = longest.max(value.len() - begin);
    }
    // serde_json's slice reader borrows unescaped strings and reuses one Vec
    // for escaped values. Decoding cannot exceed the raw token byte length;
    // 3x covers the overlapping old/replacement capacities, including growth
    // while reporting a malformed or unfinished escaped value.
    if longest == 0 {
        Ok(0)
    } else {
        checked_mul(longest.max(8), 3)
    }
}

fn json_strings(value: &str, visitor: &mut impl FnMut(&str) -> bool) -> serde_json::Result<()> {
    struct Strings<'a, F>(&'a mut F);
    struct StringValue<'a, F>(&'a mut F);

    impl<'de, F: FnMut(&str) -> bool> Visitor<'de> for Strings<'_, F> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an array of strings")
        }

        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<(), A::Error> {
            while sequence.next_element_seed(StringValue(self.0))?.is_some() {}
            Ok(())
        }
    }

    impl<'de, F: FnMut(&str) -> bool> DeserializeSeed<'de> for StringValue<'_, F> {
        type Value = ();

        fn deserialize<D: Deserializer<'de>>(
            self,
            deserializer: D,
        ) -> std::result::Result<(), D::Error> {
            deserializer.deserialize_str(self)
        }
    }

    impl<'de, F: FnMut(&str) -> bool> Visitor<'de> for StringValue<'_, F> {
        type Value = ();

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a string")
        }

        fn visit_str<E: Error>(self, value: &str) -> std::result::Result<(), E> {
            if (self.0)(value) {
                Ok(())
            } else {
                Err(E::custom("search descriptor label visitor stopped"))
            }
        }
    }

    let mut deserializer = serde_json::Deserializer::from_str(value);
    deserializer.deserialize_seq(Strings(visitor))?;
    deserializer.end()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod evidence {
    use hawdb_core::RuntimeCancellationToken;
    use std::cell::RefCell;
    thread_local! { static CANCEL: RefCell<Option<(usize, RuntimeCancellationToken)>> = const { RefCell::new(None) }; }
    pub(super) fn validated_value() {
        CANCEL.with_borrow_mut(|pending| {
            if let Some((remaining, token)) = pending {
                if *remaining == 0 {
                    token.cancel();
                    *pending = None;
                } else {
                    *remaining -= 1;
                }
            }
        });
    }
    pub(super) fn cancel_after(values: usize, token: RuntimeCancellationToken) {
        CANCEL.with_borrow_mut(|pending| *pending = Some((values, token)));
    }
}
