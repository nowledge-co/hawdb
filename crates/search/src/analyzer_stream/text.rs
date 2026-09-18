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

use super::control::Control;
use super::*;
use crate::analyzer_workspace::bounds::growing_bytes;
use crate::build_memory::{checked_add, checked_mul};
use crate::build_term::Term;
use crate::HawDBError;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub(super) enum Text<'a> {
    Borrowed(&'a str),
    Owned(Term),
}

impl<'a> Text<'a> {
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(text) => text,
            Self::Owned(text) => text.as_str(),
        }
    }

    pub(super) fn materialize(&self, control: Control<'_>) -> Result<Term> {
        match self {
            Self::Borrowed(text) => control.copy(text),
            Self::Owned(text) => Ok(text.clone()),
        }
    }

    pub(super) fn into_untracked(self) -> Result<String> {
        match self {
            Self::Borrowed(text) => Ok(text.to_owned()),
            Self::Owned(text) => text.into_untracked(),
        }
    }

    pub(super) fn lowercase(text: &'a str, scalar: bool, control: Control<'_>) -> Result<Self> {
        if lowercase_is_identity(text) {
            return Ok(Self::Borrowed(text));
        }
        // Pinned Unicode lowercase output uses at most three output bytes per
        // source byte. The shared envelope includes growth and replacement.
        let capacity = growing_bytes(checked_mul(text.len(), 3)?, 1)
            .ok_or_else(|| HawDBError::Execution("search lowercase capacity overflow".into()))?;
        control
            .build(capacity, || {
                if scalar {
                    normalize_part(text)
                } else {
                    text.to_lowercase()
                }
            })
            .map(Self::Owned)
    }

    pub(super) fn join(
        left: &str,
        middle: &str,
        right: &str,
        control: Control<'_>,
    ) -> Result<Self> {
        let bytes = checked_add(checked_add(left.len(), middle.len())?, right.len())?;
        control
            .build(bytes, || {
                let mut text = String::with_capacity(bytes);
                text.push_str(left);
                text.push_str(middle);
                text.push_str(right);
                text
            })
            .map(Self::Owned)
    }

    pub(super) fn suffix(&self, prefix: usize, tail: &str, control: Control<'_>) -> Result<Self> {
        if !tail.is_empty() {
            return Self::join(&self.as_str()[..prefix], tail, "", control);
        }
        match self {
            Self::Borrowed(text) => Ok(Self::Borrowed(&text[..prefix])),
            Self::Owned(text) => control.copy(&text[..prefix]).map(Self::Owned),
        }
    }
}

impl Borrow<str> for Text<'_> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for Text<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Text<'_> {}

impl Hash for Text<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}
