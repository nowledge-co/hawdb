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

use super::RelationalOverflowExtentInput;
use crate::background::CheckpointSharedValues;

/// Ordinary caller-owned inputs or a sorted, admitted private checkpoint list.
/// Checkpoint clones share its immutable capacity; no Vec conversion detaches it.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct RelationalOverflowInputs(Storage);

#[derive(Debug, Clone)]
enum Storage {
    Ordinary(Vec<RelationalOverflowExtentInput>),
    Checkpoint(CheckpointSharedValues<RelationalOverflowExtentInput>),
}

impl RelationalOverflowInputs {
    pub(in crate::relational) fn checkpoint(
        values: CheckpointSharedValues<RelationalOverflowExtentInput>,
    ) -> Self {
        Self(Storage::Checkpoint(values))
    }

    pub(super) fn ordinary_mut(&mut self) -> Option<&mut Vec<RelationalOverflowExtentInput>> {
        match &mut self.0 {
            Storage::Ordinary(values) => Some(values),
            Storage::Checkpoint(_) => None,
        }
    }
}

impl From<Vec<RelationalOverflowExtentInput>> for RelationalOverflowInputs {
    fn from(values: Vec<RelationalOverflowExtentInput>) -> Self {
        Self(Storage::Ordinary(values))
    }
}

impl std::ops::Deref for RelationalOverflowInputs {
    type Target = [RelationalOverflowExtentInput];

    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Storage::Ordinary(values) => values,
            Storage::Checkpoint(values) => values,
        }
    }
}

impl PartialEq for RelationalOverflowInputs {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for RelationalOverflowInputs {}

impl PartialEq<Vec<RelationalOverflowExtentInput>> for RelationalOverflowInputs {
    fn eq(&self, other: &Vec<RelationalOverflowExtentInput>) -> bool {
        &**self == other.as_slice()
    }
}

impl<'a> IntoIterator for &'a RelationalOverflowInputs {
    type Item = &'a RelationalOverflowExtentInput;
    type IntoIter = std::slice::Iter<'a, RelationalOverflowExtentInput>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[doc(hidden)]
pub struct RelationalOverflowInputsIter(Iteration);

enum Iteration {
    Ordinary(std::vec::IntoIter<RelationalOverflowExtentInput>),
    Checkpoint {
        values: CheckpointSharedValues<RelationalOverflowExtentInput>,
        index: usize,
    },
}

impl IntoIterator for RelationalOverflowInputs {
    type Item = RelationalOverflowExtentInput;
    type IntoIter = RelationalOverflowInputsIter;

    fn into_iter(self) -> Self::IntoIter {
        RelationalOverflowInputsIter(match self.0 {
            Storage::Ordinary(values) => Iteration::Ordinary(values.into_iter()),
            Storage::Checkpoint(values) => Iteration::Checkpoint { values, index: 0 },
        })
    }
}

impl Iterator for RelationalOverflowInputsIter {
    type Item = RelationalOverflowExtentInput;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Iteration::Ordinary(values) => values.next(),
            Iteration::Checkpoint { values, index } => {
                // This concrete enum contains fixed references and shared byte
                // owners; cloning an item never allocates an element payload.
                let output = values.get(*index)?.clone();
                *index += 1;
                Some(output)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = match &self.0 {
            Iteration::Ordinary(values) => values.len(),
            Iteration::Checkpoint { values, index } => values.len() - index,
        };
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RelationalOverflowInputsIter {}
impl std::iter::FusedIterator for RelationalOverflowInputsIter {}
