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

//! Immutable overlays lend delta records and retain canonical allocation owners.

use super::*;
use crate::background::CheckpointWorkContext;
use crate::canonical::{CheckpointCanonicalIterator, CheckpointCanonicalRecord, CheckpointRecord};

#[derive(Debug)]
pub(crate) enum CheckpointRecordRef<'a, R> {
    Borrowed(&'a R),
    Owned(CheckpointRecord<R>),
}
impl<R> std::ops::Deref for CheckpointRecordRef<'_, R> {
    type Target = R;
    fn deref(&self) -> &R {
        match self {
            Self::Borrowed(record) => record,
            Self::Owned(record) => record,
        }
    }
}
impl<R> std::borrow::Borrow<R> for CheckpointRecordRef<'_, R> {
    fn borrow(&self) -> &R {
        self
    }
}
impl<R: PartialEq> PartialEq<R> for CheckpointRecordRef<'_, R> {
    fn eq(&self, other: &R) -> bool {
        **self == *other
    }
}

pub(crate) struct CheckpointOverlayIterator<
    'a,
    R: OverlayRecord + CheckpointCanonicalRecord + 'a,
    D: Iterator<Item = &'a R>,
> {
    base: Option<Peekable<CheckpointCanonicalIterator<'a, R>>>,
    delta: Peekable<D>,
    tombstones: &'a BTreeSet<R::Id>,
    work: CheckpointWorkContext,
}
impl<'a, R: OverlayRecord + CheckpointCanonicalRecord + 'a, D: Iterator<Item = &'a R>>
    CheckpointOverlayIterator<'a, R, D>
{
    pub(crate) fn new(
        base: Option<&'a crate::canonical::CanonicalSegmentReader>,
        delta: D,
        tombstones: &'a BTreeSet<R::Id>,
        work: &CheckpointWorkContext,
    ) -> Self {
        Self {
            base: base.map(|reader| CheckpointCanonicalIterator::new(reader, work).peekable()),
            delta: delta.peekable(),
            tombstones,
            work: work.clone(),
        }
    }
    pub(crate) fn checkpoint_steps(
        mut self,
    ) -> impl Iterator<Item = Result<Option<CheckpointRecordRef<'a, R>>>> {
        std::iter::from_fn(move || self.next_step())
    }
    fn next_base(&mut self) -> Result<CheckpointRecordRef<'a, R>> {
        self.base
            .as_mut()
            .and_then(Iterator::next)
            .expect("peeked base record exists")
            .map(CheckpointRecordRef::Owned)
            .map_err(|error| match error {
                CanonicalSegmentError::Work(error) => HawDBError::from_storage_error(error),
                error => HawDBError::StorageIntegrity(error.to_string()),
            })
    }
    fn next_step(&mut self) -> Option<Result<Option<CheckpointRecordRef<'a, R>>>> {
        if let Err(error) = self.work.checkpoint() {
            return Some(Err(HawDBError::from_storage_error(error)));
        }
        // Hydration owns its permits. A physical error must precede both
        // replacement and tombstone handling, exactly as in the ordinary scan.
        let base_id = match self.base.as_mut().and_then(|base| base.peek()) {
            Some(Ok(record)) => Some(record.id()),
            Some(Err(_)) => return Some(self.next_base().map(Some)),
            None => None,
        };
        let unit = match self.work.start_unit() {
            Ok(unit) => unit,
            Err(error) => return Some(Err(HawDBError::from_storage_error(error))),
        };
        let delta_id = self.delta.peek().map(|record| record.id());
        let record = match (base_id, delta_id) {
            (None, None) => {
                unit.finish();
                return None;
            }
            (Some(base_id), Some(delta_id)) if base_id == delta_id => {
                if let Err(error) = self.next_base() {
                    return Some(Err(error));
                }
                Ok(CheckpointRecordRef::Borrowed(
                    self.delta.next().expect("matching delta exists"),
                ))
            }
            (None, Some(_)) => Ok(CheckpointRecordRef::Borrowed(
                self.delta.next().expect("peeked delta exists"),
            )),
            (Some(base_id), Some(delta_id)) if base_id > delta_id => Ok(
                CheckpointRecordRef::Borrowed(self.delta.next().expect("peeked delta exists")),
            ),
            (Some(_), _) => self.next_base(),
        };
        let result = record.map(|record| {
            if self.tombstones.contains(&record.id()) {
                None
            } else {
                Some(record)
            }
        });
        unit.finish();
        Some(result)
    }
}
impl<'a, R: OverlayRecord + CheckpointCanonicalRecord + 'a, D: Iterator<Item = &'a R>> Iterator
    for CheckpointOverlayIterator<'a, R, D>
{
    type Item = Result<CheckpointRecordRef<'a, R>>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.next_step()? {
                Ok(None) => continue,
                Ok(Some(record)) => return Some(Ok(record)),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}
