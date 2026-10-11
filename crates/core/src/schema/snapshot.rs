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

//! Shared catalog collections keep snapshot capture independent of schema size.
//!
//! Mutation clones only the changed collection when a snapshot still owns it.
//! This does not bound DDL cloning or final collection destruction.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

pub(super) struct CatalogSnapshot<T>(Arc<T>);

impl<T> Clone for CatalogSnapshot<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: Default> Default for CatalogSnapshot<T> {
    fn default() -> Self {
        Self(Arc::new(T::default()))
    }
}

impl<T: fmt::Debug> fmt::Debug for CatalogSnapshot<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl<T> Deref for CatalogSnapshot<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T: Clone> DerefMut for CatalogSnapshot<T> {
    fn deref_mut(&mut self) -> &mut T {
        Arc::make_mut(&mut self.0)
    }
}

impl<'a, T> IntoIterator for &'a CatalogSnapshot<T>
where
    &'a T: IntoIterator,
{
    type Item = <&'a T as IntoIterator>::Item;
    type IntoIter = <&'a T as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.as_ref().into_iter()
    }
}

impl<I, T: FromIterator<I>> FromIterator<I> for CatalogSnapshot<T> {
    fn from_iter<It: IntoIterator<Item = I>>(input: It) -> Self {
        Self(Arc::new(T::from_iter(input)))
    }
}
