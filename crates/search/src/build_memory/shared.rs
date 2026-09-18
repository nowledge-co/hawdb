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

//! Shared payloads free their control block before dropping capacity owners.

use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Shared<T>(ManuallyDrop<Arc<T>>);

impl<T> Shared<T> {
    pub(crate) fn new(value: T) -> Self {
        Self(ManuallyDrop::new(Arc::new(value)))
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(ManuallyDrop::new(Arc::clone(&self.0)))
    }
}

impl<T> Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Every clone takes this path. Concurrent final drops therefore extract
        // the payload exactly once. No weak references or raw Arcs escape, so
        // into_inner frees the control block before the returned payload dies.
        // An ordinary Arc drop would release a payload's lease before freeing
        // the block whose capacity it also accounts for.
        // SAFETY: Drop runs once; no method accesses the field after this take.
        // ManuallyDrop preserves Arc's representation and prevents a second drop.
        let arc = unsafe { ManuallyDrop::take(&mut self.0) };
        drop(Arc::into_inner(arc));
    }
}
