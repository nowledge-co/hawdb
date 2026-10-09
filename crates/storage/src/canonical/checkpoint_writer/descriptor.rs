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

//! Keep returned segment Bloom allocations admitted through their consumer.
//! Encoded leaf payload and descriptor-tree storage have separate ownership.

use super::*;

pub(in crate::canonical) struct Descriptor {
    descriptor: CanonicalSegmentDescriptor,
    _memory: CheckpointAllocationOwner,
}

impl std::ops::Deref for Descriptor {
    type Target = CanonicalSegmentDescriptor;

    fn deref(&self) -> &Self::Target {
        &self.descriptor
    }
}

impl Descriptor {
    pub(in crate::canonical) fn new(
        descriptor: CanonicalSegmentDescriptor,
        memory: CheckpointAllocationOwner,
    ) -> Self {
        Self {
            descriptor,
            _memory: memory,
        }
    }
}
