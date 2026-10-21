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

//! Borrowed immutable metadata for the ordinary reader refresh path.
//! Production cleanup requires captured retention rather than pathname evidence.

use super::{SearchOutOfCoreManifestBody, SearchOutOfCoreReader, SearchOutOfCoreSegmentReader};

#[derive(Clone, Copy)]
pub(super) struct View<'a> {
    pub(super) manifest: &'a SearchOutOfCoreManifestBody,
    pub(super) segments: &'a [SearchOutOfCoreSegmentReader],
}

impl<'a> View<'a> {
    pub(super) fn from_reader(reader: &'a SearchOutOfCoreReader) -> Self {
        Self {
            manifest: &reader.manifest,
            segments: &reader.segments,
        }
    }
}
