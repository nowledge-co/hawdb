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

use crate::build_control::json;
use crate::build_memory::BuildMemory;
use crate::Result;
use hawdb_core::RuntimeTaskContext;
use serde::Serialize;

pub(super) use json::{checksum_with_context, EncodedManifest};

const NAME: &str = "lexical projection manifest";

#[cfg(test)]
pub(super) fn encode(body: &impl Serialize, max_bytes: u64) -> Result<Vec<u8>> {
    json::encode(body, max_bytes, NAME)
}

pub(super) fn encode_with_context(
    body: &impl Serialize,
    max_bytes: u64,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<EncodedManifest> {
    json::encode_with_context(body, max_bytes, memory, task, NAME)
}

#[cfg(test)]
pub(super) mod tests;
