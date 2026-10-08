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
use std::collections::LinkedList;

pub(super) use json::{checksum_with_context, EncodedManifest};

const NAME: &str = "lexical projection manifest";

/// Decode records without repeatedly reallocating a corpus-sized directory.
///
/// Each node owns one validated record. Once the count is known, move the
/// records into one exact-capacity Vec; strings are never copied. Nodes plus
/// the final Vec fit the existing three-slot-per-record decode admission.
pub(super) fn deserialize_blocks<'de, D>(
    deserializer: D,
) -> std::result::Result<Vec<super::BlockDescriptor>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Blocks;

    impl<'de> serde::de::Visitor<'de> for Blocks {
        type Value = Vec<super::BlockDescriptor>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a sequence")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut nodes = LinkedList::new();
            while let Some(block) = sequence.next_element::<super::BlockDescriptor>()? {
                nodes.push_back(block);
            }
            let mut blocks = Vec::new();
            blocks.try_reserve_exact(nodes.len()).map_err(|error| {
                serde::de::Error::custom(format!(
                    "lexical block directory allocation failed: {error}"
                ))
            })?;
            blocks.extend(nodes);
            Ok(blocks)
        }
    }

    deserializer.deserialize_seq(Blocks)
}

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
