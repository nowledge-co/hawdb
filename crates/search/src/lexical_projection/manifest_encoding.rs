use crate::build_control::json;
use crate::build_memory::BuildMemory;
use crate::Result;
use serde::Serialize;
use skein_core::RuntimeTaskContext;

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
