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

//! Admit the complete canonical record before encoding. Inline values stream
//! directly into that owning buffer; spilled values retain their own inventory.

use super::*;
use crate::background::CheckpointBytes;

#[cfg(test)]
mod tests;

pub(in crate::canonical) fn node(
    node: &NodeRecord,
    spills: Option<&mut PropertySpillWriter>,
    dictionary: &mut PropertyKeyDictionary,
    config: CanonicalSegmentConfig,
) -> Result<CheckpointBytes, CanonicalSegmentError> {
    let work = dictionary
        .checkpoint_work()
        .expect("checkpoint dictionary")
        .clone();
    let count = u32_len(node.labels.len(), "node labels")?;
    let prefix = 4u64 + u64::from(count) * 4;
    let length = length(
        prefix,
        &node.properties,
        spills.as_deref(),
        dictionary,
        &work,
    )?;
    let mut output = allocate(length, config, &work)?;
    output.append(&count.to_le_bytes(), &work)?;
    let mut labels = node.labels.iter();
    let mut block = [0u8; 4096];
    loop {
        let unit = work.start_unit()?;
        let mut bytes = 0;
        for label in labels.by_ref().take(block.len() / 4) {
            block[bytes..bytes + 4].copy_from_slice(&label.0.to_le_bytes());
            bytes += 4;
        }
        unit.finish();
        if bytes == 0 {
            break;
        }
        output.append(&block[..bytes], &work)?;
    }
    properties(&node.properties, spills, dictionary, &mut output, &work)?;
    work.checkpoint()?;
    Ok(output)
}

pub(in crate::canonical) fn relationship(
    relationship: &RelRecord,
    spills: Option<&mut PropertySpillWriter>,
    dictionary: &mut PropertyKeyDictionary,
    config: CanonicalSegmentConfig,
) -> Result<CheckpointBytes, CanonicalSegmentError> {
    let work = dictionary
        .checkpoint_work()
        .expect("checkpoint dictionary")
        .clone();
    let length = length(
        20,
        &relationship.properties,
        spills.as_deref(),
        dictionary,
        &work,
    )?;
    let mut output = allocate(length, config, &work)?;
    output.append(&relationship.source.0.to_le_bytes(), &work)?;
    output.append(&relationship.target.0.to_le_bytes(), &work)?;
    output.append(&relationship.rel_type.0.to_le_bytes(), &work)?;
    properties(
        &relationship.properties,
        spills,
        dictionary,
        &mut output,
        &work,
    )?;
    work.checkpoint()?;
    Ok(output)
}

fn length(
    prefix: u64,
    properties: &BTreeMap<String, Value>,
    spills: Option<&PropertySpillWriter>,
    dictionary: &mut PropertyKeyDictionary,
    work: &CheckpointWorkContext,
) -> Result<u64, CanonicalSegmentError> {
    u32_len(properties.len(), "property map")?;
    let mut length = prefix + 4;
    for (key, value) in properties {
        // Intern before final record admission, preserving first-seen IDs.
        // The second lookup avoids a separate temporary key-ID array.
        dictionary.intern(key)?;
        let value_bytes = value_size(value, work)?;
        let wire = if spills.is_some_and(|spills| spills.should_spill(value_bytes)) {
            9
        } else {
            value_bytes as u64
        };
        length = length
            .checked_add(4)
            .and_then(|length| length.checked_add(wire))
            .ok_or_else(|| {
                CanonicalSegmentError::Work(work.record_failure(CheckpointWorkError::Allocation {
                    bytes: u64::MAX,
                    reason: "canonical record length overflows u64".into(),
                }))
            })?;
    }
    work.checkpoint()?;
    Ok(length)
}

fn allocate(
    length: u64,
    config: CanonicalSegmentConfig,
    work: &CheckpointWorkContext,
) -> Result<CheckpointBytes, CanonicalSegmentError> {
    let record_bytes = length.saturating_add(12);
    if record_bytes > config.max_record_bytes.get() {
        return Err(CanonicalSegmentError::RecordTooLarge {
            record_bytes,
            max_bytes: config.max_record_bytes.get(),
        });
    }
    let capacity = usize::try_from(length).map_err(|_| {
        CanonicalSegmentError::Work(work.record_failure(CheckpointWorkError::Allocation {
            bytes: length,
            reason: "canonical record capacity exceeds usize".into(),
        }))
    })?;
    CheckpointBytes::new(capacity, work).map_err(CanonicalSegmentError::Work)
}

fn properties(
    properties: &BTreeMap<String, Value>,
    mut spills: Option<&mut PropertySpillWriter>,
    dictionary: &mut PropertyKeyDictionary,
    output: &mut CheckpointBytes,
    work: &CheckpointWorkContext,
) -> Result<(), CanonicalSegmentError> {
    output.append(
        &u32_len(properties.len(), "property map")?.to_le_bytes(),
        work,
    )?;
    for (key, value) in properties {
        output.append(&dictionary.intern(key)?.to_le_bytes(), work)?;
        let value_bytes = value_size(value, work)?;
        if let Some(spills) = spills.as_deref_mut()
            && spills.should_spill(value_bytes)
        {
            let encoded = encode_value(value, work)?;
            let id = spills.push_checkpoint(encoded)?;
            output.append(&[7], work)?;
            output.append(&id.to_le_bytes(), work)?;
        } else {
            value::append(value, output, work)?;
        }
    }
    work.checkpoint()?;
    Ok(())
}

fn value_size(value: &Value, work: &CheckpointWorkContext) -> Result<usize, CanonicalSegmentError> {
    let length = encoded_value_len_with_work(value, 1, Some(work))?;
    usize::try_from(length).map_err(|_| {
        CanonicalSegmentError::Work(work.record_failure(CheckpointWorkError::Allocation {
            bytes: length,
            reason: "canonical value capacity exceeds usize".into(),
        }))
    })
}
