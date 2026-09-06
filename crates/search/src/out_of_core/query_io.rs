use super::{read_exact_at, SearchMetadataDocument};
use crate::query_memory::Admitted;
use crate::{checksum_bytes, Result, RuntimeTaskContext, SkeinError};
use skein_executor::QueryMemoryAccount;
use std::fs::File;
use std::mem::size_of;
use std::num::NonZeroUsize;

// zstd 1.5.7's non-streaming, dictionary-free modern-frame path allocates
// one DCtx, including its fixed entropy/literal workspaces. No window buffer
// is needed: history is the admitted contiguous output. Requalify on upgrades.
pub(super) const DECODE_WORKSPACE_BYTES: usize = 1024 * 1024;

pub(super) fn checkpoint(task: &RuntimeTaskContext) -> Result<()> {
    task.checkpoint()
        .map_err(|reason| SkeinError::Execution(format!("search candidate task {reason}")))
}

pub(super) fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right).ok_or_else(overflow)
}

pub(super) fn mul(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right).ok_or_else(overflow)
}

fn overflow() -> SkeinError {
    SkeinError::Execution("search candidate capacity overflow".to_owned())
}

pub(super) fn read(
    file: &File,
    offset: u64,
    length: u64,
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
) -> Result<Admitted<Vec<u8>>> {
    checkpoint(task)?;
    let length = usize::try_from(length).map_err(|_| overflow())?;
    let lease = memory.reserve(length)?;
    let _permit = task.acquire_io_wave(NonZeroUsize::MIN).map_err(|error| {
        SkeinError::Execution(format!("search candidate I/O admission failed: {error}"))
    })?;
    checkpoint(task)?;
    #[cfg(test)]
    evidence::read();
    let mut bytes = vec![0; length];
    read_exact_at(file, offset, &mut bytes)?;
    checkpoint(task)?;
    Ok(Admitted::new(bytes, lease))
}

pub(super) fn decode(
    bytes: &[u8],
    limit: u64,
    memory: &QueryMemoryAccount,
    task: &RuntimeTaskContext,
) -> Result<Admitted<String>> {
    checkpoint(task)?;
    let envelope = crate::snapshot_envelope::Envelope::parse(bytes, limit)?;
    if zstd::zstd_safe::version_number() != 10507 {
        return Err(SkeinError::Execution(
            "search decode workspace needs zstd version qualification".to_owned(),
        ));
    }
    // Reject legacy/skippable frames before any native context allocation.
    // Modern concatenated frames retain the preceding reader's decode behavior.
    let mut remaining = envelope.payload;
    while !remaining.is_empty() {
        checkpoint(task)?;
        if !remaining.starts_with(&0xfd2f_b528u32.to_le_bytes()) {
            return Err(SkeinError::Storage(
                "search metadata requires modern zstd frames".to_owned(),
            ));
        }
        let length = zstd::zstd_safe::find_frame_compressed_size(remaining).map_err(|_| {
            SkeinError::Storage("search metadata zstd frame extent is invalid".to_owned())
        })?;
        if length == 0 || length > remaining.len() {
            return Err(overflow());
        }
        remaining = &remaining[length..];
    }
    let workspace = memory.reserve(DECODE_WORKSPACE_BYTES)?;
    let output_memory = memory.reserve(envelope.decoded_len)?;
    #[cfg(test)]
    evidence::decode();
    let mut context = zstd::zstd_safe::DCtx::try_create()
        .ok_or_else(|| SkeinError::Execution("search zstd context allocation failed".to_owned()))?;
    if context.sizeof() > DECODE_WORKSPACE_BYTES {
        return Err(SkeinError::Execution(
            "search zstd context exceeds admission".to_owned(),
        ));
    }
    let mut output = vec![0; envelope.decoded_len];
    let length = context
        .decompress(output.as_mut_slice(), envelope.payload)
        .map_err(|error| {
            SkeinError::Storage(format!(
                "search metadata zstd decompression failed: {}",
                zstd::zstd_safe::get_error_name(error)
            ))
        })?;
    checkpoint(task)?;
    if context.sizeof() > DECODE_WORKSPACE_BYTES
        || length != output.len()
        || checksum_bytes(&output) != envelope.decoded_checksum
    {
        return Err(SkeinError::Storage(
            "search metadata decompressed length, checksum or workspace mismatch".to_owned(),
        ));
    }
    drop(context);
    drop(workspace);
    let text = String::from_utf8(output)
        .map_err(|_| SkeinError::Storage("search metadata is not UTF-8".to_owned()))?;
    Ok(Admitted::new(text, output_memory))
}

pub(super) fn metadata_fields(line: &str) -> Result<(&str, &str, &str)> {
    let mut fields = line.split('\t');
    match (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) {
        (Some("meta"), Some(id), Some(ordinal), Some(metadata), None) => {
            Ok((id, ordinal, metadata))
        }
        _ => Err(SkeinError::Storage(
            "search metadata sidecar line is invalid".to_owned(),
        )),
    }
}

pub(super) fn metadata_bytes(
    text: &str,
    expected: usize,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let mut lines = text.lines();
    if lines.next() != Some("SKEIN_SEARCH_METADATA_SEGMENT_V1") {
        return Err(SkeinError::Storage(
            "search metadata sidecar header is invalid".to_owned(),
        ));
    }
    let mut count = 0;
    let mut bytes = mul(expected, size_of::<SearchMetadataDocument>())?;
    for line in lines {
        checkpoint(task)?;
        let (id, _, metadata) = metadata_fields(line)?;
        count += 1;
        if count > expected {
            return Err(SkeinError::Storage(
                "search metadata sidecar count mismatch".to_owned(),
            ));
        }
        bytes = add(bytes, id.len() / 2)?;
        if !metadata.is_empty() {
            for pair in metadata.split(';') {
                let (key, value) = pair
                    .split_once('=')
                    .ok_or_else(|| SkeinError::Storage("invalid metadata pair".to_owned()))?;
                // Same conservative B-tree occupancy/split envelope as the
                // build decoder; duplicate keys may over-admit but never grow it.
                bytes = add(bytes, add(2048, add(key.len() / 2, value.len() / 2)?)?)?;
            }
        }
    }
    if count != expected {
        return Err(SkeinError::Storage(
            "search metadata sidecar count mismatch".to_owned(),
        ));
    }
    Ok(bytes)
}

#[cfg(test)]
pub(super) mod evidence {
    use std::cell::Cell;
    thread_local! { static READS: Cell<usize> = const { Cell::new(0) }; static DECODES: Cell<usize> = const { Cell::new(0) }; }
    pub(super) fn read() {
        READS.with(|v| v.set(v.get() + 1));
    }
    pub(super) fn decode() {
        DECODES.with(|v| v.set(v.get() + 1));
    }
    pub(in super::super) fn take() -> (usize, usize) {
        (READS.with(|v| v.replace(0)), DECODES.with(|v| v.replace(0)))
    }
}
