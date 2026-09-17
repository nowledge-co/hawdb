//! Admit and verify private manifest files before publishing their identity.

use super::{
    artifact_file, artifacts::DirectoryMemory, manifest_encoding, BlockDescriptor,
    LexicalProjectionConfig, LexicalProjectionReader, ManifestBody, TermStatistics, MANIFEST_FILE,
    SPILL_IO_BUFFER_BYTES,
};
use crate::build_control::{checkpoint, temporary::RemoveOnDrop, CheckedWriter};
use crate::build_memory::{checked_add as add, checked_mul as mul, path::OwnedPath, BuildMemory};
use crate::{Result, SkeinError};
use skein_core::RuntimeTaskContext;
use skein_executor::QueryMemoryLease;
use skein_integrity::Crc32cHasher;
use skein_storage::durable_replace_file;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub(super) struct Paths {
    pub(super) artifact_name: String,
    pub(super) artifact: OwnedPath,
    pub(super) artifact_tmp: OwnedPath,
    manifest: OwnedPath,
    manifest_tmp: OwnedPath,
    _name_memory: QueryMemoryLease,
}

impl Paths {
    pub(super) fn new(
        root: &Path,
        generation: u64,
        memory: &BuildMemory,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let mut name_memory = memory.retained.reserve(3 * 128)?;
        let artifact_name = artifact_file(generation);
        if artifact_name.capacity() > 128 {
            return Err(SkeinError::Execution(
                "lexical artifact name exceeded admission".into(),
            ));
        }
        name_memory.shrink(name_memory.bytes() - artifact_name.capacity());
        let artifact = OwnedPath::join(root, Path::new(&artifact_name), memory, task)?;
        let artifact_tmp = OwnedPath::with_extension(&artifact, "skein.tmp", memory, task)?;
        let manifest = OwnedPath::join(root, Path::new(MANIFEST_FILE), memory, task)?;
        let manifest_tmp = OwnedPath::with_extension(&manifest, "skein.tmp", memory, task)?;
        Ok(Self {
            artifact_name,
            artifact,
            artifact_tmp,
            manifest,
            manifest_tmp,
            _name_memory: name_memory,
        })
    }
}

// The plan applies only to bytes emitted from this known body. The private file
// must match those bytes exactly before deserializing; an arbitrary replacement
// cannot claim the original shape's budget.
struct DecodePlan {
    output_bytes: usize,
    scratch_bytes: usize,
}

impl DecodePlan {
    fn new(body: &ManifestBody, task: &RuntimeTaskContext) -> Result<Self> {
        checkpoint(task)?;
        let mut strings = add(body.format.len(), body.artifact_file.len())?;
        let mut largest = body.format.len().max(body.artifact_file.len()).max(64);
        for value in body
            .term_statistics
            .iter()
            .map(|value| value.term.as_str())
            .chain(
                body.blocks
                    .iter()
                    .flat_map(|block| [block.min_key.as_str(), block.max_key.as_str()]),
            )
        {
            checkpoint(task)?;
            strings = add(strings, value.len())?;
            largest = largest.max(value.len());
        }
        // serde Vec uses geometric growth (minimum four slots); include both
        // old and replacement capacities. String visitors copy decoded slices.
        let statistics = mul(
            mul(body.term_statistics.len().max(4), 3)?,
            size_of::<TermStatistics>(),
        )?;
        let blocks = mul(
            mul(body.blocks.len().max(4), 3)?,
            size_of::<BlockDescriptor>(),
        )?;
        let output_bytes = add(add(strings, add(statistics, blocks)?)?, reader_bytes())?;
        // SliceRead reuses escaped-string scratch. Include realloc overlap and
        // the generated filename used by schema validation before releasing it.
        let scratch_bytes = add(mul(largest.max(8), 3)?, 3 * 128)?;
        Ok(Self {
            output_bytes,
            scratch_bytes,
        })
    }
}

fn reader_bytes() -> usize {
    size_of::<LexicalProjectionReader>() + size_of::<File>() + 4 * size_of::<usize>()
}

fn retained_bytes(body: &ManifestBody, task: &RuntimeTaskContext) -> Result<usize> {
    let mut bytes = add(
        reader_bytes(),
        add(body.format.capacity(), body.artifact_file.capacity())?,
    )?;
    bytes = add(
        bytes,
        mul(body.term_statistics.capacity(), size_of::<TermStatistics>())?,
    )?;
    bytes = add(
        bytes,
        mul(body.blocks.capacity(), size_of::<BlockDescriptor>())?,
    )?;
    for capacity in body
        .term_statistics
        .iter()
        .map(|value| value.term.capacity())
        .chain(
            body.blocks
                .iter()
                .flat_map(|block| [block.min_key.capacity(), block.max_key.capacity()]),
        )
    {
        checkpoint(task)?;
        bytes = add(bytes, capacity)?;
    }
    Ok(bytes)
}

pub(super) fn finish(
    input_body: ManifestBody,
    directory_memory: DirectoryMemory,
    paths: &Paths,
    config: LexicalProjectionConfig,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<Arc<LexicalProjectionReader>> {
    // Locals drop before parameters: the original directory is always owned
    // until after its payload is destroyed, including during failed encoding.
    let body = input_body;
    checkpoint(task)?;
    let _validation = memory.spool.reserve(3 * 128)?;
    body.validate_with_context(Some(task))?;
    let plan = DecodePlan::new(&body, task)?;
    let identity = (
        body.source_graph_commit_epoch,
        body.analyzer_digest,
        body.documents_digest,
    );
    let encoded = manifest_encoding::encode_with_context(
        &body,
        config.max_manifest_bytes.get(),
        memory,
        task,
    )?;
    drop(body);
    drop(directory_memory);
    drop(_validation);

    let mut output_memory = memory.retained.reserve(plan.output_bytes)?;
    let decode_scratch = memory.spool.reserve(plan.scratch_bytes)?;
    let mut guard = RemoveOnDrop::new(&paths.manifest_tmp);
    {
        let mut file = File::create(&paths.manifest_tmp)?;
        CheckedWriter::new(&mut file, Some(task)).write_all(&encoded.bytes)?;
        checkpoint(task)?;
        file.sync_all()?;
    }
    #[cfg(test)]
    tests::before_verify(paths, task);
    verify_file_bytes(&paths.manifest_tmp, &encoded.bytes, memory, task)?;
    // The opaque serde parse is a cooperative call boundary. The checksum and
    // schema walks below have bounded output/record checkpoints.
    let decoded = ManifestBody::decode_with_context(&encoded.bytes, Some(task))?;
    let actual = retained_bytes(&decoded, task)?;
    if actual > output_memory.bytes() {
        return Err(SkeinError::Execution(
            "decoded lexical manifest exceeded admission".into(),
        ));
    }
    output_memory.shrink(output_memory.bytes() - actual);
    drop(decode_scratch);
    drop(encoded);
    let reader = LexicalProjectionReader::load_decoded_manifest(
        &paths.artifact_tmp,
        decoded,
        identity.0,
        identity.1,
        identity.2,
        config,
        Some((memory, task)),
        Some(output_memory),
    )?
    .ok_or_else(|| SkeinError::Storage("built lexical projection identity mismatch".into()))?;
    // Both paths and platform rename scratch are admitted before publication.
    let _rename_memory = rename_memory(paths, memory)?;
    checkpoint(task)?;
    durable_replace_file(&paths.artifact_tmp, &paths.artifact)?;
    checkpoint(task)?;
    durable_replace_file(&paths.manifest_tmp, &paths.manifest)?;
    guard.disarm();
    // Publication has committed. A late cancellation cannot turn this into an
    // unreported failure; the outer generation checks its own commit boundary.
    Ok(reader)
}

#[cfg(not(windows))]
fn rename_memory(_paths: &Paths, memory: &BuildMemory) -> Result<QueryMemoryLease> {
    memory.spool.reserve(0)
}

#[cfg(windows)]
fn rename_memory(paths: &Paths, memory: &BuildMemory) -> Result<QueryMemoryLease> {
    use std::os::windows::ffi::OsStrExt;
    let lengths = [
        &paths.artifact_tmp,
        &paths.artifact,
        &paths.manifest_tmp,
        &paths.manifest,
    ]
    .map(|path| path.as_os_str().encode_wide().count());
    memory.spool.reserve(windows_rename_bytes(lengths)?)
}

#[cfg(any(windows, test))]
fn windows_rename_bytes(lengths: [usize; 4]) -> Result<usize> {
    lengths.into_iter().try_fold(0, |bytes, length| {
        // Native MoveFileExW owns two geometrically collected Vec<u16>.
        add(
            bytes,
            mul(mul(add(length, 1)?.max(4), 3)?, size_of::<u16>())?,
        )
    })
}

fn verify_file_bytes(
    path: &Path,
    expected: &[u8],
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    let _scratch = memory.spool.reserve(SPILL_IO_BUFFER_BYTES)?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected.len() as u64 {
        return Err(SkeinError::Storage(
            "private lexical manifest length changed".into(),
        ));
    }
    let mut buffer = [0; SPILL_IO_BUFFER_BYTES];
    for chunk in expected.chunks(buffer.len()) {
        checkpoint(task)?;
        file.read_exact(&mut buffer[..chunk.len()])?;
        if &buffer[..chunk.len()] != chunk {
            return Err(SkeinError::Storage(
                "private lexical manifest bytes changed".into(),
            ));
        }
    }
    checkpoint(task)?;
    if file.read(&mut buffer[..1])? != 0 {
        return Err(SkeinError::Storage(
            "private lexical manifest grew during verification".into(),
        ));
    }
    Ok(())
}

pub(super) fn file_digest(
    file: &File,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<(u64, u64)> {
    checkpoint(task)?;
    let _scratch = memory.spool.reserve(SPILL_IO_BUFFER_BYTES)?;
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut buffer = [0; SPILL_IO_BUFFER_BYTES];
    let mut length = 0u64;
    let mut digest = Crc32cHasher::new();
    loop {
        checkpoint(task)?;
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        length = length
            .checked_add(count as u64)
            .ok_or_else(|| SkeinError::Storage("lexical artifact length exceeds u64".into()))?;
    }
    checkpoint(task)?;
    Ok((length, digest.finish()))
}
