//! Admitted control-record inspection while the publication lease is held.

use super::invalid;
use crate::build_control::checkpoint;
use crate::build_memory::{
    decoder::Decoder, path::OwnedPath, reserved::native_path, BuildMemory, SPOOL_BUFFER_BYTES,
};
use crate::{Result, SEARCH_COMPRESSION_HEADER, SEARCH_SNAPSHOT_FILE};
use skein_core::RuntimeTaskContext;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;

const LINE_BYTES: usize = 1025;
const PREFIX_BYTES: usize = 27;

/// Inspect the leading records; a binding immediately follows the snapshot header.
pub(crate) fn require_unregistered_directory(
    root: &Path,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<()> {
    let file = {
        let path = OwnedPath::join(root, Path::new(SEARCH_SNAPSHOT_FILE), memory, task)?;
        let _native = memory.spool.reserve(native_path::bytes(&path)?)?;
        checkpoint(task)?;
        let opened = File::open(&path);
        checkpoint(task)?;
        match opened {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    };
    probe(file, memory, task)
}

fn probe(input: impl Read, memory: &BuildMemory, task: &RuntimeTaskContext) -> Result<()> {
    checkpoint(task)?;
    let _input_memory = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
    let _control_memory = memory.spool.reserve(LINE_BYTES + PREFIX_BYTES)?;
    let mut reader = BufReader::with_capacity(SPOOL_BUFFER_BYTES, input);
    // Reuse the same admitted capacity through every envelope/control line.
    let mut line = String::with_capacity(LINE_BYTES);
    bounded_line(&mut reader, &mut line, task)?;
    if line.trim_end() == SEARCH_COMPRESSION_HEADER {
        let mut total = line.len();
        loop {
            bounded_line(&mut reader, &mut line, task)?;
            total += line.len();
            if total > 4096 {
                return Err(invalid("snapshot envelope exceeds header limit"));
            }
            if line == "\n" {
                break;
            }
            if line.is_empty() {
                return Err(invalid("incomplete snapshot envelope"));
            }
        }
        let _decoded_memory = memory.spool.reserve(SPOOL_BUFFER_BYTES)?;
        let decoder = Decoder::new(reader, memory, task)?;
        let mut reader = BufReader::with_capacity(SPOOL_BUFFER_BYTES, decoder);
        bounded_line(&mut reader, &mut line, task)?;
        let (prefix, length) = control_prefix(&mut reader, task)?;
        check_control_records(&line, &prefix[..length])
    } else {
        let (prefix, length) = control_prefix(&mut reader, task)?;
        check_control_records(&line, &prefix[..length])
    }
}

fn check_control_records(first: &str, second: &[u8]) -> Result<()> {
    if first != "SKEIN_SEARCH_PROJECTION_V1\n" {
        return Err(invalid("invalid snapshot header"));
    }
    // The reserved ASCII prefix is unchanged by the old lossy UTF-8 conversion.
    if second.starts_with(b"projection_consumer_binding") {
        return Err(invalid("registered projection requires its consumer owner"));
    }
    Ok(())
}

fn control_prefix(
    reader: &mut impl Read,
    task: &RuntimeTaskContext,
) -> Result<([u8; PREFIX_BYTES], usize)> {
    let mut prefix = [0; PREFIX_BYTES];
    let mut length = 0;
    while length < prefix.len() {
        checkpoint(task)?;
        match reader.read(&mut prefix[length..]) {
            Ok(0) => break,
            Ok(read) => length += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    checkpoint(task)?;
    Ok((prefix, length))
}

fn bounded_line(
    reader: &mut impl BufRead,
    line: &mut String,
    task: &RuntimeTaskContext,
) -> Result<()> {
    checkpoint(task)?;
    line.clear();
    reader.take(LINE_BYTES as u64).read_line(line)?;
    checkpoint(task)?;
    if line.len() > 1024 {
        return Err(invalid("snapshot control record exceeds limit"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
