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

use super::*;
use crate::background::{CheckpointWorkContext, CheckpointWorkError};

pub(in crate::relational::row_page::publication) fn work_error(
    error: CheckpointWorkError,
) -> RelationalRowPagePublicationError {
    RelationalRowPagePublicationError::Admission(error.to_string())
}

pub(super) fn pad_slot(
    encoded: &mut Vec<u8>,
    slot_bytes: usize,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    encoded.reserve(slot_bytes - encoded.len());
    unit.finish();
    while encoded.len() < slot_bytes {
        let unit = work.start_unit().map_err(work_error)?;
        encoded.resize((encoded.len() + 64 * 1024).min(slot_bytes), 0);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn write_slot(
    writer: &mut BufWriter<File>,
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    write_bytes(writer, hasher, bytes, "write row-page slot", work)
}

pub(super) fn finish(
    writer: &mut BufWriter<File>,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    flush(writer, "flush row-page artifact", work)?;
    sync(writer, "sync row-page artifact", work)
}

pub(in crate::relational::row_page::publication) fn compare(
    left: &[u8],
    right: &[u8],
    work: &CheckpointWorkContext,
) -> Result<std::cmp::Ordering, RelationalRowPagePublicationError> {
    crate::relational::row_page::checkpoint::compare(left, right, work).map_err(Into::into)
}

pub(in crate::relational::row_page::publication) fn clone_bytes(
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<Vec<u8>, RelationalRowPagePublicationError> {
    let mut output = Vec::new();
    crate::relational::row_page::checkpoint::append(&mut output, bytes, work)?;
    Ok(output)
}

pub(super) fn hash(
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        hasher.update(block);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn write_bytes(
    writer: &mut BufWriter<File>,
    hasher: &mut IntegrityHasher,
    bytes: &[u8],
    operation: &'static str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    for block in bytes.chunks(64 * 1024) {
        let unit = work.start_unit().map_err(work_error)?;
        let wave = work.io_wave().map_err(work_error)?;
        writer.write_all(block).map_err(durability(operation))?;
        hasher.update(block);
        drop(wave);
        unit.finish();
    }
    work.checkpoint().map_err(work_error)
}

pub(super) fn flush(
    writer: &mut BufWriter<File>,
    operation: &'static str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    writer.flush().map_err(durability(operation))?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)
}

pub(super) fn sync(
    writer: &BufWriter<File>,
    operation: &'static str,
    work: &CheckpointWorkContext,
) -> Result<(), RelationalRowPagePublicationError> {
    let unit = work.start_unit().map_err(work_error)?;
    let wave = work.io_wave().map_err(work_error)?;
    writer.get_ref().sync_all().map_err(durability(operation))?;
    drop(wave);
    unit.finish();
    work.checkpoint().map_err(work_error)
}

pub(in crate::relational::row_page::publication) use reader::{
    read_descriptor, validate_descriptor,
};
mod reader;
