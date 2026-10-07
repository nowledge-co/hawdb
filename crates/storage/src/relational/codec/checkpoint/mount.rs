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

//! Buffered, cooperatively controlled input for the ordinary relational parser.
//! Schema/row allocations, validation and index reconstruction remain separate
//! resource boundaries; this module controls file I/O and integrity traversal.

use super::*;

const INPUT_BYTES: usize = 64 * 1024;

struct CheckpointFileInput<'a> {
    inner: FileDecodeInput,
    work: &'a CheckpointWorkContext,
    buffer: Vec<u8>,
    buffer_position: usize,
    buffer_length: usize,
    consumed: usize,
}

impl<'a> CheckpointFileInput<'a> {
    fn new(
        inner: FileDecodeInput,
        work: &'a CheckpointWorkContext,
    ) -> Result<Self, RelationalError> {
        let unit = work.start_unit().map_err(work_error)?;
        let buffer = vec![0; inner.payload_len.min(INPUT_BYTES)];
        unit.finish();
        work.checkpoint().map_err(work_error)?;
        Ok(Self {
            inner,
            work,
            buffer,
            buffer_position: 0,
            buffer_length: 0,
            consumed: 0,
        })
    }

    fn refill(&mut self) -> Result<(), RelationalError> {
        let length = self
            .inner
            .payload_len
            .saturating_sub(self.inner.offset)
            .min(self.buffer.len());
        let unit = self.work.start_unit().map_err(work_error)?;
        {
            let _wave = self.work.io_wave().map_err(work_error)?;
            self.inner.read_exact(&mut self.buffer[..length])?;
        }
        self.buffer_position = 0;
        self.buffer_length = length;
        unit.finish();
        self.work.checkpoint().map_err(work_error)
    }
}

impl DecodeInput for CheckpointFileInput<'_> {
    fn checkpoint_work_context(&self) -> Option<&CheckpointWorkContext> {
        Some(self.work)
    }

    fn len(&self) -> usize {
        self.inner.payload_len
    }

    fn position(&self) -> usize {
        self.consumed
    }

    fn read_exact(&mut self, output: &mut [u8]) -> Result<(), RelationalError> {
        self.work.checkpoint().map_err(work_error)?;
        // Check the entire request before read-ahead to preserve ordinary
        // truncated/overflow diagnostics and logical decoder offsets.
        let end = self
            .consumed
            .checked_add(output.len())
            .ok_or_else(|| RelationalError::Corruption("durable decoder offset overflow".into()))?;
        if end > self.len() {
            return Err(RelationalError::Corruption(
                "truncated relational durable payload".into(),
            ));
        }
        let mut written = 0;
        while written < output.len() {
            if self.buffer_position == self.buffer_length {
                self.refill()?;
            }
            let length = (output.len() - written).min(self.buffer_length - self.buffer_position);
            let unit = self.work.start_unit().map_err(work_error)?;
            output[written..written + length]
                .copy_from_slice(&self.buffer[self.buffer_position..self.buffer_position + length]);
            self.buffer_position += length;
            self.consumed += length;
            written += length;
            unit.finish();
        }
        self.work.checkpoint().map_err(work_error)
    }

    fn finish(self) -> Result<(), RelationalError> {
        let unit = self.work.start_unit().map_err(work_error)?;
        self.inner.finish()?;
        unit.finish();
        self.work.checkpoint().map_err(work_error)
    }
}

pub(crate) fn decode_relational_checkpoint_file_with_work_context(
    path: &Path,
    limits: RelationalDecodeLimits,
    index_load: RelationalCheckpointIndexLoad,
    work: &CheckpointWorkContext,
) -> Result<RelationalCheckpoint, RelationalError> {
    let encoded_len = {
        let unit = work.start_unit().map_err(work_error)?;
        let _wave = work.io_wave().map_err(work_error)?;
        let length = crate::file_io::metadata(path)
            .map_err(|error| {
                RelationalError::from_io(
                    &format!("failed to inspect relational checkpoint {}", path.display()),
                    error,
                )
            })?
            .len();
        unit.finish();
        length
    };
    if encoded_len > limits.max_record_bytes as u64 {
        return Err(RelationalError::Admission(format!(
            "relational checkpoint {} contains {encoded_len} bytes, exceeding max_record_bytes {}",
            path.display(),
            limits.max_record_bytes
        )));
    }
    let (epoch, input) = {
        let unit = work.start_unit().map_err(work_error)?;
        let _wave = work.io_wave().map_err(work_error)?;
        let result = FileDecodeInput::open_checkpoint(path, encoded_len, limits)?;
        unit.finish();
        result
    };
    let mut reader = FileSegmentRangeReader::new();
    {
        let unit = work.start_unit().map_err(work_error)?;
        let _wave = work.io_wave().map_err(work_error)?;
        reader.register(RELATIONAL_CHECKPOINT_ARTIFACT_ID, path);
        unit.finish();
    }
    let input = CheckpointFileInput::new(input, work)?;
    let checkpoint = decode_relational_checkpoint_from_decoder(
        epoch,
        Decoder::new(input, limits, false),
        limits,
        OverflowDecodeStorage::File(Arc::new(reader)),
        index_load,
    )?;
    work.checkpoint().map_err(work_error)?;
    Ok(checkpoint)
}
