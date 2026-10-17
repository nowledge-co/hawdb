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

//! Verify immutable vector payloads with one bounded file pass before mmap.
//!
//! At every chunk boundary the hashers cover exactly the consumed segment and
//! file prefixes. `previous_id` is the last decoded ID; the pending violation is
//! the least (row, field) violation observed. This preserves the mapped verifier's
//! row order even though the file stores IDs, scales and offsets in columns.
//! Checksum failure takes precedence over header errors in the same segment.

use super::*;

pub(super) const BUFFER_BYTES: usize = 8192;

#[derive(Clone, Copy)]
enum InvalidField {
    Id { previous: u64, next: u64 },
    Scale,
    Offset,
}

#[derive(Clone, Copy)]
struct Violation {
    row: usize,
    field: InvalidField,
}

impl Violation {
    fn rank(self) -> (usize, u8) {
        (
            self.row,
            match self.field {
                InvalidField::Id { .. } => 0,
                InvalidField::Scale => 1,
                InvalidField::Offset => 2,
            },
        )
    }

    fn retain_first(slot: &mut Option<Self>, row: usize, field: InvalidField) {
        let candidate = Self { row, field };
        if slot.is_none_or(|current| candidate.rank() < current.rank()) {
            *slot = Some(candidate);
        }
    }

    fn error(self, segment: usize) -> ProjectionError {
        ProjectionError::CorruptArtifact(match self.field {
            InvalidField::Id { previous, next } => {
                format!("ids are not strictly increasing at {next} after {previous}")
            }
            InvalidField::Scale => format!(
                "invalid RaBitQ reconstruction scale in segment {segment} row {}",
                self.row
            ),
            InvalidField::Offset => format!(
                "invalid RaBitQ reconstruction offset in segment {segment} row {}",
                self.row
            ),
        })
    }
}

/// The caller has validated contiguous descriptors and the payload/footer boundary.
#[cfg(test)]
pub(super) fn verify(reader: &mut (impl Read + Seek), manifest: &ProjectionManifest) -> Result<()> {
    verify_admitted(reader, manifest, &admission::Unaccounted)
}

pub(super) fn verify_admitted(
    reader: &mut (impl Read + Seek),
    manifest: &ProjectionManifest,
    admission: &impl ProjectionReadAdmission,
) -> Result<()> {
    admission.checkpoint()?;
    {
        let _io = admission.acquire_io()?;
        reader.seek(SeekFrom::Start(0))?;
    }
    let bit_width = RaBitQBitWidth::from_bits(manifest.bit_width)?;
    let mut previous_id = None;
    let mut payload_hasher = Hasher::new();
    let mut buffer = [0u8; BUFFER_BYTES];
    for descriptor in &manifest.segments {
        let rows = descriptor.row_count;
        let header_bytes = rows
            .checked_mul(std::mem::size_of::<u64>() + 2 * std::mem::size_of::<f32>())
            .ok_or_else(|| {
                ProjectionError::CorruptArtifact("segment header arrays exceed payload".into())
            })?;
        let payload_bytes = usize::try_from(descriptor.payload_bytes).map_err(|_| {
            ProjectionError::CorruptArtifact(format!(
                "segment {} payload exceeds address space",
                descriptor.index
            ))
        })?;
        let codes_bytes = payload_bytes.checked_sub(header_bytes).ok_or_else(|| {
            ProjectionError::CorruptArtifact("segment header arrays exceed payload".into())
        })?;
        let expected_codes = rows.saturating_mul(crate::model::encoded_vector_bytes(
            manifest.dimension,
            bit_width,
        ));
        if codes_bytes != expected_codes {
            return Err(ProjectionError::CorruptArtifact(format!(
                "segment codes have {codes_bytes} bytes, expected {expected_codes}"
            )));
        }
        let mut segment_hasher = Hasher::new();
        let mut invalid = None;
        column::<8>(
            reader,
            rows,
            &mut buffer,
            &mut segment_hasher,
            &mut payload_hasher,
            admission,
            |row, word| {
                let id = u64::from_le_bytes(*word);
                if let Some(previous) = previous_id
                    && id <= previous
                {
                    Violation::retain_first(
                        &mut invalid,
                        row,
                        InvalidField::Id { previous, next: id },
                    );
                }
                previous_id = Some(id);
            },
        )?;
        column::<4>(
            reader,
            rows,
            &mut buffer,
            &mut segment_hasher,
            &mut payload_hasher,
            admission,
            |row, word| {
                let scale = f32::from_le_bytes(*word);
                if !scale.is_finite() || scale < 0.0 {
                    Violation::retain_first(&mut invalid, row, InvalidField::Scale);
                }
            },
        )?;
        column::<4>(
            reader,
            rows,
            &mut buffer,
            &mut segment_hasher,
            &mut payload_hasher,
            admission,
            |row, word| {
                if !f32::from_le_bytes(*word).is_finite() {
                    Violation::retain_first(&mut invalid, row, InvalidField::Offset);
                }
            },
        )?;
        let mut remaining = codes_bytes;
        while remaining != 0 {
            let length = remaining.min(buffer.len());
            admission::read_exact(reader, &mut buffer[..length], admission)?;
            segment_hasher.update(&buffer[..length]);
            payload_hasher.update(&buffer[..length]);
            remaining -= length;
        }
        #[cfg(test)]
        SEGMENT_CHECKSUM_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
        if segment_hasher.finalize() != descriptor.payload_checksum {
            return Err(ProjectionError::CorruptArtifact(format!(
                "segment {} checksum mismatch",
                descriptor.index
            )));
        }
        if let Some(invalid) = invalid {
            return Err(invalid.error(descriptor.index));
        }
    }
    if payload_hasher.finalize() != manifest.payload_checksum {
        return Err(ProjectionError::CorruptArtifact(
            "payload checksum mismatch".into(),
        ));
    }
    Ok(())
}

fn column<const WIDTH: usize>(
    reader: &mut impl Read,
    rows: usize,
    buffer: &mut [u8; BUFFER_BYTES],
    segment_hasher: &mut Hasher,
    payload_hasher: &mut Hasher,
    admission: &impl ProjectionReadAdmission,
    mut visit: impl FnMut(usize, &[u8; WIDTH]),
) -> Result<()> {
    let mut row = 0;
    while row < rows {
        let count = (rows - row).min(buffer.len() / WIDTH);
        let length = count * WIDTH;
        admission::read_exact(reader, &mut buffer[..length], admission)?;
        segment_hasher.update(&buffer[..length]);
        payload_hasher.update(&buffer[..length]);
        for (offset, word) in buffer[..length].chunks_exact(WIDTH).enumerate() {
            visit(
                row + offset,
                word.try_into().expect("complete fixed-width column word"),
            );
        }
        row += count;
    }
    Ok(())
}
