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

//! Sparse record directories for canonical point reads.
//!
//! Canonical segment records are variable-length frames in ascending id
//! order, so locating one record used to walk every preceding frame of its
//! (up to `target_segment_bytes`) segment. Out-of-core expansion point-reads
//! each relationship and each target node, which made it
//! O(relationships x segment records). A directory keeps the id and frame
//! offset of every `POINT_DIRECTORY_STRIDE`-th record, so a point read binary
//! searches the directory and walks at most a stride of frames.
//!
//! The directory is derived from segment bytes the range reader already
//! verified against the descriptor's content digest. It is admitted into the
//! shared segment cache under its own representation, so it is charged to the
//! cache capacity, evictable, and dropped with its generation. When admission
//! is refused the freshly built directory serves the current read only.

use super::*;
use crate::cache::{RepresentationKind, SegmentBytes, SegmentCacheError, SegmentCacheKey};

/// Records per directory entry. Each entry costs 16 bytes, while a canonical
/// frame is at least 12 bytes, so a directory is bounded by 1/24 of its segment.
pub(crate) const POINT_DIRECTORY_STRIDE: u32 = 32;
const ENTRY_BYTES: usize = 16;

#[cfg(test)]
thread_local! {
    static POINT_FRAMES_EXAMINED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Frames examined by canonical point reads on this thread (tests only).
#[cfg(test)]
pub(crate) fn point_frames_examined() -> u64 {
    POINT_FRAMES_EXAMINED.with(std::cell::Cell::get)
}

impl CanonicalSegmentReader {
    /// Locates one record of an already selected and read segment. Segments
    /// larger than one stride consult (and on first use build) the cached
    /// sparse directory instead of walking every preceding frame.
    pub(super) fn point_frame(
        &self,
        segment: &CanonicalSegmentDescriptor,
        bytes: &[u8],
        id: u64,
    ) -> Result<Option<SegmentRecordFrame>, CanonicalSegmentError> {
        let mut cursor = SegmentRecordCursor::new(bytes, self.manifest.generation, segment)?;
        if id < segment.min_record_id || id > segment.max_record_id {
            return Ok(None);
        }
        let mut expected_first = None;
        if segment.record_count > POINT_DIRECTORY_STRIDE {
            let directory = self.point_directory(segment, bytes)?;
            let (ordinal, offset, entry_id) = directory_seek(&directory, segment, id)?;
            cursor.offset = offset;
            cursor.index = ordinal;
            cursor.previous_id = None;
            expected_first = Some(entry_id);
        }
        while let Some(frame) = cursor.next(bytes, segment)? {
            #[cfg(test)]
            POINT_FRAMES_EXAMINED.with(|frames| frames.set(frames.get() + 1));
            if let Some(expected) = expected_first.take()
                && frame.id != expected
            {
                return Err(CanonicalSegmentError::Corrupt(format!(
                    "canonical segment {} point directory disagrees with its records",
                    segment.segment_id
                )));
            }
            if frame.id >= id {
                return Ok((frame.id == id).then_some(frame));
            }
            cursor.accept(frame);
        }
        Ok(None)
    }

    fn point_directory(
        &self,
        segment: &CanonicalSegmentDescriptor,
        bytes: &[u8],
    ) -> Result<SegmentBytes, CanonicalSegmentError> {
        let Some((cache, store_id, manifest_generation)) = self.range_reader.cache_identity()
        else {
            return build_point_directory(bytes, self.manifest.generation, segment).map(Into::into);
        };
        let key = SegmentCacheKey {
            store_id,
            manifest_generation,
            segment_id: segment.segment_id,
            content_digest: segment.content_digest,
            representation: RepresentationKind::CanonicalSegmentPointDirectory,
        };
        let tag = point_directory_tag(segment);
        if let Some(lease) = cache.get_verified(&key, tag) {
            return Ok(lease.into_bytes());
        }
        let directory = build_point_directory(bytes, self.manifest.generation, segment)?;
        match cache.insert_verified(key, tag, directory) {
            Ok(lease) => Ok(lease.into_bytes()),
            Err(error) => {
                let (reason, directory) = error.into_parts();
                match reason {
                    SegmentCacheError::EntryTooLarge { .. }
                    | SegmentCacheError::PinnedCapacity { .. } => Ok(directory.into()),
                    reason => Err(CanonicalSegmentError::Corrupt(format!(
                        "canonical point directory cache rejected segment {}: {reason}",
                        segment.segment_id
                    ))),
                }
            }
        }
    }
}

/// Binds a directory to the exact verified segment extent and stride.
fn point_directory_tag(segment: &CanonicalSegmentDescriptor) -> [u8; 32] {
    let mut tag = [0u8; 32];
    tag[..8].copy_from_slice(&segment.content_digest.0.to_le_bytes());
    tag[8..16].copy_from_slice(&segment.offset.to_le_bytes());
    tag[16..24].copy_from_slice(&segment.length.get().to_le_bytes());
    tag[24..28].copy_from_slice(&segment.record_count.to_le_bytes());
    tag[28..].copy_from_slice(&POINT_DIRECTORY_STRIDE.to_le_bytes());
    tag
}

fn directory_entries(segment: &CanonicalSegmentDescriptor) -> usize {
    segment.record_count.div_ceil(POINT_DIRECTORY_STRIDE) as usize
}

/// Walks and validates the complete segment framing once.
fn build_point_directory(
    bytes: &[u8],
    generation: ManifestGeneration,
    segment: &CanonicalSegmentDescriptor,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let length = directory_entries(segment)
        .checked_mul(ENTRY_BYTES)
        .ok_or_else(|| CanonicalSegmentError::Corrupt("point directory size overflow".into()))?;
    let mut directory = Vec::new();
    directory.try_reserve_exact(length).map_err(|_| {
        CanonicalSegmentError::Source("canonical point directory allocation failed".into())
    })?;
    let mut cursor = SegmentRecordCursor::new(bytes, generation, segment)?;
    while let Some(frame) = cursor.next(bytes, segment)? {
        if cursor.index % POINT_DIRECTORY_STRIDE == 0 {
            directory.extend_from_slice(&frame.id.to_le_bytes());
            directory.extend_from_slice(&(cursor.offset as u64).to_le_bytes());
        }
        cursor.accept(frame);
    }
    if directory.len() != length {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} framing disagrees with its record count",
            segment.segment_id
        )));
    }
    Ok(directory)
}

/// Returns the ordinal, frame offset and id of the last directory entry whose
/// id does not exceed `id`. The caller has checked `min_record_id <= id`.
fn directory_seek(
    directory: &[u8],
    segment: &CanonicalSegmentDescriptor,
    id: u64,
) -> Result<(u32, usize, u64), CanonicalSegmentError> {
    let corrupt = || {
        CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} point directory is malformed",
            segment.segment_id
        ))
    };
    let entries = directory_entries(segment);
    if directory.len() != entries.checked_mul(ENTRY_BYTES).ok_or_else(corrupt)? || entries == 0 {
        return Err(corrupt());
    }
    let entry = |index: usize| {
        let start = index * ENTRY_BYTES;
        let id = u64::from_le_bytes(directory[start..start + 8].try_into().expect("fixed id"));
        let offset = u64::from_le_bytes(
            directory[start + 8..start + ENTRY_BYTES]
                .try_into()
                .expect("fixed offset"),
        );
        (id, offset)
    };
    // First entry whose id exceeds `id`; the preceding entry starts the walk.
    let (mut low, mut high) = (0usize, entries);
    while low < high {
        let middle = low + (high - low) / 2;
        if entry(middle).0 <= id {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    let index = low.checked_sub(1).ok_or_else(corrupt)?;
    let (entry_id, offset) = entry(index);
    let offset = usize::try_from(offset).map_err(|_| corrupt())?;
    let ordinal = u32::try_from(index)
        .ok()
        .and_then(|index| index.checked_mul(POINT_DIRECTORY_STRIDE))
        .ok_or_else(corrupt)?;
    Ok((ordinal, offset, entry_id))
}
