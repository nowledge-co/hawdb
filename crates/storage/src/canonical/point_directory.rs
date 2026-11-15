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
//! offset of every stride-th record, so a point read binary searches the
//! directory and walks at most a stride of frames.
//!
//! The stride adapts to the segment's average frame size: it is the smallest
//! power of two up to [`POINT_DIRECTORY_STRIDE`] whose directory stays within
//! 1/24 of the segment bytes, the bound the maximum stride gives segments of
//! minimum-size frames. Segments of large records (messages with content)
//! thus get a dense directory and a point read examines one or two frames
//! instead of half a maximum stride, each frame usually on its own cache line.
//! The stride is derived only from descriptor fields and is part of the
//! directory's cache tag; the directory is never persisted, so the on-disk
//! format is unchanged.
//!
//! When a segment's ids are dense (`max - min + 1 == record_count`, which
//! checkpoints produce), the entry covering an id is computed rather than
//! binary searched, so a read touches one directory entry instead of
//! O(log entries) mostly cache-missing ones. The entry's id and its first
//! frame's id are still checked, so a disagreeing directory fails closed.
//!
//! The directory is derived from segment bytes the range reader already
//! verified against the descriptor's content digest. It is admitted into the
//! shared segment cache under its own representation, so it is charged to the
//! cache capacity, evictable, and dropped with its generation. When admission
//! is refused the freshly built directory serves the current read only.
//!
//! Building a directory also validates every record of the segment once:
//! property key ids, value tags and lengths, UTF-8 strings, temporal and
//! vector payloads, and spill references (the scrub's record check). A
//! directory therefore proves that the verified segment bytes it is bound to
//! hold only well-formed records, so projected reads skip unrequested values
//! by their encoded length instead of revalidating them on every read
//! (#1482). Requested values are still fully decoded and validated per read.

use super::*;
use crate::cache::{RepresentationKind, SegmentBytes, SegmentCacheError, SegmentCacheKey};

#[cfg(test)]
#[path = "point_directory_tests.rs"]
mod tests;

/// Segments of more than this many records have a directory, whose stride
/// never exceeds it. Each entry costs 16 bytes, while a canonical frame is at
/// least 12 bytes, so a maximum-stride directory is bounded by 1/24 of its
/// segment.
pub(crate) const POINT_DIRECTORY_STRIDE: u32 = 32;
const ENTRY_BYTES: usize = 16;
/// A finer stride is chosen only while the directory stays within this
/// fraction of the segment bytes.
const DIRECTORY_SEGMENT_FRACTION: u64 = 24;

/// Records per directory entry of `segment`: the smallest power of two whose
/// directory stays within [`DIRECTORY_SEGMENT_FRACTION`] of the segment, and
/// at most [`POINT_DIRECTORY_STRIDE`]. Depends only on descriptor fields that
/// the directory's cache tag binds.
pub(crate) fn directory_stride(segment: &CanonicalSegmentDescriptor) -> u32 {
    let mut stride = 1;
    while stride < POINT_DIRECTORY_STRIDE {
        let entries = u64::from(segment.record_count.div_ceil(stride));
        if entries
            .saturating_mul(ENTRY_BYTES as u64)
            .saturating_mul(DIRECTORY_SEGMENT_FRACTION)
            <= segment.length.get()
        {
            return stride;
        }
        stride *= 2;
    }
    POINT_DIRECTORY_STRIDE
}

#[cfg(test)]
thread_local! {
    static POINT_FRAMES_EXAMINED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static DIRECTORY_ENTRIES_EXAMINED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Frames examined by canonical point reads on this thread (tests only).
#[cfg(test)]
pub(crate) fn point_frames_examined() -> u64 {
    POINT_FRAMES_EXAMINED.with(std::cell::Cell::get)
}

/// Directory entries examined by canonical point reads on this thread
/// (tests only).
#[cfg(test)]
pub(crate) fn point_directory_entries_examined() -> u64 {
    DIRECTORY_ENTRIES_EXAMINED.with(std::cell::Cell::get)
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
        let directory = self.point_directory_for(segment, bytes)?;
        locate_point_frame(
            self.manifest.generation,
            segment,
            bytes,
            directory.as_deref(),
            id,
        )
    }

    /// The validated directory of a segment larger than one stride, or `None`
    /// for a smaller segment, whose reads walk its few frames directly.
    pub(super) fn point_directory_for(
        &self,
        segment: &CanonicalSegmentDescriptor,
        bytes: &[u8],
    ) -> Result<Option<SegmentBytes>, CanonicalSegmentError> {
        if segment.record_count > POINT_DIRECTORY_STRIDE {
            self.point_directory(segment, bytes).map(Some)
        } else {
            Ok(None)
        }
    }

    fn point_directory(
        &self,
        segment: &CanonicalSegmentDescriptor,
        bytes: &[u8],
    ) -> Result<SegmentBytes, CanonicalSegmentError> {
        let records = RecordValidation {
            property_keys: self.property_keys(),
            property_spill_count: self
                .property_spills
                .as_ref()
                .map(|spills| spills.manifest().value_count),
        };
        let Some((cache, store_id, manifest_generation)) = self.range_reader.cache_identity()
        else {
            return build_point_directory(bytes, self.manifest.generation, segment, records)
                .map(Into::into);
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
        let directory = build_point_directory(bytes, self.manifest.generation, segment, records)?;
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

/// Locates record `id` in verified segment `bytes`, starting from the
/// directory entry at or below `id` when the segment has a directory.
pub(super) fn locate_point_frame(
    generation: ManifestGeneration,
    segment: &CanonicalSegmentDescriptor,
    bytes: &[u8],
    directory: Option<&[u8]>,
    id: u64,
) -> Result<Option<SegmentRecordFrame>, CanonicalSegmentError> {
    let mut cursor = SegmentRecordCursor::new(bytes, generation, segment)?;
    if id < segment.min_record_id || id > segment.max_record_id {
        return Ok(None);
    }
    if (segment.record_count > POINT_DIRECTORY_STRIDE) != directory.is_some() {
        return Err(CanonicalSegmentError::Corrupt(format!(
            "canonical segment {} point directory does not match its record count",
            segment.segment_id
        )));
    }
    let mut expected_first = None;
    if let Some(directory) = directory {
        let (ordinal, offset, entry_id) = directory_seek(directory, segment, id)?;
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

/// What a directory build checks in every record of its segment.
#[derive(Clone, Copy)]
struct RecordValidation<'a> {
    property_keys: &'a [String],
    property_spill_count: Option<u64>,
}

/// Marks directories whose build validated every record of the segment.
const RECORDS_VALIDATED: u32 = 1 << 31;

/// Binds a directory to the exact verified segment extent, its stride and the
/// record validation its build performed. Every stride is a power of two up
/// to `POINT_DIRECTORY_STRIDE`, so it never collides with the flag bit.
fn point_directory_tag(segment: &CanonicalSegmentDescriptor) -> [u8; 32] {
    let mut tag = [0u8; 32];
    tag[..8].copy_from_slice(&segment.content_digest.0.to_le_bytes());
    tag[8..16].copy_from_slice(&segment.offset.to_le_bytes());
    tag[16..24].copy_from_slice(&segment.length.get().to_le_bytes());
    tag[24..28].copy_from_slice(&segment.record_count.to_le_bytes());
    tag[28..].copy_from_slice(&(directory_stride(segment) | RECORDS_VALIDATED).to_le_bytes());
    tag
}

fn directory_entries(segment: &CanonicalSegmentDescriptor, stride: u32) -> usize {
    segment.record_count.div_ceil(stride) as usize
}

/// Walks and validates the complete segment framing and every record once.
fn build_point_directory(
    bytes: &[u8],
    generation: ManifestGeneration,
    segment: &CanonicalSegmentDescriptor,
    records: RecordValidation<'_>,
) -> Result<Vec<u8>, CanonicalSegmentError> {
    let stride = directory_stride(segment);
    let length = directory_entries(segment, stride)
        .checked_mul(ENTRY_BYTES)
        .ok_or_else(|| CanonicalSegmentError::Corrupt("point directory size overflow".into()))?;
    let mut directory = Vec::new();
    directory.try_reserve_exact(length).map_err(|_| {
        CanonicalSegmentError::Source("canonical point directory allocation failed".into())
    })?;
    let mut cursor = SegmentRecordCursor::new(bytes, generation, segment)?;
    while let Some(frame) = cursor.next(bytes, segment)? {
        validate_canonical_record_payload(
            segment.kind,
            &bytes[frame.payload_start..frame.payload_end],
            records.property_keys,
            records.property_spill_count,
        )?;
        if cursor.index % stride == 0 {
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

/// Whether the segment's ids are exactly `min_record_id..=max_record_id`.
/// Record ids are strictly increasing, so `record_count` of them spanning
/// that many values leave no gap. Checkpoints assign dense ids, so this is
/// the common case.
fn dense_ids(segment: &CanonicalSegmentDescriptor) -> bool {
    segment
        .max_record_id
        .checked_sub(segment.min_record_id)
        .and_then(|span| span.checked_add(1))
        == Some(u64::from(segment.record_count))
}

/// Returns the ordinal, frame offset and id of the last directory entry whose
/// id does not exceed `id`. The caller has checked
/// `min_record_id <= id <= max_record_id`.
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
    let stride = directory_stride(segment);
    let entries = directory_entries(segment, stride);
    if directory.len() != entries.checked_mul(ENTRY_BYTES).ok_or_else(corrupt)? || entries == 0 {
        return Err(corrupt());
    }
    let entry = |index: usize| {
        #[cfg(test)]
        DIRECTORY_ENTRIES_EXAMINED.with(|entries| entries.set(entries.get() + 1));
        let start = index * ENTRY_BYTES;
        let id = u64::from_le_bytes(directory[start..start + 8].try_into().expect("fixed id"));
        let offset = u64::from_le_bytes(
            directory[start + 8..start + ENTRY_BYTES]
                .try_into()
                .expect("fixed offset"),
        );
        (id, offset)
    };
    let (index, (entry_id, offset)) = if dense_ids(segment) {
        // Record `id` is the `id - min_record_id`-th record, so its entry is
        // computed instead of searched. The entry must hold the id it covers,
        // and the caller checks that its frame does too.
        let ordinal = id.checked_sub(segment.min_record_id).ok_or_else(corrupt)?;
        let index = usize::try_from(ordinal / u64::from(stride)).map_err(|_| corrupt())?;
        if index >= entries {
            return Err(corrupt());
        }
        let found = entry(index);
        if Some(found.0) != id.checked_sub(ordinal % u64::from(stride)) {
            return Err(corrupt());
        }
        (index, found)
    } else {
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
        (index, entry(index))
    };
    let offset = usize::try_from(offset).map_err(|_| corrupt())?;
    let ordinal = u32::try_from(index)
        .ok()
        .and_then(|index| index.checked_mul(stride))
        .ok_or_else(corrupt)?;
    Ok((ordinal, offset, entry_id))
}
