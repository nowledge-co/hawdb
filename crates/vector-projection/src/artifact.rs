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

use crate::build::BuildState;
use crate::error::{ProjectionError, Result};
use crate::model::{
    ids_bytes, reconstruction_factor_bytes, ProjectionBuildConfig, ProjectionBuildReport,
    ProjectionManifest, QuantizedSegment, RaBitQBitWidth, SegmentDescriptor,
};
use crc32fast::Hasher;
use hawdb_core::SharedAllocation;
#[cfg(feature = "storage-io")]
use hawdb_storage::file_io::try_exists as target_exists;
#[cfg(feature = "storage-io")]
use hawdb_storage::file_io::{self as fs, File, OpenOptions};
use memmap2::Mmap;
#[cfg(not(feature = "storage-io"))]
use std::fs::exists as target_exists;
#[cfg(not(feature = "storage-io"))]
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

mod admission;
mod stream_verify;

#[doc(hidden)]
pub use admission::ProjectionReadAdmission;

const FOOTER_MAGIC: &[u8; 8] = b"SKRQBF01";
const FOOTER_BYTES: u64 = 8 + 4 + FOOTER_MAGIC.len() as u64;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static BEFORE_PROJECTION_MAP: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(all(test, feature = "storage-io"))]
thread_local! {
    static BEFORE_PROJECTION_OPEN: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
thread_local! {
    static SEGMENT_CHECKSUM_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
pub struct ProjectionWriter {
    target: PathBuf,
    temporary: PathBuf,
    file: Option<File>,
    state: BuildState,
    payload_hasher: Hasher,
    payload_bytes: u64,
    segments: Vec<SegmentDescriptor>,
    finished: bool,
}

impl ProjectionWriter {
    pub fn create(path: impl AsRef<Path>, config: ProjectionBuildConfig) -> Result<Self> {
        let target = path.as_ref().to_path_buf();
        let parent = target.parent().ok_or_else(|| {
            ProjectionError::InvalidConfiguration(
                "projection artifact path must have a parent".to_string(),
            )
        })?;
        fs::create_dir_all(parent)?;
        if target_exists(&target)? {
            return Err(ProjectionError::InvalidConfiguration(format!(
                "projection generation already exists at {}",
                target.display()
            )));
        }
        let state = BuildState::new(config.validated()?)?;
        let temporary = temporary_path(&target);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        Ok(Self {
            target,
            temporary,
            file: Some(file),
            state,
            payload_hasher: Hasher::new(),
            payload_bytes: 0,
            segments: Vec::new(),
            finished: false,
        })
    }

    pub fn push(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        self.state.push(id, vector)?;
        if self.state.pending_is_full() {
            self.flush_segment(true)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<FileProjection> {
        self.flush_segment(false)?;
        let payload_checksum = self.payload_hasher.clone().finalize();
        let manifest = ProjectionManifest::new(
            &self.state.config,
            self.state.document_count,
            self.state.source_digest,
            self.payload_bytes,
            payload_checksum,
            std::mem::take(&mut self.segments),
        );
        manifest.validate()?;
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        let manifest_checksum = crc32fast::hash(&manifest_bytes);
        let file = self.file.as_mut().expect("unfinished writer owns a file");
        file.write_all(&manifest_bytes)?;
        file.write_all(&(manifest_bytes.len() as u64).to_le_bytes())?;
        file.write_all(&manifest_checksum.to_le_bytes())?;
        file.write_all(FOOTER_MAGIC)?;
        file.sync_all()?;
        drop(self.file.take());
        // Admit and validate the complete mapping before publishing the name.
        // Keep capacity through rename and the directory barrier so another
        // project owner cannot exhaust this operation's remaining IO slots.
        #[cfg(feature = "storage-io")]
        let _publication_quota = {
            let project =
                hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_component(
                    self.temporary
                        .parent()
                        .expect("temporary artifact has a parent"),
                    false,
                )
                .map_err(|error| ProjectionError::Io(std::io::Error::other(error)))?;
            project.reserve_admission(2)?
        };
        let projection = FileProjection::open_named(&self.temporary, &self.target)?;
        fs::rename(&self.temporary, &self.target)?;
        sync_parent(&self.target)?;
        self.finished = true;
        Ok(projection)
    }

    fn flush_segment(&mut self, reserve_next_segment: bool) -> Result<()> {
        if self.state.pending.is_empty() {
            return Ok(());
        }
        let segment = self.state.take_pending();
        let offset = self.payload_bytes;
        let mut segment_hasher = Hasher::new();
        let file = self.file.as_mut().expect("unfinished writer owns a file");
        for id in &segment.ids {
            write_payload(
                file,
                &id.to_le_bytes(),
                &mut segment_hasher,
                &mut self.payload_hasher,
            )?;
        }
        for scale in &segment.reconstruction_scales {
            write_payload(
                file,
                &scale.to_bits().to_le_bytes(),
                &mut segment_hasher,
                &mut self.payload_hasher,
            )?;
        }
        for offset in &segment.reconstruction_offsets {
            write_payload(
                file,
                &offset.to_bits().to_le_bytes(),
                &mut segment_hasher,
                &mut self.payload_hasher,
            )?;
        }
        write_payload(
            file,
            &segment.codes,
            &mut segment_hasher,
            &mut self.payload_hasher,
        )?;
        let payload_bytes = segment_payload_len(&segment) as u64;
        self.segments.push(SegmentDescriptor {
            index: self.segments.len(),
            base_ordinal: segment.base_ordinal,
            row_count: segment.row_count(),
            payload_offset: offset,
            payload_bytes,
            payload_checksum: segment_hasher.finalize(),
        });
        self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes);
        drop(segment);
        if reserve_next_segment {
            self.state.reserve_pending();
        }
        Ok(())
    }
}

impl Drop for ProjectionWriter {
    fn drop(&mut self) {
        if !self.finished {
            drop(self.file.take());
            // Budget/OS exhaustion may defer best-effort removal. Retain the
            // unpublished temporary name for explicit cleanup; never bypass
            // descriptor admission or remove the final published generation.
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[derive(Debug)]
pub struct FileProjection<Memory = ()> {
    contents: SharedAllocation<ProjectionContents<Memory>>,
}

#[derive(Debug)]
struct ProjectionContents<Memory> {
    path: PathBuf,
    manifest: ProjectionManifest,
    mmap: Mmap,
    // Payloads and their shared allocation die before either capacity owner.
    _path_memory: Memory,
    _metadata_memory: Memory,
}

impl<Memory> Clone for FileProjection<Memory> {
    fn clone(&self) -> Self {
        Self {
            contents: self.contents.clone(),
        }
    }
}

impl FileProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        Self::open_named(path, path)
    }

    fn open_named(source: &Path, identity_path: &Path) -> Result<Self> {
        #[cfg(all(test, feature = "storage-io"))]
        if let Some(callback) = BEFORE_PROJECTION_OPEN.with(|slot| slot.replace(None)) {
            callback();
        }
        Self::open_admitted_file(File::open(source)?, identity_path, &admission::Unaccounted)
    }

    /// Constructs shared metadata under caller-supplied admission. The supplied
    /// handle must already have native admission and an immutable lifetime.
    /// `identity_path` is display metadata, not a source/binding certificate.
    #[doc(hidden)]
    pub fn open_admitted_file<A: ProjectionReadAdmission>(
        input: File,
        identity_path: &Path,
        admission: &A,
    ) -> Result<FileProjection<A::Retained>> {
        admission.checkpoint()?;
        let fixed = admission::add(
            stream_verify::BUFFER_BYTES + 4096 + FOOTER_BYTES as usize,
            std::mem::size_of::<ProjectionContents<A::Retained>>(),
        )?;
        let _open_scratch = admission.reserve_scratch(fixed)?;
        let path_bytes = admission::mul(identity_path.as_os_str().as_encoded_bytes().len(), 2)?;
        let allocation = admission::add(
            std::mem::size_of::<ProjectionContents<A::Retained>>(),
            admission::add(
                2 * std::mem::size_of::<usize>(),
                std::mem::align_of::<ProjectionContents<A::Retained>>(),
            )?,
        )?;
        let path_memory = admission.reserve_retained(admission::add(path_bytes, allocation)?)?;
        let path = identity_path.to_path_buf();
        let mut file = input;
        let file_bytes = {
            let _io = admission.acquire_io()?;
            file.metadata()?.len()
        };
        if file_bytes < FOOTER_BYTES {
            return Err(ProjectionError::CorruptArtifact(
                "artifact is shorter than the footer".into(),
            ));
        }
        {
            let _io = admission.acquire_io()?;
            file.seek(SeekFrom::End(-(FOOTER_BYTES as i64)))?;
        }
        let mut footer = [0u8; FOOTER_BYTES as usize];
        admission::read_exact(&mut file, &mut footer, admission)?;
        let manifest_bytes =
            u64::from_le_bytes(footer[..8].try_into().expect("fixed footer length"));
        let manifest_checksum =
            u32::from_le_bytes(footer[8..12].try_into().expect("fixed footer length"));
        if &footer[12..] != FOOTER_MAGIC {
            return Err(ProjectionError::CorruptArtifact(
                "footer magic mismatch".into(),
            ));
        }
        let manifest_offset = file_bytes
            .checked_sub(FOOTER_BYTES)
            .and_then(|offset| offset.checked_sub(manifest_bytes))
            .ok_or_else(|| {
                ProjectionError::CorruptArtifact("manifest length exceeds artifact".to_string())
            })?;
        let manifest_len = usize::try_from(manifest_bytes).map_err(|_| {
            ProjectionError::CorruptArtifact("manifest length exceeds address space".to_string())
        })?;
        let _encoded_memory = admission.reserve_scratch(manifest_len)?;
        let mut encoded_manifest = vec![0u8; manifest_len];
        {
            let _io = admission.acquire_io()?;
            file.seek(SeekFrom::Start(manifest_offset))?;
        }
        admission::read_exact(&mut file, &mut encoded_manifest, admission)?;
        if crc32fast::hash(&encoded_manifest) != manifest_checksum {
            return Err(ProjectionError::CorruptArtifact(
                "manifest checksum mismatch".to_string(),
            ));
        }
        let plan = admission::DecodePlan::inspect(&encoded_manifest, admission)?;
        let metadata_memory = admission.reserve_retained(plan.retained)?;
        let _decode_scratch = admission.reserve_scratch(plan.scratch)?;
        let manifest: ProjectionManifest = serde_json::from_slice(&encoded_manifest)?;
        admission.checkpoint()?;
        manifest.validate()?;
        if manifest.payload_bytes != manifest_offset {
            return Err(ProjectionError::CorruptArtifact(
                "manifest offset does not match payload length".to_string(),
            ));
        }

        // Validate the held file before mapping it. Opening a reader must not
        // fault the complete payload into its mapping just to establish integrity.
        stream_verify::verify_admitted(&mut file, &manifest, admission)?;

        // SAFETY: the artifact is published via atomic rename and never mutated in
        // place after that point, so external truncation/mutation racing this map is
        // not part of HawDB's supported artifact lifecycle.
        #[cfg(test)]
        if let Some(callback) = BEFORE_PROJECTION_MAP.with(|slot| slot.replace(None)) {
            callback();
        }
        let mmap = {
            admission.checkpoint()?;
            let _io = admission.acquire_io()?;
            #[cfg(feature = "storage-io")]
            let mapped = unsafe { file.map_read_only()? };
            #[cfg(not(feature = "storage-io"))]
            let mapped = unsafe { Mmap::map(&file)? };
            mapped
        };

        Ok(FileProjection {
            contents: SharedAllocation::new(ProjectionContents {
                path,
                manifest,
                mmap,
                _path_memory: path_memory,
                _metadata_memory: metadata_memory,
            }),
        })
    }
}

impl<Memory> FileProjection<Memory> {
    pub fn manifest(&self) -> &ProjectionManifest {
        &self.contents.manifest
    }

    pub fn path(&self) -> &Path {
        &self.contents.path
    }

    pub fn build_report(&self) -> ProjectionBuildReport {
        ProjectionBuildReport {
            document_count: self.contents.manifest.document_count,
            segment_count: self.contents.manifest.segments.len(),
            raw_vector_bytes: (self.contents.manifest.document_count as u64)
                .saturating_mul(self.contents.manifest.dimension as u64)
                .saturating_mul(std::mem::size_of::<f32>() as u64),
            projection_payload_bytes: self.contents.manifest.payload_bytes,
            configured_working_bytes: self.contents.manifest.configured_build_working_bytes,
            peak_working_bytes: self.contents.manifest.peak_build_working_bytes,
            requested_segment_rows: self.contents.manifest.requested_segment_rows,
            admitted_segment_rows: self.contents.manifest.admitted_segment_rows,
        }
    }

    /// Revalidates the mapped payload against the manifest and segment checksums.
    ///
    /// Normal searches trust the immutable artifact established by [`Self::open`]
    /// and avoid checksum work on the query hot path. Scrub and deep-verification
    /// callers can use this method to detect payload mutation after open.
    pub fn verify(&self) -> Result<()> {
        verify_projection_payload(&self.contents.mmap, &self.contents.manifest)
    }

    /// Zero-copy view of a segment's payload backed by the projection's mmap.
    ///
    /// Unlike the previous per-call `File::open` + `seek` + `read_exact`, this
    /// borrows directly from the mapping opened and fully validated once in
    /// `open()`: no syscall, checksum pass, or heap allocation/copy per search.
    pub(crate) fn read_segment(&self, index: usize) -> Result<SegmentBuffer<'_>> {
        let descriptor = self.contents.manifest.segments.get(index).ok_or_else(|| {
            ProjectionError::CorruptArtifact(format!("missing segment descriptor {index}"))
        })?;
        segment_buffer(&self.contents.mmap, descriptor)
    }
}

fn verify_projection_payload(mmap: &Mmap, manifest: &ProjectionManifest) -> Result<()> {
    let mut previous_id = None;
    let mut payload_hasher = Hasher::new();
    for descriptor in &manifest.segments {
        let buffer = verified_segment_buffer(mmap, descriptor)?;
        payload_hasher.update(buffer.bytes);
        let parts = buffer.parts(
            manifest.dimension,
            RaBitQBitWidth::from_bits(manifest.bit_width)?,
            descriptor.row_count,
        )?;
        for row in 0..descriptor.row_count {
            let id = parts.id(row);
            if let Some(previous) = previous_id
                && id <= previous
            {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "ids are not strictly increasing at {id} after {previous}"
                )));
            }
            previous_id = Some(id);
            let scale = parts.reconstruction_scale(row);
            if !scale.is_finite() || scale < 0.0 {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "invalid RaBitQ reconstruction scale in segment {} row {row}",
                    descriptor.index
                )));
            }
            if !parts.reconstruction_offset(row).is_finite() {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "invalid RaBitQ reconstruction offset in segment {} row {row}",
                    descriptor.index
                )));
            }
        }
    }
    if payload_hasher.finalize() != manifest.payload_checksum {
        return Err(ProjectionError::CorruptArtifact(
            "payload checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentBuffer<'a> {
    pub bytes: &'a [u8],
}

impl<'a> SegmentBuffer<'a> {
    pub fn parts(
        &self,
        dimension: usize,
        bit_width: RaBitQBitWidth,
        rows: usize,
    ) -> Result<SegmentParts<'a>> {
        let ids_end = ids_bytes(rows);
        let scales_end = ids_end.saturating_add(reconstruction_factor_bytes(rows));
        let offsets_end = scales_end.saturating_add(reconstruction_factor_bytes(rows));
        if offsets_end > self.bytes.len() {
            return Err(ProjectionError::CorruptArtifact(
                "segment header arrays exceed payload".to_string(),
            ));
        }
        let codes = &self.bytes[offsets_end..];
        let expected_codes =
            rows.saturating_mul(crate::model::encoded_vector_bytes(dimension, bit_width));
        if codes.len() != expected_codes {
            return Err(ProjectionError::CorruptArtifact(format!(
                "segment codes have {} bytes, expected {expected_codes}",
                codes.len()
            )));
        }
        Ok(SegmentParts {
            ids: &self.bytes[..ids_end],
            reconstruction_scales: &self.bytes[ids_end..scales_end],
            reconstruction_offsets: &self.bytes[scales_end..offsets_end],
            codes,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentParts<'a> {
    pub ids: &'a [u8],
    pub reconstruction_scales: &'a [u8],
    pub reconstruction_offsets: &'a [u8],
    pub codes: &'a [u8],
}

impl SegmentParts<'_> {
    pub fn id(self, row: usize) -> u64 {
        let offset = row * std::mem::size_of::<u64>();
        u64::from_le_bytes(
            self.ids[offset..offset + std::mem::size_of::<u64>()]
                .try_into()
                .expect("validated id slice"),
        )
    }

    pub fn reconstruction_scale(self, row: usize) -> f32 {
        let offset = row * std::mem::size_of::<f32>();
        f32::from_bits(u32::from_le_bytes(
            self.reconstruction_scales[offset..offset + std::mem::size_of::<f32>()]
                .try_into()
                .expect("validated scale slice"),
        ))
    }

    pub fn reconstruction_offset(self, row: usize) -> f32 {
        let offset = row * std::mem::size_of::<f32>();
        f32::from_bits(u32::from_le_bytes(
            self.reconstruction_offsets[offset..offset + std::mem::size_of::<f32>()]
                .try_into()
                .expect("validated offset slice"),
        ))
    }
}

fn segment_payload_len(segment: &QuantizedSegment) -> usize {
    segment.ids.len() * std::mem::size_of::<u64>()
        + segment.reconstruction_scales.len() * std::mem::size_of::<f32>()
        + segment.reconstruction_offsets.len() * std::mem::size_of::<f32>()
        + segment.codes.len()
}

fn write_payload(
    file: &mut File,
    bytes: &[u8],
    segment_hasher: &mut Hasher,
    payload_hasher: &mut Hasher,
) -> Result<()> {
    file.write_all(bytes)?;
    segment_hasher.update(bytes);
    payload_hasher.update(bytes);
    Ok(())
}

fn segment_buffer<'a>(mmap: &'a Mmap, descriptor: &SegmentDescriptor) -> Result<SegmentBuffer<'a>> {
    let offset = usize::try_from(descriptor.payload_offset).map_err(|_| {
        ProjectionError::CorruptArtifact(format!(
            "segment {} payload offset exceeds address space",
            descriptor.index
        ))
    })?;
    let payload_len = usize::try_from(descriptor.payload_bytes).map_err(|_| {
        ProjectionError::CorruptArtifact(format!(
            "segment {} payload exceeds address space",
            descriptor.index
        ))
    })?;
    let end = offset.checked_add(payload_len).ok_or_else(|| {
        ProjectionError::CorruptArtifact(format!(
            "segment {} payload range overflows",
            descriptor.index
        ))
    })?;
    let bytes = mmap.get(offset..end).ok_or_else(|| {
        ProjectionError::CorruptArtifact(format!(
            "segment {} payload range exceeds mapped artifact",
            descriptor.index
        ))
    })?;
    Ok(SegmentBuffer { bytes })
}

fn verified_segment_buffer<'a>(
    mmap: &'a Mmap,
    descriptor: &SegmentDescriptor,
) -> Result<SegmentBuffer<'a>> {
    let buffer = segment_buffer(mmap, descriptor)?;
    if segment_checksum(buffer.bytes) != descriptor.payload_checksum {
        return Err(ProjectionError::CorruptArtifact(format!(
            "segment {} checksum mismatch",
            descriptor.index
        )));
    }
    Ok(buffer)
}

fn segment_checksum(bytes: &[u8]) -> u32 {
    #[cfg(test)]
    SEGMENT_CHECKSUM_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
    crc32fast::hash(bytes)
}

#[cfg(test)]
fn reset_segment_checksum_calls() {
    SEGMENT_CHECKSUM_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn segment_checksum_calls() -> usize {
    SEGMENT_CHECKSUM_CALLS.with(std::cell::Cell::get)
}

fn temporary_path(target: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    target.with_extension(format!("tmp.{}.{}", std::process::id(), sequence))
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ProjectionIdentity, DEFAULT_BUILD_MEMORY_BYTES};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generation_artifact_round_trips_and_validates_checksums() {
        let root = unique_test_dir("roundtrip");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1))
            .with_bit_width(RaBitQBitWidth::One)
            .with_segment_rows(2);
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        writer
            .push(10, &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .unwrap();
        writer
            .push(20, &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .unwrap();
        writer
            .push(30, &[0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0])
            .unwrap();
        let projection = writer.finish().unwrap();

        assert_eq!(projection.manifest().document_count, 3);
        assert_eq!(projection.manifest().segments.len(), 2);
        let build_report = projection.build_report();
        assert_eq!(
            build_report.configured_working_bytes,
            DEFAULT_BUILD_MEMORY_BYTES
        );
        assert!(build_report.peak_working_bytes > 0);
        assert!(build_report.peak_working_bytes <= build_report.configured_working_bytes);
        assert_eq!(build_report.raw_vector_bytes, 3 * 8 * 4);
        let first = projection.read_segment(0).unwrap();
        let first = first.parts(8, RaBitQBitWidth::One, 2).unwrap();
        assert_eq!([first.id(0), first.id(1)], [10, 20]);
        let second = projection.read_segment(1).unwrap();
        let second = second.parts(8, RaBitQBitWidth::One, 1).unwrap();
        assert_eq!(second.id(0), 30);
        FileProjection::open(&artifact).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupted_segment_fails_closed() {
        let root = unique_test_dir("corruption");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1));
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        writer.push(10, &[1.0; 8]).unwrap();
        writer.finish().unwrap();

        let mut file = OpenOptions::new().write(true).open(&artifact).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        let error = FileProjection::open(&artifact).unwrap_err();
        assert!(matches!(error, ProjectionError::CorruptArtifact(_)));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cloned_file_projection_shares_manifest_and_path_ownership() {
        let root = unique_test_dir("shared-projection-metadata");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let mut writer = ProjectionWriter::create(
            &artifact,
            ProjectionBuildConfig::new(8, ProjectionIdentity::new(1)),
        )
        .unwrap();
        writer.push(10, &[1.0; 8]).unwrap();
        let original = writer.finish().unwrap();
        let cloned = original.clone();
        assert!(std::ptr::eq(original.manifest(), cloned.manifest()));
        assert!(std::ptr::eq(original.path(), cloned.path()));
        drop(original);
        assert_eq!(cloned.manifest().document_count, 1);
        assert_eq!(
            cloned
                .read_segment(0)
                .unwrap()
                .parts(8, RaBitQBitWidth::Four, 1)
                .unwrap()
                .id(0),
            10
        );
        drop(cloned);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_payload_is_rejected_before_mapping() {
        let root = unique_test_dir("reject-before-map");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let mut writer = ProjectionWriter::create(
            &artifact,
            ProjectionBuildConfig::new(8, ProjectionIdentity::new(1)),
        )
        .unwrap();
        writer.push(10, &[1.0; 8]).unwrap();
        writer.finish().unwrap();
        let mut file = OpenOptions::new().write(true).open(&artifact).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();
        let mapped = std::rc::Rc::new(std::cell::Cell::new(false));
        let observed = mapped.clone();
        BEFORE_PROJECTION_MAP.with(|slot| {
            slot.replace(Some(Box::new(move || observed.set(true))));
        });
        let result = FileProjection::open(&artifact);
        BEFORE_PROJECTION_MAP.with(|slot| slot.replace(None));
        let corrupt = matches!(&result, Err(ProjectionError::CorruptArtifact(_)));
        drop(result);
        drop(file);
        fs::remove_dir_all(root).unwrap();
        assert!(corrupt);
        assert!(
            !mapped.get(),
            "corrupt payload reached mmap before validation"
        );
    }

    #[test]
    fn streamed_payload_verification_matches_mapped_corruption_oracle() {
        for bit_width in [RaBitQBitWidth::One, RaBitQBitWidth::Four] {
            let root = unique_test_dir("streamed-verification-oracle");
            fs::create_dir_all(&root).unwrap();
            let artifact = root.join("search_rabitq.1.hawdb");
            let mut writer = ProjectionWriter::create(
                &artifact,
                ProjectionBuildConfig::new(8, ProjectionIdentity::new(1))
                    .with_bit_width(bit_width)
                    .with_segment_rows(3),
            )
            .unwrap();
            for id in [10, 20, 30, 40, 50, 60] {
                writer.push(id, &[1.0; 8]).unwrap();
            }
            let projection = writer.finish().unwrap();
            let original_manifest = projection.manifest().clone();
            assert_eq!(original_manifest.segments.len(), 2);
            drop(projection);
            let mut original = fs::read(&artifact).unwrap();
            original.truncate(original_manifest.payload_bytes as usize);
            for case in [
                "valid",
                "duplicate",
                "cross-segment",
                "negative-scale",
                "nan-scale",
                "infinite-offset",
                "row-order-scale-first",
                "row-order-offset-first",
                "field-order-id-first",
                "bad-segment-checksum",
                "bad-total-checksum",
                "bad-header-and-checksum",
                "negative-zero-scale",
            ] {
                let mut bytes = original.clone();
                let mut manifest = original_manifest.clone();
                let second = manifest.segments[1].payload_offset as usize;
                match case {
                    "duplicate" => bytes[8..16].copy_from_slice(&10_u64.to_le_bytes()),
                    "cross-segment" => {
                        bytes[second..second + 8].copy_from_slice(&30_u64.to_le_bytes())
                    }
                    "negative-scale" => bytes[24..28].copy_from_slice(&(-1_f32).to_le_bytes()),
                    "nan-scale" | "bad-header-and-checksum" => {
                        bytes[24..28].copy_from_slice(&f32::NAN.to_le_bytes())
                    }
                    "infinite-offset" => {
                        bytes[36..40].copy_from_slice(&f32::INFINITY.to_le_bytes())
                    }
                    "row-order-scale-first" => {
                        bytes[8..16].copy_from_slice(&10_u64.to_le_bytes());
                        bytes[24..28].copy_from_slice(&f32::NAN.to_le_bytes());
                    }
                    "row-order-offset-first" => {
                        bytes[28..32].copy_from_slice(&f32::NAN.to_le_bytes());
                        bytes[36..40].copy_from_slice(&f32::INFINITY.to_le_bytes());
                    }
                    "field-order-id-first" => {
                        bytes[second..second + 8].copy_from_slice(&30_u64.to_le_bytes());
                        bytes[second + 24..second + 28].copy_from_slice(&f32::NAN.to_le_bytes());
                    }
                    "negative-zero-scale" => bytes[24..28].copy_from_slice(&(-0_f32).to_le_bytes()),
                    "valid" | "bad-segment-checksum" | "bad-total-checksum" => {}
                    _ => unreachable!(),
                }
                if case != "bad-header-and-checksum" {
                    for segment in &mut manifest.segments {
                        let start = segment.payload_offset as usize;
                        let end = start + segment.payload_bytes as usize;
                        segment.payload_checksum = crc32fast::hash(&bytes[start..end]);
                    }
                    manifest.payload_checksum = crc32fast::hash(&bytes);
                }
                if case == "bad-segment-checksum" {
                    manifest.segments[0].payload_checksum ^= 1;
                } else if case == "bad-total-checksum" {
                    manifest.payload_checksum ^= 1;
                }
                manifest.validate().unwrap();
                let mut mapped = memmap2::MmapMut::map_anon(bytes.len()).unwrap();
                mapped.copy_from_slice(&bytes);
                let mapped = mapped.make_read_only().unwrap();
                let expected =
                    verify_projection_payload(&mapped, &manifest).map_err(|e| e.to_string());
                let actual = stream_verify::verify(&mut std::io::Cursor::new(&bytes), &manifest)
                    .map_err(|e| e.to_string());
                assert_eq!(actual, expected, "{bit_width:?} {case}");
                if matches!(case, "valid" | "negative-zero-scale") {
                    assert!(actual.is_ok());
                } else {
                    assert!(
                        actual.is_err(),
                        "the oracle must exercise rejection: {case}"
                    );
                }
            }
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn streamed_payload_verification_bounds_reads_and_handles_short_reads() {
        struct ShortReader<'a> {
            cursor: std::io::Cursor<&'a [u8]>,
            max_request: usize,
            seeks: usize,
        }
        impl Read for ShortReader<'_> {
            fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
                self.max_request = self.max_request.max(bytes.len());
                assert!(bytes.len() <= stream_verify::BUFFER_BYTES);
                let length = bytes.len().min(3);
                self.cursor.read(&mut bytes[..length])
            }
        }
        impl Seek for ShortReader<'_> {
            fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
                self.seeks += 1;
                self.cursor.seek(position)
            }
        }
        let root = unique_test_dir("streamed-bounded-reads");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let mut writer = ProjectionWriter::create(
            &artifact,
            ProjectionBuildConfig::new(8, ProjectionIdentity::new(1)).with_segment_rows(2200),
        )
        .unwrap();
        for id in 1..=2200 {
            writer.push(id, &[1.0; 8]).unwrap();
        }
        let projection = writer.finish().unwrap();
        let bytes = fs::read(&artifact).unwrap();
        let mut reader = ShortReader {
            cursor: std::io::Cursor::new(&bytes),
            max_request: 0,
            seeks: 0,
        };
        stream_verify::verify(&mut reader, projection.manifest()).unwrap();
        assert_eq!(reader.max_request, stream_verify::BUFFER_BYTES);
        assert_eq!(reader.seeks, 1);
        assert_eq!(
            reader.cursor.position(),
            projection.manifest().payload_bytes
        );
        assert!(reader.cursor.position() < bytes.len() as u64);
        drop(projection);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn repeated_segment_reads_skip_crc_but_explicit_verify_detects_mutation() {
        let root = unique_test_dir("steady-state-checksum");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1));
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        writer.push(10, &[1.0; 8]).unwrap();
        let projection = writer.finish().unwrap();

        reset_segment_checksum_calls();
        projection.read_segment(0).unwrap();
        projection.read_segment(0).unwrap();
        assert_eq!(segment_checksum_calls(), 0);

        let descriptor = &projection.manifest().segments[0];
        let original = projection.read_segment(0).unwrap().bytes[0];
        let mut file = OpenOptions::new().write(true).open(&artifact).unwrap();
        file.seek(SeekFrom::Start(descriptor.payload_offset))
            .unwrap();
        file.write_all(&[original ^ 0xff]).unwrap();
        file.sync_all().unwrap();

        let error = projection.verify().unwrap_err();
        assert!(matches!(error, ProjectionError::CorruptArtifact(_)));
        assert_eq!(segment_checksum_calls(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn one_bit_generation_uses_one_bit_code_payloads() {
        let root = unique_test_dir("one-bit");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.hawdb");
        let config = ProjectionBuildConfig::new(9, ProjectionIdentity::new(1))
            .with_bit_width(RaBitQBitWidth::One)
            .with_segment_rows(2);
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        writer.push(10, &[1.0; 9]).unwrap();
        writer.push(20, &[-1.0; 9]).unwrap();
        writer.push(30, &[0.5; 9]).unwrap();
        let projection = writer.finish().unwrap();

        assert_eq!(projection.manifest().bit_width, 1);
        assert_eq!(
            projection.manifest().quantizer,
            "rabitq_sign_then_refinement_scalar_1bit_v1"
        );
        assert_eq!(
            projection.manifest().segments[0].payload_bytes,
            2 * (8 + 8 + 2)
        );
        let first = projection.read_segment(0).unwrap();
        let first = first.parts(9, RaBitQBitWidth::One, 2).unwrap();
        assert_eq!([first.id(0), first.id(1)], [10, 20]);
        FileProjection::open(&artifact).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    pub(super) fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb_vector_projection_{name}_{}_{nanos}",
            std::process::id()
        ))
    }
}

#[cfg(all(test, feature = "storage-io"))]
mod descriptor_tests;
