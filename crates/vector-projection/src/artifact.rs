use crate::build::BuildState;
use crate::error::{ProjectionError, Result};
use crate::model::{
    ids_bytes, reconstruction_factor_bytes, ProjectionBuildConfig, ProjectionBuildReport,
    ProjectionManifest, QuantizedSegment, RaBitQBitWidth, SegmentDescriptor,
};
use crc32fast::Hasher;
use memmap2::Mmap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const FOOTER_MAGIC: &[u8; 8] = b"SKRQBF01";
const FOOTER_BYTES: u64 = 8 + 4 + FOOTER_MAGIC.len() as u64;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
        if target.exists() {
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
        fs::rename(&self.temporary, &self.target)?;
        sync_parent(&self.target)?;
        self.finished = true;
        FileProjection::open(&self.target)
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
            let _ = fs::remove_file(&self.temporary);
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileProjection {
    path: PathBuf,
    manifest: ProjectionManifest,
    mmap: Arc<Mmap>,
}

impl FileProjection {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_bytes = file.metadata()?.len();
        if file_bytes < FOOTER_BYTES {
            return Err(ProjectionError::CorruptArtifact(
                "artifact is shorter than the footer".to_string(),
            ));
        }
        file.seek(SeekFrom::End(-(FOOTER_BYTES as i64)))?;
        let manifest_bytes = read_u64(&mut file)?;
        let manifest_checksum = read_u32(&mut file)?;
        let mut magic = [0u8; FOOTER_MAGIC.len()];
        file.read_exact(&mut magic)?;
        if &magic != FOOTER_MAGIC {
            return Err(ProjectionError::CorruptArtifact(
                "footer magic mismatch".to_string(),
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
        file.seek(SeekFrom::Start(manifest_offset))?;
        let mut encoded_manifest = vec![0u8; manifest_len];
        file.read_exact(&mut encoded_manifest)?;
        if crc32fast::hash(&encoded_manifest) != manifest_checksum {
            return Err(ProjectionError::CorruptArtifact(
                "manifest checksum mismatch".to_string(),
            ));
        }
        let manifest: ProjectionManifest = serde_json::from_slice(&encoded_manifest)?;
        manifest.validate()?;
        if manifest.payload_bytes != manifest_offset {
            return Err(ProjectionError::CorruptArtifact(
                "manifest offset does not match payload length".to_string(),
            ));
        }

        // SAFETY: the artifact is published via atomic rename and never mutated in
        // place after that point, so external truncation/mutation racing this map is
        // not part of Skein's supported artifact lifecycle.
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });

        let mut previous_id = None;
        let mut payload_hasher = Hasher::new();
        for descriptor in &manifest.segments {
            let buffer = segment_buffer(&mmap, descriptor)?;
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
        Ok(Self {
            path,
            manifest,
            mmap,
        })
    }

    pub fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn build_report(&self) -> ProjectionBuildReport {
        ProjectionBuildReport {
            document_count: self.manifest.document_count,
            segment_count: self.manifest.segments.len(),
            raw_vector_bytes: (self.manifest.document_count as u64)
                .saturating_mul(self.manifest.dimension as u64)
                .saturating_mul(std::mem::size_of::<f32>() as u64),
            projection_payload_bytes: self.manifest.payload_bytes,
            configured_working_bytes: self.manifest.configured_build_working_bytes,
            peak_working_bytes: self.manifest.peak_build_working_bytes,
            requested_segment_rows: self.manifest.requested_segment_rows,
            admitted_segment_rows: self.manifest.admitted_segment_rows,
        }
    }

    /// Zero-copy view of a segment's payload backed by the projection's mmap.
    ///
    /// Unlike the previous per-call `File::open` + `seek` + `read_exact`, this
    /// borrows directly from the mapping opened once in `open()`: no syscall
    /// and no heap allocation/copy per search. The checksum is still verified
    /// on every call to preserve the existing fail-closed guarantee.
    pub(crate) fn read_segment(&self, index: usize) -> Result<SegmentBuffer<'_>> {
        let descriptor = self.manifest.segments.get(index).ok_or_else(|| {
            ProjectionError::CorruptArtifact(format!("missing segment descriptor {index}"))
        })?;
        segment_buffer(&self.mmap, descriptor)
    }
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
    if crc32fast::hash(bytes) != descriptor.payload_checksum {
        return Err(ProjectionError::CorruptArtifact(format!(
            "segment {} checksum mismatch",
            descriptor.index
        )));
    }
    Ok(SegmentBuffer { bytes })
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut bytes = [0u8; std::mem::size_of::<u64>()];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut bytes = [0u8; std::mem::size_of::<u32>()];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
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
        let artifact = root.join("search_rabitq.1.skein");
        let config = ProjectionBuildConfig::new(8, ProjectionIdentity::new(1)).with_segment_rows(2);
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

        assert_eq!(projection.manifest.document_count, 3);
        assert_eq!(projection.manifest.segments.len(), 2);
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
        let artifact = root.join("search_rabitq.1.skein");
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
    fn one_bit_generation_uses_one_bit_code_payloads() {
        let root = unique_test_dir("one-bit");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.skein");
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

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_vector_projection_{name}_{}_{nanos}",
            std::process::id()
        ))
    }
}
