use crate::error::{ProjectionError, Result};
use serde::{Deserialize, Serialize};

pub const PROJECTION_PROTOCOL: &str = "skein-rabitq-projection";
pub const PROJECTION_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_PROJECTION_BIT_WIDTH: u8 = 1;
pub const PROJECTION_BIT_WIDTH: u8 = DEFAULT_PROJECTION_BIT_WIDTH;
pub const PROJECTION_ALGORITHM: &str = "rabitq";
pub const PROJECTION_TRANSFORM: &str = "signed_block_hadamard_v1";
pub const PROJECTION_QUANTIZER: &str = "rabitq_sign_then_refinement_scalar_1bit_v1";
const FOUR_BIT_PROJECTION_QUANTIZER: &str = "rabitq_sign_then_refinement_scalar_4bit_v1";
pub const PROJECTION_CALIBRATION: &str = "none";
pub const DEFAULT_TRANSFORM_SEED: u64 = 0x534b_4549_4e56_5134;
pub const DEFAULT_SEGMENT_ROWS: usize = 1_024;
pub const DEFAULT_BUILD_MEMORY_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const BUILD_FIXED_WORKING_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum RaBitQBitWidth {
    #[default]
    One = 1,
    Four = 4,
}

impl RaBitQBitWidth {
    pub const fn bits(self) -> u8 {
        self as u8
    }

    pub const fn quantizer(self) -> &'static str {
        match self {
            Self::One => PROJECTION_QUANTIZER,
            Self::Four => FOUR_BIT_PROJECTION_QUANTIZER,
        }
    }

    pub(crate) const fn sign_code_bytes(self, dimension: usize) -> usize {
        let _ = self;
        dimension.div_ceil(u8::BITS as usize)
    }

    pub(crate) const fn refinement_bits(self) -> usize {
        self.bits() as usize - 1
    }

    pub(crate) const fn refinement_code_bytes(self, dimension: usize) -> usize {
        dimension
            .saturating_mul(self.refinement_bits())
            .div_ceil(u8::BITS as usize)
    }

    pub(crate) fn from_bits(bits: u8) -> Result<Self> {
        match bits {
            1 => Ok(Self::One),
            4 => Ok(Self::Four),
            _ => Err(ProjectionError::CorruptArtifact(format!(
                "unsupported RaBitQ bit width {bits}"
            ))),
        }
    }
}

pub(crate) fn encoded_vector_bytes(dimension: usize, bit_width: RaBitQBitWidth) -> usize {
    bit_width
        .sign_code_bytes(dimension)
        .saturating_add(bit_width.refinement_code_bytes(dimension))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionMetric {
    Cosine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionIdentity {
    pub generation: u64,
    pub source_epoch: Option<u64>,
    pub embedding_model: Option<String>,
    pub embedding_version: Option<String>,
}

impl ProjectionIdentity {
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            source_epoch: None,
            embedding_model: None,
            embedding_version: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBuildConfig {
    pub dimension: usize,
    pub bit_width: RaBitQBitWidth,
    pub segment_rows: usize,
    pub max_working_bytes: usize,
    pub transform_seed: u64,
    pub identity: ProjectionIdentity,
}

impl ProjectionBuildConfig {
    pub fn new(dimension: usize, identity: ProjectionIdentity) -> Self {
        Self {
            dimension,
            bit_width: RaBitQBitWidth::default(),
            segment_rows: DEFAULT_SEGMENT_ROWS,
            max_working_bytes: DEFAULT_BUILD_MEMORY_BYTES,
            transform_seed: DEFAULT_TRANSFORM_SEED,
            identity,
        }
    }

    pub fn with_segment_rows(mut self, segment_rows: usize) -> Self {
        self.segment_rows = segment_rows;
        self
    }

    pub fn with_bit_width(mut self, bit_width: RaBitQBitWidth) -> Self {
        self.bit_width = bit_width;
        self
    }

    pub fn with_max_working_bytes(mut self, max_working_bytes: usize) -> Self {
        self.max_working_bytes = max_working_bytes;
        self
    }

    pub fn with_transform_seed(mut self, transform_seed: u64) -> Self {
        self.transform_seed = transform_seed;
        self
    }

    pub fn resource_admission(&self) -> Result<ProjectionBuildAdmission> {
        let validated = self.validated()?;
        Ok(ProjectionBuildAdmission {
            configured_working_bytes: validated.max_working_bytes,
            peak_working_bytes: validated.peak_working_bytes,
            requested_segment_rows: validated.requested_segment_rows,
            admitted_segment_rows: validated.admitted_segment_rows,
        })
    }

    pub(crate) fn validated(&self) -> Result<ValidatedBuildConfig> {
        if self.dimension == 0 {
            return Err(ProjectionError::InvalidConfiguration(
                "dimension must be greater than zero".to_string(),
            ));
        }
        if self.segment_rows == 0 {
            return Err(ProjectionError::InvalidConfiguration(
                "segment_rows must be greater than zero".to_string(),
            ));
        }
        let bytes_per_vector = encoded_vector_bytes(self.dimension, self.bit_width);
        let row_bytes = std::mem::size_of::<u64>()
            .saturating_add(2 * std::mem::size_of::<f32>())
            .saturating_add(bytes_per_vector);
        let scratch_bytes = self
            .dimension
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(bytes_per_vector)
            .saturating_add(BUILD_FIXED_WORKING_BYTES);
        let available_for_rows = self.max_working_bytes.saturating_sub(scratch_bytes);
        let admitted_rows = (available_for_rows / row_bytes).min(self.segment_rows);
        if admitted_rows == 0 {
            return Err(ProjectionError::ResourceBudgetExceeded {
                required: scratch_bytes.saturating_add(row_bytes),
                available: self.max_working_bytes,
            });
        }
        let peak_working_bytes = scratch_bytes.saturating_add(admitted_rows * row_bytes);
        Ok(ValidatedBuildConfig {
            dimension: self.dimension,
            requested_segment_rows: self.segment_rows,
            admitted_segment_rows: admitted_rows,
            max_working_bytes: self.max_working_bytes,
            peak_working_bytes,
            bytes_per_vector,
            bit_width: self.bit_width,
            transform_seed: self.transform_seed,
            identity: self.identity.clone(),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProjectionBuildAdmission {
    pub configured_working_bytes: usize,
    pub peak_working_bytes: usize,
    pub requested_segment_rows: usize,
    pub admitted_segment_rows: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ValidatedBuildConfig {
    pub dimension: usize,
    pub bit_width: RaBitQBitWidth,
    pub requested_segment_rows: usize,
    pub admitted_segment_rows: usize,
    pub max_working_bytes: usize,
    pub peak_working_bytes: usize,
    pub bytes_per_vector: usize,
    pub transform_seed: u64,
    pub identity: ProjectionIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub index: usize,
    pub base_ordinal: usize,
    pub row_count: usize,
    pub payload_offset: u64,
    pub payload_bytes: u64,
    pub payload_checksum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionManifest {
    pub protocol: String,
    pub format_version: u32,
    pub bit_width: u8,
    pub algorithm: String,
    pub metric: ProjectionMetric,
    pub transform: String,
    pub quantizer: String,
    pub calibration: String,
    pub transform_seed: u64,
    pub dimension: usize,
    pub requested_segment_rows: usize,
    pub admitted_segment_rows: usize,
    pub configured_build_working_bytes: usize,
    pub peak_build_working_bytes: usize,
    pub document_count: usize,
    pub source_digest: u64,
    pub payload_bytes: u64,
    pub payload_checksum: u32,
    pub identity: ProjectionIdentity,
    pub segments: Vec<SegmentDescriptor>,
}

impl ProjectionManifest {
    pub(crate) fn new(
        config: &ValidatedBuildConfig,
        document_count: usize,
        source_digest: u64,
        payload_bytes: u64,
        payload_checksum: u32,
        segments: Vec<SegmentDescriptor>,
    ) -> Self {
        Self {
            protocol: PROJECTION_PROTOCOL.to_string(),
            format_version: PROJECTION_FORMAT_VERSION,
            bit_width: config.bit_width.bits(),
            algorithm: PROJECTION_ALGORITHM.to_string(),
            metric: ProjectionMetric::Cosine,
            transform: PROJECTION_TRANSFORM.to_string(),
            quantizer: config.bit_width.quantizer().to_string(),
            calibration: PROJECTION_CALIBRATION.to_string(),
            transform_seed: config.transform_seed,
            dimension: config.dimension,
            requested_segment_rows: config.requested_segment_rows,
            admitted_segment_rows: config.admitted_segment_rows,
            configured_build_working_bytes: config.max_working_bytes,
            peak_build_working_bytes: config.peak_working_bytes,
            document_count,
            source_digest,
            payload_bytes,
            payload_checksum,
            identity: config.identity.clone(),
            segments,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.protocol != PROJECTION_PROTOCOL {
            return Err(ProjectionError::CorruptArtifact(format!(
                "unsupported protocol {}",
                self.protocol
            )));
        }
        if self.format_version != PROJECTION_FORMAT_VERSION {
            return Err(ProjectionError::CorruptArtifact(format!(
                "unsupported format version {}",
                self.format_version
            )));
        }
        let bit_width = RaBitQBitWidth::from_bits(self.bit_width)?;
        if self.algorithm != PROJECTION_ALGORITHM
            || self.metric != ProjectionMetric::Cosine
            || self.transform != PROJECTION_TRANSFORM
            || self.quantizer != bit_width.quantizer()
            || self.calibration != PROJECTION_CALIBRATION
        {
            return Err(ProjectionError::CorruptArtifact(
                "unsupported algorithm, metric, transform, quantizer, or calibration".to_string(),
            ));
        }
        if self.dimension == 0
            || self.admitted_segment_rows == 0
            || self.peak_build_working_bytes == 0
            || self.peak_build_working_bytes > self.configured_build_working_bytes
        {
            return Err(ProjectionError::CorruptArtifact(
                "dimension, segment rows, or build resource admission is invalid".to_string(),
            ));
        }

        let mut expected_offset = 0u64;
        let mut expected_ordinal = 0usize;
        for (index, segment) in self.segments.iter().enumerate() {
            if segment.index != index
                || segment.base_ordinal != expected_ordinal
                || segment.payload_offset != expected_offset
                || segment.row_count == 0
                || segment.row_count > self.admitted_segment_rows
            {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "segment {} has an invalid index, ordinal, offset, or row count",
                    segment.index
                )));
            }
            let expected_bytes =
                segment_payload_bytes(self.dimension, bit_width, segment.row_count);
            if segment.payload_bytes != expected_bytes as u64 {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "segment {} payload length mismatch",
                    segment.index
                )));
            }
            expected_offset = expected_offset.saturating_add(segment.payload_bytes);
            expected_ordinal = expected_ordinal.saturating_add(segment.row_count);
        }
        if expected_offset != self.payload_bytes || expected_ordinal != self.document_count {
            return Err(ProjectionError::CorruptArtifact(
                "manifest totals do not match segment descriptors".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBuildReport {
    pub document_count: usize,
    pub segment_count: usize,
    pub raw_vector_bytes: u64,
    pub projection_payload_bytes: u64,
    pub configured_working_bytes: usize,
    pub peak_working_bytes: usize,
    pub requested_segment_rows: usize,
    pub admitted_segment_rows: usize,
}

#[derive(Debug)]
pub(crate) struct QuantizedSegment {
    pub base_ordinal: usize,
    pub ids: Vec<u64>,
    pub reconstruction_scales: Vec<f32>,
    pub reconstruction_offsets: Vec<f32>,
    pub codes: Vec<u8>,
}

impl QuantizedSegment {
    pub fn with_capacity(config: &ValidatedBuildConfig, base_ordinal: usize) -> Self {
        Self {
            base_ordinal,
            ids: Vec::with_capacity(config.admitted_segment_rows),
            reconstruction_scales: Vec::with_capacity(config.admitted_segment_rows),
            reconstruction_offsets: Vec::with_capacity(config.admitted_segment_rows),
            codes: Vec::with_capacity(
                config
                    .admitted_segment_rows
                    .saturating_mul(config.bytes_per_vector),
            ),
        }
    }

    pub fn row_count(&self) -> usize {
        self.ids.len()
    }

    pub fn empty(base_ordinal: usize) -> Self {
        Self {
            base_ordinal,
            ids: Vec::new(),
            reconstruction_scales: Vec::new(),
            reconstruction_offsets: Vec::new(),
            codes: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

#[derive(Debug)]
pub struct InMemoryProjection {
    pub(crate) manifest: ProjectionManifest,
    pub(crate) segments: Vec<QuantizedSegment>,
    pub(crate) build_report: ProjectionBuildReport,
}

impl InMemoryProjection {
    pub fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    pub fn build_report(&self) -> &ProjectionBuildReport {
        &self.build_report
    }
}

pub(crate) fn segment_payload_bytes(
    dimension: usize,
    bit_width: RaBitQBitWidth,
    rows: usize,
) -> usize {
    rows.saturating_mul(
        std::mem::size_of::<u64>()
            .saturating_add(2 * std::mem::size_of::<f32>())
            .saturating_add(encoded_vector_bytes(dimension, bit_width)),
    )
}

pub(crate) fn ids_bytes(rows: usize) -> usize {
    rows.saturating_mul(std::mem::size_of::<u64>())
}

pub(crate) fn reconstruction_factor_bytes(rows: usize) -> usize {
    rows.saturating_mul(std::mem::size_of::<f32>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_bit_width_matches_faiss_standard_rabitq() {
        assert_eq!(DEFAULT_PROJECTION_BIT_WIDTH, 1);
        assert_eq!(RaBitQBitWidth::default(), RaBitQBitWidth::One);
        assert_eq!(RaBitQBitWidth::default().quantizer(), PROJECTION_QUANTIZER);
    }
}
