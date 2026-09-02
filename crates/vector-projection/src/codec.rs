use crate::error::Result;
use crate::model::RaBitQBitWidth;
use crate::rabitq::{self, RaBitQEncoding};
use crate::transform::normalize_and_transform;

pub(crate) fn encode_vector(
    vector: &[f32],
    transform_seed: u64,
    bit_width: RaBitQBitWidth,
    transformed: &mut [f32],
    packed: &mut Vec<u8>,
) -> Result<RaBitQEncoding> {
    normalize_and_transform(vector, transform_seed, transformed)?;
    rabitq::encode(transformed, bit_width, packed)
}

pub(crate) use rabitq::code_at;
