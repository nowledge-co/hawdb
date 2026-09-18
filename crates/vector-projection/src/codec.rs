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
