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
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};

#[test]
fn shared_encoder_preserves_fixed_wire_for_all_scalar_tags_and_float_bits() {
    // Independently assembled V1 header, slot offsets, payload, CRC32C and SHA.
    // This is fixed wire evidence, independent of either encoder entry point.
    const EXPECTED_HEX: &str = "534b494e524f57310100000007000000000000001700000000000000130000000000000001000000080000000900000009000000100000000900000000000000ae00000000000000a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a525bf8f3dcad136f2ff578511817701141eddb81fdfae99d994185936a3365b0e3195fc3b02800000000000002a02800000000000002a000000000900000000000000ae00000002800000000000002a080000000000000001000000010000000200000003000000090000000c00000009000000150000000a0000001f000000080000002700000011000000380000003200000000010102efffffffffffffff03420000000000f87f0405000000e7958c007805030000000001ff07000102030405060708090a0b0c0d0e0f0604070000000000000000100000000000005a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";
    let expected = EXPECTED_HEX
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect::<Vec<_>>();
    let page = ImmutableRelationalRowPage {
        generation: 7,
        source_commit_epoch: 23,
        page_id: RelationalRowPageId::new(NonZeroU64::new(19).unwrap()),
        schema_digest: Sha256Digest::from_bytes([0xa5; 32]),
        column_count: 8,
        rows: vec![RelationalRowPageEntry {
            primary_key: RelationalKey(vec![RelationalValue::BigInt(42)]),
            row: RelationalRow::new(vec![
                RelationalValue::Null,
                RelationalValue::Boolean(true),
                RelationalValue::BigInt(-17),
                RelationalValue::DoublePrecision(f64::from_bits(0x7ff8_0000_0000_0042)),
                RelationalValue::Text("界\0x".into()),
                RelationalValue::Bytea(vec![0, 1, 255]),
                RelationalValue::Uuid(hawdb_core::Uuid::from_bytes(std::array::from_fn(|i| {
                    i as u8
                }))),
                RelationalValue::Overflow(RelationalOverflowRef {
                    digest: Sha256Digest::from_bytes([0x5a; 32]),
                    scalar_type: RelationalScalarType::Text,
                    compressed_bytes: 7,
                    uncompressed_bytes: 4096,
                }),
            ]),
        }],
    };
    let limits = RelationalRowPageLimits::default();
    assert_eq!(page.encode(limits).unwrap(), expected);
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        ..Default::default()
    });
    let work = CheckpointWorkContext::default().with_scheduler(scheduler.clone());
    assert_eq!(encode(&page, limits, &work).unwrap(), expected);
    let decoded = ImmutableRelationalRowPage::decode(&expected, limits).unwrap();
    assert_eq!(decoded, page);
    let RelationalValue::DoublePrecision(value) = &decoded.rows[0].row.values()[3] else {
        panic!("the NaN payload must retain its scalar type");
    };
    assert_eq!(value.to_bits(), 0x7ff8_0000_0000_0042);
    assert_eq!(scheduler.state().running_background_operations, 0);
}
