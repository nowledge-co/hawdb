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

use hawdb_integrity::{checksum_u64, sha256};
use serde_json::json;
use std::hint::black_box;
use std::time::{Duration, Instant};

const PAYLOAD_BYTES: [usize; 3] = [4 * 1024, 64 * 1024, 1024 * 1024];
const SAMPLE_DURATION: Duration = Duration::from_millis(250);
const SAMPLES: usize = 9;

fn main() {
    let results = PAYLOAD_BYTES
        .into_iter()
        .map(measure_payload)
        .collect::<Vec<_>>();
    println!("integrity_checksum {}", json!({ "results": results }));
}

fn measure_payload(payload_bytes: usize) -> serde_json::Value {
    let payload = (0..payload_bytes)
        .map(|offset| (offset as u8).wrapping_mul(31).wrapping_add(17))
        .collect::<Vec<_>>();
    let crc32c = median_throughput(&payload, checksum_u64);
    let legacy_fnv64_sample = median_throughput(&payload, legacy_fnv64);
    let sha256 = median_throughput(&payload, |bytes| {
        black_box(sha256(bytes));
        0
    });
    assert_eq!(checksum_u64(&payload), crc32c.checksum);
    assert_eq!(legacy_fnv64(&payload), legacy_fnv64_sample.checksum);
    json!({
        "payload_bytes": payload_bytes,
        "crc32c_mib_per_second": crc32c.mib_per_second,
        "legacy_fnv64_mib_per_second": legacy_fnv64_sample.mib_per_second,
        "sha256_mib_per_second": sha256.mib_per_second,
        "crc32c_vs_legacy_fnv64": crc32c.mib_per_second / legacy_fnv64_sample.mib_per_second,
    })
}

fn median_throughput(payload: &[u8], operation: impl Fn(&[u8]) -> u64) -> Sample {
    let mut samples = (0..SAMPLES)
        .map(|_| sample_throughput(payload, &operation))
        .collect::<Vec<_>>();
    samples.sort_unstable_by(|left, right| left.mib_per_second.total_cmp(&right.mib_per_second));
    samples[samples.len() / 2]
}

fn sample_throughput(payload: &[u8], operation: &impl Fn(&[u8]) -> u64) -> Sample {
    let started = Instant::now();
    let mut iterations = 0u64;
    let mut checksum = 0u64;
    while started.elapsed() < SAMPLE_DURATION {
        checksum ^= black_box(operation(black_box(payload)));
        iterations += 1;
    }
    let elapsed = started.elapsed();
    let bytes = (payload.len() as u128).saturating_mul(iterations as u128);
    let mib_per_second = bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
    Sample {
        checksum: if iterations.is_multiple_of(2) {
            operation(payload)
        } else {
            checksum
        },
        mib_per_second,
    }
}

#[derive(Clone, Copy)]
struct Sample {
    checksum: u64,
    mib_per_second: f64,
}

fn legacy_fnv64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}
