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

use hawdb_storage::{
    FileSegmentRangeReader, SegmentReadExecutor, SegmentReadPool, SegmentReadRange,
    SegmentReadScheduler,
};
use serde_json::json;
use std::hint::black_box;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const RANGE_BYTES: usize = 1024 * 1024;
const RANGE_GAP_BYTES: usize = 4096;
const RANGE_COUNT: usize = 64;
const WORKERS: [usize; 4] = [1, 4, 8, 16];
const SAMPLES: usize = 5;

fn main() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-storage-segment-read-bench-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    let stride = RANGE_BYTES + RANGE_GAP_BYTES;
    let mut artifact = vec![0u8; stride * RANGE_COUNT];
    for (index, byte) in artifact.iter_mut().enumerate() {
        *byte = (index as u8).wrapping_mul(31).wrapping_add(17);
    }
    std::fs::write(&path, &artifact).expect("benchmark artifact must be writable");
    let mut reader = FileSegmentRangeReader::new();
    reader.register(1, &path);
    let ranges = (0..RANGE_COUNT)
        .map(|index| {
            SegmentReadRange::new(
                1,
                index as u64,
                (index * stride) as u64,
                NonZeroU64::new(RANGE_BYTES as u64).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let results = WORKERS
        .into_iter()
        .map(|workers| measure(&reader, &ranges, workers))
        .collect::<Vec<_>>();
    println!(
        "storage_segment_read {}",
        json!({
            "range_bytes": RANGE_BYTES,
            "range_count": RANGE_COUNT,
            "samples": SAMPLES,
            "results": results,
        })
    );
    std::fs::remove_file(path).expect("benchmark artifact must be removable");
}

fn measure(
    reader: &FileSegmentRangeReader,
    ranges: &[SegmentReadRange],
    workers: usize,
) -> serde_json::Value {
    let worker_count = NonZeroUsize::new(workers).unwrap();
    let pool = SegmentReadPool::new(worker_count).expect("benchmark read pool must start");
    let schedule =
        SegmentReadScheduler::new(worker_count, NonZeroU64::new(RANGE_BYTES as u64).unwrap())
            .schedule(ranges.iter().cloned());
    let max_wave_bytes = NonZeroU64::new((workers * RANGE_BYTES) as u64).unwrap();
    let mut samples = (0..SAMPLES)
        .map(|_| {
            let started = Instant::now();
            let mut checksum = 0u64;
            let report = SegmentReadExecutor::with_pool(max_wave_bytes, pool.clone())
                .execute(reader, &schedule, |payload| {
                    checksum ^= u64::from(payload.bytes[0]);
                    checksum ^= u64::from(payload.bytes[payload.bytes.len() - 1]) << 8;
                    Ok::<(), std::convert::Infallible>(())
                })
                .expect("benchmark reads must succeed");
            black_box(checksum);
            let elapsed = started.elapsed();
            report.bytes_read as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0)
        })
        .collect::<Vec<_>>();
    samples.sort_unstable_by(f64::total_cmp);
    json!({
        "workers": workers,
        "median_mib_per_second": samples[samples.len() / 2],
        "min_mib_per_second": samples[0],
        "max_mib_per_second": samples[samples.len() - 1],
    })
}
