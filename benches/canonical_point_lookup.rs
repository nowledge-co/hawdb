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

//! Point-lookup cost against canonical segments.
//!
//! Two shapes, because a lookup spends its time in two different places. Large
//! segments make the intra-segment walk dominate, so they show what stopping at
//! the matching record is worth. Small segments make the descriptor search
//! dominate, so they show what binary searching the manifest is worth.

use hawdb_core::{LabelId, Value};
use hawdb_storage::{
    CanonicalSegmentConfig, CanonicalSegmentReader, CanonicalSegmentWriter, NodeId, NodeRecord,
    SegmentCache, StoreId,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

// Debug-assertion builds are the smoke execution CI drives through
// `cargo test --benches`; numbers are only meaningful from `cargo bench`.
const NODE_COUNT: u64 = if cfg!(debug_assertions) {
    4_000
} else {
    120_000
};
const BODY_BYTES: usize = 320;
const LOOKUPS: u64 = if cfg!(debug_assertions) {
    1_000
} else {
    20_000
};
const SAMPLES: usize = if cfg!(debug_assertions) { 2 } else { 5 };
const SHAPES: [(&str, u64); 2] = [
    ("large_segments", 4 * 1024 * 1024),
    ("small_segments", 32 * 1024),
];

fn main() {
    let results = SHAPES
        .into_iter()
        .map(|(name, segment_bytes)| measure(name, segment_bytes))
        .collect::<Vec<_>>();
    println!(
        "canonical_point_lookup {}",
        json!({
            "node_count": NODE_COUNT,
            "body_bytes": BODY_BYTES,
            "lookups": LOOKUPS,
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn measure(name: &str, segment_bytes: u64) -> serde_json::Value {
    let path = std::env::temp_dir().join(format!(
        "hawdb-canonical-point-lookup-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    // Ids step by two, so half the probes land in a gap and half on a record.
    // A lookup that misses is the case an early exit helps most, and it is not
    // a rare one once deletes have punched holes in the id space.
    let nodes = (0..NODE_COUNT).map(|index| {
        Ok(NodeRecord {
            id: NodeId(index * 2),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([
                ("id".to_string(), Value::Int(index as i64)),
                ("body".to_string(), Value::String("x".repeat(BODY_BYTES))),
            ]),
        })
    });
    let config = CanonicalSegmentConfig {
        target_segment_bytes: NonZeroU64::new(segment_bytes).unwrap(),
        max_record_bytes: NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    };
    let manifest = CanonicalSegmentWriter::new(config)
        .write_fallible(
            &path,
            hawdb_storage::ManifestGeneration(1),
            nodes,
            Vec::new(),
        )
        .expect("benchmark artifact must be writable");
    let segment_count = manifest.segment_count as usize;
    let artifact_len = manifest.artifact_len;
    // Sized to hold the whole artifact, so the measurement is the search and
    // the decode rather than the file system.
    let cache = Arc::new(SegmentCache::new(artifact_len * 2));
    let reader = CanonicalSegmentReader::open(
        &path,
        manifest,
        Arc::clone(&cache),
        StoreId(1),
        NonZeroU64::new(16 * 1024 * 1024).unwrap(),
    )
    .expect("benchmark artifact must be readable");

    let probes = (0..LOOKUPS)
        .map(|index| NodeId(index.wrapping_mul(2_654_435_761) % (NODE_COUNT * 2)))
        .collect::<Vec<_>>();
    for probe in probes.iter().take(1_000) {
        black_box(reader.get_node(*probe).expect("lookup must succeed"));
    }

    let mut nanos = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let start = Instant::now();
        for probe in &probes {
            black_box(reader.get_node(*probe).expect("lookup must succeed"));
        }
        nanos.push(start.elapsed().as_nanos() as u64);
    }
    nanos.sort_unstable();
    let median = nanos[nanos.len() / 2];
    std::fs::remove_file(&path).expect("benchmark artifact must be removable");
    json!({
        "shape": name,
        "segment_count": segment_count,
        "artifact_len": artifact_len,
        "median_total_nanos": median,
        "nanos_per_lookup": median / LOOKUPS,
        "lookups_per_second": (LOOKUPS as f64 / (median as f64 / 1e9)).round() as u64,
    })
}
