use serde_json::json;
use skein_qos::ProcessMemorySnapshot;
use skein_storage::{
    RelationalColumnSchema, RelationalHydrationBudget, RelationalInsertMode, RelationalKey,
    RelationalMutationLimits, RelationalOverflowConfig, RelationalRow, RelationalScalarType,
    RelationalStore, RelationalTableSchema, RelationalTransaction, RelationalValue,
    RelationalWrite,
};
use std::hint::black_box;
use std::time::Instant;

const PAYLOAD_BYTES: [usize; 4] = [1024, 4 * 1024, 16 * 1024, 64 * 1024];
const SAMPLES: usize = 21;

fn main() {
    let results = PAYLOAD_BYTES
        .into_iter()
        .flat_map(|payload_bytes| {
            [
                PayloadShape::Repetitive,
                PayloadShape::Varied,
                PayloadShape::HighEntropy,
            ]
            .into_iter()
            .flat_map(move |shape| {
                [StorageMode::Inline, StorageMode::Overflow]
                    .into_iter()
                    .map(move |mode| measure(payload_bytes, shape, mode))
            })
        })
        .collect::<Vec<_>>();
    println!(
        "relational_overflow {}",
        json!({
            "samples": SAMPLES,
            "results": results,
        })
    );
}

fn measure(payload_bytes: usize, shape: PayloadShape, mode: StorageMode) -> serde_json::Value {
    let payload = payload(payload_bytes, shape);
    let memory_before = ProcessMemorySnapshot::capture().ok();
    let mut write_samples = Vec::with_capacity(SAMPLES);
    let mut hydration_samples = Vec::with_capacity(SAMPLES);
    let mut compressed_bytes = None;
    for sample in 0..SAMPLES {
        let store = store(mode);
        let id = format!("message-{sample}");
        let started = Instant::now();
        store
            .commit(
                RelationalTransaction {
                    writes: vec![RelationalWrite::Insert {
                        table: "messages".to_string(),
                        rows: vec![RelationalRow::new(vec![
                            RelationalValue::Text(id.clone()),
                            RelationalValue::Text(payload.clone()),
                        ])],
                        mode: RelationalInsertMode::Error,
                    }],
                },
                |_, _| Ok(()),
            )
            .expect("benchmark insert");
        write_samples.push(started.elapsed().as_nanos());
        let snapshot = store.snapshot().expect("benchmark snapshot");
        let key = RelationalKey(vec![RelationalValue::Text(id)]);
        if let Some(RelationalValue::Overflow(reference)) = snapshot
            .value()
            .row("messages", &key)
            .and_then(|row| row.values().get(1))
        {
            compressed_bytes = Some(reference.compressed_bytes);
        }
        let mut budget = RelationalHydrationBudget::default();
        let started = Instant::now();
        let hydrated = snapshot
            .value()
            .hydrate_row("messages", &key, &mut budget)
            .expect("benchmark hydrate")
            .expect("benchmark row");
        black_box(hydrated);
        hydration_samples.push(started.elapsed().as_nanos());
    }
    let memory_after = ProcessMemorySnapshot::capture().ok();
    write_samples.sort_unstable();
    hydration_samples.sort_unstable();
    json!({
        "payload_bytes": payload_bytes,
        "payload_shape": shape.name(),
        "storage_mode": mode.name(),
        "compressed_bytes": compressed_bytes,
        "overflow_codec": compressed_bytes.map(|bytes| {
            if bytes == payload_bytes { "raw" } else { "zstd" }
        }),
        "compression_ratio": compressed_bytes.map(|bytes| bytes as f64 / payload_bytes as f64),
        "write_ns_p50": percentile(&write_samples, 50),
        "write_ns_p95": percentile(&write_samples, 95),
        "write_ns_p99": percentile(&write_samples, 99),
        "hydrate_ns_p50": percentile(&hydration_samples, 50),
        "hydrate_ns_p95": percentile(&hydration_samples, 95),
        "hydrate_ns_p99": percentile(&hydration_samples, 99),
        "resident_delta_bytes": memory_before.zip(memory_after).map(|(before, after)| {
            after.resident_bytes.saturating_sub(before.resident_bytes)
        }),
    })
}

fn store(mode: StorageMode) -> RelationalStore {
    let overflow_config = RelationalOverflowConfig {
        threshold_bytes: match mode {
            StorageMode::Inline => usize::MAX,
            StorageMode::Overflow => 1,
        },
        ..RelationalOverflowConfig::default()
    };
    let store =
        RelationalStore::with_overflow_config(RelationalMutationLimits::default(), overflow_config);
    store
        .commit(
            RelationalTransaction {
                writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                    name: "messages".to_string(),
                    columns: vec![text_column("id"), text_column("content")],
                    primary_key: vec!["id".to_string()],
                    unique_constraints: Vec::new(),
                    foreign_keys: Vec::new(),
                    indexes: Vec::new(),
                })],
            },
            |_, _| Ok(()),
        )
        .expect("benchmark schema");
    store
}

fn text_column(name: &str) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name: name.to_string(),
        scalar_type: RelationalScalarType::Text,
        nullable: false,
        default: None,
    }
}

fn payload(payload_bytes: usize, shape: PayloadShape) -> String {
    let bytes = match shape {
        PayloadShape::Repetitive => (0..payload_bytes)
            .map(|offset| b'a' + (offset % 4) as u8)
            .collect::<Vec<_>>(),
        PayloadShape::Varied => (0..payload_bytes)
            .map(|offset| {
                (offset as u8)
                    .wrapping_mul(31)
                    .wrapping_add((offset >> 8) as u8)
                    .max(1)
            })
            .map(|byte| b' ' + (byte % 95))
            .collect::<Vec<_>>(),
        PayloadShape::HighEntropy => {
            let mut state = 0x9e37_79b9_7f4a_7c15_u64;
            (0..payload_bytes)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    b' ' + ((state as u8) % 95)
                })
                .collect::<Vec<_>>()
        }
    };
    String::from_utf8(bytes).expect("benchmark payload is ASCII")
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    let rank = samples
        .len()
        .saturating_mul(percentile.min(100))
        .saturating_add(99)
        / 100;
    let index = rank.saturating_sub(1);
    samples.get(index).copied().unwrap_or_default()
}

#[derive(Clone, Copy)]
enum PayloadShape {
    Repetitive,
    Varied,
    HighEntropy,
}

impl PayloadShape {
    fn name(self) -> &'static str {
        match self {
            Self::Repetitive => "repetitive",
            Self::Varied => "varied",
            Self::HighEntropy => "high_entropy",
        }
    }
}

#[derive(Clone, Copy)]
enum StorageMode {
    Inline,
    Overflow,
}

impl StorageMode {
    fn name(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Overflow => "overflow_zstd",
        }
    }
}
