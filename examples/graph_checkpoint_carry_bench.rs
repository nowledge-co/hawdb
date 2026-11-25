// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual embedded checkpoint benchmark, including sealed-root publication.
use hawdb::{
    BranchSelector, Database, DatabaseConfig, DurabilityPolicy, StorageResidencyMode, Value,
};
use hawdb_storage::durable_manifest::DurableManifest;
use serde_json::json;
use std::io::{BufRead, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    assert!(matches!(args.len(), 5..=9),
        "usage: graph_checkpoint_carry_bench <new-root> <nodes> <edges> <rounds> <carry|rebuild> [step-node|step-relationship] [cpu-profile] [sync-every-write|sync-on-checkpoint] [checkpoint-every=N]");
    let mut profile = false;
    let mut selected = None;
    let mut durability_option = None;
    let mut checkpoint_interval_option = None;
    for option in &args[5..] {
        match option.as_str() {
            "step-node" | "step-relationship" => {
                assert!(selected.is_none(), "duplicate step mode");
                selected = Some(if option == "step-node" {
                    "node_property"
                } else {
                    "relationship_property"
                });
            }
            "cpu-profile" => {
                assert!(!profile, "duplicate profile option");
                profile = true;
            }
            "sync-every-write" | "sync-on-checkpoint" => {
                assert!(durability_option.is_none(), "duplicate durability option");
                durability_option = Some(if option == "sync-every-write" {
                    DurabilityPolicy::SyncOnEveryWrite
                } else {
                    DurabilityPolicy::SyncOnCheckpoint
                });
            }
            _ => {
                let interval = option
                    .strip_prefix("checkpoint-every=")
                    .unwrap_or_else(|| panic!("unknown option: {option}"));
                assert!(
                    checkpoint_interval_option.is_none(),
                    "duplicate checkpoint interval"
                );
                let interval: usize = interval.parse().expect("invalid checkpoint interval");
                assert!(interval > 0, "checkpoint interval must be positive");
                checkpoint_interval_option = Some(interval);
            }
        }
    }
    let durability = durability_option.unwrap_or_default();
    let durability_name = format!("{durability:?}");
    let checkpoint_interval = checkpoint_interval_option.unwrap_or(1);
    let stdin = std::io::stdin();
    let mut input = stdin.lock();
    let root = Path::new(&args[0]);
    assert!(!root.exists(), "fixture path must be new");
    let nodes: usize = args[1].parse().unwrap();
    let edges: usize = args[2].parse().unwrap();
    let rounds: usize = args[3].parse().unwrap();
    assert!(nodes > 1 && edges > 0 && rounds > 0);
    assert!(matches!(args[4].as_str(), "carry" | "rebuild"));
    for workload in ["node_property", "relationship_property"] {
        if selected.is_some_and(|selected| selected != workload) {
            continue;
        }
        let path = root.join(workload);
        let mut config = DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        };
        // Keep all checkpoint work inside the explicit timed calls.
        config.runtime_capabilities.background_maintenance = false;
        let mut db =
            Database::open_with_durability_and_config(&path, durability, config.clone()).unwrap();
        let telemetry = Arc::new(ProfileTelemetry::default());
        if profile {
            db.set_telemetry_sink(Some(telemetry.clone()));
        }
        db.query("CREATE INDEX ON :Note(id)").unwrap();
        for id in 0..nodes {
            db.query(&format!("CREATE (:Note {{id: {id}, value: 0}})"))
                .unwrap();
        }
        for index in 0..edges {
            db.query(&format!("MATCH (s:Note {{id: {}}}), (t:Note {{id: {}}}) CREATE (s)-[:LINK {{weight: 0}}]->(t)", index % nodes, (index * 7 + 1) % nodes)).unwrap();
        }
        db.checkpoint().unwrap();
        let branch = db
            .describe_branch(BranchSelector::Name("main".into()))
            .unwrap()
            .id;
        let branch_path = path.join("branches").join(branch.to_string());
        let manifest_path = branch_path.join("runtime/manifest.hawdb");
        let initial = DurableManifest::load(&manifest_path)
            .unwrap()
            .canonical_adjacency_generation_artifacts
            .unwrap();
        if selected.is_some() {
            println!(
                "{}",
                json!({"protocol":"hawdb-checkpoint-pair-v1","ready":workload,"storage_version":db.storage_version().unwrap(),"automatic_checkpoint":false,"durability":durability_name,"checkpoint_interval":checkpoint_interval})
            );
            std::io::stdout().flush().unwrap();
        }
        for round in 0..rounds + 2 {
            if selected.is_some() {
                command(&mut input, "step");
            }
            let value = round as i64 + 1;
            let (update, verify, count) = if workload == "node_property" {
                (
                    format!("MATCH (n:Note) SET n.value = {value}"),
                    "MATCH (n:Note) RETURN n.value AS value",
                    nodes,
                )
            } else {
                (
                    format!("MATCH ()-[r:LINK]->() SET r.weight = {value}"),
                    "MATCH ()-[r:LINK]->() RETURN r.weight AS value",
                    edges,
                )
            };
            let mutation_usage_before = process_usage(profile, &telemetry);
            let mutation_started = Instant::now();
            db.query(&update).unwrap();
            let mutation_elapsed = mutation_started.elapsed();
            let mutation_usage_after = process_usage(profile, &telemetry);
            let checkpoint_usage_before = process_usage(profile, &telemetry);
            let started = Instant::now();
            // End warmup with a clean checkpoint boundary. Always checkpoint
            // the final measured write before scrub and reopen validation.
            let checkpoint_performed =
                round < 2 || (round - 1).is_multiple_of(checkpoint_interval) || round == rounds + 1;
            let elapsed = if checkpoint_performed {
                db.checkpoint().unwrap();
                started.elapsed()
            } else {
                std::time::Duration::ZERO
            };
            let checkpoint_usage_after = process_usage(profile, &telemetry);
            let manifest = DurableManifest::load(&manifest_path).unwrap();
            let binding = manifest.canonical_adjacency_generation_artifacts.unwrap();
            assert_eq!(
                binding == initial,
                args[4] == "carry" && workload == "node_property"
            );
            let rows = db.query(verify).unwrap().rows;
            assert_eq!(rows.len(), count);
            assert!(rows.iter().all(|row| row["value"] == Value::Int(value)));
            let mut graph_bytes = 0;
            let mut adjacency_bytes = 0;
            for entry in std::fs::read_dir(branch_path.join("data")).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name();
                let name = name.to_str().unwrap();
                if checkpoint_performed
                    && hawdb_storage::artifact_files::parse_graph_generation_file(name)
                        == Some(manifest.checkpoint_epoch)
                {
                    let bytes = entry.metadata().unwrap().len();
                    graph_bytes += bytes;
                    if name.starts_with("adjacency") {
                        adjacency_bytes += bytes;
                    }
                }
            }
            let mut row = json!({"api":"Database::checkpoint","automatic_checkpoint":false,"workload":workload,"nodes":nodes,"edges":edges,"round":round,"warmup":round<2,"checkpoint_ns":elapsed.as_nanos(),"checkpoint_performed":checkpoint_performed,"checkpoint_interval":checkpoint_interval,"mutation_ns":mutation_elapsed.as_nanos(),"new_graph_bytes":graph_bytes,"new_adjacency_bytes":adjacency_bytes,"durability":durability_name,"residency":"OutOfCore"});
            if profile {
                row["process_usage"] = json!({
                    "scope": "RUSAGE_SELF",
                    "mutation_before": mutation_usage_before,
                    "mutation_after": mutation_usage_after,
                    "checkpoint_before": checkpoint_usage_before,
                    "checkpoint_after": checkpoint_usage_after,
                });
            }
            println!("{row}");
            if selected.is_some() {
                std::io::stdout().flush().unwrap();
            }
        }
        if selected.is_some() {
            command(&mut input, "finish");
        }
        db.scrub_storage().unwrap();
        drop(db);
        let mut reopened =
            Database::open_with_durability_and_config(&path, durability, config).unwrap();
        assert_eq!(
            reopened
                .query("MATCH (n:Note) RETURN n.id")
                .unwrap()
                .rows
                .len(),
            nodes
        );
        assert_eq!(
            reopened
                .query("MATCH ()-[r:LINK]->() RETURN r.weight")
                .unwrap()
                .rows
                .len(),
            edges
        );
        let (verify, count) = if workload == "node_property" {
            ("MATCH (n:Note) RETURN n.value AS value", nodes)
        } else {
            ("MATCH ()-[r:LINK]->() RETURN r.weight AS value", edges)
        };
        let rows = reopened.query(verify).unwrap().rows;
        assert_eq!(rows.len(), count);
        assert!(rows
            .iter()
            .all(|row| row["value"] == Value::Int(rounds as i64 + 2)));
        reopened.scrub_storage().unwrap();
    }
}

#[derive(Debug, Default)]
struct ProfileTelemetry {
    wal_appends: AtomicU64,
    wal_elapsed_micros: AtomicU64,
    wal_fsync_micros: AtomicU64,
    wal_bytes: AtomicU64,
}

impl hawdb::TelemetrySink for ProfileTelemetry {
    fn record_query(&self, _event: hawdb::QueryTelemetry<'_>) {}

    fn record_kernel(&self, event: hawdb::KernelTelemetry) {
        if event.operation == hawdb::KernelTelemetryOperation::WalAppend {
            assert!(event.success);
            self.wal_appends.fetch_add(1, Ordering::Relaxed);
            self.wal_elapsed_micros
                .fetch_add(event.elapsed_micros, Ordering::Relaxed);
            self.wal_fsync_micros
                .fetch_add(event.fsync_micros, Ordering::Relaxed);
            self.wal_bytes
                .fetch_add(event.byte_count, Ordering::Relaxed);
        }
    }
}

fn process_usage(enabled: bool, telemetry: &ProfileTelemetry) -> Option<serde_json::Value> {
    let _ = telemetry;
    if !enabled {
        return None;
    }
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // getrusage initializes the complete output on success. This manual
        // producer has background maintenance disabled; counters still cover
        // every process thread, rather than claiming thread-isolated CPU time.
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
            0
        );
        let usage = unsafe { usage.assume_init() };
        let nanos = |time: libc::timeval| {
            u64::try_from(time.tv_sec).unwrap() * 1_000_000_000
                + u64::try_from(time.tv_usec).unwrap() * 1_000
        };
        Some(json!({
            "user_ns": nanos(usage.ru_utime),
            "system_ns": nanos(usage.ru_stime),
            "minor_faults": usage.ru_minflt,
            "major_faults": usage.ru_majflt,
            "voluntary_switches": usage.ru_nvcsw,
            "involuntary_switches": usage.ru_nivcsw,
            "input_blocks": usage.ru_inblock,
            "output_blocks": usage.ru_oublock,
            "wal_appends": telemetry.wal_appends.load(Ordering::Relaxed),
            "wal_elapsed_micros": telemetry.wal_elapsed_micros.load(Ordering::Relaxed),
            "wal_fsync_micros": telemetry.wal_fsync_micros.load(Ordering::Relaxed),
            "wal_bytes": telemetry.wal_bytes.load(Ordering::Relaxed),
        }))
    }
    #[cfg(not(unix))]
    panic!("cpu-profile requires Unix getrusage support");
}

fn command(input: &mut impl BufRead, expected: &str) {
    let mut command = String::new();
    assert!(
        input.read_line(&mut command).unwrap() > 0,
        "step controller closed input"
    );
    assert_eq!(command.trim(), expected);
}
