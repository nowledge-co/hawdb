// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! With several admitted workers, batched construction runs in parallel
//! (#1472) and builds the same graph for every worker count. One worker keeps
//! the sequential construction.

use super::*;
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::num::NonZeroUsize;

fn entries(count: u64, dimension: usize) -> Vec<(u64, Vec<f32>)> {
    let mut random = 0x0123_4567_89ab_cdefu64;
    (0..count)
        .map(|id| {
            let vector = (0..dimension)
                .map(|_| {
                    random ^= random << 13;
                    random ^= random >> 7;
                    random ^= random << 17;
                    (random % 10_000) as f32 / 5000.0 - 1.0
                })
                .collect();
            (id, vector)
        })
        .collect()
}

fn parallelism(threads: usize) -> RuntimeTaskContext {
    RuntimeTaskContext::default().with_admitted_parallelism(NonZeroUsize::new(threads).unwrap())
}

fn links(graph: &DiskAnnIndex) -> Vec<Vec<u32>> {
    (0..graph.len() as u32)
        .map(|ordinal| graph.neighbors(ordinal).unwrap().collect())
        .collect()
}

fn build(
    entries: &[(u64, Vec<f32>)],
    config: DiskAnnBuildConfig,
    task: Option<&RuntimeTaskContext>,
) -> DiskAnnIndex {
    DiskAnnIndex::build(8, VectorDistance::Euclidean, entries.to_vec(), config, task).unwrap()
}

#[test]
fn several_workers_build_one_graph_and_one_worker_keeps_sequential_insertion() {
    let entries = entries(1_500, 8);
    for lsg in [None, Some(LsgBuildConfig::default())] {
        let config = DiskAnnBuildConfig {
            max_degree: 16,
            build_beam: 32,
            lsg,
            ..Default::default()
        };
        // One worker (no task, one admitted thread, or a limit of one) keeps
        // the sequential construction.
        let sequential = links(&build(&entries, config, None));
        let one = parallelism(1);
        let limited = parallelism(8).with_executor_thread_limit(NonZeroUsize::MIN);
        assert_eq!(links(&build(&entries, config, Some(&one))), sequential);
        assert_eq!(links(&build(&entries, config, Some(&limited))), sequential);
        // Several workers build the batched graph, identical for every count.
        let batched = links(&build(&entries, config, Some(&parallelism(2))));
        assert_ne!(batched, sequential, "lsg={}", lsg.is_some());
        for threads in [3, 8] {
            let task = parallelism(threads);
            assert_eq!(
                links(&build(&entries, config, Some(&task))),
                batched,
                "threads={threads} lsg={}",
                lsg.is_some()
            );
            // The configured cap only lowers the worker count.
            let capped = DiskAnnBuildConfig {
                max_build_threads: NonZeroUsize::new(2).unwrap(),
                ..config
            };
            assert_eq!(links(&build(&entries, capped, Some(&task))), batched);
        }
    }
}

#[test]
fn worker_threads_follow_the_admitted_parallelism_cap_and_memory() {
    let count = 10_000;
    let config = DiskAnnBuildConfig {
        max_working_bytes: 1 << 30,
        ..Default::default()
    };
    let per_thread = scratch_bytes(count, config.build_beam, count).unwrap() + count * 4;
    let batch = build::batch_bytes_for_test(count, config);
    let base = 1 << 20;
    assert_eq!(build::build_threads_for_test(count, config, None, base), 1);
    let task = parallelism(6);
    assert_eq!(
        build::build_threads_for_test(count, config, Some(&task), base),
        6
    );
    let capped = DiskAnnBuildConfig {
        max_build_threads: NonZeroUsize::new(4).unwrap(),
        ..config
    };
    assert_eq!(
        build::build_threads_for_test(count, capped, Some(&task), base),
        4
    );
    let limited = parallelism(6).with_executor_thread_limit(NonZeroUsize::new(3).unwrap());
    assert_eq!(
        build::build_threads_for_test(count, config, Some(&limited), base),
        3
    );
    // Each extra worker needs its own traversal scratch inside the budget.
    let tight = DiskAnnBuildConfig {
        max_working_bytes: base + batch + 2 * per_thread + per_thread / 2,
        ..config
    };
    assert_eq!(
        build::build_threads_for_test(count, tight, Some(&task), base),
        3
    );
    let no_batch = DiskAnnBuildConfig {
        max_working_bytes: base + 2 * per_thread,
        ..config
    };
    assert!(build::build_threads_for_test(count, no_batch, Some(&task), base) < 3);
    let exhausted = DiskAnnBuildConfig {
        max_working_bytes: base + batch,
        ..config
    };
    assert_eq!(
        build::build_threads_for_test(count, exhausted, Some(&task), base),
        1
    );
}

#[test]
fn a_tight_budget_falls_back_to_one_sequential_worker() {
    let entries = entries(1_200, 8);
    let config = DiskAnnBuildConfig {
        max_degree: 16,
        build_beam: 32,
        ..Default::default()
    };
    let required = DiskAnnIndex::build_memory_bound(entries.len(), 8, config).unwrap();
    let expected = links(&build(&entries, config, None));
    let exact = DiskAnnBuildConfig {
        max_working_bytes: required + 4096,
        ..config
    };
    let task = parallelism(8);
    assert_eq!(links(&build(&entries, exact, Some(&task))), expected);
}

#[test]
fn cancellation_during_parallel_construction_returns_no_graph() {
    let entries = entries(3_000, 8);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone())
        .with_admitted_parallelism(NonZeroUsize::new(4).unwrap());
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        token.cancel();
    });
    let result = DiskAnnIndex::build(
        8,
        VectorDistance::Euclidean,
        entries,
        DiskAnnBuildConfig {
            max_degree: 16,
            build_beam: 64,
            ..Default::default()
        },
        Some(&task),
    );
    canceller.join().unwrap();
    assert!(
        matches!(result, Err(ProjectionError::Cancelled(_))),
        "{result:?}"
    );
}

#[test]
fn batches_double_from_one_up_to_one_percent_and_cover_every_insertion() {
    for count in [1, 2, 7, 50, 51, 384, 10_000] {
        let mut start = 0;
        let mut sizes = Vec::new();
        while start < count {
            let size = build::batch_size_for_test(start, count);
            assert!(size >= 1);
            sizes.push(size);
            start += size;
        }
        assert_eq!(start, count);
        let largest = (count / 100).max(1);
        assert!(sizes.iter().all(|size| *size <= largest), "{count}");
        assert_eq!(sizes[0], 1);
    }
}

/// FNV-1a over every link list, each followed by a separator byte.
fn link_hash(graph: &DiskAnnIndex) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for ordinal in 0..graph.len() as u32 {
        for neighbor in graph.neighbors(ordinal).unwrap() {
            for byte in neighbor.to_le_bytes() {
                hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
        hash = (hash ^ 0xff).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The single-worker graph is the one the sequential builder produced before
/// batched construction (`072699aa2`), link for link.
#[test]
fn one_worker_reproduces_the_previous_sequential_graph() {
    let entries = entries(600, 8);
    for (lsg, expected) in [
        (None, 0xd89d_a814_d178_b54au64),
        (Some(LsgBuildConfig::default()), 0xab63_50f0_7c9d_991f),
    ] {
        let config = DiskAnnBuildConfig {
            max_degree: 16,
            build_beam: 32,
            lsg,
            ..Default::default()
        };
        assert_eq!(
            link_hash(&build(&entries, config, None)),
            expected,
            "lsg={}",
            lsg.is_some()
        );
        // A multi-core task whose budget is exactly the sequential bound (as
        // before batching) builds, and falls back to the sequential graph.
        let required = DiskAnnIndex::build_memory_bound(entries.len(), 8, config).unwrap();
        let tight = DiskAnnBuildConfig {
            max_working_bytes: required,
            ..config
        };
        assert_eq!(
            link_hash(&build(&entries, tight, Some(&parallelism(8)))),
            expected
        );
    }
}
