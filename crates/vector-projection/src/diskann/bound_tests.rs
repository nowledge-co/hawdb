// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Early end of a graph search at a result bound from other graphs (#1918).

use super::*;
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::cell::Cell;

/// `distance(ordinal)` over a list of links. No graph here fits the
/// small-domain scan (at most beam * 8 / 3 vertices), unless a test says so.
struct Scripted {
    count: usize,
    metric: VectorDistance,
    sorted: bool,
    links: fn(u32, usize) -> Vec<u32>,
    distance: fn(u32) -> f64,
    scored: Cell<usize>,
    cancel: Option<(usize, RuntimeCancellationToken)>,
}

impl DiskAnnRead for Scripted {
    fn len(&self) -> usize {
        self.count
    }
    fn dimension(&self) -> usize {
        2
    }
    fn metric(&self) -> VectorDistance {
        self.metric
    }
    fn max_degree(&self) -> usize {
        8
    }
    fn entry_point(&self) -> Option<u32> {
        Some(0)
    }
    fn ids_are_sorted(&self) -> bool {
        self.sorted
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        Ok(u64::from(ordinal))
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        Ok((self.links)(ordinal, self.count).into_iter())
    }
    fn distance(&self, _: &[f32], ordinal: u32, _: Option<&RuntimeTaskContext>) -> Result<f64> {
        self.scored.set(self.scored.get() + 1);
        if let Some((count, token)) = &self.cancel
            && self.scored.get() == *count
        {
            token.cancel();
        }
        Ok((self.distance)(ordinal))
    }
}

/// Every vertex links to its eight successors and lies at its ordinal.
fn line() -> Scripted {
    Scripted {
        count: 4096,
        metric: VectorDistance::Euclidean,
        sorted: false,
        links: |ordinal, count| {
            (1..=8)
                .map(|step| (ordinal as usize + step).min(count - 1) as u32)
                .collect()
        },
        distance: f64::from,
        scored: Cell::new(0),
        cancel: None,
    }
}

fn bounded(beam_width: usize, result_bound: Option<f64>) -> DiskAnnSearchOptions<'static> {
    DiskAnnSearchOptions {
        beam_width,
        result_bound,
        ..Default::default()
    }
}

fn run(graph: &Scripted, top_k: usize, options: DiskAnnSearchOptions<'_>) -> DiskAnnSearchOutput {
    search_diskann(graph, &[1.0, 0.0], top_k, None, options).unwrap()
}

#[test]
fn diskann_result_bound_ends_a_traversal_whose_frontier_is_worse() {
    let full = run(&line(), 10, bounded(256, None));
    assert_eq!(full.report.expanded_nodes, 256);
    assert!(!full.report.stopped_at_result_bound);
    for (bound, expanded, stopped) in [
        // The nearest quarter of the beam is always expanded.
        (0.0, 64, true),
        (10.0, 64, true),
        // 1.2 times the bound is still reachable, and nothing beyond it.
        (100.0, 121, true),
        (1e6, 256, false),
    ] {
        let output = run(&line(), 10, bounded(256, Some(bound)));
        assert_eq!(output.report.expanded_nodes, expanded, "{bound}");
        assert_eq!(output.report.stopped_at_result_bound, stopped, "{bound}");
        assert_eq!(output.hits, full.hits, "{bound}");
        let again = run(&line(), 10, bounded(256, Some(bound)));
        assert_eq!((again.hits, again.report), (output.hits, output.report));
    }
    // The rank scales with the beam, with a floor of 64.
    for (beam, expanded) in [(128, 64), (512, 128), (1024, 256)] {
        let output = run(&line(), 10, bounded(beam, Some(1.0)));
        assert_eq!(output.report.expanded_nodes, expanded, "{beam}");
        assert!(output.report.stopped_at_result_bound);
    }
    // A beam below the floor is always expanded.
    let narrow = run(&line(), 10, bounded(64, Some(1.0)));
    assert_eq!(narrow.report.expanded_nodes, 64);
    assert!(!narrow.report.stopped_at_result_bound);
}

#[test]
fn diskann_result_bound_compares_cosine_chord_lengths() {
    // Half the squared chord of 0.1 is 0.005; 1.2 chords are 1.44 times that.
    for (bound, expanded) in [(0.1, 145), (0.2, 256)] {
        let graph = Scripted {
            metric: VectorDistance::Cosine,
            distance: |ordinal| f64::from(ordinal) / 1000.0,
            ..line()
        };
        let output = run(&graph, 10, bounded(256, Some(bound)));
        assert_eq!(output.report.expanded_nodes, expanded, "{bound}");
    }
}

#[test]
fn diskann_result_bound_waits_until_the_traversal_is_within_its_reach() {
    // The frontier stays at distance 1000 and more for 200 vertices before it
    // reaches the neighbors of the query. Stopping there would lose them all.
    let detour = |distance: fn(u32) -> f64| Scripted { distance, ..line() };
    let hidden = |ordinal: u32| {
        if ordinal < 200 {
            1000.0 + f64::from(ordinal)
        } else {
            f64::from(ordinal - 200)
        }
    };
    let expected = run(&detour(hidden), 10, bounded(256, None)).hits;
    assert_eq!(expected[0].id, 200);
    let output = run(&detour(hidden), 10, bounded(256, Some(10.0)));
    assert_eq!(output.hits, expected);
    // Control: one vertex within reach makes the same frontier final.
    let near_entry = |ordinal: u32| match ordinal {
        0 => 11.0,
        1..200 => 1000.0 + f64::from(ordinal),
        _ => f64::from(ordinal - 200),
    };
    let output = run(&detour(near_entry), 10, bounded(256, Some(10.0)));
    assert!(output.report.stopped_at_result_bound);
    assert_eq!(output.hits[0].id, 0);
}

#[test]
fn diskann_result_bound_keeps_the_candidate_count() {
    // Each expansion discovers one vertex, so the beam fills slowly.
    let chain = || Scripted {
        links: |ordinal, count| vec![(ordinal as usize + 1).min(count - 1) as u32],
        ..line()
    };
    let output = run(&chain(), 128, bounded(256, Some(1.0)));
    assert!(output.report.stopped_at_result_bound);
    assert_eq!(output.hits.len(), 128);
    assert_eq!(output.report.expanded_nodes, 127);
    let output = run(&chain(), 10, bounded(256, Some(1.0)));
    assert_eq!(output.report.expanded_nodes, 64);
}

#[test]
fn diskann_result_bound_applies_to_complete_domains_only() {
    let sorted = || Scripted {
        sorted: true,
        ..line()
    };
    let all: Vec<u64> = (0..4096).collect();
    let half: Vec<u64> = (0..4096).step_by(2).collect();
    for (ids, stops) in [(&all, true), (&half, false)] {
        let search = |graph: &Scripted, bound| {
            let set = Some(CandidateSet::exact(ids));
            search_diskann(graph, &[1.0, 0.0], 10, set, bounded(256, bound)).unwrap()
        };
        for sorted_ids in [true, false] {
            let graph = || Scripted {
                sorted: sorted_ids,
                ..sorted()
            };
            let output = search(&graph(), Some(10.0));
            assert_eq!(
                output.report.stopped_at_result_bound,
                stops && sorted_ids,
                "{} {sorted_ids}",
                ids.len()
            );
            if !output.report.stopped_at_result_bound {
                let plain = search(&graph(), None);
                assert_eq!((plain.hits, plain.report), (output.hits, output.report));
            }
        }
    }
}

#[test]
fn diskann_result_bound_does_not_replace_the_small_domain_scan() {
    // The probe decides after 16 expansions, which discover 24 vertices, before
    // the bound is consulted.
    let graph = Scripted {
        count: 336,
        ..line()
    };
    let output = run(&graph, 10, bounded(256, Some(1.0)));
    assert!(output.report.adaptive_exact_scan);
    assert!(!output.report.stopped_at_result_bound);
    assert_eq!(graph.scored.get(), 336);
    // The rank-1 vertex with almost no in-links (#1970, #1971) is still found,
    // whatever other graphs hold.
    let (entries, query) = super::tests::hard_to_reach_entries();
    let graph = DiskAnnIndex::build(
        query.len(),
        VectorDistance::Cosine,
        entries,
        DiskAnnBuildConfig::default(),
        None,
    )
    .unwrap();
    let plain = search_diskann(&graph, &query, 10, None, bounded(256, None)).unwrap();
    assert!(plain.report.adaptive_exact_scan);
    assert_eq!(plain.hits[0].id, 899);
    for bound in [0.0, 0.5, 1.0] {
        let output = search_diskann(&graph, &query, 10, None, bounded(256, Some(bound))).unwrap();
        assert_eq!((&output.hits, &output.report), (&plain.hits, &plain.report));
    }
}

#[test]
fn diskann_result_bound_must_be_a_distance() {
    for bound in [f64::NAN, f64::INFINITY, -1.0] {
        let graph = line();
        let result = search_diskann(&graph, &[1.0, 0.0], 10, None, bounded(256, Some(bound)));
        assert!(matches!(
            result,
            Err(ProjectionError::InvalidConfiguration(_))
        ));
        assert_eq!(graph.scored.get(), 0);
    }
}

#[test]
fn diskann_result_bound_keeps_work_memory_and_cancellation_limits() {
    let options = bounded(256, Some(10.0));
    let visited = run(&line(), 10, options).report.visited_nodes;
    assert!(visited < run(&line(), 10, bounded(256, None)).report.visited_nodes);
    // Work is charged per vertex, and a limit below the shortened traversal
    // still refuses instead of returning a partial result.
    for (limit, admitted) in [(visited, true), (visited - 1, false)] {
        let task = RuntimeTaskContext::default().with_work_limit(limit);
        for options in [
            DiskAnnSearchOptions {
                task: Some(&task),
                ..options
            },
            DiskAnnSearchOptions {
                max_visited: limit,
                ..options
            },
        ] {
            let result = search_diskann(&line(), &[1.0, 0.0], 10, None, options);
            assert_eq!(result.is_ok(), admitted);
        }
        if admitted {
            assert_eq!(task.work_units(), visited);
        }
    }
    // Admission covers the full beam: the bound is not a reservation discount.
    assert_eq!(
        options.working_memory_bound(4096, 10).unwrap(),
        bounded(256, None).working_memory_bound(4096, 10).unwrap()
    );
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let graph = Scripted {
        cancel: Some((visited / 2, token)),
        ..line()
    };
    let result = search_diskann(
        &graph,
        &[1.0, 0.0],
        10,
        None,
        DiskAnnSearchOptions {
            task: Some(&task),
            ..options
        },
    );
    assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    assert_eq!(graph.scored.get(), visited / 2);
}

/// Independent graphs searched in order, as a segmented generation does.
#[test]
fn diskann_result_bound_across_graphs_keeps_recall_and_scores_less() {
    const DIMENSION: usize = 16;
    const GRAPHS: usize = 3;
    const ROWS: usize = 1500;
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut vector = |cluster: Option<u64>| -> Vec<f32> {
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 23) as f32 - 1.0
        };
        let mut vector: Vec<f32> = (0..DIMENSION).map(|_| next()).collect();
        if let Some(cluster) = cluster {
            for value in &mut vector {
                *value *= 0.3;
            }
            vector[(cluster % DIMENSION as u64) as usize] += if cluster < 16 { 2.0 } else { -2.0 };
        }
        vector
    };
    for (clusters, ordered) in [(0, false), (24, false), (24, true)] {
        let cluster = |row: usize| {
            (clusters != 0).then(|| {
                if ordered {
                    // Each graph holds a different part of the space.
                    (row * clusters as usize / (GRAPHS * ROWS)) as u64
                } else {
                    row as u64 % clusters
                }
            })
        };
        let rows: Vec<_> = (0..GRAPHS * ROWS).map(|row| vector(cluster(row))).collect();
        for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
            let graphs: Vec<_> = rows
                .chunks(ROWS)
                .enumerate()
                .map(|(part, rows)| {
                    let entries = rows
                        .iter()
                        .enumerate()
                        .map(|(row, vector)| ((part * ROWS + row) as u64, vector.clone()))
                        .collect();
                    DiskAnnIndex::build(
                        DIMENSION,
                        metric,
                        entries,
                        DiskAnnBuildConfig {
                            max_degree: 16,
                            build_beam: 64,
                            ..Default::default()
                        },
                        None,
                    )
                    .unwrap()
                })
                .collect();
            let (mut scored, mut recall) = ([0usize; 2], [0usize; 2]);
            for query_row in 0..32 {
                let query = vector(cluster(query_row * 187));
                let mut exact: Vec<_> = rows
                    .iter()
                    .enumerate()
                    .map(|(row, value)| {
                        (
                            metric
                                .distance_slice_to_vector_with_work(
                                    &query,
                                    hawdb_core::Vector::new(value).unwrap().as_ref(),
                                    || Ok(()),
                                )
                                .unwrap(),
                            row as u64,
                        )
                    })
                    .collect();
                exact.sort_by(|left, right| left.0.total_cmp(&right.0).then(left.1.cmp(&right.1)));
                for share in [false, true] {
                    let part = usize::from(share);
                    let mut nearest: Vec<(f64, u64)> = Vec::new();
                    for graph in &graphs {
                        let result_bound = (share && nearest.len() == 10).then(|| nearest[9].0);
                        let output =
                            search_diskann(graph, &query, 128, None, bounded(256, result_bound))
                                .unwrap();
                        scored[part] += output.report.distance_comparisons;
                        nearest.extend(output.hits.iter().map(|hit| (hit.distance, hit.id)));
                        nearest.sort_by(|left, right| {
                            left.0.total_cmp(&right.0).then(left.1.cmp(&right.1))
                        });
                        nearest.truncate(10);
                    }
                    recall[part] += nearest
                        .iter()
                        .filter(|hit| exact[..10].iter().any(|truth| truth.1 == hit.1))
                        .count();
                }
            }
            assert_eq!(recall[1], recall[0], "{clusters} {ordered} {metric:?}");
            assert!(recall[0] >= 310, "{recall:?} {clusters} {ordered}");
            assert!(scored[1] <= scored[0]);
            if !ordered {
                assert!(
                    scored[1] * 10 < scored[0] * 9,
                    "{scored:?} {clusters} {metric:?}"
                );
            }
        }
    }
}
