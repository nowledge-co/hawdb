// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::cell::RefCell;

struct FeedbackGraph {
    count: usize,
    eligible: usize,
    sparse_ids: bool,
    useful_links: bool,
    scored: RefCell<Vec<u32>>,
    cancel: Option<(usize, RuntimeCancellationToken)>,
}

impl DiskAnnRead for FeedbackGraph {
    fn len(&self) -> usize {
        self.count
    }
    fn dimension(&self) -> usize {
        2
    }
    fn metric(&self) -> VectorDistance {
        VectorDistance::Euclidean
    }
    fn max_degree(&self) -> usize {
        16
    }
    fn entry_point(&self) -> Option<u32> {
        Some(0)
    }
    fn ids_are_sorted(&self) -> bool {
        true
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        Ok(if self.sparse_ids {
            u64::from(ordinal) * 3 + 7
        } else {
            u64::from(ordinal)
        })
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        Ok((0..16).map(move |offset| {
            (if self.useful_links {
                (ordinal as usize + offset + 1) % self.eligible
            } else {
                self.eligible + (ordinal as usize * 17 + offset) % (self.count - self.eligible)
            }) as u32
        }))
    }
    fn distance(&self, _: &[f32], ordinal: u32, _: Option<&RuntimeTaskContext>) -> Result<f64> {
        assert!(
            (ordinal as usize) < self.eligible,
            "excluded payload must never be read"
        );
        let mut scored = self.scored.borrow_mut();
        assert!(
            !scored.contains(&ordinal),
            "a path switch must not rescore visited candidates"
        );
        scored.push(ordinal);
        if let Some((count, token)) = &self.cancel
            && scored.len() == *count
        {
            token.cancel();
        }
        Ok((self.eligible - ordinal as usize) as f64)
    }
}

#[test]
fn diskann_feedback_switches_once_and_matches_the_complete_filtered_domain() {
    for sparse_ids in [false, true] {
        for ratio in [10, 100, 1000] {
            for eligible in [33, 64, 129] {
                let graph = FeedbackGraph {
                    count: eligible * ratio,
                    eligible,
                    sparse_ids,
                    useful_links: false,
                    scored: RefCell::default(),
                    cancel: None,
                };
                let ids: Vec<_> = (0..eligible as u32)
                    .map(|id| graph.id(id).unwrap())
                    .collect();
                let result = search_diskann(
                    &graph,
                    &[0.0, 0.0],
                    5,
                    Some(CandidateSet::exact(&ids)),
                    DiskAnnSearchOptions {
                        beam_width: 8,
                        max_visited: 8192,
                        ..Default::default()
                    },
                )
                .unwrap();
                assert!(
                    result.report.adaptive_exact_scan,
                    "ratio={ratio}, eligible={eligible}, sparse={sparse_ids}"
                );
                assert_eq!(result.report.distance_comparisons, eligible);
                assert_eq!(
                    result.hits,
                    (eligible - 5..eligible)
                        .rev()
                        .map(|ordinal| DiskAnnHit {
                            id: graph.id(ordinal as u32).unwrap(),
                            distance: (eligible - ordinal) as f64,
                        })
                        .collect::<Vec<_>>()
                );
                assert!(
                    result.report.visited_nodes
                        + result.report.id_lookup_steps
                        + result.report.seed_steps
                        <= 8192
                );
            }
        }
    }
}

#[test]
fn diskann_feedback_keeps_productive_navigation_and_preserves_refusal() {
    for (useful_links, work) in [(true, 8192), (false, 80)] {
        let graph = FeedbackGraph {
            count: 6400,
            eligible: 64,
            sparse_ids: false,
            useful_links,
            scored: RefCell::default(),
            cancel: None,
        };
        let ids: Vec<_> = (0..64).collect();
        let result = search_diskann(
            &graph,
            &[0.0, 0.0],
            5,
            Some(CandidateSet::exact(&ids)),
            DiskAnnSearchOptions {
                beam_width: 8,
                max_visited: work,
                ..Default::default()
            },
        );
        if useful_links {
            assert!(!result.unwrap().report.adaptive_exact_scan);
        } else {
            assert!(matches!(result, Err(ProjectionError::Admission(_))));
            assert!(graph.scored.borrow().len() < graph.eligible);
        }
    }
}

#[test]
fn diskann_feedback_cancellation_during_exact_completion_returns_no_output() {
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let graph = FeedbackGraph {
        count: 6400,
        eligible: 64,
        sparse_ids: false,
        useful_links: false,
        scored: RefCell::default(),
        cancel: Some((12, token)),
    };
    let ids: Vec<_> = (0..64).collect();
    assert!(matches!(
        search_diskann(
            &graph,
            &[0.0, 0.0],
            5,
            Some(CandidateSet::exact(&ids)),
            DiskAnnSearchOptions {
                beam_width: 8,
                task: Some(&task),
                ..Default::default()
            },
        ),
        Err(ProjectionError::Cancelled(_))
    ));
    assert_eq!(graph.scored.borrow().len(), 12);
}

#[test]
#[ignore = "local filtered DiskANN feedback differential campaign"]
fn diskann_feedback_differential_campaign() {
    diskann_feedback_switches_once_and_matches_the_complete_filtered_domain();
    diskann_feedback_keeps_productive_navigation_and_preserves_refusal();
    diskann_feedback_cancellation_during_exact_completion_returns_no_output();
    diskann_small_beam_recall_is_compared_with_exhaustive_filtered_groundtruth();
}

fn options(count: usize) -> DiskAnnSearchOptions<'static> {
    DiskAnnSearchOptions {
        beam_width: count.max(1),
        ..Default::default()
    }
}

fn graph(entries: Vec<(u64, Vec<f32>)>, metric: VectorDistance, lsg: bool) -> DiskAnnIndex {
    DiskAnnIndex::build(
        2,
        metric,
        entries,
        DiskAnnBuildConfig {
            max_degree: 4,
            build_beam: 16,
            lsg: lsg.then_some(LsgBuildConfig::default()),
            ..Default::default()
        },
        None,
    )
    .unwrap()
}

#[test]
fn diskann_build_preflight_refuses_unrepresentable_count_without_allocating() {
    if let Some(count) = (u32::MAX as usize).checked_add(1) {
        assert!(matches!(
            DiskAnnIndex::build_memory_bound(count, 2, Default::default()),
            Err(ProjectionError::InvalidConfiguration(_))
        ));
    }
}

#[test]
fn diskann_full_beam_matches_exhaustive_distances_with_and_without_lsg() {
    let entries: Vec<_> = (0..61)
        .map(|index| {
            (
                index * 3 + 7,
                vec![((index * 17) % 41) as f32 - 20.0, index as f32 - 30.5],
            )
        })
        .collect();
    for metric in [VectorDistance::Cosine, VectorDistance::Euclidean] {
        for lsg in [false, true] {
            let graph = graph(entries.clone(), metric, lsg);
            let mut expected: Vec<_> = entries
                .iter()
                .map(|(id, vector)| DiskAnnHit {
                    id: *id,
                    distance: distance(metric, &[2.0, -1.0], vector, None).unwrap(),
                })
                .collect();
            expected.sort_by(|left, right| {
                left.distance
                    .total_cmp(&right.distance)
                    .then_with(|| left.id.cmp(&right.id))
            });
            let output =
                search_diskann(&graph, &[2.0, -1.0], 17, None, options(graph.len())).unwrap();
            assert_eq!(output.hits, expected[..17], "{metric:?}, LSG={lsg}");
            assert_eq!(output.report.visited_nodes, entries.len());
            assert_eq!(output.report.distance_comparisons, entries.len());
            assert!(graph.retained_bytes() <= graph.config().max_working_bytes);
            for ordinal in 0..graph.len() as u32 {
                let neighbors: Vec<_> = graph.neighbors(ordinal).unwrap().collect();
                assert!(neighbors.len() <= graph.config().max_degree);
                assert!(!neighbors.is_empty());
            }
        }
    }
}

/// The matching vertex is reachable only through excluded vertices. Reading
/// any excluded vector fails, so post-filtering an ordinary ANN cannot pass.
struct BridgeGraph {
    scored: RefCell<Vec<u64>>,
}
impl DiskAnnRead for BridgeGraph {
    fn len(&self) -> usize {
        5
    }
    fn dimension(&self) -> usize {
        2
    }
    fn metric(&self) -> VectorDistance {
        VectorDistance::Euclidean
    }
    fn max_degree(&self) -> usize {
        1
    }
    fn entry_point(&self) -> Option<u32> {
        Some(0)
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        Ok(u64::from(ordinal) * 10 + 3)
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        Ok((ordinal + 1 < 5).then_some(ordinal + 1).into_iter())
    }
    fn distance(&self, _: &[f32], ordinal: u32, _: Option<&RuntimeTaskContext>) -> Result<f64> {
        assert_eq!(ordinal, 4, "excluded vector reached the scorer");
        self.scored.borrow_mut().push(self.id(ordinal)?);
        Ok(0.25)
    }
}

#[test]
fn diskann_filtered_navigation_crosses_rejected_vertices_before_scoring() {
    let graph = BridgeGraph {
        scored: RefCell::default(),
    };
    let output = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        Some(CandidateSet::exact(&[43])),
        options(1),
    )
    .unwrap();
    assert_eq!(
        output.hits,
        vec![DiskAnnHit {
            id: 43,
            distance: 0.25
        }]
    );
    assert_eq!(*graph.scored.borrow(), vec![43]);
    assert_eq!(output.report.bridge_nodes, 4);
    assert_eq!(output.report.distance_comparisons, 1);
    assert_eq!(output.report.visited_nodes, 5);
}

#[test]
fn diskann_work_refusal_never_returns_partial_filtered_results() {
    let graph = BridgeGraph {
        scored: RefCell::default(),
    };
    let result = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        Some(CandidateSet::exact(&[43])),
        DiskAnnSearchOptions {
            max_visited: 4,
            ..options(1)
        },
    );
    assert!(matches!(
        result,
        Err(ProjectionError::Admission(
            hawdb_core::HawDBError::ResourceBudgetExceeded(ref error)
        )) if error.resource == hawdb_core::ResourceBudgetKind::WorkUnits
    ));
    assert!(graph.scored.borrow().is_empty());
}

#[test]
fn diskann_parent_work_limit_counts_navigation_before_scoring() {
    let graph = BridgeGraph {
        scored: RefCell::default(),
    };
    let task = RuntimeTaskContext::default().with_work_limit(4);
    let result = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        Some(CandidateSet::exact(&[43])),
        DiskAnnSearchOptions {
            task: Some(&task),
            ..options(1)
        },
    );
    assert!(matches!(
        result,
        Err(ProjectionError::Admission(
            hawdb_core::HawDBError::ResourceBudgetExceeded(ref error)
        )) if error.resource == hawdb_core::ResourceBudgetKind::WorkUnits
    ));
    assert_eq!(task.work_units(), 4);
    assert!(graph.scored.borrow().is_empty());
    let task = RuntimeTaskContext::default().with_work_limit(5);
    let output = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        Some(CandidateSet::exact(&[43])),
        DiskAnnSearchOptions {
            task: Some(&task),
            ..options(1)
        },
    )
    .unwrap();
    assert_eq!(task.work_units(), output.report.visited_nodes);
    assert_eq!(output.hits[0].id, 43);
}

#[test]
fn diskann_query_preflight_matches_the_kernel_workspace_boundary() {
    let graph = BridgeGraph {
        scored: RefCell::default(),
    };
    let bound = options(1).working_memory_bound(5, 1).unwrap();
    assert_eq!(options(1).working_memory_bound(0, 1).unwrap(), 0);
    assert!(matches!(
        search_diskann(
            &graph,
            &[0.0, 0.0],
            1,
            Some(CandidateSet::exact(&[43])),
            DiskAnnSearchOptions {
                max_working_bytes: bound - 1,
                ..options(1)
            },
        ),
        Err(ProjectionError::ResourceBudgetExceeded { required, available })
            if required == bound && available == bound - 1
    ));
    assert!(graph.scored.borrow().is_empty());
    assert_eq!(
        search_diskann(
            &graph,
            &[0.0, 0.0],
            1,
            Some(CandidateSet::exact(&[43])),
            DiskAnnSearchOptions {
                max_working_bytes: bound,
                ..options(1)
            },
        )
        .unwrap()
        .hits[0]
            .id,
        43
    );
}

#[test]
fn diskann_filtered_topk_does_not_let_excluded_near_points_consume_the_window() {
    let entries = (0..80).map(|id| (id, vec![id as f32, 1.0])).collect();
    let graph = graph(entries, VectorDistance::Euclidean, true);
    let output = search_diskann(
        &graph,
        &[0.0, 1.0],
        3,
        Some(CandidateSet::exact(&[70, 75, 79])),
        options(3),
    )
    .unwrap();
    assert_eq!(
        output.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        vec![70, 75, 79]
    );
    assert_eq!(output.report.distance_comparisons, 3);
    assert_eq!(output.report.bridge_nodes, 0);
    assert_eq!(output.report.neighbor_reads, 0);
}

#[test]
fn diskann_small_eligible_domain_uses_id_lookups_without_scanning_rejected_links() {
    let graph = graph(
        (0..512)
            .map(|ordinal| (ordinal * 3 + 7, vec![ordinal as f32, 1.0]))
            .collect(),
        VectorDistance::Euclidean,
        false,
    );
    let ids = [7, 391, 775, 1540];
    let output = search_diskann(
        &graph,
        &[0.0, 1.0],
        4,
        Some(CandidateSet::exact(&ids)),
        DiskAnnSearchOptions {
            max_visited: 96,
            ..options(4)
        },
    )
    .unwrap();
    assert_eq!(
        output.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        ids
    );
    assert_eq!(output.report.distance_comparisons, 4);
    assert_eq!(output.report.visited_nodes, 4);
    assert_eq!(output.report.neighbor_reads, 0);
    assert_eq!(output.report.bridge_nodes, 0);
}

struct SortedBridgeGraph {
    sparse_ids: bool,
    scored: RefCell<Vec<u32>>,
}

impl DiskAnnRead for SortedBridgeGraph {
    fn len(&self) -> usize {
        1024
    }
    fn dimension(&self) -> usize {
        2
    }
    fn max_degree(&self) -> usize {
        1
    }
    fn metric(&self) -> VectorDistance {
        VectorDistance::Euclidean
    }
    fn entry_point(&self) -> Option<u32> {
        Some(0)
    }
    fn ids_are_sorted(&self) -> bool {
        true
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        Ok(if self.sparse_ids {
            u64::from(ordinal) * 3 + 7
        } else {
            u64::from(ordinal)
        })
    }
    fn neighbors(&self, ordinal: u32) -> Result<impl Iterator<Item = u32>> {
        let next = if ordinal < 10 {
            Some(10)
        } else {
            (ordinal < 1023).then_some(ordinal + 1)
        };
        Ok(next.into_iter())
    }
    fn distance(&self, _: &[f32], ordinal: u32, _: Option<&RuntimeTaskContext>) -> Result<f64> {
        assert!(ordinal < 10, "excluded vector reached the scorer");
        self.scored.borrow_mut().push(ordinal);
        Ok(f64::from(10 - ordinal))
    }
}

#[test]
fn diskann_sorted_filter_bounds_rejected_hops_without_reading_rejected_vectors() {
    let graph = SortedBridgeGraph {
        sparse_ids: false,
        scored: RefCell::default(),
    };
    let ids: Vec<_> = (0..10).collect();
    let output = search_diskann(
        &graph,
        &[0.0, 0.0],
        2,
        Some(CandidateSet::exact(&ids)),
        DiskAnnSearchOptions {
            max_visited: 64,
            ..options(4)
        },
    )
    .unwrap();
    assert_eq!(output.hits.len(), 2);
    assert!(output.hits.iter().all(|hit| hit.id < 10));
    assert_eq!(output.report.bridge_nodes, 2);
    assert!(output.report.expanded_nodes <= 5);
    assert!(output.report.neighbor_reads <= 5);
    assert_eq!(output.report.seed_steps, 4);
    assert_eq!(output.report.id_lookup_steps, 2);
}

#[test]
fn diskann_rejected_second_hop_can_be_promoted_once_to_a_first_hop() {
    let graph = SortedBridgeGraph {
        sparse_ids: false,
        scored: RefCell::default(),
    };
    for max_visited in [4, 1024] {
        let mut scratch = Scratch::new(graph.len(), 1, max_visited).unwrap();
        let mut report = DiskAnnSearchReport::default();
        let mut policy = VisitPolicy {
            allowed: |_| Ok(false),
            score: |_| panic!("excluded vector reached the scorer"),
            expand_rejected: false,
        };
        let options = DiskAnnSearchOptions {
            max_visited,
            ..options(1)
        };
        discover(&graph, 11, &mut scratch, options, &mut report, &mut policy).unwrap();
        assert!(scratch.bridges.is_empty());
        policy.expand_rejected = true;
        discover(&graph, 11, &mut scratch, options, &mut report, &mut policy).unwrap();
        discover(&graph, 11, &mut scratch, options, &mut report, &mut policy).unwrap();
        assert_eq!(scratch.bridges, [11]);
        assert_eq!(report.visited_nodes, 1);
        assert_eq!(report.bridge_nodes, 1);
        assert_eq!(report.predicate_checks, 1);
        assert_eq!(report.distance_comparisons, 0);
        scratch.reset(None).unwrap();
        assert_eq!(scratch.seen.state(11), 0);
    }
}

#[test]
fn diskann_unknown_eligible_ids_charge_lookup_work_and_refuse_without_partial_results() {
    let graph = SortedBridgeGraph {
        sparse_ids: true,
        scored: RefCell::default(),
    };
    let task = RuntimeTaskContext::default().with_work_limit(5);
    for task in [None, Some(&task)] {
        let result = search_diskann(
            &graph,
            &[0.0, 0.0],
            1,
            Some(CandidateSet::exact(&[1, 2, 3, 4])),
            DiskAnnSearchOptions {
                max_visited: if task.is_some() { 64 } else { 5 },
                task,
                ..options(4)
            },
        );
        assert!(matches!(result, Err(ProjectionError::Admission(
            hawdb_core::HawDBError::ResourceBudgetExceeded(ref error)
        )) if error.resource == hawdb_core::ResourceBudgetKind::WorkUnits && error.used == 5));
        assert!(graph.scored.borrow().is_empty());
    }
    assert_eq!(task.work_units(), 5);
    let task = RuntimeTaskContext::default().with_work_limit(1024);
    let output = search_diskann(
        &graph,
        &[0.0, 0.0],
        2,
        Some(CandidateSet::exact(&[1, 2, 3, 4, 7, 10, 100_000])),
        DiskAnnSearchOptions {
            task: Some(&task),
            ..options(8)
        },
    )
    .unwrap();
    assert_eq!(
        output.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        [10, 7]
    );
    assert_eq!(output.report.seed_steps, 7);
    assert!(output.report.id_lookup_steps > 7);
    assert_eq!(
        task.work_units(),
        output.report.visited_nodes + output.report.seed_steps + output.report.id_lookup_steps
    );
    assert_eq!(output.report.neighbor_reads, 0);
}

#[test]
fn diskann_eligible_entries_are_unique_bounded_and_deterministic() {
    for count in [1, 2, 7, 128, 1000, u32::MAX as usize, usize::MAX] {
        for samples in [1, 2, 7, 128].map(|samples| samples.min(count)) {
            let positions: Vec<_> = (0..samples)
                .map(|index| seed_position(index, samples, count))
                .collect();
            assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(positions.iter().all(|position| *position < count));
            assert_eq!(
                positions,
                (0..samples)
                    .map(|index| seed_position(index, samples, count))
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn diskann_duplicate_vectors_have_finite_lsg_radii_and_stable_distance_ties() {
    let entries = (0..40).rev().map(|id| (id * 2, vec![1.0, 1.0])).collect();
    let graph = graph(entries, VectorDistance::Cosine, true);
    assert!((0..graph.len() as u32)
        .map(|ordinal| graph.local_radius(ordinal).unwrap())
        .all(|radius| radius.is_finite() && radius > 0.0));
    let output = search_diskann(&graph, &[1.0, 1.0], 7, None, options(40)).unwrap();
    assert_eq!(
        output.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
        vec![0, 2, 4, 6, 8, 10, 12]
    );
    assert!(output.hits.iter().all(|hit| hit.distance.abs() < 1e-14));
}

#[test]
fn diskann_lsg_radii_are_mean_sample_distances_excluding_the_source_point() {
    let graph = DiskAnnIndex::build(
        2,
        VectorDistance::Euclidean,
        vec![
            (7, vec![0.0, 0.0]),
            (11, vec![2.0, 0.0]),
            (19, vec![8.0, 0.0]),
        ],
        DiskAnnBuildConfig {
            lsg: Some(LsgBuildConfig {
                sample_size: 3,
                neighbors: 2,
                exponent: 1.0,
            }),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert_eq!(graph.local_radius(0), Some(5.0));
    assert_eq!(graph.local_radius(1), Some(4.0));
    assert_eq!(graph.local_radius(2), Some(7.0));
}

#[test]
fn diskann_input_order_does_not_change_the_published_graph() {
    let entries: Vec<_> = (0..40).map(|id| (id, vec![id as f32, 1.0])).collect();
    let mut reversed = entries.clone();
    reversed.reverse();
    let first = graph(entries, VectorDistance::Euclidean, true);
    let second = graph(reversed, VectorDistance::Euclidean, true);
    assert_eq!(first.entry_point(), second.entry_point());
    for ordinal in 0..first.len() as u32 {
        assert_eq!(first.local_radius(ordinal), second.local_radius(ordinal));
        assert_eq!(first.id(ordinal).unwrap(), second.id(ordinal).unwrap());
        assert_eq!(
            first.neighbors(ordinal).unwrap().collect::<Vec<_>>(),
            second.neighbors(ordinal).unwrap().collect::<Vec<_>>()
        );
    }
}

#[test]
fn diskann_empty_and_singleton_sources_keep_validation_and_filter_semantics() {
    let empty = graph(Vec::new(), VectorDistance::Euclidean, true);
    assert!(search_diskann(&empty, &[0.0, 0.0], 1, None, options(1))
        .unwrap()
        .hits
        .is_empty());
    let singleton = graph(vec![(17, vec![-1.0, 0.0])], VectorDistance::Cosine, true);
    let output = search_diskann(&singleton, &[1.0, 0.0], 1, None, options(1)).unwrap();
    assert_eq!(output.hits[0].distance, 2.0);
    let output = search_diskann(
        &singleton,
        &[1.0, 0.0],
        1,
        Some(CandidateSet::exact(&[])),
        options(1),
    )
    .unwrap();
    assert!(output.hits.is_empty());
    assert_eq!(output.report.visited_nodes, 0);
    assert!(search_diskann(&empty, &[f32::NAN, 0.0], 1, None, options(1)).is_err());
}

#[test]
fn diskann_invalid_sources_and_settings_are_refused() {
    for entries in [
        vec![(1, vec![1.0, 0.0]), (1, vec![0.0, 1.0])],
        vec![(1, vec![0.0, 0.0])],
        vec![(1, vec![f32::INFINITY, 0.0])],
        vec![(1, vec![1.0])],
    ] {
        assert!(DiskAnnIndex::build(
            2,
            VectorDistance::Cosine,
            entries,
            DiskAnnBuildConfig::default(),
            None
        )
        .is_err());
    }
    for config in [
        DiskAnnBuildConfig {
            max_degree: 1,
            ..Default::default()
        },
        DiskAnnBuildConfig {
            alpha: f64::NAN,
            ..Default::default()
        },
        DiskAnnBuildConfig {
            build_beam: 2,
            ..Default::default()
        },
        DiskAnnBuildConfig {
            lsg: Some(LsgBuildConfig {
                exponent: 0.0,
                ..Default::default()
            }),
            ..Default::default()
        },
        DiskAnnBuildConfig {
            lsg: Some(LsgBuildConfig {
                neighbors: 257,
                ..Default::default()
            }),
            ..Default::default()
        },
    ] {
        assert!(DiskAnnIndex::build(2, VectorDistance::Cosine, Vec::new(), config, None).is_err());
    }
}

#[test]
fn diskann_allowlist_validation_crosses_chunk_boundaries_without_allocation() {
    let graph = graph(vec![(17, vec![1.0, 0.0])], VectorDistance::Cosine, false);
    let mut ids: Vec<_> = (0..8192).collect();
    ids[4096] = ids[4095];
    for set in [
        CandidateSet::exact(&ids),
        CandidateSet {
            ids: &[17],
            exact: false,
        },
    ] {
        assert!(matches!(
            search_diskann(&graph, &[1.0, 0.0], 1, Some(set), options(1)),
            Err(ProjectionError::InvalidConfiguration(_))
        ));
    }
}

#[test]
fn diskann_build_and_query_refuse_memory_before_constructing_workspace() {
    let entries: Vec<_> = (0..20).map(|id| (id, vec![id as f32, 1.0])).collect();
    let result = DiskAnnIndex::build(
        2,
        VectorDistance::Euclidean,
        entries.clone(),
        DiskAnnBuildConfig {
            max_working_bytes: 1,
            ..Default::default()
        },
        None,
    );
    assert!(matches!(
        result,
        Err(ProjectionError::ResourceBudgetExceeded { .. })
    ));
    let graph = graph(entries, VectorDistance::Euclidean, false);
    let result = search_diskann(
        &graph,
        &[0.0, 1.0],
        1,
        None,
        DiskAnnSearchOptions {
            max_working_bytes: 1,
            ..options(8)
        },
    );
    assert!(matches!(
        result,
        Err(ProjectionError::ResourceBudgetExceeded { .. })
    ));
}

#[test]
fn diskann_cancelled_tasks_cannot_build_or_query_even_empty_sources() {
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    token.cancel();
    let result = DiskAnnIndex::build(
        2,
        VectorDistance::Euclidean,
        Vec::new(),
        DiskAnnBuildConfig::default(),
        Some(&task),
    );
    assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    let graph = graph(Vec::new(), VectorDistance::Euclidean, false);
    let result = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        None,
        DiskAnnSearchOptions {
            task: Some(&task),
            ..options(1)
        },
    );
    assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
}

struct VirtualGraph {
    count: usize,
    link: Option<u32>,
}
impl DiskAnnRead for VirtualGraph {
    fn len(&self) -> usize {
        self.count
    }
    fn dimension(&self) -> usize {
        2
    }
    fn metric(&self) -> VectorDistance {
        VectorDistance::Euclidean
    }
    fn max_degree(&self) -> usize {
        1
    }
    fn entry_point(&self) -> Option<u32> {
        Some(0)
    }
    fn id(&self, ordinal: u32) -> Result<u64> {
        Ok(u64::from(ordinal))
    }
    fn neighbors(&self, _: u32) -> Result<impl Iterator<Item = u32>> {
        Ok(self.link.into_iter())
    }
    fn distance(&self, _: &[f32], _: u32, _: Option<&RuntimeTaskContext>) -> Result<f64> {
        Ok(1.0)
    }
}

#[test]
fn diskann_query_workspace_is_bounded_by_work_rather_than_disk_graph_size() {
    let graph = VirtualGraph {
        count: u32::MAX as usize,
        link: None,
    };
    let output = search_diskann(
        &graph,
        &[0.0, 0.0],
        1,
        None,
        DiskAnnSearchOptions {
            beam_width: 4,
            max_visited: 4,
            max_working_bytes: 1024,
            task: None,
        },
    )
    .unwrap();
    assert_eq!(
        output.hits,
        vec![DiskAnnHit {
            id: 0,
            distance: 1.0
        }]
    );
    assert_eq!(output.report.visited_nodes, 1);
}

#[test]
fn diskann_invalid_provider_links_are_refused_before_indexing_workspace() {
    let graph = VirtualGraph {
        count: 3,
        link: Some(3),
    };
    assert!(matches!(
        search_diskann(&graph, &[0.0, 0.0], 1, None, options(1)),
        Err(ProjectionError::CorruptArtifact(_))
    ));
}

#[test]
fn diskann_small_beam_recall_is_compared_with_exhaustive_filtered_groundtruth() {
    let dimension = 12;
    let count = 384;
    let mut random = 0x1234_5678_9abc_def0u64;
    let mut entries = Vec::new();
    for id in 0..count {
        let mut vector = Vec::new();
        for _ in 0..dimension {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            vector.push((random % 10_000) as f32 / 5000.0 - 1.0);
        }
        entries.push((id, vector));
    }
    let mut baseline_links = None;
    for lsg in [false, true] {
        let graph = DiskAnnIndex::build(
            dimension,
            VectorDistance::Euclidean,
            entries.clone(),
            DiskAnnBuildConfig {
                max_degree: 24,
                build_beam: 64,
                lsg: lsg.then_some(LsgBuildConfig {
                    sample_size: 64,
                    ..Default::default()
                }),
                ..Default::default()
            },
            None,
        )
        .unwrap();
        let links: Vec<Vec<_>> = (0..graph.len() as u32)
            .map(|ordinal| graph.neighbors(ordinal).unwrap().collect())
            .collect();
        if let Some(baseline) = &baseline_links {
            assert_ne!(
                &links, baseline,
                "LSG must affect construction rather than just metadata"
            );
        } else {
            baseline_links = Some(links);
        }
        let eligible: Vec<_> = (0..count).filter(|id| id % 4 == 0).collect();
        for allowlist in [None, Some(CandidateSet::exact(&eligible))] {
            let mut matched = 0;
            for query_index in 0..16 {
                let mut query = entries[query_index * 17].1.clone();
                query[0] += 0.1;
                let mut expected: Vec<_> = entries
                    .iter()
                    .filter(|(id, _)| allowlist.is_none_or(|set| set.ids.binary_search(id).is_ok()))
                    .map(|(id, vector)| {
                        (
                            *id,
                            distance(VectorDistance::Euclidean, &query, vector, None).unwrap(),
                        )
                    })
                    .collect();
                expected.sort_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| left.0.cmp(&right.0))
                });
                let output = search_diskann(&graph, &query, 10, allowlist, options(32)).unwrap();
                assert_eq!(output.hits.len(), 10);
                matched += output
                    .hits
                    .iter()
                    .filter(|hit| expected[..10].iter().any(|(id, _)| *id == hit.id))
                    .count();
                assert!(
                    output.report.distance_comparisons
                        <= allowlist.map_or(count as usize, |set| set.ids.len())
                );
            }
            assert!(
                matched >= 152,
                "recall {matched}/160, LSG={lsg}, filtered={}",
                allowlist.is_some()
            );
        }
    }
}
