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
use crate::RelationalValue;
use std::collections::VecDeque;

#[derive(Clone)]
struct Case {
    request: LockRequest,
    atoms: BTreeSet<u16>,
    rank: u8,
    dynamic_bytes: usize,
    broad: Option<usize>,
}

impl Case {
    fn request(&self, exclusive: bool) -> LockRequest {
        let mut request = self.request.clone();
        request.mode = if exclusive {
            LockMode::Exclusive
        } else {
            LockMode::Shared
        };
        request
    }

    fn bytes(&self) -> usize {
        // ABI widths are measured, while payload accounting is independent
        // of the production estimator and remains portable to 32-bit hosts.
        size_of::<HeldLock>() + self.dynamic_bytes
    }
}

fn next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn index(state: &mut u64, len: usize) -> usize {
    (next(state) % len as u64) as usize
}

fn bound(value: Option<(i16, bool)>) -> Bound<RelationalKey> {
    match value {
        None => Bound::Unbounded,
        Some((value, included)) => {
            let key = RelationalKey(vec![RelationalValue::BigInt(i64::from(value))]);
            if included {
                Bound::Included(key)
            } else {
                Bound::Excluded(key)
            }
        }
    }
}

fn cases() -> Vec<Case> {
    let mut cases = vec![Case {
        request: LockRequest::database(LockMode::Shared),
        atoms: BTreeSet::new(),
        rank: 0,
        dynamic_bytes: 0,
        broad: None,
    }];
    // Doubled coordinates include an atom between every pair of endpoints.
    // Tail atoms distinguish finite bounds from unbounded intervals. A spare
    // index namespace keeps a full index range narrower than a table lock.
    let endpoints = [
        None,
        Some((-2, true)),
        Some((-2, false)),
        Some((0, true)),
        Some((0, false)),
        Some((2, true)),
        Some((2, false)),
    ];
    for (table_id, table) in ["messages", "threads"].into_iter().enumerate() {
        let table_atom =
            |column: usize, point: i16| (table_id * 39 + column * 13 + (point + 6) as usize) as u16;
        let broad = cases.len();
        cases.push(Case {
            request: LockRequest::relational_table(LockMode::Shared, table),
            atoms: (0..3)
                .flat_map(|column| (-6..=6).map(move |point| table_atom(column, point)))
                .collect(),
            rank: 1,
            dynamic_bytes: table.len(),
            broad: None,
        });
        for (column_id, column) in ["id", "owner_id"].into_iter().enumerate() {
            for lower in endpoints {
                for upper in endpoints {
                    let start =
                        lower.map_or(-6, |(value, included)| value * 2 + i16::from(!included));
                    let end = upper.map_or(6, |(value, included)| value * 2 - i16::from(!included));
                    if start > end {
                        continue;
                    }
                    let bound_bytes = size_of::<RelationalKey>() + size_of::<RelationalValue>() + 8;
                    cases.push(Case {
                        request: LockRequest::relational_range(
                            LockMode::Shared,
                            table,
                            vec![column.to_string()],
                            bound(lower),
                            bound(upper),
                        ),
                        atoms: (start..=end)
                            .map(|point| table_atom(column_id, point))
                            .collect(),
                        rank: 2,
                        dynamic_bytes: table.len()
                            + size_of::<String>()
                            + column.len()
                            + usize::from(lower.is_some()) * bound_bytes
                            + usize::from(upper.is_some()) * bound_bytes,
                        broad: Some(broad),
                    });
                }
            }
        }
    }
    let fixed = [
        (
            LockRequest::graph_allocation(GraphAllocationKind::Node),
            3,
            200,
            0,
        ),
        (
            LockRequest::graph_allocation(GraphAllocationKind::Relationship),
            3,
            201,
            0,
        ),
        (
            LockRequest::graph_label(LockMode::Shared, "Memory"),
            4,
            210,
            6,
        ),
        (
            LockRequest::graph_label(LockMode::Shared, "Source"),
            4,
            211,
            6,
        ),
        (
            LockRequest::graph_relationship_type(LockMode::Shared, "EDGE"),
            5,
            220,
            4,
        ),
        (
            LockRequest::graph_relationship_type(LockMode::Shared, "LINK"),
            5,
            221,
            4,
        ),
        (LockRequest::graph_node(LockMode::Shared, 1), 6, 230, 0),
        (LockRequest::graph_node(LockMode::Shared, 2), 6, 231, 0),
        (
            LockRequest::graph_node_delete_guard(LockMode::Shared, 1),
            7,
            240,
            0,
        ),
        (
            LockRequest::graph_node_delete_guard(LockMode::Shared, 2),
            7,
            241,
            0,
        ),
        (
            LockRequest::graph_relationship(LockMode::Shared, 1),
            8,
            250,
            0,
        ),
        (
            LockRequest::graph_relationship(LockMode::Shared, 2),
            8,
            251,
            0,
        ),
    ];
    for (request, rank, atom, dynamic_bytes) in fixed {
        cases.push(Case {
            request,
            atoms: BTreeSet::from([atom]),
            rank,
            dynamic_bytes,
            broad: None,
        });
    }
    for node in 1..=2 {
        for (direction_id, direction) in [
            GraphAdjacencyDirection::Outgoing,
            GraphAdjacencyDirection::Incoming,
        ]
        .into_iter()
        .enumerate()
        {
            let broad = cases.len();
            for rel_type in [None, Some(0), Some(1), Some(2)] {
                let atoms = (0..4)
                    .filter(|kind| rel_type.is_none_or(|value| value == *kind))
                    .map(|kind| 1000 + node as u16 * 8 + direction_id as u16 * 4 + kind as u16)
                    .collect();
                cases.push(Case {
                    request: LockRequest::graph_adjacency(
                        LockMode::Shared,
                        node,
                        rel_type,
                        direction,
                    ),
                    atoms,
                    rank: 9,
                    dynamic_bytes: 0,
                    broad: rel_type.map(|_| broad),
                });
            }
        }
    }
    cases[0].atoms = cases
        .iter()
        .skip(1)
        .flat_map(|case| case.atoms.iter().copied())
        .collect();
    cases
}

fn matrix(cases: &[Case]) -> usize {
    let mut checks = 0;
    for (left_id, left) in cases.iter().enumerate() {
        for left_exclusive in [false, true] {
            let request = left.request(left_exclusive);
            let mut table = LockTable::default();
            table.grant(1, request.clone()).unwrap();
            assert_eq!(table.estimated_bytes, left.bytes());
            for (right_id, right) in cases.iter().enumerate() {
                for right_exclusive in [false, true] {
                    let probe = right.request(right_exclusive);
                    let conflict = (left_exclusive || right_exclusive)
                        && !left.atoms.is_disjoint(&right.atoms);
                    let covered = (left_exclusive || !right_exclusive)
                        && left.atoms.is_superset(&right.atoms);
                    assert_eq!(
                        table.blockers(2, &probe),
                        if conflict {
                            BTreeSet::from([1])
                        } else {
                            BTreeSet::new()
                        },
                        "overlap left={left_id} right={right_id}"
                    );
                    assert_eq!(
                        table.covers_all(1, std::slice::from_ref(&probe)),
                        covered,
                        "coverage left={left_id} right={right_id}"
                    );
                    assert!(table.blockers(1, &probe).is_empty());
                    let order = request.acquisition_cmp(&probe);
                    assert_eq!(order, probe.acquisition_cmp(&request).reverse());
                    assert_eq!(order == Ordering::Equal, request == probe);
                    if left.rank != right.rank {
                        assert_eq!(order, left.rank.cmp(&right.rank));
                    }
                    if request.target == probe.target {
                        assert_eq!(order, right_exclusive.cmp(&left_exclusive));
                    }
                    checks += 1;
                }
            }
        }
    }
    checks
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Held {
    owner: u64,
    case: usize,
    exclusive: bool,
}

fn requests(held: &[Held], cases: &[Case], owner: u64) -> Vec<LockRequest> {
    held.iter()
        .filter(|held| held.owner == owner)
        .map(|held| cases[held.case].request(held.exclusive))
        .collect()
}

fn covers(left: Held, right: Held, cases: &[Case]) -> bool {
    left.owner == right.owner
        && (left.exclusive || !right.exclusive)
        && cases[left.case].atoms.is_superset(&cases[right.case].atoms)
}

fn blockers(held: &[Held], request: Held, cases: &[Case]) -> BTreeSet<u64> {
    held.iter()
        .filter(|item| {
            item.owner != request.owner
                && (item.exclusive || request.exclusive)
                && !cases[item.case]
                    .atoms
                    .is_disjoint(&cases[request.case].atoms)
        })
        .map(|item| item.owner)
        .collect()
}

fn bytes(held: &[Held], cases: &[Case]) -> usize {
    held.iter().map(|held| cases[held.case].bytes()).sum()
}

fn normalized(held: &[Held], mut request: Held, cases: &[Case], threshold: usize) -> Held {
    if let Some(broad) = cases[request.case].broad {
        let narrow: Vec<_> = held
            .iter()
            .filter(|item| item.owner == request.owner && cases[item.case].broad == Some(broad))
            .collect();
        if narrow.len() + 1 > threshold {
            request.case = broad;
            request.exclusive |= narrow.iter().any(|item| item.exclusive);
        }
    }
    request
}

fn state_campaign(seed: u64, cases: &[Case]) -> usize {
    let mut random = seed;
    let limits = LockTableLimits {
        max_entries: 5 + seed as usize % 4,
        max_bytes: (size_of::<HeldLock>() + 100) * (3 + seed as usize % 4),
        escalation_entries_per_table: 2,
    };
    let mut table = LockTable::with_limits(limits);
    let mut held = Vec::<Held>::new();
    let mut saved = BTreeMap::<u64, Vec<Held>>::new();
    for step in 0..128 {
        let owner = (next(&mut random) % 4) + 1;
        match next(&mut random) % 8 {
            0 => {
                table.release_transaction(owner);
                held.retain(|item| item.owner != owner);
                saved.remove(&owner);
            }
            1 => {
                assert_eq!(table.savepoint(owner), requests(&held, cases, owner));
                saved.insert(
                    owner,
                    held.iter()
                        .filter(|item| item.owner == owner)
                        .copied()
                        .collect(),
                );
            }
            2 => {
                if let Some(snapshot) = saved.remove(&owner) {
                    table.restore_transaction(owner, requests(&snapshot, cases, owner));
                    held.retain(|item| item.owner != owner);
                    held.extend(snapshot);
                }
            }
            _ => {
                let raw = Held {
                    owner,
                    case: index(&mut random, cases.len()),
                    exclusive: next(&mut random) & 1 != 0,
                };
                let requested = normalized(&held, raw, cases, limits.escalation_entries_per_table);
                assert_eq!(
                    table.normalized_request(owner, cases[raw.case].request(raw.exclusive)),
                    cases[requested.case].request(requested.exclusive),
                    "seed={seed} step={step} escalation"
                );
                let expected_blockers = blockers(&held, requested, cases);
                assert_eq!(
                    table.blockers(owner, &cases[requested.case].request(requested.exclusive)),
                    expected_blockers
                );
                if expected_blockers.is_empty() {
                    let covered = held.iter().any(|item| covers(*item, requested, cases));
                    let mut candidate = held.clone();
                    if !covered {
                        candidate.retain(|item| !covers(requested, *item, cases));
                        candidate.push(requested);
                    }
                    let admitted = covered
                        || (candidate.len() <= limits.max_entries
                            && bytes(&candidate, cases) <= limits.max_bytes);
                    let result =
                        table.grant(owner, cases[requested.case].request(requested.exclusive));
                    assert_eq!(
                        result.is_ok(),
                        admitted,
                        "seed={seed} step={step} admission"
                    );
                    if admitted {
                        held = candidate;
                    } else {
                        let error = result.unwrap_err();
                        assert!(matches!(error, HawDBError::Execution(_)));
                        assert!(error
                            .to_string()
                            .contains("lock table resource budget exceeded"));
                    }
                }
            }
        }
        assert_eq!(
            table.lock_count(),
            held.len(),
            "seed={seed} step={step} entry count"
        );
        assert_eq!(
            table.estimated_bytes,
            bytes(&held, cases),
            "seed={seed} step={step} residency"
        );
        for owner in 1..=4 {
            assert_eq!(table.savepoint(owner), requests(&held, cases, owner));
            let probe = Held {
                owner,
                case: index(&mut random, cases.len()),
                exclusive: next(&mut random) & 1 != 0,
            };
            let request = cases[probe.case].request(probe.exclusive);
            assert_eq!(
                table.blockers(owner, &request),
                blockers(&held, probe, cases)
            );
            assert_eq!(
                table.covers_all(owner, &[request]),
                held.iter().any(|item| covers(*item, probe, cases))
            );
        }
        let a = &cases[index(&mut random, cases.len())].request(false);
        let b = &cases[index(&mut random, cases.len())].request(true);
        let c = &cases[index(&mut random, cases.len())].request(false);
        if a.acquisition_cmp(b).is_le() && b.acquisition_cmp(c).is_le() {
            assert!(a.acquisition_cmp(c).is_le());
        }
    }
    128
}

fn reachable(edges: &BTreeMap<u64, BTreeSet<u64>>, start: u64, target: u64) -> bool {
    let mut queue: VecDeque<_> = edges.get(&start).into_iter().flatten().copied().collect();
    let mut seen = BTreeSet::new();
    while let Some(node) = queue.pop_front() {
        if node == target {
            return true;
        }
        if seen.insert(node) {
            queue.extend(edges.get(&node).into_iter().flatten().copied());
        }
    }
    false
}

fn wait_campaign(seed: u64) -> usize {
    let mut graph = WaitForGraph::default();
    let mut edges = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut random = seed;
    for step in 0..128 {
        let owner = next(&mut random) % 6 + 1;
        match next(&mut random) % 5 {
            0 => {
                graph.clear_waiter(owner);
                edges.remove(&owner);
            }
            1 => {
                graph.remove_transaction(owner);
                edges.remove(&owner);
                for blockers in edges.values_mut() {
                    blockers.remove(&owner);
                }
                edges.retain(|_, blockers| !blockers.is_empty());
            }
            _ => {
                let mask = next(&mut random);
                let blockers: BTreeSet<_> =
                    (1..=6).filter(|node| mask & (1 << node) != 0).collect();
                if blockers.is_empty() {
                    edges.remove(&owner);
                } else {
                    edges.insert(owner, blockers.clone());
                }
                let cyclic = reachable(&edges, owner, owner);
                let result = graph.register(owner, &blockers);
                assert_eq!(result.is_err(), cyclic, "seed={seed} step={step} deadlock");
                if cyclic {
                    let error = result.unwrap_err();
                    assert!(matches!(error, HawDBError::Execution(_)));
                    let message = error.to_string();
                    let witness: Vec<u64> = message
                        .split("wait cycle: ")
                        .nth(1)
                        .unwrap()
                        .split(" -> ")
                        .map(|node| node.parse().unwrap())
                        .collect();
                    assert_eq!(witness.first(), Some(&owner));
                    assert_eq!(witness.last(), Some(&owner));
                    assert_eq!(
                        witness[..witness.len() - 1]
                            .iter()
                            .collect::<BTreeSet<_>>()
                            .len(),
                        witness.len() - 1
                    );
                    for edge in witness.windows(2) {
                        assert!(edges[&edge[0]].contains(&edge[1]));
                    }
                    edges.remove(&owner);
                }
            }
        }
        assert_eq!(graph.edges, edges, "seed={seed} step={step} wait edges");
    }
    128
}

#[test]
fn lock_admission_exact_bytes_and_replacement_are_atomic() {
    let cases = cases();
    for case in &cases {
        for (max_bytes, admitted) in [
            (case.bytes() - 1, false),
            (case.bytes(), true),
            (case.bytes() + 1, true),
        ] {
            let mut table = LockTable::with_limits(LockTableLimits {
                max_entries: 1,
                max_bytes,
                escalation_entries_per_table: 2,
            });
            assert_eq!(table.grant(1, case.request(true)).is_ok(), admitted);
            assert_eq!(table.lock_count(), usize::from(admitted));
            assert_eq!(
                table.estimated_bytes,
                if admitted { case.bytes() } else { 0 }
            );
        }
    }
    let narrow = cases.iter().position(|case| case.broad.is_some()).unwrap();
    let mut table = LockTable::with_limits(LockTableLimits {
        max_entries: 1,
        max_bytes: usize::MAX,
        escalation_entries_per_table: 1,
    });
    table.grant(1, cases[narrow].request(false)).unwrap();
    let snapshot = table.savepoint(1);
    let original_bytes = table.estimated_bytes;
    let unrelated = LockRequest::graph_node(LockMode::Exclusive, 99);
    assert!(table.grant(2, unrelated).is_err());
    assert_eq!(table.savepoint(1), snapshot);
    assert_eq!(table.estimated_bytes, original_bytes);
    table.grant(1, cases[narrow].request(true)).unwrap();
    assert_eq!(table.lock_count(), 1);
    assert_eq!(table.estimated_bytes, original_bytes);
    table.restore_transaction(1, snapshot.clone());
    assert_eq!(table.savepoint(1), snapshot);
    table.release_transaction(1);
    assert_eq!(table.estimated_bytes, 0);
}

#[test]
fn widening_over_byte_budget_preserves_the_original_lock() {
    let text_key = |value: String| RelationalKey(vec![RelationalValue::Text(value)]);
    let original = LockRequest::relational_point(
        LockMode::Shared,
        "messages",
        vec!["id".to_string()],
        text_key("m".to_string()),
    );
    let original_bytes = size_of::<HeldLock>()
        + "messages".len()
        + size_of::<String>()
        + "id".len()
        + 2 * (size_of::<RelationalKey>() + size_of::<RelationalValue>() + 1);
    let mut table = LockTable::with_limits(LockTableLimits {
        max_entries: 1,
        max_bytes: original_bytes,
        escalation_entries_per_table: usize::MAX,
    });
    table.grant(1, original.clone()).unwrap();
    let widening = LockRequest::relational_range(
        LockMode::Exclusive,
        "messages",
        vec!["id".to_string()],
        Bound::Included(text_key("a".repeat(64))),
        Bound::Included(text_key("z".repeat(64))),
    );
    let error = table.grant(1, widening.clone()).unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)));
    assert!(error
        .to_string()
        .contains("lock table resource budget exceeded"));
    assert_eq!(table.savepoint(1), vec![original.clone()]);
    assert_eq!(table.lock_count(), 1);
    assert_eq!(table.estimated_bytes, original_bytes);
    assert!(table.blockers(2, &original).is_empty());
    assert_eq!(table.blockers(2, &widening), BTreeSet::from([1]));
    assert!(!table.covers_all(1, &[widening]));
}

fn campaign(seeds: u64) {
    let cases = cases();
    let pairs = matrix(&cases);
    let mut lock_steps = 0;
    let mut wait_steps = 0;
    for seed in 0..seeds {
        lock_steps += state_campaign(seed, &cases);
        wait_steps += wait_campaign(seed);
    }
    eprintln!("transaction-locks-v1 seeds={seeds} targets={} mode_pairs={pairs} lock_steps={lock_steps} wait_steps={wait_steps}", cases.len());
}

#[test]
fn transaction_locks_differential_smoke() {
    campaign(2);
}

#[test]
#[ignore = "explicit local transaction lock state-machine campaign"]
fn transaction_locks_differential_campaign() {
    campaign(128);
}
