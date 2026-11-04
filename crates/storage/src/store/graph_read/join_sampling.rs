// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::join_sampling::{
    sample_positions, sampled_rows, JoinHopSample, JoinPathSample, JoinSamplingContext,
    JoinSamplingEstimates, JoinSamplingLimits, JoinSamplingNode, JoinSamplingRefusal,
    JoinSamplingReport, JoinSamplingRequest, NodeSamplingPosting, SamplingError, SamplingResult,
};

enum AdjacencySamplingPosting<'a> {
    Resident(Vec<OrderedAdjacencyEntry>),
    Canonical(crate::canonical_adjacency::CanonicalSamplingPosting<'a>),
}

impl AdjacencySamplingPosting<'_> {
    fn len(&self) -> u64 {
        match self {
            Self::Resident(entries) => entries.len() as u64,
            Self::Canonical(posting) => posting.len(),
        }
    }

    fn select(
        &self,
        ordinal: u64,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<OrderedAdjacencyEntry> {
        match self {
            Self::Resident(entries) => {
                context.probe()?;
                entries
                    .get(ordinal as usize)
                    .copied()
                    .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))
            }
            Self::Canonical(posting) => posting.select(ordinal, context),
        }
    }
}

impl GraphStore {
    /// Estimates both endpoint orientations of a small indexed inner path on
    /// this immutable read view. Preparation, rank selection and predicate
    /// checks share one budget. A refusal publishes no partial estimates.
    pub fn sample_join_path(
        &self,
        request: &JoinSamplingRequest,
        limits: JoinSamplingLimits,
        task: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<JoinSamplingReport> {
        self.ensure_usable()?;
        let context = JoinSamplingContext::new(limits, task);
        let result = context
            .reserve_runtime_memory()
            .and_then(|_permit| self.sample_join_path_inner(request, &context));
        let (estimates, refusal) = match result {
            Ok(estimates) => (Some(estimates), None),
            Err(SamplingError::Refused(reason)) => (None, Some(reason)),
            Err(SamplingError::Storage(error)) => return Err(error),
        };
        Ok(JoinSamplingReport {
            snapshot_epoch: self.commit_epoch(),
            estimates,
            refusal,
            work: context.work(),
        })
    }

    fn sample_join_path_inner(
        &self,
        request: &JoinSamplingRequest,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<JoinSamplingEstimates> {
        context.checkpoint()?;
        if !(1..=2).contains(&request.hops.len())
            || request.nodes.len() != request.hops.len() + 1
            || !(1..=256).contains(&context.limits.max_samples)
            || (request.hops.len() == 2 && request.hops[0].rel_type == request.hops[1].rel_type)
            || request
                .nodes
                .iter()
                .any(|node| !sampling_value_supported(node))
        {
            return Err(SamplingError::Refused(JoinSamplingRefusal::Unsupported));
        }
        // Published projections describe the complete checkpoint base. Do not
        // substitute their counts for a merged visible population after writes.
        if self.canonical_base_out_of_core
            && (!self.nodes.is_empty()
                || !self.relationships.is_empty()
                || !self.node_tombstones.is_empty()
                || !self.relationship_tombstones.is_empty())
        {
            return Err(SamplingError::Refused(JoinSamplingRefusal::ChangedBase));
        }
        let mut domains = Vec::new();
        context.reserve(&mut domains, request.nodes.len())?;
        for node in &request.nodes {
            domains.push(self.sampling_node_domain(node, context)?);
        }
        if domains.iter().any(|domain| domain.ids.is_empty()) {
            return Err(SamplingError::Refused(JoinSamplingRefusal::EmptySample));
        }
        let forward = self.sample_path_orientation(request, &domains, false, context)?;
        let reverse = self.sample_path_orientation(request, &domains, true, context)?;
        Ok(JoinSamplingEstimates { forward, reverse })
    }

    fn sampling_node_domain(
        &self,
        node: &JoinSamplingNode,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<NodeSamplingPosting> {
        context.probe()?;
        let ids = if self.canonical_base_out_of_core {
            self.persistent_property_projection
                .as_ref()
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::Unsupported))?
                .sampling_equality_ids(node.label, &node.property, &node.value, context)?
        } else {
            let posting =
                self.property_index
                    .get(&(node.label, node.property.clone(), node.value.clone()));
            let count = posting.map_or(0, |posting| posting.len());
            context.prepare(count)?;
            let mut ids = Vec::new();
            context.reserve(&mut ids, count)?;
            if let Some(posting) = posting {
                ids.extend(posting.iter().copied());
            }
            ids
        };
        context.checkpoint()?;
        Ok(NodeSamplingPosting { ids })
    }

    fn sampling_adjacency(
        &self,
        source: NodeId,
        rel_type: RelTypeId,
        direction: AdjacencyDirection,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<AdjacencySamplingPosting<'_>> {
        if self.canonical_base_out_of_core {
            let reader = self
                .canonical_adjacency
                .as_ref()
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::Unsupported))?;
            return reader
                .sampling_posting(source, direction, rel_type, context)
                .map(AdjacencySamplingPosting::Canonical);
        }
        context.probe()?;
        let posting = self.adjacency_relationship_ids(source, rel_type, direction);
        let count = posting.map_or(0, AdjacencyPostingList::len);
        // The resident BTreeSet has no order statistics. Copy only an admitted
        // bounded posting, once, and report that preparation rather than hiding
        // a linear nth() traversal behind every random selection.
        context.prepare(count)?;
        let mut entries = Vec::new();
        context.reserve(&mut entries, count)?;
        if let Some(posting) = posting {
            entries.extend(posting.iter_copied());
        }
        context.checkpoint()?;
        Ok(AdjacencySamplingPosting::Resident(entries))
    }

    fn sample_path_orientation(
        &self,
        request: &JoinSamplingRequest,
        domains: &[NodeSamplingPosting],
        reverse: bool,
        context: &JoinSamplingContext<'_>,
    ) -> SamplingResult<JoinPathSample> {
        let seed = if reverse { domains.len() - 1 } else { 0 };
        let source_count = domains[seed].ids.len() as u64;
        let mut random = context.limits.seed
            ^ self.commit_epoch()
            ^ if reverse { 0xbb67_ae85_84ca_a73b } else { 0 };
        let positions = sample_positions(
            source_count,
            context.limits.max_samples,
            &mut random,
            context,
        )?;
        let mut input = Vec::new();
        context.reserve(&mut input, positions.len())?;
        input.extend(
            positions
                .into_iter()
                .map(|position| domains[seed].ids[position as usize]),
        );
        let mut population = source_count as f64;
        let mut work_rows = source_count;
        let mut hops = Vec::new();
        context.reserve(&mut hops, request.hops.len())?;
        for step in 0..request.hops.len() {
            let hop_index = if reverse {
                request.hops.len() - 1 - step
            } else {
                step
            };
            let target = if reverse { hop_index } else { hop_index + 1 };
            let hop = request.hops[hop_index];
            let direction = if reverse {
                reverse_direction(hop.direction)
            } else {
                hop.direction
            };
            let mut postings = BTreeMap::new();
            let mut prefix = Vec::new();
            context.reserve(&mut prefix, input.len())?;
            let mut total = 0u64;
            for &source in &input {
                if let std::collections::btree_map::Entry::Vacant(entry) = postings.entry(source) {
                    context.memory(128 + std::mem::size_of::<AdjacencySamplingPosting<'_>>())?;
                    entry.insert(self.sampling_adjacency(
                        source,
                        hop.rel_type,
                        direction,
                        context,
                    )?);
                }
                total = total
                    .checked_add(postings[&source].len())
                    .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))?;
                prefix.push((source, total));
            }
            if total == 0 {
                return Err(SamplingError::Refused(JoinSamplingRefusal::EmptySample));
            }
            let candidates = population * total as f64 / input.len() as f64;
            let selected =
                sample_positions(total, context.limits.max_samples, &mut random, context)?;
            let drawn = selected.len();
            let mut output = Vec::new();
            context.reserve(&mut output, drawn)?;
            for position in selected {
                let slot = prefix.partition_point(|(_, end)| *end <= position);
                let (source, end) = prefix[slot];
                let posting = &postings[&source];
                let entry = posting.select(position - (end - posting.len()), context)?;
                context.probe()?;
                if domains[target].contains(entry.neighbor_id) {
                    output.push(entry.neighbor_id);
                }
            }
            if output.is_empty() {
                return Err(SamplingError::Refused(JoinSamplingRefusal::EmptySample));
            }
            population = candidates * output.len() as f64 / drawn as f64;
            let candidate_rows = sampled_rows(candidates)?;
            hops.push(JoinHopSample {
                candidate_rows,
                output_rows: sampled_rows(population)?.min(candidate_rows),
            });
            work_rows = work_rows
                .checked_add(candidate_rows)
                .ok_or(SamplingError::Refused(JoinSamplingRefusal::Arithmetic))?;
            input = output;
        }
        context.checkpoint()?;
        Ok(JoinPathSample {
            seed_rows: source_count,
            hops,
            work_rows,
        })
    }
}

fn sampling_value_supported(node: &JoinSamplingNode) -> bool {
    !node.property.is_empty()
        && node.property.len() <= 128
        && (matches!(&node.value, Value::Int(_) | Value::Bool(_))
            || matches!(&node.value, Value::String(value) if value.len() <= 256))
}

fn reverse_direction(direction: AdjacencyDirection) -> AdjacencyDirection {
    match direction {
        AdjacencyDirection::Outgoing => AdjacencyDirection::Incoming,
        AdjacencyDirection::Incoming => AdjacencyDirection::Outgoing,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct Fixture {
        store: GraphStore,
        catalog: Catalog,
        request: JoinSamplingRequest,
        a: [NodeId; 3],
        b: [NodeId; 2],
        _directory: Option<FixtureDirectory>,
    }

    struct FixtureDirectory(std::path::PathBuf);

    impl Drop for FixtureDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn limits() -> JoinSamplingLimits {
        JoinSamplingLimits {
            max_samples: 16,
            max_duration: Duration::from_secs(10),
            ..Default::default()
        }
    }

    fn fixture(persistent: bool) -> Fixture {
        fixture_with_projection_values(persistent, &[])
    }

    fn fixture_with_projection_values(persistent: bool, extra_values: &[Value]) -> Fixture {
        let directory = persistent.then(|| {
            std::env::temp_dir().join(format!(
                "hawdb-join-sampling-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ))
        });
        let mut catalog = Catalog::default();
        let config = WalReplayConfig {
            residency_mode: StorageResidencyMode::OutOfCore,
            ..Default::default()
        };
        let mut store = match &directory {
            Some(path) => GraphStore::open_with_durability_and_replay_config(
                path,
                &mut catalog,
                DurabilityPolicy::SyncOnCheckpoint,
                config,
            )
            .unwrap(),
            None => GraphStore::default(),
        };
        let mut node = |label| {
            store
                .create_node(
                    &mut catalog,
                    label,
                    BTreeMap::from([("kind".into(), Value::Int(1))]),
                )
                .unwrap()
        };
        let a = [node("A"), node("A"), node("A")];
        let b = [node("B"), node("B")];
        let c = node("C");
        for value in extra_values {
            store
                .create_node(
                    &mut catalog,
                    "A",
                    BTreeMap::from([("kind".into(), value.clone())]),
                )
                .unwrap();
        }
        for (source, target, kind) in [
            (a[0], b[0], "R"),
            (a[0], b[0], "R"),
            (a[0], b[1], "R"),
            (a[1], b[0], "R"),
            (b[0], c, "S"),
            (b[0], c, "S"),
            (b[1], c, "S"),
        ] {
            store
                .create_relationship(&mut catalog, source, target, kind, BTreeMap::new())
                .unwrap();
        }
        for label in ["A", "B", "C"] {
            store
                .create_property_index(&mut catalog, label, "kind")
                .unwrap();
        }
        let request = JoinSamplingRequest {
            nodes: ["A", "B", "C"]
                .into_iter()
                .map(|label| JoinSamplingNode {
                    label: catalog.label_id(label).unwrap(),
                    property: "kind".into(),
                    value: Value::Int(1),
                })
                .collect(),
            hops: ["R", "S"]
                .into_iter()
                .map(|kind| crate::join_sampling::JoinSamplingHop {
                    rel_type: catalog.rel_type_id(kind).unwrap(),
                    direction: AdjacencyDirection::Outgoing,
                })
                .collect(),
        };
        if let Some(path) = &directory {
            store.checkpoint(&catalog).unwrap();
            drop(store);
            store = GraphStore::open_with_durability_and_replay_config(
                path,
                &mut catalog,
                DurabilityPolicy::SyncOnCheckpoint,
                config,
            )
            .unwrap();
            assert!(store.canonical_base_out_of_core && store.nodes.is_empty());
        }
        Fixture {
            store,
            catalog,
            request,
            a,
            b,
            _directory: directory.map(FixtureDirectory),
        }
    }

    #[test]
    fn join_sampling_preserves_duplicate_fanout_in_both_directions_and_backends() {
        for persistent in [false, true] {
            let fixture = fixture(persistent);
            let report = fixture
                .store
                .sample_join_path(&fixture.request, limits(), None)
                .unwrap();
            assert_eq!(report.refusal, None);
            let estimates = report.estimates.unwrap();
            assert_eq!(
                estimates.forward.hops,
                vec![
                    JoinHopSample {
                        candidate_rows: 4,
                        output_rows: 4
                    },
                    JoinHopSample {
                        candidate_rows: 7,
                        output_rows: 7
                    },
                ]
            );
            assert_eq!(
                estimates.reverse.hops,
                vec![
                    JoinHopSample {
                        candidate_rows: 3,
                        output_rows: 3
                    },
                    JoinHopSample {
                        candidate_rows: 7,
                        output_rows: 7
                    },
                ]
            );
            assert!(report.work.prepared_entries > 0);
        }
    }

    #[test]
    fn join_sampling_rank_access_keeps_duplicate_relationship_identities() {
        for persistent in [false, true] {
            let fixture = fixture(persistent);
            let context = JoinSamplingContext::new(limits(), None);
            let posting = fixture
                .store
                .sampling_adjacency(
                    fixture.a[0],
                    fixture.request.hops[0].rel_type,
                    AdjacencyDirection::Outgoing,
                    &context,
                )
                .unwrap();
            assert_eq!(posting.len(), 3);
            let first = posting.select(0, &context).unwrap();
            let second = posting.select(1, &context).unwrap();
            let last = posting.select(2, &context).unwrap();
            assert_eq!(
                (first.neighbor_id, second.neighbor_id, last.neighbor_id),
                (fixture.b[0], fixture.b[0], fixture.b[1])
            );
            assert_ne!(first.relationship_id, second.relationship_id);
            let reads = context.work().read_operations;
            assert_eq!(posting.select(0, &context).unwrap(), first);
            assert_eq!(context.work().read_operations, reads);
        }
    }

    #[test]
    fn join_sampling_refuses_before_spending_a_limit_or_publishing_partial_estimates() {
        let fixture = fixture(false);
        for (limits, reason) in [
            (
                JoinSamplingLimits {
                    max_probes: 0,
                    ..limits()
                },
                JoinSamplingRefusal::ProbeBudget,
            ),
            (
                JoinSamplingLimits {
                    max_prepared_entries: 2,
                    ..limits()
                },
                JoinSamplingRefusal::PreparationBudget,
            ),
            (
                JoinSamplingLimits {
                    max_retained_bytes: 0,
                    ..limits()
                },
                JoinSamplingRefusal::MemoryBudget,
            ),
            (
                JoinSamplingLimits {
                    max_duration: Duration::ZERO,
                    ..limits()
                },
                JoinSamplingRefusal::Deadline,
            ),
        ] {
            let report = fixture
                .store
                .sample_join_path(&fixture.request, limits, None)
                .unwrap();
            assert_eq!(report.refusal, Some(reason));
            assert!(report.estimates.is_none());
        }
    }

    #[test]
    fn join_sampling_cold_read_refusal_does_not_poison_or_charge_cached_reads() {
        let fixture = fixture(true);
        let refused = fixture
            .store
            .sample_join_path(
                &fixture.request,
                JoinSamplingLimits {
                    max_read_operations: 0,
                    ..limits()
                },
                None,
            )
            .unwrap();
        assert_eq!(
            refused.refusal,
            Some(JoinSamplingRefusal::ReadOperationBudget)
        );
        assert_eq!(refused.work.read_operations, 0);
        let zero_reads = JoinSamplingLimits {
            max_read_bytes: 0,
            ..limits()
        };
        let refused = fixture
            .store
            .sample_join_path(&fixture.request, zero_reads, None)
            .unwrap();
        assert_eq!(refused.refusal, Some(JoinSamplingRefusal::ReadByteBudget));
        assert_eq!(refused.work.read_bytes, 0);
        let warmed = fixture
            .store
            .sample_join_path(&fixture.request, limits(), None)
            .unwrap();
        assert!(warmed.estimates.is_some());
        let cached = fixture
            .store
            .sample_join_path(&fixture.request, zero_reads, None)
            .unwrap();
        assert_eq!(cached.estimates, warmed.estimates);
        assert_eq!(cached.work.read_bytes, 0);
        fixture.store.ensure_usable().unwrap();
    }

    #[test]
    fn join_sampling_admits_nested_projection_allocations_even_on_cache_hits() {
        let large = Value::List(vec![Value::Null; 128]);
        // First cover a large descriptor bound, then a large interior key
        // whose shallow maximum does not reveal the payload decode cost.
        for values in [
            vec![large.clone()],
            vec![large, Value::List(vec![Value::Int(1)])],
        ] {
            let fixture = fixture_with_projection_values(true, &values);
            let warmed = fixture
                .store
                .sample_join_path(
                    &fixture.request,
                    JoinSamplingLimits {
                        max_probes: 1,
                        ..limits()
                    },
                    None,
                )
                .unwrap();
            assert_eq!(warmed.refusal, Some(JoinSamplingRefusal::ProbeBudget));
            let refused = fixture
                .store
                .sample_join_path(
                    &fixture.request,
                    JoinSamplingLimits {
                        max_retained_bytes: 8 * 1024,
                        max_read_bytes: 0,
                        ..limits()
                    },
                    None,
                )
                .unwrap();
            assert_eq!(refused.refusal, Some(JoinSamplingRefusal::MemoryBudget));
            assert_eq!(
                refused.work.probes, 1,
                "refuse in the first domain before decoding an oversized value"
            );
            assert_eq!(
                refused.work.read_operations, 0,
                "cached reads still require allocation admission"
            );
            assert!(refused.estimates.is_none());
            assert!(fixture
                .store
                .sample_join_path(&fixture.request, limits(), None)
                .unwrap()
                .estimates
                .is_some());
        }
    }

    #[test]
    fn join_sampling_retains_old_population_and_declines_a_changed_canonical_base() {
        for persistent in [false, true] {
            let mut fixture = fixture(persistent);
            let old = fixture.store.snapshot();
            fixture
                .store
                .create_relationship(
                    &mut fixture.catalog,
                    fixture.a[2],
                    fixture.b[1],
                    "R",
                    BTreeMap::new(),
                )
                .unwrap();
            let old_result = old
                .sample_join_path(&fixture.request, limits(), None)
                .unwrap();
            assert_eq!(old_result.estimates.unwrap().forward.hops[1].output_rows, 7);
            let live = fixture
                .store
                .sample_join_path(&fixture.request, limits(), None)
                .unwrap();
            if persistent {
                assert_eq!(live.refusal, Some(JoinSamplingRefusal::ChangedBase));
            } else {
                assert_eq!(live.estimates.unwrap().forward.hops[1].output_rows, 8);
            }
        }
    }

    #[test]
    fn join_sampling_empty_domain_is_not_an_exact_empty_proof_and_cancel_is_an_error() {
        let mut fixture = fixture(false);
        fixture.request.nodes[0].value = Value::Int(99);
        let report = fixture
            .store
            .sample_join_path(&fixture.request, limits(), None)
            .unwrap();
        assert_eq!(report.refusal, Some(JoinSamplingRefusal::EmptySample));
        assert!(report.estimates.is_none());
        let task = hawdb_core::RuntimeTaskContext::default();
        task.cancellation().cancel();
        let error = fixture
            .store
            .sample_join_path(
                &fixture.request,
                JoinSamplingLimits {
                    max_duration: Duration::ZERO,
                    ..limits()
                },
                Some(&task),
            )
            .unwrap_err();
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        let task = hawdb_core::RuntimeTaskContext::default().with_work_limit(0);
        assert!(fixture
            .store
            .sample_join_path(&fixture.request, limits(), Some(&task))
            .is_err());
    }

    #[derive(Debug, Clone, Default)]
    struct Ledger {
        memory: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        io: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        io_acquisitions: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    #[derive(Debug)]
    struct MemoryLease(Ledger, u64);
    impl hawdb_core::RuntimeMemoryPermit for MemoryLease {
        fn bytes(&self) -> u64 {
            self.1
        }
    }
    impl Drop for MemoryLease {
        fn drop(&mut self) {
            self.0
                .memory
                .fetch_sub(self.1 as usize, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl hawdb_core::RuntimeMemoryController for Ledger {
        fn reserve(
            &self,
            bytes: u64,
            ceiling: u64,
        ) -> std::result::Result<
            Box<dyn hawdb_core::RuntimeMemoryPermit>,
            hawdb_core::RuntimeMemoryError,
        > {
            assert!(bytes <= ceiling);
            self.memory
                .fetch_add(bytes as usize, std::sync::atomic::Ordering::SeqCst);
            Ok(Box::new(MemoryLease(self.clone(), bytes)))
        }
    }

    #[derive(Debug)]
    struct IoLease(Ledger);
    impl Drop for IoLease {
        fn drop(&mut self) {
            self.0.io.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    impl hawdb_core::RuntimeIoWaveController for Ledger {
        fn try_acquire(
            &self,
            slots: std::num::NonZeroUsize,
            _task: &hawdb_core::RuntimeTaskContext,
        ) -> std::result::Result<
            Option<Box<dyn hawdb_core::RuntimeIoWavePermit>>,
            hawdb_core::RuntimeIoWaveError,
        > {
            use std::sync::atomic::Ordering::SeqCst;
            assert_eq!(slots.get(), 1);
            assert!(
                self.memory.load(SeqCst) > 0,
                "scratch reservation must precede physical I/O"
            );
            if self.io.compare_exchange(0, 1, SeqCst, SeqCst).is_err() {
                return Ok(None);
            }
            self.io_acquisitions.fetch_add(1, SeqCst);
            Ok(Some(Box::new(IoLease(self.clone()))))
        }
        fn acquire(
            &self,
            _slots: std::num::NonZeroUsize,
            _task: &hawdb_core::RuntimeTaskContext,
        ) -> std::result::Result<
            Box<dyn hawdb_core::RuntimeIoWavePermit>,
            hawdb_core::RuntimeIoWaveError,
        > {
            panic!("optional sampling must not block waiting for I/O capacity");
        }
    }

    #[test]
    fn join_sampling_keeps_runtime_memory_and_io_custody_and_yields_when_busy() {
        use std::sync::atomic::Ordering::SeqCst;
        let fixture = fixture(true);
        let ledger = std::sync::Arc::new(Ledger::default());
        let task = hawdb_core::RuntimeTaskContext::default()
            .with_memory_controller(ledger.clone())
            .with_io_wave_controller(ledger.clone());
        ledger.io.store(1, SeqCst);
        let refused = fixture
            .store
            .sample_join_path(&fixture.request, limits(), Some(&task))
            .unwrap();
        assert_eq!(refused.refusal, Some(JoinSamplingRefusal::IoAdmission));
        assert_eq!(refused.work.read_operations, 0);
        assert_eq!(ledger.memory.load(SeqCst), 0);
        ledger.io.store(0, SeqCst);
        let report = fixture
            .store
            .sample_join_path(&fixture.request, limits(), Some(&task))
            .unwrap();
        assert!(report.estimates.is_some());
        assert!(report.work.read_operations > 0);
        assert_eq!(ledger.io_acquisitions.load(SeqCst), 1);
        assert_eq!(ledger.memory.load(SeqCst), 0);
        assert_eq!(ledger.io.load(SeqCst), 0);
    }
}
