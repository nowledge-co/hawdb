// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Resident search snapshot admission refusals (#1815). Every read that pins
//! the resident search projection funds its retained generation through
//! `SearchIndexReadSnapshot::retain_with_permit`. With too little governor
//! memory left, that refusal must surface as the typed, retryable
//! `HawDBError::RuntimeAdmissionRefused`, leave the handle usable, and succeed
//! on retry once capacity is free.

use super::*;
use crate::executor::ExecutionMemoryConfig;
use crate::{EmbeddedReadSnapshotBudget, RuntimeAdmissionRefusal};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimePermit, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeRetainedMemory,
    RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest,
};

const MEMORY_BUDGET: u64 = 256 * 1024 * 1024;
const SEED: &str = "CALL text_search($text, topK := 1) YIELD id, score \
                    MATCH (m:Memory) WHERE m.id = 'pinned' RETURN m.id AS id";

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(MEMORY_BUDGET),
            result_budget_bytes: 256 * 1024,
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(8 << 30), Some(8 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(8, 2),
    )
}

/// A handle whose resident projection retains a multi-megabyte generation.
fn handle(governor: RuntimeGovernor) -> EmbeddedStoreHandle {
    let mut database = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(16),
        max_read_result_payload_bytes: Some(16 * 1024),
        execution_memory: ExecutionMemoryConfig {
            query_memory_bytes: std::num::NonZeroUsize::new(256 * 1024).unwrap(),
            blocking_operator_bytes: std::num::NonZeroUsize::new(64 * 1024).unwrap(),
            ..ExecutionMemoryConfig::default()
        },
        ..DatabaseConfig::default()
    });
    database
        .query("CREATE (:Memory {id: 'pinned', title: 'Pinned memory'})")
        .unwrap();
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        database,
        GraphMode::WritableCutover,
        governor,
    );
    let mut index = SearchIndex::in_memory();
    // A small document the reads return, and a bulky one that only makes the
    // retained generation large.
    for (external_id, body) in [
        ("pinned", "resident memory".to_string()),
        ("bulk", "payload ".repeat(512 * 1024)),
    ] {
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: external_id.to_string(),
                title: external_id.to_string(),
                body,
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    let projection = SearchProjection::from_index(index);
    EmbeddedStoreHandle::new(EmbeddedStore::new(graph, Some(projection))).unwrap()
}

fn retained_capacity(handle: &EmbeddedStoreHandle) -> u64 {
    let published = handle.lock_published_read().unwrap();
    let resident = published.as_ref().unwrap().resident_projection.as_ref();
    resident.unwrap().retained_capacity_bytes() as u64
}

#[derive(Clone, Copy, Debug)]
enum PinningRead {
    SearchCandidates,
    SearchDocuments,
    BoundedSnapshot,
    SeededCypher,
}

impl PinningRead {
    const ALL: [Self; 4] = [
        Self::SearchCandidates,
        Self::SearchDocuments,
        Self::BoundedSnapshot,
        Self::SeededCypher,
    ];

    fn run(self, handle: &EmbeddedStoreHandle) -> Result<(), HawDBError> {
        match self {
            Self::SearchCandidates => handle
                .search_candidates_with_report(&SearchCandidateRequest::text("resident", 1))
                .map(drop),
            Self::SearchDocuments => handle
                .search_projection_documents(&["memory:pinned".to_string()], 1)
                .map(drop),
            Self::BoundedSnapshot => handle.with_bounded_read_snapshot(
                EmbeddedReadSnapshotBudget {
                    max_rows: 1,
                    max_payload_bytes: 4096,
                },
                |_| Ok(()),
            ),
            Self::SeededCypher => handle
                .query_with_params_with_report(
                    SEED,
                    &BTreeMap::from([("text".to_string(), Value::String("resident".into()))]),
                )
                .map(drop),
        }
    }
}

/// Another owner holding all but `headroom` bytes of governor memory.
fn hold_all_but(
    governor: &RuntimeGovernor,
    headroom: u64,
) -> (RuntimePermit, RuntimeRetainedMemory) {
    let blocker = governor
        .try_admit(RuntimeWorkRequest::new(
            RuntimeWorkPriority::Foreground,
            RuntimeWorkKind::Control,
        ))
        .unwrap();
    let held = blocker
        .reserve_retained_memory(MEMORY_BUDGET - headroom)
        .unwrap();
    (blocker, held)
}

fn typed_refusal(read: PinningRead, handle: &EmbeddedStoreHandle) -> RuntimeAdmissionRefusal {
    match read.run(handle) {
        Err(HawDBError::RuntimeAdmissionRefused(refusal)) => refusal,
        other => panic!("{read:?}: expected a typed admission refusal, got {other:?}"),
    }
}

#[test]
fn every_pinning_read_reports_a_typed_retryable_refusal_and_recovers() {
    let governor = governor();
    let handle = handle(governor.clone());
    let capacity = retained_capacity(&handle);
    for read in PinningRead::ALL {
        // First leave less than the generation needs. If the read's own
        // request does not fit either, that admission refusal is typed too;
        // then leave room for the request but not also for the generation.
        let mut headroom = capacity / 2;
        let (blocker, held) = hold_all_but(&governor, headroom);
        let mut refusal = typed_refusal(read, &handle);
        let (blocker, held) = if refusal.requested == capacity {
            (blocker, held)
        } else {
            assert_eq!(
                governor.snapshot().admitted_memory_bytes,
                MEMORY_BUDGET - headroom
            );
            drop((held, blocker));
            headroom = refusal.requested + capacity / 2;
            let blocked = hold_all_but(&governor, headroom);
            refusal = typed_refusal(read, &handle);
            blocked
        };
        // The read request was admitted; only funding the resident
        // generation it pins was refused, with what the request left over.
        assert_eq!(
            refusal,
            RuntimeAdmissionRefusal {
                code: "memory_saturated",
                requested: capacity,
                available: refusal.available,
                retryable: true,
            },
            "{read:?}"
        );
        assert!(refusal.available < headroom, "{read:?}: {refusal:?}");
        assert!(
            HawDBError::RuntimeAdmissionRefused(refusal)
                .to_string()
                .starts_with("execution error: runtime admission memory_saturated"),
            "{read:?}"
        );
        // The refused read released everything it was admitted with.
        assert_eq!(
            governor.snapshot().admitted_memory_bytes,
            MEMORY_BUDGET - headroom,
            "{read:?}"
        );

        // Once the other owner releases its memory the same read succeeds.
        drop(held);
        drop(blocker);
        read.run(&handle)
            .unwrap_or_else(|error| panic!("{read:?}: retry failed: {error}"));
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0, "{read:?}");
        // Not poisoned: ordinary reads and writes keep working on the handle.
        let output = handle
            .query_with_report("MATCH (m:Memory {id: 'pinned'}) RETURN m.title AS title")
            .unwrap();
        assert_eq!(output.output.rows.len(), 1, "{read:?}");
        handle
            .query_with_report("MATCH (m:Memory {id: 'pinned'}) SET m.reads = 1")
            .unwrap();
    }
}
