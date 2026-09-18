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

use crate::binding::{binding_payload_bytes, Binding};
use crate::{QueryMemoryAccount, QueryMemoryLease};
use hawdb_core::Result;
use hawdb_plan::GraphExpansionBudget;
use hawdb_storage::NodeId;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphExpansionTruncationReason {
    CandidateLimit,
    PayloadByteLimit,
}

impl GraphExpansionTruncationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidateLimit => "candidate_limit",
            Self::PayloadByteLimit => "payload_byte_limit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphExpansionExecutionReport {
    pub seed_count: usize,
    pub expanded_node_count: usize,
    pub expanded_edge_count: usize,
    pub relation_types: Vec<String>,
    pub min_hops: usize,
    pub max_hops: usize,
    pub reranked_seed_count: usize,
    pub candidate_limit: usize,
    pub payload_byte_limit: usize,
    pub payload_bytes_used: usize,
    pub returned_count: usize,
    pub truncation_reason: Option<GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionReport {
    pub fn truncated(&self) -> bool {
        self.truncation_reason.is_some()
    }
}

#[doc(hidden)]
pub struct GraphExpansionExecutionState {
    budget: Option<GraphExpansionBudget>,
    seed_count: usize,
    expanded_nodes: BTreeSet<NodeId>,
    expanded_nodes_lease: Option<QueryMemoryLease>,
    expanded_edge_count: usize,
    reranked_seed_count: usize,
    payload_bytes_used: usize,
    returned_count: usize,
    pub truncation_reason: Option<GraphExpansionTruncationReason>,
}

impl GraphExpansionExecutionState {
    pub fn new(
        budget: Option<GraphExpansionBudget>,
        seed_count: usize,
        reranked_seed_count: usize,
    ) -> Self {
        Self {
            budget,
            seed_count,
            expanded_nodes: BTreeSet::new(),
            expanded_nodes_lease: None,
            expanded_edge_count: 0,
            reranked_seed_count,
            payload_bytes_used: 0,
            returned_count: 0,
            truncation_reason: None,
        }
    }

    pub fn with_memory_account(
        budget: Option<GraphExpansionBudget>,
        seed_count: usize,
        reranked_seed_count: usize,
        memory_account: &QueryMemoryAccount,
    ) -> Result<Self> {
        Ok(Self {
            expanded_nodes_lease: Some(memory_account.reserve(0)?),
            ..Self::new(budget, seed_count, reranked_seed_count)
        })
    }

    pub fn try_push(
        &mut self,
        output: &mut Vec<Binding>,
        candidate: Binding,
        target_id: Option<NodeId>,
        hop: usize,
    ) -> Result<bool> {
        if !self.try_admit(&candidate, target_id, hop)? {
            return Ok(false);
        }
        output.push(candidate);
        Ok(true)
    }

    pub fn try_admit(
        &mut self,
        candidate: &Binding,
        target_id: Option<NodeId>,
        hop: usize,
    ) -> Result<bool> {
        let Some(budget) = self.budget else {
            self.returned_count = self.returned_count.saturating_add(1);
            return Ok(true);
        };
        if self.returned_count >= budget.candidate_limit {
            self.truncation_reason = Some(GraphExpansionTruncationReason::CandidateLimit);
            return Ok(false);
        }
        let candidate_bytes = binding_payload_bytes(candidate);
        if self.payload_bytes_used.saturating_add(candidate_bytes) > budget.payload_byte_limit {
            self.truncation_reason = Some(GraphExpansionTruncationReason::PayloadByteLimit);
            return Ok(false);
        }
        if let Some(target_id) = target_id
            && !self.expanded_nodes.contains(&target_id)
        {
            const EXPANDED_NODE_MEMORY_BYTES: usize =
                std::mem::size_of::<NodeId>() + 3 * std::mem::size_of::<usize>();
            if let Some(lease) = self.expanded_nodes_lease.as_mut() {
                lease.grow(EXPANDED_NODE_MEMORY_BYTES)?;
            }
            if !self.expanded_nodes.insert(target_id)
                && let Some(lease) = self.expanded_nodes_lease.as_mut()
            {
                lease.shrink(EXPANDED_NODE_MEMORY_BYTES);
            }
        }
        self.payload_bytes_used = self.payload_bytes_used.saturating_add(candidate_bytes);
        self.returned_count = self.returned_count.saturating_add(1);
        self.expanded_edge_count = self.expanded_edge_count.saturating_add(hop);
        Ok(true)
    }

    pub fn record_seed(&mut self) {
        self.seed_count = self.seed_count.saturating_add(1);
    }

    pub fn set_reranked_seed_count(&mut self, reranked_seed_count: usize) {
        self.reranked_seed_count = reranked_seed_count;
    }

    pub fn returned_count(&self) -> usize {
        self.returned_count
    }

    pub fn report(
        &self,
        rel_type: &str,
        min_hops: usize,
        max_hops: usize,
        returned_count: usize,
    ) -> Option<GraphExpansionExecutionReport> {
        let budget = self.budget?;
        Some(GraphExpansionExecutionReport {
            seed_count: self.seed_count,
            expanded_node_count: self.expanded_nodes.len(),
            expanded_edge_count: self.expanded_edge_count,
            relation_types: if rel_type.is_empty() {
                Vec::new()
            } else {
                vec![rel_type.to_string()]
            },
            min_hops,
            max_hops,
            reranked_seed_count: self.reranked_seed_count,
            candidate_limit: budget.candidate_limit,
            payload_byte_limit: budget.payload_byte_limit,
            payload_bytes_used: self.payload_bytes_used,
            returned_count,
            truncation_reason: self.truncation_reason,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_reason_codes_are_stable() {
        assert_eq!(
            GraphExpansionTruncationReason::CandidateLimit.as_str(),
            "candidate_limit"
        );
        assert_eq!(
            GraphExpansionTruncationReason::PayloadByteLimit.as_str(),
            "payload_byte_limit"
        );
    }

    #[test]
    fn reranked_seed_count_can_be_recorded_after_streaming_input() {
        let mut state = GraphExpansionExecutionState::new(
            Some(GraphExpansionBudget {
                candidate_limit: 8,
                payload_byte_limit: 1_024,
            }),
            0,
            0,
        );

        state.set_reranked_seed_count(3);

        let report = state.report("related", 1, 1, 0).expect("budgeted report");
        assert_eq!(report.reranked_seed_count, 3);
    }

    #[test]
    fn expansion_state_enforces_candidate_and_payload_budgets_before_push() {
        let binding = Binding {
            values: std::collections::BTreeMap::from([(
                "value".to_string(),
                hawdb_core::Value::String("payload".to_string()),
            )]),
            nodes: std::collections::BTreeMap::new(),
            relationships: std::collections::BTreeMap::new(),
        };
        let mut state = GraphExpansionExecutionState::new(
            Some(GraphExpansionBudget {
                candidate_limit: 1,
                payload_byte_limit: usize::MAX,
            }),
            1,
            1,
        );
        let mut output = Vec::new();

        assert!(state
            .try_push(&mut output, binding.clone(), None, 1)
            .unwrap());
        assert!(!state.try_push(&mut output, binding, None, 1).unwrap());
        assert_eq!(
            state.truncation_reason,
            Some(GraphExpansionTruncationReason::CandidateLimit)
        );

        let binding = Binding {
            values: std::collections::BTreeMap::from([(
                "value".to_string(),
                hawdb_core::Value::String("payload".to_string()),
            )]),
            nodes: std::collections::BTreeMap::new(),
            relationships: std::collections::BTreeMap::new(),
        };
        let mut state = GraphExpansionExecutionState::new(
            Some(GraphExpansionBudget {
                candidate_limit: 2,
                payload_byte_limit: binding_payload_bytes(&binding).saturating_sub(1),
            }),
            1,
            1,
        );
        let mut output = Vec::new();

        assert!(!state.try_push(&mut output, binding, None, 1).unwrap());
        assert!(output.is_empty());
        assert_eq!(
            state.truncation_reason,
            Some(GraphExpansionTruncationReason::PayloadByteLimit)
        );
    }

    #[test]
    fn expansion_node_set_uses_and_releases_the_query_root() {
        let ledger = crate::QueryMemoryLedger::new(std::num::NonZeroUsize::new(64).unwrap());
        let account = ledger.account(
            crate::QueryMemoryClass::BlockingState,
            "test expansion",
            std::num::NonZeroUsize::new(64).unwrap(),
        );
        let mut state = GraphExpansionExecutionState::with_memory_account(
            Some(GraphExpansionBudget {
                candidate_limit: 2,
                payload_byte_limit: usize::MAX,
            }),
            1,
            0,
            &account,
        )
        .unwrap();
        let binding = Binding::values(std::collections::BTreeMap::new());

        assert!(state.try_admit(&binding, Some(NodeId(7)), 1).unwrap());
        assert!(ledger.snapshot().used_bytes > 0);
        assert!(state.try_admit(&binding, Some(NodeId(7)), 1).unwrap());
        assert_eq!(state.expanded_nodes.len(), 1);

        drop(state);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
}
