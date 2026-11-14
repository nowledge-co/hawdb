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

use std::fmt::{Display, Formatter};

/// A condition that requires retrying the complete transaction with a fresh snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionRetryReason {
    SnapshotChanged {
        read_epoch: u64,
        committed_epoch: u64,
    },
    Deadlock {
        wait_cycle: Vec<u64>,
    },
}

/// Preserves the original retry cause on subsequent calls to an aborted transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRetryError {
    pub transaction_id: u64,
    pub reason: TransactionRetryReason,
    /// This call encountered a transaction that had already aborted.
    pub already_aborted: bool,
}

impl Display for TransactionRetryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let transaction_id = self.transaction_id;
        if self.already_aborted {
            write!(f, "concurrent transaction {transaction_id} is aborted: ")?;
        }
        match &self.reason {
            TransactionRetryReason::SnapshotChanged { read_epoch, committed_epoch } => write!(
                f,
                "pessimistic transaction {transaction_id} cannot acquire a new lock after its snapshot changed from commit epoch {read_epoch} to {committed_epoch}; retry the transaction"
            ),
            TransactionRetryReason::Deadlock { wait_cycle } => {
                write!(f, "deadlock detected; transaction {transaction_id} selected as victim; wait cycle: ")?;
                for (index, transaction) in wait_cycle.iter().enumerate() {
                    if index != 0 {
                        write!(f, " -> ")?;
                    }
                    write!(f, "{transaction}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for TransactionRetryError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HawDBError;

    #[test]
    fn transaction_retry_classification_uses_typed_causes_only() {
        for reason in [
            TransactionRetryReason::SnapshotChanged {
                read_epoch: 1,
                committed_epoch: 2,
            },
            TransactionRetryReason::Deadlock {
                wait_cycle: vec![1, 2, 1],
            },
        ] {
            for already_aborted in [false, true] {
                let error = HawDBError::TransactionRetryRequired(Box::new(TransactionRetryError {
                    transaction_id: 1,
                    reason: reason.clone(),
                    already_aborted,
                }));
                assert!(error.is_retryable_transaction_conflict());
                assert_eq!(error.to_string().contains("is aborted"), already_aborted);
            }
        }
        assert!(HawDBError::TransactionConflict {
            read_epoch: 1,
            committed_epoch: 2,
            key: "row".into(),
        }
        .is_retryable_transaction_conflict());
        for error in [
            HawDBError::Execution("deadlock detected; retry the transaction".into()),
            HawDBError::Execution("transaction lock wait timed out".into()),
            HawDBError::Execution("concurrent lock manager is poisoned".into()),
            HawDBError::Storage("snapshot changed; retry the transaction".into()),
            HawDBError::Semantic("invalid query".into()),
        ] {
            assert!(!error.is_retryable_transaction_conflict());
        }
    }
}
