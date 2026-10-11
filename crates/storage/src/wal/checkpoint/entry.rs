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
use crate::background::CheckpointAllocationOwner;

/// Borrow a complete decoded record without separating it from admission.
/// Data fields drop before their retained allocation inventory.
pub(crate) struct CheckpointWalEntry {
    entry: WalEntry,
    memory: CheckpointAllocationOwner,
}

impl std::ops::Deref for CheckpointWalEntry {
    type Target = WalEntry;

    fn deref(&self) -> &Self::Target {
        &self.entry
    }
}

impl std::borrow::Borrow<WalEntry> for CheckpointWalEntry {
    fn borrow(&self) -> &WalEntry {
        &self.entry
    }
}

impl CheckpointWalEntry {
    pub(crate) fn new(entry: WalEntry, memory: CheckpointAllocationOwner) -> Self {
        Self { entry, memory }
    }

    #[cfg(test)]
    pub(crate) fn replay_into(
        self,
        store: &mut crate::store::GraphStore,
        catalog: &mut hawdb_core::Catalog,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        self.replay_into_with_boundary(store, catalog, work, &mut false)
    }

    pub(crate) fn replay_into_with_boundary(
        mut self,
        store: &mut crate::store::GraphStore,
        catalog: &mut hawdb_core::Catalog,
        work: &CheckpointWorkContext,
        mutation_started: &mut bool,
    ) -> Result<()> {
        // Transfer ownership after admission/preflight, before the first
        // mutation. A rejected preflight releases this record's allocations;
        // a partial apply retains moved values until the private runtime drops.
        store.apply_replayed_checkpoint_wal_transaction_with_boundary(
            catalog,
            self.entry.op,
            work,
            mutation_started,
            Some(&mut self.memory),
        )
    }
}
