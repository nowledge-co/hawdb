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

    pub(crate) fn replay_into(
        mut self,
        store: &mut crate::store::GraphStore,
        catalog: &mut hawdb_core::Catalog,
        work: &CheckpointWorkContext,
    ) -> Result<()> {
        // Install retained ownership before mutation. Even a partial failed
        // application must keep moved values admitted until the runtime drops.
        store.retain_decoded_checkpoint_memory(&mut self.memory, work)?;
        store.apply_replayed_checkpoint_wal_transaction(catalog, self.entry.op, work)
    }
}
