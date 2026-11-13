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

/// Owned filesystem authority for deferred checkpoint-generation cleanup.
/// It retains directory/runtime ownership, but no graph values or MVCC pin.
#[doc(hidden)]
#[derive(Debug)]
pub struct CheckpointReclamationContext {
    durable: DurableStore,
    _branch_lease: Option<Arc<crate::ownership::DatabaseDirectoryLease>>,
    _runtime_owner: Option<Arc<crate::file_descriptors::AdmittedRuntimeOwner>>,
    _file_context: Option<crate::file_descriptors::FileOpenContext>,
}

impl GraphStore {
    /// Captures cleanup ownership without creating a graph read snapshot.
    #[doc(hidden)]
    pub fn checkpoint_reclamation_context(&self) -> Result<CheckpointReclamationContext> {
        let mut durable = self.durable.clone().ok_or_else(|| {
            HawDBError::Storage("checkpoint reclamation requires a durable runtime".into())
        })?;
        // Cleanup cannot append to the serving WAL or retain its append handle.
        durable.wal_append_file = None;
        Ok(CheckpointReclamationContext {
            durable,
            _branch_lease: self.branch_lease.clone(),
            _runtime_owner: self.branch_runtime_owner.clone(),
            _file_context: self.file_descriptor_context(),
        })
    }
}

impl CheckpointCandidate {
    /// Reclaims a previously selected generation through retained filesystem
    /// authority, checking the same store/generation identity as live cleanup.
    #[doc(hidden)]
    pub fn reclaim_published_generations_with_context(
        &self,
        context: &mut CheckpointReclamationContext,
        pinned_reader_generations: &BTreeSet<u64>,
    ) -> Result<()> {
        let (durable, generation, previous) =
            self.reclamation_durable_runtime(&mut context.durable)?;
        durable.reclaim_old_generations(
            generation,
            previous,
            Some(pinned_reader_generations),
            context._branch_lease.as_deref(),
        );
        Ok(())
    }

    /// Marks cleanup debt on the shared durable receipt without a graph view.
    #[doc(hidden)]
    pub fn defer_published_generation_reclamation_with_context(
        &self,
        context: &mut CheckpointReclamationContext,
    ) -> Result<()> {
        let (durable, _, _) = self.reclamation_durable_runtime(&mut context.durable)?;
        durable.defer_generation_reclamation();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_context_has_no_graph_pin_and_rejects_an_old_generation() {
        let root = std::env::temp_dir().join(format!(
            "hawdb-reclamation-context-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([("body".into(), Value::String("界🙂\0".repeat(8192)))]),
            )
            .unwrap();
        assert_eq!(store.version_snapshot_pins.oldest_epoch(), None);
        let mut stale = store.checkpoint_reclamation_context().unwrap();
        assert_eq!(store.version_snapshot_pins.oldest_epoch(), None);
        assert!(stale.durable.wal_append_file.is_none());
        let pinned = store.checkpoint_source();
        let mut candidate = pinned
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        candidate.catch_up(&store).unwrap();
        candidate.finish_catch_up().unwrap();
        store
            .publish_checkpoint_candidate(&mut candidate, None, &BTreeSet::new())
            .unwrap();
        assert!(candidate
            .defer_published_generation_reclamation_with_context(&mut stale)
            .is_err());
        let mut selected = store.checkpoint_reclamation_context().unwrap();
        candidate
            .defer_published_generation_reclamation_with_context(&mut selected)
            .unwrap();
        candidate
            .reclaim_published_generations_with_context(&mut selected, &BTreeSet::new())
            .unwrap();
        drop(selected);
        drop(stale);
        drop(candidate);
        drop(pinned);
        drop(store);
        fs::remove_dir_all(root).unwrap();
    }
}
