// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Applying system schema registries to an open store through the shared
//! handle, under maintenance admission.

use super::*;
use crate::{SystemSchemaApplyReport, SystemSchemaRegistry};

/// The `operation` of the [`HawDBError::ReadOnly`] a read-only handle returns
/// when a registry has pending migrations.
const APPLY_OPERATION: &str = "apply_system_schema_registry";

/// The `resource` of the [`HawDBError::BranchBusy`] a call returns while
/// another system schema apply runs on the same handle.
pub(super) const APPLY_IN_PROGRESS: &str = "system schema apply in progress";

impl EmbeddedStoreHandle {
    /// Applies one system schema registry to this already-open database, as
    /// [`crate::Database::apply_system_schema_registry`] does at open time;
    /// see [`Self::apply_system_schema_registries_context`].
    pub fn apply_system_schema_registry_context(
        &self,
        registry: &SystemSchemaRegistry,
        task_context: &RuntimeTaskContext,
    ) -> Result<SystemSchemaApplyReport> {
        self.apply_system_schema_registries_context(std::slice::from_ref(registry), task_context)
    }

    /// Applies system schema registries (for example Cypher `CREATE INDEX`
    /// migrations) to this already-open database from any thread, with the
    /// semantics of [`crate::Database::apply_system_schema_registries`]: the
    /// engine registry first, then every pending application migration in
    /// one transaction, applied migrations skipped after checksum
    /// verification, and the post-schema checkpoint that publishes the
    /// persistent projection of a new graph index on an out-of-core store.
    ///
    /// Order and bounds:
    ///
    /// 1. Registries are validated and the per-handle apply guard is taken
    ///    (see below), then the call is admitted as foreground
    ///    maintenance exactly like [`Self::checkpoint_context`]: the
    ///    context's priority and admission wait apply, and no store guard or
    ///    write access is held while it queues, so other readers and writers
    ///    proceed. A refusal returns the typed
    ///    [`HawDBError::RuntimeAdmissionRefused`]; the default context refuses
    ///    at once rather than waiting.
    /// 2. The exclusive database write access is then taken, bounded by the
    ///    database lock timeout ([`crate::DEFAULT_PESSIMISTIC_LOCK_TIMEOUT`]).
    ///    Writers on the handle, and readers that need the store guard, may
    ///    wait for it to be released; no online index build is attempted.
    /// 3. Cancellation and the deadline are checked before admission, after
    ///    it and once the write access is held, returning
    ///    [`HawDBError::RuntimeTaskStopped`] with nothing applied. After that
    ///    last check the batch runs to completion: it is never interrupted
    ///    midway and never leaves a half-built index.
    ///
    /// Any failing migration step rolls the whole batch back: no DDL and no
    /// migration record is published, the error is returned and the handle
    /// stays usable, so a later call applies every pending migration again.
    /// One apply runs per handle (shared by its clones) at a time. A call
    /// made while another is in progress returns
    /// `HawDBError::BranchBusy { resource: "system schema apply in progress" }`
    /// at once, before admission and without taking any lock or applying
    /// anything; it is retryable, and a retry after the first call finished
    /// finds its migrations applied and skips them. An index is therefore
    /// never built twice.
    ///
    /// On a read-only handle a registry with nothing pending returns its
    /// report (no migration applied, `checkpointed == false`). A registry
    /// with a pending migration returns
    /// `HawDBError::ReadOnly { operation: "apply_system_schema_registry" }`
    /// and writes nothing; open the store writable to apply it.
    pub fn apply_system_schema_registries_context(
        &self,
        registries: &[SystemSchemaRegistry],
        task_context: &RuntimeTaskContext,
    ) -> Result<SystemSchemaApplyReport> {
        task_context.checkpoint()?;
        crate::api::validate_system_schema_registries(registries)?;
        // Released on every path when `_applying` drops.
        let _applying = match self.system_schema_apply.try_lock() {
            Ok(guard) => guard,
            // The guard protects no data; a panicked apply leaves none behind.
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(HawDBError::BranchBusy {
                    resource: APPLY_IN_PROGRESS,
                })
            }
        };
        let shadow_admission_bytes = self
            .read_store()?
            .graph
            .database()
            .columnar_shadow_admission_bytes()?;
        let admission_started = Instant::now();
        let _permit = self.admit_typed_maintenance_context(
            RuntimeWorkPriority::Foreground,
            usize::try_from(shadow_admission_bytes).unwrap_or(usize::MAX),
            1,
            task_context,
        )?;
        let admission_wait = admission_started.elapsed();
        task_context.checkpoint()?;
        // Admission first, then the write access: never queue while holding it.
        let lock_started = Instant::now();
        let mut store = self.write_store()?;
        let write_lock_wait = lock_started.elapsed();
        // The last stop point; from here the batch runs to completion.
        task_context.checkpoint()?;
        let held_started = Instant::now();
        let database = store.graph_mut().database_mut();
        let read_only = database.config().read_only;
        let outcome = database
            .apply_system_schema_registries_with_outcome(registries, Some(shadow_admission_bytes))
            .map_err(|error| match error {
                HawDBError::SystemSchemaUpgradeRequired { .. } if read_only => {
                    HawDBError::ReadOnly {
                        operation: APPLY_OPERATION,
                    }
                }
                other => other,
            })?;
        let write_lock_held = held_started.elapsed();
        drop(store);
        Ok(SystemSchemaApplyReport {
            registries: outcome.reports,
            checkpointed: outcome.checkpointed,
            admission_wait,
            write_lock_wait,
            write_lock_held,
        })
    }
}
