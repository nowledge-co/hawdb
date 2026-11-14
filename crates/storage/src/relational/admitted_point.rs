// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Canonical point reads with allocation ownership retained through consumption.

use super::*;
use crate::background::{
    CheckpointAllocationOwner, CheckpointDecodeContext, CheckpointWorkContext,
};
use hawdb_core::{HawDBError, RuntimeTaskContext};

pub struct RelationalPointReadRequest<'a> {
    pub table: &'a str,
    pub primary_key: &'a RelationalKey,
    pub fields: &'a [usize],
    pub limits: RelationalRowPageSnapshotReadLimits,
}

#[derive(Debug)]
pub struct RelationalPointAllocation {
    _memory: CheckpointAllocationOwner,
}

#[derive(Debug)]
pub struct RelationalAdmittedPoint {
    row: Option<RelationalProjectedRow>,
    report: Option<RelationalRowPageSnapshotPointReport>,
    allocation: RelationalPointAllocation,
}

impl RelationalAdmittedPoint {
    /// Internal consumers move this owner alongside the decoded row. No reader,
    /// execution slot or I/O slot is held by the allocation inventory.
    pub fn into_parts(
        self,
    ) -> (
        Option<RelationalProjectedRow>,
        Option<RelationalRowPageSnapshotPointReport>,
        RelationalPointAllocation,
    ) {
        (self.row, self.report, self.allocation)
    }
}

impl RelationalState {
    pub fn read_point_with_admission(
        &self,
        snapshot: Option<&RelationalRowPageSnapshotReader>,
        request: RelationalPointReadRequest<'_>,
        hydration: &mut RelationalHydrationBudget,
        task: &RuntimeTaskContext,
    ) -> hawdb_core::Result<RelationalAdmittedPoint> {
        let work = CheckpointWorkContext::new(task.clone());
        work.classify(|scoped| {
            let context = CheckpointDecodeContext {
                work: scoped.clone(),
                memory: Default::default(),
            };
            let mut staged_hydration = *hydration;
            // Includes the consuming runtime's row Arc and allocation-owner metadata.
            context.reserve(std::mem::size_of::<RelationalProjectedRow>() + 256)?;
            let (mut row, report, resolve_state) = if let Some(reader) = snapshot {
                let (row, report) = reader
                    .point_projected_with_hydration_mode(CheckpointSnapshotPointRequest {
                        table: request.table,
                        primary_key: request.primary_key,
                        requested_fields: request.fields,
                        limits: request.limits,
                        hydration: &mut staged_hydration,
                        task,
                        hydration_fields: Some(request.fields),
                        allocations: Some(&context),
                    })
                    .map_err(HawDBError::from_storage_error)?;
                let resolve = report.source == RelationalRowPageSnapshotRowSource::Live
                    || (report.source == RelationalRowPageSnapshotRowSource::Recovery
                        && !reader.has_overlay_overflow_root());
                (row, Some(report), resolve)
            } else if let Some((key, source)) = self.row_entry(request.table, request.primary_key) {
                let primary_key = key
                    .clone_with_allocations(&context)
                    .map_err(HawDBError::from_storage_error)?;
                let mut fields = context.vector_capacity(request.fields.len())?;
                for ordinal in request.fields {
                    scoped
                        .checkpoint()
                        .map_err(HawDBError::from_storage_error)?;
                    let value = source
                        .values()
                        .get(*ordinal)
                        .ok_or_else(|| {
                            HawDBError::StorageIntegrity(
                                "point field is outside canonical row shape".into(),
                            )
                        })?
                        .as_ref()
                        .to_owned_value_with_allocations(&context)
                        .map_err(HawDBError::from_storage_error)?;
                    fields.push(RelationalProjectedField {
                        ordinal: *ordinal,
                        value,
                    });
                }
                (
                    Some(RelationalProjectedRow {
                        primary_key,
                        fields,
                    }),
                    None,
                    true,
                )
            } else {
                (None, None, false)
            };
            if resolve_state && let Some(row) = &mut row {
                overflow::hydrate_projected_row_fields_with_allocations(
                    self,
                    request.table,
                    row,
                    Some(request.fields),
                    &mut staged_hydration,
                    Some(task),
                    Some(&context),
                )
                .map_err(HawDBError::from_storage_error)?;
            }
            scoped
                .checkpoint()
                .map_err(HawDBError::from_storage_error)?;
            *hydration = staged_hydration;
            Ok::<_, HawDBError>(RelationalAdmittedPoint {
                row,
                report,
                allocation: RelationalPointAllocation {
                    _memory: context.memory.into_inner(),
                },
            })
        })
        .map_err(|error| match error {
            crate::background::CheckpointOperationError::Operation(error) => error,
            crate::background::CheckpointOperationError::Work(error) => match error {
                crate::background::CheckpointWorkError::Memory(
                    hawdb_core::RuntimeMemoryError::ReservationExceeded {
                        requested_bytes,
                        available_bytes,
                    }
                    | hawdb_core::RuntimeMemoryError::AdmissionDenied {
                        requested_bytes,
                        available_bytes,
                        ..
                    },
                ) => hawdb_core::ResourceBudgetError {
                    resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                    scope: hawdb_core::ResourceBudgetScope::Query,
                    owner: "canonical point decode".into(),
                    memory_class: Some("pipeline_batch"),
                    requested: requested_bytes,
                    used: 0,
                    budget: available_bytes,
                    budget_option: "RuntimeTaskContext.memory_reservation.memory_bytes",
                    message: "canonical point decode exceeded remaining allocation admission"
                        .into(),
                }
                .into(),
                error => HawDBError::from_storage_error(error),
            },
        })
    }
}
