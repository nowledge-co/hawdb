// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Retirement of a private birth that has never supplied native producer access.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RetirementStep {
    Intent,
    RemoveMarker,
    RemoveDirectory,
    SyncRemoval,
    Tombstone,
    Close,
    Settle,
}

impl PendingConsumerGeneration {
    fn retiring_state(&self) -> NativeBirth {
        NativeBirth::Retiring {
            generation_name: self.generation_name().to_owned(),
            parent_locator: self.parent_locator.clone(),
        }
    }

    fn retired_state(&self) -> NativeBirth {
        NativeBirth::Retired {
            generation_name: self.generation_name().to_owned(),
            parent_locator: self.parent_locator.clone(),
        }
    }

    /// Certify completed cleanup of a producer that never sealed or selected.
    /// The caller has closed all producer readers/writers. A remaining child,
    /// unknown effect, seal attempt or publication intent retains the debt.
    pub fn retire_cleaned_unsealed(&mut self) -> Result<()> {
        if self.phase != Phase::Recorded
            || self.candidate_seal_attempted
            || self.publication_intent_attempted
            || self.selection_preparation_attempted
        {
            return Err(invalid("Consumer producer cleanup is not unsealed"));
        }
        self.allocation
            .as_ref()
            .ok_or_else(|| invalid("Consumer allocation missing"))?
            .with_row(|row| {
                if row.candidate_seal.is_some() || row.publication_intent.is_some() {
                    return Err(invalid("Consumer producer acquired publication evidence"));
                }
                Ok(())
            })?;
        let directory = self
            .directory
            .as_ref()
            .ok_or_else(|| invalid("Consumer birth directory missing"))?;
        let context = directory.operation_context();
        directory.visit_children(&context, 2, |name| {
            if name != BIRTH_FILE {
                return Err(io::Error::other(
                    "Consumer cleanup retains an unexpected child",
                ));
            }
            Ok(())
        })?;
        self.producer_exposed.store(false, Ordering::Relaxed);
        self.retire_unpublished()
    }

    /// Retire only a completed private birth never exposed by `born_directory`.
    /// The caller still excludes mutation of its private component. This is not
    /// GC for a producer/reader/selector-referenced generation, recursive cleanup,
    /// recovery adoption, or resolution of an uncertain creation.
    ///
    /// Intent precedes deletion; actual parent durability precedes the tombstone;
    /// native target custody closes before ledger capacity can be released. Any
    /// error retains the operation for fresh admission, never path-based rollback.
    pub fn retire_unpublished(&mut self) -> Result<()> {
        if self.phase == Phase::Retired {
            return Ok(());
        }
        if !matches!(self.phase, Phase::Recorded | Phase::Retiring)
            || self.producer_exposed.load(Ordering::Relaxed)
        {
            return Err(HawDBError::Execution(
                "consumer birth is incomplete or exposed; retain its debt".into(),
            ));
        }
        self.phase = Phase::Retiring;
        loop {
            match self.retirement_step {
                RetirementStep::Intent => {
                    let expected = NativeBirth::Recorded {
                        generation_name: self.generation_name().to_owned(),
                        parent_locator: self.parent_locator.clone(),
                    };
                    self.allocation
                        .as_ref()
                        .expect("retiring allocation")
                        .transition_birth_with_retry(expected, self.retiring_state(), true)?;
                    self.retirement_step = RetirementStep::RemoveMarker;
                }
                RetirementStep::RemoveMarker => {
                    // Windows may forbid unlinking the still-open marker. The
                    // original child capture and durable intent retain custody.
                    drop(self.marker.take());
                    let directory = self.directory.as_ref().expect("private born directory");
                    let context = directory.operation_context();
                    match directory.remove_child(&context, BIRTH_FILE) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                    directory.sync(&context)?;
                    self.retirement_step = RetirementStep::RemoveDirectory;
                }
                RetirementStep::RemoveDirectory => {
                    drop(self.directory.take());
                    let parent = self.parent.as_ref().expect("retiring parent");
                    let context = parent.operation_context();
                    parent.remove_created_directory(&context, &mut self.candidate)?;
                    self.retirement_step = RetirementStep::SyncRemoval;
                }
                RetirementStep::SyncRemoval => {
                    let parent = self.parent.as_ref().expect("retiring parent");
                    parent.sync(&parent.operation_context())?;
                    self.retirement_step = RetirementStep::Tombstone;
                }
                RetirementStep::Tombstone => {
                    self.allocation
                        .as_ref()
                        .expect("retiring allocation")
                        .transition_birth_with_retry(
                            self.retiring_state(),
                            self.retired_state(),
                            true,
                        )?;
                    self.retirement_step = RetirementStep::Close;
                }
                RetirementStep::Close => {
                    drop(self.parent.take());
                    drop(std::mem::take(&mut self.marker_bytes));
                    self.retirement_step = RetirementStep::Settle;
                }
                RetirementStep::Settle => {
                    self.allocation
                        .as_ref()
                        .expect("retiring allocation")
                        .settle_closed_record(
                            self.retired_state(),
                            &mut self.settlement_publication_attempted,
                        )?;
                    drop(self.allocation.take());
                    self.phase = Phase::Retired;
                    return Ok(());
                }
            }
        }
    }
}
