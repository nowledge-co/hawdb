// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The project-wide gate that serializes cold opens with retirement.

use crate::file_io::ReadAdmissionControl;
use std::io;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Duration;

/// A waiter re-checks its caller's cancellation and deadline this often.
const CONTROL_INTERVAL: Duration = Duration::from_millis(5);

/// A mutex whose acquisition can observe a caller's cancellation and deadline.
/// A holder that unwinds releases it: it protects no data of its own.
#[derive(Debug, Default)]
pub(super) struct OpeningGate {
    held: Mutex<bool>,
    released: Condvar,
}

#[derive(Debug)]
#[must_use]
pub(super) struct OpeningGuard<'a>(&'a OpeningGate);

impl OpeningGate {
    fn held(&self) -> MutexGuard<'_, bool> {
        self.held.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub(super) fn try_lock(&self) -> Option<OpeningGuard<'_>> {
        let mut held = self.held();
        if *held {
            return None;
        }
        *held = true;
        Some(OpeningGuard(self))
    }

    /// Waits without a caller control. The holder runs either one retirement,
    /// which waits for nothing, or one cold open and its identity hash.
    pub(super) fn lock(&self) -> OpeningGuard<'_> {
        let mut held = self
            .released
            .wait_while(self.held(), |held| *held)
            .unwrap_or_else(|error| error.into_inner());
        *held = true;
        OpeningGuard(self)
    }

    /// Like [`Self::lock`], but stops when `control` refuses to continue.
    pub(super) fn lock_controlled(
        &self,
        control: &mut impl ReadAdmissionControl,
    ) -> io::Result<OpeningGuard<'_>> {
        loop {
            if let Some(guard) = self.try_lock() {
                return Ok(guard);
            }
            {
                let held = self.held();
                if *held {
                    drop(
                        self.released
                            .wait_timeout(held, CONTROL_INTERVAL)
                            .unwrap_or_else(|error| error.into_inner()),
                    );
                }
            }
            // Never call out while holding the gate's own mutex.
            control.checkpoint()?;
        }
    }
}

impl Drop for OpeningGuard<'_> {
    fn drop(&mut self) {
        *self.0.held() = false;
        // All, not one: a woken waiter may stop instead of taking the gate.
        self.0.released.notify_all();
    }
}
