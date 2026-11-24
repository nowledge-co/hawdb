// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Deterministic pause inside immutable-object retirement, for tests.

use crate::immutable_object::ObjectReference;
use std::cell::RefCell;
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
use std::time::Duration;

thread_local! {
    static PAUSE: RefCell<Option<(Sender<ObjectReference>, Receiver<()>)>> = const { RefCell::new(None) };
}

/// Controls one retirement that another thread runs. Dropping it resumes the
/// retirement, so a failing test cannot leave the retiring thread stopped.
#[doc(hidden)]
#[derive(Debug)]
pub struct RetirementPause {
    paused: Receiver<ObjectReference>,
    _resume: Sender<()>,
}

/// Arms the calling thread; move it there and call [`Self::arm`].
#[doc(hidden)]
#[derive(Debug)]
pub struct RetirementPauseArm {
    paused: Sender<ObjectReference>,
    resume: Receiver<()>,
}

impl RetirementPause {
    pub fn new() -> (Self, RetirementPauseArm) {
        let (paused, wait) = channel();
        let (resume, resumed) = channel();
        (
            Self {
                paused: wait,
                _resume: resume,
            },
            RetirementPauseArm {
                paused,
                resume: resumed,
            },
        )
    }

    /// The object whose retirement the armed thread stopped in, if any did
    /// within `timeout`.
    pub fn wait_paused(&self, timeout: Duration) -> Option<ObjectReference> {
        self.paused.recv_timeout(timeout).ok()
    }
}

impl RetirementPauseArm {
    /// The next retirement on the calling thread pauses once.
    pub fn arm(self) {
        PAUSE.with(|slot| *slot.borrow_mut() = Some((self.paused, self.resume)));
    }
}

/// Runs while retirement holds every lock it ever holds.
pub(super) fn reached(reference: ObjectReference) {
    if let Some((paused, resume)) = PAUSE.with(|slot| slot.borrow_mut().take())
        && paused.send(reference).is_ok()
    {
        // Bounded, so a deadlocked test fails instead of hanging.
        assert_ne!(
            resume.recv_timeout(Duration::from_secs(60)),
            Err(RecvTimeoutError::Timeout),
            "paused retirement was never resumed"
        );
    }
}
