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

use super::{lock_unpoisoned, SegmentCacheShard};
use std::fmt;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

/// Immutable owned bytes whose cached payload remains pinned across every clone.
/// The backing allocation is never exported; use `to_vec` for a detached copy.
pub struct SegmentBytes {
    payload: Arc<SegmentPayload>,
}

#[derive(Debug)]
pub(super) struct SegmentPayload {
    bytes: Box<[u8]>,
    pin: Option<ResidentPin>,
}

#[derive(Debug)]
struct ResidentPin {
    shard: Weak<Mutex<SegmentCacheShard>>,
    external_handles: AtomicUsize,
    // Protected by the owning shard lock. Atomic storage lets handles keep the
    // state in their payload without another mutex or unsafe interior mutation.
    accounted: AtomicBool,
}

impl SegmentPayload {
    pub(super) fn cached(bytes: Vec<u8>, shard: Weak<Mutex<SegmentCacheShard>>) -> Arc<Self> {
        Arc::new(Self {
            // Discard spare Vec capacity only after admission has succeeded;
            // the resident payload budget accounts for this exact byte extent.
            bytes: bytes.into_boxed_slice(),
            pin: Some(ResidentPin {
                shard,
                external_handles: AtomicUsize::new(0),
                accounted: AtomicBool::new(false),
            }),
        })
    }

    pub(super) fn data(&self) -> &[u8] {
        &self.bytes
    }

    /// Called under the owning shard lock; the caller adds the returned charge.
    pub(super) fn lease(self: &Arc<Self>) -> (SegmentBytes, u64) {
        let payload = Arc::clone(self);
        let pin = self.pin.as_ref().expect("resident payload has pin state");
        // Clone the Arc first: its reference-count limit also bounds this count.
        let previous = pin.external_handles.fetch_add(1, Ordering::Relaxed);
        let charge = if previous == 0 && !pin.accounted.swap(true, Ordering::Relaxed) {
            self.bytes.len() as u64
        } else {
            0
        };
        (SegmentBytes { payload }, charge)
    }

    /// Called under the owning shard lock, including while a final drop waits.
    pub(super) fn is_pinned(&self) -> bool {
        self.pin
            .as_ref()
            .expect("resident payload has pin state")
            .accounted
            .load(Ordering::Relaxed)
    }
}

impl Clone for SegmentBytes {
    fn clone(&self) -> Self {
        let payload = Arc::clone(&self.payload);
        if let Some(pin) = &payload.pin {
            pin.external_handles.fetch_add(1, Ordering::Relaxed);
        }
        Self { payload }
    }
}

impl Drop for SegmentBytes {
    fn drop(&mut self) {
        let Some(pin) = &self.payload.pin else {
            return;
        };
        if pin.external_handles.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        if let Some(shard) = pin.shard.upgrade() {
            let mut shard = lock_unpoisoned(&shard);
            // A new get can repin the same payload while final drop waits for
            // the lock. That get inherits the existing charge; only an empty
            // external handle set releases it. Eviction uses the same flag.
            if pin.external_handles.load(Ordering::Acquire) == 0
                && pin.accounted.swap(false, Ordering::Relaxed)
            {
                shard.pinned_bytes = shard
                    .pinned_bytes
                    .saturating_sub(self.payload.bytes.len() as u64);
            }
        }
    }
}

impl From<Vec<u8>> for SegmentBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            payload: Arc::new(SegmentPayload {
                bytes: bytes.into_boxed_slice(),
                pin: None,
            }),
        }
    }
}

impl From<Box<[u8]>> for SegmentBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        Self {
            payload: Arc::new(SegmentPayload { bytes, pin: None }),
        }
    }
}

impl Deref for SegmentBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.payload.bytes
    }
}

impl AsRef<[u8]> for SegmentBytes {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl PartialEq for SegmentBytes {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.payload, &other.payload) || self.as_ref() == other.as_ref()
    }
}

impl Eq for SegmentBytes {}

impl fmt::Debug for SegmentBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(formatter)
    }
}

#[cfg(test)]
mod tests;
