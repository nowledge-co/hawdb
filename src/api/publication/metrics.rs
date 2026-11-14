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

//! Fixed-memory qualification counters for the actual publication owner lock.

use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

const BUCKETS: usize = 64;

/// Logarithmic nanosecond histogram. Bucket zero includes 0 and 1 ns; buckets
/// 1..62 have inclusive upper bound 2^index; the final bucket ends at u64::MAX.
/// Concurrent snapshots are observations, not atomic cross-field snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct PublicationLockDurationSnapshot {
    pub buckets: Vec<u64>,
    pub total_nanos: u64,
    pub max_nanos: u64,
}

/// Successful acquisitions completed since owner creation, including unwinds.
/// Wait ends just after lock acquisition. Hold ends immediately before unlock;
/// histogram updates happen after unlock and are excluded from both intervals.
/// Failed and poisoned acquisitions do not contribute duration samples.
#[derive(Debug, Clone, Serialize)]
pub struct PublicationLockMetricsSnapshot {
    pub read_wait: PublicationLockDurationSnapshot,
    pub read_hold: PublicationLockDurationSnapshot,
    pub write_wait: PublicationLockDurationSnapshot,
    pub write_hold: PublicationLockDurationSnapshot,
    pub busy_try_writes: u64,
}

#[derive(Debug)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            total_nanos: AtomicU64::new(0),
            max_nanos: AtomicU64::new(0),
        }
    }
}

fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
        Some(old.saturating_add(value))
    });
}

impl Histogram {
    fn record(&self, nanos: u64) {
        let bucket = (u64::BITS - nanos.saturating_sub(1).leading_zeros()) as usize;
        saturating_add(&self.buckets[bucket.min(BUCKETS - 1)], 1);
        saturating_add(&self.total_nanos, nanos);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    fn snapshot(&self) -> PublicationLockDurationSnapshot {
        PublicationLockDurationSnapshot {
            buckets: self
                .buckets
                .iter()
                .map(|v| v.load(Ordering::Relaxed))
                .collect(),
            total_nanos: self.total_nanos.load(Ordering::Relaxed),
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct LockMetrics {
    wait: Histogram,
    hold: Histogram,
}

#[derive(Debug, Default)]
pub(super) struct PublicationLockMetrics {
    pub(super) read: LockMetrics,
    pub(super) write: LockMetrics,
    busy_try_writes: AtomicU64,
}

impl PublicationLockMetrics {
    pub(super) fn busy_try_write(&self) {
        saturating_add(&self.busy_try_writes, 1);
    }

    pub(super) fn snapshot(&self) -> PublicationLockMetricsSnapshot {
        PublicationLockMetricsSnapshot {
            read_wait: self.read.wait.snapshot(),
            read_hold: self.read.hold.snapshot(),
            write_wait: self.write.wait.snapshot(),
            write_hold: self.write.hold.snapshot(),
            busy_try_writes: self.busy_try_writes.load(Ordering::Relaxed),
        }
    }
}

pub(super) struct LockTiming<'a> {
    metrics: &'a LockMetrics,
    acquired: Instant,
    waited_nanos: u64,
    held_nanos: Option<u64>,
}

impl<'a> LockTiming<'a> {
    pub(super) fn acquired(metrics: &'a LockMetrics, started: Instant) -> Self {
        let acquired = Instant::now();
        Self {
            metrics,
            acquired,
            waited_nanos: acquired
                .duration_since(started)
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            held_nanos: None,
        }
    }

    pub(super) fn stop(&mut self) {
        self.held_nanos = Some(self.acquired.elapsed().as_nanos().min(u64::MAX as u128) as u64);
    }
}

impl Drop for LockTiming<'_> {
    fn drop(&mut self) {
        if let Some(held) = self.held_nanos {
            self.metrics.wait.record(self.waited_nanos);
            self.metrics.hold.record(held);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_bounds_and_saturation_preserve_samples() {
        let histogram = Histogram::default();
        for nanos in [0, 1, 2, 3, 4, 5, 1 << 62, 1 << 63, u64::MAX] {
            histogram.record(nanos);
        }
        let snapshot = histogram.snapshot();
        assert_eq!(snapshot.buckets.iter().sum::<u64>(), 9);
        assert_eq!(&snapshot.buckets[..4], &[2, 1, 2, 1]);
        assert_eq!(snapshot.buckets[62], 1);
        assert_eq!(snapshot.buckets[63], 2);
        assert_eq!(snapshot.total_nanos, u64::MAX);
        assert_eq!(snapshot.max_nanos, u64::MAX);
    }
}
