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

use crate::ContentDigest;
use std::num::{NonZeroU64, NonZeroUsize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadRange {
    pub artifact_id: u64,
    pub segment_ids: Vec<u64>,
    pub offset: u64,
    pub length: NonZeroU64,
    pub content_digest: Option<ContentDigest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadWave {
    pub ranges: Vec<SegmentReadRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentReadSchedule {
    pub io_depth: NonZeroUsize,
    pub input_range_count: usize,
    pub coalesced_range_count: usize,
    pub waves: Vec<SegmentReadWave>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentReadScheduler {
    io_depth: NonZeroUsize,
    max_coalesced_bytes: NonZeroU64,
}

impl SegmentReadRange {
    pub fn new(artifact_id: u64, segment_id: u64, offset: u64, length: NonZeroU64) -> Self {
        Self {
            artifact_id,
            segment_ids: vec![segment_id],
            offset,
            length,
            content_digest: None,
        }
    }

    pub const fn with_content_digest(mut self, content_digest: ContentDigest) -> Self {
        self.content_digest = Some(content_digest);
        self
    }

    pub fn end_offset(&self) -> u64 {
        self.offset.saturating_add(self.length.get())
    }
}

impl SegmentReadSchedule {
    pub fn wave_count(&self) -> usize {
        self.waves.len()
    }

    pub fn scheduled_bytes(&self) -> u64 {
        self.waves
            .iter()
            .flat_map(|wave| &wave.ranges)
            .map(|range| range.length.get())
            .fold(0, u64::saturating_add)
    }

    pub fn max_in_flight(&self) -> usize {
        self.waves
            .iter()
            .map(|wave| wave.ranges.len())
            .max()
            .unwrap_or(0)
    }

    /// Returns the largest byte sum assigned to one scheduled I/O wave.
    pub fn max_scheduled_wave_bytes(&self) -> u64 {
        self.waves
            .iter()
            .map(|wave| {
                wave.ranges
                    .iter()
                    .map(|range| range.length.get())
                    .fold(0, u64::saturating_add)
            })
            .max()
            .unwrap_or(0)
    }
}

impl SegmentReadScheduler {
    pub fn new(io_depth: NonZeroUsize, max_coalesced_bytes: NonZeroU64) -> Self {
        Self {
            io_depth,
            max_coalesced_bytes,
        }
    }

    pub fn schedule<I>(self, ranges: I) -> SegmentReadSchedule
    where
        I: IntoIterator<Item = SegmentReadRange>,
    {
        self.schedule_inner(ranges, None)
    }

    /// Packs coalesced ranges under both the configured I/O depth and a byte
    /// budget. A single range larger than the budget stays intact so the
    /// executor can reject it before allocation.
    pub fn schedule_with_wave_budget<I>(
        self,
        ranges: I,
        max_wave_bytes: NonZeroU64,
    ) -> SegmentReadSchedule
    where
        I: IntoIterator<Item = SegmentReadRange>,
    {
        self.schedule_inner(ranges, Some(max_wave_bytes))
    }

    fn schedule_inner<I>(self, ranges: I, max_wave_bytes: Option<NonZeroU64>) -> SegmentReadSchedule
    where
        I: IntoIterator<Item = SegmentReadRange>,
    {
        let mut ranges = ranges.into_iter().collect::<Vec<_>>();
        let input_range_count = ranges.len();
        ranges.sort_by_key(|range| {
            (
                range.artifact_id,
                range.offset,
                range.segment_ids.first().copied().unwrap_or_default(),
            )
        });

        let mut coalesced = Vec::<SegmentReadRange>::new();
        for range in ranges {
            let Some(previous) = coalesced.last_mut() else {
                coalesced.push(range);
                continue;
            };
            let merged_end = previous.end_offset().max(range.end_offset());
            let merged_length = merged_end.saturating_sub(previous.offset);
            let max_coalesced_bytes = max_wave_bytes
                .map_or(self.max_coalesced_bytes.get(), |budget| {
                    self.max_coalesced_bytes.get().min(budget.get())
                });
            let can_merge = previous.artifact_id == range.artifact_id
                && previous.content_digest.is_none()
                && range.content_digest.is_none()
                && range.offset <= previous.end_offset()
                && merged_length <= max_coalesced_bytes;
            if can_merge {
                previous.length =
                    NonZeroU64::new(merged_length).expect("merged read range remains non-zero");
                previous.segment_ids.extend(range.segment_ids);
            } else {
                coalesced.push(range);
            }
        }

        let coalesced_range_count = coalesced.len();
        let waves = build_waves(coalesced, self.io_depth, max_wave_bytes);
        SegmentReadSchedule {
            io_depth: self.io_depth,
            input_range_count,
            coalesced_range_count,
            waves,
        }
    }
}

fn build_waves(
    ranges: Vec<SegmentReadRange>,
    io_depth: NonZeroUsize,
    max_wave_bytes: Option<NonZeroU64>,
) -> Vec<SegmentReadWave> {
    let mut waves = Vec::new();
    let mut current_ranges = Vec::new();
    let mut current_bytes = 0u64;
    for range in ranges {
        let range_bytes = range.length.get();
        let exceeds_depth = current_ranges.len() == io_depth.get();
        let exceeds_byte_budget = max_wave_bytes.is_some_and(|budget| {
            !current_ranges.is_empty() && current_bytes.saturating_add(range_bytes) > budget.get()
        });
        if exceeds_depth || exceeds_byte_budget {
            waves.push(SegmentReadWave {
                ranges: std::mem::take(&mut current_ranges),
            });
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(range_bytes);
        current_ranges.push(range);
    }
    if !current_ranges.is_empty() {
        waves.push(SegmentReadWave {
            ranges: current_ranges,
        });
    }
    waves
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(artifact_id: u64, segment_id: u64, offset: u64, length: u64) -> SegmentReadRange {
        SegmentReadRange::new(
            artifact_id,
            segment_id,
            offset,
            NonZeroU64::new(length).unwrap(),
        )
    }

    #[test]
    fn coalesces_adjacent_ranges_before_scheduling_parallel_waves() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(256).unwrap());
        let schedule = scheduler.schedule([
            range(1, 2, 100, 100),
            range(1, 1, 0, 100),
            range(1, 3, 400, 50),
            range(2, 4, 0, 50),
        ]);

        assert_eq!(schedule.input_range_count, 4);
        assert_eq!(schedule.coalesced_range_count, 3);
        assert_eq!(schedule.wave_count(), 2);
        assert_eq!(schedule.max_in_flight(), 2);
        assert_eq!(schedule.scheduled_bytes(), 300);
        assert_eq!(schedule.waves[0].ranges[0].segment_ids, vec![1, 2]);
    }

    #[test]
    fn does_not_coalesce_across_artifacts_or_over_the_byte_limit() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(4).unwrap(), NonZeroU64::new(128).unwrap());
        let schedule = scheduler.schedule([
            range(1, 1, 0, 100),
            range(1, 2, 100, 100),
            range(2, 3, 0, 100),
        ]);

        assert_eq!(schedule.coalesced_range_count, 3);
        assert_eq!(schedule.wave_count(), 1);
    }

    #[test]
    fn partitions_waves_by_io_depth_and_byte_budget() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(4).unwrap(), NonZeroU64::new(256).unwrap());
        let schedule = scheduler.schedule_with_wave_budget(
            [
                range(1, 1, 0, 80),
                range(2, 2, 0, 80),
                range(3, 3, 0, 80),
                range(4, 4, 0, 80),
            ],
            NonZeroU64::new(160).unwrap(),
        );

        assert_eq!(schedule.wave_count(), 2);
        assert_eq!(schedule.max_in_flight(), 2);
        assert_eq!(schedule.max_scheduled_wave_bytes(), 160);
        assert_eq!(schedule.scheduled_bytes(), 320);
    }

    #[test]
    fn wave_budget_also_bounds_coalesced_ranges() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(4).unwrap(), NonZeroU64::new(256).unwrap());
        let schedule = scheduler.schedule_with_wave_budget(
            [range(1, 1, 0, 80), range(1, 2, 80, 80)],
            NonZeroU64::new(128).unwrap(),
        );

        assert_eq!(schedule.coalesced_range_count, 2);
        assert_eq!(schedule.wave_count(), 2);
        assert_eq!(schedule.max_scheduled_wave_bytes(), 80);
    }

    #[test]
    fn leaves_an_individually_oversized_range_for_fail_closed_execution() {
        let scheduler =
            SegmentReadScheduler::new(NonZeroUsize::new(4).unwrap(), NonZeroU64::new(256).unwrap());
        let schedule = scheduler
            .schedule_with_wave_budget([range(1, 1, 0, 256)], NonZeroU64::new(128).unwrap());

        assert_eq!(schedule.wave_count(), 1);
        assert_eq!(schedule.max_scheduled_wave_bytes(), 256);
    }

    #[test]
    fn empty_schedule_has_no_waves() {
        let scheduler = SegmentReadScheduler::new(
            NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(4096).unwrap(),
        );
        let schedule = scheduler.schedule([]);

        assert_eq!(schedule.wave_count(), 0);
        assert_eq!(schedule.max_in_flight(), 0);
        assert_eq!(schedule.scheduled_bytes(), 0);
    }
}
