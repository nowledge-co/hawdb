// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Advance only the term cursors consumed by the previous document.

use super::*;

pub(super) struct OrdinalFrontier {
    heads: Vec<Option<u64>>,
    pending: Vec<usize>,
}

impl OrdinalFrontier {
    pub(super) fn new(streams: usize) -> Self {
        Self {
            heads: vec![None; streams],
            pending: (0..streams).collect(),
        }
    }

    pub(super) fn next_group(
        &mut self,
        streams: &mut [TermPostingStream<'_>],
        group: &mut Vec<(usize, QueryPosting)>,
    ) -> Result<Option<u64>> {
        // Pending indices preserve the original order of block reads.
        for index in self.pending.drain(..) {
            self.heads[index] = streams[index].peek_ordinal()?;
        }
        group.clear();
        let Some(ordinal) = self.heads.iter().flatten().copied().min() else {
            return Ok(None);
        };
        // Term order also preserves floating-point contribution accumulation.
        for (index, head) in self.heads.iter().enumerate() {
            if *head == Some(ordinal) {
                let posting = streams[index]
                    .take()
                    .expect("frontier owns a buffered posting");
                group.push((index, posting));
                self.pending.push(index);
            }
        }
        Ok(Some(ordinal))
    }
}
