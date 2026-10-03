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

use super::SearchProjectionGraphChange;
use crate::cow::CowSegment;
use std::collections::VecDeque;
use std::sync::Arc;

const PAGE_ENTRIES: usize = 64;
type ChangePage = CowSegment<VecDeque<Arc<SearchProjectionGraphChange>>>;

/// Snapshots share the directory and immutable record pages. Append and prefix
/// removal detach only their boundary pages; directory cost scales with pages.
#[derive(Debug, Clone, Default)]
pub(super) struct SearchProjectionChangeLog {
    pages: CowSegment<VecDeque<ChangePage>>,
    len: usize,
}

impl SearchProjectionChangeLog {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn first(&self) -> Option<&Arc<SearchProjectionGraphChange>> {
        self.pages.front().and_then(|page| page.front())
    }

    pub(super) fn last(&self) -> Option<&Arc<SearchProjectionGraphChange>> {
        self.pages.back().and_then(|page| page.back())
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Arc<SearchProjectionGraphChange>> + Clone {
        self.pages.iter().flat_map(|page| page.iter())
    }

    pub(super) fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut Arc<SearchProjectionGraphChange>> {
        self.pages.iter_mut().flat_map(|page| page.iter_mut())
    }

    pub(super) fn push(&mut self, change: Arc<SearchProjectionGraphChange>) {
        if self
            .pages
            .back()
            .is_none_or(|page| page.len() == PAGE_ENTRIES)
        {
            self.pages
                .push_back(VecDeque::with_capacity(PAGE_ENTRIES).into());
        }
        self.pages
            .back_mut()
            .expect("append has a non-full page")
            .push_back(change);
        self.len += 1;
    }

    pub(super) fn discard_prefix(&mut self, mut count: usize) {
        assert!(count <= self.len, "discarded prefix exceeds change log");
        if count == 0 {
            return;
        }
        if count == self.len {
            *self = Self::default();
            return;
        }
        self.len -= count;
        while count > 0 {
            let page = self.pages.front_mut().expect("retained prefix has a page");
            if count >= page.len() {
                count -= page.len();
                self.pages.pop_front();
            } else {
                page.drain(..count);
                break;
            }
        }
    }
}

impl FromIterator<Arc<SearchProjectionGraphChange>> for SearchProjectionChangeLog {
    fn from_iter<T: IntoIterator<Item = Arc<SearchProjectionGraphChange>>>(iter: T) -> Self {
        let mut log = Self::default();
        for change in iter {
            log.push(change);
        }
        log
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::RelationalPrimaryKeyChangeCapture;

    fn change(epoch: u64) -> Arc<SearchProjectionGraphChange> {
        Arc::new(SearchProjectionGraphChange {
            commit_epoch: epoch,
            upsert_node_ids: vec![epoch],
            delete_document_ids: vec![format!("deleted-{epoch}")],
            relational_primary_key_changes: RelationalPrimaryKeyChangeCapture::Captured {
                tables: Vec::new(),
                encoded_bytes: 0,
            },
        })
    }

    fn assert_contents(
        log: &SearchProjectionChangeLog,
        expected: &[Arc<SearchProjectionGraphChange>],
    ) {
        assert_eq!(log.len(), expected.len());
        assert_eq!(log.first(), expected.first());
        assert_eq!(log.last(), expected.last());
        assert!(log.iter().eq(expected.iter()));
    }

    #[test]
    fn prefix_removal_and_append_preserve_snapshot_contents_at_page_boundaries() {
        let original = (0..(PAGE_ENTRIES * 3 + 1) as u64)
            .map(change)
            .collect::<Vec<_>>();
        let snapshot = original
            .iter()
            .cloned()
            .collect::<SearchProjectionChangeLog>();
        for removed in [
            0,
            1,
            PAGE_ENTRIES - 1,
            PAGE_ENTRIES,
            PAGE_ENTRIES + 1,
            original.len(),
        ] {
            let mut log = snapshot.clone();
            log.discard_prefix(removed);
            let retained = log.clone();
            let mut expected = original[removed..].to_vec();
            for epoch in 1_000..1_000 + (PAGE_ENTRIES * 2) as u64 {
                let next = change(epoch);
                log.push(Arc::clone(&next));
                expected.push(next);
            }
            assert_contents(&log, &expected);
            assert_contents(&retained, &original[removed..]);
            assert_contents(&snapshot, &original);
            for item in log.iter_mut() {
                Arc::make_mut(item).upsert_node_ids.clear();
            }
            assert!(log.iter().all(|item| item.upsert_node_ids.is_empty()));
            assert_contents(&retained, &original[removed..]);
            assert_contents(&snapshot, &original);
        }
    }

    #[test]
    fn append_and_partial_trim_keep_interior_pages_shared() {
        let mut log = (0..(PAGE_ENTRIES * 4 - 1) as u64)
            .map(change)
            .collect::<SearchProjectionChangeLog>();
        let snapshot = log.clone();
        log.push(change(1_000));
        log.discard_prefix(1);
        assert!(!log.pages[0].shares_storage_with(&snapshot.pages[0]));
        assert!(log.pages[1].shares_storage_with(&snapshot.pages[1]));
        assert!(log.pages[2].shares_storage_with(&snapshot.pages[2]));
        assert!(!log.pages[3].shares_storage_with(&snapshot.pages[3]));
        log.discard_prefix(PAGE_ENTRIES - 1);
        assert!(log.pages[0].shares_storage_with(&snapshot.pages[1]));
    }
}
