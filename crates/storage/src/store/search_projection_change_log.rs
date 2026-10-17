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
use crate::projection::owner_capture::CapturedProjectionChange;
use std::collections::VecDeque;
use std::sync::Arc;

#[path = "search_projection_change_log/cursor.rs"]
mod cursor;
pub use cursor::*;

const PAGE_ENTRIES: usize = 64;
type ChangePage = CowSegment<VecDeque<Arc<CapturedProjectionChange>>>;

/// Snapshots share the directory and immutable record pages. Append and prefix
/// removal detach only their boundary pages; directory cost scales with pages.
#[derive(Debug, Clone, Default)]
pub(crate) struct SearchProjectionChangeLog {
    pages: CowSegment<VecDeque<ChangePage>>,
    len: usize,
    // Exclusive floor for the current endpoint-complete capture contract.
    // Older contracts start at their checkpoint head.
    owner_floor: u64,
}

impl SearchProjectionChangeLog {
    pub(crate) fn owner_floor(&self) -> u64 {
        self.owner_floor
    }
    pub(crate) fn at_owner_floor(owner_floor: u64) -> Self {
        Self {
            owner_floor,
            ..Self::default()
        }
    }

    pub(crate) fn recover_owner_capture(
        state: &mut crate::checkpoint::DecodedCheckpoint,
        head: u64,
    ) -> Self {
        // V1 payloads did not enumerate arbitrary relationship endpoints or
        // relationship-property changes. Preserve their source transactions,
        // but never upgrade those old facts into the stronger V2 contract.
        let qualified = state.projection_owner_capture_version == Some(2);
        let floor = if qualified {
            state.projection_owner_capture_floor.unwrap_or(head)
        } else {
            head
        };
        let mut log = Self::at_owner_floor(floor);
        if !qualified {
            state.projection_owner_changes.clear();
        }
        for legacy in std::mem::take(&mut state.search_projection_graph_changes) {
            let owners_v1 = state
                .projection_owner_changes
                .remove(&legacy.commit_epoch)
                .map(|(_, owners)| owners);
            log.push(Arc::new(CapturedProjectionChange {
                legacy: Arc::new(legacy),
                owners_v1,
            }));
        }
        log
    }
    pub(super) fn len(&self) -> usize {
        self.len
    }

    // Only the first and last pages can be partial. Prefix trim preserves this.
    fn get(&self, index: usize) -> Option<&Arc<CapturedProjectionChange>> {
        let first = self.pages.front()?;
        if index < first.len() {
            first.get(index)
        } else {
            let offset = index - first.len();
            self.pages
                .get(1 + offset / PAGE_ENTRIES)?
                .get(offset % PAGE_ENTRIES)
        }
    }

    pub(super) fn first(&self) -> Option<&Arc<CapturedProjectionChange>> {
        self.pages.front().and_then(|page| page.front())
    }

    pub(crate) fn last(&self) -> Option<&Arc<CapturedProjectionChange>> {
        self.pages.back().and_then(|page| page.back())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<CapturedProjectionChange>> + Clone {
        self.pages.iter().flat_map(|page| page.iter())
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Arc<CapturedProjectionChange>> {
        self.pages.iter_mut().flat_map(|page| page.iter_mut())
    }

    pub(crate) fn push(&mut self, change: Arc<CapturedProjectionChange>) {
        if change.owners_v1.is_none() {
            self.owner_floor = self.owner_floor.max(change.commit_epoch);
        }
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
        self.owner_floor = self
            .owner_floor
            .max(self.get(count - 1).expect("retained prefix").commit_epoch);
        if count == self.len {
            *self = Self::at_owner_floor(self.owner_floor);
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

impl FromIterator<Arc<CapturedProjectionChange>> for SearchProjectionChangeLog {
    fn from_iter<T: IntoIterator<Item = Arc<CapturedProjectionChange>>>(iter: T) -> Self {
        let mut log = Self::default();
        for change in iter {
            log.push(change);
        }
        log
    }
}

impl FromIterator<Arc<SearchProjectionGraphChange>> for SearchProjectionChangeLog {
    fn from_iter<T: IntoIterator<Item = Arc<SearchProjectionGraphChange>>>(iter: T) -> Self {
        iter.into_iter()
            .map(|legacy| {
                Arc::new(CapturedProjectionChange {
                    legacy,
                    owners_v1: None,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relational::RelationalPrimaryKeyChangeCapture;

    fn change(epoch: u64) -> Arc<CapturedProjectionChange> {
        Arc::new(CapturedProjectionChange {
            legacy: Arc::new(SearchProjectionGraphChange {
                commit_epoch: epoch,
                upsert_node_ids: vec![epoch],
                delete_document_ids: vec![format!("deleted-{epoch}")],
                relational_primary_key_changes: RelationalPrimaryKeyChangeCapture::Captured {
                    tables: Vec::new(),
                    encoded_bytes: 0,
                },
            }),
            owners_v1: None,
        })
    }

    fn assert_contents(
        log: &SearchProjectionChangeLog,
        expected: &[Arc<CapturedProjectionChange>],
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
