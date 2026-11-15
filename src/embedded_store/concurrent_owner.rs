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

use super::{EmbeddedStore, EmbeddedStoreWriteGuard, PublishedCanonicalRead};
use crate::api::{PublishedConcurrentRead, SharedDatabaseGuard, SharedDatabaseOwner};
use crate::{Database, DatabasePublicationOwner, HawDBError, Result};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

/// Contains no coordinator handle: the coordinator can retain this owner without
/// forming a cycle. All direct and transactional publications share these fields.
#[derive(Debug)]
pub(super) struct MemDatabaseOwner {
    pub(super) inner: Arc<DatabasePublicationOwner<EmbeddedStore>>,
    pub(super) published_read: Arc<Mutex<Option<PublishedCanonicalRead>>>,
    pub(super) read_publication_failed: Arc<AtomicBool>,
}

impl MemDatabaseOwner {
    fn guard<'a>(
        &'a self,
        store: crate::api::publication::DatabasePublicationWriteGuard<'a, EmbeddedStore>,
    ) -> SharedDatabaseGuard<'a> {
        Box::new(MemDatabaseGuard(EmbeddedStoreWriteGuard {
            store,
            _exclusive: None,
            retired: None,
            published: &self.published_read,
            read_publication_failed: &self.read_publication_failed,
        }))
    }
}

impl SharedDatabaseOwner for MemDatabaseOwner {
    fn lock(&self) -> Result<SharedDatabaseGuard<'_>> {
        if self.is_poisoned() {
            return Err(poisoned());
        }
        let store = self.inner.write()?;
        if self.is_poisoned() {
            return Err(poisoned());
        }
        Ok(self.guard(store))
    }

    fn try_lock(&self) -> Result<Option<SharedDatabaseGuard<'_>>> {
        if self.is_poisoned() {
            return Err(poisoned());
        }
        match self.inner.try_write() {
            Ok(store) => {
                if self.is_poisoned() {
                    return Err(poisoned());
                }
                Ok(Some(self.guard(store)))
            }
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => Err(poisoned()),
        }
    }

    fn is_poisoned(&self) -> bool {
        self.inner.is_poisoned() || self.read_publication_failed.load(Ordering::Acquire)
    }

    fn read_view(&self) -> Result<Arc<PublishedConcurrentRead>> {
        if self.is_poisoned() {
            return Err(poisoned());
        }
        let publication = self.published_read.lock().map_err(|_| poisoned())?;
        let publication = publication.as_ref().ok_or_else(poisoned)?;
        publication.view.snapshot.ensure_usable()?;
        Ok(Arc::clone(&publication.view))
    }
}

pub(super) struct PublicationUnwindGuard<'a>(pub(super) &'a AtomicBool);

impl Drop for PublicationUnwindGuard<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

struct MemDatabaseGuard<'a>(EmbeddedStoreWriteGuard<'a>);

impl Deref for MemDatabaseGuard<'_> {
    type Target = Database;

    fn deref(&self) -> &Database {
        self.0.graph.database()
    }
}

impl DerefMut for MemDatabaseGuard<'_> {
    fn deref_mut(&mut self) -> &mut Database {
        self.0.graph.database_mut()
    }
}

fn poisoned() -> HawDBError {
    HawDBError::StorageIntegrity("canonical read publication is poisoned; close and reopen".into())
}

#[cfg(test)]
mod tests {
    use super::super::app_read_snapshot_tests::app_read_handle;
    use crate::{QueryRequest, QueryStreamOptions, Value};
    use std::sync::mpsc;
    use std::time::Duration;

    fn title(snapshot: &mut crate::DatabaseReadTransaction, query: &str) -> Value {
        snapshot
            .query_request(
                QueryRequest::new(query).with_output_limits(QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: Some(4096),
                }),
            )
            .unwrap()
            .rows[0]["title"]
            .clone()
    }

    #[test]
    fn two_staging_writers_publish_through_one_owner_while_old_and_new_readers_advance() {
        let handle = app_read_handle();
        const THREAD: &str = "MATCH (t:Thread) RETURN t.title AS title";
        const MEMORY: &str = "MATCH (m:Memory) RETURN m.title AS title";
        handle
            .with_read_transaction(4096, |old| {
                let epoch = old.commit_epoch();
                let (staged, staged_events) = mpsc::channel();
                let (release, release_events) = mpsc::channel();
                let first_handle = handle.clone();
                let first = std::thread::spawn(move || {
                    first_handle.with_transaction(|transaction| {
                        transaction.query("MATCH (t:Thread) SET t.title = 'First writer'")?;
                        staged.send(()).unwrap();
                        release_events
                            .recv_timeout(Duration::from_secs(30))
                            .unwrap();
                        Ok(())
                    })
                });
                staged_events.recv_timeout(Duration::from_secs(30)).unwrap();
                let (completed, completion_events) = mpsc::channel();
                let second_handle = handle.clone();
                let second = std::thread::spawn(move || {
                    let result = second_handle.with_transaction(|transaction| {
                        transaction.query("MATCH (m:Memory) SET m.title = 'Second writer'")?;
                        Ok(())
                    });
                    completed.send(result).unwrap();
                });
                // Always release the first callback before asserting a timeout,
                // so a regression cannot leave a blocked worker behind.
                let progress = completion_events.recv_timeout(Duration::from_secs(10));
                let intermediate = matches!(progress, Ok(Ok(()))).then(|| {
                    handle.with_read_transaction(4096, |current| {
                        Ok((
                            current.commit_epoch(),
                            title(current, THREAD),
                            title(current, MEMORY),
                        ))
                    })
                });
                release.send(()).unwrap();
                let first_result = first.join().unwrap();
                second.join().unwrap();
                progress.expect("a disjoint writer must commit before the first callback exits")?;
                first_result?;
                let (current_epoch, thread_title, memory_title) = intermediate.unwrap()?;
                assert!(current_epoch > epoch);
                assert_eq!(thread_title, Value::String("App thread".into()));
                assert_eq!(memory_title, Value::String("Second writer".into()));
                assert_eq!(old.commit_epoch(), epoch);
                assert_eq!(title(old, THREAD), Value::String("App thread".into()));
                assert_eq!(title(old, MEMORY), Value::String("Nearest memory".into()));
                Ok(())
            })
            .unwrap();
        handle
            .with_read_transaction(4096, |current| {
                assert_eq!(title(current, THREAD), Value::String("First writer".into()));
                assert_eq!(
                    title(current, MEMORY),
                    Value::String("Second writer".into())
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(
            handle
                .runtime_governor_snapshot()
                .unwrap()
                .admitted_memory_bytes,
            0
        );
    }
}
