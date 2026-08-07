use std::fmt;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug)]
pub struct SnapshotCoordinator<T> {
    published: RwLock<Arc<VersionedSnapshot<T>>>,
    writer: Mutex<()>,
}

#[derive(Debug)]
pub struct VersionedSnapshot<T> {
    epoch: u64,
    value: T,
}

#[derive(Debug, Clone)]
pub struct SnapshotReadGuard<T> {
    snapshot: Arc<VersionedSnapshot<T>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotCommitError<E> {
    Poisoned,
    Stage(E),
    Durability(E),
}

impl<T> SnapshotCoordinator<T> {
    pub fn new(value: T) -> Self {
        Self::new_at_epoch(value, 0)
    }

    pub fn new_at_epoch(value: T, epoch: u64) -> Self {
        Self {
            published: RwLock::new(Arc::new(VersionedSnapshot { epoch, value })),
            writer: Mutex::new(()),
        }
    }

    pub fn read(&self) -> Result<SnapshotReadGuard<T>, SnapshotCommitError<()>> {
        let snapshot = self
            .published
            .read()
            .map_err(|_| SnapshotCommitError::Poisoned)?
            .clone();
        Ok(SnapshotReadGuard { snapshot })
    }

    pub fn commit<E>(
        &self,
        stage: impl FnOnce(&T, u64) -> Result<T, E>,
        make_durable: impl FnOnce(u64, &T) -> Result<(), E>,
    ) -> Result<SnapshotReadGuard<T>, SnapshotCommitError<E>> {
        let _writer = self
            .writer
            .lock()
            .map_err(|_| SnapshotCommitError::Poisoned)?;
        let current = self
            .published
            .read()
            .map_err(|_| SnapshotCommitError::Poisoned)?
            .clone();
        let next_epoch = current.epoch.saturating_add(1);
        let next_value = stage(current.value(), next_epoch).map_err(SnapshotCommitError::Stage)?;
        make_durable(next_epoch, &next_value).map_err(SnapshotCommitError::Durability)?;
        let next = Arc::new(VersionedSnapshot {
            epoch: next_epoch,
            value: next_value,
        });
        *self
            .published
            .write()
            .map_err(|_| SnapshotCommitError::Poisoned)? = Arc::clone(&next);
        Ok(SnapshotReadGuard { snapshot: next })
    }
}

impl<T> VersionedSnapshot<T> {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn value(&self) -> &T {
        &self.value
    }
}

impl<T> SnapshotReadGuard<T> {
    pub fn epoch(&self) -> u64 {
        self.snapshot.epoch()
    }

    pub fn value(&self) -> &T {
        self.snapshot.value()
    }
}

impl<E: fmt::Display> fmt::Display for SnapshotCommitError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Poisoned => formatter.write_str("snapshot coordinator lock is poisoned"),
            Self::Stage(error) => write!(formatter, "snapshot staging failed: {error}"),
            Self::Durability(error) => write!(formatter, "snapshot durability failed: {error}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for SnapshotCommitError<E> {}

#[cfg(test)]
mod tests {
    use super::SnapshotCoordinator;
    use std::sync::{Arc, Barrier};
    use std::thread;

    #[test]
    fn durability_failure_does_not_publish_staged_value() {
        let coordinator = SnapshotCoordinator::new(10_u64);

        let result = coordinator.commit(
            |value, _| Ok::<_, &'static str>(value + 1),
            |_, _| Err("wal fsync failed"),
        );

        assert!(result.is_err());
        let snapshot = coordinator.read().unwrap();
        assert_eq!(snapshot.epoch(), 0);
        assert_eq!(*snapshot.value(), 10);
    }

    #[test]
    fn reader_keeps_a_stable_snapshot_after_publish() {
        let coordinator = SnapshotCoordinator::new(String::from("old"));
        let old = coordinator.read().unwrap();

        let new = coordinator
            .commit(
                |_, _| Ok::<_, ()>(String::from("new")),
                |_, _| Ok::<_, ()>(()),
            )
            .unwrap();

        assert_eq!(old.epoch(), 0);
        assert_eq!(old.value(), "old");
        assert_eq!(new.epoch(), 1);
        assert_eq!(new.value(), "new");
    }

    #[test]
    fn concurrent_commits_are_serialized() {
        let coordinator = Arc::new(SnapshotCoordinator::new(0_u64));
        let start = Arc::new(Barrier::new(3));
        let handles = (0..2)
            .map(|_| {
                let coordinator = Arc::clone(&coordinator);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    coordinator
                        .commit(|value, _| Ok::<_, ()>(value + 1), |_, _| Ok::<_, ()>(()))
                        .unwrap()
                        .epoch()
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        let mut epochs = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        epochs.sort_unstable();

        assert_eq!(epochs, vec![1, 2]);
        assert_eq!(*coordinator.read().unwrap().value(), 2);
    }
}
