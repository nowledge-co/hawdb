//! Bounded private state-machine proof; filesystem encoding is deliberately separate.

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Binding {
    database: u128,
    projection: u128,
    registration: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct VerifiedCheckpoint {
    binding: Binding,
    nonce: u128,
    complete_epoch: u64,
    durable_epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    Retention,
    Expired,
    Identity,
    Checkpoint,
    SourceRewound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Active,
    Unverified,
    Rebuild(Reason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Cursor {
    checkpoint: VerifiedCheckpoint,
    expires_at: u64,
    state: State,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Registry {
    database: u128,
    graph_epoch: u64,
    hard_floor: u64,
    max_consumers: usize,
    cursors: BTreeMap<String, Cursor>,
}

impl Registry {
    fn register(
        &mut self,
        id: &str,
        checkpoint: VerifiedCheckpoint,
        expires_at: u64,
    ) -> Result<(), &'static str> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        {
            return Err("invalid consumer identity");
        }
        if self.cursors.contains_key(id) || self.cursors.len() >= self.max_consumers {
            return Err("consumer registry full or duplicate");
        }
        self.validate_checkpoint(checkpoint)?;
        let mut cursor = Cursor {
            checkpoint,
            expires_at,
            state: State::Active,
        };
        cursor.state = self.derived_state(&cursor);
        if cursor.state != State::Active {
            return Err("initial cursor cannot resume");
        }
        self.cursors.insert(id.to_string(), cursor);
        Ok(())
    }

    fn validate_checkpoint(&self, checkpoint: VerifiedCheckpoint) -> Result<(), &'static str> {
        if checkpoint.binding.database != self.database {
            return Err("database identity mismatch");
        }
        if checkpoint.complete_epoch > checkpoint.durable_epoch {
            return Err("checkpoint is not durable through the requested boundary");
        }
        if checkpoint.complete_epoch > self.graph_epoch {
            return Err("checkpoint is ahead of the source database");
        }
        Ok(())
    }

    fn derived_state(&self, cursor: &Cursor) -> State {
        if cursor.checkpoint.binding.database != self.database {
            State::Rebuild(Reason::Identity)
        } else if cursor.checkpoint.complete_epoch > self.graph_epoch {
            State::Rebuild(Reason::SourceRewound)
        } else if cursor.checkpoint.complete_epoch < self.hard_floor {
            State::Rebuild(Reason::Retention)
        } else if self.graph_epoch >= cursor.expires_at {
            State::Rebuild(Reason::Expired)
        } else {
            State::Active
        }
    }

    fn refresh(&mut self, graph_epoch: u64, hard_floor: u64) {
        self.graph_epoch = graph_epoch;
        self.hard_floor = hard_floor;
        let states = self
            .cursors
            .iter()
            .map(|(id, cursor)| (id.clone(), self.derived_state(cursor)))
            .collect::<Vec<_>>();
        for (id, state) in states {
            let cursor = self.cursors.get_mut(&id).unwrap();
            if matches!(cursor.state, State::Active | State::Unverified) && state != State::Active {
                cursor.state = state;
            }
        }
    }

    fn soft_floor(&self) -> Option<u64> {
        self.cursors
            .values()
            .filter(|cursor| cursor.state == State::Active)
            .map(|cursor| cursor.checkpoint.complete_epoch)
            .min()
    }

    fn advance(&mut self, id: &str, checkpoint: VerifiedCheckpoint) -> Result<(), &'static str> {
        self.validate_checkpoint(checkpoint)?;
        let cursor = self.cursors.get_mut(id).ok_or("consumer not registered")?;
        if cursor.state != State::Active {
            return Err("consumer requires validation or rebuild");
        }
        if cursor.checkpoint.binding != checkpoint.binding {
            return Err("stale consumer handle");
        }
        if checkpoint.complete_epoch < cursor.checkpoint.complete_epoch {
            return Err("cursor regression");
        }
        if checkpoint.complete_epoch == cursor.checkpoint.complete_epoch
            && checkpoint != cursor.checkpoint
        {
            return Err("different checkpoint at an already acknowledged epoch");
        }
        cursor.checkpoint = checkpoint;
        Ok(())
    }

    fn reopened(&self) -> Self {
        let mut reopened = self.clone();
        for cursor in reopened.cursors.values_mut() {
            if cursor.state == State::Active {
                cursor.state = State::Unverified;
            }
        }
        reopened.refresh(self.graph_epoch, self.hard_floor);
        reopened
    }

    fn verify(&mut self, id: &str, checkpoint: VerifiedCheckpoint) -> Result<(), &'static str> {
        self.validate_checkpoint(checkpoint)?;
        let cursor = self.cursors.get_mut(id).ok_or("consumer not registered")?;
        if matches!(cursor.state, State::Rebuild(_)) {
            return Err("consumer requires rebuild");
        }
        if checkpoint != cursor.checkpoint {
            cursor.state = State::Rebuild(if checkpoint.binding != cursor.checkpoint.binding {
                Reason::Identity
            } else {
                Reason::Checkpoint
            });
            return Err("unverified checkpoint");
        }
        cursor.state = State::Active;
        Ok(())
    }
}

fn registry() -> Registry {
    Registry {
        database: 1,
        graph_epoch: 5,
        hard_floor: 0,
        max_consumers: 64,
        cursors: BTreeMap::new(),
    }
}

fn checkpoint(consumer: u128, epoch: u64) -> VerifiedCheckpoint {
    VerifiedCheckpoint {
        binding: Binding {
            database: 1,
            projection: consumer,
            registration: consumer + 100,
        },
        nonce: consumer + u128::from(epoch) * 1000,
        complete_epoch: epoch,
        durable_epoch: epoch,
    }
}

#[test]
fn multiple_consumers_use_the_minimum_and_only_lagging_consumers_are_invalidated() {
    let mut registry = registry();
    registry.register("slow", checkpoint(1, 1), 100).unwrap();
    registry.register("fast", checkpoint(2, 3), 100).unwrap();
    assert_eq!(registry.soft_floor(), Some(1));
    registry.advance("fast", checkpoint(2, 4)).unwrap();
    assert_eq!(registry.soft_floor(), Some(1));
    registry.refresh(5, 2);
    assert_eq!(
        registry.cursors["slow"].state,
        State::Rebuild(Reason::Retention)
    );
    assert_eq!(registry.cursors["fast"].state, State::Active);
    assert_eq!(registry.soft_floor(), Some(4));
    registry.refresh(6, 5);
    assert_eq!(registry.soft_floor(), None);
    assert_eq!(
        registry.cursors["fast"].state,
        State::Rebuild(Reason::Retention)
    );
}

#[test]
fn advancement_is_idempotent_monotonic_and_bounded_by_durable_source_progress() {
    let mut registry = registry();
    registry
        .register("consumer", checkpoint(1, 2), 100)
        .unwrap();
    registry.advance("consumer", checkpoint(1, 3)).unwrap();
    let before = registry.clone();
    registry.advance("consumer", checkpoint(1, 3)).unwrap();
    assert_eq!(registry, before);
    for proof in [
        checkpoint(1, 2),
        checkpoint(1, 6),
        VerifiedCheckpoint {
            durable_epoch: 3,
            ..checkpoint(1, 4)
        },
        checkpoint(2, 4),
        VerifiedCheckpoint {
            nonce: 999,
            ..checkpoint(1, 3)
        },
    ] {
        assert!(registry.advance("consumer", proof).is_err());
        assert_eq!(registry, before);
    }
}

#[test]
fn restart_requires_exact_checkpoint_validation_and_detects_identity_or_source_rewind() {
    let mut original = registry();
    original
        .register("consumer", checkpoint(1, 3), 100)
        .unwrap();
    let mut resumed = original.reopened();
    assert_eq!(resumed.soft_floor(), None);
    assert!(resumed.advance("consumer", checkpoint(1, 4)).is_err());
    resumed.verify("consumer", checkpoint(1, 3)).unwrap();
    assert_eq!(resumed.soft_floor(), Some(3));
    let mut replaced = original.reopened();
    assert!(replaced.verify("consumer", checkpoint(2, 3)).is_err());
    assert_eq!(
        replaced.cursors["consumer"].state,
        State::Rebuild(Reason::Identity)
    );
    let mut newer_projection = original.reopened();
    assert!(newer_projection
        .verify("consumer", checkpoint(1, 4))
        .is_err());
    assert_eq!(
        newer_projection.cursors["consumer"].state,
        State::Rebuild(Reason::Checkpoint)
    );
    let mut rewound = original.reopened();
    rewound.refresh(2, 0);
    assert_eq!(
        rewound.cursors["consumer"].state,
        State::Rebuild(Reason::SourceRewound)
    );
}

#[test]
fn expiry_and_unregister_release_the_soft_floor_without_unbounded_registry_growth() {
    let mut registry = registry();
    registry.register("consumer", checkpoint(1, 3), 6).unwrap();
    registry.refresh(6, 0);
    assert_eq!(
        registry.cursors["consumer"].state,
        State::Rebuild(Reason::Expired)
    );
    assert_eq!(registry.soft_floor(), None);
    registry.cursors.remove("consumer");
    let mut replacement = checkpoint(1, 3);
    replacement.binding.registration += 1;
    registry.register("consumer", replacement, 100).unwrap();
    assert!(registry.advance("consumer", checkpoint(1, 4)).is_err());
    for n in 1..64 {
        registry
            .register(&format!("consumer-{n}"), checkpoint(n + 1, 3), 100)
            .unwrap();
    }
    assert_eq!(registry.cursors.len(), 64);
    assert!(registry
        .register("overflow", checkpoint(99, 3), 100)
        .is_err());
    registry.refresh(100, 0);
    assert_eq!(registry.soft_floor(), None);
    assert_eq!(registry.cursors.len(), 64);
}
