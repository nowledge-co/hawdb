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

//! Per-statement write counters, in the spirit of Neo4j `SummaryCounters`.

/// How many write operations one Cypher statement executed.
///
/// The counters count **write operations executed**, not how much stored
/// data changed:
///
/// - They are counted in the mutation layer, where each write of a statement
///   is decided, never derived from the write-ahead log. In-memory
///   (`Database::new()`), persistent, and out-of-core databases therefore
///   report identical counters; WAL coalescing or elision has no effect.
/// - `SET n.p = v` counts 1 in [`properties_set`](Self::properties_set) even
///   when `n.p` already equals `v`. Setting the same property twice in one
///   statement counts 2, also when two input rows (`UNWIND`) write the same
///   node. `SET n.p = null` (property removal) also counts 1, as will
///   `REMOVE n.p` (Neo4j counts it in `properties_set` too).
/// - Each property written by a `CREATE` counts in `properties_set`:
///   `CREATE (:A {x: 1, y: 2})` reports `nodes_created == 1`,
///   `labels_added == 1` and `properties_set == 2`. Values filled from a
///   declared `DEFAULT` are not counted; they are not given by the statement.
/// - `DETACH DELETE` counts the relationships it deletes in
///   [`relationships_deleted`](Self::relationships_deleted).
/// - `MERGE` that matches an existing node counts only its `ON MATCH SET`
///   properties (`nodes_created == 0`). A `MERGE` that creates counts
///   `nodes_created == 1` plus the merge-pattern properties and its
///   `ON CREATE SET` properties.
/// - Creating an index or constraint that already exists counts 0 in
///   [`indexes_added`](Self::indexes_added) /
///   [`constraints_added`](Self::constraints_added).
/// - [`labels_added`](Self::labels_added) and
///   [`labels_removed`](Self::labels_removed) count only labels actually
///   added to or removed from a node.
/// - A statement whose pattern matches nothing (for example `SET` on a node
///   id that does not exist) and every read-only statement report all zeros.
/// - Inside an explicit transaction, each statement's output carries only
///   that statement's counters; the output of the transaction's `commit()`
///   carries none. A statement that fails returns its error and no counters.
///
/// `properties_set == 0` does **not** imply that the statement matched no
/// node: a `SET n += {}` with an empty map, a label-only write, or a `MERGE`
/// that matches without `ON MATCH SET` all match nodes without setting a
/// property. Use the returned rows to learn what a statement matched.
///
/// The Cypher dialect does not parse `REMOVE`, `SET n += {...}`, `SET n = {...}`,
/// `SET n:Label`, `DROP INDEX` / `DROP CONSTRAINT` or `IF NOT EXISTS` yet: a
/// node receives its label when it is created, and a repeated
/// `CREATE INDEX` / `CREATE CONSTRAINT` is idempotent. Until those
/// statements exist, `labels_removed`, `indexes_removed` and
/// `constraints_removed` are always 0; they are part of the type so callers
/// do not have to change when they arrive.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct QueryCounters {
    pub nodes_created: u64,
    pub nodes_deleted: u64,
    pub relationships_created: u64,
    pub relationships_deleted: u64,
    pub properties_set: u64,
    pub labels_added: u64,
    pub labels_removed: u64,
    pub indexes_added: u64,
    pub indexes_removed: u64,
    pub constraints_added: u64,
    pub constraints_removed: u64,
}

impl QueryCounters {
    /// Whether the statement executed any write counted here.
    pub fn contains_updates(&self) -> bool {
        *self != Self::default()
    }
}

impl std::ops::AddAssign for QueryCounters {
    fn add_assign(&mut self, other: Self) {
        let Self {
            nodes_created,
            nodes_deleted,
            relationships_created,
            relationships_deleted,
            properties_set,
            labels_added,
            labels_removed,
            indexes_added,
            indexes_removed,
            constraints_added,
            constraints_removed,
        } = other;
        self.nodes_created = self.nodes_created.saturating_add(nodes_created);
        self.nodes_deleted = self.nodes_deleted.saturating_add(nodes_deleted);
        self.relationships_created = self
            .relationships_created
            .saturating_add(relationships_created);
        self.relationships_deleted = self
            .relationships_deleted
            .saturating_add(relationships_deleted);
        self.properties_set = self.properties_set.saturating_add(properties_set);
        self.labels_added = self.labels_added.saturating_add(labels_added);
        self.labels_removed = self.labels_removed.saturating_add(labels_removed);
        self.indexes_added = self.indexes_added.saturating_add(indexes_added);
        self.indexes_removed = self.indexes_removed.saturating_add(indexes_removed);
        self.constraints_added = self.constraints_added.saturating_add(constraints_added);
        self.constraints_removed = self.constraints_removed.saturating_add(constraints_removed);
    }
}

#[cfg(test)]
mod tests {
    use super::QueryCounters;

    #[test]
    fn counters_default_to_no_updates_and_accumulate() {
        let mut counters = QueryCounters::default();
        assert!(!counters.contains_updates());
        counters += QueryCounters {
            properties_set: 2,
            ..QueryCounters::default()
        };
        counters += QueryCounters {
            properties_set: 1,
            constraints_removed: 1,
            ..QueryCounters::default()
        };
        assert!(counters.contains_updates());
        assert_eq!(counters.properties_set, 3);
        assert_eq!(counters.constraints_removed, 1);
        assert_eq!(counters.nodes_created, 0);
    }
}
