// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Transaction-owned, fail-closed read evidence for optional serializability.

use super::*;
use hawdb_storage::store::{GraphReadDependencies, GraphReadDependencyError};
use hawdb_storage::version::VersionKey;

/// Isolation is independent of the concurrent transaction's lock mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TransactionIsolation {
    #[default]
    SnapshotIsolation,
    Serializable,
}

/// Bounds the inline capture owner and retained key payload estimates.
/// Allocator overhead, shared history registries and COW pages are excluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionReadDependencyLimits {
    pub entries: NonZeroUsize,
    pub bytes: NonZeroUsize,
}

/// Inspection counters, not a detached validation token or publication permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionReadDependencyUsage {
    pub source_epoch: u64,
    pub entries: usize,
    pub estimated_bytes: usize,
    pub refused: bool,
    pub exact_relational_rows: usize,
    pub relational_index_prefixes: usize,
    pub relational_table_domains: usize,
    pub whole_database_barrier: bool,
}

impl Default for TransactionReadDependencyLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TransactionReadDependencyLimits {
    pub const DEFAULT: Self = Self {
        entries: NonZeroUsize::new(4096).expect("constant is nonzero"),
        bytes: NonZeroUsize::new(1024 * 1024).expect("constant is nonzero"),
    };
}

/// A capture cannot be cloned or separated from its history pin and lease.
#[derive(Debug)]
pub(super) struct TransactionReadDependencies {
    source: DatabaseInstanceIdentity,
    graph: GraphReadDependencies,
    refused: bool,
}

pub(super) fn refusal(reason: impl Into<String>) -> HawDBError {
    HawDBError::TransactionReadDependencyRefused {
        reason: reason.into(),
    }
}

impl TransactionReadDependencies {
    pub(super) fn usage(&self) -> TransactionReadDependencyUsage {
        TransactionReadDependencyUsage {
            source_epoch: self.graph.source_epoch(),
            entries: self.graph.entry_count(),
            estimated_bytes: std::mem::size_of::<Self>()
                .saturating_add(self.graph.estimated_bytes()),
            refused: self.refused,
            exact_relational_rows: self.graph.exact_relational_row_count(),
            relational_index_prefixes: self.graph.relational_index_prefix_count(),
            relational_table_domains: self.graph.relational_table_domain_count(),
            whole_database_barrier: self.graph.requires_unchanged_database_epoch(),
        }
    }
    pub(super) fn capture(db: &Database, limits: TransactionReadDependencyLimits) -> Result<Self> {
        let key_bytes = limits
            .bytes
            .get()
            .checked_sub(std::mem::size_of::<Self>())
            .ok_or_else(|| refusal("dependency budget cannot admit the capture owner"))?;
        let mut graph = db
            .runtime
            .get()?
            .store
            .capture_read_dependencies(limits.entries.get(), key_bytes)?;
        // The existing schema stamp is interpreted at this same source epoch.
        graph
            .record(VersionKey::Schema)
            .map_err(|e| refusal(e.to_string()))?;
        Ok(Self {
            source: db.instance_identity()?,
            graph,
            refused: false,
        })
    }

    pub(super) fn fail(&mut self) {
        self.refused = true;
    }

    pub(super) fn admit_cypher(
        &mut self,
        text: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<()> {
        self.ensure_complete()?;
        if parameters.len() > 1024 || text.len() > 64 * 1024 {
            return Err(refusal("Cypher dependency input exceeds admission limits"));
        }
        let mut bytes = text.len();
        for (name, value) in parameters {
            let size = match value {
                Value::String(value) => value.len(),
                Value::Binary(value) => value.len(),
                Value::Vector(value) => value.encoded().len(),
                Value::List(_) | Value::Map(_) => {
                    return Err(refusal("compound Cypher dependency parameters"))
                }
                _ => 16,
            };
            bytes = bytes.saturating_add(name.len()).saturating_add(size);
            if bytes > 64 * 1024 {
                return Err(refusal("Cypher dependency input exceeds 64 KiB"));
            }
        }
        Ok(())
    }

    pub(super) fn admit_sql(&mut self, text: &str, parameters: &[Value]) -> Result<()> {
        self.ensure_complete()?;
        super::dependency_read::relational::admit_transaction_input(text, parameters)
            .map_err(|e| refusal(e.to_string()))?;
        Ok(())
    }

    pub(super) fn ensure_complete(&self) -> Result<()> {
        if self.refused {
            return Err(refusal(
                "transaction contains an incomplete or failed statement",
            ));
        }
        Ok(())
    }

    pub(super) fn collect_cypher(
        &mut self,
        statement: &cypher::Statement,
        plan: &PhysicalPlan,
        catalog: &Catalog,
        store: &GraphStore,
    ) -> Result<()> {
        self.ensure_complete()?;
        let result = (|| {
            if !qualified_cypher(statement_body(statement)) {
                return Err(refusal("unqualified, volatile or external Cypher input"));
            }
            if executor::is_mutation_plan(plan)? {
                // Mutation predicates/expressions may read outside their final
                // write set. Until a narrower collector proves every input,
                // retain the complete canonical epoch, including empty writes.
                self.graph
                    .record(VersionKey::Database)
                    .map_err(|e| refusal(e.to_string()))
            } else {
                super::dependency_read::collect_node_dependencies(
                    plan,
                    catalog,
                    store,
                    &mut self.graph,
                )
                .map_err(|e| refusal(e.to_string()))
            }
        })();
        self.refused |= result.is_err();
        result
    }

    pub(super) fn collect_sql(
        &mut self,
        text: &str,
        statement: &crate::sql::SqlStatement,
        parameters: &[Value],
        relational: &hawdb_storage::relational::RelationalState,
    ) -> Result<()> {
        self.ensure_complete()?;
        let result = super::dependency_read::relational::collect_transaction(
            text,
            statement,
            parameters,
            relational,
            &mut self.graph,
        )
        .map_err(|e| refusal(e.to_string()));
        self.refused |= result.is_err();
        result
    }

    pub(super) fn validate(&self, db: &Database) -> Result<()> {
        self.ensure_complete()?;
        if self.source != db.instance_identity()? {
            return Err(refusal("transaction source instance or branch changed"));
        }
        db.runtime
            .get()?
            .store
            .validate_read_dependencies(&self.graph)
            .map_err(|e| match e {
                GraphReadDependencyError::Conflict(conflict) => HawDBError::TransactionConflict {
                    read_epoch: self.graph.source_epoch(),
                    committed_epoch: conflict.committed_epoch,
                    key: format!("read dependency {:?}", conflict.key),
                },
                GraphReadDependencyError::Unavailable(error) => error,
                other => refusal(other.to_string()),
            })
    }
}

fn qualified_sets(sets: &[cypher::SetProperty]) -> bool {
    use super::plan_cache::determinism::value_is_deterministic as deterministic;
    use cypher::SetValueExpression as S;
    sets.iter().all(|set| match &set.value {
        S::Value(value) | S::PropertyAdd { value, .. } => deterministic(value),
        S::CoalesceProperty { default, .. } => deterministic(default),
        S::CoalescePropertyAdd { default, value, .. } => {
            deterministic(default) && deterministic(value)
        }
        S::PreserveNewerExisting {
            incoming, preserve, ..
        } => deterministic(incoming) && deterministic(preserve),
        S::Property { .. } | S::DecrementFloorZero { .. } => true,
    })
}

fn qualified_cypher(statement: &cypher::Statement) -> bool {
    use super::plan_cache::determinism::{
        match_is_deterministic, projection_is_deterministic, value_is_deterministic,
    };
    use cypher::{ClauseKind as C, Statement as S};
    match statement {
        S::Pipeline(query) | S::UnwindMutation(query) => {
            query.clauses.iter().all(|clause| match &clause.kind {
                C::Match {
                    patterns,
                    predicate,
                    ..
                } => match_is_deterministic(patterns, predicate.as_ref()),
                C::With(projection) | C::Return(projection) => {
                    projection_is_deterministic(projection)
                }
                C::Create(patterns) => match_is_deterministic(patterns, None),
                C::Merge {
                    pattern,
                    on_create,
                    on_match,
                } => {
                    match_is_deterministic(std::slice::from_ref(pattern), None)
                        && qualified_sets(on_create)
                        && qualified_sets(on_match)
                }
                C::Set(sets) => qualified_sets(sets),
                C::Unwind { source, .. } => value_is_deterministic(source),
                C::Delete { .. } => true,
                // File contents are external input with no read dependency.
                C::Call { .. } | C::LoadCsv(_) => false,
            })
        }
        S::CreateNode(node) => node.properties.values().all(value_is_deterministic),
        S::CreateRelationship(rel) | S::MergeRelationship(rel) => rel
            .properties
            .values()
            .chain(rel.source.properties.values())
            .chain(rel.target.properties.values())
            .all(value_is_deterministic),
        S::MergeNode(node) => {
            node.properties.values().all(value_is_deterministic)
                && qualified_sets(&node.on_create_sets)
                && qualified_sets(&node.on_match_sets)
                && qualified_sets(&node.post_merge_sets)
        }
        S::CreateNodeLabel(_)
        | S::CreateRelationshipType(_)
        | S::CreateNodeTable(_)
        | S::CreateRelationshipTable(_)
        | S::CreateProperty(_)
        | S::AlterTableState(_)
        | S::AlterPropertyState(_)
        | S::CreateIndex(_)
        | S::CreateCompositeIndex(_)
        | S::CreateRangeIndex(_)
        | S::CreateFullTextIndex(_)
        | S::CreateUniqueConstraint(_)
        | S::CreateNodePropertyExistsConstraint(_)
        | S::CreateRelationshipUniqueConstraint(_)
        | S::CreateRelationshipPropertyExistsConstraint(_) => true,
        _ => false,
    }
}

impl DatabaseTransactionState {
    pub(super) fn read_dependency_usage(&self) -> Option<TransactionReadDependencyUsage> {
        self.read_dependencies
            .as_ref()
            .map(TransactionReadDependencies::usage)
    }
    pub(super) fn from_database_with_isolation(
        db: &Database,
        isolation: TransactionIsolation,
        limits: TransactionReadDependencyLimits,
    ) -> Result<Self> {
        let dependencies = match isolation {
            TransactionIsolation::SnapshotIsolation => None,
            TransactionIsolation::Serializable => {
                Some(TransactionReadDependencies::capture(db, limits)?)
            }
        };
        let mut state = Self::from_database(db)?;
        state.read_dependencies = dependencies;
        Ok(state)
    }

    pub(crate) fn fail_read_dependencies(&mut self) {
        if let Some(dependencies) = &mut self.read_dependencies {
            dependencies.fail();
        }
    }
}

#[cfg(test)]
mod vector_tests {
    use super::*;

    #[test]
    fn shared_vector_serializable_inputs_charge_payload_bytes_in_both_languages() {
        let database = Database::new();
        let mut capture = TransactionReadDependencies::capture(
            &database,
            TransactionReadDependencyLimits::default(),
        )
        .unwrap();
        for (dimension, accepted) in [(1024, true), (16384, false)] {
            let vector = Value::Vector(hawdb_core::Vector::new(&vec![1.0; dimension]).unwrap());
            let cypher =
                capture.admit_cypher("RETURN $q", &BTreeMap::from([("q".into(), vector.clone())]));
            let sql = capture.admit_sql("SELECT $1", &[vector]);
            assert_eq!(cypher.is_ok(), accepted);
            assert_eq!(sql.is_ok(), accepted);
            if !accepted {
                assert!(matches!(
                    cypher,
                    Err(HawDBError::TransactionReadDependencyRefused { .. })
                ));
                assert!(matches!(
                    sql,
                    Err(HawDBError::TransactionReadDependencyRefused { .. })
                ));
            }
        }
    }
}
