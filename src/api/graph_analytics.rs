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

//! Complete, pinned analytics staging and atomic Cypher/SQL publication.

use super::*;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PUBLICATION: AtomicU64 = AtomicU64::new(0);

const PUBLICATIONS: &str = "__hawdb_analytics_publications";
const STAGED_ROW_BYTES: usize = 128;
const STAGED_HEADER_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GraphAnalyticsAlgorithm {
    PageRank(crate::PageRankOptions),
    Louvain(crate::LouvainOptions),
}

/// Row and payload limits cover the complete query output, including every
/// Louvain hierarchy level. The staging limit covers one final value per node.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphAnalyticsRequest {
    pub projection: String,
    pub algorithm: GraphAnalyticsAlgorithm,
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
    pub max_staged_bytes: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphAnalyticsFreshness {
    Unavailable,
    Fresh,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphAnalyticsPublicationStatus {
    pub projection: String,
    pub property: String,
    pub publication_id: Option<String>,
    pub computed_at_commit_epoch: Option<u64>,
    pub published_at_commit_epoch: Option<u64>,
    pub current_commit_epoch: u64,
    pub freshness: GraphAnalyticsFreshness,
}

/// A complete result bound to one database incarnation, branch, projection,
/// options and pinned source epoch. Keeping it permits a lost-response retry.
#[derive(Debug)]
pub struct PreparedGraphAnalytics {
    snapshot: DatabaseReadTransaction,
    request: GraphAnalyticsRequest,
    publication_id: String,
    rows: BTreeMap<i64, (i64, Value)>,
    _staging: hawdb_executor::QueryMemoryLease,
    report: QueryStreamReport,
}

impl PreparedGraphAnalytics {
    pub fn computed_at_commit_epoch(&self) -> u64 {
        self.snapshot.commit_epoch()
    }

    pub fn publication_id(&self) -> &str {
        &self.publication_id
    }

    /// Number of node values to publish, after selecting the highest Louvain
    /// level per node. The execution report counts all hierarchy rows instead.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn execution_report(&self) -> &QueryStreamReport {
        &self.report
    }
}

impl Database {
    /// Computes a complete result under background admission. Ordinary algorithm
    /// CALLs remain read-only; this method stages without publishing properties.
    pub fn prepare_graph_analytics(
        &self,
        request: GraphAnalyticsRequest,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<PreparedGraphAnalytics> {
        self.ensure_runtime_capability(hawdb_core::RuntimeCapability::BackgroundMaintenance)?;
        validate_request(&request)?;
        let budget = self.config.execution_memory.query_memory_bytes.get();
        let query_bytes = budget
            .checked_sub(request.max_staged_bytes.get())
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| {
                HawDBError::Execution("analytics staging leaves no query_memory_bytes".into())
            })?;
        let ledger =
            hawdb_executor::QueryMemoryLedger::new(self.config.execution_memory.query_memory_bytes);
        let staging = ledger
            .account(
                hawdb_executor::QueryMemoryClass::ResultMaterialization,
                "complete analytics staging",
                request.max_staged_bytes,
            )
            .reserve(request.max_staged_bytes.get())?;
        let mut snapshot = match task_context {
            Some(context) => self.begin_read_transaction_with_context(context)?,
            None => self.begin_read_transaction()?,
        };
        snapshot.config.execution_memory.query_memory_bytes = query_bytes;
        let scheduler = self.local_qos_scheduler_for_work();
        let units = snapshot
            .store
            .node_count_for_label(None)
            .saturating_add(snapshot.store.relationship_count_for_type(None))
            .max(1);
        let permit = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, units))
            .map_err(|admission| {
                HawDBError::Execution(format!(
                    "background graph analytics not admitted: {admission:?}"
                ))
            })?;
        let (query, parameters, value_column) = algorithm_query(&request)?;
        let mut rows = BTreeMap::new();
        let mut streamed_rows = 0;
        let result = snapshot.query_with_params_streaming(
            &query,
            &parameters,
            QueryStreamOptions {
                max_rows: Some(request.max_rows.get()),
                max_payload_bytes: Some(request.max_payload_bytes.get()),
            },
            |row| {
                let Some(Value::Int(node)) = row.get("node") else {
                    return Err(HawDBError::StorageIntegrity(
                        "analytics returned an invalid node ID".into(),
                    ));
                };
                let value = row.get(value_column).ok_or_else(|| {
                    HawDBError::StorageIntegrity("analytics result is missing its value".into())
                })?;
                if !matches!(value, Value::Int(_) | Value::Float(_)) {
                    return Err(HawDBError::StorageIntegrity(
                        "analytics returned a nonscalar value".into(),
                    ));
                }
                let level = match request.algorithm {
                    GraphAnalyticsAlgorithm::PageRank(_) => 0,
                    GraphAnalyticsAlgorithm::Louvain(options) => match row.get("level") {
                        Some(Value::Int(level))
                            if usize::try_from(*level)
                                .is_ok_and(|level| level < options.max_levels.max(1)) =>
                        {
                            *level
                        }
                        _ => {
                            return Err(HawDBError::StorageIntegrity(
                                "analytics returned an invalid hierarchy level".into(),
                            ));
                        }
                    },
                };
                stage_analytics_row(
                    &mut rows,
                    *node,
                    level,
                    value,
                    request.max_staged_bytes.get(),
                )?;
                streamed_rows += 1;
                Ok(())
            },
        );
        permit.finish_with_outcome(result.is_ok());
        let report = result?;
        if !report.fully_streamed || report.output_rows != streamed_rows {
            return Err(HawDBError::Execution(
                "analytics staging did not cover the complete result".into(),
            ));
        }
        let nonce = NEXT_PUBLICATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                HawDBError::Execution("analytics publication identity exhausted".into())
            })?;
        let publication_id = format!("{}:{nonce}", snapshot.commit_epoch());
        Ok(PreparedGraphAnalytics {
            snapshot,
            request,
            publication_id,
            rows,
            _staging: staging,
            report,
        })
    }

    /// Atomically applies the staged scalar results and their provenance. A
    /// changed source is rejected before staging mutations. Repeating the same
    /// publication ID returns its durable outcome, even after a lost response.
    pub fn publish_graph_analytics(
        &mut self,
        prepared: &PreparedGraphAnalytics,
        property: &str,
        task_context: Option<&hawdb_core::RuntimeTaskContext>,
    ) -> Result<GraphAnalyticsPublicationStatus> {
        validate_identifier(property)?;
        if !Arc::ptr_eq(
            &prepared.snapshot._pin.pins,
            &self.runtime.get()?.reader_pins,
        ) {
            return Err(HawDBError::Execution(
                "analytics result belongs to a different database incarnation or branch".into(),
            ));
        }
        let existing =
            self.graph_analytics_publication_status(&prepared.request.projection, property)?;
        if existing.publication_id.as_deref() == Some(prepared.publication_id()) {
            return Ok(existing);
        }
        hawdb_executor::pipeline::runtime_checkpoint(task_context)?;
        let source_epoch = prepared.computed_at_commit_epoch();
        if self.commit_epoch()? != source_epoch {
            return Err(HawDBError::Execution(
                "analytics source epoch changed before publication".into(),
            ));
        }
        let published_epoch = source_epoch
            .checked_add(1)
            .ok_or_else(|| HawDBError::Execution("analytics publication epoch overflow".into()))?;
        let source = i64::try_from(source_epoch).map_err(|_| {
            HawDBError::Execution("analytics source epoch exceeds SQL BIGINT".into())
        })?;
        let published = i64::try_from(published_epoch).map_err(|_| {
            HawDBError::Execution("analytics publication epoch exceeds SQL BIGINT".into())
        })?;
        let operations = prepared.rows.len().saturating_mul(4).saturating_add(3);
        if operations > self.config.mutation_limits.max_operations.get()
            || self
                .config
                .max_wal_batch_operations
                .is_some_and(|max| operations > max)
        {
            return Err(HawDBError::Execution(
                "complete analytics publication exceeds transaction operation budget".into(),
            ));
        }
        let scheduler = self.local_qos_scheduler_for_work();
        let permit = scheduler
            .try_start(WorkRequest::background(WorkClass::Projection, operations))
            .map_err(|admission| {
                HawDBError::Execution(format!(
                    "background analytics publication not admitted: {admission:?}"
                ))
            })?;
        let result = (|| {
            let needs_schema = self
                .runtime
                .get()?
                .store
                .relational_state()
                .table_schema(PUBLICATIONS)
                .is_none();
            let mut tx = match task_context {
                Some(context) => self.begin_transaction_with_context(context)?,
                None => self.begin_transaction()?,
            };
            // The property name is validated structural input. Every data value,
            // internal ID, and epoch is bound. The host does no graph scan or join.
            let update = format!("MATCH (n) WHERE id(n) = $node SET n.{property} = $value, n.{property}_computed_at_commit_epoch = $source, n.{property}_published_at_commit_epoch = $published, n.{property}_publication_id = $publication");
            for (ordinal, (node, (_, value))) in prepared.rows.iter().enumerate() {
                if ordinal.is_multiple_of(1024) {
                    hawdb_executor::pipeline::runtime_checkpoint(task_context)?;
                }
                tx.query_with_params(
                    &update,
                    &BTreeMap::from([
                        ("node".into(), Value::Int(*node)),
                        ("value".into(), value.clone()),
                        ("source".into(), Value::Int(source)),
                        ("published".into(), Value::Int(published)),
                        (
                            "publication".into(),
                            Value::String(prepared.publication_id.clone()),
                        ),
                    ]),
                )?;
            }
            if needs_schema {
                tx.query_sql(&format!("CREATE TABLE {PUBLICATIONS} (projection TEXT NOT NULL, property TEXT NOT NULL, publication_id TEXT NOT NULL, computed_at_commit_epoch BIGINT NOT NULL, published_at_commit_epoch BIGINT NOT NULL, algorithm TEXT NOT NULL, definition TEXT NOT NULL, row_count BIGINT NOT NULL, PRIMARY KEY (projection, property))"))?;
            }
            let key = [
                Value::String(prepared.request.projection.clone()),
                Value::String(property.into()),
            ];
            tx.query_sql_with_params(
                &format!("DELETE FROM {PUBLICATIONS} WHERE projection = $1 AND property = $2"),
                &key,
            )?;
            let definition = prepared
                .snapshot
                .store
                .projected_graph_definition(&prepared.request.projection)
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity("pinned analytics projection disappeared".into())
                })?;
            tx.query_sql_with_params(&format!("INSERT INTO {PUBLICATIONS} (projection, property, publication_id, computed_at_commit_epoch, published_at_commit_epoch, algorithm, definition, row_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"), &[
            key[0].clone(), key[1].clone(), Value::String(prepared.publication_id.clone()), Value::Int(source), Value::Int(published),
            Value::String(format!("{:?}", prepared.request.algorithm)), Value::String(format!("{definition:?}")),
            Value::Int(i64::try_from(prepared.rows.len()).map_err(|_| HawDBError::Execution("analytics row count overflow".into()))?),
        ])?;
            hawdb_executor::pipeline::runtime_checkpoint(task_context)?;
            tx.commit()?;
            self.graph_analytics_publication_status(&prepared.request.projection, property)
        })();
        permit.finish_with_outcome(result.is_ok());
        result
    }

    /// Freshness is conservative: any commit after publication makes the result
    /// stale. Publishing the result itself does not make it immediately stale.
    /// Nodes outside a refreshed projection retain older property values;
    /// readers must match the property's `_publication_id` to this status.
    pub fn graph_analytics_publication_status(
        &self,
        projection: &str,
        property: &str,
    ) -> Result<GraphAnalyticsPublicationStatus> {
        let snapshot = self.begin_read_transaction()?;
        let current = snapshot.commit_epoch();
        let mut status = GraphAnalyticsPublicationStatus {
            projection: projection.into(),
            property: property.into(),
            publication_id: None,
            computed_at_commit_epoch: None,
            published_at_commit_epoch: None,
            current_commit_epoch: current,
            freshness: GraphAnalyticsFreshness::Unavailable,
        };
        let Some(schema) = snapshot.store.relational_state().table_schema(PUBLICATIONS) else {
            return Ok(status);
        };
        if schema != &publication_schema() {
            return Err(HawDBError::StorageIntegrity(
                "analytics publication schema does not match its contract".into(),
            ));
        }
        let output = snapshot.query_sql_with_params_bounded(&format!("SELECT publication_id, computed_at_commit_epoch, published_at_commit_epoch FROM {PUBLICATIONS} WHERE projection = $1 AND property = $2"), &[
            Value::String(projection.into()), Value::String(property.into()),
        ], Some(1))?;
        if let Some(row) = output.rows.first() {
            let (Some(Value::String(id)), Some(Value::Int(source)), Some(Value::Int(published))) = (
                row.get("publication_id"),
                row.get("computed_at_commit_epoch"),
                row.get("published_at_commit_epoch"),
            ) else {
                return Err(HawDBError::StorageIntegrity(
                    "analytics publication has invalid provenance".into(),
                ));
            };
            if *source < 0 || *published <= *source || *published as u64 > current {
                return Err(HawDBError::StorageIntegrity(
                    "analytics publication has inconsistent epochs".into(),
                ));
            }
            status.publication_id = Some(id.clone());
            status.computed_at_commit_epoch = Some(*source as u64);
            status.published_at_commit_epoch = Some(*published as u64);
            status.freshness = if *published as u64 == current {
                GraphAnalyticsFreshness::Fresh
            } else {
                GraphAnalyticsFreshness::Stale
            };
        }
        Ok(status)
    }
}

fn stage_analytics_row(
    rows: &mut BTreeMap<i64, (i64, Value)>,
    node: i64,
    level: i64,
    value: &Value,
    max_staged_bytes: usize,
) -> Result<()> {
    // The charge covers the scalar, hierarchy level and BTreeMap overhead.
    // Publication iterates this map directly, avoiding a second retained copy.
    let bytes = rows
        .len()
        .saturating_add(1)
        .saturating_mul(STAGED_ROW_BYTES)
        .saturating_add(STAGED_HEADER_BYTES);
    match rows.entry(node) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            if bytes > max_staged_bytes {
                return Err(HawDBError::Execution(
                    "complete analytics result exceeds max_staged_bytes".into(),
                ));
            }
            entry.insert((level, value.clone()));
        }
        std::collections::btree_map::Entry::Occupied(mut entry) => {
            let (previous_level, previous_value) = entry.get();
            if level > *previous_level {
                entry.insert((level, value.clone()));
            } else if level == *previous_level && value != previous_value {
                return Err(HawDBError::StorageIntegrity(
                    "analytics returned conflicting values for one node and level".into(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_request(request: &GraphAnalyticsRequest) -> Result<()> {
    if request.projection.is_empty()
        || request.projection.len() > 256
        || request.max_staged_bytes.get() < STAGED_HEADER_BYTES
    {
        return Err(HawDBError::Execution(
            "invalid analytics projection or staging budget".into(),
        ));
    }
    match request.algorithm {
        GraphAnalyticsAlgorithm::PageRank(options)
            if !options.damping.is_finite() || !(0.0..=1.0).contains(&options.damping) =>
        {
            Err(HawDBError::Execution(
                "analytics damping must be finite and between zero and one".into(),
            ))
        }
        _ => Ok(()),
    }
}

fn validate_identifier(identifier: &str) -> Result<()> {
    if identifier.is_empty()
        || identifier.len() > 128
        || !identifier.bytes().enumerate().all(|(index, byte)| {
            byte == b'_' || byte.is_ascii_alphabetic() || index > 0 && byte.is_ascii_digit()
        })
    {
        return Err(HawDBError::Semantic(
            "analytics property must be an ASCII identifier of at most 128 bytes".into(),
        ));
    }
    Ok(())
}

fn algorithm_query(
    request: &GraphAnalyticsRequest,
) -> Result<(String, BTreeMap<String, Value>, &'static str)> {
    let graph = request
        .projection
        .replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    let integer = |value: usize| {
        i64::try_from(value).map(Value::Int).map_err(|_| {
            HawDBError::Execution("analytics iteration count exceeds Cypher integer".into())
        })
    };
    match request.algorithm {
        GraphAnalyticsAlgorithm::PageRank(options) => Ok((
            format!("CALL page_rank('{graph}', maxIterations := $iterations, dampingFactor := $damping) RETURN node, pagerank_score"),
            BTreeMap::from([("iterations".into(), integer(options.iterations)?), ("damping".into(), Value::Float(options.damping))]), "pagerank_score",
        )),
        GraphAnalyticsAlgorithm::Louvain(options) => Ok((
            format!("CALL louvain('{graph}', maxIterations := $iterations, maxLevels := $levels) RETURN node, level, louvain_id"),
            BTreeMap::from([("iterations".into(), integer(options.max_iterations)?), ("levels".into(), integer(options.max_levels)?)]), "louvain_id",
        )),
    }
}

fn publication_schema() -> hawdb_storage::relational::RelationalTableSchema {
    use hawdb_storage::relational::{
        RelationalColumnSchema, RelationalScalarType, RelationalTableSchema,
    };
    RelationalTableSchema {
        name: PUBLICATIONS.into(),
        columns: [
            ("projection", RelationalScalarType::Text),
            ("property", RelationalScalarType::Text),
            ("publication_id", RelationalScalarType::Text),
            ("computed_at_commit_epoch", RelationalScalarType::BigInt),
            ("published_at_commit_epoch", RelationalScalarType::BigInt),
            ("algorithm", RelationalScalarType::Text),
            ("definition", RelationalScalarType::Text),
            ("row_count", RelationalScalarType::BigInt),
        ]
        .into_iter()
        .map(|(name, scalar_type)| RelationalColumnSchema {
            name: name.into(),
            scalar_type,
            nullable: false,
            default: None,
        })
        .collect(),
        primary_key: vec!["projection".into(), "property".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_selects_the_highest_level_independently_of_row_order() {
        let inputs = [(1, 0, 1), (2, 0, 2), (1, 2, 3), (2, 2, 3), (1, 1, 2)];
        let budget = STAGED_HEADER_BYTES + 2 * STAGED_ROW_BYTES;
        let mut forward = BTreeMap::new();
        let mut reverse = BTreeMap::new();
        for (node, level, value) in inputs {
            stage_analytics_row(&mut forward, node, level, &Value::Int(value), budget).unwrap();
        }
        for (node, level, value) in inputs.into_iter().rev() {
            stage_analytics_row(&mut reverse, node, level, &Value::Int(value), budget).unwrap();
        }
        let expected = BTreeMap::from([(1, (2, Value::Int(3))), (2, (2, Value::Int(3)))]);
        assert_eq!(forward, expected);
        assert_eq!(reverse, expected);
        assert!(
            stage_analytics_row(&mut forward, 3, 0, &Value::Int(3), budget)
                .unwrap_err()
                .to_string()
                .contains("max_staged_bytes")
        );
        assert!(
            stage_analytics_row(&mut forward, 1, 2, &Value::Int(4), budget)
                .unwrap_err()
                .to_string()
                .contains("conflicting values")
        );
        assert_eq!(forward, expected);
    }
}
