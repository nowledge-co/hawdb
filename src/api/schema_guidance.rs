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

use super::{BoundedReadQueryOutput, Database, DatabaseReadTransaction, QueryOutput};
use crate::error::{HawDBError, Result};
use crate::value::Value;
use hawdb_core::{
    build_graph_rag_schema_context, GraphRagGeneratedQuery, GraphRagSchemaContext,
    GraphRagSchemaContextOptions,
};
use std::collections::BTreeMap;

impl Database {
    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        build_graph_rag_schema_context(
            &self.catalog,
            &self.store.statistics(&self.catalog),
            options,
        )
    }
}

impl DatabaseReadTransaction {
    pub fn graph_rag_schema_context(
        &self,
        options: GraphRagSchemaContextOptions,
    ) -> GraphRagSchemaContext {
        build_graph_rag_schema_context(
            &self.catalog,
            &self.store.statistics(&self.catalog),
            options,
        )
    }

    pub fn query_generated_graph_rag(
        &mut self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        Ok(self
            .query_generated_graph_rag_bounded_profile(
                query,
                parameters,
                self.config.max_read_result_rows,
            )?
            .output)
    }

    pub fn query_generated_graph_rag_bounded_profile(
        &mut self,
        query: &GraphRagGeneratedQuery,
        parameters: &BTreeMap<String, Value>,
        max_rows: Option<usize>,
    ) -> Result<BoundedReadQueryOutput> {
        let pinned_epoch = self
            .store
            .statistics(&self.catalog)
            .computed_at_commit_epoch;
        if query.context_commit_epoch() != pinned_epoch {
            return Err(HawDBError::Semantic(format!(
                "GraphRAG schema context is stale: generated at graph epoch {}, pinned at {}",
                query.context_commit_epoch(),
                pinned_epoch
            )));
        }
        query
            .validate_parameters(parameters)
            .map_err(|error| HawDBError::Semantic(error.to_string()))?;
        self.query_with_params_bounded_profile(query.cypher(), parameters, max_rows)
    }
}
