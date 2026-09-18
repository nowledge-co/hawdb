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

use std::collections::BTreeMap;

use super::PgqDataType;

pub trait PgqCatalog {
    fn property_graph(&self, name: &[String]) -> Option<&PropertyGraphSchema>;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PropertyGraphCatalog {
    graphs: BTreeMap<Vec<String>, PropertyGraphSchema>,
}

impl PropertyGraphCatalog {
    pub fn insert(&mut self, graph: PropertyGraphSchema) -> Option<PropertyGraphSchema> {
        self.graphs.insert(graph.name.clone(), graph)
    }
}

impl PgqCatalog for PropertyGraphCatalog {
    fn property_graph(&self, name: &[String]) -> Option<&PropertyGraphSchema> {
        self.graphs.get(name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyGraphSchema {
    pub name: Vec<String>,
    pub vertex_labels: BTreeMap<String, PropertyGraphElementSchema>,
    pub edge_labels: BTreeMap<String, PropertyGraphElementSchema>,
}

impl PropertyGraphSchema {
    pub fn new(name: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            name: name.into_iter().map(Into::into).collect(),
            vertex_labels: BTreeMap::new(),
            edge_labels: BTreeMap::new(),
        }
    }

    pub fn add_vertex_label(
        &mut self,
        name: impl Into<String>,
        schema: PropertyGraphElementSchema,
    ) {
        self.vertex_labels.insert(name.into(), schema);
    }

    pub fn add_edge_label(&mut self, name: impl Into<String>, schema: PropertyGraphElementSchema) {
        self.edge_labels.insert(name.into(), schema);
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PropertyGraphElementSchema {
    pub properties: BTreeMap<String, PgqDataType>,
}

impl PropertyGraphElementSchema {
    pub fn with_property(mut self, name: impl Into<String>, data_type: PgqDataType) -> Self {
        self.properties.insert(name.into(), data_type);
        self
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PgqBindingContext {
    pub outer_columns: BTreeMap<Vec<String>, PgqDataType>,
}

impl PgqBindingContext {
    pub fn with_outer_column(
        mut self,
        name: impl IntoIterator<Item = impl Into<String>>,
        data_type: PgqDataType,
    ) -> Self {
        self.outer_columns
            .insert(name.into_iter().map(Into::into).collect(), data_type);
        self
    }
}
