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

use crate::api::*;
use crate::error::Result;

mod entities;
mod properties;

use entities::{
    knowledge_entity_batch_via_query_runtime, knowledge_entity_via_query_runtime,
    knowledge_scoped_entity_batch_via_query_runtime, knowledge_scoped_entity_via_query_runtime,
};
use properties::{
    knowledge_property_batch_via_query_runtime, knowledge_scoped_property_batch_via_query_runtime,
};

pub(super) trait QueryFirstTestExt {
    fn query_entity_via_cypher(
        &self,
        request: &KnowledgeEntityRequest,
    ) -> Result<KnowledgeEntityOutput>;

    fn query_entity_batch_via_cypher(
        &self,
        request: &KnowledgeEntityBatchRequest,
    ) -> Result<KnowledgeEntityBatchOutput>;

    fn query_community_entity_visibility_via_cypher(
        &self,
        request: &KnowledgeCommunityEntityVisibilityRequest,
    ) -> Result<KnowledgeCommunityEntityVisibilityOutput>;

    fn query_community_memories_via_cypher(
        &self,
        request: &KnowledgeCommunityMemoryListRequest,
    ) -> Result<KnowledgeCommunityMemoryListOutput>;

    fn query_crystals_via_cypher(
        &self,
        request: &KnowledgeCrystalListRequest,
    ) -> Result<KnowledgeCrystalListOutput>;

    fn query_crystal_communities_via_cypher(
        &self,
        request: &KnowledgeCrystalCommunityListRequest,
    ) -> Result<KnowledgeCrystalCommunityListOutput>;

    fn query_crystal_source_visibility_via_cypher(
        &self,
        request: &KnowledgeCrystalSourceVisibilityRequest,
    ) -> Result<KnowledgeCrystalSourceVisibilityOutput>;

    fn query_scoped_entity_via_cypher(
        &self,
        request: &KnowledgeScopedEntityRequest,
    ) -> Result<KnowledgeEntityOutput>;

    fn query_scoped_entity_batch_via_cypher(
        &self,
        request: &KnowledgeScopedEntityBatchRequest,
    ) -> Result<KnowledgeEntityBatchOutput>;

    fn query_property_batch_via_cypher(
        &self,
        request: &KnowledgePropertyBatchRequest,
    ) -> Result<KnowledgePropertyBatchOutput>;

    fn query_scoped_property_batch_via_cypher(
        &self,
        request: &KnowledgeScopedPropertyBatchRequest,
    ) -> Result<KnowledgePropertyBatchOutput>;

    fn query_entity_labels_via_cypher(
        &self,
        request: &KnowledgeEntityLabelListRequest,
    ) -> Result<KnowledgeEntityLabelListOutput>;

    fn query_entity_label_projected_list_via_cypher(
        &self,
        request: &KnowledgeEntityLabelProjectedListRequest,
    ) -> Result<KnowledgeEntityLabelProjectedListOutput>;

    fn query_communities_via_cypher(
        &self,
        request: &KnowledgeCommunityListRequest,
    ) -> Result<KnowledgeCommunityListOutput>;

    fn query_community_via_cypher(
        &self,
        request: &KnowledgeCommunityRequest,
    ) -> Result<KnowledgeCommunityOutput>;

    fn query_neighbors_via_cypher(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> Result<KnowledgeNeighborsOutput>;

    fn query_scoped_neighbors_via_cypher(
        &self,
        request: &KnowledgeScopedNeighborsRequest,
    ) -> Result<KnowledgeNeighborsOutput>;

    fn query_relationships_via_cypher(
        &self,
        request: &KnowledgeRelationshipsRequest,
    ) -> Result<KnowledgeRelationshipsOutput>;

    fn query_scoped_relationships_via_cypher(
        &self,
        request: &KnowledgeScopedRelationshipsRequest,
    ) -> Result<KnowledgeRelationshipsOutput>;

    fn query_induced_edges_via_cypher(
        &self,
        request: &KnowledgeInducedEdgeListRequest,
    ) -> Result<KnowledgeInducedEdgeListOutput>;

    fn query_paths_via_cypher(&self, request: &KnowledgePathRequest)
        -> Result<KnowledgePathOutput>;

    fn query_scoped_paths_via_cypher(
        &self,
        request: &KnowledgeScopedPathRequest,
    ) -> Result<KnowledgePathOutput>;

    fn query_subgraph_via_cypher(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> Result<KnowledgeSubgraphOutput>;

    fn query_scoped_subgraph_via_cypher(
        &self,
        request: &KnowledgeScopedSubgraphRequest,
    ) -> Result<KnowledgeSubgraphOutput>;
}

impl QueryFirstTestExt for Database {
    fn query_entity_via_cypher(
        &self,
        request: &KnowledgeEntityRequest,
    ) -> Result<KnowledgeEntityOutput> {
        knowledge_entity_via_query_runtime(self, request)
    }

    fn query_entity_batch_via_cypher(
        &self,
        request: &KnowledgeEntityBatchRequest,
    ) -> Result<KnowledgeEntityBatchOutput> {
        knowledge_entity_batch_via_query_runtime(self, request)
    }

    fn query_community_entity_visibility_via_cypher(
        &self,
        request: &KnowledgeCommunityEntityVisibilityRequest,
    ) -> Result<KnowledgeCommunityEntityVisibilityOutput> {
        super::super::knowledge_community_entity_visibility_via_query_runtime(self, request)
    }

    fn query_community_memories_via_cypher(
        &self,
        request: &KnowledgeCommunityMemoryListRequest,
    ) -> Result<KnowledgeCommunityMemoryListOutput> {
        super::super::knowledge_community_memories_via_query_runtime(self, request)
    }

    fn query_crystals_via_cypher(
        &self,
        request: &KnowledgeCrystalListRequest,
    ) -> Result<KnowledgeCrystalListOutput> {
        super::super::knowledge_crystals_via_query_runtime(self, request)
    }

    fn query_crystal_communities_via_cypher(
        &self,
        request: &KnowledgeCrystalCommunityListRequest,
    ) -> Result<KnowledgeCrystalCommunityListOutput> {
        super::super::knowledge_crystal_communities_via_query_runtime(self, request)
    }

    fn query_crystal_source_visibility_via_cypher(
        &self,
        request: &KnowledgeCrystalSourceVisibilityRequest,
    ) -> Result<KnowledgeCrystalSourceVisibilityOutput> {
        super::super::knowledge_crystal_source_visibility_via_query_runtime(self, request)
    }

    fn query_scoped_entity_via_cypher(
        &self,
        request: &KnowledgeScopedEntityRequest,
    ) -> Result<KnowledgeEntityOutput> {
        knowledge_scoped_entity_via_query_runtime(self, request)
    }

    fn query_scoped_entity_batch_via_cypher(
        &self,
        request: &KnowledgeScopedEntityBatchRequest,
    ) -> Result<KnowledgeEntityBatchOutput> {
        knowledge_scoped_entity_batch_via_query_runtime(self, request)
    }

    fn query_property_batch_via_cypher(
        &self,
        request: &KnowledgePropertyBatchRequest,
    ) -> Result<KnowledgePropertyBatchOutput> {
        knowledge_property_batch_via_query_runtime(self, request)
    }

    fn query_scoped_property_batch_via_cypher(
        &self,
        request: &KnowledgeScopedPropertyBatchRequest,
    ) -> Result<KnowledgePropertyBatchOutput> {
        knowledge_scoped_property_batch_via_query_runtime(self, request)
    }

    fn query_entity_labels_via_cypher(
        &self,
        request: &KnowledgeEntityLabelListRequest,
    ) -> Result<KnowledgeEntityLabelListOutput> {
        super::super::knowledge_entity_labels_via_query_runtime(self, request)
    }

    fn query_entity_label_projected_list_via_cypher(
        &self,
        request: &KnowledgeEntityLabelProjectedListRequest,
    ) -> Result<KnowledgeEntityLabelProjectedListOutput> {
        super::super::knowledge_entity_label_projected_list_via_query_runtime(self, request)
    }

    fn query_communities_via_cypher(
        &self,
        request: &KnowledgeCommunityListRequest,
    ) -> Result<KnowledgeCommunityListOutput> {
        super::super::knowledge_communities_via_query_runtime(self, request)
    }

    fn query_community_via_cypher(
        &self,
        request: &KnowledgeCommunityRequest,
    ) -> Result<KnowledgeCommunityOutput> {
        super::super::knowledge_community_via_query_runtime(self, request)
    }

    fn query_neighbors_via_cypher(
        &self,
        request: &KnowledgeNeighborsRequest,
    ) -> Result<KnowledgeNeighborsOutput> {
        super::super::knowledge_neighbors_via_query_runtime(self, request)
    }

    fn query_scoped_neighbors_via_cypher(
        &self,
        request: &KnowledgeScopedNeighborsRequest,
    ) -> Result<KnowledgeNeighborsOutput> {
        super::super::knowledge_scoped_neighbors_via_query_runtime(self, request)
    }

    fn query_relationships_via_cypher(
        &self,
        request: &KnowledgeRelationshipsRequest,
    ) -> Result<KnowledgeRelationshipsOutput> {
        super::super::knowledge_relationships_via_query_runtime(self, request)
    }

    fn query_scoped_relationships_via_cypher(
        &self,
        request: &KnowledgeScopedRelationshipsRequest,
    ) -> Result<KnowledgeRelationshipsOutput> {
        super::super::knowledge_scoped_relationships_via_query_runtime(self, request)
    }

    fn query_induced_edges_via_cypher(
        &self,
        request: &KnowledgeInducedEdgeListRequest,
    ) -> Result<KnowledgeInducedEdgeListOutput> {
        super::super::knowledge_induced_edges_via_query_runtime(self, request)
    }

    fn query_paths_via_cypher(
        &self,
        request: &KnowledgePathRequest,
    ) -> Result<KnowledgePathOutput> {
        super::super::knowledge_paths_via_query_runtime(self, request)
    }

    fn query_scoped_paths_via_cypher(
        &self,
        request: &KnowledgeScopedPathRequest,
    ) -> Result<KnowledgePathOutput> {
        super::super::knowledge_scoped_paths_via_query_runtime(self, request)
    }

    fn query_subgraph_via_cypher(
        &self,
        request: &KnowledgeSubgraphRequest,
    ) -> Result<KnowledgeSubgraphOutput> {
        super::super::knowledge_subgraph_via_query_runtime(self, request)
    }

    fn query_scoped_subgraph_via_cypher(
        &self,
        request: &KnowledgeScopedSubgraphRequest,
    ) -> Result<KnowledgeSubgraphOutput> {
        super::super::knowledge_scoped_subgraph_via_query_runtime(self, request)
    }
}
