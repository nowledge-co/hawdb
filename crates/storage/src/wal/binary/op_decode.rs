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

//! Shared WAL operation dispatch. Backends own allocation and cooperative work;
//! operation codes, field identities and required-field order live here once.

use super::*;
use crate::projection::ProjectedRelationshipPredicate;

pub(super) trait OpFieldsDecode {
    fn required_string(&mut self, id: u32, name: &str) -> Result<String>;
    fn strings_for(&mut self, id: u32) -> Result<Vec<String>>;
    fn required_varint(&self, id: u32, name: &str) -> Result<u64>;
    fn properties_for(&self, id: u32) -> Result<BTreeMap<String, Value>>;
    fn value(&self, id: u32, name: &str) -> Result<Value>;
    fn record(&self, id: u32, name: &str) -> Result<Arc<[u8]>>;
    fn predicates(&mut self) -> Result<BTreeMap<String, ProjectedRelationshipPredicate>>;
}

pub(super) trait OpDecoder<'a> {
    type Fields: OpFieldsDecode;
    fn parse(
        &self,
        body: &'a [u8],
        strings: &'static [u32],
        messages: &'static [u32],
    ) -> Result<Self::Fields>;
}

pub(super) fn decode_op_body<'a, D: OpDecoder<'a>>(
    op_code: u64,
    body: &'a [u8],
    decoder: &D,
) -> Result<WalOp> {
    match op_code {
        OP_CREATE_NODE_LABEL => {
            let mut fields = decoder.parse(body, &[1], &[])?;
            Ok(WalOp::CreateNodeLabel {
                label: fields.required_string(1, "label")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TYPE => {
            let mut fields = decoder.parse(body, &[1], &[])?;
            Ok(WalOp::CreateRelationshipType {
                rel_type: fields.required_string(1, "relationship type")?,
            })
        }
        OP_CREATE_NODE_TABLE => {
            let mut fields = decoder.parse(body, &[1], &[])?;
            Ok(WalOp::CreateNodeTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_RELATIONSHIP_TABLE => {
            let mut fields = decoder.parse(body, &[1], &[])?;
            Ok(WalOp::CreateRelationshipTable {
                name: fields.required_string(1, "table name")?,
            })
        }
        OP_CREATE_PROPERTY => {
            let mut fields = decoder.parse(body, &[2, 3], &[])?;
            Ok(WalOp::CreateProperty {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                value_type: decode_property_type_code(fields.required_varint(4, "value type")?)?,
                nullable: fields.required_varint(5, "nullable flag")? != 0,
            })
        }
        OP_ALTER_TABLE_STATE => {
            let mut fields = decoder.parse(body, &[2], &[])?;
            Ok(WalOp::AlterTableState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                state: decode_schema_object_state_code(fields.required_varint(3, "state")?)?,
            })
        }
        OP_ALTER_PROPERTY_STATE => {
            let mut fields = decoder.parse(body, &[2, 3], &[])?;
            Ok(WalOp::AlterPropertyState {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
                state: decode_schema_object_state_code(fields.required_varint(4, "state")?)?,
            })
        }
        OP_GC_TABLE_DESCRIPTOR => {
            let mut fields = decoder.parse(body, &[2], &[])?;
            Ok(WalOp::GcTableDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
            })
        }
        OP_GC_PROPERTY_DESCRIPTOR => {
            let mut fields = decoder.parse(body, &[2, 3], &[])?;
            Ok(WalOp::GcPropertyDescriptor {
                table_kind: decode_table_kind_code(fields.required_varint(1, "table kind")?)?,
                table: fields.required_string(2, "table")?,
                property: fields.required_string(3, "property")?,
            })
        }
        OP_CREATE_INDEX => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_COMPOSITE_INDEX => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateCompositeIndex {
                label: fields.required_string(1, "label")?,
                properties: fields.strings_for(2)?,
            })
        }
        OP_CREATE_RANGE_INDEX => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRangeIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_FULL_TEXT_INDEX => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateFullTextIndex {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_UNIQUE_CONSTRAINT => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateUniqueConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE_PROPERTY_EXISTS_CONSTRAINT => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateNodePropertyExistsConstraint {
                label: fields.required_string(1, "label")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_UNIQUE_CONSTRAINT => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRelationshipUniqueConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_RELATIONSHIP_PROPERTY_EXISTS_CONSTRAINT => {
            let mut fields = decoder.parse(body, &[1, 2], &[])?;
            Ok(WalOp::CreateRelationshipPropertyExistsConstraint {
                rel_type: fields.required_string(1, "relationship type")?,
                property: fields.required_string(2, "property")?,
            })
        }
        OP_CREATE_NODE => {
            let mut fields = decoder.parse(body, &[2], &[3])?;
            Ok(WalOp::CreateNode {
                id: NodeId(fields.required_varint(1, "node id")?),
                label: fields.required_string(2, "label")?,
                properties: fields.properties_for(3)?,
            })
        }
        OP_CREATE_RELATIONSHIP => {
            let mut fields = decoder.parse(body, &[4], &[5])?;
            Ok(WalOp::CreateRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
                source: NodeId(fields.required_varint(2, "source node id")?),
                target: NodeId(fields.required_varint(3, "target node id")?),
                rel_type: fields.required_string(4, "relationship type")?,
                properties: fields.properties_for(5)?,
            })
        }
        OP_SET_NODE_PROPERTY => {
            let mut fields = decoder.parse(body, &[2], &[3])?;
            Ok(WalOp::SetNodeProperty {
                id: NodeId(fields.required_varint(1, "node id")?),
                property: fields.required_string(2, "property")?,
                value: fields.value(3, "value")?,
            })
        }
        OP_SET_RELATIONSHIP_PROPERTY => {
            let mut fields = decoder.parse(body, &[2], &[3])?;
            Ok(WalOp::SetRelationshipProperty {
                id: RelId(fields.required_varint(1, "relationship id")?),
                property: fields.required_string(2, "property")?,
                value: fields.value(3, "value")?,
            })
        }
        OP_DELETE_NODE => {
            let fields = decoder.parse(body, &[], &[])?;
            Ok(WalOp::DeleteNode {
                id: NodeId(fields.required_varint(1, "node id")?),
            })
        }
        OP_DELETE_RELATIONSHIP => {
            let fields = decoder.parse(body, &[], &[])?;
            Ok(WalOp::DeleteRelationship {
                id: RelId(fields.required_varint(1, "relationship id")?),
            })
        }
        OP_PROJECT_GRAPH => {
            let mut fields = decoder.parse(body, &[1, 2, 3, 4], &[])?;
            let relationship_predicates = fields.predicates()?;
            Ok(WalOp::ProjectGraph {
                name: fields.required_string(1, "projected graph name")?,
                node_labels: fields.strings_for(2)?,
                rel_types: fields.strings_for(3)?,
                relationship_predicates,
            })
        }
        OP_MARK_INITIAL_IMPORT_SOURCE => {
            let mut fields = decoder.parse(body, &[1], &[])?;
            Ok(WalOp::MarkInitialImportSource {
                source_fingerprint: fields.required_string(1, "source fingerprint")?,
            })
        }
        OP_RELATIONAL => {
            let fields = decoder.parse(body, &[], &[1])?;
            Ok(WalOp::Relational {
                record: fields.record(1, "relational record")?,
            })
        }
        OP_RELATIONAL_SNAPSHOT => {
            let fields = decoder.parse(body, &[], &[1])?;
            Ok(WalOp::RelationalSnapshot {
                record: fields.record(1, "relational record")?,
            })
        }
        OP_APPEND => {
            let fields = decoder.parse(body, &[], &[1])?;
            Ok(WalOp::Append {
                record: fields.record(1, "append record")?,
            })
        }
        op_code => Err(HawDBError::Storage(format!(
            "unknown WAL op code {op_code}"
        ))),
    }
}
