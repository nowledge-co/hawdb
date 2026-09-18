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

use hawdb::Value;

pub(super) struct ThreadDocumentParameters<'a> {
    pub(super) content_document_id: &'a str,
    pub(super) thread_storage_id: &'a str,
    pub(super) space_id: &'a str,
    pub(super) media_type: &'a str,
    pub(super) created_at: &'a str,
    pub(super) updated_at: &'a str,
}

pub(super) struct ThreadMessageParameters<'a> {
    pub(super) content_message_id: &'a str,
    pub(super) message_id: &'a str,
    pub(super) thread_storage_id: &'a str,
    pub(super) thread_id: &'a str,
    pub(super) content_document_id: &'a str,
    pub(super) space_id: &'a str,
    pub(super) order_index: i64,
    pub(super) role: &'a str,
    pub(super) content: &'a str,
    pub(super) timestamp: &'a str,
    pub(super) token_count: i64,
    pub(super) metadata_json: &'a str,
    pub(super) external_id: &'a str,
    pub(super) exclude_from_distillation: bool,
    pub(super) content_hash: &'a str,
    pub(super) created_at: &'a str,
    pub(super) updated_at: &'a str,
}

pub(super) struct ThreadMessageAnchorParameters<'a> {
    pub(super) anchor_id: &'a str,
    pub(super) memory_id: &'a str,
    pub(super) content_document_id: &'a str,
    pub(super) thread_storage_id: &'a str,
    pub(super) content_message_id: Option<&'a str>,
    pub(super) message_id: &'a str,
    pub(super) order_index: i64,
    pub(super) quote_hash: &'a str,
    pub(super) metadata_json: &'a str,
    pub(super) created_at: &'a str,
}

pub(super) fn thread_document_parameters(parameters: ThreadDocumentParameters<'_>) -> Vec<Value> {
    vec![
        Value::String(parameters.content_document_id.to_string()),
        Value::String("thread".to_string()),
        Value::String(parameters.thread_storage_id.to_string()),
        Value::String(parameters.space_id.to_string()),
        Value::String(parameters.media_type.to_string()),
        Value::Int(1),
        Value::String(parameters.created_at.to_string()),
        Value::String(parameters.updated_at.to_string()),
    ]
}

pub(super) fn thread_message_parameters(parameters: ThreadMessageParameters<'_>) -> Vec<Value> {
    vec![
        Value::String(parameters.content_message_id.to_string()),
        Value::String(parameters.message_id.to_string()),
        Value::String(parameters.thread_storage_id.to_string()),
        Value::String(parameters.thread_id.to_string()),
        Value::String(parameters.content_document_id.to_string()),
        Value::String(parameters.space_id.to_string()),
        Value::Int(parameters.order_index),
        Value::String(parameters.role.to_string()),
        Value::String(parameters.content.to_string()),
        Value::String(parameters.timestamp.to_string()),
        Value::Int(parameters.token_count),
        Value::String(parameters.metadata_json.to_string()),
        Value::String(parameters.external_id.to_string()),
        Value::Bool(parameters.exclude_from_distillation),
        Value::String(parameters.content_hash.to_string()),
        Value::String(parameters.created_at.to_string()),
        Value::String(parameters.updated_at.to_string()),
    ]
}

pub(super) fn thread_message_anchor_parameters(
    parameters: ThreadMessageAnchorParameters<'_>,
) -> Vec<Value> {
    vec![
        Value::String(parameters.anchor_id.to_string()),
        Value::String(parameters.memory_id.to_string()),
        Value::String(parameters.content_document_id.to_string()),
        Value::String(parameters.thread_storage_id.to_string()),
        parameters
            .content_message_id
            .map_or(Value::Null, |value| Value::String(value.to_string())),
        Value::String(parameters.message_id.to_string()),
        Value::Int(parameters.order_index),
        Value::String(parameters.quote_hash.to_string()),
        Value::String(parameters.metadata_json.to_string()),
        Value::String(parameters.created_at.to_string()),
    ]
}

pub(super) fn thread_page_parameters(thread_storage_id: &str, limit: usize) -> Vec<Value> {
    vec![
        Value::String(thread_storage_id.to_string()),
        Value::Int(i64::try_from(limit).unwrap_or(i64::MAX)),
        Value::Int(0),
    ]
}
