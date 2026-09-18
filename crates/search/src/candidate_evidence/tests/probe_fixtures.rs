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

use crate::NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS;

pub(crate) fn ready_probe() -> serde_json::Value {
    serde_json::json!({
        "requests": [
            {
                "primary_candidate_ids": ["mem_1", "mem_2"],
                "shadow_candidate_ids": ["mem_1", "mem_2"]
            },
            {
                "primary_candidate_ids": ["mem_3"],
                "shadow_candidate_ids": ["mem_3"]
            }
        ],
        "filter_pushdown": {
            "pushed_predicate_count": 1,
            "fields": NOWLEDGE_SEARCH_PROJECTION_SCAN_FILTER_FIELDS
        },
        "retriever_legs": {
            "text": {
                "available": true,
                "candidate_count": 3
            },
            "vector": {
                "available": true,
                "candidate_count": 3
            }
        },
        "top_k_overlap": {
            "fts": {
                "primary_candidate_ids": ["mem_1", "mem_2"],
                "shadow_candidate_ids": ["mem_1", "mem_2"]
            },
            "vector": {
                "primary_candidate_ids": ["mem_3"],
                "shadow_candidate_ids": ["mem_3"]
            }
        },
        "candidate_readiness": {
            "source_chunk_identity_ready": true,
            "fail_soft_observed": true,
            "projection_marker_status_visible": true,
            "projection_watermark_ready": true,
            "embedding_identity_ready": true
        }
    })
}
