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

use super::query::ProductionVectorQueryEvidence;
use super::{ProductionVectorQualificationConfig, ProductionVectorQualificationError};
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProductionVectorRaBitQReferenceCase {
    pub name: String,
    pub rabitq_candidate_digest: String,
    pub scalar_candidate_digest: String,
    pub candidate_parity: bool,
    pub final_matches_exact: bool,
    pub serving_final_parity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProductionVectorRaBitQReferenceEvidence {
    pub required: bool,
    pub compiled: bool,
    pub available: bool,
    pub ready: bool,
    pub implementation: String,
    pub bit_width: usize,
    pub cases: Vec<ProductionVectorRaBitQReferenceCase>,
}

impl ProductionVectorRaBitQReferenceEvidence {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "required": self.required,
            "compiled": self.compiled,
            "available": self.available,
            "ready": self.ready,
            "implementation": self.implementation,
            "role": "native_scalar_reference_for_dispatch_parity",
            "bit_width": self.bit_width,
            "cases": self.cases,
        })
    }
}

pub(super) fn collect_reference_verification(
    config: &ProductionVectorQualificationConfig,
    query_evidence: &[ProductionVectorQueryEvidence],
    bit_width: usize,
) -> Result<ProductionVectorRaBitQReferenceEvidence, ProductionVectorQualificationError> {
    let cases = query_evidence
        .iter()
        .map(|evidence| ProductionVectorRaBitQReferenceCase {
            name: evidence.name.clone(),
            rabitq_candidate_digest: evidence.auto_candidate_digest.clone(),
            scalar_candidate_digest: evidence.scalar_candidate_digest.clone(),
            candidate_parity: evidence.auto_scalar_candidate_parity,
            final_matches_exact: evidence.auto_final_matches_exact,
            serving_final_parity: evidence.serving_auto_final_parity,
        })
        .collect::<Vec<_>>();
    let ready = !cases.is_empty()
        && cases.iter().all(|case| {
            case.candidate_parity && case.final_matches_exact && case.serving_final_parity
        });
    Ok(ProductionVectorRaBitQReferenceEvidence {
        required: config.require_rabitq_reference_verification,
        compiled: true,
        available: true,
        ready,
        implementation: "native_rabitq_scalar_reference".to_string(),
        bit_width,
        cases,
    })
}
