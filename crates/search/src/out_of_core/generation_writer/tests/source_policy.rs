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

use super::*;
use crate::SearchLexicalSourcePolicy;

fn policy(bytes: u64) -> SearchLexicalSourcePolicy {
    SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes).unwrap()).unwrap()
}

#[test]
fn source_policy_has_checked_bounds_and_unchanged_default() {
    assert_eq!(
        SearchLexicalSourcePolicy::default(),
        policy(4 * 1024 * 1024)
    );
    assert_eq!(policy(1).max_document_source_bytes().get(), 1);
    assert_eq!(policy(1).max_document_tokens().get(), 1_000_000);
    for bytes in [isize::MAX as u64 + 1, u64::MAX] {
        assert!(SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes).unwrap()).is_err());
    }
}
