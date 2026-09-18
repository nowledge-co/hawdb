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

use super::{GraphStore, RelationalIndexReadLimits, RelationalTransactionIndexView};
use std::sync::Arc;

impl GraphStore {
    pub(crate) fn begin_authoritative_relational_transaction_index(
        &self,
    ) -> crate::Result<Option<RelationalTransactionIndexView>> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(None);
        }
        self.validate_authoritative_relational_index_open()?;
        let view = Arc::clone(
            self.relational_index_shadow
                .current_read_view(self.commit_epoch)
                .expect("validated authoritative view must remain current"),
        );
        Ok(Some(RelationalTransactionIndexView::new(
            view,
            self.relational_index_shadow.live_limits,
            RelationalIndexReadLimits::default(),
        )))
    }
}
