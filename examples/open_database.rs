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

//! README quickstart. Keep the statements aligned with `README.md`.
//!
//! ```console
//! cargo run --example open_database
//! ```

use hawdb::{Database, Value};
use std::collections::BTreeMap;

fn main() -> hawdb::Result<()> {
    let mut db = Database::open("./knowledge")?;

    let mut note = BTreeMap::new();
    note.insert("title".into(), Value::String("Graph foundations".into()));

    let mut tx = db.begin_transaction()?;
    tx.query_with_params(
        "CREATE (:Note {title: $title})-[:MENTIONS]->(:Entity {name: 'context layer'})",
        &note,
    )?;
    tx.commit()?;

    let found = db.query(
        "MATCH (note:Note)-[:MENTIONS]->(entity:Entity)
         RETURN note.title AS title, entity.name AS name
         ORDER BY title",
    )?;
    println!("{:?}", found.rows.get(0, "title"));
    Ok(())
}
