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

//! One text value grammar for ordinary and admitted predicate decoding.

use hawdb_core::{HawDBError, Result, Value};
use std::collections::BTreeMap;
use std::ops::Deref;

pub(crate) trait Decoder {
    type Items<'a>;
    type TemporaryText: Deref<Target = str>;
    type MapMemory: Default;
    fn visit(&self) -> Result<()>;
    fn integer(&self, input: &str) -> Result<i64>;
    fn unsigned(&self, input: &str) -> Result<u64>;
    fn string(&self, input: &str) -> Result<String>;
    fn binary(&self, input: &str) -> Result<Vec<u8>>;
    fn temporary(&self, input: &str) -> Result<Self::TemporaryText>;
    fn items<'a>(&self, input: &'a str, delimiter: u8) -> Self::Items<'a>;
    fn next<'a>(&self, items: &mut Self::Items<'a>) -> Result<Option<&'a str>>;
    fn equal(&self, input: &str) -> Result<Option<usize>>;
    fn push(&self, values: &mut Vec<Value>, value: Value) -> Result<()>;
    fn insert(
        &self,
        values: &mut BTreeMap<String, Value>,
        memory: &mut Self::MapMemory,
        key: String,
        value: Value,
    ) -> Result<()>;
}

pub(crate) fn decode<D: Decoder>(input: &str, backend: &D) -> Result<Value> {
    backend.visit()?;
    if input.is_empty() {
        return Err(HawDBError::Storage("empty encoded value".into()));
    }
    let (kind, rest) = input
        .split_at_checked(1)
        .ok_or_else(|| HawDBError::Storage("invalid encoded value tag".into()))?;
    match kind {
        "n" if rest.is_empty() => Ok(Value::Null),
        "b" => match rest {
            "0" => Ok(Value::Bool(false)),
            "1" => Ok(Value::Bool(true)),
            _ => Err(HawDBError::Storage(format!("invalid bool value: {input}"))),
        },
        "i" => backend.integer(rest).map(Value::Int),
        "f" => backend.unsigned(rest).map(f64::from_bits).map(Value::Float),
        "s" => backend.string(rest).map(Value::String),
        "u" => hawdb_core::Uuid::parse_str(rest)
            .map(Value::Uuid)
            .map_err(|error| HawDBError::Storage(format!("invalid UUID value: {error}"))),
        "x" => backend.binary(rest).map(Value::Binary),
        "l" => {
            let mut values = Vec::new();
            if !rest.is_empty() {
                let mut items = backend.items(rest, b',');
                while let Some(item) = backend.next(&mut items)? {
                    let text = backend.temporary(item)?;
                    let value = decode(&text, backend)?;
                    drop(text);
                    backend.push(&mut values, value)?;
                }
            }
            Ok(Value::List(values))
        }
        "m" => {
            let mut values = BTreeMap::new();
            let mut memory = D::MapMemory::default();
            if !rest.is_empty() {
                let mut items = backend.items(rest, b';');
                while let Some(item) = backend.next(&mut items)? {
                    let equal = backend.equal(item)?.ok_or_else(|| {
                        HawDBError::Storage(format!("invalid encoded map item: {item}"))
                    })?;
                    let key = backend.string(&item[..equal])?;
                    let text = backend.temporary(&item[equal + 1..])?;
                    let value = decode(&text, backend)?;
                    drop(text);
                    backend.insert(&mut values, &mut memory, key, value)?;
                }
            }
            Ok(Value::Map(values))
        }
        _ => Err(HawDBError::Storage(format!(
            "invalid encoded value tag or payload: {kind:?}"
        ))),
    }
}
