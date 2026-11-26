// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! The exact pinned Jieba dictionary, loaded through HawDB's patched Zstd.

use jieba_rs::Jieba;
use std::io::BufReader;

const EMBEDDED_DICTIONARY: &[u8] = include_bytes!("data/jieba-0.11.0.dict.txt.zst");

pub(crate) fn load() -> std::io::Result<Jieba> {
    // Stream directly into Jieba; retaining the decoded source would keep a
    // second copy alongside its immutable dictionary structures.
    let decoder = hawdb_storage::compression::Decoder::new(EMBEDDED_DICTIONARY)?;
    Jieba::with_dict(&mut BufReader::new(decoder)).map_err(std::io::Error::other)
}

#[cfg(test)]
#[path = "cjk_dictionary/tests.rs"]
mod tests;
