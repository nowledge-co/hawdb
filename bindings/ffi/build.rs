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

use std::path::Path;

fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let header = Path::new(&crate_dir).join("include/hawdb_ffi.h");
    cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(
            cbindgen::Config::from_file(Path::new(&crate_dir).join("cbindgen.toml"))
                .expect("cbindgen.toml should parse"),
        )
        .generate()
        .expect("cbindgen should generate the header")
        .write_to_file(header);
}
