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

use cfgrammar::yacc::YaccKind;
use lrlex::CTLexerBuilder;

fn main() {
    CTLexerBuilder::new()
        .lrpar_config(|parser| {
            parser
                .yacckind(YaccKind::Grmtools)
                .grammar_in_src_dir("exact_lookup.y")
                .expect("exact lookup Yacc grammar must compile")
        })
        .lexer_in_src_dir("exact_lookup.l")
        .expect("exact lookup lexer must compile")
        .build()
        .expect("exact lookup parser must compile");
}
