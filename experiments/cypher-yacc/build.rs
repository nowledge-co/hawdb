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
