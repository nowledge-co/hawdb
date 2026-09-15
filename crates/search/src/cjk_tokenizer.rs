use jieba_rs::Jieba;
use std::sync::LazyLock;

pub(super) const ANALYZER_FORMAT_VERSION: &[u8] =
    b"skein-search-analyzer-v2-jieba-search-han-ngrams";

static CHINESE_TOKENIZER: LazyLock<Jieba> = LazyLock::new(Jieba::new);

#[cfg(test)]
pub(super) fn chinese_search_tokens(text: &str) -> Vec<String> {
    if !text.chars().any(is_han_search_char) {
        return Vec::new();
    }
    CHINESE_TOKENIZER
        .cut_for_search(text, true)
        .into_iter()
        .filter(|token| token.word.chars().any(is_han_search_char))
        .map(|token| token.word.to_string())
        .collect()
}

pub(super) fn visit_chinese_search_tokens_with_workspace<'a>(
    text: &'a str,
    workspace: Option<&crate::analyzer_workspace::Workspace>,
    mut emit: impl FnMut(&'a str) -> super::Result<()>,
) -> super::Result<()> {
    if !text.chars().any(is_han_search_char) {
        return Ok(());
    }
    let _scratch = workspace
        .map(|workspace| workspace.admit(text))
        .transpose()?;
    let tokens = CHINESE_TOKENIZER.cut_for_search(text, true);
    for (index, token) in tokens.into_iter().enumerate() {
        if index.is_multiple_of(1024)
            && let Some(workspace) = workspace
        {
            workspace.checkpoint()?;
        }
        if token.word.chars().any(is_han_search_char) {
            emit(token.word)?;
        }
    }
    if let Some(workspace) = workspace {
        workspace.checkpoint()?;
    }
    Ok(())
}

pub(super) fn prime_workspace(text: &str) -> super::Result<()> {
    // An unknown two-Han word forces HMM and its skip regex to initialize on
    // this worker. The private dictionary is immutable; fail closed if a future
    // dictionary invalidates the dependency qualification fixture.
    if CHINESE_TOKENIZER.has_word(text) {
        return Err(super::SkeinError::Execution(
            "search analyzer dictionary invalidated workspace initialization".into(),
        ));
    }
    drop(CHINESE_TOKENIZER.cut_for_search(text, true));
    Ok(())
}

pub(super) fn is_cjk_search_char(ch: char) -> bool {
    is_han_search_char(ch)
        || matches!(
            ch as u32,
            0x3040..=0x309F
                | 0x30A0..=0x30FF
                | 0xAC00..=0xD7AF
        )
}

pub(super) fn is_han_search_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x20000..=0x2A6DF
            | 0x2A700..=0x2B73F
            | 0x2B740..=0x2B81F
            | 0x2B820..=0x2CEAF
            | 0x2CEB0..=0x2EBEF
            | 0x2F800..=0x2FA1F
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tokenized_han_character_also_receives_ngrams() {
        for ch in (0..=0x10FFFF).filter_map(char::from_u32) {
            if is_han_search_char(ch) {
                assert!(is_cjk_search_char(ch), "missing n-grams for {ch:?}");
            }
        }
    }

    #[test]
    fn supplementary_han_runs_produce_cross_character_ngrams() {
        let mut tokens = crate::TokenSequence::default();
        crate::push_cjk_ngram_tokens(
            &mut tokens,
            "\u{20000}\u{20001}\u{20002}",
            &crate::SearchAnalyzerLexicon::default(),
        );
        let tokens = tokens.into_vec();
        for expected in ["\u{20000}\u{20001}", "\u{20001}\u{20002}"] {
            assert!(tokens.iter().any(|token| token == expected), "{tokens:?}");
        }
        assert!(!is_cjk_search_char('a'));
        assert!(!is_cjk_search_char('\u{1F600}'));
        for ch in ['\u{3042}', '\u{30A2}', '\u{AC00}'] {
            assert!(is_cjk_search_char(ch));
        }
    }
}
