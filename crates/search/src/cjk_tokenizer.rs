use jieba_rs::Jieba;
use std::sync::LazyLock;

pub(super) const ANALYZER_FORMAT_VERSION: &[u8] = b"skein-search-analyzer-v2-jieba-search";

static CHINESE_TOKENIZER: LazyLock<Jieba> = LazyLock::new(Jieba::new);

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

pub(super) fn is_cjk_search_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x3040..=0x309F
            | 0x30A0..=0x30FF
            | 0xAC00..=0xD7AF
    )
}

fn is_han_search_char(ch: char) -> bool {
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
