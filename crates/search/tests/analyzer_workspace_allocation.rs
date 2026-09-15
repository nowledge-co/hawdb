//! Qualify the pinned opaque dependency against the production capacity model.

#[path = "support/live_allocation.rs"]
mod allocation;
#[path = "../src/analyzer_workspace/bounds.rs"]
mod bounds;

use jieba_rs::Jieba;

#[test]
fn opaque_workspace_capacity_covers_construction_growth_and_tls_destruction() {
    // Dictionary residency is a shared process owner, outside the operation.
    let jieba = Jieba::new();
    let (regex, peak) = allocation::measure(|| regex::Regex::new(bounds::REGEX_PATTERN).unwrap());
    assert!(peak <= bounds::regex_retained().unwrap() + bounds::regex_construction().unwrap());
    assert!(allocation::live() <= bounds::regex_retained().unwrap());
    drop(regex);
    assert_eq!(allocation::live(), 0);

    for (case, unit) in [
        ("rare-han", "\u{9f98}\u{9750}\u{9f49}"),
        (
            "dictionary",
            "\u{4e2d}\u{534e}\u{4eba}\u{6c11}\u{5171}\u{548c}\u{56fd}",
        ),
        ("mixed", "a_\u{660}\u{104a0}%\u{9f98}\u{9f49}"),
        ("supplementary", "\u{20000}\u{20001}"),
    ] {
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let warmup = "\u{9f98}\u{9750}";
                assert!(!jieba.has_word(warmup));
                let (_, peak) = allocation::measure(|| drop(jieba.cut_for_search(warmup, true)));
                assert!(peak <= bounds::regex_construction().unwrap()
                    + bounds::regex_retained().unwrap()
                    + bounds::hmm_retained(2).unwrap()
                    + bounds::invocation(warmup.len(), 2).unwrap());
                let mut high_water = 2;
                // Short-after-long and subsequent growth exercise retained TLS
                // capacity and coexistence of old/replacement allocations.
                for repeat in [1, 16, 1024, 1, 8192, 16] {
                    let input = unit.repeat(repeat);
                    let characters = input.chars().count();
                    high_water = high_water.max(characters);
                    let retained = bounds::regex_retained().unwrap()
                        + bounds::hmm_retained(high_water).unwrap();
                    let allowed = retained + bounds::invocation(input.len(), characters).unwrap();
                    let (tokens, peak) = allocation::measure(|| {
                        let tokens = jieba.cut_for_search(&input, true);
                        assert!(tokens.len() <= 2 * characters);
                        let count = tokens.len();
                        drop(tokens);
                        count
                    });
                    assert!(peak <= allowed, "{case}: {peak} exceeds {allowed}");
                    assert!(allocation::live() <= retained, "{case}: retained TLS capacity");
                    println!(
                        "case={case} bytes={} tokens={tokens} peak={peak} admitted={allowed} retained={}",
                        input.len(), allocation::live(),
                    );
                }
            }).join().unwrap();
        });
        assert_eq!(
            allocation::live(),
            0,
            "{case}: native join must destroy TLS"
        );
    }
}
