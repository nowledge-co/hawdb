use super::*;

struct Random(u64);

impl Random {
    fn below(&mut self, bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as usize
    }
}

fn reference_compaction(
    mut inputs: Vec<Vec<Posting>>,
    fan_in: usize,
) -> (Vec<Vec<Posting>>, Vec<Vec<Posting>>) {
    let mut outputs = Vec::new();
    while inputs.len() > fan_in {
        let next = inputs
            .chunks(fan_in)
            .map(|group| {
                group
                    .iter()
                    .flatten()
                    .cloned()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        outputs.extend(next.iter().cloned());
        inputs = next;
    }
    (outputs, inputs)
}

fn run_case(seed: u64, case: usize) {
    let mut random = Random(seed);
    let fan_in = 2 + random.below(5);
    let input_count = fan_in + 1 + random.below(11);
    let terms = ["", "alpha", "beta", "\u{4e2d}\u{6587}", "\u{1f4da}"];
    let inputs = (0..input_count)
        .map(|_| {
            let mut postings = Vec::new();
            for _ in 0..random.below(15) {
                let posting = Posting {
                    term: terms[random.below(terms.len())].into(),
                    document_id: format!("document-{}", random.below(5)),
                    term_frequency: 1 + random.below(7) as u32,
                    document_len: 24 + random.below(3) as u32,
                };
                // Only identical postings are deduplicated by this spill
                // format; different frequencies are not partial-tf reduction.
                if random.below(2) == 0 {
                    postings.push(posting.clone());
                }
                postings.push(posting);
            }
            postings
        })
        .collect::<Vec<_>>();
    let (outputs, final_runs) = reference_compaction(inputs.clone(), fan_in);
    let output_bytes = outputs
        .iter()
        .map(|run| reference_run(run.clone()).len())
        .sum::<usize>();
    let fixture = Fixture::new();
    let mut io = ObservedIo::default();
    let mut runs = SpillRuns::new(
        &fixture.0,
        1,
        LexicalProjectionConfig {
            max_merge_fan_in: NonZeroUsize::new(fan_in).unwrap(),
            ..Default::default()
        },
    );
    let mut initial_bytes = 0;
    for input in &inputs {
        let wire = reference_run(input.clone());
        initial_bytes += wire.len();
        runs.spill_with_io(&mut input.clone(), &mut io).unwrap();
        assert_eq!(fs::read(runs.paths.last().unwrap()).unwrap(), wire);
    }
    assert_eq!(io.written.get(), initial_bytes);
    assert_eq!(runs.bytes, initial_bytes as u64);
    let remaining = match case % 8 {
        0 => output_bytes,
        1 => output_bytes - 1,
        2 => random.below(output_bytes + 1),
        _ => output_bytes,
    };
    let allowed_runs = if case % 8 == 3 {
        random.below(outputs.len() + 1)
    } else {
        outputs.len()
    };
    runs.config.max_spill_bytes = NonZeroU64::new((initial_bytes + remaining) as u64).unwrap();
    runs.config.max_spill_runs = NonZeroUsize::new(input_count + allowed_runs).unwrap();
    io.fault = match case % 8 {
        4 => Fault::CreateAfterFile,
        5 => Fault::WriteAfter(random.below(8)),
        6 => Fault::Flush,
        7 => Fault::RemoveAfter(random.below(fan_in + 1)),
        _ => Fault::None,
    };
    let result = runs.compact_with_io(&mut io);
    if case % 8 >= 4 {
        assert!(result.is_err());
        assert!(io.fired.get(), "fault did not fire: {:?}", io.fault);
        assert!(io.written.get() - initial_bytes <= remaining);
    } else {
        let mut written = 0;
        let mut committed = 0;
        let mut created = 0;
        let mut completed = 0;
        for output in outputs.iter().take(allowed_runs) {
            let units = reference_units(output);
            let bytes = admitted_prefix(units.iter().copied(), remaining - written);
            written += bytes;
            created += usize::from(bytes >= 8);
            if bytes != units.iter().sum::<usize>() {
                break;
            }
            completed += 1;
            committed += bytes;
        }
        assert_eq!(result.is_ok(), completed == outputs.len());
        assert_eq!(io.written.get() - initial_bytes, written);
        assert_eq!(io.created - input_count, created);
        assert_eq!(runs.bytes, (initial_bytes + committed) as u64);
        if let Err(error) = result {
            assert!(error.to_string().contains("spill"));
        } else {
            let actual = runs
                .paths
                .iter()
                .map(|path| fs::read(path).unwrap())
                .collect::<Vec<_>>();
            let expected = final_runs
                .into_iter()
                .map(reference_run)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
        }
    }
    drop(runs);
    fixture.assert_empty();
}

#[test]
#[ignore = "manual seeded spill admission and fault differential campaign"]
fn lexical_spill_admission_differential_campaign() {
    for case in 0..512 {
        let seed = 0x3925_7069_6c6c_u64.wrapping_add(case as u64 * 104729);
        let outcome = std::panic::catch_unwind(|| run_case(seed, case));
        assert!(
            outcome.is_ok(),
            "spill differential failure: seed={seed}, case={case}"
        );
    }
}
