// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use shiro_core::{qualify_generated_optimizer_case, FuzzError, OptimizerQualificationVariant};
use shiro_core::{
    OptimizerCostPerturbation, OptimizerQualificationOptions, OptimizerRuleAssertion,
    OptimizerSearchDirective, OptionalOptimizerRule,
};
use std::process::ExitCode;

const USAGE: &str = "Usage: shiro qualify [--seed N] [--case-index N] [--disable-rule NAME] [--require-rule NAME] [--forbid-rule NAME] [--cost-seed N] [--cost-percent 0..25]\nRule NAME is a full transformation:proof_* identifier. Compares baseline memo with direct fallback and the configured memo variant on one snapshot. Emits replay JSON; matching errors fail.";

fn rule(value: &str) -> Result<OptionalOptimizerRule, FuzzError> {
    OptionalOptimizerRule::ALL
        .into_iter()
        .find(|rule| rule.as_str() == value)
        .ok_or_else(|| FuzzError::new(format!("unknown optional rule: {value}")))
}
fn parse(
    args: impl IntoIterator<Item = String>,
) -> Result<(u64, usize, OptimizerQualificationVariant), FuzzError> {
    let mut seed = 7;
    let mut index = 0;
    let mut options = OptimizerQualificationOptions::default();
    let mut assertions = Vec::new();
    let mut cost_seed = None;
    let mut percent = 25;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = args.next().ok_or_else(|| FuzzError::new(USAGE))?;
        match arg.as_str() {
            "--seed" => seed = value.parse().map_err(|_| FuzzError::new("invalid seed"))?,
            "--case-index" => {
                index = value
                    .parse()
                    .map_err(|_| FuzzError::new("invalid case index"))?
            }
            "--disable-rule" => options = options.without_rule(rule(&value)?),
            "--require-rule" => assertions.push(OptimizerRuleAssertion::Required(rule(&value)?)),
            "--forbid-rule" => assertions.push(OptimizerRuleAssertion::Forbidden(rule(&value)?)),
            "--cost-seed" => {
                cost_seed = Some(
                    value
                        .parse()
                        .map_err(|_| FuzzError::new("invalid cost seed"))?,
                )
            }
            "--cost-percent" => {
                percent = value
                    .parse()
                    .map_err(|_| FuzzError::new("invalid cost percent"))?
            }
            _ => return Err(FuzzError::new(USAGE)),
        }
        if assertions.len() > 6 {
            return Err(FuzzError::new("at most six rule assertions"));
        }
    }
    if cost_seed.is_none() && percent != 25 {
        return Err(FuzzError::new("--cost-percent requires --cost-seed"));
    }
    if let Some(seed) = cost_seed {
        options = options.with_cost_perturbation(
            OptimizerCostPerturbation::new(seed, percent)
                .map_err(|error| FuzzError::new(error.to_string()))?,
        );
    }
    Ok((
        seed,
        index,
        OptimizerQualificationVariant {
            options,
            assertions,
            search: OptimizerSearchDirective::Memo,
        },
    ))
}
pub fn run(args: impl IntoIterator<Item = String>) -> ExitCode {
    let args = args.into_iter().collect::<Vec<_>>();
    if args == ["--help"] {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let result = parse(args).and_then(|(seed, index, variant)| {
        let direct = OptimizerQualificationVariant {
            options: OptimizerQualificationOptions::default(),
            search: OptimizerSearchDirective::DirectFallback,
            assertions: Vec::new(),
        };
        qualify_generated_optimizer_case(seed, index, &[direct, variant])
    });
    match result {
        Ok(report) => {
            println!("{}", report.json());
            if report.qualification.success() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Err(error) => {
            eprintln!("shiro qualify: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn controls_are_known_bounded_and_replayable() {
        let args = [
            "--disable-rule",
            "transformation:proof_redundant_distinct",
            "--cost-seed",
            "42",
            "--cost-percent",
            "25",
        ];
        let (_, _, variant) = parse(args.map(str::to_owned)).unwrap();
        assert!(!variant
            .options
            .rule_enabled(OptionalOptimizerRule::RedundantDistinct));
        assert_eq!(variant.options.cost_perturbation().unwrap().seed(), 42);
        for args in [
            vec!["--disable-rule", "legality"],
            vec!["--cost-seed", "2", "--cost-percent", "26"],
            vec!["--cost-percent", "1"],
        ] {
            assert!(parse(args.into_iter().map(str::to_owned)).is_err());
        }
        let args =
            (0..7).flat_map(|_| ["--require-rule", "transformation:proof_redundant_distinct"]);
        assert!(parse(args.map(str::to_owned)).is_err());
    }
}
