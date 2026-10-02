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

#[cfg(test)]
mod tests {
    use hawdb::{
        IoConcurrencyBudget, ProcessMemoryCapabilities, ProcessMemoryPolicy,
        ProcessMemoryPolicyConfig, ProcessMemorySnapshot, RuntimeAdmissionCode, RuntimeGovernor,
        RuntimeGovernorConfig, RuntimeMemorySnapshot, RuntimePermit, RuntimeResourceBudget,
        RuntimeResourceSnapshot, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest,
    };
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::time::Duration;

    const LIMIT: u64 = 256;
    const RECOVERY: u64 = 32;

    struct Claim {
        governor: usize,
        original: u64,
        unseen: u64,
        permit: Option<RuntimePermit>,
    }

    #[derive(Default)]
    struct Model {
        rss: Option<u64>,
        paused: bool,
        claims: Vec<Claim>,
    }

    impl Model {
        fn unseen(&self) -> u64 {
            self.claims.iter().map(|claim| claim.unseen).sum()
        }

        fn sample(&mut self, rss: u64) {
            // Each live claim occupies an interval in the cumulative unseen debt.
            // Positive RSS growth covers the prefix of those intervals, independently
            // of the production reservation map and its mutable consumption loop.
            let covered = rss.saturating_sub(self.rss.unwrap_or(rss));
            let mut interval_start = 0;
            for claim in &mut self.claims {
                let interval_end = interval_start + claim.unseen;
                claim.unseen = interval_end.saturating_sub(covered.max(interval_start));
                interval_start = interval_end;
            }
            self.rss = Some(rss);
            let occupied = rss + self.unseen();
            self.paused = occupied >= LIMIT || (self.paused && occupied + RECOVERY > LIMIT);
        }

        fn admission(&self, bytes: u64) -> Result<(), (RuntimeAdmissionCode, bool)> {
            if bytes > LIMIT {
                Err((RuntimeAdmissionCode::MemorySaturated, false))
            } else if let Some(rss) = self.rss {
                if self.paused || rss + self.unseen() + bytes > LIMIT {
                    Err((RuntimeAdmissionCode::MemorySaturated, true))
                } else {
                    Ok(())
                }
            } else {
                Err((RuntimeAdmissionCode::MemoryPressure, true))
            }
        }
    }

    fn resources(cpu: usize) -> RuntimeResourceSnapshot {
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(cpu).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(16_384), Some(16_384), None, None, None),
        )
    }

    fn sample(rss: u64, supported: bool) -> ProcessMemorySnapshot {
        ProcessMemorySnapshot {
            capabilities: ProcessMemoryCapabilities {
                resident_memory: supported,
                ..ProcessMemoryCapabilities::default()
            },
            resident_bytes: rss,
            // Admission must ignore the lifetime peak, including a peak above the limit.
            peak_resident_bytes: 2 * LIMIT,
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
        }
    }

    fn next(random: &mut u64) -> u64 {
        *random ^= *random << 13;
        *random ^= *random >> 7;
        *random ^= *random << 17;
        *random
    }

    #[test]
    fn process_memory_state_machine_matches_interval_oracle() {
        let mut actions = [0_usize; 7];
        let mut admitted = 0;
        let mut pressure_rejections = 0;
        let mut capacity_rejections = 0;
        let mut recoveries = 0;
        for seed in 1..=32 {
            let policy = ProcessMemoryPolicy::new(
                ProcessMemoryPolicyConfig::new(NonZeroU64::new(LIMIT).unwrap())
                    .with_recovery_headroom(RECOVERY)
                    .with_sample_max_age(Duration::from_secs(60)),
            );
            let governors = std::array::from_fn::<_, 2, _>(|_| {
                let governor = RuntimeGovernor::new_with_process_memory_policy(
                    RuntimeGovernorConfig {
                        memory_budget_bytes: Some(4096),
                        ..RuntimeGovernorConfig::shared_host()
                    },
                    resources(64),
                    IoConcurrencyBudget::new(1, 1),
                    policy.clone(),
                );
                governor.pin_resources();
                governor
            });
            let mut model = Model {
                paused: true,
                ..Model::default()
            };
            let mut random = seed;
            for step in 0..512 {
                let mut action = usize::try_from(next(&mut random) % 7).unwrap();
                if model
                    .claims
                    .iter()
                    .filter(|claim| claim.permit.is_some())
                    .count()
                    >= 16
                {
                    action = 3;
                }
                actions[action] += 1;
                match action {
                    0 => {
                        let rss = next(&mut random) % (LIMIT + LIMIT / 2 + 1);
                        let was_paused = model.paused;
                        model.sample(rss);
                        policy.update(sample(rss, true));
                        recoveries += usize::from(was_paused && !model.paused);
                    }
                    1 | 2 => {
                        let governor = action - 1;
                        let bytes = next(&mut random) % (2 * LIMIT + 1);
                        let expected = model.admission(bytes);
                        let actual = governors[governor].try_admit(
                            RuntimeWorkRequest::new(
                                RuntimeWorkPriority::Foreground,
                                RuntimeWorkKind::Control,
                            )
                            .with_memory_bytes(bytes),
                        );
                        match (expected, actual) {
                            (Ok(()), Ok(permit)) => {
                                admitted += 1;
                                model.claims.push(Claim { governor, original: bytes, unseen: bytes, permit: Some(permit) });
                            }
                            (Err((code, retryable)), Err(error)) => {
                                assert_eq!((error.code, error.is_retryable()), (code, retryable), "seed={seed} step={step}");
                                pressure_rejections += usize::from(retryable);
                                capacity_rejections += usize::from(!retryable);
                            }
                            (expected, actual) => panic!("admission mismatch seed={seed} step={step} bytes={bytes}: expected={expected:?}, actual={actual:?}"),
                        }
                    }
                    3 => {
                        if !model.claims.is_empty() {
                            let index =
                                usize::try_from(next(&mut random) % model.claims.len() as u64)
                                    .unwrap();
                            model.claims[index].unseen = 0;
                            drop(model.claims[index].permit.take());
                        }
                    }
                    4 | 5 => {
                        model.rss = None;
                        model.paused = true;
                        if action == 4 {
                            policy.clear_sample();
                        } else {
                            policy.update(sample(0, false));
                        }
                    }
                    6 => {
                        for governor in &governors {
                            let cpu = if next(&mut random).is_multiple_of(2) {
                                64
                            } else {
                                128
                            };
                            governor.update_resources(resources(cpu));
                            assert!(!governor.refresh_from_host());
                            assert_eq!(governor.snapshot().resources, resources(cpu));
                        }
                    }
                    _ => unreachable!(),
                }
                let snapshot = policy.snapshot();
                assert_eq!(
                    snapshot.sampled_resident_bytes, model.rss,
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    snapshot.unobserved_reserved_bytes,
                    model.unseen(),
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    snapshot.available_bytes,
                    model
                        .rss
                        .map(|rss| LIMIT.saturating_sub(rss + model.unseen())),
                    "seed={seed} step={step}"
                );
                assert_eq!(
                    snapshot.admission_paused, model.paused,
                    "seed={seed} step={step}"
                );
                for (index, governor) in governors.iter().enumerate() {
                    let live: Vec<_> = model
                        .claims
                        .iter()
                        .filter(|claim| claim.governor == index && claim.permit.is_some())
                        .collect();
                    let snapshot = governor.snapshot();
                    assert_eq!(
                        snapshot.active_foreground_tasks,
                        live.len(),
                        "seed={seed} step={step}"
                    );
                    assert_eq!(
                        snapshot.admitted_memory_bytes,
                        live.iter().map(|claim| claim.original).sum::<u64>(),
                        "seed={seed} step={step}"
                    );
                    assert!(snapshot.resources_pinned);
                }
            }
            drop(model);
            assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
            for governor in governors {
                assert_eq!(
                    governor.snapshot().admissions,
                    governor.snapshot().completions
                );
            }
        }
        assert!(actions.iter().all(|count| *count > 0));
        assert!(
            admitted > 0 && pressure_rejections > 0 && capacity_rejections > 0 && recoveries > 0
        );
        println!("hawdb-process-memory-oracle-v1 seeds=32 steps_per_seed=512 admitted={admitted} pressure_rejections={pressure_rejections} capacity_rejections={capacity_rejections} recoveries={recoveries} actions={actions:?}");
    }
}
