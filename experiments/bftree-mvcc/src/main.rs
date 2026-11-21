// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

mod adapter;
mod benchmark;
mod index;
#[cfg(test)]
#[path = "../simd/key_compare.rs"]
mod key_compare;
mod performance;
mod storage;
#[cfg(test)]
#[path = "../structural/keys.rs"]
mod structural_keys;

use adapter::Store;
use std::path::Path;

fn demo(path: &Path) -> adapter::Result<()> {
    let store = Store::open(path)?;
    let mut seed = store.begin_write()?;
    seed.put(b"left", b"old")?;
    seed.put(b"right", b"old")?;
    seed.commit(1)?;
    let old = store.begin_read()?;
    let mut update = store.begin_write()?;
    update.put(b"left", b"new")?;
    update.put(b"right", b"new")?;
    assert_eq!(update.get(b"left")?, Some(b"new".to_vec()));
    update.commit(2)?;
    assert_eq!(old.get(b"left")?, Some(b"old".to_vec()));
    assert_eq!(old.get(b"right")?, Some(b"old".to_vec()));
    let mut discarded = store.begin_write()?;
    discarded.delete(b"left")?;
    discarded.rollback();
    assert_eq!(store.begin_read()?.get(b"left")?, Some(b"new".to_vec()));
    println!("PASS | atomic pair, private writes, old snapshot, rollback");
    drop(old);
    drop(store);
    let reopened = Store::open(path)?;
    let rows = reopened.begin_read()?.scan(b"left", b"z")?;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|(_, value)| value == b"new"));
    println!(
        "PASS | complete journal replay | epoch {}",
        reopened.begin_read()?.epoch()
    );
    Ok(())
}

fn maintenance_demo(path: &Path) -> adapter::Result<()> {
    let store = Store::open(path)?;
    let mut seed = store.begin_write()?;
    seed.put(b"key", b"old")?;
    seed.commit(1)?;
    let old = store.begin_read()?;
    let mut update = store.begin_write()?;
    update.put(b"key", b"new")?;
    update.commit(2)?;
    store.collect_garbage(adapter::MAX_GC_WORK)?;
    store.checkpoint()?;
    assert_eq!(old.get(b"key")?, Some(b"old".to_vec()));
    drop(old);
    while !store.collect_garbage(4)?.complete_pass {}
    store.checkpoint()?;
    let mut retry = store.begin_write()?;
    retry.put(b"key", b"old")?;
    assert_eq!(retry.commit(1)?, 1);
    drop(store);
    let reopened = Store::open(path)?;
    assert_eq!(reopened.begin_read()?.get(b"key")?, Some(b"new".to_vec()));
    let batch = reopened
        .begin_read()?
        .get_many(&[b"key", b"missing", b"key"])?;
    assert_eq!(
        batch.values,
        vec![Some(b"new".to_vec()), None, Some(b"new".to_vec())]
    );
    println!(
        "{}",
        serde_json::to_string(&reopened.storage_stats()?)
            .map_err(|e| adapter::Error::Index(e.to_string()))?
    );
    Ok(())
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    let result = match args.get(1).and_then(|a| a.to_str()) {
        Some("audit") if args.len() == 4 => args[3]
            .to_str()
            .ok_or(adapter::Error::Invalid("invalid audit layout"))
            .and_then(index::Layout::parse)
            .and_then(|layout| {
                adapter::audit::transaction_stream(
                    Path::new(&args[2]),
                    layout,
                    std::io::stdout().lock(),
                )
            }),
        Some("bench-advanced") if args.len() == 8 => {
            let parsed = args[3]
                .to_str()
                .and_then(|s| index::Layout::parse(s).ok())
                .zip(args[4].to_str().and_then(|s| s.parse().ok()))
                .zip(args[5].to_str().and_then(|s| s.parse().ok()))
                .zip(
                    args[6]
                        .to_str()
                        .and_then(|s| benchmark::KeyShape::parse(s).ok()),
                );
            match (parsed, args[7].to_str()) {
                (Some((((layout, keys), rounds), shape)), Some(mode @ ("keep" | "collect"))) => {
                    performance::advanced(
                        Path::new(&args[2]),
                        layout,
                        keys,
                        rounds,
                        shape,
                        mode == "collect",
                    )
                }
                _ => Err(adapter::Error::Invalid(
                    "invalid advanced benchmark arguments",
                )),
            }
        }
        Some(command @ ("demo" | "maintenance-demo")) if args.len() == 3 => {
            let path = Path::new(&args[2]);
            if path.exists() {
                Err(adapter::Error::Invalid("demo requires a new journal path"))
            } else {
                if command == "demo" {
                    demo(path)
                } else {
                    maintenance_demo(path)
                }
            }
        }
        Some("bench") if (3..=5).contains(&args.len()) => {
            let parse = |i: usize, default: usize| -> adapter::Result<usize> {
                args.get(i).map_or(Ok(default), |n| {
                    n.to_str()
                        .and_then(|n| n.parse().ok())
                        .ok_or(adapter::Error::Invalid("invalid benchmark size"))
                })
            };
            parse(3, 512).and_then(|keys| {
                parse(4, 24)
                    .and_then(|rounds| benchmark::compare(Path::new(&args[2]), keys, rounds))
            })
        }
        Some(command @ ("bench-one" | "bench-shaped"))
            if args.len() == if command == "bench-one" { 6 } else { 7 } =>
        {
            let parsed = args[3]
                .to_str()
                .and_then(|s| index::Layout::parse(s).ok())
                .zip(args[4].to_str().and_then(|s| s.parse().ok()))
                .zip(args[5].to_str().and_then(|s| s.parse().ok()));
            match parsed {
                Some(((layout, keys), rounds)) => {
                    if command == "bench-one" {
                        benchmark::one(Path::new(&args[2]), layout, keys, rounds)
                    } else {
                        args[6]
                            .to_str()
                            .ok_or(adapter::Error::Invalid("invalid key shape"))
                            .and_then(benchmark::KeyShape::parse)
                            .and_then(|shape| {
                                benchmark::one_shaped(
                                    Path::new(&args[2]),
                                    layout,
                                    keys,
                                    rounds,
                                    shape,
                                )
                            })
                    }
                }
                None => Err(adapter::Error::Invalid("invalid benchmark arguments")),
            }
        }
        _ => {
            eprintln!("Usage: hawdb-bftree-mvcc-experiment <demo|maintenance-demo> <new-journal> | bench <new-directory> [keys] [rounds] | bench-one <new-journal> <versioned|hybrid|cow> <keys> <rounds> | bench-shaped <new-journal> <layout> <keys> <rounds> <numeric8|prefix64|spread64> | bench-advanced <new-journal> <layout> <keys> <rounds> <shape> <keep|collect> | audit <journal> <layout> (binary normalized transaction stream on stdout)");
            std::process::exit(2);
        }
    };
    if let Err(error) = result {
        eprintln!("FAIL | {error}");
        std::process::exit(1);
    }
}
