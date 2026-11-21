// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Default, Serialize)]
pub struct LookupWork {
    pub unique_keys: usize,
    pub cursor_starts: usize,
    pub cursor_resolutions: usize,
    pub scanned_records: usize,
    pub point_probes: usize,
}

#[derive(Serialize)]
pub struct BatchRead {
    pub values: Vec<Option<Vec<u8>>>,
    pub work: LookupWork,
}

enum Lookup {
    Ready(Option<Vec<u8>>),
    History,
}

fn decode_head(bytes: &[u8], epoch: u64) -> Result<Lookup> {
    let version = u64::from_be_bytes(
        bytes
            .get(..8)
            .ok_or(Error::Invalid("short batch head"))?
            .try_into()
            .expect("epoch"),
    );
    if version > epoch {
        Ok(Lookup::History)
    } else {
        Ok(Lookup::Ready(decode_value(&bytes[8..])?))
    }
}

fn point(storage: &impl Storage, key: &[u8], epoch: u64, history: bool) -> Result<Lookup> {
    if history {
        return Ok(Lookup::Ready(history_get(storage, key, epoch)?));
    }
    let mut buffer = [0; MAX_VALUE + 9];
    match storage.get(&prefix(key, 2), &mut buffer)? {
        Some(n) => decode_head(&buffer[..n], epoch),
        None => Ok(Lookup::Ready(None)),
    }
}

fn lookup_ordered(
    storage: &impl Storage,
    keys: &[&[u8]],
    epoch: u64,
    history: bool,
    work: &mut LookupWork,
) -> Result<Vec<Lookup>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let namespace = if history { 1 } else { 2 };
    let encode = |key: &[u8]| {
        if history {
            version_key(key, epoch)
        } else {
            prefix(key, 2)
        }
    };
    // Carry the current encoded target across native records/windows. In
    // particular, a matched row followed by the next row must not encode the
    // same long target again just to compare it a second time.
    let mut target = encode(keys[0]);
    let mut out: Vec<_> = (0..keys.len()).map(|_| Lookup::Ready(None)).collect();
    let mut next = 0;
    let mut wasted = 0;
    let mut fallback = false;
    while next < keys.len() && !fallback {
        let seek = encode(keys[next]);
        let window_start = next;
        let mut visited = 0;
        work.cursor_starts += 1;
        {
            // Reuse a cursor for dense keys. Bound each guard by returned
            // records and fall back on sparse or deep-history inputs.
            const WINDOW: NonZeroUsize = NonZeroUsize::new(32).unwrap();
            let mut scan = storage.scan(&seek, ScanLimit::Count(WINDOW))?;
            let mut buffer = [0; MAX_KEY * 2 + MAX_VALUE + 64];
            while let Some((k, v)) = scan.next(&mut buffer)? {
                visited += 1;
                work.scanned_records += 1;
                if work.scanned_records + work.point_probes > crate::adapter::MAX_BATCH_LOOKUPS * 66
                {
                    return Err(Error::Budget("batch lookup work"));
                }
                let physical = &buffer[..k];
                if physical.first() != Some(&namespace) {
                    next = keys.len();
                    break;
                }
                let before = next;
                while next < keys.len() {
                    if physical < &*target {
                        break;
                    }
                    out[next] = if history {
                        if k == target.len() && physical[..k - 8] == target[..target.len() - 8] {
                            // Equality with the generated prefix already proves
                            // its escape grammar; only the fixed epoch remains.
                            let version =
                                !u64::from_be_bytes(physical[k - 8..].try_into().expect("epoch"));
                            if version > epoch {
                                return Err(Error::Invalid("batch snapshot ordering"));
                            }
                            Lookup::Ready(decode_value(&buffer[k..k + v])?)
                        } else {
                            Lookup::Ready(None)
                        }
                    } else if physical == &*target {
                        // Exact equality with our valid encoded head key makes
                        // another full logical-key decode unnecessary.
                        decode_head(&buffer[k..k + v], epoch)?
                    } else {
                        Lookup::Ready(None)
                    };
                    if next > window_start {
                        work.cursor_resolutions += 1;
                    }
                    next += 1;
                    if next < keys.len() {
                        target = encode(keys[next]);
                    }
                }
                if next == keys.len() {
                    break;
                }
                if next == before {
                    wasted += 1;
                    if wasted > next.max(3) {
                        fallback = true;
                        break;
                    }
                }
            }
        }
        if next == window_start && !fallback {
            // A cursor that returned no records reached the end of this
            // namespace; all remaining exact logical lookups are absent.
            if visited == 0 {
                break;
            }
            fallback = true;
        }
    }
    // The native guard above has been dropped before every independent point
    // or historical lookup; no nested native latch acquisition is possible.
    if fallback {
        for position in next..keys.len() {
            work.point_probes += 1;
            out[position] = point(storage, keys[position], epoch, history)?;
        }
    }
    Ok(out)
}

impl<S: Storage> View<S> {
    pub fn get_many(&self, keys: &[&[u8]]) -> Result<BatchRead> {
        let mut order: Vec<_> = (0..keys.len()).collect();
        order.sort_unstable_by(|a, b| keys[*a].cmp(keys[*b]));
        let mut targets = Vec::new();
        let mut groups = Vec::new();
        let mut start = 0;
        while start < order.len() {
            let key = keys[order[start]];
            let end = start + order[start..].partition_point(|position| keys[*position] == key);
            targets.push(key);
            groups.push((start, end));
            start = end;
        }
        let mut work = LookupWork {
            unique_keys: targets.len(),
            ..LookupWork::default()
        };
        let selected = match self {
            Self::Cow(state) => targets
                .iter()
                .map(|key| {
                    work.point_probes += 1;
                    Lookup::Ready(
                        state
                            .get(&Key(key.to_vec()))
                            .map(|value| value.0.as_ref().clone()),
                    )
                })
                .collect(),
            Self::Ordered {
                storage,
                layout,
                epoch,
            } => {
                let mut selected = lookup_ordered(
                    storage.as_ref(),
                    &targets,
                    *epoch,
                    *layout == Layout::Versioned,
                    &mut work,
                )?;
                if *layout == Layout::Hybrid {
                    let unresolved: Vec<_> = selected
                        .iter()
                        .enumerate()
                        .filter_map(|(position, value)| {
                            matches!(value, Lookup::History).then_some(position)
                        })
                        .collect();
                    let history_keys: Vec<_> = unresolved
                        .iter()
                        .map(|position| targets[*position])
                        .collect();
                    let values =
                        lookup_ordered(storage.as_ref(), &history_keys, *epoch, true, &mut work)?;
                    for (position, value) in unresolved.into_iter().zip(values) {
                        selected[position] = value;
                    }
                }
                selected
            }
        };
        let mut values = vec![None; keys.len()];
        for ((start, end), value) in groups.into_iter().zip(selected) {
            let Lookup::Ready(value) = value else {
                return Err(Error::Invalid("unresolved historical batch key"));
            };
            for position in &order[start..end - 1] {
                values[*position] = value.clone();
            }
            values[order[end - 1]] = value;
        }
        Ok(BatchRead { values, work })
    }
}
