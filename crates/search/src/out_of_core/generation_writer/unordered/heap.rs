// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

/// A resumable min-heap sort. Descending storage lets the run writer pop IDs
/// in ascending order without moving the remaining buffer on every record.
pub(super) struct Heap {
    start: usize,
    end: usize,
    root: Option<usize>,
    child: Option<usize>,
}
impl Heap {
    pub(super) fn new(length: usize) -> Self {
        Self {
            start: length / 2,
            end: length,
            root: None,
            child: None,
        }
    }
    pub(super) fn advance(
        &mut self,
        values: &mut [Buffered],
        control: &mut Control,
    ) -> Result<bool> {
        loop {
            let Some(root) = self.root else {
                if !control.allow(1, 0, 0)? {
                    return Ok(false);
                }
                if self.start > 0 {
                    self.start -= 1;
                    self.root = Some(self.start);
                } else if self.end > 1 {
                    self.end -= 1;
                    values.swap(0, self.end);
                    self.root = Some(0);
                } else {
                    return Ok(true);
                }
                continue;
            };
            if let Some(child) = self.child {
                let work = 1 + values[root].id().len().min(values[child].id().len());
                if !control.allow(work, 0, 0)? {
                    return Ok(false);
                }
                self.child = None;
                if values[root].key() > values[child].key() {
                    values.swap(root, child);
                    self.root = Some(child);
                } else {
                    self.root = None;
                }
                continue;
            }
            // root <= end/2 for every live sift; checked arithmetic also
            // protects the terminal leaf case on a maximal usize buffer.
            let child = root.checked_mul(2).and_then(|n| n.checked_add(1));
            let Some(child) = child.filter(|child| *child < self.end) else {
                if !control.allow(1, 0, 0)? {
                    return Ok(false);
                }
                self.root = None;
                continue;
            };
            let work = if child + 1 < self.end {
                1 + values[child].id().len().min(values[child + 1].id().len())
            } else {
                1
            };
            if !control.allow(work, 0, 0)? {
                return Ok(false);
            }
            self.child = Some(
                if child + 1 < self.end && values[child].key() > values[child + 1].key() {
                    child + 1
                } else {
                    child
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn permutations(values: &mut [&str], start: usize, visit: &mut impl FnMut(&[&str])) {
        if start == values.len() {
            visit(values);
            return;
        }
        for index in start..values.len() {
            values.swap(start, index);
            permutations(values, start + 1, visit);
            values.swap(start, index);
        }
    }
    #[test]
    fn every_small_permutation_preserves_heap_continuations_across_quanta() {
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut cases = 0;
        permutations(&mut ["a", "aa", "aaa", "b", "ba"], 0, &mut |input| {
            for work in 4..=7 {
                let mut values: Vec<_> = input
                    .iter()
                    .map(|id| Buffered::Delete(Term::copy(id, Some(&memory)).unwrap()))
                    .collect();
                let mut heap = Heap::new(values.len());
                let mut control =
                    Control::new(Default::default(), memory.clone(), task.clone(), 1024);
                let mut iterations = 0;
                loop {
                    control.begin_step(SearchMutationSortStepLimits {
                        work: NonZeroUsize::new(work).unwrap(),
                        ..Default::default()
                    });
                    let complete = heap.advance(&mut values, &mut control).unwrap();
                    let usage = control.end_step();
                    assert!(usage.work > 0 && usage.work <= work);
                    assert_eq!(usage.read_bytes, 0);
                    assert_eq!(usage.spill_bytes, 0);
                    iterations += 1;
                    assert!(iterations < 100);
                    if complete {
                        break;
                    }
                }
                let mut expected = input.to_vec();
                expected.sort_unstable_by(|a, b| b.cmp(a));
                assert_eq!(
                    values.iter().map(Buffered::id).collect::<Vec<_>>(),
                    expected
                );
                cases += 1;
            }
        });
        assert_eq!(cases, 480);
    }
}
