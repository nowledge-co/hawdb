//! Deterministic local campaigns, deliberately ignored by ordinary test jobs.

use super::*;

mod analysis;
mod artifact;
mod bytes;
mod dictionary_memory;
mod merge;
mod query_memory;
mod state_machine;

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 ^ (self.0 >> 29)
    }

    fn index(&mut self, bound: usize) -> usize {
        assert!(bound > 0);
        (self.next() % bound as u64) as usize
    }

    fn bytes(&mut self, length: usize) -> Vec<u8> {
        (0..length).map(|_| self.next() as u8).collect()
    }

    fn mutate(&mut self, original: &[u8], case: usize) -> Vec<u8> {
        let mut bytes = original.to_vec();
        match case % 7 {
            0 => bytes.truncate(self.index(bytes.len() + 1)),
            1 => bytes.extend_from_slice(&self.next().to_le_bytes()),
            2 => {
                let length = self.index(original.len() + 16);
                bytes = self.bytes(length);
            }
            3 => {
                if bytes.len() >= 8 {
                    let offset = self.index(bytes.len() - 7);
                    let value = [0, 1, u32::MAX as u64, u64::MAX][self.index(4)];
                    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
                }
            }
            _ => {
                for _ in 0..1 + self.index(4) {
                    if !bytes.is_empty() {
                        let offset = self.index(bytes.len());
                        bytes[offset] ^= 1 << self.index(8);
                    }
                }
            }
        }
        bytes
    }
}

#[derive(Default, Debug)]
struct Outcomes {
    accepted: usize,
    rejected: usize,
}

impl Outcomes {
    fn record(&mut self, accepted: bool) {
        if accepted {
            self.accepted += 1;
        } else {
            self.rejected += 1;
        }
    }

    fn finish(&self, name: &str, cases: usize) {
        assert_eq!(self.accepted + self.rejected, cases);
        assert!(self.accepted > 0 && self.rejected > 0, "{name}: {self:?}");
        eprintln!("{name}: {self:?}, cases={cases}");
    }
}

fn temporary_root(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = projection_root(&format!(
        "fuzz-{name}-{}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    // Never erase a previous failed run's artifacts before constructing a fixture.
    fs::create_dir(&root).unwrap();
    root
}
