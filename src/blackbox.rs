//! Compatibility facade for storage-neutral, redacted diagnostic evidence.

pub use skein_evidence::blackbox::*;

#[cfg(test)]
#[path = "blackbox_facade_tests.rs"]
mod tests;
