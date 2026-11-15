// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cargo-only source receipt for manual performance campaigns. Bazel tools
//! without this receipt remain usable for correctness and offline analysis.

use sha2::{Digest, Sha256};
use std::{fs, path::Path, process::Command};

fn git(root: &Path, args: &[&str]) -> Vec<u8> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "source receipt requires a Git checkout"
    );
    output.stdout
}

fn main() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let revision = git(&root, &["rev-parse", "HEAD"]);
    println!(
        "cargo:rustc-env=QUERY_LAB_BUILD_REVISION={}",
        String::from_utf8(revision).unwrap().trim()
    );
    let files = git(
        &root,
        &[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
    );
    let mut paths = files
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.dedup();
    let mut hasher = Sha256::new();
    for path in paths {
        let relative = std::str::from_utf8(path).unwrap();
        if ![".rs", ".toml", ".lock", ".c", ".h"]
            .iter()
            .any(|suffix| relative.ends_with(suffix))
        {
            continue;
        }
        let absolute = root.join(relative);
        println!("cargo:rerun-if-changed={}", absolute.display());
        hasher.update((path.len() as u64).to_le_bytes());
        hasher.update(path);
        let bytes = match fs::read(absolute) {
            Ok(bytes) => {
                hasher.update([1]);
                bytes
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                hasher.update([0]);
                continue;
            }
            Err(error) => panic!("source receipt input must be readable: {relative}: {error}"),
        };
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    println!("cargo:rerun-if-changed={}", root.join(".git").display());
    for name in ["HEAD", "index", "packed-refs"] {
        let path = git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-path", name],
        );
        println!(
            "cargo:rerun-if-changed={}",
            String::from_utf8(path).unwrap().trim()
        );
    }
    let branch = Command::new("git")
        .current_dir(&root)
        .args(["symbolic-ref", "--quiet", "HEAD"])
        .output()
        .unwrap();
    if branch.status.success() {
        let branch = std::str::from_utf8(&branch.stdout).unwrap().trim();
        let path = git(
            &root,
            &["rev-parse", "--path-format=absolute", "--git-path", branch],
        );
        println!(
            "cargo:rerun-if-changed={}",
            String::from_utf8(path).unwrap().trim()
        );
    }
    let receipt = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    println!("cargo:rustc-env=QUERY_LAB_BUILD_SOURCE_SHA256={receipt}");
}
