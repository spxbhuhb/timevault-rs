#![cfg(test)]

use std::{env, fs};
use std::sync::Once;
use std::path::PathBuf;
use uuid::Uuid;

static INIT: Once = Once::new();

pub fn test_dir(test_name: &str, uuid: Uuid) -> PathBuf {
    let target_dir = env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| project_root().join("target"));

    let tests_root = target_dir.join("test");
    fs::create_dir_all(&tests_root).expect("create tests_root");

    // this lets the test with the same name create more test dirs during one run
    INIT.call_once(|| {
        // Clear previous runs' outputs for this test to avoid accumulation
        if let Ok(entries) = fs::read_dir(&tests_root) {
            for e in entries.flatten() {
                if let Ok(name) = e.file_name().into_string() {
                    if name.starts_with(test_name) {
                        let _ = fs::remove_dir_all(e.path());
                    }
                }
            }
        }
    });

    // Make it unique to avoid clashes with parallel tests
    let dir = tests_root.join(format!("{test_name}-{uuid}"));

    fs::create_dir_all(&dir).expect("create per-test dir");
    dir
}

/// Best-effort project root (crate root with Cargo.toml), compile-time stable.
pub fn project_root() -> PathBuf {
    // Works from the crate that defines this function, both in unit & integration tests.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
