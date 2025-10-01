use std::fs;
use std::path::PathBuf;
use std::sync::Once;
use uuid::Uuid;

static INIT: Once = Once::new();

pub fn test_dir(test_name: &str, uuid: Uuid) -> PathBuf {
    // Figure out the target directory:
    // 1) Honor CARGO_TARGET_DIR if set
    // 2) Else use <workspace_root>/target
    let target = cargo_target_dir().unwrap_or_else(|| workspace_root().join("target"));

    let tests_root = target.join("test");
    fs::create_dir_all(&tests_root).unwrap();

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

/// Return CARGO_TARGET_DIR if set in the environment.
fn cargo_target_dir() -> Option<PathBuf> {
    std::env::var_os("CARGO_TARGET_DIR").map(PathBuf::from)
}

/// Best effort to find the **workspace** root:
/// - Prefer `CARGO_WORKSPACE_DIR` if available (newer Cargo exposes it at compile time)
/// - Otherwise walk up from the crate’s manifest dir until we hit a directory that looks like the repo root
fn workspace_root() -> PathBuf {
    // If compiling with a Cargo that sets this, it's the most reliable.
    if let Some(ws) = option_env!("CARGO_WORKSPACE_DIR") {
        return PathBuf::from(ws);
    }

    // Fall back to walking up from the crate’s manifest directory.
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        // Heuristics for the repo/workspace root:
        // - Usually contains Cargo.lock (after first build) or .git
        // - And is the place where /target sits for a workspace
        let looks_like_root = p.join("Cargo.lock").exists() || p.join(".git").is_dir() || p.join(".git").is_file();

        if looks_like_root {
            return p;
        }
        if !p.pop() {
            // Give up: use the crate dir as a last resort.
            return PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
}
