use std::fs;
use std::path::PathBuf;

pub fn test_dir(test_name : &str) -> PathBuf {
    // Compute a stable location under the project root: ./target/test/<testname>-<uuid>
    let project_root = project_root();
    let tests_root = project_root.join("../../target").join("test");
    fs::create_dir_all(&tests_root).unwrap();

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

    tests_root.join(test_name)
}

// Best-effort to get the project root when running with `cargo test`.
// It walks up from CARGO_MANIFEST_DIR if present, else current_dir.
pub fn project_root() -> PathBuf {
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return PathBuf::from(manifest_dir);
    }
    std::env::current_dir().unwrap()
}