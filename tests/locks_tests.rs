use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::PathBuf;

use fs2::FileExt;
use tempfile::TempDir;

use timevault::errors::TvError;
use timevault::store::locks::acquire_store_lock;
use timevault::store::Store;
use timevault::config::StoreConfig;

fn lock_file_path(root: &PathBuf) -> PathBuf { root.join(".timevault.write.lock") }

#[test]
fn test_acquire_creates_file_and_writes_pid() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();

    // First acquisition should succeed
    let _lock = acquire_store_lock(&root).unwrap();

    // File should exist and contain pid line
    let p = lock_file_path(&root);
    assert!(p.exists(), "lock file not created");

    let mut s = String::new();
    fs::File::open(&p).unwrap().read_to_string(&mut s).unwrap();
    assert!(s.starts_with("pid=") && s.ends_with('\n'), "unexpected content: {s:?}");
}

#[test]
fn test_acquire_fails_when_locked_by_other() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let p = lock_file_path(&root);

    // Ensure the file exists
    let f = OpenOptions::new().create(true).read(true).write(true).open(&p).unwrap();
    // Simulate another process holding the lock
    f.try_lock_exclusive().expect("should be able to lock for test");

    // Now our acquire should fail with AlreadyOpen
    let err = acquire_store_lock(&root).unwrap_err();
    match err { TvError::AlreadyOpen => {}, other => panic!("expected AlreadyOpen, got {other:?}") }

    // Drop f at end of scope to release lock
}

#[test]
fn test_acquire_succeeds_when_file_exists_but_unlocked() {
    let td = TempDir::new().unwrap();
    let root = td.path().to_path_buf();
    let p = lock_file_path(&root);

    // Create the file and write some placeholder content without locking
    {
        use std::io::Write;
        let mut f = OpenOptions::new().create(true).read(true).write(true).open(&p).unwrap();
        f.write_all(b"junk\n").unwrap();
        // no lock held when scope ends
    }

    // Should succeed and overwrite with pid line
    let _lock = acquire_store_lock(&root).unwrap();

    let mut s = String::new();
    fs::File::open(&p).unwrap().read_to_string(&mut s).unwrap();
    assert!(s.starts_with("pid="), "expected pid=..., got {s:?}");
}

#[test]
fn test_store_open_prevents_multiple_writers() {
    let td = TempDir::new().unwrap();
    let root = td.path();
    let cfg = StoreConfig::default();

    let store1 = Store::open(root, cfg.clone()).expect("first store open should succeed");

    let err = match Store::open(root, cfg.clone()) {
        Ok(_) => panic!("expected AlreadyOpen error"),
        Err(err) => err,
    };
    match err { TvError::AlreadyOpen => {}, other => panic!("expected AlreadyOpen, got {other:?}") }

    drop(store1);

    Store::open(root, cfg).expect("lock should be released after Store drop");
}
