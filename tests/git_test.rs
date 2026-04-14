// tests/git_test.rs
// Run with: cargo test --test git_test
// Requires: DITTOFS_MOUNT env var pointing to a live DittoFS mount

use std::process::Command;

#[test]
fn git_init_succeeds_on_mount() {
    let mount = std::env::var("DITTOFS_MOUNT")
        .unwrap_or_else(|_| "/tmp/ditto_a".to_string());

    let repo_path = format!("{}/test_repo", mount);
    std::fs::create_dir_all(&repo_path).expect("mkdir failed");

    let output = Command::new("git")
        .args(["init", &repo_path])
        .output()
        .expect("git not found");

    assert!(
        output.status.success(),
        "git init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Verify .git directory was created
    assert!(
        std::path::Path::new(&format!("{}/.git", repo_path)).exists(),
        ".git directory not created"
    );

    // Write and commit a file
    std::fs::write(format!("{}/README.md", repo_path), b"hello").unwrap();

    let add = Command::new("git")
        .args(["-C", &repo_path, "add", "README.md"])
        .output()
        .expect("git add failed");
    assert!(add.status.success());

    let commit = Command::new("git")
        .args(["-C", &repo_path, "commit", "-m", "init", "--author",
               "Test User <test@test.com>"])
        .output()
        .expect("git commit failed");
    assert!(
        commit.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&commit.stderr)
    );

    // Cleanup
    std::fs::remove_dir_all(&repo_path).ok();
}
