//! Regression tests for the workspace-boundary escape in `handle_git_status`
//! (SEC-4 / DOC-2 / FS-2): a caller-supplied `path` was handed straight to git,
//! so a workspace rooted at a subdirectory of a larger worktree could read
//! tracked content anywhere in that worktree via `../` or magic pathspecs.

use gpt2omo::security::Workspace;
use gpt2omo::tools::handle_git_status;
use std::fs;
use std::path::Path;
use std::process::Command;
use tempfile::tempdir;

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap_or_else(|e| panic!("failed to run git {:?}: {e}", args));
    assert!(status.success(), "git {:?} failed in {}", args, dir.display());
}

/// Builds a git worktree whose root holds a secret file and whose `sub/`
/// subdirectory is the (narrower) workspace scope. Returns (tempdir, scope).
fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let root = tempdir().unwrap();
    git(root.path(), &["init", "-q"]);
    git(root.path(), &["config", "user.email", "test@example.com"]);
    git(root.path(), &["config", "user.name", "Test"]);

    fs::write(root.path().join("outside_secret.txt"), "committed\n").unwrap();
    fs::write(root.path().join(".env"), "API_KEY=committed\n").unwrap();
    let sub = root.path().join("sub");
    fs::create_dir(&sub).unwrap();
    fs::write(sub.join("inside.txt"), "inside\n").unwrap();

    git(root.path(), &["add", "-A", "-f"]);
    git(root.path(), &["commit", "-qm", "init"]);

    // Dirty the out-of-scope files so any escape produces visible diff output.
    fs::write(root.path().join("outside_secret.txt"), "LEAKED SECRET\n").unwrap();
    fs::write(root.path().join(".env"), "API_KEY=LEAKED SECRET\n").unwrap();

    (root, sub)
}

fn rendered(result: &gpt2omo::tools::ToolCallResult) -> String {
    serde_json::to_string(&result.data).unwrap_or_default()
}

#[tokio::test]
async fn git_status_rejects_parent_traversal_outside_workspace_scope() {
    let (_root, sub) = fixture();
    let ws = Workspace::open(&sub).unwrap();

    let result = handle_git_status(&ws, Some("../outside_secret.txt")).await;

    assert!(
        !result.success,
        "expected `../outside_secret.txt` to be rejected as a workspace-boundary escape, \
         but the call succeeded and returned: {}",
        rendered(&result)
    );
    let error = result.error.unwrap_or_default();
    assert!(
        error.contains("Path traversal"),
        "expected a path-traversal security error, got: {error}"
    );
}

#[tokio::test]
async fn git_status_does_not_leak_outside_content_via_magic_pathspec() {
    let (_root, sub) = fixture();
    let ws = Workspace::open(&sub).unwrap();

    let result = handle_git_status(&ws, Some(":(top)outside_secret.txt")).await;

    // The echoed request path is excluded: only git-produced output can leak.
    let data = result.data.clone().unwrap_or_default();
    let git_output = format!(
        "{}{}{}",
        data["status"], data["diff_stat"], data["diff"]
    );
    assert!(
        !git_output.contains("outside_secret"),
        "magic pathspec `:(top)` re-widened the scope and leaked out-of-scope content: {}",
        rendered(&result)
    );
}

#[tokio::test]
async fn git_status_rejects_denied_credential_names() {
    let (_root, sub) = fixture();
    let ws = Workspace::open(&sub).unwrap();

    let result = handle_git_status(&ws, Some("../.env")).await;

    assert!(
        !result.success,
        "expected `.env` to be denied by the workspace path policy, but got: {}",
        rendered(&result)
    );
}

#[tokio::test]
async fn git_status_still_reports_in_scope_changes() {
    let (_root, sub) = fixture();
    fs::write(sub.join("inside.txt"), "modified inside\n").unwrap();
    let ws = Workspace::open(&sub).unwrap();

    let result = handle_git_status(&ws, Some("inside.txt")).await;

    assert!(result.success, "in-scope path must still work: {:?}", result.error);
    let data = result.data.unwrap();
    assert_eq!(data["is_git_repo"], true);
    assert!(
        data["diff"].as_str().unwrap().contains("modified inside"),
        "expected the in-scope diff, got: {}",
        data["diff"]
    );
}
