use crate::security::{PathPolicy, Workspace};
use crate::tools::ToolCallResult;
use std::process::Command;

const MAX_DIFF_CHARS: usize = 120_000;
const NON_GIT_MESSAGE: &str =
    "Workspace is not a Git repository; git status/diff checks were skipped";

pub(crate) struct WorktreeCheck {
    pub ok: bool,
    pub output: String,
}

pub fn handle_git_status(ws: &Workspace) -> ToolCallResult {
    if !is_git_worktree(ws) {
        return ToolCallResult::ok(serde_json::json!({
            "is_git_repo": false,
            "status": "",
            "diff_stat": "",
            "diff": "",
            "diff_truncated": false,
            "diff_check_ok": true,
            "diff_check_output": NON_GIT_MESSAGE,
            "is_clean": true
        }));
    }

    let status_out = match run_git(ws, &["status", "--porcelain", "--untracked-files=all"]) {
        Ok(out) => out,
        Err(e) => return ToolCallResult::err(e),
    };

    let unstaged_stat = run_git(ws, &["diff", "--stat"]).unwrap_or_default();
    let staged_stat = run_git(ws, &["diff", "--cached", "--stat"]).unwrap_or_default();
    let diff_stat = match (unstaged_stat.is_empty(), staged_stat.is_empty()) {
        (true, true) => String::new(),
        (false, true) => unstaged_stat,
        (true, false) => format!("# Staged changes\n{}", staged_stat),
        (false, false) => format!(
            "# Unstaged changes\n{}\n# Staged changes\n{}",
            unstaged_stat, staged_stat
        ),
    };

    let unstaged = run_git(ws, &["diff", "--no-ext-diff", "--unified=3"]).unwrap_or_default();
    let staged =
        run_git(ws, &["diff", "--cached", "--no-ext-diff", "--unified=3"]).unwrap_or_default();
    let combined = match (unstaged.is_empty(), staged.is_empty()) {
        (true, true) => String::new(),
        (false, true) => unstaged,
        (true, false) => format!("# Staged changes\n{}", staged),
        (false, false) => format!(
            "# Unstaged changes\n{}\n# Staged changes\n{}",
            unstaged, staged
        ),
    };
    let (diff, diff_truncated) = truncate_chars(&combined, MAX_DIFF_CHARS);
    let check = check_worktree_whitespace(ws);

    ToolCallResult::ok(serde_json::json!({
        "is_git_repo": true,
        "status": status_out,
        "diff_stat": diff_stat,
        "diff": diff,
        "diff_truncated": diff_truncated,
        "diff_check_ok": check.ok,
        "diff_check_output": check.output,
        "is_clean": status_out.trim().is_empty()
    }))
}

pub(crate) fn is_git_worktree(ws: &Workspace) -> bool {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "--is-inside-work-tree"])
        .current_dir(ws.root())
        .output()
    else {
        return false;
    };

    output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true"
}

pub(crate) fn check_worktree_whitespace(ws: &Workspace) -> WorktreeCheck {
    if !is_git_worktree(ws) {
        return WorktreeCheck {
            ok: true,
            output: NON_GIT_MESSAGE.into(),
        };
    }

    let mut problems = Vec::new();

    if let Err(e) = run_git(ws, &["diff", "--check"]) {
        problems.push(e);
    }
    if let Err(e) = run_git(ws, &["diff", "--cached", "--check"]) {
        problems.push(e);
    }
    if let Err(e) = check_untracked_whitespace(ws) {
        problems.push(e);
    }

    WorktreeCheck {
        ok: problems.is_empty(),
        output: problems.join("\n"),
    }
}

fn check_untracked_whitespace(ws: &Workspace) -> std::result::Result<(), String> {
    let output = Command::new("git")
        .args(["status", "--porcelain=v1", "-z", "--untracked-files=all"])
        .current_dir(ws.root())
        .output()
        .map_err(|e| format!("Failed to run git status for untracked files: {}", e))?;
    if !output.status.success() {
        return Err(format!(
            "git status for untracked files failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let dir = ws
        .cap_dir()
        .map_err(|e| format!("Failed to open workspace capability: {}", e))?;
    let mut issues = Vec::new();

    for entry in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|e| !e.is_empty())
    {
        if !entry.starts_with(b"?? ") {
            continue;
        }
        let raw_path = &entry[3..];
        let path_str = std::str::from_utf8(raw_path)
            .map_err(|_| "Cannot verify whitespace for a non-UTF8 untracked path".to_string())?;
        let rel_path = PathPolicy::sanitize_relative_path(path_str)
            .map_err(|e| format!("Cannot verify untracked '{}': {}", path_str, e))?;
        let bytes = match dir.read(&rel_path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::IsADirectory => continue,
            Err(e) => return Err(format!("Failed to read untracked '{}': {}", path_str, e)),
        };
        if bytes.contains(&0) {
            continue;
        }
        issues.extend(find_whitespace_errors(path_str, &bytes));
    }

    if issues.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "untracked file whitespace errors:\n{}",
            issues.join("\n")
        ))
    }
}

fn find_whitespace_errors(path: &str, bytes: &[u8]) -> Vec<String> {
    let mut issues = Vec::new();
    for (index, raw_line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line
            .last()
            .is_some_and(|byte| *byte == b' ' || *byte == b'\t')
        {
            issues.push(format!("{}:{}: trailing whitespace", path, index + 1));
        }

        let mut previous_space = false;
        for byte in line {
            match *byte {
                b' ' => previous_space = true,
                b'\t' if previous_space => {
                    issues.push(format!(
                        "{}:{}: space before tab in indent",
                        path,
                        index + 1
                    ));
                    break;
                }
                b'\t' => previous_space = false,
                _ => break,
            }
        }
    }
    issues
}

fn run_git(ws: &Workspace, args: &[&str]) -> std::result::Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(ws.root())
        .output()
        .map_err(|e| format!("Failed to run git {}: {}", args.join(" "), e))?;

    if !output.status.success() {
        let mut text = String::from_utf8_lossy(&output.stdout).to_string();
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        return Err(format!("git {} failed: {}", args.join(" "), text.trim()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        return (text.to_string(), false);
    }
    let half = max_chars / 2;
    let head: String = text.chars().take(half).collect();
    let tail: String = text
        .chars()
        .rev()
        .take(half)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    (
        format!(
            "{}\n\n...[diff truncated by omo-bridge; {} characters omitted]...\n\n{}",
            head,
            count.saturating_sub(max_chars),
            tail
        ),
        true,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn init_git(path: &std::path::Path) {
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(path)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(path)
            .status()
            .unwrap();
    }

    #[test]
    fn test_git_status_includes_diff_for_tracked_change() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("a.txt"), "one\n").unwrap();
        Command::new("git")
            .args(["add", "a.txt"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-qm", "init"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        fs::write(dir.path().join("a.txt"), "two\n").unwrap();

        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_git_status(&ws);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], true);
        assert!(data["diff"].as_str().unwrap().contains("+two"));
        assert!(!data["is_clean"].as_bool().unwrap());
        assert!(data["diff_check_ok"].as_bool().unwrap());
    }

    #[test]
    fn test_git_status_reports_non_git_workspace_without_error() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let result = handle_git_status(&ws);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], false);
        assert_eq!(data["diff_check_ok"], true);
        assert!(data["diff_check_output"]
            .as_str()
            .unwrap()
            .contains("not a Git repository"));
    }

    #[test]
    fn test_worktree_check_catches_staged_whitespace() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("staged.txt"), "bad trailing   \n").unwrap();
        Command::new("git")
            .args(["add", "staged.txt"])
            .current_dir(dir.path())
            .status()
            .unwrap();

        let ws = Workspace::open(dir.path()).unwrap();
        let check = check_worktree_whitespace(&ws);
        assert!(!check.ok);
        assert!(check.output.contains("whitespace"));
    }

    #[test]
    fn test_worktree_check_catches_untracked_whitespace() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("untracked.txt"), "bad trailing \n").unwrap();

        let ws = Workspace::open(dir.path()).unwrap();
        let check = check_worktree_whitespace(&ws);
        assert!(!check.ok);
        assert!(check
            .output
            .contains("untracked.txt:1: trailing whitespace"));
    }

    #[test]
    fn test_worktree_check_accepts_clean_untracked_text() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("untracked.txt"), "clean\n").unwrap();

        let ws = Workspace::open(dir.path()).unwrap();
        let check = check_worktree_whitespace(&ws);
        assert!(check.ok, "{}", check.output);
    }

    #[test]
    fn test_worktree_check_skips_non_git_workspace() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let check = check_worktree_whitespace(&ws);
        assert!(check.ok, "{}", check.output);
        assert!(check.output.contains("not a Git repository"));
    }
}
