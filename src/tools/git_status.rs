use crate::security::Workspace;
use crate::tools::ToolCallResult;
use std::process::Command;

const MAX_DIFF_CHARS: usize = 30_000;

pub fn handle_git_status(ws: &Workspace, target_path: Option<&str>) -> ToolCallResult {
    if !is_git_worktree(ws) {
        return ToolCallResult::ok(serde_json::json!({
            "is_git_repo": false,
            "status": "",
            "diff_stat": "",
            "diff": "",
            "diff_truncated": false,
            "is_clean": true
        }));
    }

    let p = target_path.map(str::trim).filter(|s| !s.is_empty() && *s != ".");

    let status_out = if let Some(path) = p {
        run_git(ws, &["status", "--porcelain", "--", path]).unwrap_or_default()
    } else {
        run_git(ws, &["status", "--porcelain", "--untracked-files=no"]).unwrap_or_default()
    };

    let (diff_stat, combined) = if let Some(path) = p {
        let unstaged_stat = run_git(ws, &["diff", "--stat", "--", path]).unwrap_or_default();
        let staged_stat = run_git(ws, &["diff", "--cached", "--stat", "--", path]).unwrap_or_default();
        let stat = match (unstaged_stat.is_empty(), staged_stat.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged_stat,
            (true, false) => format!("# Staged changes\n{}", staged_stat),
            (false, false) => format!("# Unstaged changes\n{}\n# Staged changes\n{}", unstaged_stat, staged_stat),
        };
        let unstaged = run_git(ws, &["diff", "--no-ext-diff", "--unified=3", "--", path]).unwrap_or_default();
        let staged = run_git(ws, &["diff", "--cached", "--no-ext-diff", "--unified=3", "--", path]).unwrap_or_default();
        let diff_comb = match (unstaged.is_empty(), staged.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged,
            (true, false) => format!("# Staged changes\n{}", staged),
            (false, false) => format!("# Unstaged changes\n{}\n# Staged changes\n{}", unstaged, staged),
        };
        (stat, diff_comb)
    } else {
        let unstaged_stat = run_git(ws, &["diff", "--stat"]).unwrap_or_default();
        let staged_stat = run_git(ws, &["diff", "--cached", "--stat"]).unwrap_or_default();
        let stat = match (unstaged_stat.is_empty(), staged_stat.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged_stat,
            (true, false) => format!("# Staged changes\n{}", staged_stat),
            (false, false) => format!("# Unstaged changes\n{}\n# Staged changes\n{}", unstaged_stat, staged_stat),
        };
        let msg = "# Notice: Entire repository diff scan is disabled for performance. Pass `path` parameter (e.g. `path: \"src/my_file.rs\"`) to inspect exact file diffs.";
        (stat, msg.to_string())
    };

    let (diff, diff_truncated) = truncate_chars(&combined, MAX_DIFF_CHARS);

    ToolCallResult::ok(serde_json::json!({
        "is_git_repo": true,
        "path": p.unwrap_or(""),
        "status": status_out,
        "diff_stat": diff_stat,
        "diff": diff,
        "diff_truncated": diff_truncated,
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
        let result = handle_git_status(&ws, Some("a.txt"));
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], true);
        assert!(data["diff"].as_str().unwrap().contains("+two"));
        assert!(!data["is_clean"].as_bool().unwrap());
    }

    #[test]
    fn test_git_status_reports_non_git_workspace_without_error() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let result = handle_git_status(&ws, None);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], false);
    }


}
