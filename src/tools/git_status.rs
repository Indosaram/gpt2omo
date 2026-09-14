use crate::security::{PathPolicy, Workspace};
use crate::tools::ToolCallResult;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::time::timeout;

const MAX_DIFF_CHARS: usize = 30_000;
const GIT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_GIT_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_GIT_STDERR_BYTES: usize = 256 * 1024;

pub async fn handle_git_status(ws: &Workspace, target_path: Option<&str>) -> ToolCallResult {
    if !is_git_worktree(ws).await {
        return ToolCallResult::ok(serde_json::json!({
            "is_git_repo": false,
            "status": "",
            "diff_stat": "",
            "diff": "",
            "diff_truncated": false,
            "is_clean": true
        }));
    }

    let requested = target_path
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != ".");

    // The caller-supplied path must satisfy the same workspace policy as the file
    // tools before it reaches git: git resolves pathspecs against the enclosing
    // repository, so an unchecked `..` (or a denied credential name) would read
    // tracked content from outside the workspace root. The `:(literal)` prefix
    // additionally disables git's magic pathspec syntax (`:(top)`, globs), which
    // could otherwise re-widen the scope.
    let pathspec = match requested {
        Some(path) => match PathPolicy::sanitize_relative_path(path) {
            Ok(rel) => Some(format!(
                ":(literal){}",
                rel.to_string_lossy().replace('\\', "/")
            )),
            Err(e) => return ToolCallResult::err(e.to_string()),
        },
        None => None,
    };
    let p = pathspec.as_deref();

    let status_out = if let Some(path) = p {
        run_git(ws, &["status", "--porcelain", "--", path])
            .await
            .unwrap_or_default()
    } else {
        run_git(ws, &["status", "--porcelain", "--untracked-files=no"])
            .await
            .unwrap_or_default()
    };

    let (diff_stat, combined) = if let Some(path) = p {
        let unstaged_stat = run_git(ws, &["diff", "--stat", "--", path])
            .await
            .unwrap_or_default();
        let staged_stat = run_git(ws, &["diff", "--cached", "--stat", "--", path])
            .await
            .unwrap_or_default();
        let stat = match (unstaged_stat.is_empty(), staged_stat.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged_stat,
            (true, false) => format!("# Staged changes\n{}", staged_stat),
            (false, false) => format!(
                "# Unstaged changes\n{}\n# Staged changes\n{}",
                unstaged_stat, staged_stat
            ),
        };
        let unstaged = run_git(ws, &["diff", "--no-ext-diff", "--unified=3", "--", path])
            .await
            .unwrap_or_default();
        let staged = run_git(
            ws,
            &[
                "diff",
                "--cached",
                "--no-ext-diff",
                "--unified=3",
                "--",
                path,
            ],
        )
        .await
        .unwrap_or_default();
        let diff_comb = match (unstaged.is_empty(), staged.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged,
            (true, false) => format!("# Staged changes\n{}", staged),
            (false, false) => format!(
                "# Unstaged changes\n{}\n# Staged changes\n{}",
                unstaged, staged
            ),
        };
        (stat, diff_comb)
    } else {
        let unstaged_stat = run_git(ws, &["diff", "--stat"]).await.unwrap_or_default();
        let staged_stat = run_git(ws, &["diff", "--cached", "--stat"])
            .await
            .unwrap_or_default();
        let stat = match (unstaged_stat.is_empty(), staged_stat.is_empty()) {
            (true, true) => String::new(),
            (false, true) => unstaged_stat,
            (true, false) => format!("# Staged changes\n{}", staged_stat),
            (false, false) => format!(
                "# Unstaged changes\n{}\n# Staged changes\n{}",
                unstaged_stat, staged_stat
            ),
        };
        let msg = "# Notice: Entire repository diff scan is disabled for performance. Pass `path` parameter (e.g. `path: \"src/my_file.rs\"`) to inspect exact file diffs.";
        (stat, msg.to_string())
    };

    let (diff, diff_truncated) = truncate_chars(&combined, MAX_DIFF_CHARS);

    ToolCallResult::ok(serde_json::json!({
        "is_git_repo": true,
        "path": requested.unwrap_or(""),
        "status": status_out,
        "diff_stat": diff_stat,
        "diff": diff,
        "diff_truncated": diff_truncated,
        "is_clean": status_out.trim().is_empty()
    }))
}

pub(crate) async fn is_git_worktree(ws: &Workspace) -> bool {
    let Ok(output) = run_git_bounded(
        ws,
        &["rev-parse", "--is-inside-work-tree"],
        GIT_TIMEOUT,
        1024,
    )
    .await
    else {
        return false;
    };

    output.trim() == "true"
}

pub(crate) async fn run_git(ws: &Workspace, args: &[&str]) -> std::result::Result<String, String> {
    run_git_bounded(ws, args, GIT_TIMEOUT, MAX_GIT_OUTPUT_BYTES).await
}

pub(crate) async fn run_git_bounded(
    ws: &Workspace,
    args: &[&str],
    timeout_duration: Duration,
    max_stdout_bytes: usize,
) -> std::result::Result<String, String> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(ws.root())
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|e| format!("Failed to run git {}: {}", args.join(" "), e))?;
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let args_display = args.join(" ");

    let execution = async move {
        let stdout_future = read_limited_async(stdout, max_stdout_bytes);
        let stderr_future = read_limited_async(stderr, MAX_GIT_STDERR_BYTES);
        let (status, (stdout_bytes, _), (stderr_bytes, _)) =
            tokio::join!(child.wait(), stdout_future, stderr_future,);
        let status =
            status.map_err(|e| format!("Failed while waiting for git {args_display}: {e}"))?;
        Ok::<_, String>((status, stdout_bytes, stderr_bytes))
    };

    let (status, stdout_bytes, stderr_bytes) =
        timeout(timeout_duration, execution).await.map_err(|_| {
            format!(
                "git {} timed out after {}ms",
                args.join(" "),
                timeout_duration.as_millis()
            )
        })??;

    if !status.success() {
        let mut text = String::from_utf8_lossy(&stdout_bytes).to_string();
        let err_text = String::from_utf8_lossy(&stderr_bytes);
        if !err_text.trim().is_empty() {
            if !text.is_empty() {
                text.push(char::from(10));
            }
            text.push_str(&err_text);
        }
        return Err(format!("git {} failed: {}", args.join(" "), text.trim()));
    }

    Ok(String::from_utf8_lossy(&stdout_bytes).to_string())
}

async fn read_limited_async<R: AsyncRead + Unpin>(
    pipe: Option<R>,
    limit: usize,
) -> (Vec<u8>, bool) {
    let Some(mut pipe) = pipe else {
        return (Vec::new(), false);
    };
    let mut buffer = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0u8; 8192];
    let mut total_read = 0usize;
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => {
                total_read = total_read.saturating_add(read);
                if buffer.len() < limit {
                    let remaining = limit - buffer.len();
                    buffer.extend_from_slice(&chunk[..read.min(remaining)]);
                }
            }
        }
    }
    (buffer, total_read > limit)
}

fn truncate_chars(text: &str, max_chars: usize) -> (String, bool) {
    let count = text.chars().count();
    if count <= max_chars {
        return (text.to_string(), false);
    }
    let half = max_chars / 2;
    let tail_skip = count.saturating_sub(half);
    let mut head_end = 0;
    let mut tail_start = text.len();

    for (char_count, (byte_idx, _)) in text.char_indices().enumerate() {
        if char_count == half {
            head_end = byte_idx;
        }
        if char_count == tail_skip {
            tail_start = byte_idx;
            break;
        }
    }

    let head = &text[..head_end];
    let tail = &text[tail_start..];
    (
        format!(
            "{}\n\n...[diff truncated by gpt2omo; {} characters omitted]...\n\n{}",
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
    use std::process::Command as StdCommand;
    use tempfile::tempdir;

    fn init_git(path: &std::path::Path) {
        StdCommand::new("git")
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(path)
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["config", "user.name", "Test"])
            .current_dir(path)
            .status()
            .unwrap();
    }

    #[tokio::test]
    async fn test_git_status_includes_diff_for_tracked_change() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("a.txt"), "one\n").unwrap();
        StdCommand::new("git")
            .args(["add", "a.txt"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        StdCommand::new("git")
            .args(["commit", "-qm", "init"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        fs::write(dir.path().join("a.txt"), "two\n").unwrap();

        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_git_status(&ws, Some("a.txt")).await;
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], true);
        assert!(data["diff"].as_str().unwrap().contains("+two"));
        assert!(!data["is_clean"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_git_status_reports_non_git_workspace_without_error() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let result = handle_git_status(&ws, None).await;
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["is_git_repo"], false);
    }

    #[test]
    fn test_truncate_chars_short_and_exact() {
        let (res, truncated) = truncate_chars("hello", 10);
        assert_eq!(res, "hello");
        assert!(!truncated);

        let (res, truncated) = truncate_chars("hello", 5);
        assert_eq!(res, "hello");
        assert!(!truncated);
    }

    #[test]
    fn test_truncate_chars_long_ascii_and_multibyte() {
        let (res, truncated) = truncate_chars("abcdefghij", 4);
        assert!(truncated);
        assert!(
            res.starts_with("ab\n\n...[diff truncated by gpt2omo; 6 characters omitted]...\n\nij")
        );

        let unicode = "🦀🌟🎉🔥🚀✨";
        let (res, truncated) = truncate_chars(unicode, 4);
        assert!(truncated);
        assert!(res.starts_with(
            "🦀🌟\n\n...[diff truncated by gpt2omo; 2 characters omitted]...\n\n🚀✨"
        ));
    }

    #[tokio::test]
    async fn test_run_git_bounded_limit() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("big.txt"), "x".repeat(5000)).unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let res = run_git_bounded(&ws, &["status"], Duration::from_secs(5), 50).await;
        assert!(res.is_ok());
        let text = res.unwrap();
        assert!(text.len() <= 50);
    }

    #[tokio::test]
    async fn test_run_git_timeout() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        // git with a very small timeout on a non-instant command or short duration
        let res = run_git_bounded(&ws, &["status"], Duration::from_millis(0), 1024).await;
        // Either finishes immediately or times out; if it times out it should return Err containing "timed out"
        if let Err(e) = res {
            assert!(e.contains("timed out") || e.contains("Failed to run"));
        }
    }
}
