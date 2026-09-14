use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde_json::Value;
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::time::timeout;

const DEFAULT_TIMEOUT_MS: u64 = 20_000;
const MAX_CAPTURE_BYTES: usize = 8 * 1024 * 1024;

pub async fn handle_ast_grep(
    ws: &Workspace,
    pattern: &str,
    subpath: Option<&str>,
    language: Option<&str>,
    max_results: Option<usize>,
) -> ToolCallResult {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return ToolCallResult::err("AST pattern cannot be empty");
    }
    if pattern.chars().count() > 2_000 {
        return ToolCallResult::err("AST pattern is too long (maximum 2000 characters)");
    }

    let target = match subpath {
        Some(path) if !path.trim().is_empty() && path.trim() != "." => {
            match ws.resolve_relative(path) {
                Ok(path) => path,
                Err(e) => return ToolCallResult::err(e.to_string()),
            }
        }
        _ => ws.root().to_path_buf(),
    };

    let Some(binary) = which_ast_grep().await else {
        return ToolCallResult::err(
            "ast-grep is not installed or not on PATH (expected 'sg' or 'ast-grep')",
        );
    };

    let mut args = vec![
        "--pattern".to_string(),
        pattern.to_string(),
        "--json=stream".to_string(),
    ];
    if let Some(language) = language.map(str::trim).filter(|s| !s.is_empty()) {
        args.push("--lang".into());
        args.push(language.into());
    }
    args.push(target.to_string_lossy().to_string());

    let started = Instant::now();
    let mut command = Command::new(&binary);
    command
        .args(&args)
        .current_dir(ws.root())
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => return ToolCallResult::err(format!("Failed to start ast-grep: {e}")),
    };
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let timeout_duration = Duration::from_millis(DEFAULT_TIMEOUT_MS);
    let execution = async move {
        let (status, (stdout_bytes, stdout_truncated), (stderr_bytes, stderr_truncated)) = tokio::join!(
            child.wait(),
            read_limited_async(stdout, MAX_CAPTURE_BYTES),
            read_limited_async(stderr, 256 * 1024),
        );
        status
            .map(|status| {
                (
                    status,
                    stdout_bytes,
                    stdout_truncated,
                    stderr_bytes,
                    stderr_truncated,
                )
            })
            .map_err(|e| format!("Failed while waiting for ast-grep: {e}"))
    };
    let (status, stdout_bytes, stdout_truncated, stderr_bytes, stderr_truncated, timed_out) =
        match timeout(timeout_duration, execution).await {
            Ok(Ok((status, stdout_bytes, stdout_truncated, stderr_bytes, stderr_truncated))) => (
                Some(status),
                stdout_bytes,
                stdout_truncated,
                stderr_bytes,
                stderr_truncated,
                false,
            ),
            Ok(Err(error)) => return ToolCallResult::err(error),
            Err(_) => (None, Vec::new(), false, Vec::new(), false, true),
        };

    let stdout = String::from_utf8_lossy(&stdout_bytes);
    let stderr = String::from_utf8_lossy(&stderr_bytes).to_string();
    let cap = max_results.unwrap_or(100).clamp(1, 1_000);
    let mut matches = Vec::new();
    let mut parse_errors = 0usize;

    for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
        match serde_json::from_str::<Value>(line) {
            Ok(value) => {
                matches.push(value);
                if matches.len() >= cap {
                    break;
                }
            }
            Err(_) => parse_errors += 1,
        }
    }

    let process_success = status.as_ref().is_some_and(|s| s.success());
    if !process_success && matches.is_empty() && !stderr.trim().is_empty() && !timed_out {
        return ToolCallResult::err(format!("ast-grep failed: {}", stderr.trim()));
    }

    ToolCallResult::ok(serde_json::json!({
        "pattern": pattern,
        "path": subpath.unwrap_or("."),
        "language": language,
        "binary": binary,
        "matches": matches,
        "match_count": matches.len(),
        "truncated": matches.len() >= cap || stdout_truncated,
        "parse_errors": parse_errors,
        "timed_out": timed_out,
        "duration_ms": started.elapsed().as_millis() as u64,
        "stderr": stderr,
        "stderr_truncated": stderr_truncated,
    }))
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

async fn which_ast_grep() -> Option<String> {
    for candidate in ["sg", "ast-grep"] {
        let mut command = Command::new(candidate);
        command
            .arg("--version")
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if timeout(Duration::from_secs(2), command.status())
            .await
            .ok()
            .and_then(Result::ok)
            .is_some_and(|status| status.success())
        {
            return Some(candidate.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn rejects_empty_pattern() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_ast_grep(&ws, "", None, None, None).await;
        assert!(!result.success);
    }

    #[tokio::test]
    async fn rejects_traversal_path() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_ast_grep(&ws, "$A", Some("../outside"), None, None).await;
        assert!(!result.success);
    }
}
