use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde_json::Value;
use std::io::Read;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT_MS: u64 = 20_000;
const MAX_CAPTURE_BYTES: usize = 8 * 1024 * 1024;

pub fn handle_ast_grep(
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

    let Some(binary) = which_ast_grep() else {
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
    let mut child = match Command::new(&binary)
        .args(&args)
        .current_dir(ws.root())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return ToolCallResult::err(format!("Failed to start ast-grep: {}", e)),
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_reader = thread::spawn(move || read_limited(stdout, MAX_CAPTURE_BYTES));
    let stderr_reader = thread::spawn(move || read_limited(stderr, 256 * 1024));

    let timeout = Duration::from_millis(DEFAULT_TIMEOUT_MS);
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (Some(status), false),
            Ok(None) if started.elapsed() >= timeout => {
                let _ = child.kill();
                break (child.wait().ok(), true);
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return ToolCallResult::err(format!("Failed while waiting for ast-grep: {}", e));
            }
        }
    };

    let (stdout_bytes, stdout_truncated) = stdout_reader.join().unwrap_or_default();
    let (stderr_bytes, stderr_truncated) = stderr_reader.join().unwrap_or_default();
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

fn read_limited<R: Read>(pipe: Option<R>, limit: usize) -> (Vec<u8>, bool) {
    let Some(mut pipe) = pipe else {
        return (Vec::new(), false);
    };
    let mut buffer = Vec::with_capacity(limit.min(64 * 1024));
    let mut chunk = [0u8; 8192];
    let mut total_read = 0usize;
    loop {
        match pipe.read(&mut chunk) {
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

fn which_ast_grep() -> Option<String> {
    for candidate in ["sg", "ast-grep"] {
        if Command::new(candidate)
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
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

    #[test]
    fn rejects_empty_pattern() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_ast_grep(&ws, "", None, None, None);
        assert!(!result.success);
    }

    #[test]
    fn rejects_traversal_path() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_ast_grep(&ws, "$A", Some("../outside"), None, None);
        assert!(!result.success);
    }
}
