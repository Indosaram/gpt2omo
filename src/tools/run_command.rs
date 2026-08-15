use crate::security::Workspace;
use crate::tools::ToolCallResult;
use std::io::Read;
use std::path::{Component, Path};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const MAX_CAPTURE_BYTES: usize = 120_000;

pub fn handle_run_command(ws: &Workspace, cmd_str: &str, timeout_ms: u64) -> ToolCallResult {
    let parts = match split_command_line(cmd_str) {
        Ok(parts) if !parts.is_empty() => parts,
        Ok(_) => return ToolCallResult::err("Empty command string"),
        Err(e) => return ToolCallResult::err(e),
    };

    let binary = &parts[0];
    let args = &parts[1..];

    // Deliberately no shell or general-purpose interpreter is exposed here. This runner is for
    // repository build/test/verification and narrowly-scoped git operations only.
    let allowed = ["cargo", "npm", "git", "pytest", "vitest", "go", "make"];
    if !allowed.contains(&binary.as_str()) {
        return ToolCallResult::err(format!(
            "Command '{}' is not in the allowed execution whitelist",
            binary
        ));
    }

    if let Err(e) = validate_command_shape(binary, args) {
        return ToolCallResult::err(e);
    }
    if let Err(e) = validate_obvious_path_escapes(ws, args) {
        return ToolCallResult::err(e);
    }

    let start = Instant::now();
    let mut child = match Command::new(binary)
        .args(args)
        .current_dir(ws.root())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => return ToolCallResult::err(format!("Failed to execute command: {}", e)),
    };

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_reader = thread::spawn(move || read_pipe(stdout));
    let stderr_reader = thread::spawn(move || read_pipe(stderr));

    let timeout = Duration::from_millis(timeout_ms.max(1));
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (Some(status), false),
            Ok(None) if start.elapsed() >= timeout => {
                let _ = child.kill();
                let status = child.wait().ok();
                break (status, true);
            }
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return ToolCallResult::err(format!("Failed while waiting for command: {}", e));
            }
        }
    };

    let stdout_bytes = stdout_reader.join().unwrap_or_default();
    let stderr_bytes = stderr_reader.join().unwrap_or_default();
    let (stdout, stdout_truncated) = truncate_output(&stdout_bytes);
    let (stderr, stderr_truncated) = truncate_output(&stderr_bytes);
    let duration_ms = start.elapsed().as_millis() as u64;
    let exit_code = status.as_ref().and_then(|s| s.code());
    let command_success = status.as_ref().is_some_and(|s| s.success()) && !timed_out;

    ToolCallResult::ok(serde_json::json!({
        "command": cmd_str,
        "success": command_success,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "duration_ms": duration_ms
    }))
}

fn validate_command_shape(binary: &str, args: &[String]) -> std::result::Result<(), String> {
    let first = args.first().map(String::as_str).unwrap_or("");
    match binary {
        "cargo" => {
            if !["test", "check", "clippy", "build", "fmt"].contains(&first) {
                return Err(format!("cargo subcommand '{}' is not allowed", first));
            }
        }
        "npm" => match first {
            "test" => {}
            "run" => {
                let script = args.get(1).map(String::as_str).unwrap_or("");
                if !["test", "build", "lint", "typecheck"].contains(&script) {
                    return Err(format!("npm run script '{}' is not allowed", script));
                }
            }
            _ => return Err(format!("npm subcommand '{}' is not allowed", first)),
        },
        "git" => {
            if first == "--version" {
                return Ok(());
            }
            if ![
                "status",
                "diff",
                "add",
                "commit",
                "rev-parse",
                "log",
                "show",
            ]
            .contains(&first)
            {
                return Err(format!("git subcommand '{}' is not allowed", first));
            }
        }
        "go" => {
            if !["test", "vet"].contains(&first) {
                return Err(format!("go subcommand '{}' is not allowed", first));
            }
        }
        "make" => {
            if !["test", "check"].contains(&first) {
                return Err(format!("make target '{}' is not allowed", first));
            }
        }
        "pytest" | "vitest" => {}
        _ => {
            return Err(format!(
                "Unsupported command '{}'; whitelist is inconsistent",
                binary
            ))
        }
    }
    Ok(())
}

fn read_pipe<R: Read>(pipe: Option<R>) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(mut pipe) = pipe {
        let _ = pipe.read_to_end(&mut bytes);
    }
    bytes
}

fn truncate_output(bytes: &[u8]) -> (String, bool) {
    if bytes.len() <= MAX_CAPTURE_BYTES {
        return (String::from_utf8_lossy(bytes).to_string(), false);
    }

    let half = MAX_CAPTURE_BYTES / 2;
    let head = String::from_utf8_lossy(&bytes[..half]);
    let tail = String::from_utf8_lossy(&bytes[bytes.len() - half..]);
    (
        format!(
            "{}\n\n...[output truncated by omo-bridge; {} bytes omitted]...\n\n{}",
            head,
            bytes.len().saturating_sub(MAX_CAPTURE_BYTES),
            tail
        ),
        true,
    )
}

fn split_command_line(input: &str) -> std::result::Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut escaped = false;

    for ch in input.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        if ch == '\\' && quote != Some('\'') {
            escaped = true;
            continue;
        }

        match quote {
            Some(q) if ch == q => quote = None,
            Some(_) => current.push(ch),
            None if ch == '\'' || ch == '"' => quote = Some(ch),
            None if ch.is_whitespace() => {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            }
            None => current.push(ch),
        }
    }

    if escaped {
        return Err("Command ends with an incomplete escape".into());
    }
    if quote.is_some() {
        return Err("Command contains an unterminated quote".into());
    }
    if !current.is_empty() {
        parts.push(current);
    }

    Ok(parts)
}

fn validate_obvious_path_escapes(
    ws: &Workspace,
    args: &[String],
) -> std::result::Result<(), String> {
    for arg in args {
        if arg.contains('\0') {
            return Err("Command argument contains a NUL byte".into());
        }

        validate_path_candidate(ws, arg)?;
        if let Some((_, value)) = arg.split_once('=') {
            if !value.is_empty() {
                validate_path_candidate(ws, value)?;
            }
        }
    }
    Ok(())
}

fn validate_path_candidate(ws: &Workspace, value: &str) -> std::result::Result<(), String> {
    let path = Path::new(value);
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "Parent-directory traversal is forbidden in command argument: {}",
            value
        ));
    }

    if path.is_absolute() {
        let canonical = dunce::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        if !canonical.starts_with(ws.root()) {
            return Err(format!(
                "Absolute path outside the mounted workspace is forbidden: {}",
                value
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_run_command_whitelist_and_subcommands() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let ok_res = handle_run_command(&ws, "git --version", 5000);
        assert!(ok_res.success);
        assert!(ok_res.data.unwrap()["success"].as_bool().unwrap());

        let denied_res = handle_run_command(&ws, "python3 -c \"print('escape')\"", 5000);
        assert!(!denied_res.success);
        assert!(denied_res
            .error
            .unwrap()
            .contains("not in the allowed execution whitelist"));

        let denied_git = handle_run_command(&ws, "git config --global user.name attacker", 5000);
        assert!(!denied_git.success);
        assert!(denied_git.error.unwrap().contains("git subcommand"));
    }

    #[test]
    fn test_command_parser_supports_quotes() {
        let parts = split_command_line("git commit -m \"hello world\"").unwrap();
        assert_eq!(parts, vec!["git", "commit", "-m", "hello world"]);
    }

    #[test]
    fn test_absolute_path_outside_workspace_is_denied() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_run_command(&ws, "git status --git-dir=/tmp/outside", 5000);
        assert!(!result.success);
        assert!(result
            .error
            .unwrap()
            .contains("outside the mounted workspace"));
    }

    #[test]
    fn test_parent_traversal_inside_option_value_is_denied() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_run_command(&ws, "cargo test --manifest-path=../Cargo.toml", 5000);
        assert!(!result.success);
        assert!(result.error.unwrap().contains("Parent-directory traversal"));
    }

    #[test]
    fn test_timeout_is_enforced() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 1\n").unwrap();
        let result = handle_run_command(&ws, "make test", 50);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["timed_out"], true);
        assert_eq!(data["success"], false);
    }
}
