use crate::security::Workspace;
use crate::tools::command_manager::CommandManager;
use crate::tools::ToolCallResult;
use std::path::{Component, Path};

const LEGACY_SCOPE_ID: &str = "00000000-0000-4000-8000-000000000000";

#[derive(Clone, Debug)]
pub(crate) struct PreparedCommand {
    pub(crate) binary: String,
    pub(crate) args: Vec<String>,
}

pub(crate) fn prepare_command(
    ws: &Workspace,
    cmd_str: &str,
) -> std::result::Result<PreparedCommand, String> {
    let parts = split_command_line(cmd_str)?;
    if parts.is_empty() {
        return Err("Empty command string".into());
    }

    let binary = &parts[0];
    let args = &parts[1..];

    // Deliberately no shell or general-purpose interpreter is exposed here. This runner is for
    // repository build/test/verification and narrowly-scoped git operations only.
    let allowed = ["cargo", "npm", "git", "pytest", "vitest", "go", "make"];
    if !allowed.contains(&binary.as_str()) {
        return Err(format!(
            "Command '{}' is not in the allowed execution whitelist",
            binary
        ));
    }

    validate_command_shape(binary, args)?;
    validate_obvious_path_escapes(ws, args)?;

    Ok(PreparedCommand {
        binary: binary.clone(),
        args: args.to_vec(),
    })
}

/// Compatibility wrapper for in-process callers. The MCP server owns a single shared
/// `CommandManager`; callers that need polling/cancellation should use that manager directly.
pub fn handle_run_command(ws: &Workspace, cmd_str: &str, timeout_ms: u64) -> ToolCallResult {
    CommandManager::new().run_command(ws, LEGACY_SCOPE_ID, cmd_str, timeout_ms, None)
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
        assert!(ok_res.data.unwrap()["command_success"].as_bool().unwrap());

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
        let result = prepare_command(&ws, "git status --git-dir=/tmp/outside");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("outside the mounted workspace"));
    }

    #[test]
    fn test_parent_traversal_inside_option_value_is_denied() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = prepare_command(&ws, "cargo test --manifest-path=../Cargo.toml");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Parent-directory traversal"));
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
        assert_eq!(data["command_success"], false);
    }
}
