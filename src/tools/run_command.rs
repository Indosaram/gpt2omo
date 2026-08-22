use crate::security::Workspace;
use crate::tools::command_manager::CommandManager;
use crate::tools::ToolCallResult;
use std::path::{Component, Path, PathBuf};

const LEGACY_SCOPE_ID: &str = "00000000-0000-4000-8000-000000000000";

#[derive(Clone, Debug)]
pub struct PreparedCommand {
    pub binary: String,
    pub args: Vec<String>,
}

pub const ALLOWED_BINARIES: &[&str] = &[
    "cargo", "rustc", "npm", "pnpm", "yarn", "bun", "node", "python", "python3", "pytest", "uv",
    "go", "make", "git", "vitest", "jest", "tsc", "biome", "ruff", "sg", "ast-grep",
];

pub const BLOCKED_SHELL_WRAPPERS: &[&str] = &[
    "sh",
    "bash",
    "zsh",
    "fish",
    "dash",
    "ksh",
    "csh",
    "tcsh",
    "env",
    "xargs",
    "eval",
    "perl",
    "ruby",
    "awk",
    "script",
    "sudo",
    "su",
    "doas",
    "cmd",
    "cmd.exe",
    "powershell",
    "powershell.exe",
    "pwsh",
    "pwsh.exe",
];

fn extract_binary_name(binary: &str) -> &str {
    Path::new(binary)
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or(binary)
}

fn has_explicit_path(binary: &str) -> bool {
    let path = Path::new(binary);
    path.is_absolute() || path.components().count() > 1
}

pub(crate) fn is_arbitrary_commands_allowed() -> bool {
    std::env::args().any(|arg| arg == "--allow-arbitrary-commands")
        || std::env::var("OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        || std::env::var("OMO_ALLOW_ARBITRARY_COMMANDS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        || std::env::var("ALLOW_ARBITRARY_COMMANDS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
}

fn get_explicit_whitelisted_binaries() -> Vec<String> {
    if let Ok(extra) =
        std::env::var("OMO_ALLOWED_BINARIES").or_else(|_| std::env::var("ALLOWED_BINARIES"))
    {
        extra
            .split([',', ':', ';', ' '])
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        Vec::new()
    }
}

pub fn validate_binary(binary: &str) -> std::result::Result<(), String> {
    let arbitrary = is_arbitrary_commands_allowed();
    let explicit = get_explicit_whitelisted_binaries();
    validate_binary_with_policy(binary, arbitrary, &explicit)
}

pub(crate) fn validate_binary_with_policy(
    binary: &str,
    allow_arbitrary: bool,
    explicit_whitelist: &[String],
) -> std::result::Result<(), String> {
    if allow_arbitrary {
        return Ok(());
    }

    let bin_name = extract_binary_name(binary);
    if bin_name.is_empty() {
        return Err("Empty command binary".into());
    }

    let bin_lower = bin_name.to_ascii_lowercase();
    let bin_clean = bin_lower.strip_suffix(".exe").unwrap_or(&bin_lower);

    if BLOCKED_SHELL_WRAPPERS
        .iter()
        .any(|&blocked| blocked == bin_clean || blocked == bin_lower)
    {
        return Err(format!(
            "Shell wrapper or indirect command runner '{}' is blocked for security reasons",
            bin_name
        ));
    }

    if has_explicit_path(binary) {
        if explicit_whitelist.iter().any(|allowed| allowed == binary) {
            return Ok(());
        }
        return Err(format!(
            "explicit command path '{}' is blocked; use a PATH-resolved whitelisted binary or whitelist this exact path with OMO_ALLOWED_BINARIES",
            binary
        ));
    }

    if explicit_whitelist.iter().any(|w| {
        !has_explicit_path(w)
            && (w.eq_ignore_ascii_case(bin_clean)
                || w.eq_ignore_ascii_case(&bin_lower)
                || w.eq_ignore_ascii_case(bin_name))
    }) {
        return Ok(());
    }

    if ALLOWED_BINARIES
        .iter()
        .any(|&allowed| allowed == bin_clean || allowed == bin_lower)
    {
        return Ok(());
    }

    Err(format!(
        "command binary '{}' is not in the allowed execution whitelist",
        bin_name
    ))
}

pub fn prepare_command(
    ws: &Workspace,
    cmd_str: &str,
) -> std::result::Result<PreparedCommand, String> {
    prepare_command_with_policy(ws, cmd_str, is_arbitrary_commands_allowed())
}

pub(crate) fn prepare_command_with_policy(
    ws: &Workspace,
    cmd_str: &str,
    allow_arbitrary: bool,
) -> std::result::Result<PreparedCommand, String> {
    let parts = split_command_line(cmd_str)?;
    if parts.is_empty() {
        return Err("Empty command string".into());
    }

    let binary = &parts[0];
    let args = &parts[1..];

    validate_binary_with_policy(
        binary,
        allow_arbitrary,
        &get_explicit_whitelisted_binaries(),
    )?;

    if !allow_arbitrary {
        let bin_name = extract_binary_name(binary);
        let bin_lower = bin_name.to_ascii_lowercase();
        let bin_clean = bin_lower.strip_suffix(".exe").unwrap_or(&bin_lower);
        if bin_clean == "git" {
            validate_git_args(args)?;
        }
        validate_obvious_path_escapes(ws, args)?;
    }

    Ok(PreparedCommand {
        binary: binary.clone(),
        args: args.to_vec(),
    })
}

fn validate_git_args(args: &[String]) -> std::result::Result<(), String> {
    for arg in args {
        let trimmed = arg.trim();
        if trimmed == "-c"
            || trimmed.starts_with("-c")
            || trimmed == "--exec-path"
            || trimmed.starts_with("--exec-path=")
            || trimmed == "--upload-pack"
            || trimmed.starts_with("--upload-pack=")
            || trimmed == "--receive-pack"
            || trimmed.starts_with("--receive-pack=")
            || trimmed == "--config-env"
            || trimmed.starts_with("--config-env=")
        {
            return Err(format!(
                "git option '{}' is forbidden for security reasons",
                arg
            ));
        }
    }
    Ok(())
}

/// Compatibility wrapper for in-process callers. The MCP server owns a single shared
/// `CommandManager`; callers that need polling/cancellation should use that manager directly.
pub fn handle_run_command(ws: &Workspace, cmd_str: &str, timeout_ms: u64) -> ToolCallResult {
    CommandManager::new().run_command(ws, LEGACY_SCOPE_ID, cmd_str, timeout_ms, None)
}

// Command line parsing

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

fn canonicalize_nearest(path: &Path) -> PathBuf {
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut current = path.to_path_buf();
    loop {
        match dunce::canonicalize(&current) {
            Ok(resolved) => {
                let mut result = resolved;
                for component in tail.iter().rev() {
                    result.push(component);
                }
                return result;
            }
            Err(_) => match (current.parent(), current.file_name()) {
                (Some(parent), Some(name)) if parent != current => {
                    tail.push(name.to_os_string());
                    current = parent.to_path_buf();
                }
                _ => return path.to_path_buf(),
            },
        }
    }
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
        let canonical = canonicalize_nearest(path);
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
    fn test_run_command_execution() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let ok_res = handle_run_command(&ws, "git --version", 5000);
        assert!(ok_res.success);
        assert!(ok_res.data.unwrap()["command_success"].as_bool().unwrap());
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
    #[cfg(unix)]
    fn test_absolute_path_via_symlink_to_nonexistent_leaf_is_denied() {
        let dir = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let link = dir.path().join("escape-link");
        std::os::unix::fs::symlink(outside.path(), &link).unwrap();

        let candidate = link.join("not-yet-created.txt");
        let error = validate_path_candidate(&ws, candidate.to_str().unwrap()).unwrap_err();
        assert!(error.contains("outside the mounted workspace"));
    }

    #[test]
    fn test_nonexistent_absolute_path_inside_workspace_is_allowed() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let candidate = dir.path().join("brand-new-file.txt");
        assert!(validate_path_candidate(&ws, candidate.to_str().unwrap()).is_ok());
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

    #[test]
    fn test_allowed_binaries_pass_validation() {
        let expected_allowed = [
            "cargo", "rustc", "npm", "pnpm", "yarn", "bun", "node", "python", "python3", "pytest",
            "uv", "go", "make", "git", "vitest", "jest", "tsc", "biome", "ruff", "sg", "ast-grep",
        ];
        for bin in expected_allowed {
            assert!(
                validate_binary(bin).is_ok(),
                "Expected binary '{}' to be allowed",
                bin
            );
            let explicit_path = format!("/tmp/{}", bin);
            assert!(
                validate_binary(&explicit_path).is_err(),
                "Expected untrusted explicit binary path '{}' to be rejected",
                explicit_path
            );
        }
    }

    #[test]
    fn test_shell_wrappers_are_blocked() {
        let shell_wrappers = [
            "sh", "bash", "zsh", "fish", "dash", "ksh", "csh", "tcsh", "env", "xargs", "eval",
            "perl", "ruby", "awk", "script", "sudo", "su", "doas",
        ];
        for shell in shell_wrappers {
            let res = validate_binary(shell);
            assert!(res.is_err(), "Expected shell '{}' to be blocked", shell);
            assert!(
                res.unwrap_err().contains("Shell wrapper"),
                "Expected shell wrapper error for '{}'",
                shell
            );

            let res_path = validate_binary(&format!("/bin/{}", shell));
            assert!(
                res_path.is_err(),
                "Expected shell path '/bin/{}' to be blocked",
                shell
            );
            assert!(
                res_path.unwrap_err().contains("Shell wrapper"),
                "Expected shell wrapper error for '/bin/{}'",
                shell
            );
        }
    }

    #[test]
    fn test_git_escape_options_are_blocked() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let forbidden_git_cmds = [
            "git -c core.pager=cat log",
            "git -c=core.pager=cat log",
            "git -cfoo=bar status",
            "git --exec-path=/tmp status",
            "git --exec-path status",
            "git --upload-pack=/bin/sh fetch",
            "git --receive-pack=/bin/sh push",
            "git --config-env=FOO=BAR status",
        ];

        for cmd in forbidden_git_cmds {
            let res = prepare_command(&ws, cmd);
            assert!(res.is_err(), "Expected git command '{}' to be blocked", cmd);
            assert!(
                res.unwrap_err().contains("git option"),
                "Expected git option error for '{}'",
                cmd
            );
        }

        // Benign git commands succeed
        assert!(prepare_command(&ws, "git status").is_ok());
        assert!(prepare_command(&ws, "git diff HEAD").is_ok());
        assert!(prepare_command(&ws, "git log --oneline").is_ok());
        assert!(prepare_command(&ws, "git commit -m \"fix: update parser\"").is_ok());
    }

    #[test]
    fn test_prepare_command_blocks_shell_wrappers_and_disallowed_binaries() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let sh_res = prepare_command(&ws, "sh -c 'echo 123'");
        assert!(sh_res.is_err());
        assert!(sh_res.unwrap_err().contains("Shell wrapper"));

        let bash_res = prepare_command(&ws, "/bin/bash script.sh");
        assert!(bash_res.is_err());
        assert!(bash_res.unwrap_err().contains("Shell wrapper"));

        let env_res = prepare_command(&ws, "env FOO=bar cargo test");
        assert!(env_res.is_err());
        assert!(env_res.unwrap_err().contains("Shell wrapper"));

        let xargs_res = prepare_command(&ws, "xargs -n 1 cargo");
        assert!(xargs_res.is_err());
        assert!(xargs_res.unwrap_err().contains("Shell wrapper"));

        let eval_res = prepare_command(&ws, "eval echo 123");
        assert!(eval_res.is_err());
        assert!(eval_res.unwrap_err().contains("Shell wrapper"));

        let perl_res = prepare_command(&ws, "perl -e 'print 1'");
        assert!(perl_res.is_err());
        assert!(perl_res.unwrap_err().contains("Shell wrapper"));

        let ruby_res = prepare_command(&ws, "ruby -e 'puts 1'");
        assert!(ruby_res.is_err());
        assert!(ruby_res.unwrap_err().contains("Shell wrapper"));

        let awk_res = prepare_command(&ws, "awk '{print $1}'");
        assert!(awk_res.is_err());
        assert!(awk_res.unwrap_err().contains("Shell wrapper"));

        let script_res = prepare_command(&ws, "script output.txt");
        assert!(script_res.is_err());
        assert!(script_res.unwrap_err().contains("Shell wrapper"));

        let curl_res = prepare_command(&ws, "curl https://example.com");
        assert!(curl_res.is_err());
        assert_eq!(
            curl_res.unwrap_err(),
            "command binary 'curl' is not in the allowed execution whitelist"
        );

        let rm_res = prepare_command(&ws, "rm -rf foo");
        assert!(rm_res.is_err());
        assert_eq!(
            rm_res.unwrap_err(),
            "command binary 'rm' is not in the allowed execution whitelist"
        );
    }

    #[test]
    fn test_prepare_command_allows_whitelisted_binaries() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        assert!(prepare_command(&ws, "cargo test").is_ok());
        assert!(prepare_command(&ws, "git status").is_ok());
        assert!(prepare_command(&ws, "pytest tests/").is_ok());
        assert!(prepare_command(&ws, "python3 -m unittest").is_ok());
        assert!(prepare_command(&ws, "biome check").is_ok());
        assert!(prepare_command(&ws, "ruff check").is_ok());
        assert!(prepare_command(&ws, "sg -p pattern").is_ok());
        assert!(prepare_command(&ws, "ast-grep --pattern foo").is_ok());
    }

    #[test]
    fn test_policy_arbitrary_and_explicit_whitelist() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        // Arbitrary allowed bypasses checks
        assert!(validate_binary_with_policy("bash", true, &[]).is_ok());
        assert!(validate_binary_with_policy("curl", true, &[]).is_ok());
        assert!(validate_binary_with_policy("sh", true, &[]).is_ok());

        assert!(prepare_command_with_policy(&ws, "bash -c 'echo 123'", true).is_ok());
        assert!(prepare_command_with_policy(&ws, "curl https://example.com", true).is_ok());
        assert!(prepare_command_with_policy(&ws, "git -c core.pager=cat status", true).is_ok());

        // Explicit whitelist allows specific custom basenames and exact full paths.
        let explicit = vec![
            "custom_runner".to_string(),
            "my-tool".to_string(),
            "/usr/local/bin/custom_runner".to_string(),
        ];
        assert!(validate_binary_with_policy("custom_runner", false, &explicit).is_ok());
        assert!(validate_binary_with_policy("my-tool", false, &explicit).is_ok());
        assert!(
            validate_binary_with_policy("/usr/local/bin/custom_runner", false, &explicit).is_ok()
        );
        assert!(validate_binary_with_policy("/tmp/custom_runner", false, &explicit).is_err());
        assert!(validate_binary_with_policy("unlisted_tool", false, &explicit).is_err());
    }
}
