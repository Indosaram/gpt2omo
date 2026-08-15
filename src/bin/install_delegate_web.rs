use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const TASK_DELIMITER: &str = "__OMO_DELEGATE_WEB_TASK_7B6F4F98C0E14D5A__";

#[derive(Parser, Debug)]
#[command(
    name = "install_delegate_web",
    version,
    about = "Install the global OpenCode /delegate-web command and delegate-web skill"
)]
struct Cli {
    /// Override the OpenCode config root. Defaults to $XDG_CONFIG_HOME/opencode or ~/.config/opencode.
    #[arg(long)]
    config_root: Option<PathBuf>,

    /// Override the delegate_to_chatgpt_web binary path.
    #[arg(long)]
    delegate_bin: Option<PathBuf>,

    /// Verify the installed files exactly match this installer without changing anything.
    #[arg(long)]
    check: bool,

    /// Emit compact JSON.
    #[arg(long)]
    json: bool,
}

#[derive(Serialize)]
struct InstallResult {
    ok: bool,
    check_only: bool,
    command_path: String,
    skill_path: String,
    delegate_bin: String,
    command_matches: bool,
    skill_matches: bool,
    backups: Vec<String>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_root = match cli.config_root {
        Some(path) => path,
        None => default_config_root()?,
    };
    let delegate_bin = match cli.delegate_bin {
        Some(path) => canonical_file(&path)?,
        None => default_delegate_bin()?,
    };

    let command_path = config_root.join("command/delegate-web.md");
    let skill_path = config_root.join("skill/delegate-web/SKILL.md");
    let command = render_command(&delegate_bin);
    let skill = render_skill(&delegate_bin);

    let mut backups = Vec::new();
    let mut command_matches = file_matches(&command_path, &command)?;
    let mut skill_matches = file_matches(&skill_path, &skill)?;

    if !cli.check {
        if !command_matches {
            if let Some(backup) = backup_existing(&command_path)? {
                backups.push(backup.to_string_lossy().to_string());
            }
            atomic_write(&command_path, command.as_bytes())?;
        }
        if !skill_matches {
            if let Some(backup) = backup_existing(&skill_path)? {
                backups.push(backup.to_string_lossy().to_string());
            }
            atomic_write(&skill_path, skill.as_bytes())?;
        }
        command_matches = file_matches(&command_path, &command)?;
        skill_matches = file_matches(&skill_path, &skill)?;
    }

    let result = InstallResult {
        ok: command_matches && skill_matches,
        check_only: cli.check,
        command_path: command_path.to_string_lossy().to_string(),
        skill_path: skill_path.to_string_lossy().to_string(),
        delegate_bin: delegate_bin.to_string_lossy().to_string(),
        command_matches,
        skill_matches,
        backups,
    };

    if cli.json {
        println!("{}", serde_json::to_string(&result)?);
    } else if result.ok {
        if cli.check {
            println!("delegate-web installation is current");
        } else {
            println!("Installed /delegate-web and delegate-web skill");
            println!("command: {}", result.command_path);
            println!("skill: {}", result.skill_path);
        }
    } else {
        return Err(anyhow!(
            "delegate-web installation does not match expected files"
        ));
    }

    Ok(())
}

fn default_config_root() -> Result<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg).join("opencode"));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".config/opencode"))
}

fn default_delegate_bin() -> Result<PathBuf> {
    let exe = env::current_exe().context("failed to locate installer executable")?;
    let parent = exe
        .parent()
        .ok_or_else(|| anyhow!("installer executable has no parent directory"))?;
    canonical_file(&parent.join("delegate_to_chatgpt_web"))
}

fn canonical_file(path: &Path) -> Result<PathBuf> {
    let path = dunce::canonicalize(path)
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    if !path.is_file() {
        return Err(anyhow!("not a file: {}", path.display()));
    }
    Ok(path)
}

fn render_command(delegate_bin: &Path) -> String {
    let bin = shell_single_quote(&delegate_bin.to_string_lossy());
    format!(
        r#"---
description: Delegate this coding task directly to ChatGPT Web through omo-bridge
subtask: false
---

This is a transport command, not an OMO coding task.
Do NOT analyze or implement the requested task in OMO.
Do NOT spawn Task/background agents, subagents, or provider-specific agents.

The delegation helper is executed directly below. Its output is authoritative:

!`cat <<'{delimiter}' | {bin} --stdin --json
$ARGUMENTS
{delimiter}`

If the command output is JSON with `"ok":true` and `"sent":true`, reply only with a concise delegation confirmation containing `scope_id`, `workspace`, and `terminal` from that JSON. Do not dispatch any other agents and do not implement the task locally.

If the command fails, report that failure and stop. Never fall back to OMO/Anthropic subagents.
"#,
        delimiter = TASK_DELIMITER,
        bin = bin,
    )
}

fn render_skill(delegate_bin: &Path) -> String {
    format!(
        r#"---
name: delegate-web
description: Directly delegate a coding task to ChatGPT Web through omo-bridge. Use when the user asks to delegate, hand off, or send coding work to ChatGPT Web. Never replace this flow with OMO subagents or Anthropic background agents.
compatibility: Requires the omo-bridge daemon and omo-relay plus the delegate_to_chatgpt_web helper.
metadata:
  opencode/slash: "false"
---

# Delegate to ChatGPT Web

This skill is a transport policy. It does not perform the coding task itself.

When the user asks to delegate work to ChatGPT Web:

1. Do not spawn OMO Task/background agents or provider-specific subagents.
2. Keep OMO's current repository/worktree selection as-is; worktree creation/selection is OMO's responsibility.
3. Prefer the installed `/delegate-web <task>` command, which directly executes the delegation helper with `subtask: false`.
4. If a manual fallback is necessary, invoke `{bin} --stdin --json` exactly once and pass the user's complete task on stdin. Do not analyze the task first.
5. On a successful JSON result (`ok=true`, `sent=true`), report only `scope_id`, `workspace`, and `terminal`. ChatGPT Web owns implementation from that point.
6. On failure, report the helper error. Never fall back to Anthropic/OMO subagents.

The helper automatically discovers the current git worktree root, creates an isolated omo-bridge scope, resolves the ChatGPT Web Orca terminal, and sends the task there.
"#,
        bin = delegate_bin.display(),
    )
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn file_matches(path: &Path, expected: &str) -> Result<bool> {
    match fs::read(path) {
        Ok(bytes) => Ok(bytes == expected.as_bytes()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn backup_existing(path: &Path) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("invalid file name: {}", path.display()))?;
    let backup = path.with_file_name(format!("{}.bak-{}", file_name, stamp));
    fs::copy(path, &backup).with_context(|| format!("failed to backup {}", path.display()))?;
    Ok(Some(backup))
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let temp = parent.join(format!(".delegate-web-{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&temp, bytes).with_context(|| format!("failed to write {}", temp.display()))?;
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error).with_context(|| format!("failed to install {}", path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn command_disables_subtasks_and_calls_delegate_helper() {
        let command = render_command(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(command.contains("subtask: false"));
        assert!(command.contains("--stdin --json"));
        assert!(command.contains("$ARGUMENTS"));
        assert!(command.contains("Never fall back to OMO/Anthropic subagents"));
    }

    #[test]
    fn skill_explicitly_forbids_subagent_fallback() {
        let skill = render_skill(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(skill.contains("name: delegate-web"));
        assert!(skill.contains("opencode/slash: \"false\""));
        assert!(skill.contains("Do not spawn OMO Task/background agents"));
        assert!(skill.contains("worktree creation/selection is OMO's responsibility"));
    }

    #[test]
    fn atomic_install_and_check_round_trip() {
        let dir = tempdir().unwrap();
        let command_path = dir.path().join("command/delegate-web.md");
        let expected = render_command(Path::new("/tmp/delegate_to_chatgpt_web"));
        atomic_write(&command_path, expected.as_bytes()).unwrap();
        assert!(file_matches(&command_path, &expected).unwrap());
    }

    #[test]
    fn shell_quote_handles_single_quote() {
        assert_eq!(shell_single_quote("/tmp/a'b"), "'/tmp/a'\\''b'");
    }
}
