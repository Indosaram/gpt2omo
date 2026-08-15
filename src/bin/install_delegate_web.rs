use anyhow::{anyhow, Context, Result};
use clap::Parser;
use serde::Serialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "install_delegate_web",
    version,
    about = "Install /delegate-web and delegate-web skill for OpenCode and OMO"
)]
struct Cli {
    /// Override the OpenCode config root. Defaults to $XDG_CONFIG_HOME/opencode or ~/.config/opencode.
    #[arg(long)]
    config_root: Option<PathBuf>,

    /// Override OMO's coding-agent directory. Defaults to $OMO_CODING_AGENT_DIR or ~/.omo/agent.
    #[arg(long)]
    omo_agent_dir: Option<PathBuf>,

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
struct InstalledFile {
    path: String,
    matches: bool,
}

#[derive(Serialize)]
struct InstallResult {
    ok: bool,
    check_only: bool,
    delegate_bin: String,
    open_code_command: InstalledFile,
    open_code_skill: InstalledFile,
    omo_prompt: InstalledFile,
    omo_skill: InstalledFile,
    backups: Vec<String>,
}

struct InstallTarget {
    path: PathBuf,
    content: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_root = cli.config_root.unwrap_or(default_config_root()?);
    let omo_agent_dir = cli.omo_agent_dir.unwrap_or(default_omo_agent_dir()?);
    let delegate_bin = match cli.delegate_bin {
        Some(path) => canonical_file(&path)?,
        None => default_delegate_bin()?,
    };

    let coordinator = render_coordinator_prompt(&delegate_bin);
    let skill = render_skill(&delegate_bin);
    let targets = [
        InstallTarget {
            path: config_root.join("command/delegate-web.md"),
            content: render_open_code_command(&coordinator),
        },
        InstallTarget {
            path: config_root.join("skill/delegate-web/SKILL.md"),
            content: skill.clone(),
        },
        InstallTarget {
            path: omo_agent_dir.join("prompts/delegate-web.md"),
            content: render_omo_prompt(&coordinator),
        },
        InstallTarget {
            path: omo_agent_dir.join("skills/delegate-web/SKILL.md"),
            content: skill,
        },
    ];

    let mut backups = Vec::new();
    if !cli.check {
        for target in &targets {
            if !file_matches(&target.path, &target.content)? {
                if let Some(backup) = backup_existing(&target.path)? {
                    backups.push(backup.to_string_lossy().to_string());
                }
                atomic_write(&target.path, target.content.as_bytes())?;
            }
        }
    }

    let matches = targets
        .iter()
        .map(|target| file_matches(&target.path, &target.content))
        .collect::<Result<Vec<_>>>()?;
    let result = InstallResult {
        ok: matches.iter().all(|value| *value),
        check_only: cli.check,
        delegate_bin: delegate_bin.to_string_lossy().to_string(),
        open_code_command: installed(&targets[0].path, matches[0]),
        open_code_skill: installed(&targets[1].path, matches[1]),
        omo_prompt: installed(&targets[2].path, matches[2]),
        omo_skill: installed(&targets[3].path, matches[3]),
        backups,
    };

    if cli.json {
        println!("{}", serde_json::to_string(&result)?);
    } else if result.ok {
        if cli.check {
            println!("delegate-web installation is current for OpenCode and OMO");
        } else {
            println!("Installed /delegate-web and delegate-web skill for OpenCode and OMO");
            println!("OpenCode command: {}", result.open_code_command.path);
            println!("OpenCode skill: {}", result.open_code_skill.path);
            println!("OMO prompt: {}", result.omo_prompt.path);
            println!("OMO skill: {}", result.omo_skill.path);
        }
    } else {
        return Err(anyhow!(
            "delegate-web installation does not match expected OpenCode/OMO resources"
        ));
    }

    Ok(())
}

fn installed(path: &Path, matches: bool) -> InstalledFile {
    InstalledFile {
        path: path.to_string_lossy().to_string(),
        matches,
    }
}

fn default_config_root() -> Result<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg).join("opencode"));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".config/opencode"))
}

fn default_omo_agent_dir() -> Result<PathBuf> {
    if let Some(path) = env::var_os("OMO_CODING_AGENT_DIR") {
        return Ok(PathBuf::from(path));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(".omo/agent"))
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

fn render_open_code_command(coordinator: &str) -> String {
    format!(
        "---\ndescription: Delegate coding work to one to three parallel ChatGPT Web workers\nsubtask: false\n---\n\n{coordinator}\n"
    )
}

fn render_omo_prompt(coordinator: &str) -> String {
    format!(
        "---\ndescription: Delegate coding work to one to three parallel ChatGPT Web workers\nargument-hint: <task>\n---\n\n{coordinator}\n"
    )
}

fn render_coordinator_prompt(delegate_bin: &Path) -> String {
    let bin = shell_single_quote(&delegate_bin.to_string_lossy());
    format!(
        r#"You are the OMO-side transport coordinator for `/delegate-web`.

USER TASK:
$ARGUMENTS

Do not implement, edit, test, research, or debug the user's coding task yourself. Do not call Task/background/subagent/team tools and do not dispatch Anthropic, OMO, OpenCode, Codex, or other coding agents. Your only job is to decide how many independent ChatGPT Web workers the task merits and dispatch them through the helper below.

## Fan-out policy — hard maximum 3

Choose exactly 1, 2, or 3 workers.

- Default to **1 worker**. Use one when the task is tightly coupled, mostly sequential, touches the same core files/state, or parallelism would create coordination risk.
- Use **2 workers** only when there are two genuinely independent implementation tracks with clear ownership boundaries.
- Use **3 workers** only when there are three genuinely independent tracks (for example separate backend / native UI / web UI modules) that can make useful progress concurrently.
- Never create a fourth worker. The helper independently rejects manifests containing more than 3 tasks.
- Do not split merely to increase parallelism. Each worker task must be independently actionable and contain enough original acceptance criteria to finish its assigned slice.
- OMO owns repository/worktree selection. If the current worktree is correct, omit `workspace` from a task and the helper uses the current git root. If OMO has already selected different worktrees/repos for different tracks, put those exact absolute paths in each task's `workspace` field. Do not create or switch worktrees as part of this command.
- Same physical repository may be assigned to multiple workers when OMO judges the tracks safe to run concurrently. File/index conflicts are OMO's orchestration responsibility, not omo-bridge's.

## Dispatch

Construct one valid JSON manifest with **1–3** entries. Each entry is:

```json
{{"label":"short-label","task":"complete worker instruction","workspace":"/optional/absolute/path"}}
```

`workspace` is optional. Preserve concrete file names, requirements, verification commands, and constraints from USER TASK inside the appropriate worker instruction. Do not invent unrelated work.

Invoke the helper **exactly once** by piping that JSON manifest on stdin:

```bash
cat <<'__OMO_DELEGATE_WEB_BATCH__' | {bin} --batch-stdin --json
{{"tasks":[...1 to 3 task objects...]}}
__OMO_DELEGATE_WEB_BATCH__
```

Do not invoke the helper once per worker. The helper creates fresh ChatGPT Web conversations and starts all workers concurrently.

If helper output has `"ok":true` and `"sent":true`, reply concisely with `parallel_count` and each delegation's `label`, `scope_id`, `workspace`, and `browser_page_id`. Do not continue coding locally.

If the helper fails, report the exact failure and stop. Never fall back to OMO/Anthropic subagents."#,
        bin = bin,
    )
}

fn render_skill(delegate_bin: &Path) -> String {
    format!(
        r#"---
name: delegate-web
description: Use when delegating coding work to ChatGPT Web. Splits a request into at most three independent Web workers, never OMO/Anthropic subagents, and preserves OMO ownership of repo/worktree selection.
compatibility: Requires omo-bridge, omo-relay, Orca browser access, and delegate_to_chatgpt_web.
metadata:
  opencode/slash: "false"
---

# Delegate Web

`/delegate-web` is a transport/orchestration surface, not a local coding workflow.

- The coordinator may inspect the user request only enough to choose **1–3** independent Web tasks.
- Hard maximum: **3 parallel ChatGPT Web workers**. Default to one; split only across genuinely independent tracks.
- Never use OMO Task/background/subagent/team dispatch as a substitute or fallback.
- OMO decides which repository/worktree each task uses. The bridge never creates or chooses worktrees.
- Invoke `{bin} --batch-stdin --json` exactly once with a JSON manifest containing 1–3 tasks.
- The helper creates one fresh ChatGPT Web conversation and one omo-bridge scope per task, then starts the Web prompts concurrently.
- `omo-relay` routes each scope's continuation directly back to its stored ChatGPT browser page.
- More than three tasks must be merged into at most three coherent tracks before dispatch; never queue a fourth Web worker.
- On helper failure, report the error and stop. Never fall back to provider-specific coding agents.
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
    fn coordinator_enforces_three_worker_cap_and_forbids_subagents() {
        let prompt = render_coordinator_prompt(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(prompt.contains("hard maximum 3"));
        assert!(prompt.contains("Choose exactly 1, 2, or 3 workers"));
        assert!(prompt.contains("--batch-stdin --json"));
        assert!(prompt.contains("exactly once"));
        assert!(prompt.contains("Do not call Task/background/subagent/team tools"));
        assert!(prompt.contains("OMO owns repository/worktree selection"));
    }

    #[test]
    fn open_code_command_disables_subtask_mode() {
        let command = render_open_code_command("body");
        assert!(command.contains("subtask: false"));
        assert!(command.contains("body"));
    }

    #[test]
    fn omo_prompt_exposes_delegate_web_arguments() {
        let prompt = render_omo_prompt("body");
        assert!(prompt.contains("argument-hint: <task>"));
        assert!(prompt.contains("body"));
    }

    #[test]
    fn skill_documents_parallel_limit_and_worktree_ownership() {
        let skill = render_skill(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(skill.contains("name: delegate-web"));
        assert!(skill.contains("Hard maximum: **3 parallel ChatGPT Web workers**"));
        assert!(skill.contains("OMO decides which repository/worktree"));
        assert!(skill.contains("opencode/slash: \"false\""));
    }

    #[test]
    fn atomic_install_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prompts/delegate-web.md");
        let expected = render_omo_prompt("body");
        atomic_write(&path, expected.as_bytes()).unwrap();
        assert!(file_matches(&path, &expected).unwrap());
    }

    #[test]
    fn shell_quote_handles_single_quote() {
        assert_eq!(shell_single_quote("/tmp/a'b"), "'/tmp/a'\\''b'");
    }
}
