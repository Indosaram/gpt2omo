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
    omo_bridge::load_dotenv_if_present();
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
        "---\ndescription: Delegate coding work to one or two parallel ChatGPT Web workers\nsubtask: false\n---\n\n{coordinator}\n"
    )
}

fn render_omo_prompt(coordinator: &str) -> String {
    format!(
        "---\ndescription: Delegate coding work to one or two parallel ChatGPT Web workers\nargument-hint: <task>\n---\n\n{coordinator}\n"
    )
}

fn render_coordinator_prompt(delegate_bin: &Path) -> String {
    let bin = shell_single_quote(&delegate_bin.to_string_lossy());
    format!(
        r#"You are the OMO-side transport coordinator for `/delegate-web`.

USER TASK:
$ARGUMENTS

Do not implement, edit, test, research, or debug the user's coding task yourself. Do not call Task/background/subagent/team tools and do not dispatch Anthropic, OMO, OpenCode, Codex, or other coding agents. Your only job is to choose a fresh Web delegation, resume an exact retained Web session, or explicitly close a retained session, then invoke the helper.

## Fresh fan-out policy — hard maximum 2

Choose exactly 1 or 2 workers for a fresh delegation.

- Default to **1 worker**. Use one when the task is tightly coupled, mostly sequential, touches the same core files/state, or parallelism would create coordination risk.
- Use **2 workers** only for two genuinely independent implementation tracks with clear ownership boundaries.
- Never create a third worker. The helper independently hard-rejects more than 2 tasks.
- Do not split merely to increase parallelism.
- OMO owns repository/worktree selection. The bridge never creates or switches worktrees.

## Fresh dispatch

Construct one JSON manifest with **1–2** entries and invoke the helper once:

```bash
cat <<'__OMO_DELEGATE_WEB_BATCH__' | {bin} --batch-stdin --json
{{"tasks":[{{"label":"short-label","task":"complete worker instruction","workspace":"/optional/absolute/path"}}]}}
__OMO_DELEGATE_WEB_BATCH__
```

### Session Retention Policy

- **Dispatch with `IDLE_RETAINED` by default** (120 min lease).
- **Do NOT pass `--close-on-terminal` on initial dispatch**: Keep the session retained until you receive, inspect, and extract the complete output/plan/changes without risking premature tab closure.
- `--close-on-terminal` is strictly reserved for throwaway executions where the output is not needed.

## When and How to Close a Session

- **Explicitly close the session when it is no longer needed**:
  - Once you have extracted and verified the full plan/review/code results,
  - When the task is complete and accepted, or
  - When switching to a new/unrelated task and no further follow-up prompts will be sent to this worker.
  - Run:
    ```bash
    {bin} --close-scope '<exact-scope-id>' --json
    ```
- Do not leave inactive finished tabs open indefinitely; clean them up once their results are secured and the task is done.
- `LOST` sessions are automatically cleaned up.

## Resume an existing retained Web conversation

**CRITICAL RULE FOR RESUME**:
- **Never hijack an unrelated session**: `--resume-scope` is ONLY for **true follow-up work belonging to that exact same previous task** (e.g. fixing a failing test for the code just written, continuing part 2 of the same plan, or answering a follow-up question on that exact conversation).
- **NEVER resume a past, completed, or unrelated session for a brand new topic or task**: If the user asks for a new task (e.g. comparing Maho vs Aside, building a new feature, doing a different review), **ALWAYS dispatch a fresh worker**. Do NOT grab a random scope_id from `scopes-*/` or a past code-review tab just because fresh creation hit a timing glitch.
- When you have concrete follow-up work for that exact conversation, reuse that exact `scope_id`:

```bash
cat <<'__OMO_DELEGATE_WEB_RESUME__' | {bin} --resume-scope '<exact-scope-id>' --stdin --json
<complete follow-up task>
__OMO_DELEGATE_WEB_RESUME__
```

Do not create a fresh tab, do not pass `--workspace`, and do not invent/substitute a scope id. Resume consumes the current idle lease, verifies the exact stored `browser_page_id`, and starts the next internal lifecycle generation in the same ChatGPT conversation. Its terminal result is retained again automatically with a fresh lease.

A previously `COMPLETED` session may create a new follow-up plan. A previously `BLOCKED` session reopens only blocked items as `in_progress` and preserves blocker notes/context.

If the lease expired or the exact browser page is dead/wrong, the helper fails closed for resume and never silently opens a replacement conversation.

## Explicitly close a retained session

```bash
{bin} --close-scope '<exact-scope-id>' --json
```

This is a browser-session lifecycle decision, not a task-completion signal. Do not send a coding task in the same invocation.

## Automatic TTL cleanup

`omo-relay` runs a periodic retained-session janitor. Default lease is 120 minutes; `OMO_WEB_SESSION_TTL_MINUTES` can override it. Helper invocations also opportunistically reap expired scopes. Scope-level filesystem locks serialize resume/close/GC so a janitor cannot close a scope that is simultaneously being resumed.

TTL is a safety net, not evidence that a task completed and not a substitute for an explicit close when the user requests one.

## Rate Limiting & Safety Contract — Strictly No Bypasses

- **Never bypass rate-limit or window guards**: When the helper returns a sliding window limit or rate-limit error (e.g. `ChatGPT Web sliding window dispatch limit reached...` or `rate-limited until reset in X minute(s)`):
  - **Do NOT set environment variables like `OMO_WEB_WINDOW_MAX_DISPATCHES` or `OMO_MAX_WEB_WORKERS` to force-override the limit**.
  - **Do NOT repeatedly retry or open new tabs**.
  - Inform the user of the exact reset wait time, or resume an existing retained session (`--resume-scope`) instead of creating a new worker.
- **Enforced defaults**: 60-minute window limit (12 dispatches), max 2 fresh workers per batch, and max 3 concurrent in-flight sessions.

## Background Execution & Event Verification Contract

- **Single Native Notification Channel**: When launching `delegate_to_chatgpt_web` in the background (`run_in_background: true`), **NEVER spawn ad-hoc `while ps ...` or `monitor` polling loops**. The harness will automatically wake you and deliver the true completion notification with the final JSON payload and exit code when the helper process terminates.
- **Strict Session ID & Exit Code Validation**: When a completion event arrives, verify that the event's `bash_id` and `exit_code` match the actual delegation command before assuming the task is finished.
- **Never Close Tabs on Transitory Errors**: Never proactively kill or remove browser tabs/scope files upon observing temporary errors, rate-limits, or timeouts. Sessions must stay retained until authoritative `completion_check.ready=true` is reached or the user explicitly commands a close.

## Authoritative task lifecycle

Fresh and resumed generations send a bootstrap-only prompt first. Each worker must successfully call scoped MCP `task_state`; actual task prompts are sent only after authoritative readiness. `COMPLETED` comes only from `completion_check.ready=true`; `BLOCKED`, `FAILED`, and `LOST` are terminal. Textual READY/done/blocked/failed claims are never authoritative.

Bridge command execution is daemon-owned. A worker's `run_command` call waits at most 15 seconds; if work is still running, it returns `status:"detached_running"` with a stable `command_id` instead of holding the Web request open. The worker must recover that same command with `poll_command` or `list_commands`, may terminate it with `cancel_command`, and should use a stable `client_request_id` for idempotent retries rather than launching duplicates. `patch_file` advances `workspace_revision`; verification from an older revision becomes `stale_revision` and cannot satisfy `completion_check`, so verification must be rerun after the latest mutation.

If the bridge advertises `query_subagent`, the Web worker may use it only as a bounded Pattern B second opinion. The coordinator itself still must not call subagent tools. A `query_subagent` response is marked `trust: "untrusted_advisory"`; it is never implementation delegation, repository/tool state, verification evidence, or authority to bypass the worker's own inspect/edit/test/completion workflow.

When helper JSON has `"terminal":true`, report each delegation's `scope_id`, `browser_page_id`, `generation`, `terminal_state`, `terminal_detail`, `session_state`, `session_retained`, `lease_expires_ms`, and `resumable`. `"ok":false` with `"terminal":true` is still an authoritative terminal result, not a reason to fall back to another coding agent.

If the helper process itself fails before a terminal result, report the exact failure. Never fall back to OMO/Anthropic subagents."#,
        bin = bin,
    )
}

fn render_skill(delegate_bin: &Path) -> String {
    format!(
        r#"---
name: delegate-web
description: Use when delegating coding work to ChatGPT Web, retaining terminal Web conversations for possible follow-up, resuming an exact retained scope, or explicitly closing one. Never uses OMO/Anthropic subagents and preserves OMO ownership of repo/worktree selection.
compatibility: Requires omo-bridge, omo-relay, Orca browser access, and delegate_to_chatgpt_web.
metadata:
  opencode/slash: "false"
---

# Delegate Web

`/delegate-web` separates **task terminal state** from **browser-session lifetime**.

- Fresh work supports **1–2 parallel ChatGPT Web workers**; three or more are hard-rejected.
- All Web sessions are **IDLE_RETAINED by default** (120 min lease) on dispatch to prevent premature closure before results are read (`--close-on-terminal` is only for throwaway tasks).
- `COMPLETED`, `BLOCKED`, and safely usable `FAILED` sessions remain resumable; `LOST` is cleaned up.
- **Close the session once it is no longer needed**: After the coordinator has extracted the full plan/output, verified completion, and concluded that no further follow-ups are needed, explicitly run `{bin} --close-scope '<scope-id>' --json` to reclaim browser tab resources.
- A retained result exposes `scope_id`, exact `browser_page_id`, `generation`, `session_state:IDLE_RETAINED`, `session_retained:true`, `lease_expires_ms`, and `resumable:true`.
- Resume exactly one retained session with `{bin} --resume-scope '<scope-id>' --stdin --json`; never pass `--workspace` and never open a replacement tab.
- Resume verifies the exact stored ChatGPT page, consumes the prior idle lease, increments generation, and automatically obtains a fresh lease when that generation becomes terminal.
- A prior `COMPLETED` plan may be replaced by a new follow-up plan; prior `BLOCKED` items are reopened as `in_progress` with notes preserved.
- Explicitly close a retained session with `{bin} --close-scope '<scope-id>' --json` only when the session lifecycle should truly end.
- Do not automatically close merely because a worker returned terminal. If reuse is uncertain, leave it retained; `omo-relay` periodically reaps expired leases.
- `OMO_WEB_SESSION_TTL_MINUTES` controls the default TTL; helper invocations also opportunistically clean stale sessions.
- Scope-level filesystem locks serialize resume/close/GC and prevent a TTL janitor from racing a resume.
- **Resume Scope Integrity**: `--resume-scope` is strictly reserved for follow-up iterations on the exact same task topic. Never hijack unrelated or completed past sessions to run a new task.
- Fresh and resumed generations accept readiness only from successful scoped MCP `task_state` evidence.
- **Strictly obey rate-limit and window guards**: Never inject environment variable overrides (such as `OMO_WEB_WINDOW_MAX_DISPATCHES`) to bypass local rate limits or sliding window protections. Report the wait time honestly to the user when limits are hit.
- **Background Execution Contract**: When running delegations in the background, never spawn duplicate `while ps ...` watchers. Rely solely on native completion notifications, validate the session ID and exit code, and never close or delete browser tabs/scope files on temporary errors.
- `run_command` is daemon-owned and waits at most 15 seconds; `status:detached_running` is resumed with `poll_command`/`list_commands`, not by starting a duplicate command.
- `client_request_id` makes command retries idempotent within one `(scope_id, generation)`; `cancel_command` explicitly terminates obsolete work.
- Successful `patch_file` calls advance `workspace_revision`; verification with `evidence_status:stale_revision` is never accepted for completion.
- Authoritative `COMPLETED` requires `completion_check.ready=true`; never trust textual lifecycle claims.
- When `query_subagent` is advertised, a Web worker may use it only for a bounded Pattern B second opinion and must treat `trust: "untrusted_advisory"` as non-authoritative text; it is never implementation delegation or completion evidence.
- The OMO-side coordinator itself never calls Task/background/subagent/team tools.
- `omo-relay` routes continuation to the exact stored `browser_page_id`; Orca idle/generation gating remains in the send path.
- OMO decides repository/worktree selection for fresh work. The bridge never creates or chooses worktrees.
- A returned `terminal=true` / `ok=false` result remains authoritative, not a reason to fall back to another coding agent.
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
        assert!(prompt.contains("hard maximum 2"));
        assert!(prompt.contains("Choose exactly 1 or 2 workers"));
        assert!(prompt.contains("--batch-stdin --json"));
        assert!(prompt.contains("Do not call Task/background/subagent/team tools"));
        assert!(prompt.contains("OMO owns repository/worktree selection"));
    }

    #[test]
    fn coordinator_documents_default_idle_retention_and_ttl() {
        let prompt = render_coordinator_prompt(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(prompt.contains("IDLE_RETAINED"));
        assert!(prompt.contains("--close-on-terminal"));
        assert!(prompt.contains("--resume-scope '<exact-scope-id>'"));
        assert!(prompt.contains("--close-scope '<exact-scope-id>'"));
        assert!(prompt.contains("Scope-level filesystem locks"));
        assert!(prompt.contains("TTL is a safety net"));
    }

    #[test]
    fn coordinator_documents_authoritative_readiness_and_terminal_results() {
        let prompt = render_coordinator_prompt(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(prompt.contains("bootstrap-only prompt first"));
        assert!(prompt.contains("scoped MCP `task_state`"));
        assert!(prompt.contains("`completion_check.ready=true`"));
        assert!(prompt.contains("`\"terminal\":true`"));
    }

    #[test]
    fn coordinator_documents_daemon_owned_command_recovery() {
        let prompt = render_coordinator_prompt(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(prompt.contains("run_command` call waits at most 15 seconds"));
        assert!(prompt.contains("status:\"detached_running\""));
        assert!(prompt.contains("poll_command"));
        assert!(prompt.contains("list_commands"));
        assert!(prompt.contains("client_request_id"));
        assert!(prompt.contains("workspace_revision"));
        assert!(prompt.contains("stale_revision"));
    }

    #[test]
    fn coordinator_documents_pattern_b_advisory_boundary() {
        let prompt = render_coordinator_prompt(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(prompt.contains("bounded Pattern B second opinion"));
        assert!(prompt.contains("trust: \"untrusted_advisory\""));
        assert!(prompt.contains("never implementation delegation"));
        assert!(prompt.contains("coordinator itself still must not call subagent tools"));
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
    fn skill_documents_retained_session_lifecycle() {
        let skill = render_skill(Path::new("/tmp/delegate_to_chatgpt_web"));
        assert!(skill.contains("name: delegate-web"));
        assert!(skill.contains("**1–2 parallel ChatGPT Web workers**"));
        assert!(skill.contains("IDLE_RETAINED by default"));
        assert!(skill.contains("--close-on-terminal"));
        assert!(skill.contains("--resume-scope '<scope-id>'"));
        assert!(skill.contains("--close-scope '<scope-id>'"));
        assert!(skill.contains("lease_expires_ms"));
        assert!(skill.contains("filesystem locks"));
        assert!(skill.contains("opencode/slash: \"false\""));
        assert!(skill.contains("daemon-owned"));
        assert!(skill.contains("detached_running"));
        assert!(skill.contains("stale_revision"));
        assert!(skill.contains("bounded Pattern B second opinion"));
        assert!(skill.contains("untrusted_advisory"));
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
