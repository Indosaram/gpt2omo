use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::future::join_all;
use omo_bridge::orca::{
    close_browser_page, create_chatgpt_tab, send_chatgpt_prompt, verify_chatgpt_conversation,
    verify_chatgpt_page, OrcaConfig,
};
use omo_bridge::tools::task_state::{
    clear_delegation_lifecycle, load_delegation_lifecycle, record_terminal_evidence,
    release_session_retention, retain_session_with_lease, retained_session_expired,
    start_fresh_delegation_lifecycle, start_next_delegation_generation, DelegationLifecycle,
    DelegationTerminalState,
};
use omo_bridge::web_session::cleanup_expired_retained_sessions;
use omo_bridge::{default_scope_dir, Workspace, WorkspaceMux, WorkspaceScope};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};
use url::Url;

const MAX_PARALLEL_WEB_WORKERS: usize = 2;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const READINESS_TIMEOUT: Duration = Duration::from_secs(180);
const READINESS_RETRY_AFTER: Duration = Duration::from_secs(45);
const READINESS_FRESHNESS_MS: u64 = 240_000;
const TERMINAL_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_SESSION_TTL_MINUTES: u64 = 120;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "delegate_to_chatgpt_web",
    version,
    about = "Create, retain, resume, or close up to two isolated ChatGPT Web coding delegations through omo-bridge",
    trailing_var_arg = true
)]
struct Cli {
    /// Single task text. Multiple trailing words are joined with spaces.
    #[arg(value_name = "TASK")]
    task: Vec<String>,

    /// Read one complete task from stdin.
    #[arg(long, conflicts_with = "batch_stdin")]
    stdin: bool,

    /// Read a JSON batch manifest from stdin: {"tasks":[{"task":"...","workspace":"...","label":"..."}]}.
    #[arg(long, conflicts_with = "stdin")]
    batch_stdin: bool,

    /// Resume one retained delegation in its exact existing ChatGPT Web conversation.
    #[arg(long, conflicts_with = "batch_stdin")]
    resume_scope: Option<String>,

    /// Close and unregister one retained delegation scope without sending a task.
    #[arg(
        long,
        conflicts_with_all = [
            "resume_scope",
            "batch_stdin",
            "stdin",
            "keep_session",
            "close_on_terminal"
        ]
    )]
    close_scope: Option<String>,

    /// Backward-compatible no-op: sessions are now retained by default after terminal work.
    #[arg(long, hide = true)]
    keep_session: bool,

    /// Opt out of the safe default and close this generation's Web session immediately at terminal.
    #[arg(long)]
    close_on_terminal: bool,

    /// Idle-retained session TTL in minutes. The relay janitor closes stale sessions automatically.
    #[arg(
        long,
        env = "OMO_WEB_SESSION_TTL_MINUTES",
        default_value_t = DEFAULT_SESSION_TTL_MINUTES
    )]
    session_ttl_minutes: u64,

    /// Override automatic workspace discovery for a fresh task, or provide the default workspace
    /// for fresh batch items that do not specify one. Resume always trusts the stored scope workspace.
    #[arg(long, env = "OMO_WORKSPACE")]
    workspace: Option<PathBuf>,

    /// Broad mount root used by the running bridge daemon.
    #[arg(long, env = "OMO_MOUNT_ROOT", default_value = "/")]
    mount_root: PathBuf,

    /// omo-bridge base URL.
    #[arg(long, env = "OMO_BRIDGE_URL", default_value = "http://127.0.0.1:18800")]
    bridge_url: String,

    /// Override the shared directory that stores per-delegation workspace scopes.
    #[arg(long, env = "OMO_SCOPE_DIR")]
    scope_dir: Option<PathBuf>,

    /// Orca worktree selector used for browser tabs.
    #[arg(long, env = "OMO_ORCA_WORKTREE", default_value = "active")]
    worktree: String,

    /// Legacy terminal selector retained for compatibility. Browser-scoped delegations do not use it.
    #[arg(long, env = "OMO_RELAY_TERMINAL")]
    terminal: Option<String>,

    /// Orca CLI executable.
    #[arg(long, env = "OMO_ORCA_BIN", default_value = "orca")]
    orca_bin: String,

    /// Optional bearer token used by omo-bridge.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    token: Option<String>,

    /// Validate fresh workspaces/create scopes, but do not create/send browser prompts.
    #[arg(long)]
    dry_run: bool,

    /// Emit a compact JSON result for machine callers such as OMO.
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct BatchManifest {
    tasks: Vec<BatchTask>,
}

#[derive(Clone, Debug, Deserialize)]
struct BatchTask {
    task: String,
    #[serde(default)]
    workspace: Option<PathBuf>,
    #[serde(default)]
    label: Option<String>,
}

#[derive(Clone, Debug)]
struct PreparedTask {
    task: String,
    workspace: PathBuf,
    label: Option<String>,
}

#[derive(Clone, Debug)]
struct StagedDelegation {
    scope_id: String,
    workspace: String,
    label: Option<String>,
    browser_page_id: Option<String>,
    generation: u64,
    generation_started_ms: u64,
    resumed: bool,
    bootstrap_prompt: Option<String>,
    task_prompt: Option<String>,
}

#[derive(Clone, Debug)]
struct TerminalObservation {
    state: DelegationTerminalState,
    detail: Option<String>,
    terminal_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
struct SessionDisposition {
    retained: bool,
    closed: bool,
    expired: bool,
    lease_expires_ms: Option<u64>,
    error: Option<String>,
}

#[derive(Default)]
struct BatchOutcome {
    readiness_complete: bool,
    terminal_complete: bool,
    actual_sent: Vec<bool>,
    terminal: Vec<TerminalObservation>,
    sessions: Vec<SessionDisposition>,
}

enum ResumeStage {
    Ready(StagedDelegation),
    Lost {
        staged: StagedDelegation,
        terminal: TerminalObservation,
        session: SessionDisposition,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    validate_control_mode(&cli)?;
    let ttl_ms = session_ttl_ms(cli.session_ttl_minutes)?;

    let bridge_url = cli.bridge_url.trim_end_matches('/');
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(8))
        .build()?;
    probe_bridge(&client, bridge_url, cli.token.as_deref()).await?;

    let port = bridge_port(bridge_url)?;
    let scope_dir = cli
        .scope_dir
        .clone()
        .unwrap_or_else(|| default_scope_dir(port));
    let mux = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;
    let orca = OrcaConfig::new(
        cli.worktree.clone(),
        cli.terminal.clone(),
        cli.orca_bin.clone(),
    );

    if !cli.dry_run {
        let excluded = cli.resume_scope.as_deref().or(cli.close_scope.as_deref());
        cleanup_expired_retained_sessions(&mux, &orca, epoch_ms(), ttl_ms, excluded).await?;
    }

    if let Some(scope_id) = cli.close_scope.as_deref() {
        let value = close_retained_scope(&mux, &orca, scope_id).await?;
        if cli.json {
            println!("{}", serde_json::to_string(&value)?);
        } else {
            println!(
                "Closed retained Web scope {} page={}",
                scope_id,
                value["browser_page_id"].as_str().unwrap_or("<missing>")
            );
        }
        return Ok(());
    }

    let staged = if let Some(scope_id) = cli.resume_scope.as_deref() {
        if cli.dry_run {
            return Err(anyhow!(
                "--dry-run is not supported with --resume-scope because resume requires authoritative browser-page liveness verification"
            ));
        }
        let task = prepare_resume_task(&cli)?;
        match stage_resume_delegation(&mux, &orca, scope_id, &task).await? {
            ResumeStage::Ready(item) => vec![item],
            ResumeStage::Lost {
                staged,
                terminal,
                session,
            } => {
                emit_result(
                    &cli,
                    &scope_dir,
                    bridge_url,
                    &[staged],
                    BatchOutcome {
                        readiness_complete: false,
                        terminal_complete: true,
                        actual_sent: vec![false],
                        terminal: vec![terminal],
                        sessions: vec![session],
                    },
                )?;
                return Ok(());
            }
        }
    } else {
        let tasks = prepare_tasks(&cli)?;
        if cli.dry_run {
            stage_dry_run(&mux, &tasks)?
        } else {
            stage_browser_delegations(&mux, &orca, &tasks).await?
        }
    };

    if cli.dry_run {
        emit_result(
            &cli,
            &scope_dir,
            bridge_url,
            &staged,
            BatchOutcome::default(),
        )?;
        return Ok(());
    }

    if let Err(error) = dispatch_bootstrap(&orca, &staged).await {
        cleanup_unstarted_staged(&mux, &orca, &staged).await;
        return Err(error.context(
            "readiness bootstrap failed; actual task dispatch count is 0 and staged tabs/scopes were cleaned up",
        ));
    }

    if let Err(error) = wait_for_all_ready(&mux, &orca, &staged, READINESS_TIMEOUT).await {
        cleanup_unstarted_staged(&mux, &orca, &staged).await;
        return Err(error.context(
            "authoritative readiness handshake failed closed; actual task dispatch count is 0 and staged tabs/scopes were cleaned up",
        ));
    }

    let actual_sent = match dispatch_actual_tasks(&mux, &orca, &staged).await {
        Ok(sent) => sent,
        Err(error) => {
            cleanup_unstarted_staged(&mux, &orca, &staged).await;
            return Err(error.context(
                "readiness became invalid before dispatch; actual task dispatch count is 0 and staged tabs/scopes were cleaned up",
            ));
        }
    };

    let terminal = wait_for_terminal_states(&mux, &staged, TERMINAL_TIMEOUT).await;
    let sessions = finalize_terminal_sessions(
        &mux,
        &orca,
        &staged,
        &terminal,
        cli.close_on_terminal,
        ttl_ms,
    )
    .await;

    cleanup_expired_retained_sessions(&mux, &orca, epoch_ms(), ttl_ms, None).await?;

    emit_result(
        &cli,
        &scope_dir,
        bridge_url,
        &staged,
        BatchOutcome {
            readiness_complete: true,
            terminal_complete: true,
            actual_sent,
            terminal,
            sessions,
        },
    )?;
    Ok(())
}

fn validate_control_mode(cli: &Cli) -> Result<()> {
    if cli.keep_session && cli.close_on_terminal {
        return Err(anyhow!(
            "--keep-session is retained only for backward compatibility and cannot be combined with --close-on-terminal"
        ));
    }
    if cli.close_scope.is_some()
        && (!cli.task.is_empty()
            || cli.batch_stdin
            || cli.stdin
            || cli.workspace.is_some()
            || cli.dry_run)
    {
        return Err(anyhow!(
            "--close-scope cannot be combined with a task, stdin/batch input, --workspace, or --dry-run"
        ));
    }
    if cli.resume_scope.is_some() {
        if cli.workspace.is_some() {
            return Err(anyhow!(
                "--resume-scope uses the authoritative workspace stored in that scope and cannot be combined with --workspace"
            ));
        }
        if cli.batch_stdin {
            return Err(anyhow!(
                "--resume-scope cannot be combined with --batch-stdin"
            ));
        }
    }
    Ok(())
}

fn session_ttl_ms(minutes: u64) -> Result<u64> {
    if minutes == 0 {
        return Err(anyhow!("--session-ttl-minutes must be greater than zero"));
    }
    minutes
        .checked_mul(60_000)
        .ok_or_else(|| anyhow!("--session-ttl-minutes is too large"))
}

fn emit_result(
    cli: &Cli,
    scope_dir: &Path,
    bridge_url: &str,
    staged: &[StagedDelegation],
    outcome: BatchOutcome,
) -> Result<()> {
    let delegations = staged
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let terminal_observation = outcome.terminal.get(index);
            let session = outcome.sessions.get(index).cloned().unwrap_or_default();
            let session_state = if session.retained {
                "IDLE_RETAINED"
            } else if session.closed {
                "CLOSED"
            } else {
                "UNAVAILABLE"
            };
            serde_json::json!({
                "index": index + 1,
                "label": item.label,
                "scope_id": item.scope_id,
                "workspace": item.workspace,
                "browser_page_id": item.browser_page_id,
                "generation": item.generation,
                "resumed": item.resumed,
                "ready": outcome.readiness_complete,
                "actual_task_sent": outcome.actual_sent.get(index).copied().unwrap_or(false),
                "terminal_state": terminal_observation.map(|state| state.state),
                "terminal_detail": terminal_observation.and_then(|state| state.detail.clone()),
                "terminal_ms": terminal_observation.and_then(|state| state.terminal_ms),
                "session_state": session_state,
                "session_retained": session.retained,
                "session_closed": session.closed,
                "session_expired": session.expired,
                "lease_expires_ms": session.lease_expires_ms,
                "session_error": session.error,
                "resumable": session.retained,
            })
        })
        .collect::<Vec<_>>();
    let all_completed = outcome.terminal_complete
        && outcome.terminal.len() == staged.len()
        && outcome
            .terminal
            .iter()
            .all(|state| state.state == DelegationTerminalState::Completed);
    let all_sent = !outcome.actual_sent.is_empty()
        && outcome.actual_sent.len() == staged.len()
        && outcome.actual_sent.iter().all(|sent| *sent);
    let session_ok = outcome.sessions.is_empty()
        || (outcome.sessions.len() == staged.len()
            && outcome
                .sessions
                .iter()
                .all(|session| session.error.is_none()));
    let result = serde_json::json!({
        "ok": if cli.dry_run { true } else { all_completed && all_sent },
        "sent": all_sent,
        "ready": outcome.readiness_complete,
        "terminal": outcome.terminal_complete,
        "session_ok": session_ok,
        "session_policy": if cli.close_on_terminal { "CLOSE_ON_TERMINAL" } else { "IDLE_RETAINED" },
        "session_ttl_minutes": cli.session_ttl_minutes,
        "parallel_count": staged.len(),
        "max_parallel": MAX_PARALLEL_WEB_WORKERS,
        "scope_dir": scope_dir.to_string_lossy(),
        "bridge_url": bridge_url,
        "delegations": delegations,
    });

    if cli.json {
        println!("{}", serde_json::to_string(&result)?);
    } else if cli.dry_run {
        println!("Prepared {} ChatGPT Web worker(s)", staged.len());
        for (index, item) in staged.iter().enumerate() {
            println!(
                "{}. scope={} workspace={} page=<dry-run>",
                index + 1,
                item.scope_id,
                item.workspace
            );
        }
    } else {
        println!("Finished {} ChatGPT Web worker(s)", staged.len());
        for (index, item) in staged.iter().enumerate() {
            let state = outcome
                .terminal
                .get(index)
                .map(|value| format!("{:?}", value.state).to_ascii_uppercase())
                .unwrap_or_else(|| "LOST".into());
            let session = outcome.sessions.get(index).cloned().unwrap_or_default();
            let session_state = if session.retained {
                "IDLE_RETAINED"
            } else if session.closed {
                "CLOSED"
            } else {
                "UNAVAILABLE"
            };
            println!(
                "{}. scope={} generation={} page={} terminal={} session={}",
                index + 1,
                item.scope_id,
                item.generation,
                item.browser_page_id.as_deref().unwrap_or("<missing>"),
                state,
                session_state
            );
        }
    }

    Ok(())
}

fn prepare_resume_task(cli: &Cli) -> Result<String> {
    if !cli.task.is_empty() && cli.stdin {
        return Err(anyhow!("TASK arguments cannot be combined with --stdin"));
    }
    let task = if cli.stdin {
        read_stdin_bounded()?
    } else {
        cli.task.join(" ")
    };
    let task = task.trim().to_string();
    if task.is_empty() {
        return Err(anyhow!(
            "--resume-scope requires one non-empty follow-up task"
        ));
    }
    Ok(task)
}

fn prepare_tasks(cli: &Cli) -> Result<Vec<PreparedTask>> {
    if cli.batch_stdin && !cli.task.is_empty() {
        return Err(anyhow!(
            "TASK arguments cannot be combined with --batch-stdin"
        ));
    }
    if cli.stdin && !cli.task.is_empty() {
        return Err(anyhow!("TASK arguments cannot be combined with --stdin"));
    }

    let default_workspace = discover_workspace(cli.workspace.as_deref())?;
    let raw_tasks = if cli.batch_stdin {
        let input = read_stdin_bounded()?;
        let manifest: BatchManifest =
            serde_json::from_str(&input).context("failed to parse --batch-stdin JSON manifest")?;
        manifest.tasks
    } else {
        let task = if cli.stdin {
            read_stdin_bounded()?
        } else {
            cli.task.join(" ")
        };
        vec![BatchTask {
            task,
            workspace: None,
            label: None,
        }]
    };

    validate_parallel_count(raw_tasks.len())?;

    raw_tasks
        .into_iter()
        .enumerate()
        .map(|(index, raw)| {
            let task = raw.task.trim().to_string();
            if task.is_empty() {
                return Err(anyhow!("batch task {} is empty", index + 1));
            }
            let workspace = match raw.workspace {
                Some(path) => canonical_directory(&path)
                    .with_context(|| format!("invalid workspace for batch task {}", index + 1))?,
                None => default_workspace.clone(),
            };
            let label = raw
                .label
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            Ok(PreparedTask {
                task,
                workspace,
                label,
            })
        })
        .collect()
}

fn validate_parallel_count(count: usize) -> Result<()> {
    if count == 0 {
        return Err(anyhow!("at least one Web delegation task is required"));
    }
    if count > MAX_PARALLEL_WEB_WORKERS {
        return Err(anyhow!(
            "parallel Web delegation is limited to {} workers; received {}",
            MAX_PARALLEL_WEB_WORKERS,
            count
        ));
    }
    Ok(())
}

fn read_stdin_bounded() -> Result<String> {
    let mut input = String::new();
    std::io::stdin()
        .take((MAX_INPUT_BYTES + 1) as u64)
        .read_to_string(&mut input)
        .context("failed to read delegation input from stdin")?;
    if input.len() > MAX_INPUT_BYTES {
        return Err(anyhow!("delegation input exceeds 1 MiB"));
    }
    Ok(input)
}

fn stage_dry_run(mux: &WorkspaceMux, tasks: &[PreparedTask]) -> Result<Vec<StagedDelegation>> {
    tasks
        .iter()
        .map(|task| {
            let scope = mux.register(&task.workspace, None)?;
            Ok(StagedDelegation {
                scope_id: scope.scope_id,
                workspace: scope.workspace,
                label: task.label.clone(),
                browser_page_id: None,
                generation: 0,
                generation_started_ms: scope.created_ms,
                resumed: false,
                bootstrap_prompt: None,
                task_prompt: None,
            })
        })
        .collect()
}

async fn stage_browser_delegations(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    tasks: &[PreparedTask],
) -> Result<Vec<StagedDelegation>> {
    let mut staged = Vec::with_capacity(tasks.len());
    for task in tasks {
        let page = match create_chatgpt_tab(orca).await {
            Ok(page) => page,
            Err(error) => {
                cleanup_unstarted_staged(mux, orca, &staged).await;
                return Err(error.context("failed to create a fresh ChatGPT Web worker tab"));
            }
        };
        let scope = match mux.register_browser(&task.workspace, page.clone()) {
            Ok(scope) => scope,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                cleanup_unstarted_staged(mux, orca, &staged).await;
                return Err(error.into());
            }
        };
        let workspace = match mux.resolve(&scope.scope_id) {
            Ok(workspace) => workspace,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                let _ = mux.remove(&scope.scope_id);
                cleanup_unstarted_staged(mux, orca, &staged).await;
                return Err(error.into());
            }
        };
        let lifecycle = match start_fresh_delegation_lifecycle(&workspace, &scope.scope_id) {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                let _ = mux.remove(&scope.scope_id);
                cleanup_unstarted_staged(mux, orca, &staged).await;
                return Err(anyhow!(error));
            }
        };
        staged.push(build_staged_delegation(
            &scope,
            Some(page),
            task.label.clone(),
            &task.task,
            &lifecycle,
            false,
        ));
    }
    Ok(staged)
}

async fn stage_resume_delegation(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    scope_id: &str,
    task: &str,
) -> Result<ResumeStage> {
    let scope_lock = mux.lock_scope(scope_id)?;
    let (scope, workspace, previous) = load_resumable_scope(mux, scope_id)?;
    let page = scope
        .browser_page_id
        .clone()
        .ok_or_else(|| anyhow!("retained scope has no browser_page_id"))?;
    let conversation_id = scope
        .browser_conversation_id
        .clone()
        .ok_or_else(|| anyhow!("retained scope has no stored ChatGPT conversation identity"))?;

    if retained_session_expired(&previous, epoch_ms()) {
        let detail = format!("retained Web session lease expired before resume: {scope_id}");
        release_session_retention(&workspace, scope_id).map_err(anyhow::Error::msg)?;
        mux.remove(scope_id)?;
        drop(scope_lock);
        let close_error = close_browser_page(orca, &page).await.err();
        return Ok(ResumeStage::Lost {
            staged: staged_from_existing_terminal(&scope, task, &previous),
            terminal: TerminalObservation {
                state: DelegationTerminalState::Lost,
                detail: Some(detail.clone()),
                terminal_ms: Some(epoch_ms()),
            },
            session: SessionDisposition {
                retained: false,
                closed: close_error.is_none(),
                expired: true,
                lease_expires_ms: previous.lease_expires_ms,
                error: close_error.map(|error| format!("{detail}; tab close failed: {error}")),
            },
        });
    }

    if let Err(error) = verify_chatgpt_conversation(orca, &page, &conversation_id).await {
        let lifecycle = start_next_delegation_generation(&workspace, scope_id, false)
            .map_err(anyhow::Error::msg)?;
        let detail = format!(
            "retained browser_page_id {} is no longer the expected retained ChatGPT conversation: {}",
            page, error
        );
        let terminal_lifecycle = record_terminal_evidence(
            &workspace,
            scope_id,
            DelegationTerminalState::Lost,
            Some(&detail),
        )
        .map_err(anyhow::Error::msg)?;
        let _ = release_session_retention(&workspace, scope_id);
        let _ = mux.remove(scope_id);
        drop(scope_lock);
        let close_error = close_browser_page(orca, &page).await.err();
        let staged = build_staged_delegation(&scope, Some(page), None, task, &lifecycle, true);
        return Ok(ResumeStage::Lost {
            staged,
            terminal: TerminalObservation {
                state: terminal_lifecycle
                    .terminal_state
                    .unwrap_or(DelegationTerminalState::Lost),
                detail: terminal_lifecycle.terminal_detail,
                terminal_ms: terminal_lifecycle.terminal_ms,
            },
            session: SessionDisposition {
                retained: false,
                closed: close_error.is_none(),
                expired: false,
                lease_expires_ms: None,
                error: close_error
                    .map(|close_error| format!("{detail}; tab close failed: {close_error}"))
                    .or(Some(detail)),
            },
        });
    }

    let reopen_blocked = previous.terminal_state == Some(DelegationTerminalState::Blocked);
    let lifecycle = start_next_delegation_generation(&workspace, scope_id, reopen_blocked)
        .map_err(anyhow::Error::msg)?;
    drop(scope_lock);
    Ok(ResumeStage::Ready(build_staged_delegation(
        &scope,
        Some(page),
        None,
        task,
        &lifecycle,
        true,
    )))
}

fn load_resumable_scope(
    mux: &WorkspaceMux,
    scope_id: &str,
) -> Result<(WorkspaceScope, Workspace, DelegationLifecycle)> {
    let scope = mux.lookup(scope_id)?;
    let workspace = mux.resolve(scope_id)?;
    let lifecycle = load_delegation_lifecycle(&workspace, scope_id)
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow!("retained scope has no delegation lifecycle evidence"))?;
    if !lifecycle.session_retained {
        return Err(anyhow!(
            "scope {} is not retained/resumable or its lease has been released",
            scope_id
        ));
    }
    if lifecycle.terminal_state.is_none() {
        return Err(anyhow!(
            "scope {} is not terminal yet and cannot be resumed concurrently",
            scope_id
        ));
    }
    Ok((scope, workspace, lifecycle))
}

fn staged_from_existing_terminal(
    scope: &WorkspaceScope,
    task: &str,
    lifecycle: &DelegationLifecycle,
) -> StagedDelegation {
    StagedDelegation {
        scope_id: scope.scope_id.clone(),
        workspace: scope.workspace.clone(),
        label: None,
        browser_page_id: scope.browser_page_id.clone(),
        generation: lifecycle.generation,
        generation_started_ms: lifecycle.generation_started_ms,
        resumed: true,
        bootstrap_prompt: None,
        task_prompt: Some(task.to_string()),
    }
}

fn build_staged_delegation(
    scope: &WorkspaceScope,
    page: Option<String>,
    label: Option<String>,
    task: &str,
    lifecycle: &DelegationLifecycle,
    resumed: bool,
) -> StagedDelegation {
    let workspace_path = Path::new(&scope.workspace);
    StagedDelegation {
        scope_id: scope.scope_id.clone(),
        workspace: scope.workspace.clone(),
        label,
        browser_page_id: page,
        generation: lifecycle.generation,
        generation_started_ms: lifecycle.generation_started_ms,
        resumed,
        bootstrap_prompt: Some(build_bootstrap_prompt(
            &scope.scope_id,
            workspace_path,
            lifecycle.generation,
            resumed,
        )),
        task_prompt: Some(build_delegation_prompt(
            &scope.scope_id,
            workspace_path,
            lifecycle.generation,
            resumed,
            task,
        )),
    }
}

async fn dispatch_bootstrap(orca: &OrcaConfig, staged: &[StagedDelegation]) -> Result<()> {
    let futures = staged.iter().map(|item| {
        send_chatgpt_prompt(
            orca,
            item.browser_page_id.as_deref().unwrap_or_default(),
            item.bootstrap_prompt.as_deref().unwrap_or_default(),
        )
    });
    let results = join_all(futures).await;
    let failures = results
        .into_iter()
        .enumerate()
        .filter_map(|(index, result)| {
            result
                .err()
                .map(|error| format!("worker {}: {}", index + 1, error))
        })
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "one or more ChatGPT Web readiness bootstraps failed: {}",
            failures.join("; ")
        ))
    }
}

async fn wait_for_all_ready(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    staged: &[StagedDelegation],
    timeout: Duration,
) -> Result<()> {
    let started = Instant::now();
    let deadline = started + timeout;
    let mut retried = false;
    loop {
        let now = epoch_ms();
        let mut pending = Vec::new();
        for (index, item) in staged.iter().enumerate() {
            let lifecycle = lifecycle_for(mux, item)?;
            if let Some(lifecycle) = lifecycle.as_ref() {
                if lifecycle.generation != item.generation {
                    return Err(anyhow!(
                        "worker {} lifecycle generation changed from {} to {} before dispatch",
                        index + 1,
                        item.generation,
                        lifecycle.generation
                    ));
                }
                if let Some(state) = lifecycle.terminal_state {
                    return Err(anyhow!(
                        "worker {} entered terminal state {:?} before actual task dispatch",
                        index + 1,
                        state
                    ));
                }
            }
            if !has_fresh_readiness(item, lifecycle.as_ref(), now) {
                pending.push(index);
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "readiness timeout after {}s; unready/stale worker(s): {}",
                timeout.as_secs(),
                pending
                    .iter()
                    .map(|index| (index + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if !retried && Instant::now().duration_since(started) >= READINESS_RETRY_AFTER {
            let futures = pending.iter().map(|index| {
                let item = &staged[*index];
                send_chatgpt_prompt(
                    orca,
                    item.browser_page_id.as_deref().unwrap_or_default(),
                    item.bootstrap_prompt.as_deref().unwrap_or_default(),
                )
            });
            let results = join_all(futures).await;
            let failures = pending
                .iter()
                .zip(results.into_iter())
                .filter_map(|(index, result)| {
                    result
                        .err()
                        .map(|error| format!("worker {}: {}", index + 1, error))
                })
                .collect::<Vec<_>>();
            if !failures.is_empty() {
                return Err(anyhow!(
                    "one or more pending readiness bootstrap retries failed: {}",
                    failures.join("; ")
                ));
            }
            retried = true;
            continue;
        }
        sleep(LIFECYCLE_POLL_INTERVAL).await;
    }
}

fn actual_dispatch_plan(
    mux: &WorkspaceMux,
    staged: &[StagedDelegation],
    now_ms: u64,
) -> Result<Vec<(String, String)>> {
    let mut plan = Vec::with_capacity(staged.len());
    for item in staged {
        let lifecycle = lifecycle_for(mux, item)?;
        if lifecycle.as_ref().is_none_or(|state| {
            state.generation != item.generation
                || state.terminal_state.is_some()
                || !has_fresh_readiness(item, Some(state), now_ms)
        }) {
            return Ok(Vec::new());
        }
        let page = item
            .browser_page_id
            .clone()
            .ok_or_else(|| anyhow!("staged live worker has no browser_page_id"))?;
        let prompt = item
            .task_prompt
            .clone()
            .ok_or_else(|| anyhow!("staged live worker has no actual task prompt"))?;
        plan.push((page, prompt));
    }
    Ok(plan)
}

async fn dispatch_actual_tasks(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    staged: &[StagedDelegation],
) -> Result<Vec<bool>> {
    let plan = actual_dispatch_plan(mux, staged, epoch_ms())?;
    if plan.len() != staged.len() {
        return Err(anyhow!(
            "all-worker readiness gate was not satisfied immediately before actual dispatch"
        ));
    }

    let futures = plan
        .iter()
        .map(|(page, prompt)| send_chatgpt_prompt(orca, page, prompt));
    let results = join_all(futures).await;
    let mut sent = vec![false; staged.len()];
    for (index, result) in results.into_iter().enumerate() {
        match result {
            Ok(()) => sent[index] = true,
            Err(error) => {
                let detail = format!("actual task prompt dispatch failed: {}", error);
                let _ = record_helper_terminal(
                    mux,
                    &staged[index],
                    DelegationTerminalState::Failed,
                    &detail,
                );
            }
        }
    }
    Ok(sent)
}

async fn wait_for_terminal_states(
    mux: &WorkspaceMux,
    staged: &[StagedDelegation],
    timeout: Duration,
) -> Vec<TerminalObservation> {
    let deadline = Instant::now() + timeout;
    let mut observed = vec![None; staged.len()];

    loop {
        for (index, item) in staged.iter().enumerate() {
            if observed[index].is_some() {
                continue;
            }
            match lifecycle_for(mux, item) {
                Ok(Some(lifecycle)) if lifecycle.generation == item.generation => {
                    if let Some(state) = lifecycle.terminal_state {
                        observed[index] = Some(TerminalObservation {
                            state,
                            detail: lifecycle.terminal_detail,
                            terminal_ms: lifecycle.terminal_ms,
                        });
                    }
                }
                Ok(Some(lifecycle)) => {
                    observed[index] = Some(TerminalObservation {
                        state: DelegationTerminalState::Lost,
                        detail: Some(format!(
                            "lifecycle generation changed unexpectedly from {} to {}",
                            item.generation, lifecycle.generation
                        )),
                        terminal_ms: Some(epoch_ms()),
                    });
                }
                Ok(None) => {}
                Err(error) => {
                    observed[index] = Some(TerminalObservation {
                        state: DelegationTerminalState::Lost,
                        detail: Some(format!("lifecycle evidence became unreadable: {}", error)),
                        terminal_ms: Some(epoch_ms()),
                    });
                }
            }
        }

        if observed.iter().all(Option::is_some) {
            break;
        }
        if Instant::now() >= deadline {
            for (index, item) in staged.iter().enumerate() {
                if observed[index].is_some() {
                    continue;
                }
                let detail = format!(
                    "no authoritative terminal evidence within {} seconds",
                    timeout.as_secs()
                );
                let persisted =
                    record_helper_terminal(mux, item, DelegationTerminalState::Lost, &detail).ok();
                observed[index] = Some(TerminalObservation {
                    state: persisted
                        .as_ref()
                        .and_then(|lifecycle| lifecycle.terminal_state)
                        .unwrap_or(DelegationTerminalState::Lost),
                    detail: persisted
                        .as_ref()
                        .and_then(|lifecycle| lifecycle.terminal_detail.clone())
                        .or(Some(detail)),
                    terminal_ms: persisted.and_then(|lifecycle| lifecycle.terminal_ms),
                });
            }
            break;
        }
        sleep(LIFECYCLE_POLL_INTERVAL).await;
    }

    observed.into_iter().flatten().collect()
}

fn should_retain_terminal(state: DelegationTerminalState, close_on_terminal: bool) -> bool {
    !close_on_terminal && state != DelegationTerminalState::Lost
}

async fn finalize_terminal_sessions(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    staged: &[StagedDelegation],
    terminal: &[TerminalObservation],
    close_on_terminal: bool,
    ttl_ms: u64,
) -> Vec<SessionDisposition> {
    let mut dispositions = Vec::with_capacity(staged.len());
    for (index, item) in staged.iter().enumerate() {
        let retain = terminal.get(index).is_some_and(|observation| {
            should_retain_terminal(observation.state, close_on_terminal)
        });
        let disposition = if retain {
            retain_terminal_session(mux, orca, item, ttl_ms).await
        } else {
            close_terminal_session(mux, orca, item).await
        };
        dispositions.push(disposition);
    }
    dispositions
}

async fn retain_terminal_session(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    item: &StagedDelegation,
    ttl_ms: u64,
) -> SessionDisposition {
    let Some(page) = item.browser_page_id.as_deref() else {
        return SessionDisposition {
            error: Some("cannot retain delegation without browser_page_id".into()),
            ..SessionDisposition::default()
        };
    };
    let scope_lock = match mux.lock_scope(&item.scope_id) {
        Ok(lock) => lock,
        Err(error) => {
            return SessionDisposition {
                error: Some(format!("cannot lock retained scope: {error}")),
                ..SessionDisposition::default()
            }
        }
    };
    let workspace = match mux.resolve(&item.scope_id) {
        Ok(workspace) => workspace,
        Err(error) => {
            return SessionDisposition {
                error: Some(format!("cannot resolve retained scope: {}", error)),
                ..SessionDisposition::default()
            }
        }
    };
    let probe = match verify_chatgpt_page(orca, page).await {
        Ok(probe) => probe,
        Err(error) => {
            let _ = release_session_retention(&workspace, &item.scope_id);
            let _ = mux.remove(&item.scope_id);
            drop(scope_lock);
            let close_error = close_browser_page(orca, page).await.err();
            return SessionDisposition {
                retained: false,
                closed: close_error.is_none(),
                error: Some(match close_error {
                    Some(close_error) => format!(
                        "terminal browser page is not retainable: {}; tab close also failed: {}",
                        error, close_error
                    ),
                    None => format!("terminal browser page is not retainable: {}", error),
                }),
                ..SessionDisposition::default()
            };
        }
    };
    if let Err(error) = mux.update_browser_conversation_id(&item.scope_id, &probe.conversation_id) {
        let _ = release_session_retention(&workspace, &item.scope_id);
        let _ = mux.remove(&item.scope_id);
        drop(scope_lock);
        let close_error = close_browser_page(orca, page).await.err();
        return SessionDisposition {
            retained: false,
            closed: close_error.is_none(),
            error: Some(match close_error {
                Some(close_error) => format!(
                    "failed to persist ChatGPT conversation identity: {}; tab close also failed: {}",
                    error, close_error
                ),
                None => format!("failed to persist ChatGPT conversation identity: {}", error),
            }),
            ..SessionDisposition::default()
        };
    }
    match retain_session_with_lease(&workspace, &item.scope_id, ttl_ms) {
        Ok(lifecycle) => SessionDisposition {
            retained: true,
            closed: false,
            expired: false,
            lease_expires_ms: lifecycle.lease_expires_ms,
            error: None,
        },
        Err(error) => {
            let _ = mux.remove(&item.scope_id);
            drop(scope_lock);
            let close_error = close_browser_page(orca, page).await.err();
            SessionDisposition {
                retained: false,
                closed: close_error.is_none(),
                expired: false,
                lease_expires_ms: None,
                error: Some(match close_error {
                    Some(close_error) => format!(
                        "failed to persist retained-session lease: {}; fallback tab close also failed: {}",
                        error, close_error
                    ),
                    None => format!("failed to persist retained-session lease: {}", error),
                }),
            }
        }
    }
}

async fn close_terminal_session(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    item: &StagedDelegation,
) -> SessionDisposition {
    let scope_lock = mux.lock_scope(&item.scope_id).ok();
    let workspace = mux.resolve(&item.scope_id).ok();
    if let Some(workspace) = workspace.as_ref() {
        let _ = release_session_retention(workspace, &item.scope_id);
    }
    let remove_result = mux.remove(&item.scope_id);
    drop(scope_lock);
    let close_result = match item.browser_page_id.as_deref() {
        Some(page) => close_browser_page(orca, page).await,
        None => Err(anyhow!("delegation has no browser_page_id to close")),
    };
    let error = match (close_result.as_ref().err(), remove_result.as_ref().err()) {
        (None, None) => None,
        (Some(close_error), None) => Some(format!("browser tab close failed: {}", close_error)),
        (None, Some(remove_error)) => Some(format!("scope cleanup failed: {}", remove_error)),
        (Some(close_error), Some(remove_error)) => Some(format!(
            "browser tab close failed: {}; scope cleanup failed: {}",
            close_error, remove_error
        )),
    };
    SessionDisposition {
        retained: false,
        closed: close_result.is_ok(),
        expired: false,
        lease_expires_ms: None,
        error,
    }
}

async fn close_retained_scope(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    scope_id: &str,
) -> Result<Value> {
    let scope_lock = mux.lock_scope(scope_id)?;
    let (scope, workspace, lifecycle) = load_resumable_scope(mux, scope_id)?;
    let page = scope
        .browser_page_id
        .clone()
        .ok_or_else(|| anyhow!("retained scope has no browser_page_id"))?;
    release_session_retention(&workspace, scope_id).map_err(anyhow::Error::msg)?;
    mux.remove(scope_id)?;
    drop(scope_lock);
    let close_error = close_browser_page(orca, &page).await.err();
    Ok(serde_json::json!({
        "ok": close_error.is_none(),
        "closed_scope": scope_id,
        "browser_page_id": page,
        "generation": lifecycle.generation,
        "scope_removed": true,
        "session_state": "CLOSED",
        "session_retained": false,
        "session_closed": close_error.is_none(),
        "session_error": close_error.map(|error| error.to_string()),
    }))
}

fn lifecycle_for(
    mux: &WorkspaceMux,
    item: &StagedDelegation,
) -> Result<Option<DelegationLifecycle>> {
    let workspace = mux.resolve(&item.scope_id)?;
    load_delegation_lifecycle(&workspace, &item.scope_id).map_err(anyhow::Error::msg)
}

fn has_fresh_readiness(
    item: &StagedDelegation,
    lifecycle: Option<&DelegationLifecycle>,
    now_ms: u64,
) -> bool {
    let Some(lifecycle) = lifecycle else {
        return false;
    };
    let Some(ready_ms) = lifecycle.ready_ms else {
        return false;
    };
    lifecycle.generation == item.generation
        && lifecycle.generation_started_ms == item.generation_started_ms
        && ready_ms >= item.generation_started_ms
        && now_ms >= ready_ms
        && now_ms.saturating_sub(ready_ms) <= READINESS_FRESHNESS_MS
}

fn record_helper_terminal(
    mux: &WorkspaceMux,
    item: &StagedDelegation,
    state: DelegationTerminalState,
    detail: &str,
) -> Result<DelegationLifecycle> {
    let workspace = mux.resolve(&item.scope_id)?;
    let lifecycle = load_delegation_lifecycle(&workspace, &item.scope_id)
        .map_err(anyhow::Error::msg)?
        .ok_or_else(|| anyhow!("delegation lifecycle is missing"))?;
    if lifecycle.generation != item.generation {
        return Err(anyhow!(
            "refusing to record terminal state for stale generation {} (current {})",
            item.generation,
            lifecycle.generation
        ));
    }
    record_terminal_evidence(&workspace, &item.scope_id, state, Some(detail))
        .map_err(anyhow::Error::msg)
}

async fn cleanup_unstarted_staged(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    staged: &[StagedDelegation],
) {
    for item in staged {
        if let Ok(workspace) = mux.resolve(&item.scope_id) {
            let _ = clear_delegation_lifecycle(&workspace, &item.scope_id);
        }
        if let Some(page) = &item.browser_page_id {
            let _ = close_browser_page(orca, page).await;
        }
        let _ = mux.remove(&item.scope_id);
    }
}

fn discover_workspace(explicit: Option<&Path>) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to determine current directory")?;
    discover_workspace_from(explicit, &cwd)
}

fn discover_workspace_from(explicit: Option<&Path>, cwd: &Path) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return canonical_directory(path)
            .with_context(|| format!("invalid --workspace {}", path.display()));
    }

    let git = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(cwd)
        .output();

    if let Ok(output) = git {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let root = text.trim();
            if !root.is_empty() {
                return canonical_directory(Path::new(root))
                    .context("git returned an invalid worktree root");
            }
        }
    }

    canonical_directory(cwd)
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let canonical = dunce::canonicalize(path)
        .with_context(|| format!("failed to canonicalize {}", path.display()))?;
    if !canonical.is_dir() {
        return Err(anyhow!(
            "workspace is not a directory: {}",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn bridge_port(base_url: &str) -> Result<u16> {
    let parsed = Url::parse(base_url).with_context(|| format!("invalid bridge URL: {base_url}"))?;
    parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("bridge URL has no resolvable port: {base_url}"))
}

async fn probe_bridge(client: &reqwest::Client, base_url: &str, token: Option<&str>) -> Result<()> {
    let mut request = client.get(format!("{base_url}/healthz"));
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = request
        .send()
        .await
        .with_context(|| format!("omo-bridge is not reachable at {base_url}"))?
        .error_for_status()
        .context("omo-bridge health check failed")?;
    let value: Value = response
        .json()
        .await
        .context("omo-bridge health response was not JSON")?;
    validate_bridge_health(&value, base_url)
}

fn validate_bridge_health(value: &Value, base_url: &str) -> Result<()> {
    if value.get("service").and_then(Value::as_str) != Some("omo-bridge") {
        return Err(anyhow!("unexpected service at {base_url}: {value}"));
    }
    if value.get("workspace_mode").and_then(Value::as_str) != Some("multiplexed_scopes") {
        return Err(anyhow!(
            "omo-bridge at {base_url} does not support multiplexed workspace scopes; rebuild/restart the v0.6+ daemon before delegating"
        ));
    }
    Ok(())
}

fn build_bootstrap_prompt(
    scope_id: &str,
    workspace: &Path,
    generation: u64,
    resumed: bool,
) -> String {
    let mode = if resumed {
        "This is a resume readiness handshake for the existing ChatGPT Web conversation."
    } else {
        "This is a readiness handshake for a fresh ChatGPT Web worker."
    };
    format!(
        "[OMO-BRIDGE READINESS BOOTSTRAP]\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\
GENERATION: {}\n\n\
{} The actual coding task for this generation has NOT been sent yet. Your only allowed readiness action now is to call the omo-bridge MCP tool task_state with exactly scope_id={}. If the task_state tool schema is not loaded yet, you may perform only the minimal connector/tool discovery required to expose that exact task_state tool, then call it immediately. Do not inspect files, edit, run commands, delegate, or start coding.\n\n\
A textual READY/OK/complete message is ignored and provides no readiness evidence. Readiness exists only if the scoped task_state MCP call succeeds and the bridge records it for this generation. After that successful tool call, stop and wait for the actual task prompt.",
        scope_id,
        workspace.display(),
        generation,
        mode,
        scope_id,
    )
}

fn build_delegation_prompt(
    scope_id: &str,
    workspace: &Path,
    generation: u64,
    resumed: bool,
    task: &str,
) -> String {
    let resume_guidance = if resumed {
        "This is a follow-up in the same retained ChatGPT Web conversation. Recover task_state first. If the previous plan is complete, create a new plan for the follow-up when needed. If a previously blocked item has been reopened as in_progress, preserve its blocker context and continue that existing plan rather than creating a competing plan."
    } else {
        "This is a fresh Web delegation. Recover task_state before non-trivial work and create a task plan when needed."
    };
    format!(
        "[OMO-BRIDGE DELEGATION]\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\
GENERATION: {}\n\n\
The authoritative readiness handshake for this generation has completed. You are the sole coding agent for this task. Every omo-bridge tool call MUST include exactly this scope_id: {}. Do not use another scope_id and do not access parent directories. All file/search/command paths are relative to WORKSPACE.\n\n\
{}\n\n\
Do not delegate implementation to OMO, OpenCode, Codex, or another coding agent. Use omo-bridge only as the local I/O, code-intelligence, execution, task-state, and completion harness. Use inspect -> task_state/task_plan -> search/AST/LSP/read -> patch -> test/build/diagnostics -> git_status_diff -> task_update -> completion_check. Successful completion is authoritative only when completion_check returns ready=true.\n\n\
If an external blocker makes further progress impossible, mark the affected item blocked with task_update and a concrete note; BLOCKED is terminal for this generation. Textual done/blocked/failed claims are never authoritative.\n\n\
TASK:\n{}",
        scope_id,
        workspace.display(),
        generation,
        scope_id,
        resume_guidance,
        task
    )
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use omo_bridge::tools::completion::handle_completion_check;
    use omo_bridge::tools::task_state::{
        handle_task_plan, handle_task_state, handle_task_update, record_verification,
        retain_session_with_lease, start_fresh_delegation_lifecycle,
    };
    use tempfile::tempdir;

    fn cli_for_test() -> Cli {
        Cli {
            task: Vec::new(),
            stdin: false,
            batch_stdin: false,
            resume_scope: None,
            close_scope: None,
            keep_session: false,
            close_on_terminal: false,
            session_ttl_minutes: DEFAULT_SESSION_TTL_MINUTES,
            workspace: None,
            mount_root: PathBuf::from("/"),
            bridge_url: "http://127.0.0.1:18800".into(),
            scope_dir: None,
            worktree: "active".into(),
            terminal: None,
            orca_bin: "orca".into(),
            token: None,
            dry_run: false,
            json: false,
        }
    }

    fn staged_for_scope(
        scope: WorkspaceScope,
        lifecycle: &DelegationLifecycle,
        resumed: bool,
    ) -> StagedDelegation {
        StagedDelegation {
            scope_id: scope.scope_id,
            workspace: scope.workspace,
            label: None,
            browser_page_id: scope.browser_page_id,
            generation: lifecycle.generation,
            generation_started_ms: lifecycle.generation_started_ms,
            resumed,
            bootstrap_prompt: Some("bootstrap".into()),
            task_prompt: Some("actual-task".into()),
        }
    }

    #[test]
    fn default_session_policy_is_idle_retained() {
        let cli = cli_for_test();
        assert!(!cli.close_on_terminal);
        assert_eq!(cli.session_ttl_minutes, 120);
        assert!(should_retain_terminal(
            DelegationTerminalState::Completed,
            cli.close_on_terminal
        ));
        assert!(should_retain_terminal(
            DelegationTerminalState::Blocked,
            cli.close_on_terminal
        ));
        assert!(should_retain_terminal(
            DelegationTerminalState::Failed,
            cli.close_on_terminal
        ));
        assert!(!should_retain_terminal(
            DelegationTerminalState::Lost,
            cli.close_on_terminal
        ));
    }

    #[test]
    fn explicit_close_on_terminal_overrides_retention() {
        for state in [
            DelegationTerminalState::Completed,
            DelegationTerminalState::Blocked,
            DelegationTerminalState::Failed,
            DelegationTerminalState::Lost,
        ] {
            assert!(!should_retain_terminal(state, true));
        }
    }

    #[test]
    fn ttl_validation_is_bounded() {
        assert_eq!(session_ttl_ms(120).unwrap(), 7_200_000);
        assert!(session_ttl_ms(0).is_err());
        assert!(session_ttl_ms(u64::MAX).is_err());
    }

    #[test]
    fn rejects_more_than_two_parallel_tasks() {
        assert!(validate_parallel_count(2).is_ok());
        let error = validate_parallel_count(3).unwrap_err().to_string();
        assert!(error.contains("limited to 2 workers"));
    }

    #[test]
    fn resumed_scope_uses_exact_stored_browser_page_id() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let scope = mux
            .register_browser(&project, "retained-browser-page".into())
            .unwrap();
        mux.update_browser_conversation_id(&scope.scope_id, "retained-conversation")
            .unwrap();
        let ws = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&ws, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &ws,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&ws, &scope.scope_id, 60_000).unwrap();

        let (loaded, _, lifecycle) = load_resumable_scope(&mux, &scope.scope_id).unwrap();
        assert_eq!(
            loaded.browser_page_id.as_deref(),
            Some("retained-browser-page")
        );
        assert!(lifecycle.session_retained);
        assert!(lifecycle.lease_expires_ms.is_some());
    }

    #[test]
    fn stale_generation_readiness_means_zero_actual_task_dispatches() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let scope = mux.register_browser(&project, "page-1".into()).unwrap();
        let ws = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = start_fresh_delegation_lifecycle(&ws, &scope.scope_id).unwrap();
        let staged = vec![staged_for_scope(scope, &lifecycle, false)];
        assert!(handle_task_state(&ws, &staged[0].scope_id).success);
        record_terminal_evidence(
            &ws,
            &staged[0].scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&ws, &staged[0].scope_id, 60_000).unwrap();
        start_next_delegation_generation(&ws, &staged[0].scope_id, false).unwrap();

        assert!(actual_dispatch_plan(&mux, &staged, epoch_ms())
            .unwrap()
            .is_empty());
    }

    #[test]
    fn one_unready_worker_means_zero_actual_task_dispatches_for_whole_batch() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let mut staged = Vec::new();
        for page in ["page-1", "page-2", "page-3"] {
            let scope = mux.register_browser(&project, page.into()).unwrap();
            let ws = mux.resolve(&scope.scope_id).unwrap();
            let lifecycle = start_fresh_delegation_lifecycle(&ws, &scope.scope_id).unwrap();
            staged.push(staged_for_scope(scope, &lifecycle, false));
        }
        for index in [0, 2] {
            let ws = mux.resolve(&staged[index].scope_id).unwrap();
            assert!(handle_task_state(&ws, &staged[index].scope_id).success);
        }

        assert!(actual_dispatch_plan(&mux, &staged, epoch_ms())
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn completed_terminal_state_is_observed_for_omo_batch_result() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(&project)
            .status()
            .unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let scope = mux
            .register_browser(&project, "page-complete".into())
            .unwrap();
        let ws = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = start_fresh_delegation_lifecycle(&ws, &scope.scope_id).unwrap();
        let staged = vec![staged_for_scope(scope, &lifecycle, false)];
        assert!(handle_task_plan(
            &ws,
            &staged[0].scope_id,
            "complete smoke",
            vec!["verify".into()],
        )
        .success);
        assert!(handle_task_update(&ws, &staged[0].scope_id, "T1", "done", None).success);
        record_verification(&ws, &staged[0].scope_id, "cargo test", true, Some(0), 10);
        let result = handle_completion_check(
            &ws,
            &staged[0].scope_id,
            None,
            None,
            Some(false),
        );
        assert!(result.success);
        assert_eq!(result.data.unwrap()["ready"], true);

        let terminal = wait_for_terminal_states(&mux, &staged, Duration::from_millis(20)).await;
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].state, DelegationTerminalState::Completed);
    }

    #[tokio::test]
    async fn blocked_terminal_state_is_observed_for_omo_batch_result() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let scope = mux
            .register_browser(&project, "page-blocked".into())
            .unwrap();
        let ws = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = start_fresh_delegation_lifecycle(&ws, &scope.scope_id).unwrap();
        let staged = vec![staged_for_scope(scope, &lifecycle, false)];
        assert!(
            handle_task_plan(
                &ws,
                &staged[0].scope_id,
                "blocked smoke",
                vec!["external dependency".into()]
            )
            .success
        );
        assert!(
            handle_task_update(
                &ws,
                &staged[0].scope_id,
                "T1",
                "blocked",
                Some("external blocker")
            )
            .success
        );

        let terminal = wait_for_terminal_states(&mux, &staged, Duration::from_millis(20)).await;
        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].state, DelegationTerminalState::Blocked);
    }

    #[test]
    fn bootstrap_and_followup_prompts_encode_generation_and_resume_contract() {
        let scope = "44444444-4444-4444-8444-444444444444";
        let bootstrap = build_bootstrap_prompt(scope, Path::new("/tmp/project"), 2, true);
        assert!(bootstrap.contains("GENERATION: 2"));
        assert!(bootstrap.contains("resume readiness handshake"));
        assert!(bootstrap.contains("task_state"));
        assert!(bootstrap.contains("minimal connector/tool discovery"));
        assert!(!bootstrap.contains("fix tests"));

        let task = build_delegation_prompt(scope, Path::new("/tmp/project"), 2, true, "fix tests");
        assert!(task.contains("GENERATION: 2"));
        assert!(task.contains("same retained ChatGPT Web conversation"));
        assert!(task.contains("fix tests"));
        assert!(task.contains("completion_check"));
    }

    #[test]
    fn discovers_git_root_before_current_subdirectory() {
        let dir = tempdir().unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        let nested = dir.path().join("a/b");
        std::fs::create_dir_all(&nested).unwrap();

        let discovered = discover_workspace_from(None, &nested).unwrap();
        assert_eq!(discovered, dunce::canonicalize(dir.path()).unwrap());
    }

    #[test]
    fn falls_back_to_current_directory_outside_git() {
        let dir = tempdir().unwrap();
        let discovered = discover_workspace_from(None, dir.path()).unwrap();
        assert_eq!(discovered, dunce::canonicalize(dir.path()).unwrap());
    }

    #[test]
    fn explicit_workspace_overrides_git_discovery() {
        let git_dir = tempdir().unwrap();
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(git_dir.path())
            .status()
            .unwrap();
        let explicit = tempdir().unwrap();
        let discovered = discover_workspace_from(Some(explicit.path()), git_dir.path()).unwrap();
        assert_eq!(discovered, dunce::canonicalize(explicit.path()).unwrap());
    }

    #[test]
    fn accepts_multiplexed_bridge_health_shape() {
        let current = serde_json::json!({
            "service": "omo-bridge",
            "version": "0.7.0",
            "workspace_mode": "multiplexed_scopes"
        });
        validate_bridge_health(&current, "http://127.0.0.1:18800").unwrap();
    }

    #[test]
    fn derives_default_scope_dir_from_bridge_port() {
        let port = bridge_port("http://127.0.0.1:18800").unwrap();
        assert_eq!(port, 18800);
        assert!(default_scope_dir(port)
            .to_string_lossy()
            .contains("scopes-18800"));
    }

    #[test]
    fn cli_fixture_preserves_two_worker_cap_and_resume_is_single_scope() {
        let cli = cli_for_test();
        assert!(!cli.batch_stdin);
        assert!(cli.resume_scope.is_none());
        assert_eq!(MAX_PARALLEL_WEB_WORKERS, 2);
    }
}
