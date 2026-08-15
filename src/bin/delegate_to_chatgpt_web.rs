use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::future::join_all;
use omo_bridge::orca::{close_browser_page, create_chatgpt_tab, send_chatgpt_prompt, OrcaConfig};
use omo_bridge::tools::task_state::{
    clear_delegation_lifecycle, load_delegation_lifecycle, record_terminal_evidence,
    DelegationLifecycle, DelegationTerminalState,
};
use omo_bridge::{default_scope_dir, WorkspaceMux};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Instant};
use url::Url;

const MAX_PARALLEL_WEB_WORKERS: usize = 3;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const READINESS_TIMEOUT: Duration = Duration::from_secs(60);
const READINESS_FRESHNESS_MS: u64 = 90_000;
const TERMINAL_TIMEOUT: Duration = Duration::from_secs(4 * 60 * 60);
const LIFECYCLE_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Parser, Debug, Clone)]
#[command(
    name = "delegate_to_chatgpt_web",
    version,
    about = "Create up to three isolated ChatGPT Web coding delegations through omo-bridge",
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

    /// Override automatic workspace discovery for a single task, or provide the default workspace
    /// for batch items that do not specify one. OMO remains responsible for worktree selection.
    #[arg(long, env = "OMO_WORKSPACE")]
    workspace: Option<PathBuf>,

    /// Broad mount root used by the running bridge daemon.
    #[arg(long, default_value = "/")]
    mount_root: PathBuf,

    /// omo-bridge base URL.
    #[arg(long, default_value = "http://127.0.0.1:18800")]
    bridge_url: String,

    /// Override the shared directory that stores per-delegation workspace scopes.
    #[arg(long)]
    scope_dir: Option<PathBuf>,

    /// Orca worktree selector used for browser tabs.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Legacy terminal selector retained for compatibility. Browser-scoped delegations do not use it.
    #[arg(long, env = "OMO_RELAY_TERMINAL")]
    terminal: Option<String>,

    /// Orca CLI executable.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Optional bearer token used by omo-bridge.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    token: Option<String>,

    /// Validate workspaces and create scopes, but do not create ChatGPT tabs or send prompts.
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
    created_ms: u64,
    bootstrap_prompt: Option<String>,
    task_prompt: Option<String>,
}

#[derive(Clone, Debug)]
struct TerminalObservation {
    state: DelegationTerminalState,
    detail: Option<String>,
    terminal_ms: Option<u64>,
}

#[derive(Default)]
struct BatchOutcome {
    readiness_complete: bool,
    terminal_complete: bool,
    actual_sent: Vec<bool>,
    terminal: Vec<TerminalObservation>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let tasks = prepare_tasks(&cli)?;

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

    let staged = if cli.dry_run {
        stage_dry_run(&mux, &tasks)?
    } else {
        stage_browser_delegations(&mux, &orca, &tasks).await?
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
        cleanup_staged(&mux, &orca, &staged).await;
        return Err(error.context(
            "readiness bootstrap failed; actual task dispatch count is 0 and all staged tabs/scopes were cleaned up",
        ));
    }

    if let Err(error) = wait_for_all_ready(&mux, &staged, READINESS_TIMEOUT).await {
        cleanup_staged(&mux, &orca, &staged).await;
        return Err(error.context(
            "authoritative readiness handshake failed closed; actual task dispatch count is 0 and all staged tabs/scopes were cleaned up",
        ));
    }

    let actual_sent = match dispatch_actual_tasks(&mux, &orca, &staged).await {
        Ok(sent) => sent,
        Err(error) => {
            cleanup_staged(&mux, &orca, &staged).await;
            return Err(error.context(
                "readiness became invalid before dispatch; actual task dispatch count is 0 and all staged tabs/scopes were cleaned up",
            ));
        }
    };

    let terminal = wait_for_terminal_states(&mux, &staged, TERMINAL_TIMEOUT).await;
    cleanup_browser_pages(&orca, &staged).await;

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
        },
    )?;
    Ok(())
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
            serde_json::json!({
                "index": index + 1,
                "label": item.label,
                "scope_id": item.scope_id,
                "workspace": item.workspace,
                "browser_page_id": item.browser_page_id,
                "ready": outcome.readiness_complete,
                "actual_task_sent": outcome.actual_sent.get(index).copied().unwrap_or(false),
                "terminal_state": terminal_observation.map(|state| state.state),
                "terminal_detail": terminal_observation.and_then(|state| state.detail.clone()),
                "terminal_ms": terminal_observation.and_then(|state| state.terminal_ms),
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
    let result = serde_json::json!({
        "ok": if cli.dry_run { true } else { all_completed && all_sent },
        "sent": all_sent,
        "ready": outcome.readiness_complete,
        "terminal": outcome.terminal_complete,
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
            println!(
                "{}. scope={} workspace={} page={} terminal={}",
                index + 1,
                item.scope_id,
                item.workspace,
                item.browser_page_id.as_deref().unwrap_or("<missing>"),
                state
            );
        }
    }

    Ok(())
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
                created_ms: scope.created_ms,
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
                cleanup_staged(mux, orca, &staged).await;
                return Err(error.context("failed to create a fresh ChatGPT Web worker tab"));
            }
        };
        let scope = match mux.register_browser(&task.workspace, page.clone()) {
            Ok(scope) => scope,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                cleanup_staged(mux, orca, &staged).await;
                return Err(error.into());
            }
        };
        let workspace = match mux.resolve(&scope.scope_id) {
            Ok(workspace) => workspace,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                let _ = mux.remove(&scope.scope_id);
                cleanup_staged(mux, orca, &staged).await;
                return Err(error.into());
            }
        };
        if let Err(error) = clear_delegation_lifecycle(&workspace, &scope.scope_id) {
            let _ = close_browser_page(orca, &page).await;
            let _ = mux.remove(&scope.scope_id);
            cleanup_staged(mux, orca, &staged).await;
            return Err(anyhow!(error));
        }
        let bootstrap_prompt = build_bootstrap_prompt(&scope.scope_id, Path::new(&scope.workspace));
        let task_prompt =
            build_delegation_prompt(&scope.scope_id, Path::new(&scope.workspace), &task.task);
        staged.push(StagedDelegation {
            scope_id: scope.scope_id,
            workspace: scope.workspace,
            label: task.label.clone(),
            browser_page_id: Some(page),
            created_ms: scope.created_ms,
            bootstrap_prompt: Some(bootstrap_prompt),
            task_prompt: Some(task_prompt),
        });
    }
    Ok(staged)
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
    staged: &[StagedDelegation],
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let now = epoch_ms();
        let mut pending = Vec::new();
        for (index, item) in staged.iter().enumerate() {
            let lifecycle = lifecycle_for(mux, item)?;
            if let Some(lifecycle) = lifecycle.as_ref() {
                if let Some(state) = lifecycle.terminal_state {
                    return Err(anyhow!(
                        "worker {} entered terminal state {:?} before actual task dispatch",
                        index + 1,
                        state
                    ));
                }
            }
            if !has_fresh_readiness(item, lifecycle.as_ref(), now) {
                pending.push(index + 1);
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
                    .map(usize::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
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
        if lifecycle
            .as_ref()
            .and_then(|state| state.terminal_state)
            .is_some()
            || !has_fresh_readiness(item, lifecycle.as_ref(), now_ms)
        {
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
                if let Some(page) = &staged[index].browser_page_id {
                    let _ = close_browser_page(orca, page).await;
                }
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
                Ok(Some(lifecycle)) => {
                    if let Some(state) = lifecycle.terminal_state {
                        observed[index] = Some(TerminalObservation {
                            state,
                            detail: lifecycle.terminal_detail,
                            terminal_ms: lifecycle.terminal_ms,
                        });
                    }
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

fn lifecycle_for(
    mux: &WorkspaceMux,
    item: &StagedDelegation,
) -> Result<Option<DelegationLifecycle>> {
    let workspace = mux.resolve(&item.scope_id)?;
    load_delegation_lifecycle(&workspace, &item.scope_id).map_err(|error| anyhow!(error))
}

fn has_fresh_readiness(
    item: &StagedDelegation,
    lifecycle: Option<&DelegationLifecycle>,
    now_ms: u64,
) -> bool {
    let Some(ready_ms) = lifecycle.and_then(|state| state.ready_ms) else {
        return false;
    };
    ready_ms >= item.created_ms
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
    record_terminal_evidence(&workspace, &item.scope_id, state, Some(detail))
        .map_err(|error| anyhow!(error))
}

async fn cleanup_staged(mux: &WorkspaceMux, orca: &OrcaConfig, staged: &[StagedDelegation]) {
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

async fn cleanup_browser_pages(orca: &OrcaConfig, staged: &[StagedDelegation]) {
    for item in staged {
        if let Some(page) = &item.browser_page_id {
            let _ = close_browser_page(orca, page).await;
        }
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

fn build_bootstrap_prompt(scope_id: &str, workspace: &Path) -> String {
    format!(
        "[OMO-BRIDGE READINESS BOOTSTRAP]\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\n\
This is a readiness handshake only. The actual coding task has NOT been sent yet. Your only allowed action now is to call the omo-bridge MCP tool task_state with exactly scope_id={}. Do not inspect files, create a task plan, edit, run commands, delegate, or start coding.\n\n\
A textual READY/OK/complete message is ignored and provides no readiness evidence. Readiness exists only if the task_state MCP call succeeds and the bridge records it server-side for this scope. After that successful tool call, stop and wait for the actual task prompt. If the MCP schema or scope_id field is unavailable, do not fabricate readiness.",
        scope_id,
        workspace.display(),
        scope_id,
    )
}

fn build_delegation_prompt(scope_id: &str, workspace: &Path, task: &str) -> String {
    format!(
        "[OMO-BRIDGE DELEGATION]\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\n\
The authoritative readiness handshake for this fresh ChatGPT Web worker has completed. You are the sole coding agent for this task. This conversation is isolated to the scope above. Every omo-bridge tool call MUST include exactly this scope_id: {}. Do not use another scope_id and do not access parent directories. All file/search/command paths are relative to WORKSPACE.\n\n\
Do not delegate implementation to OMO, OpenCode, Codex, or another coding agent. Use omo-bridge only as the local I/O, code-intelligence, execution, task-state, and completion harness.\n\n\
Recover task_state with this scope_id before non-trivial work, then use inspect -> task_state/task_plan -> search/AST/LSP/read -> patch -> test/build/diagnostics -> git_status_diff -> task_update -> completion_check. Successful completion is authoritative only when completion_check returns ready=true. If completion_check.ready is false, continue until it is true.\n\n\
If an external blocker makes further progress impossible, do not merely say BLOCKED. Use the existing task plan: mark the affected task-plan item blocked with task_update and a concrete blocker note, then call task_state once to reconcile server-side BLOCKED evidence. BLOCKED is terminal. If expected MCP tools or the scope_id field are missing, treat that as an MCP schema/reconnect blocker and record it through the task plan when possible.\n\n\
TASK:\n{}",
        scope_id,
        workspace.display(),
        scope_id,
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
    use omo_bridge::tools::task_state::{handle_task_plan, handle_task_state, handle_task_update};
    use tempfile::tempdir;

    fn cli_for_test() -> Cli {
        Cli {
            task: Vec::new(),
            stdin: false,
            batch_stdin: false,
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

    fn staged_for_scope(scope: omo_bridge::WorkspaceScope) -> StagedDelegation {
        StagedDelegation {
            scope_id: scope.scope_id,
            workspace: scope.workspace,
            label: None,
            browser_page_id: scope.browser_page_id,
            created_ms: scope.created_ms,
            bootstrap_prompt: Some("bootstrap".into()),
            task_prompt: Some("actual-task".into()),
        }
    }

    #[test]
    fn rejects_more_than_three_parallel_tasks() {
        assert!(validate_parallel_count(3).is_ok());
        let error = validate_parallel_count(4).unwrap_err().to_string();
        assert!(error.contains("limited to 3 workers"));
    }

    #[test]
    fn one_worker_readiness_smoke_allows_one_actual_dispatch() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let scope = mux.register_browser(&project, "page-1".into()).unwrap();
        let staged = vec![staged_for_scope(scope)];
        let ws = mux.resolve(&staged[0].scope_id).unwrap();
        clear_delegation_lifecycle(&ws, &staged[0].scope_id).unwrap();

        assert!(handle_task_state(&ws, &staged[0].scope_id).success);
        let plan = actual_dispatch_plan(&mux, &staged, epoch_ms()).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].0, "page-1");
    }

    #[test]
    fn three_worker_readiness_smoke_requires_all_three_before_dispatch() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let staged = (1..=3)
            .map(|index| {
                staged_for_scope(
                    mux.register_browser(&project, format!("page-{index}"))
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();

        for item in &staged {
            let ws = mux.resolve(&item.scope_id).unwrap();
            clear_delegation_lifecycle(&ws, &item.scope_id).unwrap();
            assert!(handle_task_state(&ws, &item.scope_id).success);
        }
        let plan = actual_dispatch_plan(&mux, &staged, epoch_ms()).unwrap();
        assert_eq!(plan.len(), 3);
    }

    #[test]
    fn unready_or_stale_worker_means_zero_actual_task_dispatches() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let mut staged = (1..=3)
            .map(|index| {
                staged_for_scope(
                    mux.register_browser(&project, format!("page-{index}"))
                        .unwrap(),
                )
            })
            .collect::<Vec<_>>();

        for item in staged.iter().take(2) {
            let ws = mux.resolve(&item.scope_id).unwrap();
            clear_delegation_lifecycle(&ws, &item.scope_id).unwrap();
            assert!(handle_task_state(&ws, &item.scope_id).success);
        }
        assert!(actual_dispatch_plan(&mux, &staged, epoch_ms())
            .unwrap()
            .is_empty());

        let ws = mux.resolve(&staged[2].scope_id).unwrap();
        assert!(handle_task_state(&ws, &staged[2].scope_id).success);
        staged[0].created_ms = u64::MAX;
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
        let staged = vec![staged_for_scope(
            mux.register_browser(&project, "page-complete".into())
                .unwrap(),
        )];
        let ws = mux.resolve(&staged[0].scope_id).unwrap();
        clear_delegation_lifecycle(&ws, &staged[0].scope_id).unwrap();
        let result = handle_completion_check(
            &ws,
            &staged[0].scope_id,
            Some(false),
            Some(false),
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
        let staged = vec![staged_for_scope(
            mux.register_browser(&project, "page-blocked".into())
                .unwrap(),
        )];
        let ws = mux.resolve(&staged[0].scope_id).unwrap();
        clear_delegation_lifecycle(&ws, &staged[0].scope_id).unwrap();
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
    fn bootstrap_contains_no_actual_task_and_requires_mcp_evidence() {
        let scope = "44444444-4444-4444-8444-444444444444";
        let bootstrap = build_bootstrap_prompt(scope, Path::new("/tmp/project"));
        assert!(bootstrap.contains("task_state"));
        assert!(bootstrap.contains("actual coding task has NOT been sent"));
        assert!(bootstrap.contains("textual READY/OK/complete message is ignored"));
        assert!(!bootstrap.contains("fix tests"));

        let task = build_delegation_prompt(scope, Path::new("/tmp/project"), "fix tests");
        assert!(task.contains("fix tests"));
        assert!(task.contains("completion_check returns ready=true"));
        assert!(task.contains("task_update"));
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
    fn cli_fixture_still_defaults_to_single_process_helper_contract() {
        let cli = cli_for_test();
        assert!(!cli.batch_stdin);
        assert_eq!(MAX_PARALLEL_WEB_WORKERS, 3);
    }
}
