use anyhow::{anyhow, Context, Result};
use futures::future::join_all;
use gpt2omo::orca::{close_browser_page, create_chatgpt_tab, send_chatgpt_prompt, OrcaConfig};
use gpt2omo::tools::task_state::{
    clear_delegation_lifecycle, load_delegation_lifecycle, start_fresh_delegation_lifecycle,
};
use gpt2omo::{default_scope_dir, WorkspaceMux};
use std::path::Path;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{sleep, Instant};

const DEFAULT_BRIDGE_PORT: u16 = 18_800;
const LIVE_READINESS_TIMEOUT: Duration = Duration::from_secs(180);
const BOOTSTRAP_RETRY_AFTER: Duration = Duration::from_secs(45);
const POLL_INTERVAL: Duration = Duration::from_millis(250);

static LIVE_SMOKE_LOCK: Mutex<()> = Mutex::const_new(());

#[derive(Debug)]
struct LiveWorker {
    scope_id: String,
    browser_page_id: String,
    generation: u64,
    generation_started_ms: u64,
}

fn bridge_port() -> Result<u16> {
    match std::env::var("OMO_BRIDGE_PORT") {
        Ok(value) => value
            .parse::<u16>()
            .with_context(|| format!("invalid OMO_BRIDGE_PORT value: {value}")),
        Err(_) => Ok(DEFAULT_BRIDGE_PORT),
    }
}

fn live_orca_config() -> OrcaConfig {
    OrcaConfig::new(
        std::env::var("OMO_ORCA_WORKTREE").unwrap_or_else(|_| "active".into()),
        None,
        std::env::var("OMO_ORCA_BIN").unwrap_or_else(|_| "orca".into()),
    )
}

fn readiness_bootstrap(scope_id: &str, workspace: &Path, generation: u64) -> String {
    format!(
        "[GPT2OMO READINESS BOOTSTRAP]\n\
SCOPE_ID: {scope_id}\n\
WORKSPACE: {}\n\
GENERATION: {generation}\n\n\
This is a readiness handshake for a fresh ChatGPT Web worker. The actual coding task for this generation has NOT been sent yet. Your only allowed readiness action now is to call the gpt2omo MCP tool task_state with exactly scope_id={scope_id}. If the task_state tool schema is not loaded yet, you may perform only the minimal connector/tool discovery required to expose that exact task_state tool, then call it immediately. Do not inspect files, edit, run commands, delegate, or start coding.\n\n\
A textual READY/OK/complete message is ignored and provides no readiness evidence. Readiness exists only if the scoped task_state MCP call succeeds and the bridge records it for this generation. After that successful tool call, stop and wait for the actual task prompt.",
        workspace.display(),
    )
}

async fn cleanup_workers(mux: &WorkspaceMux, orca: &OrcaConfig, workers: &[LiveWorker]) {
    for worker in workers {
        if let Ok(workspace) = mux.resolve(&worker.scope_id) {
            let _ = clear_delegation_lifecycle(&workspace, &worker.scope_id);
        }
        let _ = close_browser_page(orca, &worker.browser_page_id).await;
        let _ = mux.remove(&worker.scope_id);
    }
}

async fn stage_workers(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    workspace: &Path,
    count: usize,
) -> Result<Vec<LiveWorker>> {
    let mut workers = Vec::with_capacity(count);
    for _ in 0..count {
        let page = match create_chatgpt_tab(orca).await {
            Ok(page) => page,
            Err(error) => {
                cleanup_workers(mux, orca, &workers).await;
                return Err(error.context("failed to create live ChatGPT Web smoke tab"));
            }
        };
        let scope = match mux.register_browser(workspace, page.clone()) {
            Ok(scope) => scope,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                cleanup_workers(mux, orca, &workers).await;
                return Err(error.into());
            }
        };
        let scoped_workspace = match mux.resolve(&scope.scope_id) {
            Ok(workspace) => workspace,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                let _ = mux.remove(&scope.scope_id);
                cleanup_workers(mux, orca, &workers).await;
                return Err(error.into());
            }
        };
        let lifecycle = match start_fresh_delegation_lifecycle(&scoped_workspace, &scope.scope_id) {
            Ok(lifecycle) => lifecycle,
            Err(error) => {
                let _ = close_browser_page(orca, &page).await;
                let _ = mux.remove(&scope.scope_id);
                cleanup_workers(mux, orca, &workers).await;
                return Err(anyhow!(error));
            }
        };
        workers.push(LiveWorker {
            scope_id: scope.scope_id,
            browser_page_id: page,
            generation: lifecycle.generation,
            generation_started_ms: lifecycle.generation_started_ms,
        });
    }
    Ok(workers)
}

async fn dispatch_bootstraps(
    orca: &OrcaConfig,
    workspace: &Path,
    workers: &[&LiveWorker],
) -> Result<()> {
    let prompts = workers
        .iter()
        .map(|worker| readiness_bootstrap(&worker.scope_id, workspace, worker.generation))
        .collect::<Vec<_>>();
    let results = join_all(
        workers
            .iter()
            .zip(prompts.iter())
            .map(|(worker, prompt)| send_chatgpt_prompt(orca, &worker.browser_page_id, prompt)),
    )
    .await;
    let failures = results
        .into_iter()
        .enumerate()
        .filter_map(|(index, result)| {
            result
                .err()
                .map(|error| format!("worker {}: {error}", index + 1))
        })
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "live readiness bootstrap dispatch failed: {}",
            failures.join("; ")
        ))
    }
}

fn pending_workers<'a>(
    mux: &WorkspaceMux,
    workers: &'a [LiveWorker],
) -> Result<Vec<&'a LiveWorker>> {
    let mut pending = Vec::new();
    for worker in workers {
        let workspace = mux.resolve(&worker.scope_id)?;
        let lifecycle =
            load_delegation_lifecycle(&workspace, &worker.scope_id).map_err(anyhow::Error::msg)?;
        let ready = lifecycle.as_ref().is_some_and(|lifecycle| {
            lifecycle.generation == worker.generation
                && lifecycle.generation_started_ms == worker.generation_started_ms
                && lifecycle.terminal_state.is_none()
                && lifecycle
                    .ready_ms
                    .is_some_and(|ready_ms| ready_ms >= worker.generation_started_ms)
        });
        if !ready {
            pending.push(worker);
        }
    }
    Ok(pending)
}

async fn wait_for_authoritative_readiness(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    workspace: &Path,
    workers: &[LiveWorker],
) -> Result<()> {
    let started = Instant::now();
    let deadline = started + LIVE_READINESS_TIMEOUT;
    let mut retried = false;
    loop {
        let pending = pending_workers(mux, workers)?;
        if pending.is_empty() {
            return Ok(());
        }
        if !retried && Instant::now().duration_since(started) >= BOOTSTRAP_RETRY_AFTER {
            dispatch_bootstraps(orca, workspace, &pending).await?;
            retried = true;
            continue;
        }
        if Instant::now() >= deadline {
            return Err(anyhow!(
                "live readiness timeout; unready scopes: {}",
                pending
                    .iter()
                    .map(|worker| worker.scope_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn run_live_readiness_smoke(count: usize) -> Result<()> {
    if !(1..=3).contains(&count) {
        return Err(anyhow!("live smoke worker count must be between 1 and 3"));
    }

    let workspace_dir = tempfile::tempdir()?;
    let workspace = dunce::canonicalize(workspace_dir.path())?;
    let scope_dir = default_scope_dir(bridge_port()?);
    let mux = WorkspaceMux::new(Path::new("/"), &scope_dir)?;
    let orca = live_orca_config();
    let workers = stage_workers(&mux, &orca, &workspace, count).await?;
    let worker_refs = workers.iter().collect::<Vec<_>>();

    let result = async {
        dispatch_bootstraps(&orca, &workspace, &worker_refs).await?;
        wait_for_authoritative_readiness(&mux, &orca, &workspace, &workers).await
    }
    .await;

    cleanup_workers(&mux, &orca, &workers).await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Orca browser, logged-in ChatGPT Web, and the running gpt2omo MCP connector"]
async fn live_one_worker_authoritative_readiness_smoke() {
    let _guard = LIVE_SMOKE_LOCK.lock().await;
    run_live_readiness_smoke(1).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Orca browser, logged-in ChatGPT Web, and the running gpt2omo MCP connector"]
async fn live_three_worker_authoritative_readiness_smoke() {
    let _guard = LIVE_SMOKE_LOCK.lock().await;
    run_live_readiness_smoke(3).await.unwrap();
}
