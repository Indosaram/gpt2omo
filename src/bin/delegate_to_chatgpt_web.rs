use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::future::join_all;
use omo_bridge::orca::{close_browser_page, create_chatgpt_tab, send_chatgpt_prompt, OrcaConfig};
use omo_bridge::{default_scope_dir, WorkspaceMux};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use url::Url;

const MAX_PARALLEL_WEB_WORKERS: usize = 3;
const MAX_INPUT_BYTES: usize = 1024 * 1024;

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
    prompt: Option<String>,
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

    if !cli.dry_run {
        dispatch_staged(&mux, &orca, &staged).await?;
    }

    let delegations = staged
        .iter()
        .enumerate()
        .map(|(index, item)| {
            serde_json::json!({
                "index": index + 1,
                "label": item.label,
                "scope_id": item.scope_id,
                "workspace": item.workspace,
                "browser_page_id": item.browser_page_id,
            })
        })
        .collect::<Vec<_>>();
    let result = serde_json::json!({
        "ok": true,
        "sent": !cli.dry_run,
        "parallel_count": staged.len(),
        "max_parallel": MAX_PARALLEL_WEB_WORKERS,
        "scope_dir": scope_dir.to_string_lossy(),
        "bridge_url": bridge_url,
        "delegations": delegations,
    });

    if cli.json {
        println!("{}", serde_json::to_string(&result)?);
    } else {
        println!(
            "{} {} ChatGPT Web worker(s)",
            if cli.dry_run { "Prepared" } else { "Spawned" },
            staged.len()
        );
        for (index, item) in staged.iter().enumerate() {
            println!(
                "{}. scope={} workspace={} page={}",
                index + 1,
                item.scope_id,
                item.workspace,
                item.browser_page_id.as_deref().unwrap_or("<dry-run>")
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

    if raw_tasks.is_empty() {
        return Err(anyhow!("at least one Web delegation task is required"));
    }
    if raw_tasks.len() > MAX_PARALLEL_WEB_WORKERS {
        return Err(anyhow!(
            "parallel Web delegation is limited to {} workers; received {}",
            MAX_PARALLEL_WEB_WORKERS,
            raw_tasks.len()
        ));
    }

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
                prompt: None,
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
        let prompt =
            build_delegation_prompt(&scope.scope_id, Path::new(&scope.workspace), &task.task);
        staged.push(StagedDelegation {
            scope_id: scope.scope_id,
            workspace: scope.workspace,
            label: task.label.clone(),
            browser_page_id: Some(page),
            prompt: Some(prompt),
        });
    }
    Ok(staged)
}

async fn dispatch_staged(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
    staged: &[StagedDelegation],
) -> Result<()> {
    let futures = staged.iter().map(|item| {
        let page = item.browser_page_id.as_deref().unwrap_or_default();
        let prompt = item.prompt.as_deref().unwrap_or_default();
        send_chatgpt_prompt(orca, page, prompt)
    });
    let results = join_all(futures).await;
    let mut failures = Vec::new();
    for (index, result) in results.into_iter().enumerate() {
        if let Err(error) = result {
            let item = &staged[index];
            if let Some(page) = &item.browser_page_id {
                let _ = close_browser_page(orca, page).await;
            }
            let _ = mux.remove(&item.scope_id);
            failures.push(format!("worker {}: {}", index + 1, error));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "one or more ChatGPT Web workers failed to start: {}",
            failures.join("; ")
        ))
    }
}

async fn cleanup_staged(mux: &WorkspaceMux, orca: &OrcaConfig, staged: &[StagedDelegation]) {
    for item in staged {
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

fn build_delegation_prompt(scope_id: &str, workspace: &Path, task: &str) -> String {
    format!(
        "[OMO-BRIDGE DELEGATION]\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\n\
You are the sole coding agent for this task. This ChatGPT Web conversation is isolated to the scope above. Every omo-bridge tool call MUST include exactly this scope_id: {}. Do not use another scope_id and do not access parent directories. All file/search/command paths are relative to WORKSPACE.\n\n\
Do not delegate implementation to OMO, OpenCode, Codex, or another coding agent. Use omo-bridge only as the local I/O, code-intelligence, execution, task-state, and completion harness.\n\n\
At the start call task_state with this scope_id. For non-trivial work use inspect -> task_state/task_plan -> search/AST/LSP/read -> patch -> test/build/diagnostics -> git_status_diff -> task_update -> completion_check. If completion_check.ready is false, continue until it is true unless an external blocker makes progress impossible. If expected MCP tools or the scope_id field are missing, treat that as an MCP schema/reconnect problem rather than a server implementation failure.\n\n\
TASK:\n{}",
        scope_id,
        workspace.display(),
        scope_id,
        task
    )
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn rejects_more_than_three_parallel_tasks() {
        let dir = tempdir().unwrap();
        let mut cli = cli_for_test();
        cli.workspace = Some(dir.path().to_path_buf());
        let manifest = BatchManifest {
            tasks: (0..4)
                .map(|index| BatchTask {
                    task: format!("task {index}"),
                    workspace: None,
                    label: None,
                })
                .collect(),
        };
        assert_eq!(manifest.tasks.len(), 4);
        assert_eq!(MAX_PARALLEL_WEB_WORKERS, 3);
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
    fn prompt_contains_scope_workspace_and_single_agent_contract() {
        let scope = "44444444-4444-4444-8444-444444444444";
        let prompt = build_delegation_prompt(scope, Path::new("/tmp/project"), "fix tests");
        assert!(prompt.contains(scope));
        assert!(prompt.contains("WORKSPACE: /tmp/project"));
        assert!(prompt.contains("sole coding agent"));
        assert!(prompt.contains("completion_check"));
        assert!(prompt.contains("fix tests"));
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
}
