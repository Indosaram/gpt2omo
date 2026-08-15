use anyhow::{anyhow, Context, Result};
use clap::Parser;
use omo_bridge::orca::{resolve_terminal, send_prompt, OrcaConfig};
use omo_bridge::{default_scope_dir, WorkspaceMux};
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use url::Url;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "delegate_to_chatgpt_web",
    version,
    about = "Create an isolated workspace scope and delegate one coding task to ChatGPT Web",
    trailing_var_arg = true
)]
struct Cli {
    /// Task text to send to ChatGPT Web. Multiple trailing words are joined with spaces.
    #[arg(value_name = "TASK", required_unless_present = "stdin")]
    task: Vec<String>,

    /// Read the complete task text from stdin. Used by the OpenCode /delegate-web command.
    #[arg(long, conflicts_with = "task")]
    stdin: bool,

    /// Override automatic workspace discovery. By default the Git worktree root is used,
    /// falling back to the current directory when not inside a Git repository.
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

    /// Orca worktree selector used while discovering the ChatGPT Web orchestrator terminal.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Pin a specific Orca terminal handle.
    #[arg(long, env = "OMO_RELAY_TERMINAL")]
    terminal: Option<String>,

    /// Orca CLI executable.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Optional bearer token used by omo-bridge.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    token: Option<String>,

    /// Create/resolve the scope and terminal, but do not type the prompt.
    #[arg(long)]
    dry_run: bool,

    /// Emit a compact JSON result for machine callers such as OMO.
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let task = read_task(&cli)?;
    if task.is_empty() {
        return Err(anyhow!("task cannot be empty"));
    }

    let workspace = discover_workspace(cli.workspace.as_deref())?;
    let bridge_url = cli.bridge_url.trim_end_matches('/');
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(8))
        .build()?;
    probe_bridge(&client, bridge_url, cli.token.as_deref()).await?;

    let orca = OrcaConfig::new(
        cli.worktree.clone(),
        cli.terminal.clone(),
        cli.orca_bin.clone(),
    );
    let terminal = resolve_terminal(&orca).await?;

    let port = bridge_port(bridge_url)?;
    let scope_dir = cli
        .scope_dir
        .clone()
        .unwrap_or_else(|| default_scope_dir(port));
    let mux = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;
    let scope = mux.register(&workspace, Some(terminal.clone()))?;
    let prompt = build_delegation_prompt(&scope.scope_id, Path::new(&scope.workspace), &task);

    if !cli.dry_run {
        send_prompt(&orca, &terminal, &prompt)
            .await
            .with_context(|| format!("failed to delegate task to terminal {terminal}"))?;
    }

    let result = serde_json::json!({
        "ok": true,
        "sent": !cli.dry_run,
        "scope_id": scope.scope_id,
        "workspace": scope.workspace,
        "scope_dir": scope_dir.to_string_lossy(),
        "terminal": terminal,
        "bridge_url": bridge_url,
    });

    if cli.json {
        println!("{}", serde_json::to_string(&result)?);
    } else if cli.dry_run {
        println!(
            "DRY RUN: scope={} workspace={} terminal={}",
            result["scope_id"].as_str().unwrap_or(""),
            result["workspace"].as_str().unwrap_or(""),
            result["terminal"].as_str().unwrap_or("")
        );
    } else {
        println!(
            "Delegated to ChatGPT Web: scope={} workspace={} terminal={}",
            result["scope_id"].as_str().unwrap_or(""),
            result["workspace"].as_str().unwrap_or(""),
            result["terminal"].as_str().unwrap_or("")
        );
    }

    Ok(())
}

fn read_task(cli: &Cli) -> Result<String> {
    let task = if cli.stdin {
        let mut input = String::new();
        std::io::stdin()
            .take(1024 * 1024 + 1)
            .read_to_string(&mut input)
            .context("failed to read delegated task from stdin")?;
        if input.len() > 1024 * 1024 {
            return Err(anyhow!("delegated task from stdin exceeds 1 MiB"));
        }
        input
    } else {
        cli.task.join(" ")
    };
    let task = task.trim().to_string();
    if task.is_empty() {
        return Err(anyhow!("task cannot be empty"));
    }
    Ok(task)
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
Terminal Main Orchestrator delegated this task to ChatGPT Web.\n\n\
SCOPE_ID: {}\n\
WORKSPACE: {}\n\n\
This bridge is a multiplexed single-daemon harness. Other ChatGPT Web tasks may be using different workspace scopes concurrently. Every omo-bridge tool call for this task MUST include exactly this scope_id: {}. Do not use another scope_id and do not access parent directories. All file/search/command paths are relative to WORKSPACE.\n\n\
You are the sole coding agent for this task. Do not delegate implementation to OMO, OpenCode, Codex, or another coding agent. Use omo-bridge only as the local I/O, code-intelligence, execution, task-state, and completion harness.\n\n\
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
    fn rejects_non_multiplexed_bridge_health_shape() {
        let old = serde_json::json!({
            "service": "omo-bridge",
            "version": "0.6.1",
            "workspace_mode": "dynamic_active_scope"
        });
        let error = validate_bridge_health(&old, "http://127.0.0.1:18800").unwrap_err();
        assert!(error.to_string().contains("multiplexed workspace scopes"));
    }

    #[test]
    fn accepts_multiplexed_bridge_health_shape() {
        let current = serde_json::json!({
            "service": "omo-bridge",
            "version": "0.6.1",
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
    fn mux_can_register_two_concurrent_delegations() {
        let mount = tempdir().unwrap();
        let first = mount.path().join("first");
        let second = mount.path().join("second");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let scopes = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
        let a = mux.register(&first, Some("term-a".into())).unwrap();
        let b = mux.register(&second, Some("term-b".into())).unwrap();
        assert_ne!(a.scope_id, b.scope_id);
        assert_eq!(
            mux.lookup(&a.scope_id).unwrap().terminal.as_deref(),
            Some("term-a")
        );
        assert_eq!(
            mux.lookup(&b.scope_id).unwrap().terminal.as_deref(),
            Some("term-b")
        );
    }
}
