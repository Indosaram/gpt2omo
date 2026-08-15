use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::StreamExt;
use omo_bridge::orca::{
    resolve_terminal, resolve_terminal_for_marker, send_chatgpt_prompt, send_prompt, OrcaConfig,
};
use omo_bridge::{default_scope_dir, WorkspaceMux};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "omo-relay",
    version,
    about = "Route omo-bridge continuation events back to each scoped ChatGPT Web conversation"
)]
struct Cli {
    /// omo-bridge SSE event endpoint.
    #[arg(long, default_value = "http://127.0.0.1:18800/events")]
    events_url: String,

    /// Broad mount root used by the bridge daemon.
    #[arg(long, default_value = "/")]
    mount_root: PathBuf,

    /// Override the shared directory containing per-delegation workspace scopes.
    #[arg(long)]
    scope_dir: Option<PathBuf>,

    /// Orca worktree selector used for browser pages and legacy terminal discovery.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Pin a terminal only for --resolve-only or legacy terminal-scoped delegations.
    #[arg(long, env = "OMO_RELAY_TERMINAL")]
    terminal: Option<String>,

    /// Orca CLI executable.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Optional bearer token used by omo-bridge.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    token: Option<String>,

    /// Resolve and print the generic orchestrator terminal without subscribing.
    #[arg(long)]
    resolve_only: bool,

    /// Observe continuation events but do not send them to ChatGPT Web/terminal.
    #[arg(long)]
    dry_run: bool,

    /// Delay before reconnecting a dropped SSE stream.
    #[arg(long, default_value_t = 1_000)]
    reconnect_ms: u64,
}

impl Cli {
    fn orca(&self) -> OrcaConfig {
        OrcaConfig::new(
            self.worktree.clone(),
            self.terminal.clone(),
            self.orca_bin.clone(),
        )
    }
}

#[derive(Default)]
struct SseFrame {
    event: String,
    data: Vec<String>,
}

impl SseFrame {
    fn clear(&mut self) {
        self.event.clear();
        self.data.clear();
    }

    fn take_json(&mut self) -> Option<(String, Value)> {
        if self.event.is_empty() && self.data.is_empty() {
            return None;
        }
        let event = std::mem::take(&mut self.event);
        let data = self.data.join("\n");
        self.data.clear();
        let value = serde_json::from_str(&data).unwrap_or(Value::Null);
        Some((event, value))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();
    let orca = cli.orca();
    if cli.resolve_only {
        println!("{}", resolve_terminal(&orca).await?);
        return Ok(());
    }

    let port = events_port(&cli.events_url)?;
    let scope_dir = cli
        .scope_dir
        .clone()
        .unwrap_or_else(|| default_scope_dir(port));
    let mux = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;
    info!(scope_dir = %scope_dir.display(), "relay using multiplexed workspace scopes");

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let mut last_continuation_seq = HashMap::<String, u64>::new();

    loop {
        match consume_events(&client, &cli, &orca, &mux, &mut last_continuation_seq).await {
            Ok(()) => warn!("event stream closed; reconnecting"),
            Err(error) => warn!(error = %error, "event stream failed; reconnecting"),
        }
        sleep(Duration::from_millis(cli.reconnect_ms.max(100))).await;
    }
}

async fn consume_events(
    client: &reqwest::Client,
    cli: &Cli,
    orca: &OrcaConfig,
    mux: &WorkspaceMux,
    last_continuation_seq: &mut HashMap<String, u64>,
) -> Result<()> {
    let mut request = client.get(&cli.events_url);
    if let Some(token) = &cli.token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = request.send().await?.error_for_status()?;
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !content_type.starts_with("text/event-stream") {
        return Err(anyhow!("events endpoint is not SSE: {content_type}"));
    }

    info!(url = %cli.events_url, "subscribed to omo-bridge events");
    let mut stream = response.bytes_stream();
    let mut pending = Vec::<u8>::new();
    let mut frame = SseFrame::default();

    while let Some(chunk) = stream.next().await {
        pending.extend_from_slice(&chunk?);
        while let Some(pos) = pending.iter().position(|byte| *byte == b'\n') {
            let mut line = pending.drain(..=pos).collect::<Vec<_>>();
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            let line = String::from_utf8_lossy(&line);

            if line.is_empty() {
                if let Some((event, payload)) = frame.take_json() {
                    handle_event(cli, orca, mux, last_continuation_seq, &event, payload).await?;
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(value) = line.strip_prefix("event:") {
                frame.event = value.trim().to_string();
            } else if let Some(value) = line.strip_prefix("data:") {
                frame.data.push(value.trim_start().to_string());
            }
        }
    }

    frame.clear();
    Ok(())
}

async fn handle_event(
    cli: &Cli,
    orca: &OrcaConfig,
    mux: &WorkspaceMux,
    last_continuation_seq: &mut HashMap<String, u64>,
    event: &str,
    payload: Value,
) -> Result<()> {
    match event {
        "connected" => {
            info!(
                seq = payload.get("seq").and_then(|value| value.as_u64()),
                "relay connected"
            );
        }
        "completion" => {
            let scope_id = payload
                .pointer("/data/scope_id")
                .and_then(Value::as_str)
                .unwrap_or("<missing>");
            let ready = payload
                .pointer("/data/ready")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            info!(scope_id, ready, "completion state received");
        }
        "continuation_required" => {
            relay_continuation(cli, orca, mux, last_continuation_seq, &payload).await?;
        }
        "lagged" => {
            warn!(payload = %payload, "SSE subscriber lagged; scope task_state should be reconciled")
        }
        _ => {}
    }
    Ok(())
}

async fn relay_continuation(
    cli: &Cli,
    orca: &OrcaConfig,
    mux: &WorkspaceMux,
    last_continuation_seq: &mut HashMap<String, u64>,
    payload: &Value,
) -> Result<()> {
    let seq = payload
        .get("seq")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let scope_id = payload
        .pointer("/data/scope_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let relay = payload
        .pointer("/data/relay_to_same_chat")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let prompt = payload
        .pointer("/data/prompt")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();

    if scope_id.is_empty() || !relay || prompt.is_empty() {
        warn!(
            seq,
            "continuation event is missing scope/prompt routing data"
        );
        return Ok(());
    }

    let last = last_continuation_seq.get(scope_id).copied().unwrap_or(0);
    if seq != 0 && seq <= last {
        info!(scope_id, seq, "skipping duplicate continuation event");
        return Ok(());
    }

    let scope = continuation_scope(mux, scope_id)?;
    if let Some(page) = scope.browser_page_id.as_deref() {
        if cli.dry_run {
            info!(scope_id, seq, browser_page_id = page, prompt = %prompt, "dry-run continuation relay to ChatGPT Web");
        } else {
            send_chatgpt_prompt(orca, page, prompt)
                .await
                .with_context(|| {
                    format!(
                        "failed to relay continuation directly to ChatGPT Web page {page} for scope {scope_id}"
                    )
                })?;
        }
        last_continuation_seq.insert(scope_id.to_string(), last.max(seq));
        info!(
            scope_id,
            seq,
            browser_page_id = page,
            "continuation relayed directly to ChatGPT Web"
        );
        return Ok(());
    }

    // Backward compatibility for pre-browser scopes created by v0.6.x.
    let stored_terminal = scope.terminal.as_deref();
    let terminal = match resolve_terminal_for_marker(orca, scope_id).await {
        Ok(fresh) => {
            if stored_terminal != Some(fresh.as_str()) {
                mux.update_terminal(scope_id, &fresh)?;
            }
            fresh
        }
        Err(marker_error) => match stored_terminal {
            Some(stored) => {
                warn!(scope_id, error = %marker_error, terminal = stored, "scope marker rediscovery failed; using stored terminal");
                stored.to_string()
            }
            None => {
                return Err(marker_error
                    .context("scope has neither browser page nor stored terminal fallback"))
            }
        },
    };

    if cli.dry_run {
        info!(scope_id, seq, terminal = %terminal, prompt = %prompt, "dry-run legacy continuation relay");
    } else if let Err(first_error) = send_prompt(orca, &terminal, prompt).await {
        warn!(scope_id, error = %first_error, terminal = %terminal, "legacy scope relay send failed; rediscovering by scope marker");
        let fresh = resolve_terminal_for_marker(orca, scope_id).await?;
        send_prompt(orca, &fresh, prompt).await.with_context(|| {
            format!("failed to relay legacy scope {scope_id} after terminal rediscovery")
        })?;
        mux.update_terminal(scope_id, &fresh)?;
    }

    last_continuation_seq.insert(scope_id.to_string(), last.max(seq));
    info!(scope_id, seq, terminal = %terminal, "legacy continuation relayed for scope");
    Ok(())
}

fn continuation_scope(mux: &WorkspaceMux, scope_id: &str) -> Result<omo_bridge::WorkspaceScope> {
    Ok(mux.lookup(scope_id)?)
}

fn events_port(events_url: &str) -> Result<u16> {
    let parsed = Url::parse(events_url)
        .with_context(|| format!("invalid events URL for scope routing: {events_url}"))?;
    parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("events URL has no resolvable port: {events_url}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_scoped_continuation_payload_shape() {
        let payload = serde_json::json!({
            "seq": 42,
            "kind": "continuation_required",
            "data": {
                "scope_id": "55555555-5555-4555-8555-555555555555",
                "prompt": "continue",
                "relay_to_same_chat": true
            }
        });
        assert_eq!(payload["seq"], 42);
        assert_eq!(
            payload["data"]["scope_id"],
            "55555555-5555-4555-8555-555555555555"
        );
        assert_eq!(payload["data"]["prompt"], "continue");
        assert_eq!(payload["data"]["relay_to_same_chat"], true);
    }

    #[test]
    fn continuation_scope_routes_each_scope_to_its_exact_browser_page_id() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let states = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), states.path()).unwrap();
        let first = mux
            .register_browser(&project, "browser-page-a".into())
            .unwrap();
        let second = mux
            .register_browser(&project, "browser-page-b".into())
            .unwrap();

        assert_eq!(
            continuation_scope(&mux, &first.scope_id)
                .unwrap()
                .browser_page_id
                .as_deref(),
            Some("browser-page-a")
        );
        assert_eq!(
            continuation_scope(&mux, &second.scope_id)
                .unwrap()
                .browser_page_id
                .as_deref(),
            Some("browser-page-b")
        );
    }

    #[test]
    fn derives_scope_port_from_events_url() {
        assert_eq!(events_port("http://127.0.0.1:18800/events").unwrap(), 18800);
    }
}
