use anyhow::{anyhow, Context, Result};
use clap::Parser;
use futures::StreamExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde_json::Value;
use std::time::Duration;
use tokio::process::Command;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Parser, Debug, Clone)]
#[command(
    name = "omo-relay",
    version,
    about = "Relay omo-bridge continuation events into the Orca terminal that drives ChatGPT Web"
)]
struct Cli {
    /// omo-bridge SSE event endpoint.
    #[arg(long, default_value = "http://127.0.0.1:18800/events")]
    events_url: String,

    /// Orca worktree selector used while discovering the orchestrator terminal.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Pin a specific Orca terminal handle. If omitted, the relay discovers it by recent output.
    #[arg(long, env = "OMO_RELAY_TERMINAL")]
    terminal: Option<String>,

    /// Orca CLI executable.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Optional bearer token used by omo-bridge.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    token: Option<String>,

    /// Resolve and print the orchestrator terminal without subscribing or sending anything.
    #[arg(long)]
    resolve_only: bool,

    /// Observe continuation events but do not type them into the orchestrator terminal.
    #[arg(long)]
    dry_run: bool,

    /// Delay before reconnecting a dropped SSE stream.
    #[arg(long, default_value_t = 1_000)]
    reconnect_ms: u64,
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
    let mut terminal = resolve_terminal(&cli).await?;
    info!(terminal = %terminal, "resolved ChatGPT Web orchestrator terminal");

    if cli.resolve_only {
        println!("{}", terminal);
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .build()?;
    let mut last_continuation_seq = 0u64;

    loop {
        match consume_events(&client, &cli, &mut terminal, &mut last_continuation_seq).await {
            Ok(()) => warn!("event stream closed; reconnecting"),
            Err(error) => warn!(error = %error, "event stream failed; reconnecting"),
        }
        sleep(Duration::from_millis(cli.reconnect_ms.max(100))).await;
    }
}

async fn consume_events(
    client: &reqwest::Client,
    cli: &Cli,
    terminal: &mut String,
    last_continuation_seq: &mut u64,
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
                    handle_event(cli, terminal, last_continuation_seq, &event, payload).await?;
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
    terminal: &mut String,
    last_continuation_seq: &mut u64,
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
            let ready = payload
                .pointer("/data/ready")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            info!(ready, "completion state received");
        }
        "continuation_required" => {
            let seq = payload
                .get("seq")
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            if seq != 0 && seq <= *last_continuation_seq {
                info!(seq, "skipping duplicate continuation event");
                return Ok(());
            }
            let relay = payload
                .pointer("/data/relay_to_same_chat")
                .and_then(|value| value.as_bool())
                .unwrap_or(false);
            let prompt = payload
                .pointer("/data/prompt")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            if !relay || prompt.is_empty() {
                warn!(seq, "continuation event had no relayable prompt");
                return Ok(());
            }

            if cli.dry_run {
                info!(seq, terminal = %terminal, prompt = %prompt, "dry-run continuation relay");
            } else if let Err(first_error) = send_prompt(cli, terminal, prompt).await {
                warn!(error = %first_error, terminal = %terminal, "relay send failed; rediscovering terminal");
                let fresh = resolve_terminal_uncached(cli).await?;
                send_prompt(cli, &fresh, prompt).await.with_context(|| {
                    format!("failed to relay after terminal rediscovery ({fresh})")
                })?;
                *terminal = fresh;
            }
            *last_continuation_seq = (*last_continuation_seq).max(seq);
            info!(seq, terminal = %terminal, "continuation relayed to main orchestrator");
        }
        "lagged" => {
            warn!(payload = %payload, "SSE subscriber lagged; bridge state should be reconciled")
        }
        _ => {}
    }
    Ok(())
}

async fn send_prompt(cli: &Cli, terminal: &str, prompt: &str) -> Result<()> {
    let result = run_orca_json(
        cli,
        &[
            "terminal",
            "send",
            "--terminal",
            terminal,
            "--text",
            prompt,
            "--enter",
            "--json",
        ],
    )
    .await?;
    if result.get("ok").and_then(|value| value.as_bool()) != Some(true) {
        return Err(anyhow!("orca terminal send failed: {result}"));
    }
    Ok(())
}

async fn resolve_terminal(cli: &Cli) -> Result<String> {
    if let Some(terminal) = &cli.terminal {
        verify_terminal(cli, terminal).await?;
        return Ok(terminal.clone());
    }
    resolve_terminal_uncached(cli).await
}

async fn verify_terminal(cli: &Cli, terminal: &str) -> Result<()> {
    let result =
        run_orca_json(cli, &["terminal", "show", "--terminal", terminal, "--json"]).await?;
    let node = result
        .pointer("/result/terminal")
        .ok_or_else(|| anyhow!("terminal not found: {terminal}"))?;
    if node.get("connected").and_then(|value| value.as_bool()) != Some(true)
        || node.get("writable").and_then(|value| value.as_bool()) != Some(true)
    {
        return Err(anyhow!("terminal is not connected+writable: {terminal}"));
    }
    Ok(())
}

async fn resolve_terminal_uncached(cli: &Cli) -> Result<String> {
    let listed = run_orca_json(
        cli,
        &["terminal", "list", "--worktree", &cli.worktree, "--json"],
    )
    .await?;
    let terminals = listed
        .pointer("/result/terminals")
        .and_then(|value| value.as_array())
        .ok_or_else(|| anyhow!("orca terminal list returned no terminals"))?;

    let mut candidates = Vec::<(i64, i64, String)>::new();
    for terminal in terminals {
        if terminal.get("connected").and_then(|value| value.as_bool()) != Some(true)
            || terminal.get("writable").and_then(|value| value.as_bool()) != Some(true)
        {
            continue;
        }
        let Some(handle) = terminal.get("handle").and_then(|value| value.as_str()) else {
            continue;
        };
        let read = run_orca_json(
            cli,
            &[
                "terminal",
                "read",
                "--terminal",
                handle,
                "--limit",
                "160",
                "--json",
            ],
        )
        .await?;
        let tail = read
            .pointer("/result/terminal/tail")
            .and_then(|value| value.as_array())
            .map(|lines| {
                lines
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        let score = orchestrator_score(&tail);
        let last_output = terminal
            .get("lastOutputAt")
            .and_then(|value| value.as_i64())
            .unwrap_or(0);
        if score > 0 {
            candidates.push((score, last_output, handle.to_string()));
        }
    }

    candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    let best = candidates
        .first()
        .ok_or_else(|| anyhow!("could not identify the ChatGPT Web orchestrator terminal"))?;
    if best.0 < 100 {
        return Err(anyhow!(
            "terminal discovery was ambiguous (best score {}); set --terminal/OMO_RELAY_TERMINAL",
            best.0
        ));
    }
    if candidates.get(1).is_some_and(|second| second.0 == best.0) {
        return Err(anyhow!(
            "multiple terminals matched equally; set --terminal/OMO_RELAY_TERMINAL"
        ));
    }
    Ok(best.2.clone())
}

fn orchestrator_score(text: &str) -> i64 {
    let mut score = 0;
    for (needle, weight) in [
        ("ChatGPT Web", 120),
        ("ChatGPT", 30),
        ("omo-bridge", 70),
        ("연결됨", 50),
        ("single-agent harness", 40),
        ("대화창", 20),
    ] {
        if text.contains(needle) {
            score += weight;
        }
    }
    score
}

async fn run_orca_json(cli: &Cli, args: &[&str]) -> Result<Value> {
    let output = Command::new(&cli.orca_bin)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to execute {}", cli.orca_bin))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(anyhow!(
            "orca command failed ({}): {} {}",
            output.status,
            stdout.trim(),
            stderr.trim()
        ));
    }
    let value: Value = serde_json::from_str(&stdout)
        .with_context(|| format!("orca returned invalid JSON: {}", stdout.trim()))?;
    if value.get("ok").and_then(|value| value.as_bool()) == Some(false) {
        return Err(anyhow!("orca returned an error: {value}"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_real_orchestrator_markers_highly() {
        let text = "omo-bridge mount changed; ChatGPT Web에 연결됨 전송; single-agent harness";
        assert!(orchestrator_score(text) >= 200);
    }

    #[test]
    fn unrelated_pi_terminal_does_not_match() {
        assert_eq!(
            orchestrator_score("(😺 OmO Native) Goal achieved; reflecting"),
            0
        );
    }

    #[test]
    fn parses_continuation_payload_shape() {
        let payload = serde_json::json!({
            "seq": 42,
            "kind": "continuation_required",
            "data": {
                "prompt": "continue",
                "relay_to_same_chat": true
            }
        });
        assert_eq!(payload["seq"], 42);
        assert_eq!(payload["data"]["prompt"], "continue");
        assert_eq!(payload["data"]["relay_to_same_chat"], true);
    }
}
