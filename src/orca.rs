use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

#[derive(Clone, Debug)]
pub struct OrcaConfig {
    pub worktree: String,
    pub terminal: Option<String>,
    pub orca_bin: String,
}

impl OrcaConfig {
    pub fn new(
        worktree: impl Into<String>,
        terminal: Option<String>,
        orca_bin: impl Into<String>,
    ) -> Self {
        Self {
            worktree: worktree.into(),
            terminal,
            orca_bin: orca_bin.into(),
        }
    }
}

pub async fn create_chatgpt_tab(config: &OrcaConfig) -> Result<String> {
    let result = run_orca_json(
        config,
        &[
            "tab",
            "create",
            "--url",
            "https://chatgpt.com",
            "--worktree",
            &config.worktree,
            "--json",
        ],
    )
    .await?;
    let page = result
        .pointer("/result/browserPageId")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("orca tab create returned no browserPageId: {result}"))?
        .to_string();

    if let Err(error) = wait_for_chatgpt_prompt(config, &page).await {
        let _ = close_browser_page(config, &page).await;
        return Err(error);
    }
    Ok(page)
}

pub async fn close_browser_page(config: &OrcaConfig, page: &str) -> Result<()> {
    let result = run_orca_json(config, &["tab", "close", "--page", page, "--json"]).await?;
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow!("orca tab close failed: {result}"));
    }
    Ok(())
}

pub async fn send_chatgpt_prompt(config: &OrcaConfig, page: &str, prompt: &str) -> Result<()> {
    if prompt.trim().is_empty() {
        return Err(anyhow!("ChatGPT Web prompt cannot be empty"));
    }
    wait_for_chatgpt_idle(config, page).await?;

    let prompt_json = serde_json::to_string(prompt)?;
    let insert_expression = format!(
        r#"(() => {{
  const el = document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable="true"]');
  if (!el) return {{ ok: false, error: 'no_textbox' }};
  el.focus();
  document.execCommand('selectAll', false, null);
  document.execCommand('delete', false, null);
  document.execCommand('insertText', false, {prompt_json});
  el.dispatchEvent(new Event('input', {{ bubbles: true }}));
  return {{ ok: true }};
}})()"#
    );
    let inserted = eval_json(config, page, &insert_expression).await?;
    if inserted.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow!("unable to fill ChatGPT prompt box: {inserted}"));
    }

    sleep(Duration::from_millis(250)).await;
    let send_expression = r#"(() => {
  const btn = document.querySelector('button[data-testid="send-button"], button[aria-label="Send prompt"]');
  if (!btn || btn.disabled) return { ok: false, error: 'no_send_button' };
  btn.click();
  return { ok: true };
})()"#;
    let sent = eval_json(config, page, send_expression).await?;
    if sent.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow!("unable to send ChatGPT prompt: {sent}"));
    }
    Ok(())
}

async fn wait_for_chatgpt_idle(config: &OrcaConfig, page: &str) -> Result<()> {
    let expression = r#"(() => ({
  ready: !!(document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable="true"]')),
  generating: !!document.querySelector('button[data-testid="stop-button"], button[aria-label="Stop generating"], button[aria-label="Stop answering"]')
}))()"#;
    for _ in 0..240 {
        match eval_json(config, page, expression).await {
            Ok(value)
                if value.get("ready").and_then(Value::as_bool) == Some(true)
                    && value.get("generating").and_then(Value::as_bool) == Some(false) =>
            {
                return Ok(())
            }
            _ => sleep(Duration::from_millis(250)).await,
        }
    }
    Err(anyhow!(
        "ChatGPT Web conversation did not become idle within 60 seconds"
    ))
}

async fn wait_for_chatgpt_prompt(config: &OrcaConfig, page: &str) -> Result<()> {
    let expression = r#"(() => ({
  ready: !!(document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable="true"]')),
  url: location.href,
  title: document.title
}))()"#;

    for _ in 0..40 {
        match eval_json(config, page, expression).await {
            Ok(value) if value.get("ready").and_then(Value::as_bool) == Some(true) => return Ok(()),
            _ => sleep(Duration::from_millis(250)).await,
        }
    }
    Err(anyhow!(
        "ChatGPT Web prompt box did not become ready; verify the Orca browser is logged into chatgpt.com"
    ))
}

async fn eval_json(config: &OrcaConfig, page: &str, expression: &str) -> Result<Value> {
    let result = run_orca_json(
        config,
        &["eval", "--page", page, "--expression", expression, "--json"],
    )
    .await?;
    let raw = result
        .pointer("/result/result")
        .ok_or_else(|| anyhow!("orca eval returned no result: {result}"))?;
    match raw {
        Value::String(text) => serde_json::from_str(text)
            .with_context(|| format!("orca eval returned non-JSON page result: {text}")),
        other => Ok(other.clone()),
    }
}

pub async fn send_prompt(config: &OrcaConfig, terminal: &str, prompt: &str) -> Result<()> {
    let result = run_orca_json(
        config,
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
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow!("orca terminal send failed: {result}"));
    }
    Ok(())
}

pub async fn resolve_terminal(config: &OrcaConfig) -> Result<String> {
    if let Some(terminal) = &config.terminal {
        verify_terminal(config, terminal).await?;
        return Ok(terminal.clone());
    }
    resolve_terminal_uncached(config).await
}

async fn verify_terminal(config: &OrcaConfig, terminal: &str) -> Result<()> {
    let result = run_orca_json(
        config,
        &["terminal", "show", "--terminal", terminal, "--json"],
    )
    .await?;
    let node = result
        .pointer("/result/terminal")
        .ok_or_else(|| anyhow!("terminal not found: {terminal}"))?;
    if node.get("connected").and_then(Value::as_bool) != Some(true)
        || node.get("writable").and_then(Value::as_bool) != Some(true)
    {
        return Err(anyhow!("terminal is not connected+writable: {terminal}"));
    }
    Ok(())
}

pub async fn resolve_terminal_uncached(config: &OrcaConfig) -> Result<String> {
    let candidates = terminal_candidates(config).await?;
    let mut scored = Vec::<(i64, i64, String)>::new();
    for candidate in candidates {
        let score = orchestrator_score(&candidate.tail);
        if score > 0 {
            scored.push((score, candidate.last_output, candidate.handle));
        }
    }

    scored.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));
    let best = scored
        .first()
        .ok_or_else(|| anyhow!("could not identify the ChatGPT Web orchestrator terminal"))?;
    if best.0 < 100 {
        return Err(anyhow!(
            "terminal discovery was ambiguous (best score {}); set --terminal/OMO_RELAY_TERMINAL",
            best.0
        ));
    }
    if scored.get(1).is_some_and(|second| second.0 == best.0) {
        return Err(anyhow!(
            "multiple terminals matched equally; set --terminal/OMO_RELAY_TERMINAL"
        ));
    }
    Ok(best.2.clone())
}

pub async fn resolve_terminal_for_marker(config: &OrcaConfig, marker: &str) -> Result<String> {
    let marker = marker.trim();
    if marker.is_empty() {
        return Err(anyhow!("terminal marker cannot be empty"));
    }

    let mut matches = terminal_candidates(config)
        .await?
        .into_iter()
        .filter(|candidate| candidate.tail.contains(marker))
        .collect::<Vec<_>>();
    matches.sort_by(|a, b| b.last_output.cmp(&a.last_output));

    match matches.as_slice() {
        [] => Err(anyhow!(
            "could not rediscover a ChatGPT Web orchestrator terminal containing marker {marker}"
        )),
        [only] => Ok(only.handle.clone()),
        [first, second, ..] if first.last_output == second.last_output => Err(anyhow!(
            "multiple terminals contain scope marker {marker} with equal recency"
        )),
        [first, ..] => Ok(first.handle.clone()),
    }
}

#[derive(Debug)]
struct TerminalCandidate {
    handle: String,
    last_output: i64,
    tail: String,
}

async fn terminal_candidates(config: &OrcaConfig) -> Result<Vec<TerminalCandidate>> {
    let listed = run_orca_json(
        config,
        &["terminal", "list", "--worktree", &config.worktree, "--json"],
    )
    .await?;
    let terminals = listed
        .pointer("/result/terminals")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("orca terminal list returned no terminals"))?;

    let mut candidates = Vec::new();
    for terminal in terminals {
        if terminal.get("connected").and_then(Value::as_bool) != Some(true)
            || terminal.get("writable").and_then(Value::as_bool) != Some(true)
        {
            continue;
        }
        let Some(handle) = terminal.get("handle").and_then(Value::as_str) else {
            continue;
        };
        let read = run_orca_json(
            config,
            &[
                "terminal",
                "read",
                "--terminal",
                handle,
                "--limit",
                "200",
                "--json",
            ],
        )
        .await?;
        let tail = read
            .pointer("/result/terminal/tail")
            .and_then(Value::as_array)
            .map(|lines| {
                lines
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        let last_output = terminal
            .get("lastOutputAt")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        candidates.push(TerminalCandidate {
            handle: handle.to_string(),
            last_output,
            tail,
        });
    }
    Ok(candidates)
}

pub fn orchestrator_score(text: &str) -> i64 {
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

async fn run_orca_json(config: &OrcaConfig, args: &[&str]) -> Result<Value> {
    let output = Command::new(&config.orca_bin)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to execute {}", config.orca_bin))?;
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
    if value.get("ok").and_then(Value::as_bool) == Some(false) {
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
}
