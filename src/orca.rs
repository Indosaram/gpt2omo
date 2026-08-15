use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use tokio::process::Command;

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

/// Recover the terminal associated with one delegation after a stale terminal handle.
/// The delegation prompt contains the UUID scope id, so recent terminal output is a stable marker
/// that is much safer than generic ChatGPT/omo-bridge heuristics when several tasks run at once.
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
