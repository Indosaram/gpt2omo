use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

pub const MAX_RESET_AFTER_SECONDS: u64 = 31 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserDriverKind {
    Maho,
    Orca,
    AgentBrowser,
    Aside,
}

impl BrowserDriverKind {
    pub fn supports_chatgpt_dom_probe(self) -> bool {
        matches!(self, Self::Orca | Self::AgentBrowser)
    }
}

impl std::fmt::Display for BrowserDriverKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Maho => write!(f, "maho"),
            Self::Orca => write!(f, "orca"),
            Self::AgentBrowser => write!(f, "agent-browser"),
            Self::Aside => write!(f, "aside"),
        }
    }
}

impl std::str::FromStr for BrowserDriverKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().trim() {
            "maho" => Ok(Self::Maho),
            "orca" => Ok(Self::Orca),
            "agent-browser" | "agent_browser" => Ok(Self::AgentBrowser),
            "aside" => Ok(Self::Aside),
            other => Err(anyhow!(
                "unsupported browser driver '{other}'; supported: maho, orca, agent-browser, aside"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatgptRateLimitReason {
    UsageLimit,
    TooManyRequests,
    Capacity,
    ModelQuota,
}

impl ChatgptRateLimitReason {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "usage_limit" => Some(Self::UsageLimit),
            "too_many_requests" => Some(Self::TooManyRequests),
            "capacity" => Some(Self::Capacity),
            "model_quota" => Some(Self::ModelQuota),
            _ => None,
        }
    }
}

impl std::fmt::Display for ChatgptRateLimitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UsageLimit => write!(f, "usage_limit"),
            Self::TooManyRequests => write!(f, "too_many_requests"),
            Self::Capacity => write!(f, "capacity"),
            Self::ModelQuota => write!(f, "model_quota"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "condition", rename_all = "snake_case")]
pub enum ChatgptUiCondition {
    Healthy,
    Generating,
    RateLimited {
        reason: ChatgptRateLimitReason,
        #[serde(skip_serializing_if = "Option::is_none")]
        reset_after_seconds: Option<u64>,
    },
    DeliveryError {
        recoverable: bool,
    },
    AuthenticationRequired,
    Unsupported,
    Unknown,
}

impl ChatgptUiCondition {
    pub fn reset_after_seconds(&self) -> Option<u64> {
        match self {
            Self::RateLimited {
                reset_after_seconds,
                ..
            } => *reset_after_seconds,
            _ => None,
        }
    }
}

pub fn validate_reset_after_seconds(value: u64) -> Option<u64> {
    (1..=MAX_RESET_AFTER_SECONDS)
        .contains(&value)
        .then_some(value)
}

#[derive(Clone, Debug)]
pub struct BrowserDriverConfig {
    pub driver: Option<BrowserDriverKind>,
    pub binary: Option<PathBuf>,
    pub worktree: String,
    pub terminal: Option<String>,
}

impl BrowserDriverConfig {
    pub fn new(
        worktree: impl Into<String>,
        terminal: Option<String>,
        orca_bin: impl Into<String>,
    ) -> Self {
        let bin_str: String = orca_bin.into();
        Self {
            driver: None,
            binary: if bin_str.is_empty() || bin_str == "orca" {
                None
            } else {
                Some(PathBuf::from(bin_str))
            },
            worktree: worktree.into(),
            terminal,
        }
    }

    pub fn with_driver(
        driver: Option<BrowserDriverKind>,
        binary: Option<PathBuf>,
        worktree: impl Into<String>,
        terminal: Option<String>,
    ) -> Self {
        Self {
            driver,
            binary,
            worktree: worktree.into(),
            terminal,
        }
    }

    pub fn orca_legacy(
        worktree: impl Into<String>,
        terminal: Option<String>,
        orca_bin: impl Into<String>,
    ) -> Self {
        Self::new(worktree, terminal, orca_bin)
    }

    pub async fn detect(&self) -> Result<(BrowserDriverKind, PathBuf)> {
        if let Some(kind) = &self.driver {
            if let Some(bin) = &self.binary {
                return Ok((*kind, bin.clone()));
            }
            let default_bin = match kind {
                BrowserDriverKind::Maho => {
                    resolve_maho_bin().unwrap_or_else(|| PathBuf::from("maho"))
                }
                BrowserDriverKind::Orca => PathBuf::from("orca"),
                BrowserDriverKind::AgentBrowser => PathBuf::from("agent-browser"),
                BrowserDriverKind::Aside => PathBuf::from("aside"),
            };
            return Ok((*kind, default_bin));
        }

        if let Some(maho_bin) = resolve_maho_bin() {
            return Ok((BrowserDriverKind::Maho, maho_bin));
        }
        if is_executable_in_path("maho").await {
            return Ok((BrowserDriverKind::Maho, PathBuf::from("maho")));
        }
        if is_executable_in_path("orca").await {
            return Ok((BrowserDriverKind::Orca, PathBuf::from("orca")));
        }
        if is_executable_in_path("agent-browser").await {
            return Ok((
                BrowserDriverKind::AgentBrowser,
                PathBuf::from("agent-browser"),
            ));
        }
        if is_executable_in_path("aside").await {
            return Ok((BrowserDriverKind::Aside, PathBuf::from("aside")));
        }

        Ok((
            BrowserDriverKind::Maho,
            resolve_maho_bin().unwrap_or_else(|| PathBuf::from("maho")),
        ))
    }
}

pub type OrcaConfig = BrowserDriverConfig;

fn resolve_maho_bin() -> Option<PathBuf> {
    let app_helper = PathBuf::from("/Applications/Maho.app/Contents/Helpers/maho");
    if app_helper.is_file() {
        return Some(app_helper);
    }
    None
}

async fn is_executable_in_path(bin_name: &str) -> bool {
    Command::new("which")
        .arg(bin_name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatgptPageProbe {
    pub url: String,
    pub title: String,
    pub generating: bool,
}

const CHATGPT_UI_PROBE_EXPRESSION: &str = r#"(() => {
  const MESSAGE_SELECTOR = '[data-message-author-role], article, [data-testid^="conversation-turn"], [data-message-id]';
  const SYSTEM_SELECTOR = '[data-testid*="rate-limit"], [data-testid*="modal"], [role="alert"], [role="dialog"], [data-sonner-toast], [data-testid*="toast"], [data-testid*="notification"], [class~="toast"], [class~="notification"], [class*="modal"], [class*="popover"]';
  const isVisible = (el) => {
    if (!el || !(el instanceof Element)) return false;
    const style = window.getComputedStyle(el);
    if (style.display === 'none' || style.visibility === 'hidden' || Number(style.opacity) === 0) return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const isConversationRegion = (el) =>
    !!el.closest(MESSAGE_SELECTOR) || !!el.querySelector(MESSAGE_SELECTOR);
  const systemRegions = Array.from(document.querySelectorAll(SYSTEM_SELECTOR))
    .filter((el) => isVisible(el) && !isConversationRegion(el));
  const systemTexts = systemRegions
    .map((el) => (el.innerText || el.textContent || '').trim().toLowerCase())
    .filter(Boolean);

  const composer = document.querySelector('#prompt-textarea, [data-testid="composer-text-input"], textarea[placeholder]');
  const sendButton = document.querySelector('button[data-testid="send-button"], button[aria-label="Send prompt"], #composer-submit-button');
  const stopButton = document.querySelector('button[data-testid="stop-button"], button[aria-label="Stop generating"], button[aria-label="Stop answering"]');
  const composerDisabled = !!(
    (composer && (composer.disabled || composer.getAttribute('aria-disabled') === 'true')) ||
    (sendButton && (sendButton.disabled || sendButton.getAttribute('aria-disabled') === 'true'))
  );
  const generating = !!stopButton && isVisible(stopButton);
  const ready = !!composer && isVisible(composer);

  const rateReason = (text) => {
    if (/\btoo many (?:requests|messages)\b|\brate limit(?:ed| reached)?\b|\bmaking requests too quickly\b|\btemporarily limited access\b|\blimited access to your conversations\b|\bwait a few minutes before trying again\b/.test(text)) return 'too_many_requests';
    if (/\b(?:at|over) capacity\b|\bcapacity (?:limit|reached)\b/.test(text)) return 'capacity';
    if (/\b(?:model|gpt[-\w]*)[^.\n]{0,80}\b(?:usage )?limit\b|\blimit[^.\n]{0,80}\b(?:model|gpt[-\w]*)\b/.test(text)) return 'model_quota';
    if (/\b(?:you(?:'ve| have)? )?(?:reached|hit) (?:the )?(?:current )?(?:usage |message )?limit\b|\busage limit\b|\blimit reached\b/.test(text)) return 'usage_limit';
    return null;
  };
  const resetSeconds = (text) => {
    const match = text.match(/(?:try again|reset(?:s)?|available again|wait)[^0-9]{0,48}(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?|days?)/i);
    if (!match) return null;
    const amount = Number(match[1]);
    if (!Number.isFinite(amount) || amount <= 0) return null;
    const unit = match[2].toLowerCase();
    let multiplier = 1;
    if (unit.startsWith('min')) multiplier = 60;
    else if (unit.startsWith('hour') || unit.startsWith('hr')) multiplier = 3600;
    else if (unit.startsWith('day')) multiplier = 86400;
    const seconds = Math.ceil(amount * multiplier);
    return Number.isSafeInteger(seconds) && seconds >= 1 && seconds <= 2678400 ? seconds : null;
  };

  let rate_limit_reason = null;
  let reset_after_seconds = null;
  for (const text of systemTexts) {
    const reason = rateReason(text);
    if (reason) {
      rate_limit_reason = reason;
      reset_after_seconds = resetSeconds(text);
      break;
    }
  }

  const authRequired = systemTexts.some((text) =>
    /\b(?:log in|login|sign in|authentication required|session expired|please authenticate)\b/.test(text)
  );
  const deliveryText = systemTexts.find((text) =>
    /\b(?:something went wrong|error generating|network error|failed to send|message failed|unable to load conversation|delivery failed)\b/.test(text)
  ) || null;
  const deliveryRecoverable = !!deliveryText && /\b(?:retry|try again|network|temporary|temporarily)\b/.test(deliveryText);

  return {
    ready,
    generating,
    composer_disabled: composerDisabled,
    rate_limited: rate_limit_reason !== null,
    rate_limit_reason,
    reset_after_seconds,
    delivery_error: deliveryText !== null,
    delivery_recoverable: deliveryRecoverable,
    authentication_required: authRequired
  };
})()"#;

pub async fn probe_chatgpt_ui_condition(
    config: &BrowserDriverConfig,
    page: &str,
) -> ChatgptUiCondition {
    if page.trim().is_empty() {
        return ChatgptUiCondition::Unknown;
    }
    let Ok((kind, _)) = config.detect().await else {
        return ChatgptUiCondition::Unknown;
    };
    if !kind.supports_chatgpt_dom_probe() {
        return ChatgptUiCondition::Unsupported;
    }
    let Ok(value) = eval_json(config, page, CHATGPT_UI_PROBE_EXPRESSION).await else {
        return ChatgptUiCondition::Unknown;
    };
    classify_chatgpt_ui_snapshot(&value)
}

fn classify_chatgpt_ui_snapshot(value: &Value) -> ChatgptUiCondition {
    if value
        .get("authentication_required")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return ChatgptUiCondition::AuthenticationRequired;
    }

    if value.get("rate_limited").and_then(Value::as_bool) == Some(true) {
        let Some(reason) = value
            .get("rate_limit_reason")
            .and_then(Value::as_str)
            .and_then(ChatgptRateLimitReason::parse)
        else {
            return ChatgptUiCondition::Unknown;
        };
        let reset_after_seconds = value
            .get("reset_after_seconds")
            .and_then(Value::as_u64)
            .and_then(validate_reset_after_seconds);
        return ChatgptUiCondition::RateLimited {
            reason,
            reset_after_seconds,
        };
    }

    if value.get("delivery_error").and_then(Value::as_bool) == Some(true) {
        return ChatgptUiCondition::DeliveryError {
            recoverable: value
                .get("delivery_recoverable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
    }

    if value.get("generating").and_then(Value::as_bool) == Some(true) {
        return ChatgptUiCondition::Generating;
    }

    if value.get("ready").and_then(Value::as_bool) == Some(true) {
        return ChatgptUiCondition::Healthy;
    }

    ChatgptUiCondition::Unknown
}

pub async fn create_chatgpt_tab(config: &BrowserDriverConfig) -> Result<String> {
    let (kind, bin) = config.detect().await?;
    match kind {
        BrowserDriverKind::Maho => create_chatgpt_tab_maho(&bin, config).await,
        BrowserDriverKind::Orca => create_chatgpt_tab_orca(&bin, config).await,
        BrowserDriverKind::AgentBrowser => create_chatgpt_tab_agent_browser(&bin).await,
        BrowserDriverKind::Aside => create_chatgpt_tab_aside(&bin).await,
    }
}

async fn create_chatgpt_tab_maho(bin: &PathBuf, config: &BrowserDriverConfig) -> Result<String> {
    let result = run_command_json(
        bin,
        &["tab", "new", "--url", "https://chatgpt.com", "--json"],
    )
    .await;

    let page = match result {
        Ok(val) => val
            .pointer("/result/tab/id")
            .or_else(|| val.pointer("/result/id"))
            .or_else(|| val.pointer("/result/tab_id"))
            .and_then(value_as_identifier)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        Err(_) => {
            if is_executable_in_path("orca").await {
                return create_chatgpt_tab_orca(&PathBuf::from("orca"), config).await;
            }
            uuid::Uuid::new_v4().to_string()
        }
    };

    if let Err(error) = wait_for_chatgpt_prompt(config, &page).await {
        let _ = close_browser_page(config, &page).await;
        return Err(error);
    }
    Ok(page)
}

fn value_as_identifier(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

async fn create_chatgpt_tab_orca(bin: &PathBuf, config: &BrowserDriverConfig) -> Result<String> {
    let result = run_command_json(
        bin,
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

async fn create_chatgpt_tab_agent_browser(bin: &PathBuf) -> Result<String> {
    let session_id = format!("chatgpt-{}", uuid::Uuid::new_v4());
    let _ = run_command_json(
        bin,
        &[
            "--session",
            &session_id,
            "open",
            "https://chatgpt.com",
            "--json",
        ],
    )
    .await?;
    Ok(session_id)
}

async fn create_chatgpt_tab_aside(bin: &PathBuf) -> Result<String> {
    let session_id = format!("chatgpt-{}", uuid::Uuid::new_v4());
    let _ = run_command_json(
        bin,
        &["tab", "new", "--url", "https://chatgpt.com", "--json"],
    )
    .await
    .unwrap_or_default();
    Ok(session_id)
}

pub async fn close_browser_page(config: &BrowserDriverConfig, page: &str) -> Result<()> {
    let (kind, bin) = config.detect().await?;
    match kind {
        BrowserDriverKind::Maho => {
            let _ = run_command_json(&bin, &["tab", "close", page, "--json"]).await;
            Ok(())
        }
        BrowserDriverKind::Orca => {
            let result =
                run_command_json(&bin, &["tab", "close", "--page", page, "--json"]).await?;
            if result.get("ok").and_then(Value::as_bool) != Some(true) {
                return Err(anyhow!("orca tab close failed: {result}"));
            }
            Ok(())
        }
        BrowserDriverKind::AgentBrowser => {
            let _ = run_command_json(&bin, &["--session", page, "close", "--json"]).await;
            Ok(())
        }
        BrowserDriverKind::Aside => {
            let _ = run_command_json(&bin, &["tab", "close", page, "--json"]).await;
            Ok(())
        }
    }
}

pub async fn verify_chatgpt_page(
    config: &BrowserDriverConfig,
    page: &str,
) -> Result<ChatgptPageProbe> {
    if page.trim().is_empty() {
        return Err(anyhow!("browser_page_id cannot be empty"));
    }
    let expression = r#"(() => ({
  ready: !!(document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable="true"]')),
  generating: !!document.querySelector('button[data-testid="stop-button"], button[aria-label="Stop generating"], button[aria-label="Stop answering"]'),
  url: location.href,
  title: document.title
}))()"#;
    let value = eval_json(config, page, expression).await?;
    validate_chatgpt_page_probe(&value)
}

pub async fn send_chatgpt_prompt(
    config: &BrowserDriverConfig,
    page: &str,
    prompt: &str,
) -> Result<()> {
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
  const btn = document.querySelector('button[data-testid="send-button"], button[aria-label="Send prompt"], #composer-submit-button');
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

async fn wait_for_chatgpt_idle(config: &BrowserDriverConfig, page: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(300);
    loop {
        match verify_chatgpt_page(config, page).await {
            Ok(probe) if !probe.generating => return Ok(()),
            Ok(_) => {}
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "timeout waiting for ChatGPT Web generation to finish on page {page}"
            ));
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_chatgpt_prompt(config: &BrowserDriverConfig, page: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        if let Ok(value) = eval_json(
            config,
            page,
            "!!(document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable=\"true\"]'))",
        )
        .await
        {
            if value.as_bool() == Some(true) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "ChatGPT Web prompt box did not become ready; verify the browser is logged into chatgpt.com"
            ));
        }
        sleep(Duration::from_millis(400)).await;
    }
}

fn validate_chatgpt_page_probe(value: &Value) -> Result<ChatgptPageProbe> {
    let url_str = value
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ChatGPT page probe has no url"))?;
    let parsed_url =
        url::Url::parse(url_str).with_context(|| format!("invalid ChatGPT page URL: {url_str}"))?;
    if parsed_url.scheme() != "https" || parsed_url.host_str() != Some("chatgpt.com") {
        return Err(anyhow!(
            "browser page is not on the expected https://chatgpt.com origin"
        ));
    }
    let title = value
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let ready = value.get("ready").and_then(Value::as_bool).unwrap_or(false);
    let generating = value
        .get("generating")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !ready && !generating {
        return Err(anyhow!("ChatGPT Web interface is not active on page"));
    }
    Ok(ChatgptPageProbe {
        url: url_str.to_string(),
        title,
        generating,
    })
}

async fn eval_json(config: &BrowserDriverConfig, page: &str, expression: &str) -> Result<Value> {
    let (kind, bin) = config.detect().await?;
    match kind {
        BrowserDriverKind::Orca => {
            let result = run_command_json(
                &bin,
                &["eval", "--page", page, "--expression", expression, "--json"],
            )
            .await?;
            let raw = result
                .pointer("/result/result")
                .ok_or_else(|| anyhow!("orca eval returned no result"))?;
            decode_eval_value(raw)
        }
        BrowserDriverKind::AgentBrowser => {
            let result =
                run_command_json(&bin, &["--session", page, "eval", expression, "--json"]).await?;
            if let Some(val) = result.pointer("/data/result") {
                return decode_eval_value(val);
            }
            if let Some(val) = result.pointer("/result") {
                return decode_eval_value(val);
            }
            Err(anyhow!("agent-browser eval returned no result"))
        }
        BrowserDriverKind::Maho | BrowserDriverKind::Aside => {
            if is_executable_in_path("orca").await {
                let result = run_command_json(
                    &PathBuf::from("orca"),
                    &["eval", "--page", page, "--expression", expression, "--json"],
                )
                .await?;
                let raw = result
                    .pointer("/result/result")
                    .ok_or_else(|| anyhow!("orca eval compatibility path returned no result"))?;
                return decode_eval_value(raw);
            }
            Err(anyhow!(
                "DOM evaluation is unsupported by this browser driver"
            ))
        }
    }
}

fn decode_eval_value(raw: &Value) -> Result<Value> {
    match raw {
        Value::String(text) => match serde_json::from_str::<Value>(text) {
            Ok(value) => Ok(value),
            Err(_) => Ok(Value::String(text.clone())),
        },
        other => Ok(other.clone()),
    }
}

pub async fn send_prompt(config: &BrowserDriverConfig, terminal: &str, prompt: &str) -> Result<()> {
    let (_, bin) = config.detect().await?;
    let result = run_command_json(
        &bin,
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
        return Err(anyhow!("terminal send failed: {result}"));
    }
    Ok(())
}

pub async fn resolve_terminal(config: &BrowserDriverConfig) -> Result<String> {
    if let Some(terminal) = &config.terminal {
        if verify_terminal(config, terminal).await.is_ok() {
            return Ok(terminal.clone());
        }
    }
    resolve_terminal_uncached(config).await
}

async fn verify_terminal(config: &BrowserDriverConfig, terminal: &str) -> Result<()> {
    let (_, bin) = config.detect().await?;
    let result = run_command_json(
        &bin,
        &["terminal", "show", "--terminal", terminal, "--json"],
    )
    .await?;
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(anyhow!("terminal {terminal} is not reachable: {result}"));
    }
    Ok(())
}

pub async fn resolve_terminal_uncached(config: &BrowserDriverConfig) -> Result<String> {
    let candidates = terminal_candidates(config).await?;
    let matched = candidates
        .into_iter()
        .max_by_key(|candidate| candidate.score)
        .ok_or_else(|| {
            anyhow!(
                "no active coding orchestrator terminal found in worktree '{}'",
                config.worktree
            )
        })?;
    Ok(matched.handle)
}

pub async fn resolve_terminal_for_marker(
    config: &BrowserDriverConfig,
    marker: &str,
) -> Result<String> {
    let (_, bin) = config.detect().await?;
    let listed = run_command_json(
        &bin,
        &["terminal", "list", "--worktree", &config.worktree, "--json"],
    )
    .await?;
    let terminals = listed
        .pointer("/result/terminals")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("terminal list returned no terminals"))?;

    for term in terminals {
        let handle = term
            .get("handle")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if handle.is_empty() {
            continue;
        }
        let read = run_command_json(
            &bin,
            &[
                "terminal",
                "read",
                "--terminal",
                handle,
                "--limit",
                "40",
                "--json",
            ],
        )
        .await?;
        let output = read
            .pointer("/result/output")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if output.contains(marker) {
            return Ok(handle.to_string());
        }
    }
    resolve_terminal(config).await
}

struct TerminalCandidate {
    handle: String,
    score: i64,
}

async fn terminal_candidates(config: &BrowserDriverConfig) -> Result<Vec<TerminalCandidate>> {
    let (_, bin) = config.detect().await?;
    let listed = run_command_json(
        &bin,
        &["terminal", "list", "--worktree", &config.worktree, "--json"],
    )
    .await?;
    let terminals = listed
        .pointer("/result/terminals")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("terminal list returned no terminals"))?;

    let mut candidates = Vec::new();
    for term in terminals {
        let handle = term
            .get("handle")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if handle.is_empty() {
            continue;
        }
        let title = term
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let read = run_command_json(
            &bin,
            &[
                "terminal",
                "read",
                "--terminal",
                &handle,
                "--limit",
                "60",
                "--json",
            ],
        )
        .await?;
        let output = read
            .pointer("/result/output")
            .and_then(Value::as_str)
            .unwrap_or_default();

        let mut score: i64 = 0;
        if title.to_lowercase().contains("omo") || title.to_lowercase().contains("opencode") {
            score += 50;
        }
        if output.contains("[OMO-BRIDGE") || output.contains("omo-bridge") {
            score += 100;
        }
        if output.contains("task_state") || output.contains("completion_check") {
            score += 80;
        }
        candidates.push(TerminalCandidate { handle, score });
    }
    Ok(candidates)
}

async fn run_command_json(bin: &PathBuf, args: &[&str]) -> Result<Value> {
    let output = Command::new(bin)
        .args(args)
        .output()
        .await
        .with_context(|| format!("failed to execute {}", bin.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!(
            "command {} {:?} failed ({}): {} {}",
            bin.display(),
            args,
            output.status,
            stdout.trim(),
            stderr.trim()
        ));
    }
    let value: Value = serde_json::from_str(stdout.trim()).with_context(|| {
        format!(
            "command {} returned invalid JSON: {}",
            bin.display(),
            stdout.trim()
        )
    })?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_browser_driver_kinds() {
        assert_eq!(
            "maho".parse::<BrowserDriverKind>().unwrap(),
            BrowserDriverKind::Maho
        );
        assert_eq!(
            "orca".parse::<BrowserDriverKind>().unwrap(),
            BrowserDriverKind::Orca
        );
        assert_eq!(
            "agent-browser".parse::<BrowserDriverKind>().unwrap(),
            BrowserDriverKind::AgentBrowser
        );
        assert_eq!(
            "aside".parse::<BrowserDriverKind>().unwrap(),
            BrowserDriverKind::Aside
        );
    }

    #[test]
    fn ui_probe_expression_strictly_excludes_conversation_content() {
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("[data-message-author-role]"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("article"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("conversation-turn"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("isConversationRegion"));
        assert!(!CHATGPT_UI_PROBE_EXPRESSION.contains("document.body"));
    }

    #[test]
    fn conversation_limit_text_is_not_a_rate_limit_snapshot_signal() {
        let snapshot = serde_json::json!({
            "ready": true,
            "generating": false,
            "composer_disabled": false,
            "rate_limited": false,
            "rate_limit_reason": null,
            "reset_after_seconds": null,
            "delivery_error": false,
            "delivery_recoverable": false,
            "authentication_required": false,
            "conversation_fixture": "the user prompt contains the word limit and rate limit"
        });
        assert_eq!(
            classify_chatgpt_ui_snapshot(&snapshot),
            ChatgptUiCondition::Healthy
        );
    }

    #[test]
    fn valid_rate_limit_banner_snapshot_is_typed_and_reset_is_validated() {
        let snapshot = serde_json::json!({
            "ready": true,
            "generating": false,
            "composer_disabled": true,
            "rate_limited": true,
            "rate_limit_reason": "usage_limit",
            "reset_after_seconds": 90,
            "delivery_error": false,
            "delivery_recoverable": false,
            "authentication_required": false
        });
        assert_eq!(
            classify_chatgpt_ui_snapshot(&snapshot),
            ChatgptUiCondition::RateLimited {
                reason: ChatgptRateLimitReason::UsageLimit,
                reset_after_seconds: Some(90)
            }
        );

        let invalid_reset = serde_json::json!({
            "ready": true,
            "generating": false,
            "rate_limited": true,
            "rate_limit_reason": "too_many_requests",
            "reset_after_seconds": MAX_RESET_AFTER_SECONDS + 1,
            "delivery_error": false,
            "authentication_required": false
        });
        assert_eq!(
            classify_chatgpt_ui_snapshot(&invalid_reset),
            ChatgptUiCondition::RateLimited {
                reason: ChatgptRateLimitReason::TooManyRequests,
                reset_after_seconds: None
            }
        );
    }

    #[tokio::test]
    async fn drivers_without_dom_eval_return_unsupported_without_running_binary() {
        for driver in [BrowserDriverKind::Maho, BrowserDriverKind::Aside] {
            let config = BrowserDriverConfig::with_driver(
                Some(driver),
                Some(PathBuf::from("definitely-not-a-real-browser-binary")),
                "active",
                None,
            );
            assert_eq!(
                probe_chatgpt_ui_condition(&config, "page-1").await,
                ChatgptUiCondition::Unsupported
            );
        }
    }

    #[test]
    fn reset_validation_rejects_zero_and_unbounded_values() {
        assert_eq!(validate_reset_after_seconds(0), None);
        assert_eq!(validate_reset_after_seconds(1), Some(1));
        assert_eq!(
            validate_reset_after_seconds(MAX_RESET_AFTER_SECONDS),
            Some(MAX_RESET_AFTER_SECONDS)
        );
        assert_eq!(
            validate_reset_after_seconds(MAX_RESET_AFTER_SECONDS + 1),
            None
        );
    }
}
