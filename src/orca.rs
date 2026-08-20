use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Duration};

pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(20);

pub const MAX_RESET_AFTER_SECONDS: u64 = 31 * 24 * 60 * 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserDriverKind {
    Maho,
    Orca,
    Cmux,
    AgentBrowser,
    Aside,
}

impl BrowserDriverKind {
    pub fn supports_chatgpt_dom_probe(self) -> bool {
        matches!(self, Self::Orca | Self::Cmux | Self::AgentBrowser)
    }
}

impl std::fmt::Display for BrowserDriverKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Maho => write!(f, "maho"),
            Self::Orca => write!(f, "orca"),
            Self::Cmux => write!(f, "cmux"),
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
            "cmux" => Ok(Self::Cmux),
            "agent-browser" | "agent_browser" => Ok(Self::AgentBrowser),
            "aside" => Ok(Self::Aside),
            other => Err(anyhow!(
                "unsupported browser driver '{other}'; supported: maho, orca, cmux, agent-browser, aside"
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
    resolved: Arc<OnceCell<(BrowserDriverKind, PathBuf)>>,
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
            resolved: Arc::new(OnceCell::new()),
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
            resolved: Arc::new(OnceCell::new()),
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
        self.resolved
            .get_or_try_init(|| self.detect_uncached())
            .await
            .map(|(kind, bin)| (*kind, bin.clone()))
    }

    async fn detect_uncached(&self) -> Result<(BrowserDriverKind, PathBuf)> {
        if let Some(kind) = &self.driver {
            if let Some(bin) = &self.binary {
                return Ok((*kind, bin.clone()));
            }
            let default_bin = match kind {
                BrowserDriverKind::Maho => {
                    resolve_maho_bin().unwrap_or_else(|| PathBuf::from("maho"))
                }
                BrowserDriverKind::Orca => PathBuf::from("orca"),
                BrowserDriverKind::Cmux => PathBuf::from("cmux"),
                BrowserDriverKind::AgentBrowser => PathBuf::from("agent-browser"),
                BrowserDriverKind::Aside => PathBuf::from("aside"),
            };
            return Ok((*kind, default_bin));
        }

        for (kind, command) in automatic_browser_driver_priority() {
            if is_executable_in_path(command).await {
                return Ok((*kind, PathBuf::from(command)));
            }
        }
        if let Some(maho_bin) = resolve_maho_bin() {
            return Ok((BrowserDriverKind::Maho, maho_bin));
        }

        Ok((
            BrowserDriverKind::Maho,
            resolve_maho_bin().unwrap_or_else(|| PathBuf::from("maho")),
        ))
    }
}

fn automatic_browser_driver_priority() -> &'static [(BrowserDriverKind, &'static str)] {
    &[
        (BrowserDriverKind::Cmux, "cmux"),
        (BrowserDriverKind::Orca, "orca"),
        (BrowserDriverKind::Maho, "maho"),
        (BrowserDriverKind::AgentBrowser, "agent-browser"),
        (BrowserDriverKind::Aside, "aside"),
    ]
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
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&path).any(|directory| {
        let candidate = directory.join(bin_name);
        let Ok(metadata) = fs::metadata(candidate) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o111 == 0 {
            return false;
        }
        true
    })
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

  const firstVisible = (selector) => Array.from(document.querySelectorAll(selector)).find(isVisible) || null;
  const composer = firstVisible('#prompt-textarea, [data-testid="composer-text-input"], textarea[placeholder], [contenteditable="true"]');
  const sendButton = firstVisible('button[data-testid="send-button"], button[aria-label="Send prompt"], #composer-submit-button');
  const stopButton = firstVisible('button[data-testid="stop-button"], button[aria-label="Stop generating"], button[aria-label="Stop answering"]');
  const composerDisabled = !!(
    (composer && (composer.disabled || composer.getAttribute('aria-disabled') === 'true')) ||
    (sendButton && (sendButton.disabled || sendButton.getAttribute('aria-disabled') === 'true'))
  );
  const generating = !!stopButton;
  const ready = !!composer;

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

const CHATGPT_SEND_EXPRESSION: &str = r#"(() => {
  const visible = (candidate) => {
    if (!candidate || !(candidate instanceof Element)) return false;
    const style = window.getComputedStyle(candidate);
    const rect = candidate.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0 && rect.width > 0 && rect.height > 0;
  };
  const btn = Array.from(document.querySelectorAll('button[data-testid="send-button"], button[aria-label="Send prompt"], #composer-submit-button')).find(visible);
  if (!btn || btn.disabled) return { ok: false, error: 'no_send_button' };
  btn.click();
  return { ok: true };
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
        BrowserDriverKind::Cmux => create_chatgpt_tab_cmux(&bin, config).await,
        BrowserDriverKind::AgentBrowser => create_chatgpt_tab_agent_browser(&bin).await,
        BrowserDriverKind::Aside => create_chatgpt_tab_aside(&bin).await,
    }
}

async fn create_chatgpt_tab_cmux(bin: &PathBuf, config: &BrowserDriverConfig) -> Result<String> {
    let args = cmux_new_browser_surface_args(&config.worktree);
    let args = args.iter().map(String::as_str).collect::<Vec<_>>();
    let result = run_command_json(bin, &args).await?;
    let page = cmux_surface_ref(&result)?;

    if let Err(error) = wait_for_chatgpt_prompt(config, &page).await {
        let _ = close_browser_page(config, &page).await;
        return Err(error);
    }
    Ok(page)
}

fn cmux_new_browser_surface_args(worktree: &str) -> Vec<String> {
    let mut args = vec![
        "--json".into(),
        "new-surface".into(),
        "--type".into(),
        "browser".into(),
        "--url".into(),
        "https://chatgpt.com".into(),
    ];
    if worktree.trim() != "active" {
        args.extend(["--workspace".into(), worktree.into()]);
    }
    args.extend(["--focus".into(), "true".into()]);
    args
}

fn cmux_close_surface_args(worktree: &str, page: &str) -> Vec<String> {
    let mut args = vec!["--json".into(), "close-surface".into()];
    if worktree.trim() != "active" {
        args.extend(["--workspace".into(), worktree.into()]);
    }
    args.extend(["--surface".into(), page.into()]);
    args
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

fn cmux_surface_ref(result: &Value) -> Result<String> {
    result
        .pointer("/surface/ref")
        .or_else(|| result.pointer("/result/surface/ref"))
        .or_else(|| result.pointer("/surface_ref"))
        .or_else(|| result.pointer("/result/surface_ref"))
        .and_then(Value::as_str)
        .filter(|value| value.starts_with("surface:"))
        .map(str::to_string)
        .ok_or_else(|| anyhow!("cmux browser surface creation returned no surface ref: {result}"))
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
        BrowserDriverKind::Cmux => {
            let args = cmux_close_surface_args(&config.worktree, page);
            let args = args.iter().map(String::as_str).collect::<Vec<_>>();
            let _ = run_command_json(&bin, &args).await?;
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
    let expression = r#"(() => {
  const visible = (el) => {
    if (!el || !(el instanceof Element)) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0 && rect.width > 0 && rect.height > 0;
  };
  const composer = Array.from(document.querySelectorAll('#prompt-textarea, [data-testid="composer-text-input"], textarea[placeholder], [contenteditable="true"]')).find(visible);
  return {
  ready: !!composer,
  generating: !!document.querySelector('button[data-testid="stop-button"], button[aria-label="Stop generating"], button[aria-label="Stop answering"]'),
  url: location.href,
  title: document.title
};
})()"#;
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
  const visible = (candidate) => {{
    if (!candidate || !(candidate instanceof Element)) return false;
    const style = window.getComputedStyle(candidate);
    const rect = candidate.getBoundingClientRect();
    return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0 && rect.width > 0 && rect.height > 0;
  }};
  const el = Array.from(document.querySelectorAll('#prompt-textarea, [data-testid="composer-text-input"], textarea[placeholder], [contenteditable="true"]')).find(visible);
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
    let sent = eval_json(config, page, CHATGPT_SEND_EXPRESSION).await?;
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    // Give newly created tabs a brief initial grace period to start navigating
    sleep(Duration::from_millis(500)).await;

    let probe_expr = r#"(() => {
        const visible = (candidate) => {
            if (!candidate || !(candidate instanceof Element)) return false;
            const style = window.getComputedStyle(candidate);
            const rect = candidate.getBoundingClientRect();
            return style.display !== 'none' && style.visibility !== 'hidden' && Number(style.opacity) !== 0 && rect.width > 0 && rect.height > 0;
        };
        return Array.from(document.querySelectorAll('#prompt-textarea, [data-testid="composer-text-input"], textarea[placeholder], [contenteditable="true"]')).some(visible);
    })()"#;

    loop {
        if let Ok(value) = eval_json(config, page, probe_expr).await {
            if value.as_bool() == Some(true) {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow!(
                "ChatGPT Web prompt box did not become ready within 60s; verify the browser is logged into chatgpt.com"
            ));
        }
        sleep(Duration::from_millis(500)).await;
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
        BrowserDriverKind::Cmux => {
            let result =
                run_command_json(&bin, &["--json", "browser", page, "eval", expression]).await?;
            let raw = result
                .pointer("/result")
                .or_else(|| result.pointer("/value"))
                .or_else(|| result.pointer("/data/result"))
                .ok_or_else(|| anyhow!("cmux browser eval returned no result: {result}"))?;
            decode_eval_value(raw)
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
    let result = run_command_json_with_stdin(
        &bin,
        &[
            "terminal",
            "send",
            "--terminal",
            terminal,
            "--enter",
            "--json",
        ],
        prompt,
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
        if output.contains("[GPT2OMO") || output.contains("gpt2omo") {
            score += 100;
        }
        if output.contains("task_state") || output.contains("completion_check") {
            score += 80;
        }
        candidates.push(TerminalCandidate { handle, score });
    }
    Ok(candidates)
}

const MAX_COMMAND_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

async fn run_command_json(bin: &PathBuf, args: &[&str]) -> Result<Value> {
    let output = execute_command(bin, args, None).await?;
    parse_command_json(bin, args, output)
}

async fn run_command_json_with_stdin(
    bin: &PathBuf,
    args: &[&str],
    stdin_text: &str,
) -> Result<Value> {
    let output = execute_command(bin, args, Some(stdin_text)).await?;
    parse_command_json(bin, args, output)
}

async fn execute_command(
    bin: &PathBuf,
    args: &[&str],
    stdin_text: Option<&str>,
) -> Result<std::process::Output> {
    let mut command = Command::new(bin);
    command
        .args(args)
        .stdin(if stdin_text.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to execute {}", bin.display()))?;

    let output = timeout(
        COMMAND_TIMEOUT,
        collect_command_output(&mut child, stdin_text),
    )
    .await;
    match output {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(error).with_context(|| format!("failed while waiting for {}", bin.display()))
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            Err(anyhow!(
                "command {} {:?} timed out after {:?}",
                bin.display(),
                args,
                COMMAND_TIMEOUT
            ))
        }
    }
}

async fn collect_command_output(
    child: &mut tokio::process::Child,
    stdin_text: Option<&str>,
) -> Result<std::process::Output> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("command stdout was not piped"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("command stderr was not piped"))?;
    let mut stdin = child.stdin.take();
    let stdin_write = async {
        if let (Some(stdin), Some(text)) = (stdin.as_mut(), stdin_text) {
            stdin.write_all(text.as_bytes()).await?;
        }
        drop(stdin.take());
        Ok::<(), anyhow::Error>(())
    };

    let (stdout, stderr, status, stdin_result) = tokio::join!(
        read_command_output(stdout),
        read_command_output(stderr),
        child.wait(),
        stdin_write,
    );
    stdin_result?;
    Ok(std::process::Output {
        status: status?,
        stdout: stdout?,
        stderr: stderr?,
    })
}

async fn read_command_output<R>(mut reader: R) -> Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut output = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    loop {
        let read = reader.read(&mut chunk).await?;
        if read == 0 {
            return Ok(output);
        }
        if output.len() + read > MAX_COMMAND_OUTPUT_BYTES {
            return Err(anyhow!(
                "command output exceeded {} MiB",
                MAX_COMMAND_OUTPUT_BYTES / (1024 * 1024)
            ));
        }
        output.extend_from_slice(&chunk[..read]);
    }
}

fn parse_command_json(bin: &Path, args: &[&str], output: std::process::Output) -> Result<Value> {
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
    fn automatic_driver_priority_prefers_cmux_then_orca() {
        assert_eq!(
            automatic_browser_driver_priority(),
            [
                (BrowserDriverKind::Cmux, "cmux"),
                (BrowserDriverKind::Orca, "orca"),
                (BrowserDriverKind::Maho, "maho"),
                (BrowserDriverKind::AgentBrowser, "agent-browser"),
                (BrowserDriverKind::Aside, "aside"),
            ]
        );
    }

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
        assert_eq!(
            "cmux".parse::<BrowserDriverKind>().unwrap(),
            BrowserDriverKind::Cmux
        );
    }

    #[tokio::test]
    async fn explicit_cmux_driver_uses_cmux_binary() {
        let config = BrowserDriverConfig::with_driver(
            Some(BrowserDriverKind::Cmux),
            None,
            "workspace:6",
            None,
        );

        assert_eq!(
            config.detect().await.unwrap(),
            (BrowserDriverKind::Cmux, PathBuf::from("cmux"))
        );
    }

    #[test]
    fn cmux_surface_creation_requires_a_surface_reference() {
        let result = serde_json::json!({"surface":{"ref":"surface:42"}});
        assert_eq!(cmux_surface_ref(&result).unwrap(), "surface:42");

        let missing_ref = serde_json::json!({"surface":{"ref":"browser:42"}});
        assert!(cmux_surface_ref(&missing_ref).is_err());
    }

    #[test]
    fn cmux_active_workspace_uses_the_current_workspace() {
        let args = cmux_new_browser_surface_args("active");
        assert!(!args.iter().any(|argument| argument == "--workspace"));
        assert_eq!(
            args,
            vec![
                "--json",
                "new-surface",
                "--type",
                "browser",
                "--url",
                "https://chatgpt.com",
                "--focus",
                "true",
            ]
        );
    }

    #[test]
    fn cmux_explicit_workspace_is_forwarded_to_surface_creation() {
        let args = cmux_new_browser_surface_args("workspace:7");
        assert_eq!(
            args,
            vec![
                "--json",
                "new-surface",
                "--type",
                "browser",
                "--url",
                "https://chatgpt.com",
                "--workspace",
                "workspace:7",
                "--focus",
                "true",
            ]
        );
    }

    #[test]
    fn cmux_surface_close_uses_its_configured_workspace() {
        assert_eq!(
            cmux_close_surface_args("workspace:7", "surface:43"),
            vec![
                "--json",
                "close-surface",
                "--workspace",
                "workspace:7",
                "--surface",
                "surface:43",
            ]
        );
    }

    #[test]
    fn ui_probe_expression_strictly_excludes_conversation_content() {
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("[data-message-author-role]"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("article"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("conversation-turn"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("isConversationRegion"));
        assert!(CHATGPT_UI_PROBE_EXPRESSION.contains("firstVisible"));
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

    #[test]
    fn prompt_json_escapes_properly() {
        let prompt = "test prompt with \"quotes\" and \n newlines";
        let prompt_json = serde_json::to_string(prompt).unwrap();
        assert!(prompt_json.starts_with('"') && prompt_json.ends_with('"'));
    }

    #[test]
    fn cmux_send_expression_contains_valid_object_braces() {
        assert!(!CHATGPT_SEND_EXPRESSION.contains("{{"));
        assert!(!CHATGPT_SEND_EXPRESSION.contains("}}"));
    }

    #[tokio::test]
    async fn detect_caches_resolved_driver_and_binary() {
        let config = BrowserDriverConfig::with_driver(
            Some(BrowserDriverKind::Orca),
            Some(PathBuf::from("/custom/orca")),
            "worktree",
            None,
        );
        assert!(config.resolved.get().is_none());
        let first = config.detect().await.unwrap();
        let second = config.detect().await.unwrap();
        assert_eq!(
            first,
            (BrowserDriverKind::Orca, PathBuf::from("/custom/orca"))
        );
        assert_eq!(first, second);
        assert_eq!(
            config.resolved.get(),
            Some(&(BrowserDriverKind::Orca, PathBuf::from("/custom/orca")))
        );
    }
}
