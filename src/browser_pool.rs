use crate::accounts::{
    load_accounts_config, load_accounts_config_from_path, AccountConfig, LegacyAccountConfig,
};
use crate::error::{BridgeError, Result as BridgeResult};
use crate::orca::{
    close_browser_page, create_chatgpt_tab, probe_chatgpt_ui_condition, send_chatgpt_prompt,
    verify_chatgpt_page, BrowserDriverConfig, BrowserDriverKind, ChatgptPageProbe,
    ChatgptRateLimitReason, ChatgptUiCondition,
};
use crate::security::BrowserBinding;
use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use tokio::process::Command;
use tokio::time::{sleep, timeout, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

const CHATGPT_URL: &str = "https://chatgpt.com";
const PAGE_READY_TIMEOUT: Duration = Duration::from_secs(60);
const GENERATION_IDLE_TIMEOUT: Duration = Duration::from_secs(300);
const BROWSER_STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const BROWSER_STARTUP_POLL: Duration = Duration::from_millis(250);
const CDP_WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CDP_WS_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BrowserTarget {
    pub account_id: String,
    pub instance: String,
    pub driver: BrowserDriverKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdp_endpoint: Option<String>,
    pub worktree: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PageHandle {
    pub target: BrowserTarget,
    pub page_id: String,
}

impl PageHandle {
    pub fn binding(&self) -> BrowserBinding {
        BrowserBinding::new(
            self.target.account_id.clone(),
            self.target.driver,
            self.target.instance.clone(),
            self.page_id.clone(),
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserReachability {
    Reachable,
    Unreachable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserLoginState {
    Unknown,
    Ready,
    AuthenticationRequired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct BrowserHealth {
    pub account_id: String,
    pub instance: String,
    pub driver: Option<BrowserDriverKind>,
    pub reachability: BrowserReachability,
    pub login_state: BrowserLoginState,
    pub login_required: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone)]
pub struct BrowserPool {
    bridge_dir: Arc<PathBuf>,
    mount_root: Arc<PathBuf>,
    config_path: Option<Arc<PathBuf>>,
    legacy: LegacyAccountConfig,
    legacy_driver: BrowserDriverConfig,
    http: reqwest::Client,
    profile_leases: Arc<Mutex<HashMap<PathBuf, BrowserProfileLock>>>,
}

pub struct BrowserProfileLock {
    file: File,
}

impl Drop for BrowserProfileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl BrowserPool {
    pub fn new(
        bridge_dir: impl Into<PathBuf>,
        mount_root: impl Into<PathBuf>,
        legacy: LegacyAccountConfig,
        legacy_driver: BrowserDriverConfig,
    ) -> Self {
        Self {
            bridge_dir: Arc::new(bridge_dir.into()),
            mount_root: Arc::new(mount_root.into()),
            config_path: None,
            legacy,
            legacy_driver,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            profile_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_config_path(
        bridge_dir: impl Into<PathBuf>,
        mount_root: impl Into<PathBuf>,
        legacy: LegacyAccountConfig,
        legacy_driver: BrowserDriverConfig,
        config_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            bridge_dir: Arc::new(bridge_dir.into()),
            mount_root: Arc::new(mount_root.into()),
            config_path: Some(Arc::new(config_path.into())),
            legacy,
            legacy_driver,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
            profile_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn load_config(&self) -> BridgeResult<crate::accounts::AccountsConfig> {
        match self.config_path.as_deref() {
            Some(path) => load_accounts_config_from_path(
                path,
                &self.bridge_dir,
                &self.mount_root,
                self.legacy.clone(),
            ),
            None => load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone()),
        }
    }

    pub async fn target_for_account(
        &self,
        account_id: &str,
        allow_disabled: bool,
    ) -> Result<BrowserTarget> {
        let config = self.load_config().map_err(anyhow::Error::new)?;
        let account = config.account(account_id).ok_or_else(|| {
            anyhow!(
                "BROWSER_ACCOUNT_UNAVAILABLE: account '{}' is missing from accounts.json",
                account_id
            )
        })?;
        if !allow_disabled && (!account.enabled || account.draining) {
            let state = if account.draining {
                "draining"
            } else {
                "disabled"
            };
            return Err(anyhow!(
                "BROWSER_ACCOUNT_UNAVAILABLE: account '{}' is {} for new work",
                account_id,
                state
            ));
        }
        self.target_from_account(account).await
    }

    pub async fn target_for_binding(&self, binding: &BrowserBinding) -> Result<BrowserTarget> {
        let target = self.target_for_account(&binding.account_id, true).await?;
        if target.instance != binding.instance {
            return Err(anyhow!(
                "BROWSER_ACCOUNT_UNAVAILABLE: account '{}' browser instance changed from '{}' to '{}'",
                binding.account_id,
                binding.instance,
                target.instance
            ));
        }
        if target.driver != binding.driver {
            return Err(anyhow!(
                "BROWSER_ACCOUNT_UNAVAILABLE: account '{}' browser driver changed from '{}' to '{}'",
                binding.account_id,
                binding.driver,
                target.driver
            ));
        }
        Ok(target)
    }

    async fn target_from_account(&self, account: &AccountConfig) -> Result<BrowserTarget> {
        let driver = match account.browser.driver {
            Some(driver) => driver,
            None => self.legacy_driver.detect().await?.0,
        };
        Ok(BrowserTarget {
            account_id: account.id.clone(),
            instance: account.browser.instance.clone(),
            driver,
            user_data_dir: account.browser.user_data_dir.clone(),
            cdp_endpoint: account.browser.cdp_endpoint.clone(),
            worktree: account.browser.worktree.clone(),
        })
    }

    pub fn provision_profiles(&self) -> Result<Vec<PathBuf>> {
        let config = self.load_config().map_err(anyhow::Error::new)?;
        let mut created = Vec::new();
        for account in config
            .accounts
            .iter()
            .filter(|account| account.enabled || account.draining)
        {
            if let Some(profile) = account.browser.user_data_dir.as_deref() {
                fs::create_dir_all(profile).with_context(|| {
                    format!("failed to provision browser profile {}", profile.display())
                })?;
                #[cfg(unix)]
                fs::set_permissions(profile, fs::Permissions::from_mode(0o700))?;
                created.push(profile.to_path_buf());
            }
        }
        Ok(created)
    }

    pub fn try_lock_profile(&self, target: &BrowserTarget) -> Result<Option<BrowserProfileLock>> {
        let Some(profile) = target.user_data_dir.as_deref() else {
            return Ok(None);
        };
        fs::create_dir_all(profile)?;
        #[cfg(unix)]
        fs::set_permissions(profile, fs::Permissions::from_mode(0o700))?;
        let lock_path = profile.join(".gpt2omo-profile.lock");
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(&lock_path)?;
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        match file.try_lock() {
            Ok(()) => Ok(Some(BrowserProfileLock { file })),
            Err(TryLockError::WouldBlock) => Err(anyhow!(
                "browser profile is already leased by another bridge process: {}",
                profile.display()
            )),
            Err(TryLockError::Error(error)) => Err(error.into()),
        }
    }

    fn ensure_profile_lease(&self, target: &BrowserTarget) -> Result<()> {
        let Some(profile) = target.user_data_dir.as_ref() else {
            return Ok(());
        };
        let mut leases = self
            .profile_leases
            .lock()
            .map_err(|_| anyhow!("browser profile lease registry is poisoned"))?;
        if leases.contains_key(profile) {
            return Ok(());
        }
        let lease = self.try_lock_profile(target)?.ok_or_else(|| {
            anyhow!(
                "account '{}' has a CDP endpoint but no browser.user_data_dir for safe browser ownership",
                target.account_id
            )
        })?;
        leases.insert(profile.clone(), lease);
        Ok(())
    }

    pub async fn create_chatgpt_page(&self, account_id: &str) -> Result<PageHandle> {
        let target = self.target_for_account(account_id, false).await?;
        self.validate_creation_isolation(&target)?;
        if let Some(endpoint) = target.cdp_endpoint.as_deref() {
            self.ensure_browser_instance(&target).await?;
            let page_id = self.cdp_create_page(endpoint).await?;
            let handle = PageHandle { target, page_id };
            if let Err(error) = self.wait_for_prompt(&handle).await {
                let _ = self.close(&handle.binding()).await;
                return Err(error);
            }
            return Ok(handle);
        }

        let driver_config = self.driver_config(&target);
        let page_id = create_chatgpt_tab(&driver_config).await?;
        Ok(PageHandle { target, page_id })
    }

    pub async fn open_chatgpt_login_page(&self, account_id: &str) -> Result<PageHandle> {
        let target = self.target_for_account(account_id, false).await?;
        self.validate_creation_isolation(&target)?;
        if let Some(endpoint) = target.cdp_endpoint.as_deref() {
            self.ensure_browser_instance(&target).await?;
            return Ok(PageHandle {
                page_id: self.cdp_create_page(endpoint).await?,
                target,
            });
        }

        let page_id = create_chatgpt_tab(&self.driver_config(&target)).await?;
        Ok(PageHandle { target, page_id })
    }

    async fn ensure_browser_instance(&self, target: &BrowserTarget) -> Result<()> {
        let endpoint = target
            .cdp_endpoint
            .as_deref()
            .ok_or_else(|| anyhow!("browser target has no CDP endpoint"))?;
        self.ensure_profile_lease(target)?;
        if self.ensure_cdp_reachable(endpoint).await.is_ok() {
            return Ok(());
        }

        let executable = discover_chromium_executable().ok_or_else(|| {
            anyhow!(
                "browser instance '{}' for account '{}' is unreachable at {} and no Chromium executable was found; start that profile manually or set OMO_CHROMIUM_BIN",
                target.instance,
                target.account_id,
                endpoint
            )
        })?;
        let args = chromium_launch_args(target)?;
        let mut command = Command::new(&executable);
        command
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
        let child = command.spawn().with_context(|| {
            format!(
                "failed to start Chromium '{}' for account '{}'",
                executable.display(),
                target.account_id
            )
        })?;
        tracing::info!(
            account_id = %target.account_id,
            browser_instance = %target.instance,
            pid = child.id(),
            "started isolated Chromium profile for account"
        );
        drop(child);

        let deadline = Instant::now() + BROWSER_STARTUP_TIMEOUT;
        loop {
            if self.ensure_cdp_reachable(endpoint).await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "browser instance '{}' for account '{}' did not expose CDP at {} after startup",
                    target.instance,
                    target.account_id,
                    endpoint
                ));
            }
            sleep(BROWSER_STARTUP_POLL).await;
        }
    }

    fn validate_creation_isolation(&self, target: &BrowserTarget) -> Result<()> {
        let config = self.load_config().map_err(anyhow::Error::new)?;
        let live_instance_count = config
            .accounts
            .iter()
            .filter(|account| account.enabled || account.draining)
            .count();
        if live_instance_count > 1
            && (target.cdp_endpoint.is_none() || target.user_data_dir.is_none())
        {
            return Err(anyhow!(
                "multi-account browser isolation requires every enabled account to configure a distinct browser.cdp_endpoint and browser.user_data_dir; account '{}' is incomplete",
                target.account_id
            ));
        }
        Ok(())
    }

    pub async fn verify(&self, binding: &BrowserBinding) -> Result<ChatgptPageProbe> {
        let target = self.target_for_binding(binding).await?;
        if target.cdp_endpoint.is_some() {
            self.ensure_profile_lease(&target)?;
            return self.cdp_verify(&target, &binding.page_id).await;
        }
        verify_chatgpt_page(&self.driver_config(&target), &binding.page_id).await
    }

    pub async fn probe(&self, binding: &BrowserBinding) -> ChatgptUiCondition {
        let Ok(target) = self.target_for_binding(binding).await else {
            return ChatgptUiCondition::Unknown;
        };
        if target.cdp_endpoint.is_some() {
            if self.ensure_profile_lease(&target).is_err() {
                return ChatgptUiCondition::Unknown;
            }
            return self.cdp_probe(&target, &binding.page_id).await;
        }
        probe_chatgpt_ui_condition(&self.driver_config(&target), &binding.page_id).await
    }

    pub async fn send(&self, binding: &BrowserBinding, prompt: &str) -> Result<()> {
        let target = self.target_for_binding(binding).await?;
        if target.cdp_endpoint.is_some() {
            self.ensure_profile_lease(&target)?;
            return self.cdp_send(&target, &binding.page_id, prompt).await;
        }
        send_chatgpt_prompt(&self.driver_config(&target), &binding.page_id, prompt).await
    }

    pub async fn close(&self, binding: &BrowserBinding) -> Result<()> {
        let target = self.target_for_binding(binding).await?;
        if let Some(endpoint) = target.cdp_endpoint.as_deref() {
            self.ensure_profile_lease(&target)?;
            return self.cdp_close_page(endpoint, &binding.page_id).await;
        }
        close_browser_page(&self.driver_config(&target), &binding.page_id).await
    }

    pub async fn health(&self, account_id: &str) -> BrowserHealth {
        let target = match self.target_for_account(account_id, true).await {
            Ok(target) => target,
            Err(error) => {
                return BrowserHealth {
                    account_id: account_id.to_string(),
                    instance: String::new(),
                    driver: None,
                    reachability: BrowserReachability::Unreachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(error.to_string()),
                }
            }
        };
        if let Some(endpoint) = target.cdp_endpoint.as_deref() {
            if let Err(error) = self.ensure_profile_lease(&target) {
                return BrowserHealth {
                    account_id: target.account_id,
                    instance: target.instance,
                    driver: Some(target.driver),
                    reachability: BrowserReachability::Unreachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(error.to_string()),
                };
            }
            if let Err(error) = self.ensure_cdp_reachable(endpoint).await {
                return BrowserHealth {
                    account_id: target.account_id,
                    instance: target.instance,
                    driver: Some(target.driver),
                    reachability: BrowserReachability::Unreachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(error.to_string()),
                };
            }

            let targets = match self.cdp_list_targets(endpoint).await {
                Ok(targets) => targets,
                Err(error) => {
                    return BrowserHealth {
                        account_id: target.account_id,
                        instance: target.instance,
                        driver: Some(target.driver),
                        reachability: BrowserReachability::Reachable,
                        login_state: BrowserLoginState::Unknown,
                        login_required: false,
                        detail: Some(format!("CDP reachable but target listing failed: {error}")),
                    }
                }
            };
            let Some(chatgpt) = targets
                .into_iter()
                .find(|candidate| candidate.url.starts_with("https://chatgpt.com"))
            else {
                return BrowserHealth {
                    account_id: target.account_id,
                    instance: target.instance,
                    driver: Some(target.driver),
                    reachability: BrowserReachability::Reachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(
                        "no live ChatGPT page is available to determine login state".into(),
                    ),
                };
            };
            let condition = self.cdp_probe(&target, &chatgpt.id).await;
            let login_state = match condition {
                ChatgptUiCondition::AuthenticationRequired => {
                    BrowserLoginState::AuthenticationRequired
                }
                ChatgptUiCondition::Healthy | ChatgptUiCondition::Generating => {
                    BrowserLoginState::Ready
                }
                _ => BrowserLoginState::Unknown,
            };
            BrowserHealth {
                account_id: target.account_id,
                instance: target.instance,
                driver: Some(target.driver),
                reachability: BrowserReachability::Reachable,
                login_required: login_state == BrowserLoginState::AuthenticationRequired,
                login_state,
                detail: None,
            }
        } else {
            match self.driver_config(&target).detect().await {
                Ok(_) => BrowserHealth {
                    account_id: target.account_id,
                    instance: target.instance,
                    driver: Some(target.driver),
                    reachability: BrowserReachability::Reachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(
                        "legacy driver reachability only; login state requires a page probe".into(),
                    ),
                },
                Err(error) => BrowserHealth {
                    account_id: target.account_id,
                    instance: target.instance,
                    driver: Some(target.driver),
                    reachability: BrowserReachability::Unreachable,
                    login_state: BrowserLoginState::Unknown,
                    login_required: false,
                    detail: Some(error.to_string()),
                },
            }
        }
    }

    fn driver_config(&self, target: &BrowserTarget) -> BrowserDriverConfig {
        BrowserDriverConfig::with_driver(
            Some(target.driver),
            self.legacy_driver.binary.clone(),
            target.worktree.clone(),
            self.legacy_driver.terminal.clone(),
        )
    }

    async fn ensure_cdp_reachable(&self, endpoint: &str) -> Result<()> {
        let url = cdp_url(endpoint, "json/version")?;
        let response = self
            .http
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("browser CDP endpoint is unreachable at {url}"))?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "browser CDP endpoint {} returned HTTP {}",
                url,
                response.status()
            ));
        }
        let _: Value = response
            .json()
            .await
            .context("browser CDP /json/version returned invalid JSON")?;
        Ok(())
    }

    async fn cdp_create_page(&self, endpoint: &str) -> Result<String> {
        let mut url = cdp_url(endpoint, "json/new")?;
        url.set_query(Some(CHATGPT_URL));
        let response = self.http.put(url.clone()).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "CDP target creation at {} failed with HTTP {}",
                url,
                response.status()
            ));
        }
        let value: Value = response.json().await?;
        value
            .get("id")
            .or_else(|| value.get("targetId"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("CDP target creation returned no target id: {value}"))
    }

    async fn cdp_close_page(&self, endpoint: &str, page_id: &str) -> Result<()> {
        validate_page_id(page_id)?;
        let url = cdp_url(endpoint, &format!("json/close/{page_id}"))?;
        let response = self.http.get(url.clone()).send().await?;
        if matches!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::GONE
        ) {
            // Idempotent close: a prior GC/close may have already removed the target before
            // its retained-scope metadata was durably cleaned up.
            return Ok(());
        }
        if !response.status().is_success() {
            return Err(anyhow!(
                "CDP close for page {} failed with HTTP {}",
                page_id,
                response.status()
            ));
        }
        Ok(())
    }

    async fn cdp_list_targets(&self, endpoint: &str) -> Result<Vec<CdpTarget>> {
        let url = cdp_url(endpoint, "json/list")?;
        let response = self.http.get(url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "CDP target list returned HTTP {}",
                response.status()
            ));
        }
        Ok(response.json().await?)
    }

    async fn cdp_target_ws(&self, endpoint: &str, page_id: &str) -> Result<String> {
        validate_page_id(page_id)?;
        let targets = self.cdp_list_targets(endpoint).await?;
        let target = targets
            .into_iter()
            .find(|target| target.id == page_id)
            .ok_or_else(|| {
                anyhow!(
                    "CDP target '{}' does not exist on configured browser instance",
                    page_id
                )
            })?;
        validate_loopback_ws(&target.web_socket_debugger_url)?;
        Ok(target.web_socket_debugger_url)
    }

    async fn cdp_eval(
        &self,
        target: &BrowserTarget,
        page_id: &str,
        expression: &str,
    ) -> Result<Value> {
        let endpoint = target
            .cdp_endpoint
            .as_deref()
            .ok_or_else(|| anyhow!("browser target has no CDP endpoint"))?;
        let ws_url = self.cdp_target_ws(endpoint, page_id).await?;
        let (mut socket, _) = timeout(CDP_WS_CONNECT_TIMEOUT, connect_async(&ws_url))
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out connecting to CDP websocket for {}",
                    target.instance
                )
            })?
            .with_context(|| {
                format!("failed to connect to CDP websocket for {}", target.instance)
            })?;
        let request_id = 1u64;
        let request = json!({
            "id": request_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": expression,
                "returnByValue": true,
                "awaitPromise": true,
                "userGesture": true
            }
        });
        timeout(
            CDP_WS_RESPONSE_TIMEOUT,
            socket.send(Message::Text(request.to_string().into())),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "timed out sending CDP Runtime.evaluate request for {}",
                target.instance
            )
        })??;
        let deadline = Instant::now() + CDP_WS_RESPONSE_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!(
                    "timed out waiting for CDP Runtime.evaluate response for {}",
                    target.instance
                ));
            }
            let message = timeout(remaining, socket.next()).await.map_err(|_| {
                anyhow!(
                    "timed out waiting for CDP Runtime.evaluate response for {}",
                    target.instance
                )
            })?;
            let Some(message) = message else {
                return Err(anyhow!(
                    "CDP websocket closed before Runtime.evaluate response"
                ));
            };
            let message = message?;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(text.as_ref())?;
            if value.get("id").and_then(Value::as_u64) != Some(request_id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(anyhow!("CDP Runtime.evaluate failed: {error}"));
            }
            if let Some(exception) = value.pointer("/result/exceptionDetails") {
                return Err(anyhow!("CDP JavaScript exception: {exception}"));
            }
            let remote = value
                .pointer("/result/result")
                .ok_or_else(|| anyhow!("CDP Runtime.evaluate returned no result object"))?;
            if let Some(by_value) = remote.get("value") {
                return Ok(by_value.clone());
            }
            if remote.get("subtype").and_then(Value::as_str) == Some("null") {
                return Ok(Value::Null);
            }
            if let Some(description) = remote.get("description").and_then(Value::as_str) {
                return Ok(Value::String(description.to_string()));
            }
            return Ok(Value::Null);
        }
    }

    async fn wait_for_prompt(&self, handle: &PageHandle) -> Result<()> {
        let deadline = Instant::now() + PAGE_READY_TIMEOUT;
        sleep(Duration::from_millis(300)).await;
        let expression = r#"(() => !!document.querySelector('#prompt-textarea, [data-testid=\"composer-text-input\"], textarea[placeholder], [contenteditable=\"true\"]'))()"#;
        loop {
            if self
                .cdp_eval(&handle.target, &handle.page_id, expression)
                .await
                .ok()
                .and_then(|v| v.as_bool())
                == Some(true)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "ChatGPT prompt did not become ready on account '{}' instance '{}'",
                    handle.target.account_id,
                    handle.target.instance
                ));
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn cdp_verify(&self, target: &BrowserTarget, page_id: &str) -> Result<ChatgptPageProbe> {
        let expression = r#"(() => ({
  ready: !!(document.querySelector('#prompt-textarea') || document.querySelector('[contenteditable=\"true\"]')),
  generating: !!document.querySelector('button[data-testid=\"stop-button\"], button[aria-label=\"Stop generating\"], button[aria-label=\"Stop answering\"]'),
  url: location.href,
  title: document.title
}))()"#;
        let value = self.cdp_eval(target, page_id, expression).await?;
        validate_probe(&value)
    }

    async fn cdp_probe(&self, target: &BrowserTarget, page_id: &str) -> ChatgptUiCondition {
        let expression = r#"(() => {
  const MSG='[data-message-author-role], article, [data-testid^=\"conversation-turn\"], [data-message-id]';
  const SYS='[data-testid*=\"rate-limit\"], [data-testid*=\"modal\"], [role=\"alert\"], [role=\"dialog\"], [data-sonner-toast], [data-testid*=\"toast\"], [data-testid*=\"notification\"]';
  const visible=(el)=>{if(!el)return false;const s=getComputedStyle(el),r=el.getBoundingClientRect();return s.display!=='none'&&s.visibility!=='hidden'&&Number(s.opacity)!==0&&r.width>0&&r.height>0};
  const texts=Array.from(document.querySelectorAll(SYS)).filter(el=>visible(el)&&!el.closest(MSG)&&!el.querySelector(MSG)).map(el=>(el.innerText||el.textContent||'').trim().toLowerCase()).filter(Boolean);
  const composer=document.querySelector('#prompt-textarea, [data-testid=\"composer-text-input\"], textarea[placeholder]');
  const stop=document.querySelector('button[data-testid=\"stop-button\"], button[aria-label=\"Stop generating\"], button[aria-label=\"Stop answering\"]');
  const rate=(t)=>/too many (requests|messages)|rate limit|making requests too quickly/.test(t)?'too_many_requests':/(at|over) capacity|capacity limit/.test(t)?'capacity':/(model|gpt)[^.\n]{0,80}(usage )?limit/.test(t)?'model_quota':/usage limit|limit reached|reached the .*limit/.test(t)?'usage_limit':null;
  const reset=(t)=>{const m=t.match(/(?:try again|reset(?:s)?|available again|wait)[^0-9]{0,48}(\d+(?:\.\d+)?)\s*(seconds?|secs?|minutes?|mins?|hours?|hrs?|days?)/i);if(!m)return null;const n=Number(m[1]),u=m[2].toLowerCase(),k=u.startsWith('min')?60:(u.startsWith('hour')||u.startsWith('hr'))?3600:u.startsWith('day')?86400:1,s=Math.ceil(n*k);return Number.isSafeInteger(s)&&s>=1&&s<=2678400?s:null};
  let rr=null,rs=null; for(const t of texts){rr=rate(t);if(rr){rs=reset(t);break}}
  const auth=texts.some(t=>/\b(log in|login|sign in|authentication required|session expired|please authenticate)\b/.test(t));
  const delivery=texts.find(t=>/\b(something went wrong|error generating|network error|failed to send|message failed|unable to load conversation|delivery failed)\b/.test(t))||null;
  return {ready:!!composer&&visible(composer),generating:!!stop&&visible(stop),rate_limited:rr!==null,rate_limit_reason:rr,reset_after_seconds:rs,delivery_error:delivery!==null,delivery_recoverable:!!delivery&&/retry|try again|network|temporary/.test(delivery),authentication_required:auth};
})()"#;
        match self.cdp_eval(target, page_id, expression).await {
            Ok(value) => classify_ui(&value),
            Err(_) => ChatgptUiCondition::Unknown,
        }
    }

    async fn cdp_send(&self, target: &BrowserTarget, page_id: &str, prompt: &str) -> Result<()> {
        if prompt.trim().is_empty() {
            return Err(anyhow!("ChatGPT Web prompt cannot be empty"));
        }
        let deadline = Instant::now() + GENERATION_IDLE_TIMEOUT;
        loop {
            match self.cdp_verify(target, page_id).await {
                Ok(probe) if !probe.generating => break,
                Ok(_) => {}
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(_) => {}
            }
            if Instant::now() >= deadline {
                return Err(anyhow!("timeout waiting for ChatGPT generation to finish"));
            }
            sleep(Duration::from_millis(500)).await;
        }

        let prompt_json = serde_json::to_string(prompt)?;
        let insert = format!(
            r#"(() => {{
 const el=document.querySelector('#prompt-textarea')||document.querySelector('[contenteditable=\"true\"]');
 if(!el)return {{ok:false,error:'no_textbox'}};el.focus();document.execCommand('selectAll',false,null);document.execCommand('delete',false,null);document.execCommand('insertText',false,{prompt_json});el.dispatchEvent(new Event('input',{{bubbles:true}}));return {{ok:true}};
}})()"#
        );
        let inserted = self.cdp_eval(target, page_id, &insert).await?;
        if inserted.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!("unable to fill ChatGPT prompt box: {inserted}"));
        }
        sleep(Duration::from_millis(250)).await;
        let sent = self
            .cdp_eval(
                target,
                page_id,
                r#"(() => {const btn=document.querySelector('button[data-testid=\"send-button\"], button[aria-label=\"Send prompt\"], #composer-submit-button');if(!btn||btn.disabled)return {ok:false,error:'no_send_button'};btn.click();return {ok:true};})()"#,
            )
            .await?;
        if sent.get("ok").and_then(Value::as_bool) != Some(true) {
            return Err(anyhow!("unable to send ChatGPT prompt: {sent}"));
        }
        Ok(())
    }
}

fn chromium_launch_args(target: &BrowserTarget) -> Result<Vec<String>> {
    let profile = target.user_data_dir.as_deref().ok_or_else(|| {
        anyhow!(
            "account '{}' cannot launch an isolated browser without browser.user_data_dir",
            target.account_id
        )
    })?;
    let endpoint = target.cdp_endpoint.as_deref().ok_or_else(|| {
        anyhow!(
            "account '{}' cannot launch an isolated browser without browser.cdp_endpoint",
            target.account_id
        )
    })?;
    let url = Url::parse(endpoint).context("invalid configured CDP endpoint")?;
    if url.scheme() != "http" {
        return Err(anyhow!(
            "automatic Chromium startup requires an http loopback CDP endpoint"
        ));
    }
    let host = url.host_str().unwrap_or_default();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
    if !loopback {
        return Err(anyhow!(
            "automatic Chromium startup requires a loopback CDP host"
        ));
    }
    let port = url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("configured CDP endpoint has no port"))?;
    Ok(vec![
        format!("--user-data-dir={}", profile.display()),
        "--remote-debugging-address=127.0.0.1".into(),
        format!("--remote-debugging-port={port}"),
        "--no-first-run".into(),
        "--no-default-browser-check".into(),
        "about:blank".into(),
    ])
}

fn discover_chromium_executable() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("OMO_CHROMIUM_BIN") {
        let explicit = PathBuf::from(explicit);
        if explicit.is_file() {
            return Some(explicit);
        }
    }

    #[cfg(target_os = "macos")]
    const CANDIDATES: &[&str] = &[
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    ];
    #[cfg(target_os = "linux")]
    const CANDIDATES: &[&str] = &[
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/usr/bin/microsoft-edge",
    ];
    #[cfg(target_os = "windows")]
    const CANDIDATES: &[&str] = &[];
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    const CANDIDATES: &[&str] = &[];

    CANDIDATES
        .iter()
        .map(Path::new)
        .find(|candidate| candidate.is_file())
        .map(Path::to_path_buf)
}

#[derive(Debug, Deserialize)]
struct CdpTarget {
    id: String,
    #[serde(default)]
    url: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    web_socket_debugger_url: String,
}

fn cdp_url(endpoint: &str, path: &str) -> Result<Url> {
    let mut base = Url::parse(endpoint).context("invalid configured CDP endpoint")?;
    if !matches!(base.scheme(), "http" | "https") {
        return Err(anyhow!(
            "direct CDP browser backend requires an http(s) endpoint, got '{}'",
            base.scheme()
        ));
    }
    base.set_path(&format!("/{path}"));
    base.set_query(None);
    base.set_fragment(None);
    Ok(base)
}

fn validate_loopback_ws(value: &str) -> Result<()> {
    let url = Url::parse(value).context("invalid CDP websocket URL")?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(anyhow!("CDP target websocket must use ws or wss"));
    }
    let host = url.host_str().unwrap_or_default();
    let loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false);
    if !loopback {
        return Err(anyhow!("CDP target websocket must remain loopback-only"));
    }
    Ok(())
}

fn validate_page_id(page_id: &str) -> Result<()> {
    if page_id.is_empty()
        || page_id.len() > 256
        || !page_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(anyhow!("invalid browser page id"));
    }
    Ok(())
}

fn validate_probe(value: &Value) -> Result<ChatgptPageProbe> {
    let url = value
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ChatGPT page probe has no url"))?;
    let parsed = Url::parse(url)?;
    if parsed.scheme() != "https" || parsed.host_str() != Some("chatgpt.com") {
        return Err(anyhow!("browser page is not on https://chatgpt.com"));
    }
    let generating = value
        .get("generating")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let ready = value.get("ready").and_then(Value::as_bool).unwrap_or(false);
    if !ready && !generating {
        return Err(anyhow!("ChatGPT Web interface is not active on page"));
    }
    Ok(ChatgptPageProbe {
        url: url.to_string(),
        title: value
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        generating,
    })
}

fn classify_ui(value: &Value) -> ChatgptUiCondition {
    if value
        .get("authentication_required")
        .and_then(Value::as_bool)
        == Some(true)
    {
        return ChatgptUiCondition::AuthenticationRequired;
    }
    if value.get("rate_limited").and_then(Value::as_bool) == Some(true) {
        let reason = match value.get("rate_limit_reason").and_then(Value::as_str) {
            Some("usage_limit") => ChatgptRateLimitReason::UsageLimit,
            Some("too_many_requests") => ChatgptRateLimitReason::TooManyRequests,
            Some("capacity") => ChatgptRateLimitReason::Capacity,
            Some("model_quota") => ChatgptRateLimitReason::ModelQuota,
            _ => return ChatgptUiCondition::Unknown,
        };
        let reset_after_seconds = value
            .get("reset_after_seconds")
            .and_then(Value::as_u64)
            .filter(|seconds| (1..=31 * 24 * 60 * 60).contains(seconds));
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

pub fn browser_account_unavailable(error: impl Into<String>) -> BridgeError {
    BridgeError::Precondition(format!("BROWSER_ACCOUNT_UNAVAILABLE: {}", error.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{AccountLimits, BrowserInstanceConfig, CooldownConfig, RoutingConfig};

    use tempfile::tempdir;

    fn legacy() -> LegacyAccountConfig {
        LegacyAccountConfig {
            routing: RoutingConfig::default(),
            limits: AccountLimits::default(),
            cooldown: CooldownConfig::default(),
            browser: BrowserInstanceConfig::legacy("active"),
        }
    }

    fn pool_with_config(json: &str) -> (tempfile::TempDir, BrowserPool) {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        fs::write(bridge.join("accounts.json"), json).unwrap();
        let pool = BrowserPool::new(
            bridge,
            mount,
            legacy(),
            BrowserDriverConfig::with_driver(
                Some(BrowserDriverKind::Orca),
                Some(PathBuf::from("orca")),
                "active",
                None,
            ),
        );
        (root, pool)
    }

    #[tokio::test]
    async fn draining_account_is_unavailable_for_fresh_browser_work_but_binding_resolves() {
        let (_root, pool) = pool_with_config(
            r#"{"version":1,"accounts":[{"id":"a","enabled":true,"draining":true,"browser":{"driver":"orca","instance":"ia"}}]}"#,
        );
        assert!(pool
            .target_for_account("a", false)
            .await
            .unwrap_err()
            .to_string()
            .contains("draining for new work"));
        let binding = BrowserBinding::new("a", BrowserDriverKind::Orca, "ia", "page");
        assert!(pool.target_for_binding(&binding).await.is_ok());
    }

    #[tokio::test]
    async fn multi_account_creation_requires_endpoint_and_profile_isolation() {
        let (_root, pool) = pool_with_config(
            r#"{"version":1,"accounts":[
              {"id":"a","browser":{"instance":"ia"}},
              {"id":"b","browser":{"instance":"ib"}}
            ]}"#,
        );
        let target = pool.target_for_account("a", false).await.unwrap();
        let error = pool
            .validate_creation_isolation(&target)
            .unwrap_err()
            .to_string();
        assert!(error.contains("distinct browser.cdp_endpoint and browser.user_data_dir"));
    }

    #[tokio::test]
    async fn identical_page_ids_remain_bound_to_distinct_instances() {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let a_profile = bridge.join("browser-profiles/a");
        let b_profile = bridge.join("browser-profiles/b");
        let json = format!(
            r#"{{"version":1,"accounts":[
              {{"id":"a","browser":{{"driver":"orca","instance":"ia","user_data_dir":"{}","cdp_endpoint":"http://127.0.0.1:9223"}}}},
              {{"id":"b","browser":{{"driver":"orca","instance":"ib","user_data_dir":"{}","cdp_endpoint":"http://127.0.0.1:9224"}}}}
            ]}}"#,
            a_profile.display(),
            b_profile.display()
        );
        fs::write(bridge.join("accounts.json"), json).unwrap();
        let pool = BrowserPool::new(
            bridge,
            mount,
            legacy(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
        );
        let a = BrowserBinding::new("a", BrowserDriverKind::Orca, "ia", "same-page");
        let b = BrowserBinding::new("b", BrowserDriverKind::Orca, "ib", "same-page");
        let at = pool.target_for_binding(&a).await.unwrap();
        let bt = pool.target_for_binding(&b).await.unwrap();
        assert_eq!(at.cdp_endpoint.as_deref(), Some("http://127.0.0.1:9223"));
        assert_eq!(bt.cdp_endpoint.as_deref(), Some("http://127.0.0.1:9224"));
        assert_ne!(at.instance, bt.instance);
    }

    #[tokio::test]
    async fn binding_rejects_instance_drift() {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let profile = bridge.join("browser-profiles/a");
        fs::write(
            bridge.join("accounts.json"),
            format!(
                r#"{{"version":1,"accounts":[{{"id":"a","browser":{{"driver":"orca","instance":"new","user_data_dir":"{}","cdp_endpoint":"http://127.0.0.1:9223"}}}}]}}"#,
                profile.display()
            ),
        )
        .unwrap();
        let pool = BrowserPool::new(
            bridge,
            mount,
            legacy(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
        );
        let binding = BrowserBinding::new("a", BrowserDriverKind::Orca, "old", "page");
        assert!(pool
            .target_for_binding(&binding)
            .await
            .unwrap_err()
            .to_string()
            .contains("instance changed"));
    }

    #[test]
    fn profile_lock_serializes_bridge_owners() {
        let root = tempdir().unwrap();
        let profile = root.path().join("profile");
        fs::create_dir_all(&profile).unwrap();
        let pool = BrowserPool::new(
            root.path().join("bridge"),
            root.path().join("mount"),
            legacy(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
        );
        let target = BrowserTarget {
            account_id: "a".into(),
            instance: "ia".into(),
            driver: BrowserDriverKind::Orca,
            user_data_dir: Some(profile),
            cdp_endpoint: Some("http://127.0.0.1:9223".into()),
            worktree: "active".into(),
        };
        let first = pool.try_lock_profile(&target).unwrap().unwrap();
        assert!(pool.try_lock_profile(&target).is_err());
        drop(first);
        assert!(pool.try_lock_profile(&target).unwrap().is_some());
    }

    #[test]
    fn profile_lease_is_held_for_browser_pool_lifetime() {
        let root = tempdir().unwrap();
        let profile = root.path().join("profile");
        fs::create_dir_all(&profile).unwrap();
        let target = BrowserTarget {
            account_id: "a".into(),
            instance: "ia".into(),
            driver: BrowserDriverKind::Orca,
            user_data_dir: Some(profile),
            cdp_endpoint: Some("http://127.0.0.1:9223".into()),
            worktree: "active".into(),
        };
        let first = BrowserPool::new(
            root.path().join("bridge-a"),
            root.path().join("mount-a"),
            legacy(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
        );
        let second = BrowserPool::new(
            root.path().join("bridge-b"),
            root.path().join("mount-b"),
            legacy(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
        );
        first.ensure_profile_lease(&target).unwrap();
        assert!(second.ensure_profile_lease(&target).is_err());
        drop(first);
        second.ensure_profile_lease(&target).unwrap();
    }

    #[test]
    fn isolated_browser_launch_arguments_bind_exact_profile_and_loopback_port() {
        let root = tempdir().unwrap();
        let target = BrowserTarget {
            account_id: "a".into(),
            instance: "instance-a".into(),
            driver: BrowserDriverKind::Orca,
            user_data_dir: Some(root.path().join("profile-a")),
            cdp_endpoint: Some("http://127.0.0.1:19223".into()),
            worktree: "active".into(),
        };
        let args = chromium_launch_args(&target).unwrap();
        assert!(args
            .iter()
            .any(|arg| arg == "--remote-debugging-address=127.0.0.1"));
        assert!(args
            .iter()
            .any(|arg| arg == "--remote-debugging-port=19223"));
        assert!(args.iter().any(|arg| arg
            == &format!(
                "--user-data-dir={}",
                root.path().join("profile-a").display()
            )));
        assert!(args.iter().all(|arg| !arg.contains("cookie")));
    }

    #[test]
    fn cdp_websocket_must_be_loopback() {
        assert!(validate_loopback_ws("ws://127.0.0.1:9222/devtools/page/abc").is_ok());
        assert!(validate_loopback_ws("ws://[::1]:9222/devtools/page/abc").is_ok());
        assert!(validate_loopback_ws("ws://192.0.2.8:9222/devtools/page/abc").is_err());
    }

    #[test]
    fn cdp_websocket_timeouts_are_bounded() {
        assert!(CDP_WS_CONNECT_TIMEOUT <= Duration::from_secs(10));
        assert!(CDP_WS_RESPONSE_TIMEOUT <= Duration::from_secs(15));
        assert!(CDP_WS_CONNECT_TIMEOUT < PAGE_READY_TIMEOUT);
        assert!(CDP_WS_RESPONSE_TIMEOUT < PAGE_READY_TIMEOUT);
    }
}
