use crate::error::{BridgeError, Result};
use crate::orca::BrowserDriverKind;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use url::Url;

pub const ACCOUNTS_CONFIG_VERSION: u32 = 1;
pub const LEGACY_ACCOUNT_ID: &str = "default";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RoutingStrategy {
    #[default]
    RoundRobin,
    LeastLoaded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingConfig {
    pub strategy: RoutingStrategy,
    pub reservation_ttl_seconds: u64,
    pub selection_failure_backoff_seconds: u64,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            strategy: RoutingStrategy::RoundRobin,
            reservation_ttl_seconds: 120,
            selection_failure_backoff_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CooldownConfig {
    pub unknown_rate_limit_seconds: u64,
    pub delivery_failure_seconds: u64,
}

impl Default for CooldownConfig {
    fn default() -> Self {
        Self {
            unknown_rate_limit_seconds: 15 * 60,
            delivery_failure_seconds: 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccountLimits {
    pub window_seconds: u64,
    pub max_dispatches: usize,
    pub max_active_workers: usize,
}

impl Default for AccountLimits {
    fn default() -> Self {
        Self {
            window_seconds: 60 * 60,
            max_dispatches: 12,
            max_active_workers: 3,
        }
    }
}

impl AccountLimits {
    pub fn window_ms(&self) -> u64 {
        self.window_seconds.saturating_mul(1000)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PartialAccountLimits {
    pub window_seconds: Option<u64>,
    pub max_dispatches: Option<usize>,
    pub max_active_workers: Option<usize>,
}

impl PartialAccountLimits {
    fn resolve(&self, defaults: &AccountLimits) -> AccountLimits {
        AccountLimits {
            window_seconds: self.window_seconds.unwrap_or(defaults.window_seconds),
            max_dispatches: self.max_dispatches.unwrap_or(defaults.max_dispatches),
            max_active_workers: self
                .max_active_workers
                .unwrap_or(defaults.max_active_workers),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccountDefaults {
    pub limits: AccountLimits,
    pub cooldown: CooldownConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserInstanceConfig {
    #[serde(default)]
    pub driver: Option<BrowserDriverKind>,
    pub instance: String,
    #[serde(default)]
    pub user_data_dir: Option<PathBuf>,
    #[serde(default)]
    pub cdp_endpoint: Option<String>,
    #[serde(default = "default_worktree")]
    pub worktree: String,
}

impl BrowserInstanceConfig {
    pub fn legacy(worktree: impl Into<String>) -> Self {
        Self {
            driver: None,
            instance: "legacy".to_string(),
            user_data_dir: None,
            cdp_endpoint: None,
            worktree: worktree.into(),
        }
    }
}

fn default_worktree() -> String {
    "active".to_string()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountConfig {
    pub id: String,
    pub enabled: bool,
    pub draining: bool,
    pub limits: AccountLimits,
    pub browser: BrowserInstanceConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountsConfig {
    pub version: u32,
    pub routing: RoutingConfig,
    pub defaults: AccountDefaults,
    pub accounts: Vec<AccountConfig>,
    pub legacy_fallback: bool,
}

impl AccountsConfig {
    pub fn account(&self, id: &str) -> Option<&AccountConfig> {
        self.accounts.iter().find(|account| account.id == id)
    }

    pub fn legacy(legacy: LegacyAccountConfig) -> Self {
        Self {
            version: ACCOUNTS_CONFIG_VERSION,
            routing: legacy.routing,
            defaults: AccountDefaults {
                limits: legacy.limits.clone(),
                cooldown: legacy.cooldown,
            },
            accounts: vec![AccountConfig {
                id: LEGACY_ACCOUNT_ID.to_string(),
                enabled: true,
                draining: false,
                limits: legacy.limits,
                browser: legacy.browser,
            }],
            legacy_fallback: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyAccountConfig {
    pub routing: RoutingConfig,
    pub limits: AccountLimits,
    pub cooldown: CooldownConfig,
    pub browser: BrowserInstanceConfig,
}

impl Default for LegacyAccountConfig {
    fn default() -> Self {
        Self {
            routing: RoutingConfig::default(),
            limits: AccountLimits::default(),
            cooldown: CooldownConfig::default(),
            browser: BrowserInstanceConfig::legacy("active"),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountsFile {
    version: u32,
    #[serde(default)]
    routing: RoutingConfig,
    #[serde(default)]
    defaults: AccountDefaults,
    accounts: Vec<RawAccountConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAccountConfig {
    id: String,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default)]
    draining: bool,
    #[serde(default)]
    limits: PartialAccountLimits,
    browser: BrowserInstanceConfig,
}

fn default_enabled() -> bool {
    true
}

pub fn load_accounts_config(
    bridge_dir: &Path,
    mount_root: &Path,
    legacy: LegacyAccountConfig,
) -> Result<AccountsConfig> {
    let path = bridge_dir.join("accounts.json");
    match fs::read_to_string(&path) {
        Ok(content) => parse_accounts_config(&content, bridge_dir, mount_root),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            validate_legacy(&legacy)?;
            Ok(AccountsConfig::legacy(legacy))
        }
        Err(error) => Err(BridgeError::Io(error)),
    }
}

pub fn parse_accounts_config(
    content: &str,
    bridge_dir: &Path,
    mount_root: &Path,
) -> Result<AccountsConfig> {
    let raw: AccountsFile = serde_json::from_str(content)?;
    if raw.version != ACCOUNTS_CONFIG_VERSION {
        return Err(config_error(format!(
            "unsupported accounts.json version {}; expected {}",
            raw.version, ACCOUNTS_CONFIG_VERSION
        )));
    }
    validate_routing(&raw.routing)?;
    validate_limits("defaults.limits", &raw.defaults.limits)?;
    validate_cooldown(&raw.defaults.cooldown)?;
    if raw.accounts.is_empty() {
        return Err(config_error(
            "accounts.json must contain at least one account",
        ));
    }

    let mut ids = HashSet::new();
    let mut instances = HashSet::new();
    let mut profile_dirs = HashSet::new();
    let mut endpoints = HashSet::new();
    let mut accounts = Vec::with_capacity(raw.accounts.len());

    for raw_account in raw.accounts {
        validate_account_id(&raw_account.id)?;
        if !ids.insert(raw_account.id.clone()) {
            return Err(config_error(format!(
                "duplicate account id '{}'",
                raw_account.id
            )));
        }

        let limits = raw_account.limits.resolve(&raw.defaults.limits);
        validate_limits(&format!("accounts[{}].limits", raw_account.id), &limits)?;
        validate_browser(
            &raw_account.id,
            raw_account.enabled || raw_account.draining,
            &raw_account.browser,
            bridge_dir,
            mount_root,
            &mut instances,
            &mut profile_dirs,
            &mut endpoints,
        )?;
        accounts.push(AccountConfig {
            id: raw_account.id,
            enabled: raw_account.enabled,
            draining: raw_account.draining,
            limits,
            browser: raw_account.browser,
        });
    }

    Ok(AccountsConfig {
        version: raw.version,
        routing: raw.routing,
        defaults: raw.defaults,
        accounts,
        legacy_fallback: false,
    })
}

fn validate_legacy(legacy: &LegacyAccountConfig) -> Result<()> {
    validate_routing(&legacy.routing)?;
    validate_limits("legacy limits", &legacy.limits)?;
    validate_cooldown(&legacy.cooldown)?;
    if legacy.browser.instance.trim().is_empty() || legacy.browser.worktree.trim().is_empty() {
        return Err(config_error(
            "legacy browser instance and worktree must be non-empty",
        ));
    }
    Ok(())
}

fn validate_routing(routing: &RoutingConfig) -> Result<()> {
    if routing.reservation_ttl_seconds == 0 {
        return Err(config_error(
            "routing.reservation_ttl_seconds must be greater than zero",
        ));
    }
    if routing.selection_failure_backoff_seconds == 0 {
        return Err(config_error(
            "routing.selection_failure_backoff_seconds must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_cooldown(cooldown: &CooldownConfig) -> Result<()> {
    if cooldown.unknown_rate_limit_seconds == 0 {
        return Err(config_error(
            "defaults.cooldown.unknown_rate_limit_seconds must be greater than zero",
        ));
    }
    if cooldown.delivery_failure_seconds == 0 {
        return Err(config_error(
            "defaults.cooldown.delivery_failure_seconds must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_limits(label: &str, limits: &AccountLimits) -> Result<()> {
    if limits.window_seconds == 0 {
        return Err(config_error(format!(
            "{label}.window_seconds must be greater than zero"
        )));
    }
    if limits.max_dispatches == 0 {
        return Err(config_error(format!(
            "{label}.max_dispatches must be greater than zero"
        )));
    }
    if limits.max_active_workers == 0 {
        return Err(config_error(format!(
            "{label}.max_active_workers must be greater than zero"
        )));
    }
    Ok(())
}

fn validate_account_id(id: &str) -> Result<()> {
    let valid = !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid {
        return Err(config_error(format!(
            "invalid account id '{id}'; use 1-128 ASCII letters, digits, '.', '_' or '-'"
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_browser(
    account_id: &str,
    enabled: bool,
    browser: &BrowserInstanceConfig,
    bridge_dir: &Path,
    mount_root: &Path,
    instances: &mut HashSet<String>,
    profile_dirs: &mut HashSet<PathBuf>,
    endpoints: &mut HashSet<String>,
) -> Result<()> {
    let instance = browser.instance.trim();
    if instance.is_empty() {
        return Err(config_error(format!(
            "account '{account_id}' browser.instance must be non-empty"
        )));
    }
    if browser.worktree.trim().is_empty() {
        return Err(config_error(format!(
            "account '{account_id}' browser.worktree must be non-empty"
        )));
    }
    if enabled && !instances.insert(instance.to_string()) {
        return Err(config_error(format!(
            "account '{account_id}' reuses browser.instance '{instance}'"
        )));
    }

    if let Some(profile) = browser.user_data_dir.as_deref() {
        let normalized = validate_profile_path(account_id, profile, bridge_dir, mount_root)?;
        if enabled && !profile_dirs.insert(normalized) {
            return Err(config_error(format!(
                "account '{account_id}' reuses browser.user_data_dir"
            )));
        }
    }

    if let Some(endpoint) = browser.cdp_endpoint.as_deref() {
        let normalized = validate_loopback_endpoint(account_id, endpoint)?;
        if enabled && !endpoints.insert(normalized) {
            return Err(config_error(format!(
                "account '{account_id}' reuses browser.cdp_endpoint"
            )));
        }
    }
    Ok(())
}

fn validate_profile_path(
    account_id: &str,
    profile: &Path,
    bridge_dir: &Path,
    mount_root: &Path,
) -> Result<PathBuf> {
    let profile = normalize_absolute(profile).map_err(|message| {
        config_error(format!(
            "account '{account_id}' browser.user_data_dir {message}"
        ))
    })?;
    let bridge = normalize_absolute(bridge_dir)
        .map_err(|message| config_error(format!("bridge control directory {message}")))?;
    if !profile.starts_with(&bridge) {
        return Err(config_error(format!(
            "account '{account_id}' browser.user_data_dir must be inside bridge control directory {}",
            bridge.display()
        )));
    }

    let bridge_real = canonicalize_existing_ancestor(&bridge)?;
    let profile_real_ancestor = canonicalize_existing_ancestor(&profile)?;
    if !profile_real_ancestor.starts_with(&bridge_real) {
        return Err(config_error(format!(
            "account '{account_id}' browser.user_data_dir resolves through a symlink outside bridge control directory"
        )));
    }

    let mount = if mount_root.is_absolute() {
        normalize_absolute(mount_root).map_err(config_error)?
    } else {
        dunce::canonicalize(mount_root)
            .map_err(|error| config_error(format!("failed to canonicalize mount root: {error}")))?
    };
    if profile.starts_with(&mount) || mount.starts_with(&profile) {
        return Err(config_error(format!(
            "account '{account_id}' browser.user_data_dir must be outside delegated mount root {}",
            mount.display()
        )));
    }

    if let Ok(mount_real) = dunce::canonicalize(&mount) {
        if profile_real_ancestor.starts_with(&mount_real)
            || mount_real.starts_with(&profile_real_ancestor)
        {
            return Err(config_error(format!(
                "account '{account_id}' browser.user_data_dir resolves inside delegated mount root"
            )));
        }
    }
    Ok(profile)
}

fn normalize_absolute(path: &Path) -> std::result::Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err(format!("must be an absolute path: {}", path.display()));
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(Path::new(std::path::MAIN_SEPARATOR_STR)),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(format!(
                    "must not contain '..' path traversal: {}",
                    path.display()
                ));
            }
            Component::Normal(value) => normalized.push(value),
        }
    }
    Ok(normalized)
}

fn canonicalize_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut ancestor = path;
    while !ancestor.exists() {
        ancestor = ancestor.parent().ok_or_else(|| {
            config_error(format!("path has no existing ancestor: {}", path.display()))
        })?;
    }
    dunce::canonicalize(ancestor).map_err(|error| {
        config_error(format!(
            "failed to canonicalize path ancestor {}: {error}",
            ancestor.display()
        ))
    })
}

fn validate_loopback_endpoint(account_id: &str, endpoint: &str) -> Result<String> {
    let parsed = Url::parse(endpoint).map_err(|error| {
        config_error(format!(
            "account '{account_id}' browser.cdp_endpoint is invalid: {error}"
        ))
    })?;
    if !matches!(parsed.scheme(), "http" | "https" | "ws" | "wss") {
        return Err(config_error(format!(
            "account '{account_id}' browser.cdp_endpoint must use http, https, ws, or wss"
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(config_error(format!(
            "account '{account_id}' browser.cdp_endpoint must not contain credentials"
        )));
    }
    let Some(host) = parsed.host_str() else {
        return Err(config_error(format!(
            "account '{account_id}' browser.cdp_endpoint must contain a host"
        )));
    };
    let ip_host = host
        .trim_start_matches(char::from(91u8))
        .trim_end_matches(char::from(93u8));
    let loopback = host.eq_ignore_ascii_case("localhost")
        || ip_host
            .parse::<IpAddr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false);
    if !loopback {
        return Err(config_error(format!(
            "account '{account_id}' browser.cdp_endpoint must be loopback-only"
        )));
    }
    Ok(parsed.to_string())
}

fn config_error(message: impl Into<String>) -> BridgeError {
    BridgeError::Precondition(format!("accounts configuration: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn roots() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempdir().unwrap();
        let bridge = root.path().join("control/bridge");
        let mount = root.path().join("workspaces");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        (root, bridge, mount)
    }

    fn profile(bridge: &Path, id: &str) -> String {
        bridge
            .join("browser-profiles")
            .join(id)
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn missing_accounts_file_synthesizes_legacy_default() {
        let (_root, bridge, mount) = roots();
        let legacy = LegacyAccountConfig {
            limits: AccountLimits {
                window_seconds: 120,
                max_dispatches: 7,
                max_active_workers: 2,
            },
            browser: BrowserInstanceConfig::legacy("legacy-worktree"),
            ..LegacyAccountConfig::default()
        };
        let config = load_accounts_config(&bridge, &mount, legacy).unwrap();
        assert!(config.legacy_fallback);
        assert_eq!(config.accounts.len(), 1);
        assert_eq!(config.accounts[0].id, LEGACY_ACCOUNT_ID);
        assert_eq!(config.accounts[0].limits.max_dispatches, 7);
        assert_eq!(config.accounts[0].browser.worktree, "legacy-worktree");
    }

    #[test]
    fn parses_inheritance_and_account_limit_overrides() {
        let (_root, bridge, mount) = roots();
        let json = format!(
            r#"{{
              "version": 1,
              "routing": {{"strategy":"least_loaded","reservation_ttl_seconds":45,"selection_failure_backoff_seconds":5}},
              "defaults": {{
                "limits": {{"window_seconds":600,"max_dispatches":10,"max_active_workers":4}},
                "cooldown": {{"unknown_rate_limit_seconds":90,"delivery_failure_seconds":11}}
              }},
              "accounts": [
                {{"id":"web-a","enabled":true,"limits":{{"max_dispatches":3}},"browser":{{"driver":"orca","instance":"a","user_data_dir":"{}","cdp_endpoint":"http://127.0.0.1:9223","worktree":"active"}}}},
                {{"id":"web-b","browser":{{"driver":"orca","instance":"b","user_data_dir":"{}","cdp_endpoint":"http://[::1]:9224"}}}}
              ]
            }}"#,
            profile(&bridge, "a"),
            profile(&bridge, "b")
        );
        let config = parse_accounts_config(&json, &bridge, &mount).unwrap();
        assert!(!config.legacy_fallback);
        assert_eq!(config.routing.strategy, RoutingStrategy::LeastLoaded);
        assert_eq!(config.accounts[0].limits.max_dispatches, 3);
        assert_eq!(config.accounts[0].limits.window_seconds, 600);
        assert_eq!(config.accounts[1].limits.max_dispatches, 10);
        assert_eq!(config.accounts[1].browser.worktree, "active");
    }

    #[test]
    fn parses_explicit_draining_state_without_disabling_retained_configuration() {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let profile = bridge.join("browser-profiles/a");
        let config = parse_accounts_config(
            &format!(
                r#"{{"version":1,"accounts":[{{"id":"a","enabled":true,"draining":true,"browser":{{"instance":"a","user_data_dir":"{}","cdp_endpoint":"http://127.0.0.1:9223"}}}}]}}"#,
                profile.display()
            ),
            &bridge,
            &mount,
        ).unwrap();
        assert!(config.accounts[0].enabled);
        assert!(config.accounts[0].draining);
    }

    #[test]
    fn rejects_duplicate_ids_and_zero_limits() {
        let (_root, bridge, mount) = roots();
        let duplicate = r#"{
          "version":1,
          "accounts":[
            {"id":"a","browser":{"instance":"one"}},
            {"id":"a","browser":{"instance":"two"}}
          ]
        }"#;
        assert!(parse_accounts_config(duplicate, &bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("duplicate account id"));

        let zero = r#"{
          "version":1,
          "defaults":{"limits":{"window_seconds":3600,"max_dispatches":0,"max_active_workers":1}},
          "accounts":[{"id":"a","browser":{"instance":"one"}}]
        }"#;
        assert!(parse_accounts_config(zero, &bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("max_dispatches"));
    }

    #[test]
    fn rejects_non_loopback_and_duplicate_enabled_browser_targets() {
        let (_root, bridge, mount) = roots();
        let remote = r#"{
          "version":1,
          "accounts":[{"id":"a","browser":{"instance":"one","cdp_endpoint":"http://192.0.2.1:9222"}}]
        }"#;
        assert!(parse_accounts_config(remote, &bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("loopback-only"));

        let duplicate = r#"{
          "version":1,
          "accounts":[
            {"id":"a","browser":{"instance":"same","cdp_endpoint":"http://127.0.0.1:9222"}},
            {"id":"b","browser":{"instance":"same","cdp_endpoint":"http://127.0.0.1:9222"}}
          ]
        }"#;
        assert!(parse_accounts_config(duplicate, &bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("reuses browser.instance"));
    }

    #[test]
    fn rejects_profile_outside_control_dir_or_inside_mount() {
        let (root, bridge, mount) = roots();
        let outside = root.path().join("outside-profile");
        let outside_json = format!(
            r#"{{"version":1,"accounts":[{{"id":"a","browser":{{"instance":"a","user_data_dir":"{}"}}}}]}}"#,
            outside.display()
        );
        assert!(parse_accounts_config(&outside_json, &bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("inside bridge control directory"));

        let nested_bridge = mount.join("bridge");
        fs::create_dir_all(&nested_bridge).unwrap();
        let nested_profile = nested_bridge.join("profiles/a");
        let nested_json = format!(
            r#"{{"version":1,"accounts":[{{"id":"a","browser":{{"instance":"a","user_data_dir":"{}"}}}}]}}"#,
            nested_profile.display()
        );
        assert!(parse_accounts_config(&nested_json, &nested_bridge, &mount)
            .unwrap_err()
            .to_string()
            .contains("outside delegated mount root"));
    }
}
