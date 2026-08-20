use crate::account_state::AccountStateStore;
use crate::accounts::{
    load_accounts_config_from_path, parse_accounts_config, AccountDefaults, AccountsConfig,
    LegacyAccountConfig, RoutingConfig, LEGACY_ACCOUNT_ID,
};
use crate::browser_pool::{BrowserHealth, BrowserLoginState, BrowserPool};
use crate::error::{BridgeError, Result};
use crate::security::WorkspaceMux;
use crate::tools::task_state::load_delegation_lifecycle;
use serde::Serialize;
use serde_json::json;
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub const PENDING_ACCOUNTS_FILE: &str = "accounts.pending.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OnboardingAccount {
    pub id: String,
    pub instance: String,
    pub user_data_dir: PathBuf,
    pub cdp_endpoint: String,
    pub worktree: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyScopeBlockerKind {
    Active,
    Retained,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LegacyScopeBlocker {
    pub scope_id: String,
    pub kind: LegacyScopeBlockerKind,
}

pub fn pending_accounts_path(bridge_dir: &Path) -> PathBuf {
    bridge_dir.join(PENDING_ACCOUNTS_FILE)
}

pub fn prepare_pending_accounts_config(
    bridge_dir: &Path,
    mount_root: &Path,
    account_ids: &[String],
    cdp_start_port: u16,
    worktree: &str,
    replace_pending: bool,
) -> Result<AccountsConfig> {
    if account_ids.is_empty() {
        return Err(BridgeError::Precondition(
            "at least one --account is required".into(),
        ));
    }
    if bridge_dir.join("accounts.json").exists() {
        return Err(BridgeError::Precondition(
            "accounts.json already exists; onboarding only creates an initial configuration".into(),
        ));
    }

    let pending = pending_accounts_path(bridge_dir);
    if pending.exists() && !replace_pending {
        return Err(BridgeError::Precondition(format!(
            "pending onboarding already exists at {}; inspect it or pass --replace-pending",
            pending.display()
        )));
    }

    let profiles = bridge_dir.join("browser-profiles");
    let accounts = account_ids
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let offset = u16::try_from(index).map_err(|_| {
                BridgeError::Precondition("too many accounts for CDP port assignment".into())
            })?;
            let port = cdp_start_port
                .checked_add(offset)
                .ok_or_else(|| BridgeError::Precondition("CDP port range overflows u16".into()))?;
            Ok(OnboardingAccount {
                id: id.clone(),
                instance: format!("chatgpt-{id}"),
                user_data_dir: profiles.join(id),
                cdp_endpoint: format!("http://127.0.0.1:{port}"),
                worktree: worktree.to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let candidate = json!({
        "version": 1,
        "routing": RoutingConfig::default(),
        "defaults": AccountDefaults::default(),
        "accounts": accounts.iter().map(|account| json!({
            "id": account.id,
            "enabled": true,
            "draining": false,
            "browser": {
                "instance": account.instance,
                "user_data_dir": account.user_data_dir,
                "cdp_endpoint": account.cdp_endpoint,
                "worktree": account.worktree,
            }
        })).collect::<Vec<_>>(),
    });
    let content = serde_json::to_string_pretty(&candidate)?;
    let config = parse_accounts_config(&content, bridge_dir, mount_root)?;
    write_private_atomic(&pending, content.as_bytes())?;
    Ok(config)
}

pub fn load_pending_accounts_config(
    bridge_dir: &Path,
    mount_root: &Path,
    legacy: LegacyAccountConfig,
) -> Result<AccountsConfig> {
    let path = pending_accounts_path(bridge_dir);
    if !path.exists() {
        return Err(BridgeError::Precondition(format!(
            "no pending multi-account onboarding exists at {}",
            path.display()
        )));
    }
    load_accounts_config_from_path(&path, bridge_dir, mount_root, legacy)
}

pub fn activation_blocking_scope_ids(mux: &WorkspaceMux) -> Result<Vec<String>> {
    Ok(legacy_scope_blockers(mux)?
        .into_iter()
        .map(|blocker| blocker.scope_id)
        .collect())
}

pub fn legacy_scope_blockers(mux: &WorkspaceMux) -> Result<Vec<LegacyScopeBlocker>> {
    let mut blockers = Vec::new();
    for scope in mux.list_scopes_strict()? {
        if scope.account_id() != LEGACY_ACCOUNT_ID {
            continue;
        }
        let workspace = mux.resolve(&scope.scope_id)?;
        let blocker = match load_delegation_lifecycle(&workspace, &scope.scope_id) {
            Ok(Some(lifecycle)) if lifecycle.session_retained => {
                Some(LegacyScopeBlockerKind::Retained)
            }
            Ok(Some(lifecycle)) if lifecycle.terminal_state.is_none() => {
                Some(LegacyScopeBlockerKind::Active)
            }
            Ok(Some(_)) => None,
            Ok(None) if scope.page_id().is_some() => Some(LegacyScopeBlockerKind::Unknown),
            Ok(None) => None,
            Err(_) => Some(LegacyScopeBlockerKind::Unknown),
        };
        if let Some(kind) = blocker {
            blockers.push(LegacyScopeBlocker {
                scope_id: scope.scope_id,
                kind,
            });
        }
    }
    Ok(blockers)
}

pub async fn pending_account_health(
    config: &AccountsConfig,
    browsers: &BrowserPool,
) -> Vec<BrowserHealth> {
    let mut health = Vec::with_capacity(config.accounts.len());
    for account in &config.accounts {
        health.push(browsers.health(&account.id).await);
    }
    health
}

pub fn activate_pending_accounts_config(
    bridge_dir: &Path,
    mount_root: &Path,
    mux: &WorkspaceMux,
    legacy: LegacyAccountConfig,
    health: &[BrowserHealth],
) -> Result<AccountsConfig> {
    let store = AccountStateStore::new(bridge_dir);
    let _activation_lock = store.lock_account_activation()?;
    let config = load_pending_accounts_config(bridge_dir, mount_root, legacy)?;
    let ready_accounts = health
        .iter()
        .filter(|health| health.login_state == BrowserLoginState::Ready)
        .map(|health| (&health.account_id, &health.instance))
        .collect::<Vec<_>>();
    let expected_accounts = config
        .accounts
        .iter()
        .map(|account| (&account.id, &account.browser.instance))
        .collect::<Vec<_>>();
    if ready_accounts != expected_accounts {
        return Err(BridgeError::Precondition(format!(
            "cannot activate multi-account routing until every pending account is logged in; ready accounts: {}",
            ready_accounts
                .iter()
                .map(|(account_id, _)| account_id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let blocking_scopes = legacy_scope_blockers(mux)?;
    if !blocking_scopes.is_empty() {
        let details = blocking_scopes
            .iter()
            .map(|blocker| format!("{} ({:?})", blocker.scope_id, blocker.kind))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(BridgeError::Precondition(format!(
            "cannot activate multi-account routing while legacy scopes remain: {details}"
        )));
    }
    if !store
        .load_account(LEGACY_ACCOUNT_ID)?
        .reservations
        .is_empty()
    {
        return Err(BridgeError::Precondition(
            "cannot activate multi-account routing while a legacy account reservation remains"
                .into(),
        ));
    }

    let live = bridge_dir.join("accounts.json");
    if live.exists() {
        return Err(BridgeError::Precondition(
            "accounts.json already exists; refusing to overwrite active account routing".into(),
        ));
    }
    let pending = pending_accounts_path(bridge_dir);
    fs::rename(&pending, &live)?;
    #[cfg(unix)]
    fs::set_permissions(&live, fs::Permissions::from_mode(0o600))?;
    Ok(config)
}

fn write_private_atomic(path: &Path, content: &[u8]) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BridgeError::Precondition(format!("onboarding path has no parent: {}", path.display()))
    })?;
    fs::create_dir_all(parent)?;
    #[cfg(unix)]
    fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
    let temp = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("accounts.pending.json"),
        uuid::Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    use std::io::Write;
    let mut file = options.open(&temp)?;
    file.write_all(content)?;
    file.sync_all()?;
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(BridgeError::Io(error));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orca::{BrowserDriverConfig, BrowserDriverKind};
    use crate::security::WorkspaceMux;
    use crate::tools::task_state::{
        record_terminal_evidence, retain_session_with_lease, start_fresh_delegation_lifecycle,
        DelegationTerminalState,
    };
    use tempfile::tempdir;

    fn roots() -> (tempfile::TempDir, PathBuf, PathBuf, WorkspaceMux) {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        let scopes = root.path().join("scopes");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let mux = WorkspaceMux::new(&mount, &scopes).unwrap();
        (root, bridge, mount, mux)
    }

    #[test]
    fn prepare_creates_valid_pending_config_without_live_routing() {
        let (_root, bridge, mount, _mux) = roots();
        let config = prepare_pending_accounts_config(
            &bridge,
            &mount,
            &["primary".into(), "secondary".into()],
            9223,
            "active",
            false,
        )
        .unwrap();

        assert_eq!(config.accounts.len(), 2);
        assert!(config
            .accounts
            .iter()
            .all(|account| account.browser.driver.is_none()));
        assert!(!bridge.join("accounts.json").exists());
        assert!(pending_accounts_path(&bridge).exists());
        assert_eq!(
            config.accounts[0].browser.cdp_endpoint.as_deref(),
            Some("http://127.0.0.1:9223")
        );
        assert_eq!(
            config.accounts[1].browser.cdp_endpoint.as_deref(),
            Some("http://127.0.0.1:9224")
        );
    }

    #[test]
    fn activation_refuses_legacy_scopes_before_replacing_routing() {
        let (_root, bridge, mount, mux) = roots();
        prepare_pending_accounts_config(
            &bridge,
            &mount,
            &["primary".into()],
            9223,
            "active",
            false,
        )
        .unwrap();
        let project = mount.join("project");
        fs::create_dir_all(&project).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                crate::BrowserBinding::new(
                    LEGACY_ACCOUNT_ID,
                    BrowserDriverKind::Orca,
                    "legacy",
                    "page-1",
                ),
            )
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&workspace, &scope.scope_id, 60_000).unwrap();
        let health = vec![BrowserHealth {
            account_id: "primary".into(),
            instance: "chatgpt-primary".into(),
            reachability: crate::BrowserReachability::Reachable,
            login_state: BrowserLoginState::Ready,
            login_required: false,
            detail: None,
        }];

        assert!(activate_pending_accounts_config(
            &bridge,
            &mount,
            &mux,
            LegacyAccountConfig::default(),
            &health,
        )
        .unwrap_err()
        .to_string()
        .contains("legacy scopes remain"));
        assert!(pending_accounts_path(&bridge).exists());
        assert!(!bridge.join("accounts.json").exists());
    }

    #[test]
    fn activation_promotes_ready_pending_config() {
        let (_root, bridge, mount, mux) = roots();
        prepare_pending_accounts_config(
            &bridge,
            &mount,
            &["primary".into()],
            9223,
            "active",
            false,
        )
        .unwrap();
        let health = vec![BrowserHealth {
            account_id: "primary".into(),
            instance: "chatgpt-primary".into(),
            reachability: crate::BrowserReachability::Reachable,
            login_state: BrowserLoginState::Ready,
            login_required: false,
            detail: None,
        }];

        let config = activate_pending_accounts_config(
            &bridge,
            &mount,
            &mux,
            LegacyAccountConfig::default(),
            &health,
        )
        .unwrap();
        assert_eq!(config.accounts[0].id, "primary");
        assert!(bridge.join("accounts.json").exists());
        assert!(!pending_accounts_path(&bridge).exists());
    }

    #[test]
    fn activation_rejects_ready_health_for_a_different_browser_instance() {
        let (_root, bridge, mount, mux) = roots();
        prepare_pending_accounts_config(
            &bridge,
            &mount,
            &["primary".into()],
            9223,
            "active",
            false,
        )
        .unwrap();
        let health = vec![BrowserHealth {
            account_id: "primary".into(),
            instance: "different-instance".into(),
            reachability: crate::BrowserReachability::Reachable,
            login_state: BrowserLoginState::Ready,
            login_required: false,
            detail: None,
        }];

        assert!(activate_pending_accounts_config(
            &bridge,
            &mount,
            &mux,
            LegacyAccountConfig::default(),
            &health,
        )
        .unwrap_err()
        .to_string()
        .contains("every pending account is logged in"));
        assert!(pending_accounts_path(&bridge).exists());
        assert!(!bridge.join("accounts.json").exists());
    }

    #[test]
    fn terminal_non_retained_legacy_scope_does_not_block_activation() {
        let (_root, _bridge, mount, mux) = roots();
        let project = mount.join("project");
        fs::create_dir_all(&project).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                crate::BrowserBinding::new(
                    LEGACY_ACCOUNT_ID,
                    BrowserDriverKind::Orca,
                    "legacy",
                    "page-1",
                ),
            )
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        assert!(legacy_scope_blockers(&mux).unwrap().is_empty());
    }

    #[test]
    fn active_legacy_scope_blocks_activation() {
        let (_root, _bridge, mount, mux) = roots();
        let project = mount.join("project");
        fs::create_dir_all(&project).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                crate::BrowserBinding::new(
                    LEGACY_ACCOUNT_ID,
                    BrowserDriverKind::Orca,
                    "legacy",
                    "page-1",
                ),
            )
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();

        assert_eq!(
            legacy_scope_blockers(&mux).unwrap(),
            vec![LegacyScopeBlocker {
                scope_id: scope.scope_id,
                kind: LegacyScopeBlockerKind::Active,
            }]
        );
    }

    #[test]
    fn browser_bound_legacy_scope_without_lifecycle_blocks_activation() {
        let (_root, _bridge, mount, mux) = roots();
        let project = mount.join("project");
        fs::create_dir_all(&project).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                crate::BrowserBinding::new(
                    LEGACY_ACCOUNT_ID,
                    BrowserDriverKind::Orca,
                    "legacy",
                    "page-1",
                ),
            )
            .unwrap();

        assert_eq!(
            legacy_scope_blockers(&mux).unwrap(),
            vec![LegacyScopeBlocker {
                scope_id: scope.scope_id,
                kind: LegacyScopeBlockerKind::Unknown,
            }]
        );
    }

    #[test]
    fn pending_pool_uses_its_staged_configuration() {
        let (_root, bridge, mount, _mux) = roots();
        prepare_pending_accounts_config(
            &bridge,
            &mount,
            &["primary".into()],
            9223,
            "active",
            false,
        )
        .unwrap();
        let pool = BrowserPool::with_config_path(
            &bridge,
            &mount,
            LegacyAccountConfig::default(),
            BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Cmux), None, "active", None),
            pending_accounts_path(&bridge),
        );
        assert_eq!(pool.load_config().unwrap().accounts[0].id, "primary");
    }
}
