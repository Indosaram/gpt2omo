use crate::error::{BridgeError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

pub const ACCOUNT_STATE_VERSION: u32 = 1;
pub const ROUTER_STATE_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountHealth {
    #[default]
    Ready,
    AuthRequired,
    Degraded,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountReservation {
    pub reservation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    pub created_ms: u64,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRuntimeState {
    pub version: u32,
    pub account_id: String,
    #[serde(default)]
    pub dispatches_ms: Vec<u64>,
    #[serde(default)]
    pub reservations: Vec<AccountReservation>,
    #[serde(default)]
    pub cooldown_until_ms: Option<u64>,
    #[serde(default)]
    pub cooldown_reason: Option<String>,
    #[serde(default)]
    pub health: AccountHealth,
    #[serde(default)]
    pub last_selected_ms: Option<u64>,
    #[serde(default)]
    pub last_success_ms: Option<u64>,
    #[serde(default)]
    pub consecutive_browser_failures: u32,
}

impl AccountRuntimeState {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            version: ACCOUNT_STATE_VERSION,
            account_id: account_id.into(),
            dispatches_ms: Vec::new(),
            reservations: Vec::new(),
            cooldown_until_ms: None,
            cooldown_reason: None,
            health: AccountHealth::Ready,
            last_selected_ms: None,
            last_success_ms: None,
            consecutive_browser_failures: 0,
        }
    }

    pub fn reconcile(&mut self, now_ms: u64, window_ms: u64) {
        self.dispatches_ms.retain(|timestamp| {
            now_ms.saturating_sub(*timestamp) < window_ms || *timestamp > now_ms
        });
        self.reservations
            .retain(|reservation| reservation.expires_ms > now_ms);
        if self.cooldown_until_ms.is_some_and(|until| until <= now_ms) {
            self.cooldown_until_ms = None;
            self.cooldown_reason = None;
        }
    }

    pub fn window_used(&self) -> usize {
        self.dispatches_ms.len()
    }

    pub fn reserved(&self) -> usize {
        self.reservations.len()
    }

    pub fn retry_after_window_ms(&self, window_ms: u64) -> Option<u64> {
        self.dispatches_ms
            .iter()
            .min()
            .map(|oldest| oldest.saturating_add(window_ms))
    }

    pub fn remove_reservation(&mut self, reservation_id: &str) -> Option<AccountReservation> {
        let index = self
            .reservations
            .iter()
            .position(|reservation| reservation.reservation_id == reservation_id)?;
        Some(self.reservations.remove(index))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouterRuntimeState {
    pub version: u32,
    #[serde(default)]
    pub round_robin_cursor: u64,
    pub updated_ms: u64,
}

impl Default for RouterRuntimeState {
    fn default() -> Self {
        Self {
            version: ROUTER_STATE_VERSION,
            round_robin_cursor: 0,
            updated_ms: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AccountStateStore {
    bridge_dir: PathBuf,
}

impl AccountStateStore {
    pub fn new(bridge_dir: impl AsRef<Path>) -> Self {
        Self {
            bridge_dir: bridge_dir.as_ref().to_path_buf(),
        }
    }

    pub fn bridge_dir(&self) -> &Path {
        &self.bridge_dir
    }

    pub fn lock(&self) -> Result<RouterLock> {
        prepare_private_dir(&self.bridge_dir)?;
        let lock_dir = self.bridge_dir.join("locks");
        prepare_private_dir(&lock_dir)?;
        let lock_path = lock_dir.join("router.lock");
        let file = open_private_file(&lock_path)?;
        file.lock().map_err(BridgeError::Io)?;
        Ok(RouterLock { file })
    }

    pub fn try_lock(&self) -> Result<Option<RouterLock>> {
        prepare_private_dir(&self.bridge_dir)?;
        let lock_dir = self.bridge_dir.join("locks");
        prepare_private_dir(&lock_dir)?;
        let lock_path = lock_dir.join("router.lock");
        let file = open_private_file(&lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(RouterLock { file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(BridgeError::Io(error)),
        }
    }

    pub fn load_account(&self, account_id: &str) -> Result<AccountRuntimeState> {
        validate_state_account_id(account_id)?;
        let path = self.account_state_path(account_id);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AccountRuntimeState::new(account_id));
            }
            Err(error) => return Err(BridgeError::Io(error)),
        };
        let state: AccountRuntimeState = serde_json::from_slice(&bytes)?;
        if state.version != ACCOUNT_STATE_VERSION || state.account_id != account_id {
            return Err(BridgeError::Precondition(format!(
                "invalid account runtime state for '{account_id}'"
            )));
        }
        Ok(state)
    }

    pub fn save_account(&self, state: &AccountRuntimeState) -> Result<()> {
        validate_state_account_id(&state.account_id)?;
        if state.version != ACCOUNT_STATE_VERSION {
            return Err(BridgeError::Precondition(format!(
                "unsupported account runtime state version {}",
                state.version
            )));
        }
        let dir = self.bridge_dir.join("account-state");
        prepare_private_dir(&dir)?;
        write_json_atomic(&self.account_state_path(&state.account_id), state)
    }

    pub fn load_router(&self) -> Result<RouterRuntimeState> {
        let path = self.bridge_dir.join("router-state.json");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(RouterRuntimeState::default());
            }
            Err(error) => return Err(BridgeError::Io(error)),
        };
        let state: RouterRuntimeState = serde_json::from_slice(&bytes)?;
        if state.version != ROUTER_STATE_VERSION {
            return Err(BridgeError::Precondition(format!(
                "unsupported router runtime state version {}",
                state.version
            )));
        }
        Ok(state)
    }

    pub fn save_router(&self, state: &RouterRuntimeState) -> Result<()> {
        if state.version != ROUTER_STATE_VERSION {
            return Err(BridgeError::Precondition(format!(
                "unsupported router runtime state version {}",
                state.version
            )));
        }
        prepare_private_dir(&self.bridge_dir)?;
        write_json_atomic(&self.bridge_dir.join("router-state.json"), state)
    }

    fn account_state_path(&self, account_id: &str) -> PathBuf {
        self.bridge_dir
            .join("account-state")
            .join(format!("{account_id}.json"))
    }
}

pub struct RouterLock {
    file: File,
}

impl Drop for RouterLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn validate_state_account_id(account_id: &str) -> Result<()> {
    let valid = !account_id.is_empty()
        && account_id.len() <= 128
        && account_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(BridgeError::Security(format!(
            "invalid account id for runtime state: '{account_id}'"
        )))
    }
}

fn prepare_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn open_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path)?;
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(file)
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        BridgeError::Precondition(format!("state path has no parent: {}", path.display()))
    })?;
    prepare_private_dir(parent)?;
    let bytes = serde_json::to_vec_pretty(value)?;
    let temp = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("state"),
        uuid::Uuid::new_v4()
    ));

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temp)?;
    let result = (|| -> Result<()> {
        file.write_all(&bytes)?;
        file.sync_all()?;
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        {
            if let Ok(directory) = File::open(parent) {
                let _ = directory.sync_all();
            }
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn reconcile_expires_window_boundary_reservations_and_cooldown() {
        let mut state = AccountRuntimeState::new("web-a");
        state.dispatches_ms = vec![899, 900, 901, 1_100];
        state.reservations = vec![
            AccountReservation {
                reservation_id: "expired".into(),
                scope_id: None,
                created_ms: 900,
                expires_ms: 1_000,
            },
            AccountReservation {
                reservation_id: "live".into(),
                scope_id: None,
                created_ms: 999,
                expires_ms: 1_001,
            },
        ];
        state.cooldown_until_ms = Some(1_000);
        state.cooldown_reason = Some("rate_limit".into());

        state.reconcile(1_000, 100);
        assert_eq!(state.dispatches_ms, vec![901, 1_100]);
        assert_eq!(state.reservations.len(), 1);
        assert_eq!(state.reservations[0].reservation_id, "live");
        assert_eq!(state.cooldown_until_ms, None);
        assert_eq!(state.cooldown_reason, None);
    }

    #[test]
    fn state_round_trips_and_corruption_fails_closed() {
        let dir = tempdir().unwrap();
        let store = AccountStateStore::new(dir.path());
        let mut state = AccountRuntimeState::new("web-a");
        state.dispatches_ms.push(42);
        state.cooldown_until_ms = Some(100);
        store.save_account(&state).unwrap();
        assert_eq!(store.load_account("web-a").unwrap(), state);

        fs::write(
            dir.path().join("account-state/web-a.json"),
            b"{ definitely not json",
        )
        .unwrap();
        assert!(store.load_account("web-a").is_err());
    }

    #[test]
    fn router_lock_serializes_transactions() {
        let dir = tempdir().unwrap();
        let store = AccountStateStore::new(dir.path());
        let lock = store.lock().unwrap();
        assert!(store.try_lock().unwrap().is_none());
        drop(lock);
        assert!(store.try_lock().unwrap().is_some());
    }

    #[test]
    fn state_account_id_cannot_escape_state_directory() {
        let dir = tempdir().unwrap();
        let store = AccountStateStore::new(dir.path());
        assert!(store.load_account("../escape").is_err());
    }
}
