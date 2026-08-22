use crate::accounts::LEGACY_ACCOUNT_ID;
use crate::error::{BridgeError, Result};
use crate::orca::BrowserDriverKind;
use crate::security::PathPolicy;
use cap_std::ambient_authority;
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct Workspace {
    root: PathBuf,
}

impl Workspace {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let canonical = dunce::canonicalize(path.as_ref())
            .map_err(|e| BridgeError::Path(format!("Failed to canonicalize workspace: {}", e)))?;

        if !canonical.is_dir() {
            return Err(BridgeError::Path(
                "Workspace root must be a directory".into(),
            ));
        }

        Ok(Self { root: canonical })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn cap_dir(&self) -> Result<Dir> {
        Dir::open_ambient_dir(&self.root, ambient_authority()).map_err(BridgeError::Io)
    }

    pub fn resolve_relative(&self, rel: &str) -> Result<PathBuf> {
        let clean = PathPolicy::sanitize_relative_path(rel)?;
        let _ = self.cap_dir()?;
        let candidate = self.root.join(clean);

        let mut existing = candidate.as_path();
        while !existing.exists() {
            existing = existing.parent().ok_or_else(|| {
                BridgeError::Security("Path has no existing ancestor inside workspace".into())
            })?;
        }

        let canonical_existing = dunce::canonicalize(existing).map_err(|e| {
            BridgeError::Path(format!("Failed to canonicalize path ancestor: {}", e))
        })?;
        self.ensure_inside(&canonical_existing)?;

        if candidate.exists() {
            let canonical_candidate = dunce::canonicalize(&candidate)
                .map_err(|e| BridgeError::Path(format!("Failed to canonicalize path: {}", e)))?;
            self.ensure_inside(&canonical_candidate)?;
        }

        Ok(candidate)
    }

    fn ensure_inside(&self, canonical: &Path) -> Result<()> {
        if !canonical.starts_with(&self.root) {
            return Err(BridgeError::Security(format!(
                "Resolved path escapes workspace through a symlink: {}",
                canonical.display()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserBinding {
    pub account_id: String,
    pub driver: BrowserDriverKind,
    pub instance: String,
    pub page_id: String,
}

impl BrowserBinding {
    pub fn new(
        account_id: impl Into<String>,
        driver: BrowserDriverKind,
        instance: impl Into<String>,
        page_id: impl Into<String>,
    ) -> Self {
        Self {
            account_id: account_id.into(),
            driver,
            instance: instance.into(),
            page_id: page_id.into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceScope {
    pub version: u32,
    pub scope_id: String,
    pub workspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser: Option<BrowserBinding>,
    // V1 compatibility field. V2 writes only `browser`; lookup repopulates this in-memory
    // compatibility view so existing callers can continue using the exact page id.
    #[serde(default, skip_serializing)]
    pub browser_page_id: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
}

impl WorkspaceScope {
    pub fn account_id(&self) -> &str {
        self.browser
            .as_ref()
            .map(|binding| binding.account_id.as_str())
            .unwrap_or(LEGACY_ACCOUNT_ID)
    }

    pub fn browser_instance(&self) -> Option<&str> {
        self.browser
            .as_ref()
            .map(|binding| binding.instance.as_str())
    }

    pub fn page_id(&self) -> Option<&str> {
        self.browser
            .as_ref()
            .map(|binding| binding.page_id.as_str())
            .or(self.browser_page_id.as_deref())
    }
}

pub struct WorkspaceScopeLock {
    file: File,
}

impl Drop for WorkspaceScopeLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[derive(Clone, Debug)]
pub struct WorkspaceMux {
    mount_root: PathBuf,
    scope_dir: PathBuf,
}

impl WorkspaceMux {
    pub fn new(mount_root: impl AsRef<Path>, scope_dir: impl AsRef<Path>) -> Result<Self> {
        let mount_root = dunce::canonicalize(mount_root.as_ref())
            .map_err(|e| BridgeError::Path(format!("Failed to canonicalize mount root: {}", e)))?;
        if !mount_root.is_dir() {
            return Err(BridgeError::Path("Mount root must be a directory".into()));
        }

        fs::create_dir_all(scope_dir.as_ref())?;
        let scope_dir = dunce::canonicalize(scope_dir.as_ref()).map_err(|e| {
            BridgeError::Path(format!("Failed to canonicalize scope directory: {}", e))
        })?;
        harden_control_plane_permissions(&scope_dir)?;

        Ok(Self {
            mount_root,
            scope_dir,
        })
    }

    pub fn mount_root(&self) -> &Path {
        &self.mount_root
    }

    pub fn scope_dir(&self) -> &Path {
        &self.scope_dir
    }

    pub fn register(
        &self,
        workspace: impl AsRef<Path>,
        terminal: Option<String>,
    ) -> Result<WorkspaceScope> {
        let scope_id = uuid::Uuid::new_v4().to_string();
        self.register_with_id(&scope_id, workspace, terminal, None)
    }

    pub fn register_browser(
        &self,
        workspace: impl AsRef<Path>,
        browser_page_id: String,
    ) -> Result<WorkspaceScope> {
        self.register_browser_binding(
            workspace,
            BrowserBinding::new(
                LEGACY_ACCOUNT_ID,
                BrowserDriverKind::Orca,
                "legacy",
                browser_page_id,
            ),
        )
    }

    pub fn register_browser_binding(
        &self,
        workspace: impl AsRef<Path>,
        browser: BrowserBinding,
    ) -> Result<WorkspaceScope> {
        validate_browser_binding(&browser)?;
        let scope_id = uuid::Uuid::new_v4().to_string();
        self.register_with_id(&scope_id, workspace, None, Some(browser))
    }

    fn register_with_id(
        &self,
        scope_id: &str,
        workspace: impl AsRef<Path>,
        terminal: Option<String>,
        browser: Option<BrowserBinding>,
    ) -> Result<WorkspaceScope> {
        validate_scope_id(scope_id)?;
        let workspace = Workspace::open(workspace)?;
        self.ensure_within_mount(workspace.root())?;
        self.ensure_safe_workspace_root(workspace.root())?;

        if let Some(binding) = browser.as_ref() {
            validate_browser_binding(binding)?;
        }
        let now = now_ms();
        let browser_page_id = browser.as_ref().map(|binding| binding.page_id.clone());
        let scope = WorkspaceScope {
            version: 2,
            scope_id: scope_id.to_string(),
            workspace: workspace.root().to_string_lossy().to_string(),
            terminal,
            browser,
            browser_page_id,
            created_ms: now,
            updated_ms: now,
        };
        self.persist(&scope)?;
        Ok(scope)
    }

    pub fn lookup(&self, scope_id: &str) -> Result<WorkspaceScope> {
        validate_scope_id(scope_id)?;
        let path = self.scope_path(scope_id);
        if path.exists() {
            set_private_file(&path)?;
        }
        let bytes = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BridgeError::Path(format!("Unknown or expired workspace scope: {}", scope_id))
            } else {
                BridgeError::Io(e)
            }
        })?;
        let mut scope: WorkspaceScope = serde_json::from_slice(&bytes)?;
        if !matches!(scope.version, 1 | 2) || scope.scope_id != scope_id {
            return Err(BridgeError::Path(format!(
                "Invalid workspace scope state for {}",
                scope_id
            )));
        }
        match scope.version {
            1 => {
                if scope.browser.is_some() {
                    return Err(BridgeError::Path(format!(
                        "Invalid V1 workspace scope browser binding for {}",
                        scope_id
                    )));
                }
            }
            2 => {
                if let Some(binding) = scope.browser.as_ref() {
                    validate_browser_binding(binding)?;
                    if scope
                        .browser_page_id
                        .as_ref()
                        .is_some_and(|legacy| legacy != &binding.page_id)
                    {
                        return Err(BridgeError::Path(format!(
                            "Conflicting browser page ids in workspace scope {}",
                            scope_id
                        )));
                    }
                    scope.browser_page_id = Some(binding.page_id.clone());
                } else if scope.browser_page_id.is_some() {
                    return Err(BridgeError::Path(format!(
                        "V2 workspace scope {} contains legacy browser_page_id without browser binding",
                        scope_id
                    )));
                }
            }
            _ => unreachable!(),
        }
        let workspace = Workspace::open(&scope.workspace)?;
        self.ensure_within_mount(workspace.root())?;
        self.ensure_safe_workspace_root(workspace.root())?;
        Ok(scope)
    }

    pub fn list_scopes(&self) -> Result<Vec<WorkspaceScope>> {
        self.list_scopes_impl(false)
    }

    pub fn list_scopes_strict(&self) -> Result<Vec<WorkspaceScope>> {
        self.list_scopes_impl(true)
    }

    fn list_scopes_impl(&self, strict: bool) -> Result<Vec<WorkspaceScope>> {
        let entries = match fs::read_dir(&self.scope_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(BridgeError::Io(error)),
        };

        let mut scopes = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let Some(scope_id) = path.file_stem().and_then(|value| value.to_str()) else {
                if strict {
                    return Err(BridgeError::Precondition(format!(
                        "invalid scope state filename: {}",
                        path.display()
                    )));
                }
                continue;
            };
            if uuid::Uuid::parse_str(scope_id).is_err() {
                if strict {
                    return Err(BridgeError::Precondition(format!(
                        "invalid scope state id: {scope_id}"
                    )));
                }
                continue;
            }
            match self.lookup(scope_id) {
                Ok(scope) => scopes.push(scope),
                Err(error) => {
                    if strict {
                        return Err(error);
                    }
                    tracing::warn!(scope_id, error = %error, "Skipping corrupted scope file");
                }
            }
        }
        scopes.sort_by(|left, right| left.scope_id.cmp(&right.scope_id));
        Ok(scopes)
    }

    pub fn resolve(&self, scope_id: &str) -> Result<Workspace> {
        let scope = self.lookup(scope_id)?;
        Workspace::open(scope.workspace)
    }

    pub fn update_terminal(&self, scope_id: &str, terminal: &str) -> Result<WorkspaceScope> {
        let mut scope = self.lookup(scope_id)?;
        scope.terminal = Some(terminal.to_string());
        scope.updated_ms = now_ms();
        self.persist(&scope)?;
        Ok(scope)
    }

    pub fn lock_scope(&self, scope_id: &str) -> Result<WorkspaceScopeLock> {
        validate_scope_id(scope_id)?;
        let file = self.open_lock_file(scope_id)?;
        file.lock().map_err(BridgeError::Io)?;
        Ok(WorkspaceScopeLock { file })
    }

    pub fn try_lock_scope(&self, scope_id: &str) -> Result<Option<WorkspaceScopeLock>> {
        validate_scope_id(scope_id)?;
        let file = self.open_lock_file(scope_id)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(WorkspaceScopeLock { file })),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(BridgeError::Io(error)),
        }
    }

    pub fn remove(&self, scope_id: &str) -> Result<()> {
        validate_scope_id(scope_id)?;
        match fs::remove_file(self.scope_path(scope_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(BridgeError::Io(error)),
        }
    }

    fn open_lock_file(&self, scope_id: &str) -> Result<File> {
        let lock_dir = self.scope_dir.join(".locks");
        fs::create_dir_all(&lock_dir)?;
        set_private_dir(&lock_dir)?;
        let lock_path = lock_dir.join(format!("{}.lock", scope_id));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        options.mode(0o600);
        let file = options.open(&lock_path).map_err(BridgeError::Io)?;
        set_private_file(&lock_path)?;
        Ok(file)
    }

    fn ensure_within_mount(&self, workspace: &Path) -> Result<()> {
        if !workspace.starts_with(&self.mount_root) {
            return Err(BridgeError::Security(format!(
                "Workspace {} is outside mount root {}",
                workspace.display(),
                self.mount_root.display()
            )));
        }
        Ok(())
    }

    fn ensure_safe_workspace_root(&self, workspace: &Path) -> Result<()> {
        if workspace.parent().is_none() {
            return Err(BridgeError::Security(
                "Refusing to register the filesystem root itself as a workspace scope".into(),
            ));
        }

        if let Some(home) = std::env::var_os("HOME") {
            if let Ok(home) = dunce::canonicalize(PathBuf::from(home)) {
                if home.starts_with(workspace) {
                    return Err(BridgeError::Security(
                        "Refusing a workspace root that is $HOME or a broader ancestor".into(),
                    ));
                }
            }
        }

        for (label, control) in [
            ("workspace scope control directory", self.scope_dir.clone()),
            (
                "bridge control directory",
                absolute_control_path(&default_bridge_base_dir()),
            ),
        ] {
            if control.starts_with(workspace) || workspace.starts_with(&control) {
                return Err(BridgeError::Security(format!(
                    "Refusing workspace {} because it overlaps the {} {}",
                    workspace.display(),
                    label,
                    control.display()
                )));
            }
        }
        Ok(())
    }

    fn persist(&self, scope: &WorkspaceScope) -> Result<()> {
        fs::create_dir_all(&self.scope_dir)?;
        set_private_dir(&self.scope_dir)?;
        let bytes = serde_json::to_vec_pretty(scope)?;
        let temp = self.scope_dir.join(format!(
            ".scope-{}-{}.tmp",
            scope.scope_id,
            uuid::Uuid::new_v4()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&temp).map_err(BridgeError::Io)?;
        if let Err(error) = file.write_all(&bytes).and_then(|_| file.sync_all()) {
            let _ = fs::remove_file(&temp);
            return Err(BridgeError::Io(error));
        }
        drop(file);
        let final_path = self.scope_path(&scope.scope_id);
        if let Err(error) = fs::rename(&temp, &final_path) {
            let _ = fs::remove_file(&temp);
            return Err(BridgeError::Io(error));
        }
        set_private_file(&final_path)?;
        if let Ok(directory) = File::open(&self.scope_dir) {
            let _ = directory.sync_all();
        }
        Ok(())
    }

    fn scope_path(&self, scope_id: &str) -> PathBuf {
        self.scope_dir.join(format!("{}.json", scope_id))
    }
}

fn harden_control_plane_permissions(scope_dir: &Path) -> Result<()> {
    set_private_dir(scope_dir)?;
    let lock_dir = scope_dir.join(".locks");
    if lock_dir.exists() {
        set_private_dir(&lock_dir)?;
        for entry in fs::read_dir(&lock_dir)? {
            let path = entry?.path();
            if path.is_file() {
                set_private_file(&path)?;
            }
        }
    }
    for entry in fs::read_dir(scope_dir)? {
        let path = entry?.path();
        if path.is_file() && path.extension().and_then(|value| value.to_str()) == Some("json") {
            set_private_file(&path)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(BridgeError::Io)
}

#[cfg(not(unix))]
fn set_private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(BridgeError::Io)
}

#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> Result<()> {
    Ok(())
}

fn absolute_control_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = dunce::canonicalize(path) {
        return canonical;
    }
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    }
}

pub fn default_bridge_base_dir() -> PathBuf {
    if let Ok(path) = std::env::var("OMO_BRIDGE_HOME") {
        if !path.trim().is_empty() {
            return PathBuf::from(path.trim());
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.trim().is_empty() {
            return PathBuf::from(home.trim()).join(".omo").join("bridge");
        }
    }
    std::env::temp_dir().join("gpt2omo")
}

pub fn default_scope_dir(port: u16) -> PathBuf {
    default_bridge_base_dir().join(format!("scopes-{}", port))
}

fn validate_scope_id(scope_id: &str) -> Result<()> {
    uuid::Uuid::parse_str(scope_id)
        .map(|_| ())
        .map_err(|_| BridgeError::Security("Invalid workspace scope id".into()))
}

fn validate_browser_binding(binding: &BrowserBinding) -> Result<()> {
    let valid_account = !binding.account_id.is_empty()
        && binding.account_id.len() <= 128
        && binding
            .account_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid_account {
        return Err(BridgeError::Security(
            "Invalid browser binding account id".into(),
        ));
    }
    if binding.instance.trim().is_empty() || binding.page_id.trim().is_empty() {
        return Err(BridgeError::Precondition(
            "Browser binding instance and page_id must be non-empty".into(),
        ));
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_workspace_capability() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        assert_eq!(ws.root(), dunce::canonicalize(dir.path()).unwrap());
        assert!(ws.cap_dir().is_ok());
        assert!(ws.resolve_relative("src/lib.rs").is_ok());
        assert!(ws.resolve_relative("../outside.txt").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_symlink_escape_denied_for_existing_and_new_targets() {
        use std::os::unix::fs::symlink;

        let workspace_dir = tempdir().unwrap();
        let outside_dir = tempdir().unwrap();
        fs::write(outside_dir.path().join("secret.txt"), "secret").unwrap();
        symlink(outside_dir.path(), workspace_dir.path().join("escape")).unwrap();

        let ws = Workspace::open(workspace_dir.path()).unwrap();
        assert!(ws.resolve_relative("escape/secret.txt").is_err());
        assert!(ws.resolve_relative("escape/new.txt").is_err());
    }

    #[test]
    fn mux_registers_independent_scopes_without_switching_global_state() {
        let mount = tempdir().unwrap();
        let first = mount.path().join("first");
        let second = mount.path().join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();

        let a = mux.register(&first, Some("term-a".into())).unwrap();
        let b = mux.register_browser(&second, "page-b".into()).unwrap();

        assert_ne!(a.scope_id, b.scope_id);
        assert_eq!(
            mux.resolve(&a.scope_id).unwrap().root(),
            dunce::canonicalize(&first).unwrap()
        );
        assert_eq!(
            mux.resolve(&b.scope_id).unwrap().root(),
            dunce::canonicalize(&second).unwrap()
        );
        assert_eq!(
            mux.lookup(&a.scope_id).unwrap().terminal.as_deref(),
            Some("term-a")
        );
        assert_eq!(
            mux.lookup(&b.scope_id).unwrap().browser_page_id.as_deref(),
            Some("page-b")
        );
        assert_eq!(mux.lookup(&b.scope_id).unwrap().account_id(), "default");
    }

    #[test]
    fn v2_browser_binding_persists_account_affinity_without_legacy_scalar() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                BrowserBinding::new("web-a", BrowserDriverKind::Orca, "instance-a", "same-page"),
            )
            .unwrap();
        let raw =
            fs::read_to_string(state.path().join(format!("{}.json", scope.scope_id))).unwrap();
        assert!(raw.contains("\"browser\""));
        assert!(raw.contains("\"account_id\": \"web-a\""));
        assert!(!raw.contains("browser_page_id"));

        let loaded = mux.lookup(&scope.scope_id).unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.account_id(), "web-a");
        assert_eq!(loaded.browser_instance(), Some("instance-a"));
        assert_eq!(loaded.page_id(), Some("same-page"));
        assert_eq!(loaded.browser_page_id.as_deref(), Some("same-page"));
    }

    #[test]
    fn v1_scope_migrates_logically_to_legacy_default_account() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope_id = uuid::Uuid::new_v4().to_string();
        let raw = serde_json::json!({
            "version": 1,
            "scope_id": scope_id,
            "workspace": dunce::canonicalize(&project).unwrap().to_string_lossy(),
            "browser_page_id": "legacy-page",
            "created_ms": 1,
            "updated_ms": 1
        });
        fs::create_dir_all(state.path()).unwrap();
        fs::write(
            state
                .path()
                .join(format!("{}.json", raw["scope_id"].as_str().unwrap())),
            serde_json::to_vec(&raw).unwrap(),
        )
        .unwrap();

        let loaded = mux.lookup(raw["scope_id"].as_str().unwrap()).unwrap();
        assert_eq!(loaded.account_id(), LEGACY_ACCOUNT_ID);
        assert_eq!(loaded.page_id(), Some("legacy-page"));
    }

    #[test]
    fn mux_lists_only_persisted_scope_files() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let first = mux.register_browser(&project, "page-a".into()).unwrap();
        let second = mux.register_browser(&project, "page-b".into()).unwrap();
        fs::write(state.path().join("not-a-scope.txt"), "ignore").unwrap();

        let scopes = mux.list_scopes().unwrap();
        assert_eq!(scopes.len(), 2);
        assert!(scopes.iter().any(|scope| scope.scope_id == first.scope_id));
        assert!(scopes.iter().any(|scope| scope.scope_id == second.scope_id));
    }

    #[test]
    fn mux_lists_scopes_skipping_corrupted_files() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let valid = mux.register_browser(&project, "page-a".into()).unwrap();
        let corrupted_id = uuid::Uuid::new_v4().to_string();
        fs::write(
            state.path().join(format!("{}.json", corrupted_id)),
            "{ corrupted json",
        )
        .unwrap();

        let scopes = mux.list_scopes().unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].scope_id, valid.scope_id);
    }

    #[test]
    fn mux_scope_lock_can_be_acquired_and_released() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux.register_browser(&project, "page".into()).unwrap();

        let lock = mux.lock_scope(&scope.scope_id).unwrap();
        drop(lock);
        assert!(mux.try_lock_scope(&scope.scope_id).unwrap().is_some());
    }

    #[test]
    fn mux_can_remove_a_scope() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux.register_browser(&project, "page".into()).unwrap();
        assert!(mux.lookup(&scope.scope_id).is_ok());
        mux.remove(&scope.scope_id).unwrap();
        assert!(mux.lookup(&scope.scope_id).is_err());
    }

    #[test]
    fn mux_rejects_workspace_that_contains_or_enters_scope_control_plane() {
        let mount = tempdir().unwrap();
        let scope_dir = mount.path().join("control/scopes");
        let mux = WorkspaceMux::new(mount.path(), &scope_dir).unwrap();
        let broad = mux.register(mount.path(), None).unwrap_err().to_string();
        assert!(broad.contains("overlaps"));
        fs::create_dir_all(scope_dir.join("nested")).unwrap();
        let nested = mux
            .register(scope_dir.join("nested"), None)
            .unwrap_err()
            .to_string();
        assert!(nested.contains("overlaps"));
    }

    #[test]
    fn mux_rejects_scope_outside_mount_root() {
        let mount = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        assert!(mux.register(outside.path(), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn machine_root_mount_refuses_root_as_scoped_workspace() {
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new("/", state.path()).unwrap();
        assert!(mux.register("/", None).is_err());
    }

    #[test]
    fn invalid_scope_ids_are_rejected_before_filesystem_lookup() {
        let mount = tempdir().unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        assert!(mux.lookup("../escape").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn mux_control_plane_permissions_are_private() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        fs::set_permissions(state.path(), fs::Permissions::from_mode(0o777)).unwrap();

        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux.register_browser(&project, "page".into()).unwrap();
        let _lock = mux.lock_scope(&scope.scope_id).unwrap();

        assert_eq!(
            fs::metadata(state.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(state.path().join(format!("{}.json", scope.scope_id)))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(
                state
                    .path()
                    .join(".locks")
                    .join(format!("{}.lock", scope.scope_id))
            )
            .unwrap()
            .permissions()
            .mode()
                & 0o777,
            0o600
        );
    }
}
