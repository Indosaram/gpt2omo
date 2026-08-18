use crate::error::{BridgeError, Result};
use crate::security::PathPolicy;
use cap_std::ambient_authority;
use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions, TryLockError};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkspaceScope {
    pub version: u32,
    pub scope_id: String,
    pub workspace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_page_id: Option<String>,
    pub created_ms: u64,
    pub updated_ms: u64,
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

        Ok(Self {
            mount_root,
            scope_dir: scope_dir.as_ref().to_path_buf(),
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
        let scope_id = uuid::Uuid::new_v4().to_string();
        self.register_with_id(&scope_id, workspace, None, Some(browser_page_id))
    }

    fn register_with_id(
        &self,
        scope_id: &str,
        workspace: impl AsRef<Path>,
        terminal: Option<String>,
        browser_page_id: Option<String>,
    ) -> Result<WorkspaceScope> {
        validate_scope_id(scope_id)?;
        let workspace = Workspace::open(workspace)?;
        self.ensure_within_mount(workspace.root())?;
        if self.mount_root.parent().is_none() && workspace.root() == self.mount_root {
            return Err(BridgeError::Security(
                "Refusing to register the filesystem root itself as a workspace scope".into(),
            ));
        }

        let now = now_ms();
        let scope = WorkspaceScope {
            version: 1,
            scope_id: scope_id.to_string(),
            workspace: workspace.root().to_string_lossy().to_string(),
            terminal,
            browser_page_id,
            created_ms: now,
            updated_ms: now,
        };
        self.persist(&scope)?;
        Ok(scope)
    }

    pub fn lookup(&self, scope_id: &str) -> Result<WorkspaceScope> {
        validate_scope_id(scope_id)?;
        let bytes = fs::read(self.scope_path(scope_id)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BridgeError::Path(format!("Unknown or expired workspace scope: {}", scope_id))
            } else {
                BridgeError::Io(e)
            }
        })?;
        let scope: WorkspaceScope = serde_json::from_slice(&bytes)?;
        if scope.version != 1 || scope.scope_id != scope_id {
            return Err(BridgeError::Path(format!(
                "Invalid workspace scope state for {}",
                scope_id
            )));
        }
        let workspace = Workspace::open(&scope.workspace)?;
        self.ensure_within_mount(workspace.root())?;
        if self.mount_root.parent().is_none() && workspace.root() == self.mount_root {
            return Err(BridgeError::Security(
                "Filesystem root cannot be used as a workspace scope".into(),
            ));
        }
        Ok(scope)
    }

    pub fn list_scopes(&self) -> Result<Vec<WorkspaceScope>> {
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
                continue;
            };
            if uuid::Uuid::parse_str(scope_id).is_err() {
                continue;
            }
            scopes.push(self.lookup(scope_id)?);
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
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_dir.join(format!("{}.lock", scope_id)))
            .map_err(BridgeError::Io)
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

    fn persist(&self, scope: &WorkspaceScope) -> Result<()> {
        fs::create_dir_all(&self.scope_dir)?;
        let bytes = serde_json::to_vec_pretty(scope)?;
        let temp = self.scope_dir.join(format!(
            ".scope-{}-{}.tmp",
            scope.scope_id,
            uuid::Uuid::new_v4()
        ));
        fs::write(&temp, bytes)?;
        if let Err(error) = fs::rename(&temp, self.scope_path(&scope.scope_id)) {
            let _ = fs::remove_file(&temp);
            return Err(BridgeError::Io(error));
        }
        Ok(())
    }

    fn scope_path(&self, scope_id: &str) -> PathBuf {
        self.scope_dir.join(format!("{}.json", scope_id))
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
}
