use crate::error::{BridgeError, Result};
use crate::security::PathPolicy;
use cap_std::ambient_authority;
use cap_std::fs::Dir;
use std::path::{Path, PathBuf};

#[derive(Clone)]
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

    /// Resolve a user supplied relative path while preventing both lexical traversal and
    /// symlink-based escapes from the mounted workspace.
    ///
    /// For non-existent paths (for example a file about to be created), the nearest existing
    /// ancestor is canonicalized and checked. This prevents a writable path below an in-workspace
    /// symlink from escaping the sandbox.
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
}
