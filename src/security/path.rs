use crate::error::{BridgeError, Result};
use std::path::{Component, Path, PathBuf};

pub struct PathPolicy;

impl PathPolicy {
    /// Checks if a hidden file or directory name is explicitly permitted.
    pub fn is_allowed_dot_component(name: &str) -> bool {
        matches!(
            name,
            ".omo"
                | ".github"
                | ".vscode"
                | ".cargo"
                | ".gitignore"
                | ".gitattributes"
                | ".env.example"
                | ".editorconfig"
                | ".dockerignore"
                | ".prettierrc"
                | ".eslintrc"
                | ".biome"
                | ".biomerc"
        )
    }

    /// Checks if a file or directory component matches dangerous secret/credential patterns.
    pub fn is_secret_component(name: &str) -> bool {
        let lower = name.to_ascii_lowercase();
        if lower == ".git"
            || lower == ".ssh"
            || lower == ".aws"
            || lower == ".gnupg"
            || lower == ".env"
            || (lower.starts_with(".env.") && lower != ".env.example")
            || lower == "id_rsa"
            || lower == "id_ed25519"
            || lower == "id_ecdsa"
            || lower == "id_dsa"
            || lower.ends_with(".pem")
            || lower.ends_with(".key")
            || lower.ends_with(".pfx")
            || lower.ends_with(".p12")
            || lower == ".npmrc"
            || lower == ".netrc"
            || lower == ".htpasswd"
        {
            return true;
        }

        // Specific secret files like auth.json, credentials.json, secrets.json.
        if lower == "auth.json"
            || lower == "tokens.json"
            || lower == "credentials.json"
            || lower == "secrets.json"
            || lower == "token.json"
            || lower == "secret.json"
        {
            return true;
        }

        false
    }

    /// Strictly sanitizes a relative path string and checks for path traversal / secret files.
    pub fn sanitize_relative_path(input: &str) -> Result<PathBuf> {
        let p = Path::new(input);
        if p.is_absolute() {
            return Err(BridgeError::Security("Absolute paths are forbidden".into()));
        }

        let mut clean = PathBuf::new();
        for comp in p.components() {
            match comp {
                Component::Normal(c) => {
                    let s = c.to_string_lossy();
                    if s.starts_with('.') && !Self::is_allowed_dot_component(&s) {
                        return Err(BridgeError::Security(format!(
                            "Hidden/dotfile access denied: {}",
                            s
                        )));
                    }
                    if Self::is_secret_component(&s) {
                        return Err(BridgeError::Security(format!(
                            "Secret key file access denied: {}",
                            s
                        )));
                    }
                    clean.push(c);
                }
                Component::ParentDir => {
                    return Err(BridgeError::Security(
                        "Path traversal (..) is forbidden".into(),
                    ));
                }
                Component::CurDir => continue,
                _ => return Err(BridgeError::Security("Invalid path component".into())),
            }
        }

        if clean.as_os_str().is_empty() {
            return Err(BridgeError::Path("Path cannot be empty".into()));
        }

        Ok(clean)
    }

    /// Re-applies the dotfile/secret denylist to what a sanitized path actually RESOLVES to.
    ///
    /// Name-based sanitization alone is bypassable: an in-workspace symlink with an innocuous
    /// name (for example `notes.txt -> .env`) passes the spelling check, and the caller then
    /// follows the link to the denied target. Paths that do not exist yet are left to the
    /// caller (creation is governed by the sanitized name), and targets that resolve outside
    /// the workspace are rejected.
    pub fn ensure_resolved_target_allowed(root: &Path, rel: &Path) -> Result<()> {
        let resolved = match dunce::canonicalize(root.join(rel)) {
            Ok(path) => path,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(BridgeError::Path(format!("Failed to resolve path: {}", e))),
        };
        let canonical_root = dunce::canonicalize(root)
            .map_err(|e| BridgeError::Path(format!("Failed to resolve workspace root: {}", e)))?;
        let inside = resolved.strip_prefix(&canonical_root).map_err(|_| {
            BridgeError::Security(format!(
                "Resolved path escapes workspace through a symlink: {}",
                resolved.display()
            ))
        })?;

        for comp in inside.components() {
            if let Component::Normal(c) = comp {
                let s = c.to_string_lossy();
                if s.starts_with('.') && !Self::is_allowed_dot_component(&s) {
                    return Err(BridgeError::Security(format!(
                        "Hidden/dotfile access denied: {} (resolved target)",
                        s
                    )));
                }
                if Self::is_secret_component(&s) {
                    return Err(BridgeError::Security(format!(
                        "Secret key file access denied: {} (resolved target)",
                        s
                    )));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_paths() {
        assert!(PathPolicy::sanitize_relative_path("src/main.rs").is_ok());
        assert!(PathPolicy::sanitize_relative_path("tests/foo/bar.js").is_ok());
        assert!(PathPolicy::sanitize_relative_path("./Cargo.toml").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".omo/plans/my-plan.md").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".github/workflows/ci.yml").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".vscode/settings.json").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".cargo/config.toml").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".gitignore").is_ok());
        assert!(PathPolicy::sanitize_relative_path(".env.example").is_ok());
    }

    #[test]
    fn test_path_traversal_denied() {
        assert!(PathPolicy::sanitize_relative_path("../secret.txt").is_err());
        assert!(PathPolicy::sanitize_relative_path("src/../../etc/passwd").is_err());
        assert!(PathPolicy::sanitize_relative_path("/etc/passwd").is_err());
    }

    #[test]
    fn test_secret_files_denied() {
        assert!(PathPolicy::sanitize_relative_path(".git/config").is_err());
        assert!(PathPolicy::sanitize_relative_path(".env").is_err());
        assert!(PathPolicy::sanitize_relative_path(".env.local").is_err());
        assert!(PathPolicy::sanitize_relative_path(".omo/auth.json").is_err());
        assert!(PathPolicy::sanitize_relative_path("certs/server.pem").is_err());
        assert!(PathPolicy::sanitize_relative_path("certs/SERVER.PEM").is_err());
        assert!(PathPolicy::sanitize_relative_path("keys/CLIENT.KEY").is_err());
        assert!(PathPolicy::sanitize_relative_path("id_rsa").is_err());
        assert!(PathPolicy::sanitize_relative_path("ID_RSA").is_err());
        assert!(PathPolicy::sanitize_relative_path(".ssh/id_ed25519").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_resolved_target_policy_follows_symlink_alias() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "API_KEY=1").unwrap();
        std::fs::write(dir.path().join("real.txt"), "ok").unwrap();
        symlink(".env", dir.path().join("notes.txt")).unwrap();
        symlink("real.txt", dir.path().join("alias.txt")).unwrap();

        let err = PathPolicy::ensure_resolved_target_allowed(dir.path(), Path::new("notes.txt"))
            .unwrap_err();
        assert!(err.to_string().contains("denied"), "got: {}", err);
        assert!(
            PathPolicy::ensure_resolved_target_allowed(dir.path(), Path::new("alias.txt")).is_ok()
        );
        assert!(
            PathPolicy::ensure_resolved_target_allowed(dir.path(), Path::new("missing.txt"))
                .is_ok()
        );
    }
}
