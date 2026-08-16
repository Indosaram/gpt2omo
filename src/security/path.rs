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
}
