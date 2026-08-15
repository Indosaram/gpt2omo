use crate::error::{BridgeError, Result};
use std::path::{Component, Path, PathBuf};

pub struct PathPolicy;

impl PathPolicy {
    /// Strictly sanitizes a relative path string and checks for path traversal / secret files
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
                    if s.starts_with('.') && s != ".gitignore" && s != ".env.example" {
                        return Err(BridgeError::Security(format!(
                            "Hidden/dotfile access denied: {}",
                            s
                        )));
                    }
                    if s == "id_rsa"
                        || s == "id_ed25519"
                        || s.ends_with(".pem")
                        || s.ends_with(".key")
                    {
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
        assert!(PathPolicy::sanitize_relative_path("certs/server.pem").is_err());
        assert!(PathPolicy::sanitize_relative_path("id_rsa").is_err());
    }
}
