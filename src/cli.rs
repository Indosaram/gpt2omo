use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "gpt2omo",
    version,
    about = "Scoped local MCP HTTP/SSE daemon for ChatGPT"
)]
pub struct Cli {
    /// Broad filesystem mount root. File tools are constrained by per-delegation scope; command children are not an OS sandbox.
    #[arg(long, default_value_os_t = default_mount_root())]
    pub mount_root: PathBuf,

    /// Optional directory that stores per-delegation workspace scope leases.
    #[arg(long, env = "OMO_SCOPE_DIR")]
    pub scope_dir: Option<PathBuf>,

    /// Bind address for HTTP/SSE server
    #[arg(long, default_value = "127.0.0.1:18800", env = "OMO_BRIDGE_BIND")]
    pub bind: String,

    /// Optional bearer token for bridge-control endpoints such as `/events`; MCP tool calls use scope_id capabilities.
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    pub token: Option<String>,

    /// Optional path to a bearer token for bridge-control endpoints such as `/events`.
    #[arg(long, env = "OMO_BRIDGE_TOKEN_FILE")]
    pub token_file: Option<PathBuf>,

    /// Maximum file read size in bytes (default 10MB)
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub max_file_bytes: usize,

    /// Maximum command timeout in milliseconds
    #[arg(long, default_value_t = 300_000)]
    pub command_timeout_ms: u64,

    /// OpenAI-compatible subagent API base endpoint. query_subagent is disabled when unset.
    #[arg(
        long,
        env = "OMO_SUBAGENT_ENDPOINT",
        value_parser = parse_nonempty_trimmed
    )]
    pub subagent_endpoint: Option<String>,

    /// Optional bearer API key for the configured subagent endpoint.
    #[arg(long, env = "OMO_SUBAGENT_API_KEY")]
    pub subagent_api_key: Option<String>,

    /// Model name sent to the OpenAI-compatible subagent endpoint.
    #[arg(
        long,
        env = "OMO_SUBAGENT_MODEL",
        default_value = "deepseek-v4-flash-free",
        value_parser = parse_nonempty_trimmed
    )]
    pub subagent_model: String,

    /// Explicitly allow a non-loopback subagent endpoint.
    #[arg(long, env = "OMO_SUBAGENT_ALLOW_REMOTE", default_value_t = false)]
    pub subagent_allow_remote: bool,

    /// Allow bridge-control endpoints without a bearer token when no token is configured.
    #[arg(long, env = "OMO_BRIDGE_INSECURE_NO_AUTH", default_value_t = false)]
    pub insecure_no_auth: bool,

    /// Allow arbitrary commands instead of restricting to the binary allowlist.
    #[arg(
        long,
        env = "OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS",
        default_value_t = false
    )]
    pub allow_arbitrary_commands: bool,

    /// Shared/untrusted connector profile: expose read-only workspace tools and disable patch/run/cancel.
    #[arg(long, env = "OMO_BRIDGE_READ_ONLY", default_value_t = false)]
    pub read_only: bool,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            mount_root: default_mount_root(),
            scope_dir: None,
            bind: "127.0.0.1:18800".into(),
            token: None,
            token_file: None,
            max_file_bytes: 10 * 1024 * 1024,
            command_timeout_ms: 300_000,
            subagent_endpoint: None,
            subagent_api_key: None,
            subagent_model: "deepseek-v4-flash-free".into(),
            subagent_allow_remote: false,
            insecure_no_auth: false,
            allow_arbitrary_commands: false,
            read_only: false,
        }
    }
}

impl Cli {
    /// Enforces the daemon exposure invariant after bridge-control authentication has been resolved.
    pub fn validate_bind_security(&self, addr: &std::net::SocketAddr) -> Result<(), String> {
        if !addr.ip().is_loopback() && self.token.is_none() {
            return Err(
                "non-loopback bind requires bearer authentication; --insecure-no-auth is loopback-only"
                    .into(),
            );
        }
        if !addr.ip().is_loopback() && !self.read_only {
            return Err(
                "non-loopback bridge access is read-only by policy because run_command children are not OS-sandboxed; start the daemon with --read-only or keep writable access on loopback"
                    .into(),
            );
        }
        Ok(())
    }

    /// Loads token from `token_file` if `token` is None and `token_file` is specified.
    pub fn load_token_file(&mut self) -> std::io::Result<()> {
        if self.token.is_none() {
            if let Some(path) = &self.token_file {
                let content = std::fs::read_to_string(path)?;
                self.token = Some(content.trim().to_string());
            }
        }
        Ok(())
    }

    /// Ensures an authentication token is configured unless `insecure_no_auth` is true.
    /// When neither `token` nor `token_file` is provided, and `insecure_no_auth` is false:
    /// auto-generates a secure random 32-byte hex token, ensures `~/.omo/bridge/` exists,
    /// writes it to `~/.omo/bridge/token` with Unix permissions 0600, and sets it into `self.token`.
    /// If `insecure_no_auth` is true and token is None, prints a prominent warning to stderr.
    pub fn ensure_auth(&mut self) -> std::io::Result<Option<PathBuf>> {
        self.ensure_auth_in(&crate::security::default_bridge_base_dir())
    }

    /// Internal implementation of `ensure_auth` with custom base directory (useful for testing).
    pub fn ensure_auth_in(
        &mut self,
        base_dir: &std::path::Path,
    ) -> std::io::Result<Option<PathBuf>> {
        self.load_token_file()?;
        if self.token.is_some() {
            return Ok(None);
        }

        if self.insecure_no_auth {
            eprintln!(
                "WARNING: gpt2omo running in INSECURE mode without authentication! Any local process can execute commands in sandboxes."
            );
            return Ok(None);
        }

        let token = generate_secure_token();
        std::fs::create_dir_all(base_dir)?;
        let token_path = base_dir.join("token");
        std::fs::write(&token_path, format!("{}\n", token))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(0o600);
            std::fs::set_permissions(&token_path, perms)?;
        }

        tracing::info!(
            "Authentication token generated and saved to {}",
            token_path.display()
        );
        self.token = Some(token);
        Ok(Some(token_path))
    }
}

pub fn generate_secure_token() -> String {
    let u1 = uuid::Uuid::new_v4();
    let u2 = uuid::Uuid::new_v4();
    let mut bytes = [0u8; 32];
    bytes[..16].copy_from_slice(u1.as_bytes());
    bytes[16..].copy_from_slice(u2.as_bytes());
    hex::encode(bytes)
}

pub fn default_mount_root() -> PathBuf {
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
    {
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout);
            let root = text.trim();
            if !root.is_empty() {
                return PathBuf::from(root);
            }
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        if !home.is_empty() {
            return PathBuf::from(home);
        }
    }
    PathBuf::from(".")
}

fn parse_nonempty_trimmed(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        Err("value must not be empty".to_string())
    } else {
        Ok(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subagent_string_parser_trims_and_rejects_empty_values() {
        assert_eq!(
            parse_nonempty_trimmed(" http://127.0.0.1:8000 ").unwrap(),
            "http://127.0.0.1:8000"
        );
        assert!(parse_nonempty_trimmed("").is_err());
        assert!(parse_nonempty_trimmed("   ").is_err());
    }

    #[test]
    fn default_mount_root_is_git_worktree_or_home() {
        let cli = Cli::try_parse_from(["gpt2omo"]).unwrap();
        let expected = default_mount_root();
        assert_eq!(cli.mount_root, expected);
        assert_ne!(cli.mount_root, PathBuf::from("/"));
    }

    #[test]
    fn loads_token_from_file_when_token_is_none() {
        let temp_dir = tempfile::tempdir().unwrap();
        let token_path = temp_dir.path().join("token.txt");
        std::fs::write(&token_path, "  secret-token-123 \n\n").unwrap();

        let mut cli =
            Cli::try_parse_from(["gpt2omo", "--token-file", token_path.to_str().unwrap()]).unwrap();
        assert_eq!(cli.token, None);
        assert_eq!(cli.token_file, Some(token_path));

        cli.load_token_file().unwrap();
        assert_eq!(cli.token, Some("secret-token-123".to_string()));
    }

    #[test]
    fn direct_token_takes_precedence_over_token_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let token_path = temp_dir.path().join("token.txt");
        std::fs::write(&token_path, "file-token").unwrap();

        let mut cli = Cli::try_parse_from([
            "gpt2omo",
            "--token",
            "cli-token",
            "--token-file",
            token_path.to_str().unwrap(),
        ])
        .unwrap();
        assert_eq!(cli.token, Some("cli-token".to_string()));

        cli.load_token_file().unwrap();
        assert_eq!(cli.token, Some("cli-token".to_string()));
    }

    #[test]
    fn read_only_flag_parsed() {
        let cli_default = Cli::try_parse_from(["gpt2omo"]).unwrap();
        assert!(!cli_default.read_only);
        let cli = Cli::try_parse_from(["gpt2omo", "--read-only"]).unwrap();
        assert!(cli.read_only);
    }

    #[test]
    fn remote_bind_requires_effective_bearer_and_read_only_mode() {
        let remote: std::net::SocketAddr = "0.0.0.0:18800".parse().unwrap();
        let local: std::net::SocketAddr = "127.0.0.1:18800".parse().unwrap();
        let mut cli = Cli {
            insecure_no_auth: true,
            ..Cli::default()
        };
        assert!(cli.validate_bind_security(&local).is_ok());
        assert!(cli.validate_bind_security(&remote).is_err());

        cli.token = Some("configured".into());
        assert!(cli.validate_bind_security(&remote).is_err());

        cli.read_only = true;
        assert!(cli.validate_bind_security(&remote).is_ok());
    }

    #[test]
    fn allow_arbitrary_commands_flag_parsed() {
        let cli_default = Cli::try_parse_from(["gpt2omo"]).unwrap();
        assert!(!cli_default.allow_arbitrary_commands);

        let cli_flag = Cli::try_parse_from(["gpt2omo", "--allow-arbitrary-commands"]).unwrap();
        assert!(cli_flag.allow_arbitrary_commands);
    }

    #[test]
    fn ensure_auth_generates_token_and_sets_mode_0600() {
        let temp_dir = tempfile::tempdir().unwrap();

        let mut cli = Cli {
            insecure_no_auth: false,
            token: None,
            token_file: None,
            ..Cli::default()
        };

        let token_path = cli
            .ensure_auth_in(temp_dir.path())
            .unwrap()
            .expect("should return token path");
        assert_eq!(token_path, temp_dir.path().join("token"));
        assert!(token_path.exists());

        let token = cli.token.expect("token should be generated");
        assert_eq!(token.len(), 64); // 32 bytes in hex is 64 hex characters
        assert!(hex::decode(&token).is_ok());

        let saved = std::fs::read_to_string(&token_path).unwrap();
        assert_eq!(saved.trim(), token);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&token_path).unwrap();
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn ensure_auth_insecure_no_auth_leaves_token_none() {
        let mut cli = Cli {
            insecure_no_auth: true,
            token: None,
            token_file: None,
            ..Cli::default()
        };

        let token_path = cli.ensure_auth().unwrap();
        assert!(token_path.is_none());
        assert!(cli.token.is_none());
    }

    #[test]
    fn ensure_auth_respects_existing_token() {
        let mut cli = Cli {
            insecure_no_auth: false,
            token: Some("preconfigured-token".to_string()),
            token_file: None,
            ..Cli::default()
        };

        let token_path = cli.ensure_auth().unwrap();
        assert!(token_path.is_none());
        assert_eq!(cli.token.as_deref(), Some("preconfigured-token"));
    }
}
