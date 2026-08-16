use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "omo-bridge",
    version,
    about = "Sandboxed local MCP HTTP/SSE daemon for ChatGPT"
)]
pub struct Cli {
    /// Broad filesystem mount root. Individual MCP calls are sandboxed by per-delegation scope.
    #[arg(long, default_value = "/")]
    pub mount_root: PathBuf,

    /// Optional directory that stores per-delegation workspace scope leases.
    #[arg(long, env = "OMO_SCOPE_DIR")]
    pub scope_dir: Option<PathBuf>,

    /// Bind address for HTTP/SSE server
    #[arg(long, default_value = "127.0.0.1:18800", env = "OMO_BRIDGE_BIND")]
    pub bind: String,

    /// Optional bearer token for authentication
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    pub token: Option<String>,

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
}
