use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "omo-bridge",
    version,
    about = "Sandboxed local MCP HTTP/SSE daemon for ChatGPT"
)]
pub struct Cli {
    /// Workspace root directory to mount
    #[arg(long, default_value = ".")]
    pub workspace: PathBuf,

    /// Bind address for HTTP/SSE server
    #[arg(long, default_value = "127.0.0.1:8765")]
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
}
