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
    #[arg(long)]
    pub scope_dir: Option<PathBuf>,

    /// Bind address for HTTP/SSE server
    #[arg(long, default_value = "127.0.0.1:18800")]
    pub bind: String,

    /// Optional bearer token for authentication
    #[arg(long, env = "OMO_BRIDGE_TOKEN")]
    pub token: Option<String>,

    /// Allow repository-controlled build/test commands to execute with host privileges.
    /// Keep disabled unless the daemon is already inside an OS-level sandbox.
    #[arg(long, env = "OMO_BRIDGE_ALLOW_HOST_COMMAND_EXECUTION", default_value_t = false)]
    pub allow_host_command_execution: bool,

    /// Maximum file read size in bytes (default 10MB)
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    pub max_file_bytes: usize,

    /// Maximum command timeout in milliseconds
    #[arg(long, default_value_t = 300_000)]
    pub command_timeout_ms: u64,
}
