use anyhow::Result;
use clap::Parser;
use gpt2omo::cli::default_mount_root;
use gpt2omo::orca::OrcaConfig;
use gpt2omo::{
    collect_account_diagnostics, default_bridge_base_dir, default_scope_dir, AccountRouter,
    BrowserInstanceConfig, BrowserPool, LegacyAccountConfig, WorkspaceMux,
};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser, Debug)]
#[command(
    name = "gpt2omo-account-status",
    version,
    about = "Local administrator view of gpt2omo account capacity and browser health"
)]
struct Cli {
    /// Filesystem mount root used by the bridge daemon.
    #[arg(long, default_value_os_t = default_mount_root())]
    mount_root: PathBuf,

    /// Bridge control directory containing accounts.json and account runtime state.
    #[arg(long, env = "OMO_BRIDGE_HOME")]
    bridge_dir: Option<PathBuf>,

    /// Workspace scope directory used by the bridge daemon.
    #[arg(long, env = "OMO_SCOPE_DIR")]
    scope_dir: Option<PathBuf>,

    /// Bridge port used only to derive the default scope directory.
    #[arg(long, default_value_t = 18800)]
    port: u16,

    /// Legacy Orca worktree used when accounts.json is absent or a single account uses fallback mode.
    #[arg(long, default_value = "active")]
    worktree: String,

    /// Orca CLI executable used for legacy single-account health probing.
    #[arg(long, default_value = "orca")]
    orca_bin: String,

    /// Emit compact JSON instead of pretty-printed JSON.
    #[arg(long)]
    compact: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    gpt2omo::load_dotenv_if_present();
    let cli = Cli::parse();
    let bridge_dir = cli.bridge_dir.unwrap_or_else(default_bridge_base_dir);
    let scope_dir = cli.scope_dir.unwrap_or_else(|| default_scope_dir(cli.port));
    let legacy = LegacyAccountConfig {
        browser: BrowserInstanceConfig::legacy(cli.worktree.clone()),
        ..LegacyAccountConfig::default()
    };
    let orca = OrcaConfig::new(cli.worktree, None, cli.orca_bin);
    let router = AccountRouter::new(&bridge_dir, &cli.mount_root, legacy.clone());
    let browsers = BrowserPool::new(&bridge_dir, &cli.mount_root, legacy, orca);
    let mux = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;
    let report = collect_account_diagnostics(&router, &browsers, &mux, epoch_ms()).await?;

    if cli.compact {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&report)?);
    }
    Ok(())
}

fn epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
