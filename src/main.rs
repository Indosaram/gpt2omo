use clap::Parser;
use gpt2omo::{
    create_router, default_bridge_base_dir, default_scope_dir, AppState, Cli, EventBus,
    WorkspaceMux,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    gpt2omo::load_dotenv_if_present();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let mut cli = Cli::parse();
    cli.ensure_auth()?;
    let addr: SocketAddr = cli.bind.parse()?;
    cli.validate_bind_security(&addr)
        .map_err(anyhow::Error::msg)?;
    let scope_dir = cli
        .scope_dir
        .clone()
        .unwrap_or_else(|| default_scope_dir(addr.port()));
    let workspaces = WorkspaceMux::new(&cli.mount_root, &scope_dir)?;

    info!(
        "Mounted filesystem root at: {}",
        workspaces.mount_root().display()
    );
    info!("Workspace scope directory: {}", scope_dir.display());
    info!("Workspace mode: multiplexed_scopes");
    if cli.read_only {
        info!(
            "Read-only shared connector policy enabled: patch_file, run_command, and cancel_command are not exposed"
        );
    } else {
        warn!(
            "Command execution is daemon-owned but not OS-sandboxed. Treat every holder of a live scope_id in a shared ChatGPT account as one trust principal; writable mode is loopback-only."
        );
    }

    let events = Arc::new(EventBus::new_persistent(
        workspaces.mount_root().to_string_lossy().to_string(),
        default_bridge_base_dir().join("pending-continuations"),
    ));
    let state = AppState {
        workspace: Arc::new(workspaces),
        cli: Arc::new(cli.clone()),
        events,
        commands: Arc::new(gpt2omo::tools::CommandManager::with_allow_arbitrary(
            cli.allow_arbitrary_commands,
        )),
    };

    let router = create_router(state);
    info!("gpt2omo listening on http://{}", addr);
    info!("event stream available at http://{}/events", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
