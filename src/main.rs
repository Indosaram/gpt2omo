use clap::Parser;
use gpt2omo::{create_router, default_scope_dir, AppState, Cli, EventBus, WorkspaceMux};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    gpt2omo::load_dotenv_if_present();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();
    let addr: SocketAddr = cli.bind.parse()?;
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

    let events = Arc::new(EventBus::new(
        workspaces.mount_root().to_string_lossy().to_string(),
    ));
    let state = AppState {
        workspace: Arc::new(workspaces),
        cli: Arc::new(cli.clone()),
        events,
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    };

    let router = create_router(state);
    info!("gpt2omo listening on http://{}", addr);
    info!("event stream available at http://{}/events", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
