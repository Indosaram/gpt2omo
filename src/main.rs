use clap::Parser;
use omo_bridge::{create_router, AppState, Cli, EventBus, Workspace};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();
    let ws = Workspace::open(&cli.workspace)?;
    info!("Mounted workspace at: {}", ws.root().display());

    let events = Arc::new(EventBus::new(ws.root().to_string_lossy().to_string()));
    let state = AppState {
        workspace: Arc::new(ws),
        cli: Arc::new(cli.clone()),
        events,
    };

    let router = create_router(state);
    let addr: SocketAddr = cli.bind.parse()?;
    info!("omo-bridge listening on http://{}", addr);
    info!("event stream available at http://{}/events", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
