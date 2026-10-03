use std::{net::SocketAddr, path::PathBuf};
use tower_http::services::ServeDir;
use visualization::{
    api::{load_sources, router},
    dto::SourcesConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let config = args
        .next()
        .ok_or("用法：visualization <sources.json> [127.0.0.1:4317] [ui/dist]")?;
    let bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:4317".into())
        .parse()?;
    let ui = PathBuf::from(
        args.next()
            .unwrap_or_else(|| "visualization/ui/dist".into()),
    );
    let config: SourcesConfig = serde_json::from_slice(&std::fs::read(config)?)?;
    let state = load_sources(config)?;
    let app = router(state).fallback_service(ServeDir::new(ui));
    let listener = tokio::net::TcpListener::bind(bind).await?;
    eprintln!(
        "只读浏览器：http://{}；来源固定于启动时，公共页沿稳定前缀顺序推进",
        listener.local_addr()?
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
