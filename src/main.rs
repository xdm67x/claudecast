mod http_server;
mod install;
mod mcp_server;
mod state;
mod tunnel;

use mcp_server::ClaudeCastServer;
use rmcp::ServiceExt;
use state::new_app_state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--install-hooks") {
        return install::install_hooks();
    }

    let state = new_app_state();

    let http_state = state.clone();
    let app = http_server::router(http_state);
    // Bind eagerly so port conflicts fail fast and propagate to the caller (Claude Code),
    // instead of silently dying inside a spawned task while the MCP server keeps running.
    // On AddrInUse, kill the stale claudecast process and retry once — this happens when
    // Claude Code reconnects and re-launches the binary while the prior instance is still up.
    let listener = match tokio::net::TcpListener::bind("0.0.0.0:3000").await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let _ = std::process::Command::new("sh")
                .args(["-c", "lsof -ti tcp:3000 | xargs kill -9 2>/dev/null"])
                .output();
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            tokio::net::TcpListener::bind("0.0.0.0:3000").await?
        }
        Err(e) => return Err(e.into()),
    };

    let mcp_service = ClaudeCastServer { state }
        .serve(rmcp::transport::stdio())
        .await?;

    tokio::select! {
        result = axum::serve(listener, app) => { result?; }
        result = mcp_service.waiting() => { result?; }
    }

    Ok(())
}
