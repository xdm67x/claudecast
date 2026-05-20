mod http_server;
mod mcp_server;
mod state;
mod tunnel;

use mcp_server::ClaudeCastServer;
use rmcp::ServiceExt;
use state::new_app_state;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state = new_app_state();

    // Start HTTP server on :3000 in background
    let http_state = state.clone();
    tokio::spawn(async move {
        let app = http_server::router(http_state);
        let listener = tokio::net::TcpListener::bind("0.0.0.0:3000")
            .await
            .expect("Failed to bind :3000");
        axum::serve(listener, app)
            .await
            .expect("HTTP server error");
    });

    // Run MCP server on stdio (blocks until Claude Code disconnects)
    ClaudeCastServer { state }
        .serve(rmcp::transport::stdio())
        .await?
        .waiting()
        .await?;

    Ok(())
}
