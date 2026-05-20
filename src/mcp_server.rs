use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use rmcp::handler::server::wrapper::Parameters;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::state::{AppState, InteractionKind, Role, SseEvent, SseEventKind};
use crate::tunnel;

#[derive(Clone)]
pub struct ClaudeCastServer {
    pub state: AppState,
}

#[derive(Serialize, Deserialize, JsonSchema)]
pub struct BroadcastParams {
    /// "user" or "assistant"
    pub role: String,
    /// Full message text to push to the viewer feed
    pub text: String,
}

#[tool_router]
impl ClaudeCastServer {
    #[tool(description = "Start a live claudecast session. Spawns a Cloudflare tunnel and returns the public URL plus convention instructions for broadcasting. Requires cloudflared to be installed.")]
    async fn cast_start(&self) -> String {
        {
            let s = self.state.lock().unwrap();
            if s.active {
                return "Cast already active. Call cast_stop first.".to_string();
            }
        }

        match tunnel::start_tunnel().await {
            Ok((url, child)) => {
                {
                    let mut s = self.state.lock().unwrap();
                    s.active = true;
                    s.session_id = Some(uuid::Uuid::new_v4().to_string());
                    s.public_url = Some(url.clone());
                    s.tunnel_child = Some(child);
                }
                format!(
                    "[claudecast active — public URL: {url}]\n\
                     Share this URL with your audience.\n\n\
                     Convention (follow for every exchange):\n\
                     - Before processing each user message, call broadcast_message with role=\"user\" and the user's exact message.\n\
                     - After each of your responses, call broadcast_message with role=\"assistant\" and your full response text."
                )
            }
            Err(e) => format!("Error starting tunnel: {e}"),
        }
    }

    #[tool(description = "Stop the live cast session, kill the Cloudflare tunnel, and disconnect all viewers.")]
    async fn cast_stop(&self) -> String {
        let mut s = self.state.lock().unwrap();
        if !s.active {
            return "No active session to stop.".to_string();
        }
        if let Some(mut child) = s.tunnel_child.take() {
            let _ = child.start_kill();
        }
        let _ = s.tx.send(SseEvent {
            kind: SseEventKind::SessionEnded,
            role: None,
            text: None,
        });
        s.active = false;
        s.public_url = None;
        s.session_id = None;
        s.feed.clear();
        "Session stopped. Viewers have been disconnected.".to_string()
    }

    #[tool(description = "Push a message into the viewer feed. Call this with role=\"user\" before each user message, and with role=\"assistant\" after each of your responses.")]
    async fn broadcast_message(&self, Parameters(p): Parameters<BroadcastParams>) -> String {
        let mut s = self.state.lock().unwrap();
        if !s.active {
            return "No active session — call cast_start first.".to_string();
        }
        let role = match p.role.trim().to_lowercase().as_str() {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            other => return format!("Unknown role '{other}'. Use 'user' or 'assistant'."),
        };
        s.push_message(role, p.text);
        let n = s.active_viewer_count();
        if n == 0 {
            "ok (no viewers connected)".to_string()
        } else {
            format!("ok ({n} viewer{} watching)", if n == 1 { "" } else { "s" })
        }
    }

    #[tool(description = "Get all pending viewer questions and emoji reactions since the last call. Clears the queue on each call.")]
    async fn get_interactions(&self) -> String {
        let mut s = self.state.lock().unwrap();
        if !s.active {
            return "No active session.".to_string();
        }
        let interactions = s.take_interactions();
        if interactions.is_empty() {
            return "No pending viewer interactions.".to_string();
        }
        let lines: Vec<String> = interactions
            .iter()
            .map(|i| match i.kind {
                InteractionKind::Question => {
                    format!("❓ {}", i.text.as_deref().unwrap_or(""))
                }
                InteractionKind::Emoji => {
                    format!("{} (reaction)", i.emoji.as_deref().unwrap_or(""))
                }
            })
            .collect();
        format!("{} viewer interaction(s):\n{}", interactions.len(), lines.join("\n"))
    }
}

#[tool_handler]
impl ServerHandler for ClaudeCastServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::new_app_state;
    use rmcp::{ClientHandler, ServiceExt, model::ClientInfo};

    #[derive(Clone, Default)]
    struct TestClient;
    impl ClientHandler for TestClient {
        fn get_info(&self) -> ClientInfo { ClientInfo::default() }
    }

    #[tokio::test]
    async fn test_broadcast_message_when_inactive_returns_error() {
        let server = ClaudeCastServer { state: new_app_state() };
        let result = server
            .broadcast_message(Parameters(BroadcastParams {
                role: "assistant".to_string(),
                text: "hello".to_string(),
            }))
            .await;
        assert!(result.contains("cast_start"), "expected hint to call cast_start, got: {result}");
    }

    #[tokio::test]
    async fn test_broadcast_message_when_active_pushes_to_feed() {
        let state = new_app_state();
        state.lock().unwrap().active = true;
        let server = ClaudeCastServer { state: state.clone() };
        let result = server
            .broadcast_message(Parameters(BroadcastParams {
                role: "assistant".to_string(),
                text: "hello viewers".to_string(),
            }))
            .await;
        assert!(result.starts_with("ok"), "expected ok, got: {result}");
        assert_eq!(state.lock().unwrap().feed.len(), 1);
        assert_eq!(state.lock().unwrap().feed[0].text, "hello viewers");
    }

    #[tokio::test]
    async fn test_get_interactions_clears_queue() {
        let state = new_app_state();
        {
            let mut s = state.lock().unwrap();
            s.active = true;
            s.add_interaction(crate::state::Interaction {
                kind: InteractionKind::Question,
                text: Some("Why?".to_string()),
                emoji: None,
                timestamp: 0,
            });
        }
        let server = ClaudeCastServer { state: state.clone() };
        let result = server.get_interactions().await;
        assert!(result.contains("Why?"));
        assert!(state.lock().unwrap().pending_interactions.is_empty());
    }

    #[tokio::test]
    async fn test_cast_stop_clears_state() {
        let state = new_app_state();
        {
            let mut s = state.lock().unwrap();
            s.active = true;
            s.session_id = Some("abc".to_string());
            s.feed.push(crate::state::FeedMessage {
                role: Role::User,
                text: "hi".to_string(),
                timestamp: 0,
            });
        }
        let server = ClaudeCastServer { state: state.clone() };
        let result = server.cast_stop().await;
        assert!(result.contains("stopped"));
        let s = state.lock().unwrap();
        assert!(!s.active);
        assert!(s.feed.is_empty());
        assert!(s.session_id.is_none());
    }
}
