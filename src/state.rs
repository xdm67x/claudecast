use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

pub type AppState = Arc<Mutex<CastState>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedMessage {
    pub role: Role,
    pub text: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallEntry {
    pub name: String,
    pub input: serde_json::Value,
    pub output: String,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "entry_type", rename_all = "snake_case")]
pub enum FeedEntry {
    Message(FeedMessage),
    ToolCall(ToolCallEntry),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Question,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interaction {
    pub kind: InteractionKind,
    pub text: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SseEventKind {
    Message,
    ToolCall,
    SessionEnded,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseEvent {
    #[serde(rename = "type")]
    pub kind: SseEventKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}

pub struct CastState {
    pub active: bool,
    pub session_id: Option<String>,
    pub public_url: Option<String>,
    pub viewer_count: usize,
    pub viewer_sessions: HashMap<String, u64>,
    pub feed: Vec<FeedEntry>,
    pub pending_interactions: Vec<Interaction>,
    pub tx: broadcast::Sender<SseEvent>,
    pub tunnel_child: Option<tokio::process::Child>,
}

impl CastState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            active: false,
            session_id: None,
            public_url: None,
            viewer_count: 0,
            viewer_sessions: HashMap::new(),
            feed: Vec::new(),
            pending_interactions: Vec::new(),
            tx,
            tunnel_child: None,
        }
    }

    pub fn touch_viewer(&mut self, id: &str) {
        self.viewer_sessions.insert(id.to_string(), now_secs());
    }

    pub fn active_viewer_count(&self) -> usize {
        let now = now_secs();
        self.viewer_sessions.values().filter(|&&t| now.saturating_sub(t) < 60).count()
    }

    pub fn push_message(&mut self, role: Role, text: String) -> usize {
        let ts = now_secs();
        let role_str = match role {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
        .to_string();
        self.feed.push(FeedEntry::Message(FeedMessage { role, text: text.clone(), timestamp: ts }));
        match self.tx.send(SseEvent {
            kind: SseEventKind::Message,
            role: Some(role_str),
            text: Some(text),
            name: None,
            input: None,
            output: None,
        }) {
            Ok(n) => n,
            Err(_) => 0,
        }
    }

    pub fn push_tool_call(&mut self, name: String, input: serde_json::Value, output: String) {
        let ts = now_secs();
        self.feed.push(FeedEntry::ToolCall(ToolCallEntry {
            name: name.clone(),
            input: input.clone(),
            output: output.clone(),
            timestamp: ts,
        }));
        let _ = self.tx.send(SseEvent {
            kind: SseEventKind::ToolCall,
            role: None,
            text: None,
            name: Some(name),
            input: Some(input),
            output: Some(output),
        });
    }

    pub fn add_interaction(&mut self, interaction: Interaction) {
        self.pending_interactions.push(interaction);
        if self.pending_interactions.len() > 200 {
            self.pending_interactions.remove(0);
        }
    }

    pub fn take_interactions(&mut self) -> Vec<Interaction> {
        std::mem::take(&mut self.pending_interactions)
    }

    pub fn take_questions(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_interactions)
            .into_iter()
            .filter_map(|i| i.text)
            .collect()
    }
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn new_app_state() -> AppState {
    Arc::new(Mutex::new(CastState::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_message_adds_to_feed() {
        let mut s = CastState::new();
        s.push_message(Role::Assistant, "hello".to_string());
        assert_eq!(s.feed.len(), 1);
        if let FeedEntry::Message(msg) = &s.feed[0] {
            assert!(matches!(msg.role, Role::Assistant));
            assert_eq!(msg.text, "hello");
        } else {
            panic!("expected FeedEntry::Message");
        }
    }

    #[test]
    fn test_push_message_broadcasts_event() {
        let mut s = CastState::new();
        let mut rx = s.tx.subscribe();
        s.push_message(Role::User, "hi".to_string());
        let event = rx.try_recv().unwrap();
        assert!(matches!(event.kind, SseEventKind::Message));
        assert_eq!(event.role.as_deref(), Some("user"));
        assert_eq!(event.text.as_deref(), Some("hi"));
    }

    #[test]
    fn test_push_tool_call_adds_to_feed() {
        let mut s = CastState::new();
        s.push_tool_call("bash".to_string(), serde_json::json!({"command": "ls"}), "main.rs".to_string());
        assert_eq!(s.feed.len(), 1);
        if let FeedEntry::ToolCall(tc) = &s.feed[0] {
            assert_eq!(tc.name, "bash");
            assert_eq!(tc.output, "main.rs");
        } else {
            panic!("expected FeedEntry::ToolCall");
        }
    }

    #[test]
    fn test_push_tool_call_broadcasts_event() {
        let mut s = CastState::new();
        let mut rx = s.tx.subscribe();
        s.push_tool_call("bash".to_string(), serde_json::json!({}), "out".to_string());
        let event = rx.try_recv().unwrap();
        assert!(matches!(event.kind, SseEventKind::ToolCall));
        assert_eq!(event.name.as_deref(), Some("bash"));
        assert_eq!(event.output.as_deref(), Some("out"));
    }

    #[test]
    fn test_take_interactions_clears_queue() {
        let mut s = CastState::new();
        s.add_interaction(Interaction {
            kind: InteractionKind::Question,
            text: Some("Why?".to_string()),
            timestamp: 0,
        });
        let taken = s.take_interactions();
        assert_eq!(taken.len(), 1);
        assert!(s.pending_interactions.is_empty());
    }

    #[test]
    fn test_take_questions_returns_text_only() {
        let mut s = CastState::new();
        s.add_interaction(Interaction {
            kind: InteractionKind::Question,
            text: Some("Why Rust?".to_string()),
            timestamp: 0,
        });
        s.add_interaction(Interaction {
            kind: InteractionKind::Question,
            text: None,
            timestamp: 0,
        });
        let qs = s.take_questions();
        assert_eq!(qs, vec!["Why Rust?"]);
        assert!(s.pending_interactions.is_empty());
    }
}
