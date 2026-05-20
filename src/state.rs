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
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Question,
    Emoji,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interaction {
    pub kind: InteractionKind,
    pub text: Option<String>,
    pub emoji: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SseEventKind {
    Message,
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
}

pub struct CastState {
    pub active: bool,
    pub session_id: Option<String>,
    pub public_url: Option<String>,
    pub viewer_count: usize,
    pub feed: Vec<FeedMessage>,
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
            feed: Vec::new(),
            pending_interactions: Vec::new(),
            tx,
            tunnel_child: None,
        }
    }

    pub fn push_message(&mut self, role: Role, text: String) {
        let ts = now_secs();
        let role_str = match role {
            Role::User => "user",
            Role::Assistant => "assistant",
        }
        .to_string();
        self.feed.push(FeedMessage { role, text: text.clone(), timestamp: ts });
        let _ = self.tx.send(SseEvent {
            kind: SseEventKind::Message,
            role: Some(role_str),
            text: Some(text),
        });
    }

    pub fn add_interaction(&mut self, interaction: Interaction) {
        self.pending_interactions.push(interaction);
    }

    pub fn take_interactions(&mut self) -> Vec<Interaction> {
        std::mem::take(&mut self.pending_interactions)
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
        assert!(matches!(s.feed[0].role, Role::Assistant));
        assert_eq!(s.feed[0].text, "hello");
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
    fn test_take_interactions_clears_queue() {
        let mut s = CastState::new();
        s.add_interaction(Interaction {
            kind: InteractionKind::Question,
            text: Some("Why?".to_string()),
            emoji: None,
            timestamp: 0,
        });
        let taken = s.take_interactions();
        assert_eq!(taken.len(), 1);
        assert!(s.pending_interactions.is_empty());
    }
}
