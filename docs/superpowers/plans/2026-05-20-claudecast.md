# claudecast Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Single Rust binary that lets a Claude Code user broadcast their session live via 4 MCP tools; viewers watch in a browser via SSE + emoji/question interaction; public access via Cloudflare tunnel.

**Architecture:** Two async servers (MCP stdio + Axum HTTP :3000) run concurrently in one Tokio runtime via `tokio::spawn`, sharing state through `Arc<Mutex<CastState>>`. A `tokio::sync::broadcast::channel` fans out feed messages to all SSE clients. Cloudflare tunnel (`cloudflared`) is stored as `Option<tokio::process::Child>` in state.

**Tech Stack:** Rust 2024, Tokio 1, Axum 0.8, rmcp 1.7, serde/serde_json 1, tokio-stream 0.1, pin-project 1, uuid 1

---

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | All dependencies |
| `src/main.rs` | Entry point: starts HTTP in background, runs MCP server on stdio |
| `src/state.rs` | `CastState`, `FeedMessage`, `Interaction`, `SseEvent`, `AppState` |
| `src/http_server.rs` | Axum router + all HTTP handlers (`/`, `/feed`, `/interact`, `/status`) |
| `src/mcp_server.rs` | `ClaudeCastServer` with `#[tool_router]` + `#[tool_handler]` |
| `src/tunnel.rs` | Spawn `cloudflared`, parse URL from stderr |
| `assets/viewer.html` | Viewer UI embedded at compile time via `include_str!` |

---

### Task 1: Cargo dependencies

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Replace Cargo.toml**

```toml
[package]
name = "claudecast"
version = "0.1.0"
edition = "2024"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = "0.8"
rmcp = { version = "1.7", features = ["server", "transport-io", "macros"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio-stream = { version = "0.1", features = ["sync"] }
futures = "0.3"
pin-project = "1"
uuid = { version = "1", features = ["v4"] }

[dev-dependencies]
tower = { version = "0.5", features = ["util"] }
http-body-util = "0.1"
rmcp = { version = "1.7", features = ["server", "client", "transport-io", "macros"] }
```

- [ ] **Step 2: Verify dependencies resolve and compile**

```bash
cargo build
```

Expected: build succeeds (main.rs still has `println!("Hello, world!")`).

- [ ] **Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add all dependencies"
```

---

### Task 2: Shared state types

**Files:**
- Create: `src/state.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Create src/state.rs**

```rust
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
```

- [ ] **Step 2: Replace src/main.rs**

```rust
mod state;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test state
```

Expected:
```
test state::tests::test_push_message_adds_to_feed ... ok
test state::tests::test_push_message_broadcasts_event ... ok
test state::tests::test_take_interactions_clears_queue ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/state.rs src/main.rs
git commit -m "feat: add shared CastState types"
```

---

### Task 3: HTTP server — /status and /interact

**Files:**
- Create: `src/http_server.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Create src/http_server.rs**

```rust
use axum::{
    Router,
    extract::{Json, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};

use crate::state::{AppState, Interaction, InteractionKind, now_secs};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_handler))
        .route("/status", get(status_handler))
        .route("/feed", get(feed_handler))
        .route("/interact", post(interact_handler))
        .with_state(state)
}

// --- GET / ---

const VIEWER_HTML: &str = include_str!("../assets/viewer.html");

async fn ui_handler() -> axum::response::Html<&'static str> {
    axum::response::Html(VIEWER_HTML)
}

// --- GET /status ---

#[derive(Serialize)]
struct StatusResponse {
    viewer_count: usize,
    active: bool,
    session_id: Option<String>,
    public_url: Option<String>,
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let s = state.lock().unwrap();
    Json(StatusResponse {
        viewer_count: s.viewer_count,
        active: s.active,
        session_id: s.session_id.clone(),
        public_url: s.public_url.clone(),
    })
}

// --- POST /interact ---

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractPayload {
    Question { text: String },
    Emoji { emoji: String },
}

async fn interact_handler(
    State(state): State<AppState>,
    Json(payload): Json<InteractPayload>,
) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    if !s.active {
        return (StatusCode::SERVICE_UNAVAILABLE, "No active session").into_response();
    }
    let ts = now_secs();
    let interaction = match payload {
        InteractPayload::Question { text } => Interaction {
            kind: InteractionKind::Question,
            text: Some(text),
            emoji: None,
            timestamp: ts,
        },
        InteractPayload::Emoji { emoji } => Interaction {
            kind: InteractionKind::Emoji,
            text: None,
            emoji: Some(emoji),
            timestamp: ts,
        },
    };
    s.add_interaction(interaction);
    StatusCode::OK.into_response()
}

// --- GET /feed (placeholder — implemented in Task 4) ---

async fn feed_handler(State(_state): State<AppState>) -> impl IntoResponse {
    StatusCode::NOT_IMPLEMENTED
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::new_app_state;
    use axum::body::Body;
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_status_returns_ok_and_defaults() {
        let app = router(new_app_state());
        let resp = app
            .oneshot(Request::get("/status").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["viewer_count"], 0);
        assert_eq!(json["active"], false);
    }

    #[tokio::test]
    async fn test_interact_returns_503_when_inactive() {
        let app = router(new_app_state());
        let body = serde_json::json!({"type": "emoji", "emoji": "🔥"}).to_string();
        let resp = app
            .oneshot(
                Request::post("/interact")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_interact_adds_question_when_active() {
        let state = new_app_state();
        state.lock().unwrap().active = true;
        let app = router(state.clone());
        let body = serde_json::json!({"type": "question", "text": "Why Rust?"}).to_string();
        let resp = app
            .oneshot(
                Request::post("/interact")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.lock().unwrap().pending_interactions.len(), 1);
    }

    #[tokio::test]
    async fn test_interact_adds_emoji_when_active() {
        let state = new_app_state();
        state.lock().unwrap().active = true;
        let app = router(state.clone());
        let body = serde_json::json!({"type": "emoji", "emoji": "🔥"}).to_string();
        let resp = app
            .oneshot(
                Request::post("/interact")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let s = state.lock().unwrap();
        assert!(matches!(s.pending_interactions[0].kind, InteractionKind::Emoji));
    }
}
```

- [ ] **Step 2: Add modules to main.rs**

```rust
mod state;
mod http_server;

fn main() {
    println!("Hello, world!");
}
```

Note: `assets/viewer.html` must exist before this compiles (it's referenced via `include_str!`). Create a placeholder first:

```bash
mkdir -p assets && echo "<html><body>claudecast</body></html>" > assets/viewer.html
```

- [ ] **Step 3: Run tests**

```bash
cargo test http_server
```

Expected:
```
test http_server::tests::test_status_returns_ok_and_defaults ... ok
test http_server::tests::test_interact_returns_503_when_inactive ... ok
test http_server::tests::test_interact_adds_question_when_active ... ok
test http_server::tests::test_interact_adds_emoji_when_active ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/http_server.rs src/main.rs assets/viewer.html
git commit -m "feat: add /status and /interact HTTP endpoints"
```

---

### Task 4: SSE /feed endpoint

**Files:**
- Modify: `src/http_server.rs` (replace placeholder `feed_handler`)

- [ ] **Step 1: Add SSE imports at the top of http_server.rs**

Add these imports (merge with existing `use` statements):

```rust
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use pin_project::pin_project;
use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};
use tokio_stream::{StreamExt as _, wrappers::BroadcastStream};
use crate::state::{Role, SseEvent, SseEventKind};
```

- [ ] **Step 2: Replace the placeholder feed_handler with the full implementation**

Remove the placeholder `feed_handler` and add:

```rust
// Wraps a stream and decrements viewer_count when dropped (client disconnects).
#[pin_project(PinnedDrop)]
struct ViewerStream<S> {
    #[pin]
    inner: S,
    state: AppState,
}

#[pin_project::pinned_drop]
impl<S> PinnedDrop for ViewerStream<S> {
    fn drop(self: Pin<&mut Self>) {
        if let Ok(mut s) = self.state.lock() {
            s.viewer_count = s.viewer_count.saturating_sub(1);
        }
    }
}

impl<S: Stream> Stream for ViewerStream<S> {
    type Item = S::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

fn msg_to_event(msg: &crate::state::FeedMessage) -> Event {
    let role_str = match msg.role {
        Role::User => "user",
        Role::Assistant => "assistant",
    };
    let payload = SseEvent {
        kind: SseEventKind::Message,
        role: Some(role_str.to_string()),
        text: Some(msg.text.clone()),
    };
    Event::default().data(serde_json::to_string(&payload).unwrap())
}

async fn feed_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (history, rx) = {
        let mut s = state.lock().unwrap();
        s.viewer_count += 1;
        (s.feed.clone(), s.tx.subscribe())
    };

    let history_stream = tokio_stream::iter(history)
        .map(|msg| Ok::<Event, Infallible>(msg_to_event(&msg)));

    let live_stream = BroadcastStream::new(rx).filter_map(|r| match r {
        Ok(event) => {
            Some(Ok(Event::default().data(serde_json::to_string(&event).unwrap())))
        }
        Err(_) => None,
    });

    let combined = ViewerStream {
        inner: history_stream.chain(live_stream),
        state,
    };

    Sse::new(combined).keep_alive(KeepAlive::default())
}
```

- [ ] **Step 3: Write SSE test**

Add to the `tests` module:

```rust
#[tokio::test]
async fn test_feed_returns_sse_content_type() {
    let app = router(new_app_state());
    let resp = app
        .oneshot(Request::get("/feed").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("text/event-stream"), "got: {ct}");
}

#[tokio::test]
async fn test_feed_increments_viewer_count() {
    let state = new_app_state();
    // Simulate what the handler does on connect
    state.lock().unwrap().viewer_count += 1;
    assert_eq!(state.lock().unwrap().viewer_count, 1);
}
```

- [ ] **Step 4: Run all tests**

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/http_server.rs
git commit -m "feat: add SSE /feed with history replay and viewer count drop guard"
```

---

### Task 5: Viewer HTML

**Files:**
- Replace: `assets/viewer.html` (the placeholder from Task 3)

- [ ] **Step 1: Replace assets/viewer.html with the full viewer UI**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>claudecast</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, -apple-system, sans-serif; background: #0d1117; color: #e6edf3; height: 100dvh; display: flex; flex-direction: column; }
    #header { padding: 12px 20px; border-bottom: 1px solid #21262d; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
    #header h1 { font-size: 15px; font-weight: 600; letter-spacing: 0.02em; }
    #viewer-count { font-size: 13px; color: #8b949e; }
    #feed { flex: 1; overflow-y: auto; padding: 16px 20px; display: flex; flex-direction: column; gap: 10px; }
    .message { max-width: 85%; }
    .message.user { align-self: flex-end; }
    .message.assistant { align-self: flex-start; }
    .label { font-size: 11px; color: #8b949e; margin-bottom: 3px; }
    .bubble { padding: 10px 14px; border-radius: 10px; font-size: 14px; line-height: 1.55; white-space: pre-wrap; word-break: break-word; }
    .user .bubble { background: #1f6feb; border-bottom-right-radius: 3px; }
    .assistant .bubble { background: #21262d; border-bottom-left-radius: 3px; }
    #bottom-bar { padding: 10px 16px; border-top: 1px solid #21262d; display: flex; gap: 6px; align-items: center; flex-shrink: 0; }
    .emoji-btn { background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 6px 10px; cursor: pointer; font-size: 15px; display: flex; align-items: center; gap: 5px; color: #e6edf3; transition: background 0.15s; flex-shrink: 0; }
    .emoji-btn:hover { background: #30363d; }
    .emoji-count { font-size: 11px; color: #8b949e; min-width: 12px; }
    #question-input { flex: 1; background: #21262d; border: 1px solid #30363d; border-radius: 8px; padding: 8px 12px; color: #e6edf3; font-size: 14px; outline: none; min-width: 0; }
    #question-input:focus { border-color: #388bfd; }
    #send-btn { background: #1f6feb; border: none; border-radius: 8px; padding: 8px 14px; color: #fff; cursor: pointer; font-size: 14px; font-weight: 500; flex-shrink: 0; }
    #send-btn:hover { background: #388bfd; }
    #ended-banner { display: none; background: #3d1a1a; border: 1px solid #7d2020; border-radius: 8px; padding: 10px 14px; font-size: 13px; color: #f87171; margin: 12px 20px 0; }
  </style>
</head>
<body>
  <div id="header">
    <h1>claudecast</h1>
    <span id="viewer-count">● connecting…</span>
  </div>
  <div id="ended-banner">Session ended by the caster.</div>
  <div id="feed"></div>
  <div id="bottom-bar">
    <button class="emoji-btn" onclick="sendEmoji('👍')" title="Thumbs up">👍 <span class="emoji-count" id="c-thumbsup">0</span></button>
    <button class="emoji-btn" onclick="sendEmoji('🔥')" title="Fire">🔥 <span class="emoji-count" id="c-fire">0</span></button>
    <button class="emoji-btn" onclick="sendEmoji('❓')" title="Question">❓ <span class="emoji-count" id="c-question">0</span></button>
    <button class="emoji-btn" onclick="sendEmoji('😮')" title="Wow">😮 <span class="emoji-count" id="c-wow">0</span></button>
    <input id="question-input" type="text" placeholder="Ask a question…" maxlength="280">
    <button id="send-btn" onclick="sendQuestion()">Send</button>
  </div>
  <script>
    const EMOJI_IDS = { '👍': 'thumbsup', '🔥': 'fire', '❓': 'question', '😮': 'wow' };
    const counts = { '👍': 0, '🔥': 0, '❓': 0, '😮': 0 };

    function escapeHtml(t) {
      return t.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
    function appendMessage(role, text) {
      const feed = document.getElementById('feed');
      const div = document.createElement('div');
      div.className = 'message ' + role;
      div.innerHTML = '<div class="label">' + (role === 'user' ? '👤 User' : '🤖 Claude') + '</div>'
        + '<div class="bubble">' + escapeHtml(text) + '</div>';
      feed.appendChild(div);
      feed.scrollTop = feed.scrollHeight;
    }
    function sendEmoji(emoji) {
      fetch('/interact', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'emoji', emoji }) });
      counts[emoji]++;
      document.getElementById('c-' + EMOJI_IDS[emoji]).textContent = counts[emoji];
    }
    function sendQuestion() {
      const input = document.getElementById('question-input');
      const text = input.value.trim();
      if (!text) return;
      fetch('/interact', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'question', text }) });
      input.value = '';
    }
    document.getElementById('question-input').addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendQuestion(); }
    });
    const es = new EventSource('/feed');
    es.onmessage = e => {
      const msg = JSON.parse(e.data);
      if (msg.type === 'session_ended') {
        document.getElementById('ended-banner').style.display = 'block';
        document.getElementById('question-input').disabled = true;
        document.getElementById('send-btn').disabled = true;
        es.close();
        return;
      }
      if (msg.role && msg.text) appendMessage(msg.role, msg.text);
    };
    es.onerror = () => {
      document.getElementById('viewer-count').textContent = '● disconnected';
    };
    function updateStatus() {
      fetch('/status').then(r => r.json()).then(d => {
        const n = d.viewer_count;
        document.getElementById('viewer-count').textContent =
          '● ' + n + ' viewer' + (n !== 1 ? 's' : '');
      }).catch(() => {});
    }
    updateStatus();
    setInterval(updateStatus, 30000);
  </script>
</body>
</html>
```

- [ ] **Step 2: Run the root endpoint test to verify HTML is embedded correctly**

```bash
cargo test test_root_returns_viewer_html
```

Add this test to `http_server.rs` tests module first:

```rust
#[tokio::test]
async fn test_root_returns_viewer_html() {
    let app = router(new_app_state());
    let resp = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("text/html"));
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(std::str::from_utf8(&body).unwrap().contains("claudecast"));
}
```

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add assets/viewer.html src/http_server.rs
git commit -m "feat: add viewer HTML UI"
```

---

### Task 6: Cloudflare tunnel

**Files:**
- Create: `src/tunnel.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Create src/tunnel.rs**

```rust
use std::process::Stdio;
use tokio::io::AsyncBufReadExt;
use tokio::process::Command;

/// Spawns `cloudflared tunnel --url http://localhost:3000`, reads its stderr until
/// a trycloudflare.com URL appears, then returns `(url, child_process)`.
/// The caller is responsible for killing the child when done.
pub async fn start_tunnel() -> Result<(String, tokio::process::Child), String> {
    let mut child = Command::new("cloudflared")
        .args(["tunnel", "--url", "http://localhost:3000"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            format!(
                "Failed to spawn cloudflared: {e}. Install with: brew install cloudflare/cloudflare/cloudflared"
            )
        })?;

    let stderr = child.stderr.take().unwrap();
    let mut lines = tokio::io::BufReader::new(stderr).lines();

    while let Some(line) = lines.next_line().await.map_err(|e| e.to_string())? {
        if let Some(url) = extract_tunnel_url(&line) {
            // Drain stderr in background to prevent pipe buffer blocking.
            tokio::spawn(async move {
                while let Ok(Some(_)) = lines.next_line().await {}
            });
            return Ok((url, child));
        }
    }

    Err("cloudflared exited before providing a tunnel URL".to_string())
}

/// Extracts a `https://*.trycloudflare.com` URL from a cloudflared log line.
pub fn extract_tunnel_url(line: &str) -> Option<String> {
    line.split_whitespace()
        .find(|token| token.starts_with("https://") && token.contains("trycloudflare.com"))
        .map(|s| {
            // Strip trailing punctuation (pipes, commas) that may appear in table-format logs
            s.trim_end_matches(|c: char| !c.is_alphanumeric() && c != '/')
                .to_string()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_url_from_prose_log() {
        let line = "Your quick Tunnel has been created! Visit it at: https://abc-def-123.trycloudflare.com";
        assert_eq!(
            extract_tunnel_url(line),
            Some("https://abc-def-123.trycloudflare.com".to_string())
        );
    }

    #[test]
    fn test_extract_url_from_table_log() {
        let line = "| https://xyz-awesome-tunnel.trycloudflare.com |";
        assert_eq!(
            extract_tunnel_url(line),
            Some("https://xyz-awesome-tunnel.trycloudflare.com".to_string())
        );
    }

    #[test]
    fn test_no_url_in_unrelated_line() {
        let line = "Connecting to Cloudflare region us-east-1";
        assert_eq!(extract_tunnel_url(line), None);
    }
}
```

- [ ] **Step 2: Add module to main.rs**

```rust
mod state;
mod http_server;
mod tunnel;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 3: Run tunnel tests**

```bash
cargo test tunnel
```

Expected:
```
test tunnel::tests::test_extract_url_from_prose_log ... ok
test tunnel::tests::test_extract_url_from_table_log ... ok
test tunnel::tests::test_no_url_in_unrelated_line ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/tunnel.rs src/main.rs
git commit -m "feat: add cloudflared tunnel management"
```

---

### Task 7: MCP server tools

**Files:**
- Create: `src/mcp_server.rs`
- Modify: `src/main.rs`

- [ ] **Step 1: Create src/mcp_server.rs**

```rust
use rmcp::{ServerHandler, tool_handler, tool_router};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::state::{AppState, Interaction, InteractionKind, Role, SseEvent, SseEventKind};
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
                if let Some(ref url) = s.public_url {
                    return format!("[claudecast already active — {url}]");
                }
            }
        }

        {
            let mut s = self.state.lock().unwrap();
            s.active = true;
            s.session_id = Some(uuid::Uuid::new_v4().to_string());
        }

        match tunnel::start_tunnel().await {
            Ok((url, child)) => {
                {
                    let mut s = self.state.lock().unwrap();
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
            Err(e) => {
                let mut s = self.state.lock().unwrap();
                s.active = false;
                s.session_id = None;
                format!("Error starting tunnel: {e}")
            }
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
            _ => Role::Assistant,
        };
        s.push_message(role, p.text);
        "ok".to_string()
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
        assert_eq!(result, "ok");
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
```

- [ ] **Step 2: Add module to main.rs**

```rust
mod state;
mod http_server;
mod tunnel;
mod mcp_server;

fn main() {
    println!("Hello, world!");
}
```

- [ ] **Step 3: Run MCP server tests**

```bash
cargo test mcp_server
```

Expected:
```
test mcp_server::tests::test_broadcast_message_when_inactive_returns_error ... ok
test mcp_server::tests::test_broadcast_message_when_active_pushes_to_feed ... ok
test mcp_server::tests::test_get_interactions_clears_queue ... ok
test mcp_server::tests::test_cast_stop_clears_state ... ok
```

- [ ] **Step 4: Run the full test suite**

```bash
cargo test
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/mcp_server.rs src/main.rs
git commit -m "feat: add MCP server with cast_start, cast_stop, broadcast_message, get_interactions tools"
```

---

### Task 8: Main entrypoint

**Files:**
- Modify: `src/main.rs`

- [ ] **Step 1: Replace main.rs with the full async entrypoint**

```rust
mod http_server;
mod mcp_server;
mod state;
mod tunnel;

use mcp_server::ClaudeCastServer;
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
```

- [ ] **Step 2: Build the release binary**

```bash
cargo build --release
```

Expected: `target/release/claudecast` binary produced with no errors.

- [ ] **Step 3: Smoke test — verify MCP responds to initialize**

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}' \
  | ./target/release/claudecast
```

Expected: JSON response containing `"result"` with `"serverInfo"` and `"capabilities"` including `"tools"`.

- [ ] **Step 4: Smoke test — verify HTTP server starts**

Start the binary in background, check the status endpoint:

```bash
./target/release/claudecast &
sleep 1
curl -s http://localhost:3000/status | python3 -m json.tool
kill %1
```

Expected: `{"viewer_count": 0, "active": false, ...}`.

- [ ] **Step 5: Register with Claude Code**

Add to `~/.claude/claude_desktop_config.json` (or via `claude mcp add`):

```json
{
  "mcpServers": {
    "claudecast": {
      "command": "/path/to/claudecast/target/release/claudecast",
      "args": []
    }
  }
}
```

Or use the Claude Code CLI:
```bash
claude mcp add claudecast /absolute/path/to/target/release/claudecast
```

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat: wire MCP and HTTP servers in async main"
```

---

## Spec Coverage Check

| Spec requirement | Implemented in |
|---|---|
| MCP stdio server | Task 7 (`ClaudeCastServer`) + Task 8 (`main.rs`) |
| HTTP server on :3000 | Task 3 (`http_server.rs`) + Task 8 (`main.rs`) |
| `cast_start` tool | Task 7 |
| `cast_stop` tool | Task 7 |
| `broadcast_message` tool | Task 7 |
| `get_interactions` tool | Task 7 |
| Shared state via `Arc<Mutex<CastState>>` | Task 2 |
| `broadcast::channel` SSE fan-out | Task 4 |
| SSE `/feed` with history replay | Task 4 |
| `POST /interact` (question + emoji) | Task 3 |
| `GET /status` | Task 3 |
| Viewer count via drop guard | Task 4 (`ViewerStream`) |
| Session-ended SSE event on stop | Task 7 (`cast_stop`) |
| Viewer HTML embedded via `include_str!` | Task 5 |
| Cloudflare tunnel subprocess | Task 6 + Task 7 (`cast_start`) |
| Convention instructions returned by `cast_start` | Task 7 |
