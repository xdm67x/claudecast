# Live Interaction + Tool Calls Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add tool call visibility in the viewer feed, remove emoji reactions, auto-surface viewer questions to the caster via Claude Code hooks, and redesign the viewer UI to a light theme with a floating question bar.

**Architecture:** Two new HTTP endpoints (`POST /tool-event`, `GET /pending-questions`) extend the existing Axum server. The `FeedMessage`-only feed becomes a `FeedEntry` enum holding either messages or tool calls, both replayed over SSE. Two `PostToolUse` hooks in `.claude/settings.local.json` pipe Claude's tool activity to the server and surface viewer questions inline.

**Tech Stack:** Rust (Axum, rmcp, tokio, serde_json), vanilla HTML/JS (EventSource SSE), Claude Code hooks (bash + jq + curl)

---

## File Map

| File | Change |
|---|---|
| `src/state.rs` | Add `ToolCall` to `SseEventKind`; add `name/input/output` to `SseEvent`; add `ToolCallEntry`, `FeedEntry`; change `feed` to `Vec<FeedEntry>`; remove `Emoji` from `InteractionKind`; remove `emoji` from `Interaction`; add `push_tool_call()` |
| `src/http_server.rs` | Replace `msg_to_event` with `entry_to_event` handling `FeedEntry`; update `feed_handler` (accept viewer_id, use FeedEntry); update `messages_handler` (filter messages only); add `POST /tool-event`; add `GET /pending-questions`; remove `Emoji` from `InteractPayload` |
| `src/mcp_server.rs` | Fix tests: update `SseEvent` construction in `cast_stop`, update test that accesses `feed[0]` directly, update test that pushes `FeedMessage` directly |
| `assets/viewer.html` | Full rewrite: light theme, SSE EventSource replacing polling, expandable tool call blocks, floating pill question bar |
| `.claude/settings.local.json` | Add `PostToolUse` hooks |
| `.claude/hooks/broadcast-tool-call.sh` | New script: POST tool call data to `/tool-event` |
| `.claude/hooks/surface-viewer-questions.sh` | New script: GET `/pending-questions` after assistant responses |

---

## Task 1: Update state types

**Files:**
- Modify: `src/state.rs`
- Modify: `src/mcp_server.rs` (fix `SseEvent` literal in `cast_stop`)

- [ ] **Step 1: Write failing tests for new state behaviour**

Add these tests to the `#[cfg(test)]` block in `src/state.rs`, replacing the existing `test_push_message_adds_to_feed` and `test_push_message_broadcasts_event` tests:

```rust
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
```

- [ ] **Step 2: Run tests — expect compile errors (types don't exist yet)**

```bash
cargo test 2>&1 | head -40
```

Expected: compile errors about `FeedEntry`, `ToolCall`, `push_tool_call` not existing.

- [ ] **Step 3: Replace the entire `src/state.rs` with the updated types**

```rust
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
```

- [ ] **Step 4: Fix compile errors in `src/mcp_server.rs`**

The `cast_stop` tool sends an `SseEvent` literal that's now missing fields. Find this block in `mcp_server.rs`:

```rust
let _ = s.tx.send(SseEvent {
    kind: SseEventKind::SessionEnded,
    role: None,
    text: None,
});
```

Replace it with:

```rust
let _ = s.tx.send(SseEvent {
    kind: SseEventKind::SessionEnded,
    role: None,
    text: None,
    name: None,
    input: None,
    output: None,
});
```

Also fix the test `test_broadcast_message_when_active_pushes_to_feed` — it accesses `feed[0].text` directly, which no longer works with `FeedEntry`. Replace:

```rust
assert_eq!(state.lock().unwrap().feed.len(), 1);
assert_eq!(state.lock().unwrap().feed[0].text, "hello viewers");
```

With:

```rust
assert_eq!(state.lock().unwrap().feed.len(), 1);
let s = state.lock().unwrap();
if let crate::state::FeedEntry::Message(msg) = &s.feed[0] {
    assert_eq!(msg.text, "hello viewers");
} else {
    panic!("expected FeedEntry::Message");
}
```

Also fix `test_cast_stop_clears_state` — it pushes a raw `FeedMessage`. Replace:

```rust
s.feed.push(crate::state::FeedMessage {
    role: Role::User,
    text: "hi".to_string(),
    timestamp: 0,
});
```

With:

```rust
s.feed.push(crate::state::FeedEntry::Message(crate::state::FeedMessage {
    role: Role::User,
    text: "hi".to_string(),
    timestamp: 0,
}));
```

Also fix `test_get_interactions_clears_queue` — `Interaction` no longer has an `emoji` field. Replace:

```rust
s.add_interaction(crate::state::Interaction {
    kind: InteractionKind::Question,
    text: Some("Why?".to_string()),
    emoji: None,
    timestamp: 0,
});
```

With:

```rust
s.add_interaction(crate::state::Interaction {
    kind: InteractionKind::Question,
    text: Some("Why?".to_string()),
    timestamp: 0,
});
```

- [ ] **Step 5: Run tests — expect remaining compile errors in http_server.rs only**

```bash
cargo test 2>&1 | head -40
```

Expected: compile errors in `http_server.rs` about `FeedMessage`, `msg_to_event`, `Emoji`, `SseEvent` fields.

- [ ] **Step 6: Commit what compiles so far**

```bash
git add src/state.rs src/mcp_server.rs
git commit -m "feat: add FeedEntry, ToolCallEntry, ToolCall SSE event; remove emoji from state"
```

---

## Task 2: Update http_server.rs for FeedEntry

**Files:**
- Modify: `src/http_server.rs`

- [ ] **Step 1: Update imports and the `entry_to_event` function**

Replace the `msg_to_event` function and its usage in `http_server.rs`. First, update the imports at the top to include `FeedEntry` and `ToolCallEntry`:

```rust
use crate::state::{AppState, FeedEntry, Interaction, InteractionKind, Role, SseEvent, SseEventKind, now_secs};
```

Then replace the entire `msg_to_event` function with `entry_to_event`:

```rust
fn entry_to_event(entry: &FeedEntry) -> Event {
    let payload = match entry {
        FeedEntry::Message(msg) => {
            let role_str = match msg.role {
                Role::User => "user",
                Role::Assistant => "assistant",
            };
            SseEvent {
                kind: SseEventKind::Message,
                role: Some(role_str.to_string()),
                text: Some(msg.text.clone()),
                name: None,
                input: None,
                output: None,
            }
        }
        FeedEntry::ToolCall(tc) => SseEvent {
            kind: SseEventKind::ToolCall,
            role: None,
            text: None,
            name: Some(tc.name.clone()),
            input: Some(tc.input.clone()),
            output: Some(tc.output.clone()),
        },
    };
    Event::default().data(serde_json::to_string(&payload).unwrap())
}
```

- [ ] **Step 2: Update `feed_handler` to accept viewer_id and use FeedEntry**

Add a query params struct and update the handler signature and history replay:

```rust
#[derive(Deserialize)]
struct FeedQuery {
    viewer_id: Option<String>,
}

async fn feed_handler(
    State(state): State<AppState>,
    Query(params): Query<FeedQuery>,
) -> impl IntoResponse {
    let (history, rx) = {
        let mut s = state.lock().unwrap();
        s.viewer_count += 1;
        if let Some(id) = &params.viewer_id {
            s.touch_viewer(id);
        }
        (s.feed.clone(), s.tx.subscribe())
    };

    let history_stream = tokio_stream::iter(history)
        .map(|entry| Ok::<Event, Infallible>(entry_to_event(&entry)));

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

    let sse = Sse::new(combined).keep_alive(KeepAlive::new().interval(Duration::from_secs(3)));
    let mut response = sse.into_response();
    let headers = response.headers_mut();
    headers.insert(
        axum::http::HeaderName::from_static("cache-control"),
        axum::http::HeaderValue::from_static("no-cache, no-store"),
    );
    headers.insert(
        axum::http::HeaderName::from_static("pragma"),
        axum::http::HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        axum::http::HeaderName::from_static("x-accel-buffering"),
        axum::http::HeaderValue::from_static("no"),
    );
    response
}
```

- [ ] **Step 3: Update `messages_handler` to filter FeedEntry::Message**

The `/messages` endpoint is no longer used by the viewer (replaced by SSE) but must still compile. Update it to filter only `FeedEntry::Message` entries:

```rust
async fn messages_handler(
    State(state): State<AppState>,
    Query(params): Query<MessagesQuery>,
) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    if let Some(id) = &params.viewer_id {
        s.touch_viewer(id);
    }
    let messages_only: Vec<_> = s.feed.iter()
        .filter_map(|e| if let FeedEntry::Message(m) = e { Some(m) } else { None })
        .collect();
    let since = params.since.unwrap_or(0).min(messages_only.len());
    let messages: Vec<serde_json::Value> = messages_only[since..]
        .iter()
        .map(|m| serde_json::json!({
            "role": match m.role { Role::User => "user", Role::Assistant => "assistant" },
            "text": m.text,
        }))
        .collect();
    Json(serde_json::json!({
        "messages": messages,
        "next_index": messages_only.len(),
        "viewer_count": s.active_viewer_count(),
        "active": s.active,
    }))
}
```

- [ ] **Step 4: Run all tests — should be green**

```bash
cargo test 2>&1
```

Expected: all existing tests pass (18 + the new state tests = ~22 passing). No compile errors.

- [ ] **Step 5: Commit**

```bash
git add src/http_server.rs
git commit -m "feat: update http_server to handle FeedEntry; switch to entry_to_event"
```

---

## Task 3: Remove emoji from /interact

**Files:**
- Modify: `src/http_server.rs`

- [ ] **Step 1: Update the test to expect rejection**

In `src/http_server.rs` tests, rename `test_interact_adds_emoji_when_active` and change the assertion:

```rust
#[tokio::test]
async fn test_interact_rejects_emoji() {
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
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
```

- [ ] **Step 2: Run the test — expect it to fail (currently returns 200)**

```bash
cargo test test_interact_rejects_emoji 2>&1
```

Expected: FAIL — response is `200 OK` instead of `422`.

- [ ] **Step 3: Remove the `Emoji` variant from `InteractPayload`**

In `src/http_server.rs`, find:

```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractPayload {
    Question { text: String },
    Emoji { emoji: String },
}
```

Replace with:

```rust
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractPayload {
    Question { text: String },
}
```

Also remove `InteractionKind` from the `interact_handler` match — the `Emoji` arm is gone. The handler now only needs to handle `Question`:

```rust
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
            timestamp: ts,
        },
    };
    s.add_interaction(interaction);
    StatusCode::OK.into_response()
}
```

Also remove `InteractionKind` from the `http_server.rs` import if it's no longer used — update the import:

```rust
use crate::state::{AppState, FeedEntry, Interaction, InteractionKind, Role, SseEvent, SseEventKind, now_secs};
```

(`InteractionKind` is still used in the `interact_handler` match, so keep it.)

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all pass. `test_interact_rejects_emoji` now returns 422.

- [ ] **Step 5: Commit**

```bash
git add src/http_server.rs
git commit -m "feat: remove emoji interaction type from /interact"
```

---

## Task 4: Add POST /tool-event

**Files:**
- Modify: `src/http_server.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` block in `src/http_server.rs`:

```rust
#[tokio::test]
async fn test_tool_event_adds_to_feed_when_active() {
    let state = new_app_state();
    state.lock().unwrap().active = true;
    let app = router(state.clone());
    let body = serde_json::json!({
        "name": "bash",
        "input": {"command": "ls"},
        "output": "main.rs"
    })
    .to_string();
    let resp = app
        .oneshot(
            Request::post("/tool-event")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let s = state.lock().unwrap();
    assert_eq!(s.feed.len(), 1);
    if let crate::state::FeedEntry::ToolCall(tc) = &s.feed[0] {
        assert_eq!(tc.name, "bash");
        assert_eq!(tc.output, "main.rs");
    } else {
        panic!("expected ToolCall entry");
    }
}

#[tokio::test]
async fn test_tool_event_returns_ok_when_inactive() {
    let app = router(new_app_state());
    let body = serde_json::json!({"name": "bash", "input": {}, "output": ""}).to_string();
    let resp = app
        .oneshot(
            Request::post("/tool-event")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
```

- [ ] **Step 2: Run the tests — expect compile errors (route doesn't exist)**

```bash
cargo test test_tool_event 2>&1
```

Expected: compile failure — route not found at runtime would give 404, but it won't compile because the route isn't registered yet.

- [ ] **Step 3: Add the handler and register the route**

Add the payload struct and handler to `src/http_server.rs`:

```rust
// --- POST /tool-event ---

#[derive(Deserialize)]
struct ToolEventPayload {
    name: String,
    input: serde_json::Value,
    output: String,
}

async fn tool_event_handler(
    State(state): State<AppState>,
    Json(payload): Json<ToolEventPayload>,
) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    if s.active {
        s.push_tool_call(payload.name, payload.input, payload.output);
    }
    StatusCode::OK
}
```

Register the route in `router()`:

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_handler))
        .route("/status", get(status_handler))
        .route("/messages", get(messages_handler))
        .route("/feed", get(feed_handler))
        .route("/interact", post(interact_handler))
        .route("/tool-event", post(tool_event_handler))
        .with_state(state)
}
```

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/http_server.rs
git commit -m "feat: add POST /tool-event endpoint"
```

---

## Task 5: Add GET /pending-questions

**Files:**
- Modify: `src/http_server.rs`

- [ ] **Step 1: Write the failing test**

Add to the `#[cfg(test)]` block in `src/http_server.rs`:

```rust
#[tokio::test]
async fn test_pending_questions_returns_and_clears() {
    let state = new_app_state();
    {
        let mut s = state.lock().unwrap();
        s.active = true;
        s.add_interaction(crate::state::Interaction {
            kind: InteractionKind::Question,
            text: Some("Why Rust?".to_string()),
            timestamp: 0,
        });
        s.add_interaction(crate::state::Interaction {
            kind: InteractionKind::Question,
            text: Some("How does SSE work?".to_string()),
            timestamp: 0,
        });
    }
    let app = router(state.clone());
    let resp = app
        .oneshot(Request::get("/pending-questions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json, serde_json::json!(["Why Rust?", "How does SSE work?"]));
    assert!(state.lock().unwrap().pending_interactions.is_empty());
}

#[tokio::test]
async fn test_pending_questions_returns_empty_when_none() {
    let state = new_app_state();
    state.lock().unwrap().active = true;
    let app = router(state.clone());
    let resp = app
        .oneshot(Request::get("/pending-questions").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json, serde_json::json!([]));
}
```

- [ ] **Step 2: Run the tests — expect failures (404)**

```bash
cargo test test_pending_questions 2>&1
```

Expected: FAIL — 404 response (route not registered yet).

- [ ] **Step 3: Add the handler and register the route**

Add handler to `src/http_server.rs`:

```rust
// --- GET /pending-questions ---

async fn pending_questions_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    let questions = s.take_questions();
    Json(questions)
}
```

Register the route in `router()`:

```rust
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_handler))
        .route("/status", get(status_handler))
        .route("/messages", get(messages_handler))
        .route("/feed", get(feed_handler))
        .route("/interact", post(interact_handler))
        .route("/tool-event", post(tool_event_handler))
        .route("/pending-questions", get(pending_questions_handler))
        .with_state(state)
}
```

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/http_server.rs
git commit -m "feat: add GET /pending-questions endpoint"
```

---

## Task 6: Rewrite viewer.html

**Files:**
- Modify: `assets/viewer.html`

- [ ] **Step 1: Replace the entire file**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>claudecast</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: system-ui, -apple-system, sans-serif;
      background: #f9fafb;
      color: #111827;
      height: 100dvh;
      display: flex;
      flex-direction: column;
    }

    /* Header */
    #header {
      padding: 14px 24px;
      background: #fff;
      border-bottom: 1px solid #e5e7eb;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-shrink: 0;
      position: sticky;
      top: 0;
      z-index: 5;
    }
    #header h1 {
      font-size: 15px;
      font-weight: 700;
      letter-spacing: 0.01em;
      color: #111827;
    }
    #viewer-count {
      font-size: 13px;
      color: #6b7280;
      display: flex;
      align-items: center;
      gap: 6px;
    }
    #viewer-count::before {
      content: '';
      display: inline-block;
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: #10b981;
    }
    #viewer-count.disconnected::before { background: #d1d5db; }

    /* Feed */
    #feed {
      flex: 1;
      overflow-y: auto;
      padding: 20px 24px 100px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }

    /* Message bubbles */
    .message { max-width: 75%; }
    .message.user { align-self: flex-end; }
    .message.assistant { align-self: flex-start; }
    .label {
      font-size: 11px;
      font-weight: 600;
      color: #9ca3af;
      margin-bottom: 4px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .bubble {
      padding: 10px 14px;
      border-radius: 12px;
      font-size: 14px;
      line-height: 1.6;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .user .bubble {
      background: #4f46e5;
      color: #fff;
      border-bottom-right-radius: 3px;
    }
    .assistant .bubble {
      background: #fff;
      color: #111827;
      border: 1px solid #e5e7eb;
      border-bottom-left-radius: 3px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }

    /* Tool call blocks */
    .tool-call {
      align-self: stretch;
      border-radius: 8px;
      border: 1px solid #e5e7eb;
      border-left: 3px solid #4f46e5;
      background: #fff;
      cursor: pointer;
      overflow: hidden;
      transition: box-shadow 0.15s;
      user-select: none;
    }
    .tool-call:hover { box-shadow: 0 1px 4px rgba(79,70,229,0.10); }
    .tool-call.expanded { box-shadow: 0 2px 8px rgba(79,70,229,0.12); }
    .tool-header {
      padding: 8px 12px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .tool-toggle {
      font-size: 10px;
      color: #9ca3af;
      transition: transform 0.15s;
      display: inline-block;
    }
    .tool-call.expanded .tool-toggle { transform: rotate(90deg); }
    .tool-name {
      font-family: 'SF Mono', 'Fira Code', monospace;
      font-size: 12px;
      font-weight: 600;
      color: #4f46e5;
    }
    .tool-hint {
      font-size: 12px;
      color: #9ca3af;
      margin-left: auto;
    }
    .tool-details {
      display: none;
      padding: 0 12px 10px;
      border-top: 1px solid #f3f4f6;
    }
    .tool-call.expanded .tool-details { display: block; }
    .tool-section-label {
      font-size: 10px;
      font-weight: 600;
      color: #9ca3af;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin: 8px 0 4px;
    }
    .tool-code {
      font-family: 'SF Mono', 'Fira Code', monospace;
      font-size: 12px;
      color: #374151;
      white-space: pre-wrap;
      word-break: break-all;
      background: #f9fafb;
      padding: 8px 10px;
      border-radius: 6px;
      border: 1px solid #f3f4f6;
      max-height: 180px;
      overflow-y: auto;
    }

    /* Session ended banner */
    #ended-banner {
      display: none;
      background: #fef2f2;
      border: 1px solid #fecaca;
      border-radius: 8px;
      padding: 10px 14px;
      font-size: 13px;
      color: #dc2626;
      margin: 12px 24px 0;
      flex-shrink: 0;
    }

    /* Floating question bar */
    #question-bar {
      position: fixed;
      bottom: 20px;
      left: 50%;
      transform: translateX(-50%);
      width: min(680px, calc(100% - 40px));
      z-index: 10;
    }
    .question-wrapper {
      display: flex;
      align-items: center;
      background: #fff;
      border-radius: 9999px;
      border: 1px solid #e5e7eb;
      box-shadow: 0 4px 24px rgba(0,0,0,0.10), 0 1px 4px rgba(0,0,0,0.06);
      padding: 6px 6px 6px 20px;
      transition: box-shadow 0.15s;
    }
    .question-wrapper:focus-within {
      box-shadow: 0 4px 24px rgba(79,70,229,0.15), 0 1px 4px rgba(0,0,0,0.06);
      border-color: #a5b4fc;
    }
    #question-input {
      flex: 1;
      border: none;
      outline: none;
      font-size: 14px;
      color: #111827;
      background: transparent;
      min-width: 0;
    }
    #question-input::placeholder { color: #9ca3af; }
    #question-input:disabled { opacity: 0.5; cursor: not-allowed; }
    #send-btn {
      width: 34px;
      height: 34px;
      border-radius: 9999px;
      background: #4f46e5;
      border: none;
      color: #fff;
      font-size: 16px;
      line-height: 1;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      transition: background 0.15s, opacity 0.15s;
    }
    #send-btn:hover { background: #4338ca; }
    #send-btn:disabled { background: #d1d5db; cursor: not-allowed; }
  </style>
</head>
<body>
  <div id="header">
    <h1>claudecast</h1>
    <span id="viewer-count">connecting…</span>
  </div>
  <div id="ended-banner">Session ended by the caster.</div>
  <div id="feed"></div>
  <div id="question-bar">
    <div class="question-wrapper">
      <input id="question-input" type="text" placeholder="Ask a question…" maxlength="280" autocomplete="off">
      <button id="send-btn" onclick="sendQuestion()" title="Send question">&#8594;</button>
    </div>
  </div>
  <script>
    const VIEWER_ID = (() => {
      const key = 'claudecast_viewer_id';
      return localStorage.getItem(key) || (() => {
        const id = Math.random().toString(36).slice(2);
        localStorage.setItem(key, id);
        return id;
      })();
    })();

    let sessionEnded = false;

    function escapeHtml(t) {
      return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    function appendMessage(role, text) {
      const feed = document.getElementById('feed');
      const div = document.createElement('div');
      div.className = 'message ' + role;
      div.innerHTML =
        '<div class="label">' + (role === 'user' ? 'User' : 'Claude') + '</div>' +
        '<div class="bubble">' + escapeHtml(text) + '</div>';
      feed.appendChild(div);
      feed.scrollTop = feed.scrollHeight;
    }

    function appendToolCall(name, input, output) {
      const feed = document.getElementById('feed');
      const inputStr = typeof input === 'object'
        ? JSON.stringify(input, null, 2)
        : String(input || '');
      const outputStr = String(output || '');
      const truncated = outputStr.length > 2000
        ? outputStr.slice(0, 2000) + '\n… (truncated)'
        : outputStr;

      const div = document.createElement('div');
      div.className = 'tool-call';
      div.innerHTML =
        '<div class="tool-header">' +
          '<span class="tool-toggle">&#9654;</span>' +
          '<span class="tool-name">' + escapeHtml(name) + '</span>' +
          '<span class="tool-hint">click to expand</span>' +
        '</div>' +
        '<div class="tool-details">' +
          '<div class="tool-section-label">Input</div>' +
          '<pre class="tool-code">' + escapeHtml(inputStr) + '</pre>' +
          (truncated ? '<div class="tool-section-label">Output</div><pre class="tool-code">' + escapeHtml(truncated) + '</pre>' : '') +
        '</div>';

      div.addEventListener('click', () => {
        div.classList.toggle('expanded');
        div.querySelector('.tool-hint').textContent =
          div.classList.contains('expanded') ? 'click to collapse' : 'click to expand';
      });
      feed.appendChild(div);
      feed.scrollTop = feed.scrollHeight;
    }

    function showSessionEnded() {
      sessionEnded = true;
      document.getElementById('ended-banner').style.display = 'block';
      document.getElementById('question-input').disabled = true;
      document.getElementById('send-btn').disabled = true;
    }

    function sendQuestion() {
      const input = document.getElementById('question-input');
      const text = input.value.trim();
      if (!text || sessionEnded) return;
      fetch('/interact', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'question', text }),
      }).catch(() => {});
      input.value = '';
    }

    document.getElementById('question-input').addEventListener('keydown', e => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendQuestion(); }
    });

    // SSE connection
    const vc = document.getElementById('viewer-count');

    function connect() {
      const es = new EventSource('/feed?viewer_id=' + encodeURIComponent(VIEWER_ID));

      es.onopen = () => {
        vc.className = '';
        vc.textContent = 'connected';
      };

      es.onmessage = (e) => {
        let data;
        try { data = JSON.parse(e.data); } catch { return; }

        if (data.type === 'message') {
          appendMessage(data.role, data.text);
        } else if (data.type === 'tool_call') {
          appendToolCall(data.name, data.input, data.output);
        } else if (data.type === 'session_ended') {
          showSessionEnded();
          es.close();
        }
      };

      es.onerror = () => {
        vc.className = 'disconnected';
        vc.textContent = 'reconnecting…';
      };
    }

    // Poll /status every 30s for viewer count only
    async function pollStatus() {
      try {
        const resp = await fetch('/status');
        if (!resp.ok) return;
        const data = await resp.json();
        if (!sessionEnded) {
          const n = data.viewer_count;
          vc.textContent = n + ' viewer' + (n !== 1 ? 's' : '');
        }
      } catch {}
    }

    connect();
    pollStatus();
    setInterval(pollStatus, 30000);
  </script>
</body>
</html>
```

- [ ] **Step 2: Build to verify no compile errors (viewer.html is embedded via include_str!)**

```bash
cargo build 2>&1
```

Expected: compiles cleanly.

- [ ] **Step 3: Run all tests**

```bash
cargo test 2>&1
```

Expected: all pass. Note: `test_root_returns_viewer_html` checks for "claudecast" in the body — still present in the new HTML.

- [ ] **Step 4: Commit**

```bash
git add assets/viewer.html
git commit -m "feat: redesign viewer — light theme, SSE, expandable tool calls, floating question bar"
```

---

## Task 7: Add Claude Code hooks

**Files:**
- Create: `.claude/hooks/broadcast-tool-call.sh`
- Create: `.claude/hooks/surface-viewer-questions.sh`
- Modify: `.claude/settings.local.json`

- [ ] **Step 1: Create the hook directory and scripts**

```bash
mkdir -p /Users/memo/dev/claudecast/.claude/hooks
```

Create `.claude/hooks/broadcast-tool-call.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')

# Skip all claudecast MCP tools (namespaced as mcp__claudecast__*)
[[ "$TOOL" == mcp__claudecast__* ]] && exit 0

# Extract and format input/output
IN=$(echo "$INPUT" | jq '.tool_input // {}')
OUT=$(echo "$INPUT" | jq -r '
  if .tool_response.content then
    [.tool_response.content[] | select(.type=="text") | .text] | join("\n")
  else
    ""
  end' 2>/dev/null | head -c 4000)

PAYLOAD=$(jq -n \
  --arg name "$TOOL" \
  --argjson input "$IN" \
  --arg output "$OUT" \
  '{name: $name, input: $input, output: $output}')

echo "$PAYLOAD" | curl -s --max-time 1 \
  -X POST http://localhost:3000/tool-event \
  -H 'Content-Type: application/json' \
  -d @- || true
```

Create `.claude/hooks/surface-viewer-questions.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)

# Only act after assistant responses (matcher already scopes to broadcast_message)
ROLE=$(echo "$INPUT" | jq -r '.tool_input.role // empty')
[ "$ROLE" = "assistant" ] || exit 0

# Fetch and print pending viewer questions
QUESTIONS=$(curl -s --max-time 1 http://localhost:3000/pending-questions || true)
[ -z "$QUESTIONS" ] || [ "$QUESTIONS" = "[]" ] && exit 0

echo "Viewer questions from the audience:"
echo "$QUESTIONS" | jq -r '.[]' | while IFS= read -r q; do
  echo "  ❓ $q"
done
```

Make both scripts executable:

```bash
chmod +x /Users/memo/dev/claudecast/.claude/hooks/broadcast-tool-call.sh
chmod +x /Users/memo/dev/claudecast/.claude/hooks/surface-viewer-questions.sh
```

- [ ] **Step 2: Update `.claude/settings.local.json`**

Replace the entire file with:

```json
{
  "enabledMcpjsonServers": ["claudecast"],
  "enableAllProjectMcpServers": true,
  "hooks": {
    "PostToolUse": [
      {
        "matcher": ".*",
        "hooks": [
          {
            "type": "command",
            "command": "/Users/memo/dev/claudecast/.claude/hooks/broadcast-tool-call.sh"
          }
        ]
      },
      {
        "matcher": "mcp__claudecast__broadcast_message",
        "hooks": [
          {
            "type": "command",
            "command": "/Users/memo/dev/claudecast/.claude/hooks/surface-viewer-questions.sh"
          }
        ]
      }
    ]
  }
}
```

Note: the matcher for MCP tools in Claude Code follows the pattern `mcp__<server>__<tool>`. Since the MCP server is named `claudecast` and the tool is `broadcast_message`, the matcher is `mcp__claudecast__broadcast_message`.

- [ ] **Step 3: Smoke-test the broadcast hook manually**

Start the server in one terminal:

```bash
cargo run 2>/dev/null &
sleep 1
```

Simulate what the hook receives (Claude Code passes this JSON on stdin):

```bash
echo '{"tool_name":"bash","tool_input":{"command":"ls src/"},"tool_response":{"content":[{"type":"text","text":"main.rs\nstate.rs"}]}}' \
  | /Users/memo/dev/claudecast/.claude/hooks/broadcast-tool-call.sh
```

Expected: curl posts to the server; if the session is inactive, the server silently ignores it (returns 200).

```bash
curl -s http://localhost:3000/status
```

Expected: `{"viewer_count":0,"active":false,"session_id":null,"public_url":null}` — no feed entries (session inactive).

Kill the test server:

```bash
kill %1 2>/dev/null || true
```

- [ ] **Step 4: Smoke-test the question hook manually**

```bash
cargo run 2>/dev/null &
sleep 1
```

Simulate the hook with no pending questions:

```bash
echo '{"tool_name":"broadcast_message","tool_input":{"role":"assistant","text":"hello"},"tool_response":{"content":[{"type":"text","text":"ok"}]}}' \
  | /Users/memo/dev/claudecast/.claude/hooks/surface-viewer-questions.sh
```

Expected: no output (no questions pending, session inactive).

```bash
kill %1 2>/dev/null || true
```

- [ ] **Step 5: Run full test suite one last time**

```bash
cargo test 2>&1
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add .claude/hooks/ .claude/settings.local.json
git commit -m "feat: add PostToolUse hooks for tool call broadcast and viewer question surfacing"
```

---

## Final verification

After all tasks complete, run the full test suite:

```bash
cargo test 2>&1
```

Expected output: all tests pass, 0 failures. Count should be higher than the initial 18 (new tests added in Tasks 1–5).
