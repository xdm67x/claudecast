use axum::{
    Router,
    extract::{Json, Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::sse::{Event, KeepAlive, Sse},
    routing::{get, post},
};
use futures::Stream;
use pin_project::pin_project;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, pin::Pin, task::{Context, Poll}, time::Duration};
use tokio_stream::{StreamExt as _, wrappers::BroadcastStream};

use crate::state::{AppState, FeedEntry, Interaction, InteractionKind, Role, SseEvent, SseEventKind, now_secs};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ui_handler))
        .route("/status", get(status_handler))
        .route("/messages", get(messages_handler))
        .route("/feed", get(feed_handler))
        .route("/interact", post(interact_handler))
        .route("/tool-event", post(tool_event_handler))
        .route("/pending-questions", get(pending_questions_handler))
        .route("/user-message", post(user_message_handler))
        .route("/assistant-message", post(assistant_message_handler))
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
        viewer_count: s.active_viewer_count(),
        active: s.active,
        session_id: s.session_id.clone(),
        public_url: s.public_url.clone(),
    })
}

// --- GET /messages ---

#[derive(Deserialize)]
struct MessagesQuery {
    since: Option<usize>,
    viewer_id: Option<String>,
}

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
    let questions: Vec<&str> = s.pending_interactions
        .iter()
        .filter_map(|i| i.text.as_deref())
        .collect();
    Json(serde_json::json!({
        "messages": messages,
        "next_index": messages_only.len(),
        "viewer_count": s.active_viewer_count(),
        "active": s.active,
        "thinking": s.thinking,
        "questions": questions,
    }))
}

// --- POST /interact ---

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InteractPayload {
    Question { text: String },
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
            timestamp: ts,
        },
    };
    s.add_interaction(interaction);
    StatusCode::OK.into_response()
}

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

// --- POST /user-message ---

#[derive(Deserialize)]
struct RawMessagePayload {
    text: String,
}

async fn user_message_handler(
    State(state): State<AppState>,
    Json(payload): Json<RawMessagePayload>,
) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    if s.active {
        s.push_message(crate::state::Role::User, payload.text);
    }
    StatusCode::OK
}

// --- POST /assistant-message ---

async fn assistant_message_handler(
    State(state): State<AppState>,
    Json(payload): Json<RawMessagePayload>,
) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    if s.active {
        s.push_message(crate::state::Role::Assistant, payload.text);
    }
    StatusCode::OK
}

// --- GET /pending-questions ---

async fn pending_questions_handler(State(state): State<AppState>) -> impl IntoResponse {
    let mut s = state.lock().unwrap();
    let questions = s.take_questions();
    Json(questions)
}

// --- GET /feed ---

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

    // Cloudflare buffers SSE responses until ~4KB; send a large padding comment first
    // so the real events are flushed immediately rather than held in Cloudflare's buffer.
    let padding = " ".repeat(4096);
    let padding_stream = tokio_stream::iter(std::iter::once(
        Ok::<Event, Infallible>(Event::default().comment(padding)),
    ));

    let history_stream = tokio_stream::iter(history)
        .map(|entry| Ok::<Event, Infallible>(entry_to_event(&entry)));

    let live_stream = BroadcastStream::new(rx).filter_map(|r| match r {
        Ok(event) => {
            Some(Ok(Event::default().data(serde_json::to_string(&event).unwrap())))
        }
        Err(_) => None,
    });

    let combined = ViewerStream {
        inner: padding_stream.chain(history_stream).chain(live_stream),
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

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::new_app_state;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_messages_returns_feed_and_tracks_viewer() {
        let state = new_app_state();
        state.lock().unwrap().active = true;
        state.lock().unwrap().push_message(crate::state::Role::User, "hello".to_string());
        let app = router(state.clone());
        let resp = app
            .oneshot(
                Request::get("/messages?since=0&viewer_id=abc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["messages"][0]["role"], "user");
        assert_eq!(json["messages"][0]["text"], "hello");
        assert_eq!(json["next_index"], 1);
        assert_eq!(json["active"], true);
        assert_eq!(state.lock().unwrap().active_viewer_count(), 1);
    }

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
        let body = serde_json::json!({"type": "question", "text": "hello?"}).to_string();
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
    async fn test_feed_viewer_count_increments_and_decrements() {
        let state = new_app_state();
        assert_eq!(state.lock().unwrap().viewer_count, 0);

        let app = router(state.clone());
        // oneshot drives the handler to completion (increments viewer_count),
        // then drops ViewerStream (decrements viewer_count via PinnedDrop).
        let resp = app
            .oneshot(Request::get("/feed").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // Response must be 200 SSE
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(ct.contains("text/event-stream"));

        // After response is dropped, ViewerStream's PinnedDrop fires.
        // Dropping the response to trigger cleanup.
        drop(resp);

        // Yield to let the tokio runtime process the drop.
        tokio::task::yield_now().await;

        // Count should be back to 0: the increment happened, the drop decremented.
        assert_eq!(state.lock().unwrap().viewer_count, 0);
    }

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
}
