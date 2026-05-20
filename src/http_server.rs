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
    use axum::http::{Request, StatusCode};
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
