# claudecast — Design Spec

**Date:** 2026-05-20  
**Status:** Approved

---

## Overview

`claudecast` is a Rust binary that lets a Claude Code user broadcast their session live to an audience. Viewers watch the full conversation unfold in real-time in a web browser and can interact with emoji reactions and text questions. The caster receives viewer interactions as MCP tool results and decides if/when to use them.

---

## Architecture

Single Rust binary exposing two servers running concurrently in the same Tokio runtime:

1. **MCP Server** — stdio transport, handles tool calls from Claude Code
2. **HTTP Server** — Axum on `:3000`, serves the viewer UI and SSE feed

Shared state between both servers via `Arc<Mutex<CastState>>`. A `tokio::sync::broadcast::Channel` fans out each new feed message to all connected SSE clients simultaneously.

Public access is provided by launching a `cloudflared` subprocess that tunnels `localhost:3000` to a temporary `trycloudflare.com` URL — no Cloudflare account required.

```
Claude Code ←→ [MCP Server / stdio]
                       │
              [Shared State + broadcast::channel]
                       │
         [HTTP Server / Axum :3000]
                       │
              [cloudflared tunnel]
                       │
                   Internet
                       │
              [Viewer browsers / SSE]
```

---

## MCP Tools

| Tool | Description |
|---|---|
| `cast_start` | Starts the HTTP server and cloudflared tunnel. Returns the public URL and injects system prompt instructions for Claude to auto-broadcast. |
| `cast_stop` | Stops the session, kills cloudflared subprocess, disconnects all SSE clients. |
| `broadcast_message` | Pushes a message into the feed (role: `user` or `assistant`). Called by Claude automatically per system prompt convention. |
| `get_interactions` | Returns all pending viewer questions and emoji counts since last call. Clears the pending queue. |

### Conversation capture convention

`cast_start` returns a system-level instruction block:

```
[claudecast active — public URL: https://xyz.trycloudflare.com]
Convention: before processing each user message, call broadcast_message(role="user", text=<user message>).
After each of your responses, call broadcast_message(role="assistant", text=<your response>).
```

This keeps all message routing within the standard MCP tool call flow — no hooks, no sampling, no side-channels.

---

## HTTP API

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Serves the viewer HTML page (embedded in binary via `include_str!`) |
| `/feed` | GET | SSE stream — new `FeedMessage` events as they arrive. On connect, replays full history first, then streams live. |
| `/interact` | POST | Accepts `{"type":"question","text":"..."}` or `{"type":"emoji","emoji":"🔥"}`. Appends to pending interactions queue. |
| `/status` | GET | Returns `{"viewer_count": N, "session_id": "...", "public_url": "..."}` |

---

## State

```rust
struct CastState {
    session_id: String,
    public_url: Option<String>,
    viewer_count: usize,
    feed: Vec<FeedMessage>,
    pending_interactions: Vec<Interaction>,
    tx: broadcast::Sender<FeedMessage>,
}

struct FeedMessage {
    role: Role,      // User | Assistant
    text: String,
    timestamp: u64,
}

struct Interaction {
    kind: InteractionKind,   // Question | Emoji
    text: Option<String>,
    emoji: Option<String>,
    timestamp: u64,
}
```

New SSE clients receive the full `feed` history on connection, then subscribe to the broadcast channel for live updates.

**Viewer count tracking:** The SSE handler increments `viewer_count` on connect and decrements it via a drop guard when the client disconnects (stream dropped). This handles tab closes without explicit disconnect signals.

**Session end:** `cast_stop` drops the `broadcast::Sender`. All SSE handlers receive `RecvError::Closed`, send a final `{"type":"session_ended"}` SSE event, then close the stream cleanly.

---

## Viewer UI

A single HTML file embedded in the binary via `include_str!`. No frontend build step, no external dependencies.

**Layout (reading-focused):**
- Header: session name, live viewer count
- Main: scrollable conversation feed (messages appended in real-time via SSE `EventSource`)
- Bottom bar: 4 emoji reaction buttons (👍 🔥 ❓ 😮) with counts + text question input + send button

Viewer count is refreshed via `GET /status` polling every 30 seconds.

---

## Cloudflare Tunnel

`cast_start` spawns `cloudflared tunnel --url http://localhost:3000` as a child process, reads its stderr line-by-line until a line matching `https://.*trycloudflare.com` is found, then returns that URL. `cast_stop` sends SIGTERM to the child process.

**Requirement:** `cloudflared` must be installed on the caster's machine (installable via `brew install cloudflare/cloudflare/cloudflared`).

---

## Dependencies (Cargo)

| Crate | Purpose |
|---|---|
| `tokio` | Async runtime (full features) |
| `axum` | HTTP server |
| `rmcp` | MCP server (Rust MCP SDK) |
| `serde` / `serde_json` | JSON serialization |
| `tokio-stream` | SSE streaming via `StreamBodyAs` |

---

## Non-goals (v1)

- Authentication / access control for viewers
- Persistent session storage
- WebSocket transport (SSE is sufficient for unidirectional feed)
- Custom domain support for Cloudflare tunnel
- Chat history export
