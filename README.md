# claudecast

<img width="1624" height="984" alt="image" src="https://github.com/user-attachments/assets/6dfccd9a-1413-4f07-86bf-780a81dbe526" />

Broadcast your Claude Code session live. Viewers watch the full conversation — messages, tool calls, and responses — unfold in real-time in a browser. They can send text questions that surface automatically in Claude's context. Everything is broadcast automatically via Claude Code hooks; no manual tool calls needed during a session.

## Prerequisites

- [Rust](https://rustup.rs) (stable)
- [`cloudflared`](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/) — no Cloudflare account required

```bash
brew install cloudflare/cloudflare/cloudflared
```

## Install

```bash
git clone https://github.com/xdm67x/claudecast
cd claudecast
cargo build --release
# binary is at target/release/claudecast
```

## Configure as an MCP server

Add to your project's `.claude/settings.local.json`:

```json
{
  "mcpServers": {
    "claudecast": {
      "command": "target/release/claudecast"
    }
  }
}
```

Then install the Claude Code hooks that auto-broadcast your session:

```bash
target/release/claudecast --install-hooks
```

Restart Claude Code to activate the hooks.

## Usage

The following MCP tools are available once the server is connected:

| Tool                | Description                                                                 |
| ------------------- | --------------------------------------------------------------------------- |
| `cast_start`        | Starts the HTTP server and Cloudflare tunnel. Returns the public URL.       |
| `cast_stop`         | Ends the session and disconnects all viewers.                               |
| `broadcast_message` | Pushes a custom note to the viewer feed (optional — hooks handle messages). |
| `get_interactions`  | Returns pending viewer questions since the last call, then clears the queue.|

### Starting a cast

Tell Claude to call `cast_start`. It returns a `trycloudflare.com` URL to share with your audience:

```
[claudecast active — public URL: https://xyz.trycloudflare.com]
User messages and your responses are broadcast automatically via hooks.
```

From that point, everything is automatic:

- **User messages** are captured by the `UserPromptSubmit` hook and pushed to the viewer feed.
- **Tool calls** (Bash, Read, Edit, …) are captured by the `PostToolUse` hook and shown as expandable cards.
- **Assistant responses** are captured by the `Stop` hook via the `last_assistant_message` payload field.
- **Viewer questions** are fetched from the server and injected into Claude's context at the start of each turn via the same `UserPromptSubmit` hook.

### Viewer features

- Markdown rendering (GFM — bold, italic, code blocks, tables, lists)
- Expandable tool call cards showing input and output
- "Claude is thinking…" animated indicator while the agent is working
- Live question stack above the input bar — questions disappear as the streamer reads them

### Checking viewer interactions

Ask Claude to call `get_interactions`. It returns any questions viewers have submitted since the last call, then clears the queue.

### Ending a cast

Tell Claude to call `cast_stop`. All connected viewers receive a session-ended notification and the tunnel is torn down.

## How it works

```
Claude Code ←→ [MCP Server / stdio]
                       │
              [Shared State (feed + questions)]
                       │
         [HTTP Server / Axum :3000]
                       │
              [cloudflared tunnel]
                       │
           [Viewer browsers / polling /messages]
```

**Hooks** (installed via `--install-hooks`):

| Event              | Script                          | Action                                      |
| ------------------ | ------------------------------- | ------------------------------------------- |
| `UserPromptSubmit` | `broadcast-user-message.sh`     | POST user message + surface viewer questions|
| `PostToolUse`      | `broadcast-tool-call.sh`        | POST tool name, input, output               |
| `Stop`             | `broadcast-assistant-message.sh`| POST `last_assistant_message` from payload  |

**Server**:

- The binary runs two servers concurrently in the same Tokio runtime: an MCP stdio server (stdio transport) and an Axum HTTP server on `:3000`.
- The viewer polls `/messages?since=N` every second to receive new feed entries (messages and tool calls) incrementally.
- The `thinking` flag is set `true` when a user message is pushed and `false` when the assistant response arrives — used to show/hide the thinking indicator.
- Viewer session activity is tracked via a timestamp map; questions are surfaced non-destructively on each poll and cleared only when `get_interactions` is called.
