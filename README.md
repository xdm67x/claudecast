# claudecast

Broadcast your Claude Code session live. Viewers watch the full conversation unfold in real-time in a browser and can send emoji reactions and text questions. You receive their interactions as MCP tool results and decide when to act on them.

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

Add to your Claude Code MCP config (`~/.claude/claude_desktop_config.json` or `.claude/settings.local.json`):

```json
{
  "mcpServers": {
    "claudecast": {
      "command": "claudecast"
    }
  }
}
```

## Usage

Once configured, the following MCP tools are available in any Claude Code session:

| Tool                | Description                                                                                                      |
| ------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `cast_start`        | Starts the HTTP server and Cloudflare tunnel. Returns the public URL and auto-broadcast instructions for Claude. |
| `cast_stop`         | Ends the session and disconnects all viewers.                                                                    |
| `broadcast_message` | Pushes a message to the viewer feed (`role`: `"user"` or `"assistant"`).                                         |
| `get_interactions`  | Returns pending viewer questions and emoji counts since the last call, then clears the queue.                    |

### Starting a cast

Tell Claude to call `cast_start`. It will return a `trycloudflare.com` URL to share with your audience and inject a convention for auto-broadcasting:

```
[claudecast active — public URL: https://xyz.trycloudflare.com]
Convention: before processing each user message, call broadcast_message(role="user", text=<message>).
After each of your responses, call broadcast_message(role="assistant", text=<response>).
```

From that point Claude automatically mirrors each turn to the viewer feed.

### Checking viewer interactions

At any point, ask Claude to call `get_interactions`. It returns the accumulated emoji reactions and any questions viewers have submitted, then clears the queue. You decide whether to address them.

### Ending a cast

Tell Claude to call `cast_stop`. All connected viewers receive a session-ended notification and the tunnel is torn down.

## How it works

```
Claude Code ←→ [MCP Server / stdio]
                       │
              [Shared State + broadcast channel]
                       │
         [HTTP Server / Axum :3000]
                       │
              [cloudflared tunnel]
                       │
              [Viewer browsers / SSE]
```

- The binary runs two servers concurrently in the same Tokio runtime: an MCP stdio server and an Axum HTTP server on `:3000`.
- A `tokio::sync::broadcast` channel fans out each new feed message to all connected SSE clients simultaneously.
- New viewers receive full conversation history on connect, then stream live updates.
- Viewer count is tracked via drop guards on SSE connections — tab closes are handled automatically.
