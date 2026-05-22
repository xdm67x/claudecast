# claudecast — Live Interaction + Tool Calls Design

**Date:** 2026-05-22  
**Status:** Approved

---

## Overview

Three improvements to the existing claudecast system:

1. **Tool call visibility** — all Claude tool uses (bash, read, edit, etc.) appear in the viewer feed as expandable blocks
2. **Emoji removal** — emoji reactions removed from viewer UI and backend
3. **Live question surfacing** — viewer questions automatically appear to the caster via a Claude Code hook, no manual `get_interactions` call needed

---

## Architecture Changes

No new servers or processes. All changes are additive to the existing architecture:
- Two new HTTP endpoints on the existing Axum server
- New SSE event kind routed through the existing broadcast channel
- Two PostToolUse hooks in `.claude/settings.local.json`
- Updated viewer HTML (light theme, expandable tool call blocks, floating question bar)

---

## Backend Changes (Rust)

### New SSE event kind

Add `ToolCall` to `SseEventKind`:

```rust
pub enum SseEventKind {
    Message,
    ToolCall,
    SessionEnded,
}
```

Add corresponding fields to `SseEvent`:

```rust
pub struct SseEvent {
    pub kind: SseEventKind,
    pub role: Option<String>,      // for Message
    pub text: Option<String>,      // for Message
    pub name: Option<String>,      // for ToolCall
    pub input: Option<serde_json::Value>, // for ToolCall
    pub output: Option<String>,    // for ToolCall
}
```

### Feed history: unified FeedEntry

Replace `Vec<FeedMessage>` with `Vec<FeedEntry>` to replay both messages and tool calls to late-joining viewers:

```rust
pub enum FeedEntry {
    Message(FeedMessage),
    ToolCall(ToolCallEntry),
}

pub struct ToolCallEntry {
    pub name: String,
    pub input: serde_json::Value,
    pub output: String,
    pub timestamp: u64,
}
```

### New HTTP endpoints

**`POST /tool-event`**
- Accepts `{"name": "bash", "input": {...}, "output": "..."}`
- Ignored silently if no active session
- Stores entry in `feed`, broadcasts `SseEvent { kind: ToolCall, name, input, output }`
- Returns 200 OK always (even when inactive, to avoid hook errors)

**`GET /pending-questions`**
- Returns `["question1", "question2"]` — array of question text strings
- Clears the queue on read
- Returns `[]` when empty or no active session

### Remove emoji support

- Remove `Emoji` variant from `InteractionKind`
- `POST /interact` only accepts `{"type": "question", "text": "..."}` — rejects `emoji` type with 400
- Remove `Interaction.emoji` field

---

## Viewer UI Changes (viewer.html)

### Theme

Full light theme:
- Background: `#f9fafb` (near-white)
- Header: white with bottom border `#e5e7eb`
- Message bubbles: user `#4f46e5` (indigo), assistant `#ffffff` with border `#e5e7eb`
- Tool call blocks: `#f3f4f6` background, indigo left border accent
- Text: `#111827` primary, `#6b7280` secondary

### Tool call blocks

Each `tool_call` SSE event renders as a collapsed row:

```
▶ bash                     ← click to toggle
  command: ls -la src/
  ─────────────────────
  main.rs  state.rs  ...   ← output, monospace, scrollable
```

CSS: monospace font, `border-left: 3px solid #4f46e5`, `border-radius: 8px`, soft shadow on expand. Input shown as key: value pairs. Output truncated to 2000 chars client-side with "… (truncated)" if longer.

### Floating question bar

Fixed position at the bottom, centered with horizontal padding. Pill shape (`border-radius: 9999px`), white background, `box-shadow: 0 4px 24px rgba(0,0,0,0.10)`. Send button is an inline icon button (arrow icon `→`) on the right inside the input — no separate `<button>` element visible outside.

### Remove

- Emoji buttons (`👍 🔥 ❓ 😮`), `EMOJI_IDS`, `counts`, `sendEmoji()`
- Dark theme CSS entirely

### Viewer transport: switch from polling to SSE

The current viewer uses `setInterval(poll, 1500)` on `/messages`. This is replaced with a persistent `EventSource` on `/feed` for real-time delivery (tool calls appear immediately, not 1.5s late). The `/messages` endpoint remains but is no longer used by the viewer.

### SSE handling

`EventSource` on `/feed`. Handler branches on `event.type` parsed from `event.data`:
- `"message"` → render chat bubble (existing)
- `"tool_call"` → render expandable tool call block (new)
- `"session_ended"` → show session-ended banner (existing)

---

## Claude Code Hooks

Both hooks live in `.claude/settings.local.json` under `hooks.PostToolUse`.

### Hook 1 — Tool call broadcast

**Matcher:** `*` (all tools)

**Script (inline shell):**

```bash
#!/bin/bash
set -euo pipefail
INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')

# Skip claudecast's own MCP tools
case "$TOOL" in
  cast_start|cast_stop|broadcast_message|get_interactions) exit 0 ;;
esac

IN=$(echo "$INPUT" | jq '.tool_input // {}')
OUT=$(echo "$INPUT" | jq -r '
  if .tool_response.content then
    [.tool_response.content[] | select(.type=="text") | .text] | join("\n")
  else "" end' | head -c 4000)

jq -n --arg name "$TOOL" --argjson input "$IN" --arg output "$OUT" \
  '{name:$name,input:$input,output:$output}' \
  | curl -s --max-time 1 -X POST http://localhost:3000/tool-event \
      -H 'Content-Type: application/json' -d @- || true
```

Silently fails (via `|| true`) when claudecast is not running.

### Hook 2 — Question surfacing

**Matcher:** `broadcast_message`

**Script (inline shell):**

```bash
#!/bin/bash
set -euo pipefail
INPUT=$(cat)
ROLE=$(echo "$INPUT" | jq -r '.tool_input.role // empty')

# Only surface questions after assistant responses
[ "$ROLE" = "assistant" ] || exit 0

QUESTIONS=$(curl -s --max-time 1 http://localhost:3000/pending-questions || true)
[ -z "$QUESTIONS" ] || [ "$QUESTIONS" = "[]" ] && exit 0

echo "Viewer questions:"
echo "$QUESTIONS" | jq -r '.[]' | while IFS= read -r q; do
  echo "  ❓ $q"
done
```

Output goes to Claude Code as a tool result annotation — Claude sees the questions inline after each assistant response and can address them in the next turn.

---

## Non-goals

- Showing tool call results in real-time (pre/post split) — PostToolUse only
- Filtering which tool calls appear (all non-cast tools are shown)
- Persistent tool call history across sessions
- Question notifications to viewers (questions are caster-only)
