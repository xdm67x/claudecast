use std::os::unix::fs::PermissionsExt;

// Fires on PostToolUse for every tool: broadcasts the tool call to the viewer feed.
const BROADCAST_TOOL_SCRIPT: &str = r#"#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')
SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')

# When cast_start fires, register this session as the casting session
if [[ "$TOOL" == "mcp__claudecast__cast_start" ]]; then
  [ -n "$SESSION_ID" ] || exit 0
  ACTIVE=$(curl -s --max-time 1 http://localhost:3000/status 2>/dev/null | jq -r '.active // false' 2>/dev/null || echo "false")
  [ "$ACTIVE" = "true" ] || exit 0
  jq -n --arg sid "$SESSION_ID" '{session_id: $sid}' | \
    curl -s --max-time 1 -X POST http://localhost:3000/register-session \
    -H 'Content-Type: application/json' -d @- >/dev/null || true
  exit 0
fi

# Skip all other claudecast MCP tools to avoid broadcast loops
[[ "$TOOL" == mcp__claudecast__* ]] && exit 0

ACTIVE=$(curl -s --max-time 1 http://localhost:3000/status 2>/dev/null | jq -r '.active // false' 2>/dev/null || echo "false")
[ "$ACTIVE" = "true" ] || exit 0

IN=$(echo "$INPUT" | jq '.tool_input // {}')
OUT=$(echo "$INPUT" | jq -r '
  if .tool_response.content then
    [.tool_response.content[] | select(.type=="text") | .text] | join("\n")
  else ""
  end' 2>/dev/null | head -c 4000)

jq -n --arg name "$TOOL" --argjson input "$IN" --arg output "$OUT" --arg sid "$SESSION_ID" \
  '{name: $name, input: $input, output: $output, session_id: $sid}' | \
  curl -s --max-time 1 -X POST http://localhost:3000/tool-event \
  -H 'Content-Type: application/json' -d @- >/dev/null || true
"#;

// Fires on UserPromptSubmit: auto-broadcasts the user message and surfaces pending questions.
const USER_MESSAGE_SCRIPT: &str = r#"#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)

ACTIVE=$(curl -s --max-time 1 http://localhost:3000/status 2>/dev/null | jq -r '.active // false' 2>/dev/null || echo "false")
[ "$ACTIVE" = "true" ] || exit 0

TEXT=$(echo "$INPUT" | jq -r '.prompt // empty')
[ -n "$TEXT" ] || exit 0

SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')

jq -n --arg text "$TEXT" --arg sid "$SESSION_ID" '{text: $text, session_id: $sid}' | \
  curl -s --max-time 1 -X POST http://localhost:3000/user-message \
  -H 'Content-Type: application/json' -d @- >/dev/null || true

# Surface pending viewer questions as context for Claude before it processes the user message
QUESTIONS=$(curl -s --max-time 1 http://localhost:3000/pending-questions 2>/dev/null || true)
[ -z "$QUESTIONS" ] || [ "$QUESTIONS" = "[]" ] && exit 0

echo "Viewer questions from the audience:"
echo "$QUESTIONS" | jq -r '.[]' | while IFS= read -r q; do
  echo "  ❓ $q"
done
"#;

// Fires on Stop: the payload includes last_assistant_message directly — no transcript parsing needed.
const ASSISTANT_MESSAGE_SCRIPT: &str = r#"#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)

ACTIVE=$(curl -s --max-time 1 http://localhost:3000/status 2>/dev/null | jq -r '.active // false' 2>/dev/null || echo "false")
[ "$ACTIVE" = "true" ] || exit 0

TEXT=$(echo "$INPUT" | jq -r '.last_assistant_message // empty')
[ -n "$TEXT" ] || exit 0

SESSION_ID=$(echo "$INPUT" | jq -r '.session_id // empty')

jq -n --arg text "$TEXT" --arg sid "$SESSION_ID" '{text: $text, session_id: $sid}' | \
  curl -s --max-time 1 -X POST http://localhost:3000/assistant-message \
  -H 'Content-Type: application/json' -d @- >/dev/null || true
"#;


pub fn install_hooks() -> Result<(), Box<dyn std::error::Error>> {
    let home = std::env::var("HOME")?;

    let dir = format!("{}/.claudecast", home);
    std::fs::create_dir_all(&dir)?;

    let tool_path       = format!("{}/broadcast-tool-call.sh", dir);
    let user_msg_path   = format!("{}/broadcast-user-message.sh", dir);
    let asst_msg_path   = format!("{}/broadcast-assistant-message.sh", dir);

    let scripts = [
        (&tool_path,       BROADCAST_TOOL_SCRIPT),
        (&user_msg_path,   USER_MESSAGE_SCRIPT),
        (&asst_msg_path,   ASSISTANT_MESSAGE_SCRIPT),
    ];
    for (path, content) in &scripts {
        std::fs::write(path, content)?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    }

    let claude_dir = format!("{}/.claude", home);
    std::fs::create_dir_all(&claude_dir)?;
    let settings_path = format!("{}/settings.json", claude_dir);

    let raw = std::fs::read_to_string(&settings_path).unwrap_or_else(|_| "{}".to_string());
    let mut settings: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or(serde_json::json!({}));

    if settings["hooks"].is_null() || !settings["hooks"].is_object() {
        settings["hooks"] = serde_json::json!({});
    }

    // Helper: add hook entry if not already present (idempotent by command path)
    fn add_hook(
        settings: &mut serde_json::Value,
        event: &str,
        matcher: &str,
        command: &str,
    ) {
        if settings["hooks"][event].is_null() || !settings["hooks"][event].is_array() {
            settings["hooks"][event] = serde_json::json!([]);
        }
        let arr = settings["hooks"][event].as_array_mut().unwrap();
        let already = arr.iter().any(|e| {
            e["hooks"]
                .as_array()
                .and_then(|hs| hs.first())
                .and_then(|h| h["command"].as_str())
                == Some(command)
        });
        if !already {
            arr.push(serde_json::json!({
                "matcher": matcher,
                "hooks": [{"type": "command", "command": command}]
            }));
        }
    }

    add_hook(&mut settings, "PostToolUse",      ".*", &tool_path);
    add_hook(&mut settings, "UserPromptSubmit", "",   &user_msg_path);
    add_hook(&mut settings, "Stop",             "",   &asst_msg_path);

    std::fs::write(&settings_path, serde_json::to_string_pretty(&settings)?)?;

    println!("claudecast hooks installed.");
    println!("  Scripts: {}", dir);
    println!("  Settings: {}", settings_path);
    println!("\nRestart Claude Code to activate the hooks.");

    Ok(())
}
