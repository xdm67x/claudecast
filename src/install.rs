use std::os::unix::fs::PermissionsExt;

const BROADCAST_SCRIPT: &str = r#"#!/usr/bin/env bash
set -euo pipefail

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty')

# Skip all claudecast MCP tools (namespaced as mcp__claudecast__*)
[[ "$TOOL" == mcp__claudecast__* ]] && exit 0

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
"#;

const QUESTIONS_SCRIPT: &str = r#"#!/usr/bin/env bash
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
"#;

pub fn install_hooks() -> Result<(), Box<dyn std::error::Error>> {
    let home = std::env::var("HOME")?;

    // Write hook scripts to ~/.claudecast/
    let dir = format!("{}/.claudecast", home);
    std::fs::create_dir_all(&dir)?;

    let broadcast_path = format!("{}/broadcast-tool-call.sh", dir);
    let questions_path = format!("{}/surface-viewer-questions.sh", dir);

    std::fs::write(&broadcast_path, BROADCAST_SCRIPT)?;
    std::fs::write(&questions_path, QUESTIONS_SCRIPT)?;
    std::fs::set_permissions(&broadcast_path, std::fs::Permissions::from_mode(0o755))?;
    std::fs::set_permissions(&questions_path, std::fs::Permissions::from_mode(0o755))?;

    // Update ~/.claude/settings.json
    let claude_dir = format!("{}/.claude", home);
    std::fs::create_dir_all(&claude_dir)?;
    let settings_path = format!("{}/settings.json", claude_dir);

    let raw = std::fs::read_to_string(&settings_path).unwrap_or_else(|_| "{}".to_string());
    let mut settings: serde_json::Value =
        serde_json::from_str(&raw).unwrap_or(serde_json::json!({}));

    // Ensure settings["hooks"]["PostToolUse"] is an array
    if settings["hooks"].is_null() || !settings["hooks"].is_object() {
        settings["hooks"] = serde_json::json!({});
    }
    if settings["hooks"]["PostToolUse"].is_null() || !settings["hooks"]["PostToolUse"].is_array() {
        settings["hooks"]["PostToolUse"] = serde_json::json!([]);
    }

    let arr = settings["hooks"]["PostToolUse"].as_array_mut().unwrap();

    // Add broadcast hook if not already present (idempotent)
    if !arr.iter().any(|entry| {
        entry["hooks"]
            .as_array()
            .and_then(|hs| hs.first())
            .and_then(|h| h["command"].as_str())
            == Some(broadcast_path.as_str())
    }) {
        arr.push(serde_json::json!({
            "matcher": ".*",
            "hooks": [{"type": "command", "command": broadcast_path}]
        }));
    }

    // Add questions hook if not already present (idempotent)
    if !arr.iter().any(|entry| {
        entry["hooks"]
            .as_array()
            .and_then(|hs| hs.first())
            .and_then(|h| h["command"].as_str())
            == Some(questions_path.as_str())
    }) {
        arr.push(serde_json::json!({
            "matcher": "mcp__claudecast__broadcast_message",
            "hooks": [{"type": "command", "command": questions_path}]
        }));
    }

    std::fs::write(&settings_path, serde_json::to_string_pretty(&settings)?)?;

    println!("claudecast hooks installed.");
    println!("  Scripts: {}", dir);
    println!("  Settings: {}", settings_path);
    println!("\nRestart Claude Code to activate the hooks.");

    Ok(())
}
