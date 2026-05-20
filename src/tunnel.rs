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
        .kill_on_drop(true)
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
