use crate::security::{PathPolicy, Workspace};
use crate::tools::ToolCallResult;
use sha2::{Digest, Sha256};
use std::io::Read;

pub fn handle_read_file(
    ws: &Workspace,
    path_str: &str,
    start_line: Option<usize>,
    max_lines: Option<usize>,
    max_file_bytes: usize,
) -> ToolCallResult {
    let rel_path = match PathPolicy::sanitize_relative_path(path_str) {
        Ok(path) => path,
        Err(e) => return ToolCallResult::err(e.to_string()),
    };
    let dir = match ws.cap_dir() {
        Ok(dir) => dir,
        Err(e) => {
            return ToolCallResult::err(format!("Failed to open workspace capability: {}", e))
        }
    };
    let file = match dir.open(&rel_path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return ToolCallResult::err("File not found or is not a regular file")
        }
        Err(e) => return ToolCallResult::err(format!("Failed to open file: {}", e)),
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(e) => return ToolCallResult::err(format!("Failed to inspect file: {}", e)),
    };
    if !metadata.is_file() {
        return ToolCallResult::err("File not found or is not a regular file");
    }
    if metadata.len() > max_file_bytes as u64 {
        return ToolCallResult::err(format!(
            "File exceeds configured read limit ({} bytes > {} bytes)",
            metadata.len(),
            max_file_bytes
        ));
    }

    let mut bytes = Vec::with_capacity((metadata.len() as usize).min(max_file_bytes));
    let read_limit = max_file_bytes.saturating_add(1) as u64;
    if let Err(e) = file.take(read_limit).read_to_end(&mut bytes) {
        return ToolCallResult::err(format!("Failed to read file: {}", e));
    }
    if bytes.len() > max_file_bytes {
        return ToolCallResult::err(format!(
            "File exceeded configured read limit while being read (more than {} bytes)",
            max_file_bytes
        ));
    }

    let hash = format!("{:x}", Sha256::digest(&bytes));
    let text = match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return ToolCallResult::err("Binary file detected (non-UTF8)"),
    };

    let lines: Vec<&str> = text.lines().collect();
    let total_lines = lines.len();
    let start = start_line.unwrap_or(1).saturating_sub(1);
    let limit = max_lines.unwrap_or(2000);
    let end = total_lines.min(start.saturating_add(limit));
    let returned_lines = if start < total_lines { end - start } else { 0 };
    let sliced = if returned_lines == 0 {
        String::new()
    } else {
        lines[start..end].join("\n")
    };

    ToolCallResult::ok(serde_json::json!({
        "path": path_str,
        "content": sliced,
        "sha256": hash,
        "total_lines": total_lines,
        "start_line": start + 1,
        "returned_lines": returned_lines
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_read_file_hashing_and_slicing() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(
            dir.path().join("hello.txt"),
            "line 1\nline 2\nline 3\nline 4",
        )
        .unwrap();

        let res = handle_read_file(&ws, "hello.txt", Some(2), Some(2), 1024);
        assert!(res.success);
        let data = res.data.unwrap();
        assert_eq!(data["content"], "line 2\nline 3");
        assert_eq!(data["total_lines"], 4);
        assert_eq!(data["returned_lines"], 2);
        assert!(!data["sha256"].as_str().unwrap().is_empty());
    }

    #[test]
    fn test_read_file_start_past_eof_returns_empty_slice() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(dir.path().join("short.txt"), "one\ntwo\n").unwrap();

        let res = handle_read_file(&ws, "short.txt", Some(99), Some(5), 1024);
        assert!(res.success);
        let data = res.data.unwrap();
        assert_eq!(data["content"], "");
        assert_eq!(data["returned_lines"], 0);
        assert_eq!(data["total_lines"], 2);
    }

    #[test]
    fn test_read_file_enforces_configured_byte_limit() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(dir.path().join("large.txt"), "12345").unwrap();

        let res = handle_read_file(&ws, "large.txt", None, None, 4);
        assert!(!res.success);
        assert!(res.error.unwrap().contains("configured read limit"));
    }

    #[cfg(unix)]
    #[test]
    fn test_read_file_capability_denies_symlink_escape() {
        use std::os::unix::fs::symlink;

        let workspace_dir = tempdir().unwrap();
        let outside_dir = tempdir().unwrap();
        fs::write(outside_dir.path().join("secret.txt"), "secret").unwrap();
        symlink(outside_dir.path(), workspace_dir.path().join("escape")).unwrap();
        let ws = Workspace::open(workspace_dir.path()).unwrap();

        let res = handle_read_file(&ws, "escape/secret.txt", None, None, 1024);
        assert!(!res.success);
    }
}
