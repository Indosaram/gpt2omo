use crate::security::Workspace;
use crate::tools::ToolCallResult;
use ignore::WalkBuilder;

pub fn handle_list_files(
    ws: &Workspace,
    subpath: Option<&str>,
    max_depth: Option<usize>,
    limit: Option<usize>,
) -> ToolCallResult {
    let target_dir = match subpath {
        Some(s) if !s.trim().is_empty() && s.trim() != "." => match ws.resolve_relative(s) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::err(e.to_string()),
        },
        _ => ws.root().to_path_buf(),
    };

    if !target_dir.is_dir() {
        return ToolCallResult::err("Requested list path is not a directory");
    }

    let mut builder = WalkBuilder::new(&target_dir);
    builder.hidden(true); // ignore hidden files
    builder.git_ignore(true);
    builder.follow_links(false);
    builder.max_depth(max_depth.or(Some(8)));

    let mut entries = Vec::new();
    let max_results = limit.unwrap_or(500).clamp(1, 5000);

    for entry in builder.build().flatten() {
        if let Ok(rel) = entry.path().strip_prefix(ws.root()) {
            let rel_str = rel.to_string_lossy();
            if !rel_str.is_empty() && !rel_str.starts_with('.') {
                entries.push(serde_json::json!({
                    "path": rel_str,
                    "is_dir": entry.file_type().map(|t| t.is_dir()).unwrap_or(false),
                }));
                if entries.len() >= max_results {
                    break;
                }
            }
        }
    }

    ToolCallResult::ok(serde_json::json!({
        "root": ws.root().to_string_lossy(),
        "entries": entries,
        "count": entries.len()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_list_files_respects_hidden_and_limits() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
        fs::write(dir.path().join(".hidden"), "secret").unwrap();

        let res = handle_list_files(&ws, None, None, None);
        assert!(res.success);
        let data = res.data.unwrap();
        let entries = data["entries"].as_array().unwrap();
        assert!(entries.iter().any(|e| e["path"] == "main.rs"));
        assert!(!entries.iter().any(|e| e["path"] == ".hidden"));
    }

    #[test]
    fn test_list_files_accepts_dot_as_workspace_root() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(dir.path().join("main.rs"), "fn main() {}").unwrap();
        let res = handle_list_files(&ws, Some("."), None, None);
        assert!(res.success);
    }
}
