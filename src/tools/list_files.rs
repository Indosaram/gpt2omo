use crate::security::{PathPolicy, Workspace};
use crate::tools::{ToolCallResult, SKIPPED_DIR_NAMES};
use ignore::WalkBuilder;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const LIST_TIME_BUDGET: Duration = Duration::from_secs(30);

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

    let effective_depth = max_depth.unwrap_or(8);
    let walk_roots = walk_roots(ws, &target_dir, effective_depth);
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    let max_results = limit.unwrap_or(500).clamp(1, 5000);
    let started = Instant::now();

    'roots: for (walk_root, remaining_depth) in walk_roots {
        let mut builder = WalkBuilder::new(&walk_root);
        builder.hidden(false);
        builder.filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            if entry
                .file_type()
                .is_some_and(|ft| ft.is_dir())
                && SKIPPED_DIR_NAMES.contains(&name.as_ref())
            {
                return false;
            }
            if name.starts_with('.') {
                if PathPolicy::is_allowed_dot_component(&name) {
                    return !PathPolicy::is_secret_component(&name);
                }
                return false;
            }
            !PathPolicy::is_secret_component(&name)
        });
        builder.git_ignore(true);
        builder.follow_links(false);
        builder.max_depth(Some(remaining_depth));

        for entry in builder.build().flatten() {
            if started.elapsed() >= LIST_TIME_BUDGET {
                break 'roots;
            }
            let Ok(rel) = entry.path().strip_prefix(ws.root()) else {
                continue;
            };
            let rel_str = rel.to_string_lossy();
            if rel_str.is_empty() || PathPolicy::sanitize_relative_path(&rel_str).is_err() {
                continue;
            }
            if !seen.insert(rel_str.to_string()) {
                continue;
            }
            entries.push(serde_json::json!({
                "path": rel_str,
                "is_dir": entry.file_type().map(|t| t.is_dir()).unwrap_or(false),
            }));
            if entries.len() >= max_results {
                break 'roots;
            }
        }
    }

    ToolCallResult::ok(serde_json::json!({
        "root": ws.root().to_string_lossy(),
        "entries": entries,
        "count": entries.len()
    }))
}

fn walk_roots(ws: &Workspace, target_dir: &Path, max_depth: usize) -> Vec<(PathBuf, usize)> {
    let mut roots = vec![(target_dir.to_path_buf(), max_depth)];
    if max_depth == 0 {
        return roots;
    }

    let Ok(children) = std::fs::read_dir(target_dir) else {
        return roots;
    };
    for child in children.flatten() {
        let name = child.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with('.')
            || !PathPolicy::is_allowed_dot_component(&name)
            || PathPolicy::is_secret_component(&name)
        {
            continue;
        }
        let path = child.path();
        let Ok(rel) = path.strip_prefix(ws.root()) else {
            continue;
        };
        let rel = rel.to_string_lossy();
        if ws.resolve_relative(&rel).is_ok() {
            // A directly targeted dot-component must remain discoverable even when a parent
            // .gitignore entry (for example `.omo/`) hides it from the primary walk.
            roots.push((path, max_depth - 1));
        }
    }
    roots
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
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".gitignore"), ".omo/\n").unwrap();
        fs::create_dir_all(dir.path().join(".omo/plans")).unwrap();
        fs::write(dir.path().join(".omo/plans/test.md"), "plan").unwrap();
        fs::write(dir.path().join(".omo/auth.json"), "secret").unwrap();
        fs::write(dir.path().join(".omo/SERVER.PEM"), "secret").unwrap();

        let res = handle_list_files(&ws, None, None, None);
        assert!(res.success);
        let data = res.data.unwrap();
        let entries = data["entries"].as_array().unwrap();
        assert!(entries.iter().any(|e| e["path"] == "main.rs"));
        assert!(entries.iter().any(|e| e["path"] == ".omo/plans/test.md"));
        assert!(!entries.iter().any(|e| e["path"] == ".hidden"));
        assert!(!entries.iter().any(|e| e["path"] == ".omo/auth.json"));
        assert!(!entries.iter().any(|e| e["path"] == ".omo/SERVER.PEM"));
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
