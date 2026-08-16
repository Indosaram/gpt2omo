use crate::security::{PathPolicy, Workspace};
use crate::tools::ToolCallResult;
use ignore::WalkBuilder;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

const MAX_SEARCH_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_PREVIEW_CHARS: usize = 300;
const DEFAULT_MAX_DEPTH: usize = 16;

pub fn handle_search_text(
    ws: &Workspace,
    query: &str,
    subpath: Option<&str>,
    case_sensitive: Option<bool>,
    max_results: Option<usize>,
) -> ToolCallResult {
    let query = query.trim();
    if query.is_empty() {
        return ToolCallResult::err("Search query cannot be empty");
    }
    if query.chars().count() > 500 {
        return ToolCallResult::err("Search query is too long (maximum 500 characters)");
    }

    let target = match subpath {
        Some(s) if !s.trim().is_empty() && s.trim() != "." => match ws.resolve_relative(s) {
            Ok(p) => p,
            Err(e) => return ToolCallResult::err(e.to_string()),
        },
        _ => ws.root().to_path_buf(),
    };

    let case_sensitive = case_sensitive.unwrap_or(false);
    let needle = if case_sensitive {
        query.to_string()
    } else {
        query.to_lowercase()
    };
    let cap = max_results.unwrap_or(80).clamp(1, 500);
    let walk_roots = walk_roots(ws, &target, DEFAULT_MAX_DEPTH);

    let mut matches = Vec::new();
    let mut seen_files = HashSet::new();
    let mut files_scanned = 0usize;
    let mut skipped_large = 0usize;

    'roots: for (walk_root, remaining_depth) in walk_roots {
        let mut builder = WalkBuilder::new(&walk_root);
        builder.hidden(false);
        builder.filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
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
            let Some(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            let rel = entry
                .path()
                .strip_prefix(ws.root())
                .unwrap_or(entry.path())
                .to_string_lossy()
                .to_string();

            if PathPolicy::sanitize_relative_path(&rel).is_err() || !seen_files.insert(rel.clone())
            {
                continue;
            }

            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.len() > MAX_SEARCH_FILE_BYTES {
                skipped_large += 1;
                continue;
            }

            let Ok(bytes) = fs::read(entry.path()) else {
                continue;
            };
            let Ok(text) = String::from_utf8(bytes) else {
                continue;
            };
            files_scanned += 1;

            for (idx, line) in text.lines().enumerate() {
                let haystack = if case_sensitive {
                    line.to_string()
                } else {
                    line.to_lowercase()
                };
                let Some(column) = haystack.find(&needle) else {
                    continue;
                };

                matches.push(serde_json::json!({
                    "path": rel,
                    "line": idx + 1,
                    "column": column + 1,
                    "preview": truncate_preview(line),
                }));

                if matches.len() >= cap {
                    break 'roots;
                }
            }
        }
    }

    ToolCallResult::ok(serde_json::json!({
        "query": query,
        "case_sensitive": case_sensitive,
        "matches": matches,
        "match_count": matches.len(),
        "files_scanned": files_scanned,
        "skipped_large_files": skipped_large,
        "truncated": matches.len() >= cap,
    }))
}

fn walk_roots(ws: &Workspace, target: &Path, max_depth: usize) -> Vec<(PathBuf, usize)> {
    let mut roots = vec![(target.to_path_buf(), max_depth)];
    if max_depth == 0 || !target.is_dir() {
        return roots;
    }

    let Ok(children) = fs::read_dir(target) else {
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
            // Explicitly re-root allowed dot-components so parent .gitignore rules cannot hide
            // approved files such as `.omo/plans/*` from a workspace-root search.
            roots.push((path, max_depth - 1));
        }
    }
    roots
}

fn truncate_preview(line: &str) -> String {
    if line.chars().count() <= MAX_PREVIEW_CHARS {
        return line.to_string();
    }

    let mut preview: String = line.chars().take(MAX_PREVIEW_CHARS).collect();
    preview.push('…');
    preview
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_search_text_finds_lines_and_respects_case() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(
            dir.path().join("sample.txt"),
            "Alpha needle\nsecond line\nNEEDLE again\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".gitignore"), ".omo/\n").unwrap();
        fs::create_dir_all(dir.path().join(".omo/plans")).unwrap();
        fs::write(
            dir.path().join(".omo/plans/feature.md"),
            "# Feature plan with needle inside",
        )
        .unwrap();
        fs::write(
            dir.path().join(".omo/SECRET.PEM"),
            "needle must never be searchable",
        )
        .unwrap();

        let insensitive = handle_search_text(&ws, "needle", None, Some(false), None);
        assert!(insensitive.success);
        assert_eq!(insensitive.data.unwrap()["match_count"], 3);

        let sensitive = handle_search_text(&ws, "needle", None, Some(true), None);
        assert!(sensitive.success);
        assert_eq!(sensitive.data.unwrap()["match_count"], 2);
    }

    #[test]
    fn test_search_text_rejects_traversal() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_search_text(&ws, "x", Some("../outside"), None, None);
        assert!(!result.success);
    }
}
