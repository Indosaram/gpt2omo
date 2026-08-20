use crate::security::{PathPolicy, Workspace};
use crate::tools::{ToolCallResult, SKIPPED_DIR_NAMES};
use ignore::WalkBuilder;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const MAX_SEARCH_FILE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_PREVIEW_CHARS: usize = 300;
const DEFAULT_MAX_DEPTH: usize = 16;
const BINARY_CHECK_BYTES: usize = 8 * 1024;
/// Hard wall-clock budget so giant workspaces (hundreds of GB) return partial
/// results inside the MCP client call timeout instead of hanging the caller.
const SEARCH_TIME_BUDGET: Duration = Duration::from_secs(45);

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
    let needle_buf;
    let needle: &str = if case_sensitive {
        query
    } else {
        needle_buf = query.to_lowercase();
        &needle_buf
    };
    let cap = max_results.unwrap_or(80).clamp(1, 500);
    let walk_roots = walk_roots(ws, &target, DEFAULT_MAX_DEPTH);

    let mut matches = Vec::new();
    let mut seen_files = HashSet::new();
    let mut files_scanned = 0usize;
    let mut skipped_large = 0usize;
    let mut line_buf = String::new();
    let started = Instant::now();
    let mut deadline_hit = false;
    let mut entries_walked = 0usize;

    'roots: for (walk_root, remaining_depth) in walk_roots {
        let mut builder = WalkBuilder::new(&walk_root);
        builder.hidden(false);
        builder.filter_entry(|entry| {
            let name = entry.file_name().to_string_lossy();
            if entry.file_type().is_some_and(|ft| ft.is_dir())
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
            entries_walked += 1;
            if entries_walked.is_multiple_of(64) && started.elapsed() >= SEARCH_TIME_BUDGET {
                deadline_hit = true;
                break 'roots;
            }
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
            let file_len = metadata.len();
            if file_len > MAX_SEARCH_FILE_BYTES {
                skipped_large += 1;
                continue;
            }

            let mut file = match fs::File::open(entry.path()) {
                Ok(f) => f,
                Err(_) => continue,
            };
            let file_len = file_len as usize;
            let check_budget = BINARY_CHECK_BYTES.min(file_len);
            let mut initial_buf = vec![0u8; check_budget];
            if file.read_exact(&mut initial_buf).is_err() {
                continue;
            }
            if initial_buf.contains(&0) {
                continue;
            }
            let bytes = if file_len == check_budget {
                initial_buf
            } else {
                let mut bytes = Vec::with_capacity(file_len);
                bytes.extend_from_slice(&initial_buf);
                if file.read_to_end(&mut bytes).is_err() {
                    continue;
                }
                bytes
            };
            let Ok(text) = String::from_utf8(bytes) else {
                continue;
            };
            files_scanned += 1;

            for (idx, line) in text.lines().enumerate() {
                let byte_offset = if case_sensitive {
                    line.find(needle)
                } else {
                    line_buf.clear();
                    line_buf.extend(line.chars().flat_map(char::to_lowercase));
                    line_buf.find(needle)
                };

                let Some(offset) = byte_offset else {
                    continue;
                };

                let column = if case_sensitive {
                    line[..offset].chars().count() + 1
                } else {
                    column_for_insensitive_match(line, &line_buf, offset)
                };

                matches.push(serde_json::json!({
                    "path": rel,
                    "line": idx + 1,
                    "column": column,
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
        "truncated": matches.len() >= cap || deadline_hit,
        "time_budget_exceeded": deadline_hit,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    }))
}

fn column_for_insensitive_match(line: &str, line_buf: &str, byte_offset: usize) -> usize {
    let target_char_count = line_buf[..byte_offset].chars().count();
    let mut accumulated = 0;
    for (char_idx, c) in line.chars().enumerate() {
        if accumulated >= target_char_count {
            return char_idx + 1;
        }
        accumulated += c.to_lowercase().count();
    }
    line.chars().count() + 1
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

    #[test]
    fn test_search_text_skips_binary_files() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let mut binary_content = vec![0u8; 100];
        binary_content[10] = 0;
        binary_content.extend_from_slice(b"needle in binary");
        fs::write(dir.path().join("bin_file.dat"), binary_content).unwrap();

        fs::write(dir.path().join("text_file.txt"), "needle in text\n").unwrap();

        let res = handle_search_text(&ws, "needle", None, Some(true), None);
        assert!(res.success);
        let data = res.data.unwrap();
        assert_eq!(data["match_count"], 1);
        assert_eq!(data["files_scanned"], 1);
        assert_eq!(data["matches"][0]["path"], "text_file.txt");
    }

    #[test]
    fn test_search_text_unicode_column_calculation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        fs::write(
            dir.path().join("unicode.txt"),
            "🦀needle\ncafé needle\n🔥NEEDLE\n",
        )
        .unwrap();

        // Case-sensitive
        let sensitive = handle_search_text(&ws, "needle", None, Some(true), None);
        assert!(sensitive.success);
        let data = sensitive.data.unwrap();
        let matches = data["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 2);
        // Line 1: '🦀' is 1 char -> column is 2
        assert_eq!(matches[0]["line"], 1);
        assert_eq!(matches[0]["column"], 2);
        // Line 2: 'c','a','f','é',' ' is 5 chars -> column is 6
        assert_eq!(matches[1]["line"], 2);
        assert_eq!(matches[1]["column"], 6);

        // Case-insensitive
        let insensitive = handle_search_text(&ws, "needle", None, Some(false), None);
        assert!(insensitive.success);
        let data_ins = insensitive.data.unwrap();
        let matches_ins = data_ins["matches"].as_array().unwrap();
        assert_eq!(matches_ins.len(), 3);
        assert_eq!(matches_ins[0]["line"], 1);
        assert_eq!(matches_ins[0]["column"], 2);
        assert_eq!(matches_ins[1]["line"], 2);
        assert_eq!(matches_ins[1]["column"], 6);
        assert_eq!(matches_ins[2]["line"], 3);
        assert_eq!(matches_ins[2]["column"], 2);
    }
}
