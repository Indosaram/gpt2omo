use crate::security::{PathPolicy, Workspace};
use crate::tools::ToolCallResult;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

pub fn handle_patch_file(
    ws: &Workspace,
    path_str: &str,
    expected_sha256: Option<&str>,
    content: &str,
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

    match dir.read(&rel_path) {
        Ok(current_bytes) => {
            let Some(expected) = expected_sha256 else {
                return ToolCallResult::err(
                    "Precondition required when replacing an existing file".into(),
                );
            };
            let current_hash = format!("{:x}", Sha256::digest(&current_bytes));
            if current_hash != expected {
                return ToolCallResult::err(format!(
                    "Precondition failed: expected sha256 {}, but current file hash is {}",
                    expected, current_hash
                ));
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return ToolCallResult::err(format!(
                "Failed to read existing file for precondition: {}",
                e
            ))
        }
    }

    let parent = rel_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new(""));
    if !parent.as_os_str().is_empty() {
        if let Err(e) = dir.create_dir_all(parent) {
            return ToolCallResult::err(format!("Failed to create parent directory: {}", e));
        }
    }

    let temp_name = format!(".tmp-patch-{}", uuid::Uuid::new_v4());
    let temp_rel = if parent.as_os_str().is_empty() {
        PathBuf::from(&temp_name)
    } else {
        parent.join(&temp_name)
    };

    if let Err(e) = dir.write(&temp_rel, content.as_bytes()) {
        return ToolCallResult::err(format!("Failed to write temp file: {}", e));
    }

    if let Err(e) = dir.rename(&temp_rel, &dir, &rel_path) {
        let _ = dir.remove_file(&temp_rel);
        return ToolCallResult::err(format!("Failed to atomically rename file: {}", e));
    }

    let new_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
    ToolCallResult::ok(serde_json::json!({
        "path": path_str,
        "sha256": new_hash,
        "size_bytes": content.len()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_patch_file_atomic_and_precondition() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let res1 = handle_patch_file(&ws, "test.txt", None, "initial content");
        assert!(res1.success);
        let sha1 = res1.data.unwrap()["sha256"].as_str().unwrap().to_string();

        let missing_precondition = handle_patch_file(&ws, "test.txt", None, "unsafe overwrite");
        assert!(!missing_precondition.success);
        assert!(missing_precondition
            .error
            .unwrap()
            .contains("Precondition required"));

        let res2 = handle_patch_file(&ws, "test.txt", Some(&sha1), "updated content");
        assert!(res2.success);

        let res3 = handle_patch_file(&ws, "test.txt", Some("wrong_hash"), "should fail");
        assert!(!res3.success);
        assert!(res3.error.unwrap().contains("Precondition failed"));
    }

    #[cfg(unix)]
    #[test]
    fn test_patch_file_capability_denies_symlink_escape() {
        use std::fs;
        use std::os::unix::fs::symlink;

        let workspace_dir = tempdir().unwrap();
        let outside_dir = tempdir().unwrap();
        symlink(outside_dir.path(), workspace_dir.path().join("escape")).unwrap();
        let ws = Workspace::open(workspace_dir.path()).unwrap();

        let res = handle_patch_file(&ws, "escape/new.txt", None, "do not escape");
        assert!(!res.success);
        assert!(!outside_dir.path().join("new.txt").exists());
        assert!(fs::read_dir(outside_dir.path()).unwrap().next().is_none());
    }
}
