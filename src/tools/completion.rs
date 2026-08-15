use crate::security::Workspace;
use crate::tools::git_status::check_worktree_whitespace;
use crate::tools::task_state::{load_task_state, TaskStatus};
use crate::tools::ToolCallResult;
use std::process::Command;

pub fn handle_completion_check(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
) -> ToolCallResult {
    let require_task_plan = require_task_plan.unwrap_or(true);
    let require_verification = require_verification.unwrap_or(true);
    let require_changes = require_changes.unwrap_or(false);

    let state = match load_task_state(ws, scope_id) {
        Ok(state) => state,
        Err(e) => return ToolCallResult::err(e),
    };

    let mut blockers = Vec::<String>::new();
    let mut incomplete_items = Vec::new();
    let mut verification_evidence = None;

    match &state {
        Some(state) => {
            for item in &state.items {
                if item.status != TaskStatus::Done {
                    incomplete_items.push(serde_json::json!({
                        "id": item.id,
                        "title": item.title,
                        "status": item.status,
                        "note": item.note,
                    }));
                }
            }

            if require_task_plan && !incomplete_items.is_empty() {
                blockers.push(format!(
                    "{} task-plan item(s) are not done",
                    incomplete_items.len()
                ));
            }

            if require_verification {
                let threshold = state.last_mutation_ms.unwrap_or(state.created_ms);
                verification_evidence = state
                    .verifications
                    .iter()
                    .rev()
                    .find(|record| record.success && record.timestamp_ms >= threshold)
                    .cloned();

                if verification_evidence.is_none() {
                    blockers.push(
                        "No successful verification command has run since the latest bridge edit"
                            .into(),
                    );
                }
            }
        }
        None => {
            if require_task_plan {
                blockers.push("No active task plan exists".into());
            }
            if require_verification {
                blockers.push("No task state exists to provide verification evidence".into());
            }
        }
    }

    let git_status = git_output(ws, &["status", "--porcelain", "--untracked-files=all"]);
    let (git_status_text, git_status_ok) = match git_status {
        Ok(text) => (text, true),
        Err(e) => (e, false),
    };

    if require_changes && git_status_ok && git_status_text.trim().is_empty() {
        blockers.push("No working-tree changes are present".into());
    }
    if require_changes && !git_status_ok {
        blockers.push("Unable to verify working-tree changes with git status".into());
    }

    let worktree_check = check_worktree_whitespace(ws);
    if !worktree_check.ok {
        blockers.push("Working tree contains whitespace/errors".into());
    }

    let ready = blockers.is_empty();
    ToolCallResult::ok(serde_json::json!({
        "ready": ready,
        "blockers": blockers,
        "incomplete_items": incomplete_items,
        "verification_evidence": verification_evidence,
        "last_mutation_ms": state.as_ref().and_then(|s| s.last_mutation_ms),
        "last_mutation_path": state.as_ref().and_then(|s| s.last_mutation_path.clone()),
        "git": {
            "status_ok": git_status_ok,
            "status": git_status_text,
            "diff_check_ok": worktree_check.ok,
            "diff_check_output": worktree_check.output,
        },
        "requirements": {
            "task_plan": require_task_plan,
            "verification": require_verification,
            "changes": require_changes,
        }
    }))
}

fn git_output(ws: &Workspace, args: &[&str]) -> std::result::Result<String, String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(ws.root())
        .output()
        .map_err(|e| format!("Failed to execute git: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {} failed: {}", args.join(" "), stderr.trim()));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::task_state::{
        handle_task_plan, handle_task_update, record_mutation, record_verification,
    };
    use std::fs;
    use tempfile::tempdir;

    const SCOPE: &str = "22222222-2222-4222-8222-222222222222";

    fn init_git(path: &std::path::Path) {
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap();
    }

    #[test]
    fn test_completion_requires_done_plan_and_fresh_verification() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();

        handle_task_plan(
            &ws,
            SCOPE,
            "Implement",
            vec!["Edit".into(), "Verify".into()],
        );
        record_mutation(&ws, SCOPE, "src/lib.rs");

        let before = handle_completion_check(&ws, SCOPE, None, None, None);
        assert!(before.success);
        assert!(!before.data.unwrap()["ready"].as_bool().unwrap());

        handle_task_update(&ws, SCOPE, "T1", "done", None);
        handle_task_update(&ws, SCOPE, "T2", "done", None);
        record_verification(&ws, SCOPE, "cargo test", true, Some(0), 10);

        let after = handle_completion_check(&ws, SCOPE, None, None, None);
        assert!(after.success);
        assert!(after.data.unwrap()["ready"].as_bool().unwrap());
    }

    #[test]
    fn test_completion_can_be_used_without_plan_for_simple_read_only_work() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        assert!(result.data.unwrap()["ready"].as_bool().unwrap());
    }

    #[test]
    fn test_completion_without_plan_cannot_claim_verification_evidence() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(true), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        assert!(!data["ready"].as_bool().unwrap());
        assert!(data["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("verification evidence")));
    }

    #[test]
    fn test_completion_checks_untracked_whitespace() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("bad.txt"), "trailing space \n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        assert!(!data["ready"].as_bool().unwrap());
        assert_eq!(data["git"]["diff_check_ok"], false);
        assert!(data["git"]["diff_check_output"]
            .as_str()
            .unwrap()
            .contains("bad.txt:1: trailing whitespace"));
    }
}
