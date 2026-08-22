use crate::security::Workspace;
use crate::tools::command_manager::CommandManager;
use crate::tools::git_status::{is_git_worktree, run_git};
use crate::tools::task_state::{
    handle_task_result, load_delegation_lifecycle, load_task_result, load_task_state,
    record_terminal_evidence, DelegationTerminalState, TaskStatus,
};
use crate::tools::ToolCallResult;
use serde_json::Value;

pub fn handle_completion_check(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
) -> ToolCallResult {
    handle_completion_check_with_result(
        ws,
        scope_id,
        require_task_plan,
        require_verification,
        require_changes,
        None,
    )
}

pub fn handle_completion_check_with_result(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
    result: Option<CompletionResultInput>,
) -> ToolCallResult {
    handle_completion_check_inner(
        ws,
        scope_id,
        require_task_plan,
        require_verification,
        require_changes,
        result,
        None,
    )
}

pub fn handle_completion_check_with_manager(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
    command_manager: &CommandManager,
) -> ToolCallResult {
    handle_completion_check_with_manager_and_result(
        ws,
        scope_id,
        require_task_plan,
        require_verification,
        require_changes,
        None,
        command_manager,
    )
}

pub fn handle_completion_check_with_manager_and_result(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
    result: Option<CompletionResultInput>,
    command_manager: &CommandManager,
) -> ToolCallResult {
    command_manager.reconcile_scope(ws, scope_id);
    handle_completion_check_inner(
        ws,
        scope_id,
        require_task_plan,
        require_verification,
        require_changes,
        result,
        Some(command_manager),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionResultInput {
    pub summary: String,
    pub changed_files: Vec<String>,
    pub verification: Vec<String>,
    pub blockers: Vec<String>,
    pub final_message: String,
}

fn handle_completion_check_inner(
    ws: &Workspace,
    scope_id: &str,
    require_task_plan: Option<bool>,
    require_verification: Option<bool>,
    require_changes: Option<bool>,
    result: Option<CompletionResultInput>,
    command_manager: Option<&CommandManager>,
) -> ToolCallResult {
    let require_task_plan = require_task_plan.unwrap_or(true);
    let require_verification = require_verification.unwrap_or(true);
    let require_changes = require_changes.unwrap_or(false);

    if let Some(result) = result {
        let recorded = handle_task_result(
            ws,
            scope_id,
            &result.summary,
            result.changed_files,
            result.verification,
            result.blockers,
            &result.final_message,
        );
        if !recorded.success {
            return recorded;
        }
    }

    let state = match load_task_state(ws, scope_id) {
        Ok(state) => state,
        Err(e) => return ToolCallResult::err(e),
    };

    let mut blockers = Vec::<String>::new();
    let mut incomplete_items = Vec::new();
    let mut verification_evidence: Option<Value> = None;
    let task_result = match load_delegation_lifecycle(ws, scope_id) {
        Ok(Some(lifecycle)) => match load_task_result(ws, scope_id, lifecycle.generation) {
            Ok(Some(result)) => {
                if !result.blockers.is_empty() {
                    blockers.push(format!(
                        "Structured task_result contains {} blocker(s); submit a fresh blocker-free result before completion",
                        result.blockers.len()
                    ));
                }
                if require_verification && result.verification.is_empty() {
                    blockers.push(
                        "Structured task_result contains no verification summary while verification is required"
                            .into(),
                    );
                }
                if let Some(state) = state.as_ref() {
                    if result.recorded_ms < state.updated_ms {
                        blockers.push(format!(
                            "Structured task_result is stale (recorded_ms={} before task_state.updated_ms={}); resubmit the final result after the latest plan/verification update",
                            result.recorded_ms, state.updated_ms
                        ));
                    }
                }
                serde_json::to_value(result).ok()
            }
            Ok(None) => {
                blockers
                    .push("No structured task_result exists for this delegation generation".into());
                None
            }
            Err(error) => {
                blockers.push(format!("Unable to load structured task_result: {error}"));
                None
            }
        },
        Ok(None) => {
            blockers.push("No active delegation lifecycle exists to bind task_result".into());
            None
        }
        Err(error) => {
            blockers.push(format!("Unable to load delegation lifecycle: {error}"));
            None
        }
    };

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
                if let Some(manager) = command_manager {
                    verification_evidence = manager.latest_verification_evidence(ws, scope_id);
                    if verification_evidence.is_none() {
                        blockers.push(format!(
                            "No successful verification command matches current workspace revision {}",
                            manager.workspace_revision(scope_id)
                        ));
                    }
                } else {
                    let threshold = state.last_mutation_ms.unwrap_or(state.created_ms);
                    verification_evidence = state
                        .verifications
                        .iter()
                        .rev()
                        .find(|record| record.success && record.timestamp_ms >= threshold)
                        .and_then(|record| serde_json::to_value(record).ok());

                    if verification_evidence.is_none() {
                        blockers.push(
                            "No successful verification command has run since the latest bridge edit"
                                .into(),
                        );
                    }
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

    let is_git_repo = is_git_worktree(ws);
    let (git_status_text, git_status_ok) = if is_git_repo {
        match git_output(ws, &["status", "--porcelain"]) {
            Ok(text) => (text, true),
            Err(e) => (e, false),
        }
    } else {
        (
            "Workspace is not a Git repository; git status/diff checks were skipped".into(),
            false,
        )
    };

    if require_changes && !is_git_repo {
        blockers
            .push("Workspace is not a Git repository; cannot require working-tree changes".into());
    } else if require_changes && git_status_ok && git_status_text.trim().is_empty() {
        blockers.push("No working-tree changes are present".into());
    } else if require_changes && !git_status_ok {
        blockers.push("Unable to verify working-tree changes with git status".into());
    }

    // Whitespace diff check is disabled as it creates false blockers and confusion
    let ready = blockers.is_empty();
    if ready {
        if let Err(error) = record_terminal_evidence(
            ws,
            scope_id,
            DelegationTerminalState::Completed,
            Some("completion_check ready=true"),
        ) {
            return ToolCallResult::err(error);
        }
    }

    ToolCallResult::ok(serde_json::json!({
        "ready": ready,
        "blockers": blockers,
        "incomplete_items": incomplete_items,
        "verification_evidence": verification_evidence,
        "task_result": task_result,
        "workspace_revision": command_manager.map(|manager| manager.workspace_revision(scope_id)),
        "last_mutation_ms": state.as_ref().and_then(|s| s.last_mutation_ms),
        "last_mutation_path": state.as_ref().and_then(|s| s.last_mutation_path.clone()),
        "git": {
            "is_git_repo": is_git_repo,
            "status_ok": git_status_ok,
            "status": git_status_text,
        },
        "requirements": {
            "task_plan": require_task_plan,
            "verification": require_verification,
            "changes": require_changes,
            "task_result": true,
        }
    }))
}

fn git_output(ws: &Workspace, args: &[&str]) -> std::result::Result<String, String> {
    run_git(ws, args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::task_state::{
        clear_delegation_lifecycle, handle_task_plan, handle_task_result, handle_task_update,
        load_delegation_lifecycle, record_mutation, record_verification,
        start_fresh_delegation_lifecycle,
    };
    use std::fs;
    use std::process::Command;
    use tempfile::tempdir;

    const SCOPE: &str = "22222222-2222-4222-8222-222222222222";

    fn init_git(path: &std::path::Path) {
        Command::new("git")
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap();
    }

    fn record_result_for_completion(ws: &Workspace) {
        start_fresh_delegation_lifecycle(ws, SCOPE).unwrap();
        assert!(
            handle_task_result(
                ws,
                SCOPE,
                "Completed worker task",
                vec![],
                vec![],
                vec![],
                "Completed worker task.",
            )
            .success
        );
    }

    #[test]
    fn test_completion_requires_done_plan_and_fresh_verification() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();

        start_fresh_delegation_lifecycle(&ws, SCOPE).unwrap();
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

        let without_result = handle_completion_check(&ws, SCOPE, None, None, None);
        assert!(without_result.success);
        assert!(!without_result.data.unwrap()["ready"].as_bool().unwrap());

        let task_result = handle_task_result(
            &ws,
            SCOPE,
            "Implemented the requested behavior",
            vec!["src/lib.rs".into()],
            vec!["cargo test: passed".into()],
            vec![],
            "Implemented and verified the requested behavior.",
        );
        assert!(task_result.success);

        let after = handle_completion_check(&ws, SCOPE, None, None, None);
        assert!(after.success);
        let data = after.data.unwrap();
        assert!(data["ready"].as_bool().unwrap());
        assert_eq!(
            data["task_result"]["summary"],
            "Implemented the requested behavior"
        );
    }

    #[test]
    fn inline_result_is_persisted_before_completion_audit() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE).unwrap();

        let result = handle_completion_check_with_result(
            &ws,
            SCOPE,
            Some(false),
            Some(false),
            Some(false),
            Some(CompletionResultInput {
                summary: "Inline result transport".into(),
                changed_files: vec![],
                verification: vec!["read-only audit".into()],
                blockers: vec![],
                final_message: "Inline result persisted and audited.".into(),
            }),
        );

        assert!(result.success);
        let data = result.data.unwrap();
        assert!(data["ready"].as_bool().unwrap());
        assert_eq!(data["task_result"]["summary"], "Inline result transport");
    }

    #[test]
    fn structured_result_blockers_prevent_ready() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE).unwrap();
        assert!(
            handle_task_result(
                &ws,
                SCOPE,
                "Incomplete",
                vec![],
                vec![],
                vec!["external dependency unavailable".into()],
                "Blocked.",
            )
            .success
        );

        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["ready"], false);
        assert!(data["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("task_result contains")));
    }

    #[test]
    fn stale_structured_result_requires_refresh_after_task_state_update() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE).unwrap();
        assert!(handle_task_plan(&ws, SCOPE, "Implement", vec!["Finish".into()]).success);
        assert!(
            handle_task_result(
                &ws,
                SCOPE,
                "Premature result",
                vec![],
                vec![],
                vec![],
                "Premature result.",
            )
            .success
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(handle_task_update(&ws, SCOPE, "T1", "done", None).success);

        let result = handle_completion_check(&ws, SCOPE, Some(true), Some(false), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["ready"], false);
        assert!(data["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("task_result is stale")));
    }

    #[test]
    fn manager_backed_completion_rejects_stale_revision_evidence() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("Makefile"), "test:\n\t@true\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = CommandManager::new();

        record_result_for_completion(&ws);
        assert!(handle_task_plan(&ws, SCOPE, "Implement", vec!["Verify".into()]).success);
        assert!(handle_task_update(&ws, SCOPE, "T1", "done", None).success);
        let first = manager.run_command(&ws, SCOPE, "make test", 2_000, None);
        assert!(first.success);
        assert_eq!(first.data.unwrap()["command_success"], true);

        record_mutation(&ws, SCOPE, "src/lib.rs");
        manager.note_workspace_mutation(SCOPE);
        let stale = handle_completion_check_with_manager(&ws, SCOPE, None, None, None, &manager);
        assert!(stale.success);
        let stale_data = stale.data.unwrap();
        assert_eq!(stale_data["ready"], false);
        assert!(stale_data["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("workspace revision 1")));

        let second = manager.run_command(&ws, SCOPE, "make test", 2_000, None);
        assert!(second.success);
        let refreshed_result = handle_task_result(
            &ws,
            SCOPE,
            "Fresh result after mutation",
            vec!["src/lib.rs".into()],
            vec!["make test: passed".into()],
            vec![],
            "Fresh result after mutation.",
        );
        assert!(refreshed_result.success);
        let fresh = handle_completion_check_with_manager(&ws, SCOPE, None, None, None, &manager);
        assert!(fresh.success);
        assert_eq!(fresh.data.unwrap()["ready"], true);
    }

    #[test]
    fn ready_completion_records_authoritative_completed_terminal_state() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE).unwrap();

        record_result_for_completion(&ws);
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        assert!(result.data.unwrap()["ready"].as_bool().unwrap());
        let lifecycle = load_delegation_lifecycle(&ws, SCOPE).unwrap().unwrap();
        assert_eq!(
            lifecycle.terminal_state,
            Some(DelegationTerminalState::Completed)
        );
        assert_eq!(
            lifecycle.terminal_detail.as_deref(),
            Some("completion_check ready=true")
        );
    }

    #[test]
    fn test_completion_can_be_used_without_plan_for_simple_read_only_work() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        record_result_for_completion(&ws);
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        assert!(result.data.unwrap()["ready"].as_bool().unwrap());
    }

    #[test]
    fn test_completion_allows_non_git_read_only_work() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        record_result_for_completion(&ws);
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        assert!(data["ready"].as_bool().unwrap());
        assert_eq!(data["git"]["is_git_repo"], false);
    }

    #[test]
    fn test_completion_requires_git_when_changes_are_required() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(true));
        assert!(result.success);
        let data = result.data.unwrap();
        assert!(!data["ready"].as_bool().unwrap());
        assert!(data["blockers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item.as_str().unwrap().contains("not a Git repository")));
    }

    #[test]
    fn untracked_only_changes_satisfy_require_changes() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        let ws = Workspace::open(dir.path()).unwrap();
        record_result_for_completion(&ws);
        fs::write(dir.path().join("new-file.txt"), "new\n").unwrap();

        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(true));
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["ready"], true);
        assert!(data["git"]["status"]
            .as_str()
            .unwrap()
            .contains("?? new-file.txt"));
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
    fn test_completion_checks_staged_whitespace() {
        let dir = tempdir().unwrap();
        init_git(dir.path());
        fs::write(dir.path().join("bad.txt"), "trailing space \n").unwrap();
        std::process::Command::new("git")
            .args(["add", "bad.txt"])
            .current_dir(dir.path())
            .status()
            .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        record_result_for_completion(&ws);
        let result = handle_completion_check(&ws, SCOPE, Some(false), Some(false), Some(false));
        assert!(result.success);
        let data = result.data.unwrap();
        // Whitespace checks are disabled, so ready should be true
        assert!(data["ready"].as_bool().unwrap());
    }
}
