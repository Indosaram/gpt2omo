use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_VERIFICATION_HISTORY: usize = 50;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    InProgress,
    Done,
    Blocked,
}

impl TaskStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "in_progress" => Some(Self::InProgress),
            "done" => Some(Self::Done),
            "blocked" => Some(Self::Blocked),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskItem {
    pub id: String,
    pub title: String,
    pub status: TaskStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerificationRecord {
    pub command: String,
    pub success: bool,
    pub exit_code: Option<i64>,
    pub duration_ms: u64,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskState {
    pub version: u32,
    pub goal: String,
    pub items: Vec<TaskItem>,
    pub created_ms: u64,
    pub updated_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_mutation_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_mutation_path: Option<String>,
    pub verifications: Vec<VerificationRecord>,
}

pub fn handle_task_plan(ws: &Workspace, goal: &str, items: Vec<String>) -> ToolCallResult {
    let goal = goal.trim();
    if goal.is_empty() {
        return ToolCallResult::err("Task goal cannot be empty");
    }
    if items.is_empty() {
        return ToolCallResult::err("Task plan must contain at least one item");
    }
    if items.len() > 100 {
        return ToolCallResult::err("Task plan cannot contain more than 100 items");
    }

    let mut normalized = Vec::with_capacity(items.len());
    for (idx, item) in items.into_iter().enumerate() {
        let title = item.trim();
        if title.is_empty() {
            return ToolCallResult::err(format!("Task item {} cannot be empty", idx + 1));
        }
        normalized.push(TaskItem {
            id: format!("T{}", idx + 1),
            title: title.to_string(),
            status: TaskStatus::Pending,
            note: None,
        });
    }

    let now = now_ms();
    let state = TaskState {
        version: 1,
        goal: goal.to_string(),
        items: normalized,
        created_ms: now,
        updated_ms: now,
        last_mutation_ms: None,
        last_mutation_path: None,
        verifications: Vec::new(),
    };

    match save_task_state(ws, &state) {
        Ok(()) => ToolCallResult::ok(serde_json::json!({
            "active": true,
            "state": state,
        })),
        Err(e) => ToolCallResult::err(e),
    }
}

pub fn handle_task_update(
    ws: &Workspace,
    item_id: &str,
    status: &str,
    note: Option<&str>,
) -> ToolCallResult {
    let Some(parsed_status) = TaskStatus::parse(status) else {
        return ToolCallResult::err(
            "Invalid task status; expected pending, in_progress, done, or blocked",
        );
    };

    let mut state = match load_task_state(ws) {
        Ok(Some(state)) => state,
        Ok(None) => return ToolCallResult::err("No active task plan"),
        Err(e) => return ToolCallResult::err(e),
    };

    let Some(item) = state.items.iter_mut().find(|item| item.id == item_id) else {
        return ToolCallResult::err(format!("Unknown task item id: {}", item_id));
    };

    item.status = parsed_status;
    item.note = note
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    state.updated_ms = now_ms();

    match save_task_state(ws, &state) {
        Ok(()) => ToolCallResult::ok(serde_json::json!({
            "active": true,
            "state": state,
        })),
        Err(e) => ToolCallResult::err(e),
    }
}

pub fn handle_task_state(ws: &Workspace) -> ToolCallResult {
    match load_task_state(ws) {
        Ok(Some(state)) => ToolCallResult::ok(serde_json::json!({
            "active": true,
            "state": state,
        })),
        Ok(None) => ToolCallResult::ok(serde_json::json!({
            "active": false,
            "state": null,
        })),
        Err(e) => ToolCallResult::err(e),
    }
}

pub fn record_mutation(ws: &Workspace, path: &str) {
    let Ok(Some(mut state)) = load_task_state(ws) else {
        return;
    };
    let now = now_ms();
    state.last_mutation_ms = Some(now);
    state.last_mutation_path = Some(path.to_string());
    state.updated_ms = now;
    let _ = save_task_state(ws, &state);
}

pub fn record_verification(
    ws: &Workspace,
    command: &str,
    success: bool,
    exit_code: Option<i64>,
    duration_ms: u64,
) {
    if !is_verification_command(command) {
        return;
    }

    let Ok(Some(mut state)) = load_task_state(ws) else {
        return;
    };

    state.verifications.push(VerificationRecord {
        command: command.to_string(),
        success,
        exit_code,
        duration_ms,
        timestamp_ms: now_ms(),
    });
    if state.verifications.len() > MAX_VERIFICATION_HISTORY {
        let excess = state.verifications.len() - MAX_VERIFICATION_HISTORY;
        state.verifications.drain(0..excess);
    }
    state.updated_ms = now_ms();
    let _ = save_task_state(ws, &state);
}

pub fn is_verification_command(command: &str) -> bool {
    let lower = command.trim().to_ascii_lowercase();
    [
        "cargo test",
        "cargo check",
        "cargo clippy",
        "cargo build",
        "cargo fmt --check",
        "npm test",
        "npm run test",
        "npm run build",
        "npm run lint",
        "npm run typecheck",
        "pytest",
        "vitest",
        "go test",
        "go vet",
        "make test",
        "make check",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

pub fn load_task_state(ws: &Workspace) -> std::result::Result<Option<TaskState>, String> {
    let path = state_path(ws)?;
    if !path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(&path).map_err(|e| format!("Failed to read task state: {}", e))?;
    let state =
        serde_json::from_slice(&bytes).map_err(|e| format!("Failed to parse task state: {}", e))?;
    Ok(Some(state))
}

fn save_task_state(ws: &Workspace, state: &TaskState) -> std::result::Result<(), String> {
    let path = state_path(ws)?;
    let parent = path
        .parent()
        .ok_or_else(|| "Task state path has no parent".to_string())?;
    fs::create_dir_all(parent)
        .map_err(|e| format!("Failed to create task state directory: {}", e))?;

    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| format!("Failed to serialize task state: {}", e))?;
    let temp = parent.join(format!(".task-state-{}.tmp", uuid::Uuid::new_v4()));
    fs::write(&temp, bytes).map_err(|e| format!("Failed to write task state: {}", e))?;
    fs::rename(&temp, &path).map_err(|e| {
        let _ = fs::remove_file(&temp);
        format!("Failed to atomically persist task state: {}", e)
    })?;
    Ok(())
}

fn state_path(ws: &Workspace) -> std::result::Result<PathBuf, String> {
    let root = ws.root().to_string_lossy();
    let key = format!("{:x}", Sha256::digest(root.as_bytes()));
    Ok(std::env::temp_dir()
        .join("omo-bridge")
        .join("task-state")
        .join(format!("{}.json", key)))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_task_plan_update_and_persistence() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let plan = handle_task_plan(
            &ws,
            "Implement feature",
            vec!["Inspect code".into(), "Run tests".into()],
        );
        assert!(plan.success);

        let update = handle_task_update(&ws, "T1", "done", Some("inspected"));
        assert!(update.success);

        let state = load_task_state(&ws).unwrap().unwrap();
        assert_eq!(state.goal, "Implement feature");
        assert_eq!(state.items[0].status, TaskStatus::Done);
        assert_eq!(state.items[0].note.as_deref(), Some("inspected"));
    }

    #[test]
    fn test_mutation_and_verification_are_recorded() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        handle_task_plan(&ws, "Change code", vec!["Edit".into()]);

        record_mutation(&ws, "src/lib.rs");
        record_verification(&ws, "cargo test", true, Some(0), 123);

        let state = load_task_state(&ws).unwrap().unwrap();
        assert_eq!(state.last_mutation_path.as_deref(), Some("src/lib.rs"));
        assert_eq!(state.verifications.len(), 1);
        assert!(state.verifications[0].success);
    }

    #[test]
    fn test_non_verification_command_is_not_recorded() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        handle_task_plan(&ws, "Inspect", vec!["Inspect".into()]);
        record_verification(&ws, "git --version", true, Some(0), 1);
        let state = load_task_state(&ws).unwrap().unwrap();
        assert!(state.verifications.is_empty());
    }
}
