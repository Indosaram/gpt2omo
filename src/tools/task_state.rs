use crate::security::{default_bridge_base_dir, Workspace};
use crate::tools::ToolCallResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_VERIFICATION_HISTORY: usize = 50;
const TERMINAL_CLAIM_LOCK_TIMEOUT: Duration = Duration::from_secs(5);
const TERMINAL_CLAIM_LOCK_RETRY: Duration = Duration::from_millis(2);

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DelegationTerminalState {
    Completed,
    Blocked,
    Failed,
    Lost,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegationLifecycle {
    pub version: u32,
    pub scope_id: String,
    #[serde(default = "default_generation")]
    pub generation: u64,
    #[serde(default)]
    pub generation_started_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_state: Option<DelegationTerminalState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_detail: Option<String>,
    #[serde(default)]
    pub session_retained: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_since_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_ms: Option<u64>,
    pub updated_ms: u64,
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

pub fn handle_task_plan(
    ws: &Workspace,
    scope_id: &str,
    goal: &str,
    items: Vec<String>,
) -> ToolCallResult {
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

    match load_task_state(ws, scope_id) {
        Ok(Some(existing))
            if existing
                .items
                .iter()
                .any(|item| item.status != TaskStatus::Done) =>
        {
            return ToolCallResult::err(format!(
                "An incomplete task plan already exists for this delegation scope: {}. Recover task_state and continue or resolve it before creating a new plan",
                existing.goal
            ));
        }
        Ok(_) => {}
        Err(error) => return ToolCallResult::err(error),
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

    match save_task_state(ws, scope_id, &state) {
        Ok(()) => ToolCallResult::ok(serde_json::json!({
            "active": true,
            "scope_id": scope_id,
            "state": state,
        })),
        Err(e) => ToolCallResult::err(e),
    }
}

pub fn handle_task_update(
    ws: &Workspace,
    scope_id: &str,
    item_id: &str,
    status: &str,
    note: Option<&str>,
) -> ToolCallResult {
    let Some(parsed_status) = TaskStatus::parse(status) else {
        return ToolCallResult::err(
            "Invalid task status; expected pending, in_progress, done, or blocked",
        );
    };
    let becomes_blocked = parsed_status == TaskStatus::Blocked;

    let mut state = match load_task_state(ws, scope_id) {
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

    if let Err(error) = save_task_state(ws, scope_id, &state) {
        return ToolCallResult::err(error);
    }

    if becomes_blocked {
        let detail = state
            .items
            .iter()
            .find(|item| item.id == item_id)
            .map(blocked_item_detail)
            .unwrap_or_else(|| format!("{} marked blocked", item_id));
        if let Err(error) = record_terminal_evidence(
            ws,
            scope_id,
            DelegationTerminalState::Blocked,
            Some(&detail),
        ) {
            return ToolCallResult::err(error);
        }
    }

    ToolCallResult::ok(serde_json::json!({
        "active": true,
        "scope_id": scope_id,
        "state": state,
    }))
}

pub fn handle_task_state(ws: &Workspace, scope_id: &str) -> ToolCallResult {
    let state = match load_task_state(ws, scope_id) {
        Ok(state) => state,
        Err(e) => return ToolCallResult::err(e),
    };

    if let Err(error) = record_readiness_evidence(ws, scope_id) {
        return ToolCallResult::err(error);
    }

    if let Some(detail) = state.as_ref().and_then(blocked_state_detail) {
        if let Err(error) = record_terminal_evidence(
            ws,
            scope_id,
            DelegationTerminalState::Blocked,
            Some(&detail),
        ) {
            return ToolCallResult::err(error);
        }
    }

    match state {
        Some(state) => ToolCallResult::ok(serde_json::json!({
            "active": true,
            "scope_id": scope_id,
            "state": state,
        })),
        None => ToolCallResult::ok(serde_json::json!({
            "active": false,
            "scope_id": scope_id,
            "state": null,
        })),
    }
}

pub fn record_mutation(ws: &Workspace, scope_id: &str, path: &str) {
    let Ok(Some(mut state)) = load_task_state(ws, scope_id) else {
        return;
    };
    let now = now_ms();
    state.last_mutation_ms = Some(now);
    state.last_mutation_path = Some(path.to_string());
    state.updated_ms = now;
    let _ = save_task_state(ws, scope_id, &state);
}

pub fn record_verification(
    ws: &Workspace,
    scope_id: &str,
    command: &str,
    success: bool,
    exit_code: Option<i64>,
    duration_ms: u64,
) {
    if !is_verification_command(command) {
        return;
    }

    let Ok(Some(mut state)) = load_task_state(ws, scope_id) else {
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
    let _ = save_task_state(ws, scope_id, &state);
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

pub fn load_task_state(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<Option<TaskState>, String> {
    let path = task_state_path(ws, scope_id)?;
    if !path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(&path).map_err(|e| format!("Failed to read task state: {}", e))?;
    let state =
        serde_json::from_slice(&bytes).map_err(|e| format!("Failed to parse task state: {}", e))?;
    Ok(Some(state))
}

pub fn load_delegation_lifecycle(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<Option<DelegationLifecycle>, String> {
    let path = lifecycle_path(ws, scope_id)?;
    if !path.exists() {
        return Ok(None);
    }
    let bytes =
        fs::read(&path).map_err(|e| format!("Failed to read delegation lifecycle: {}", e))?;
    let mut state: DelegationLifecycle = serde_json::from_slice(&bytes)
        .map_err(|e| format!("Failed to parse delegation lifecycle: {}", e))?;
    if state.version != 1 || state.scope_id != scope_id {
        return Err(format!(
            "Invalid delegation lifecycle state for {}",
            scope_id
        ));
    }
    if state.generation == 0 {
        state.generation = 1;
    }
    if state.generation_started_ms == 0 {
        state.generation_started_ms = state.updated_ms;
    }
    Ok(Some(state))
}

pub fn start_fresh_delegation_lifecycle(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<DelegationLifecycle, String> {
    let lifecycle = new_lifecycle(scope_id, 1);
    save_lifecycle(ws, scope_id, &lifecycle)?;
    Ok(lifecycle)
}

pub fn start_next_delegation_generation(
    ws: &Workspace,
    scope_id: &str,
    reopen_blocked_items: bool,
) -> std::result::Result<DelegationLifecycle, String> {
    let previous = load_delegation_lifecycle(ws, scope_id)?
        .ok_or_else(|| "No delegation lifecycle exists for retained session".to_string())?;
    if !previous.session_retained {
        return Err("Delegation session is not retained/resumable".to_string());
    }
    if previous.terminal_state.is_none() {
        return Err("Delegation session still has an active non-terminal generation".to_string());
    }

    let original_task_state = if reopen_blocked_items {
        load_task_state(ws, scope_id)?
    } else {
        None
    };
    let mut updated_task_state = original_task_state.clone();
    let mut reopened_any = false;
    if let Some(state) = updated_task_state.as_mut() {
        for item in &mut state.items {
            if item.status == TaskStatus::Blocked {
                item.status = TaskStatus::InProgress;
                reopened_any = true;
            }
        }
        if reopened_any {
            state.updated_ms = now_ms();
            save_task_state(ws, scope_id, state)?;
        }
    }

    let lifecycle = new_lifecycle(scope_id, previous.generation.saturating_add(1).max(2));
    if let Err(error) = save_lifecycle(ws, scope_id, &lifecycle) {
        if reopened_any {
            if let Some(original) = original_task_state.as_ref() {
                let _ = save_task_state(ws, scope_id, original);
            }
        }
        return Err(error);
    }
    Ok(lifecycle)
}

pub fn retain_session_with_lease(
    ws: &Workspace,
    scope_id: &str,
    ttl_ms: u64,
) -> std::result::Result<DelegationLifecycle, String> {
    if ttl_ms == 0 {
        return Err("Retained-session TTL must be greater than zero".to_string());
    }
    let mut lifecycle = load_delegation_lifecycle(ws, scope_id)?
        .ok_or_else(|| "No delegation lifecycle exists".to_string())?;
    if lifecycle.terminal_state.is_none() {
        return Err(
            "Cannot retain a delegation session before the generation is terminal".to_string(),
        );
    }
    let now = now_ms();
    lifecycle.session_retained = true;
    lifecycle.retained_since_ms = Some(now);
    lifecycle.lease_expires_ms = Some(now.saturating_add(ttl_ms));
    lifecycle.updated_ms = now;
    save_lifecycle(ws, scope_id, &lifecycle)?;
    Ok(lifecycle)
}

pub fn release_session_retention(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<DelegationLifecycle, String> {
    let mut lifecycle = load_delegation_lifecycle(ws, scope_id)?
        .ok_or_else(|| "No delegation lifecycle exists".to_string())?;
    lifecycle.session_retained = false;
    lifecycle.retained_since_ms = None;
    lifecycle.lease_expires_ms = None;
    lifecycle.updated_ms = now_ms();
    save_lifecycle(ws, scope_id, &lifecycle)?;
    Ok(lifecycle)
}

pub fn mark_session_retained(
    ws: &Workspace,
    scope_id: &str,
    retained: bool,
) -> std::result::Result<DelegationLifecycle, String> {
    if !retained {
        return release_session_retention(ws, scope_id);
    }
    let mut lifecycle = load_delegation_lifecycle(ws, scope_id)?
        .ok_or_else(|| "No delegation lifecycle exists".to_string())?;
    if lifecycle.terminal_state.is_none() {
        return Err(
            "Cannot change retained-session state before the generation is terminal".to_string(),
        );
    }
    let now = now_ms();
    lifecycle.session_retained = true;
    lifecycle.retained_since_ms = Some(now);
    lifecycle.lease_expires_ms = None;
    lifecycle.updated_ms = now;
    save_lifecycle(ws, scope_id, &lifecycle)?;
    Ok(lifecycle)
}

pub fn retained_session_expired(lifecycle: &DelegationLifecycle, now_ms: u64) -> bool {
    lifecycle.session_retained
        && lifecycle
            .lease_expires_ms
            .is_some_and(|expires_ms| now_ms >= expires_ms)
}

pub fn record_terminal_evidence(
    ws: &Workspace,
    scope_id: &str,
    terminal_state: DelegationTerminalState,
    detail: Option<&str>,
) -> std::result::Result<DelegationLifecycle, String> {
    let lifecycle_path = lifecycle_path(ws, scope_id)?;
    let _claim_lock = LifecycleClaimLock::acquire(&lifecycle_path)?;
    let expected_generation = load_delegation_lifecycle(ws, scope_id)?
        .map(|lifecycle| lifecycle.generation)
        .unwrap_or(1);
    record_terminal_evidence_locked(
        ws,
        scope_id,
        expected_generation,
        terminal_state,
        detail,
        true,
    )
}

pub fn record_terminal_evidence_if_active(
    ws: &Workspace,
    scope_id: &str,
    expected_generation: u64,
    terminal_state: DelegationTerminalState,
    detail: Option<&str>,
) -> std::result::Result<DelegationLifecycle, String> {
    if expected_generation == 0 {
        return Err("Expected delegation generation must be greater than zero".to_string());
    }
    let lifecycle_path = lifecycle_path(ws, scope_id)?;
    let _claim_lock = LifecycleClaimLock::acquire(&lifecycle_path)?;
    record_terminal_evidence_locked(
        ws,
        scope_id,
        expected_generation,
        terminal_state,
        detail,
        false,
    )
}

fn record_terminal_evidence_locked(
    ws: &Workspace,
    scope_id: &str,
    expected_generation: u64,
    terminal_state: DelegationTerminalState,
    detail: Option<&str>,
    create_if_missing: bool,
) -> std::result::Result<DelegationLifecycle, String> {
    let mut lifecycle = match load_delegation_lifecycle(ws, scope_id)? {
        Some(lifecycle) => lifecycle,
        None if create_if_missing => new_lifecycle(scope_id, expected_generation),
        None => return Err("No active delegation lifecycle exists".to_string()),
    };

    if lifecycle.generation != expected_generation {
        return Err(format!(
            "Refusing terminal claim for stale generation {} (current {})",
            expected_generation, lifecycle.generation
        ));
    }

    if lifecycle.terminal_state.is_none() {
        let now = now_ms();
        lifecycle.terminal_state = Some(terminal_state);
        lifecycle.terminal_ms = Some(now);
        lifecycle.terminal_detail = detail
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        lifecycle.updated_ms = now;
        save_lifecycle(ws, scope_id, &lifecycle)?;
    }
    Ok(lifecycle)
}

struct LifecycleClaimLock {
    path: PathBuf,
}

impl LifecycleClaimLock {
    fn acquire(lifecycle_path: &Path) -> std::result::Result<Self, String> {
        let parent = lifecycle_path
            .parent()
            .ok_or_else(|| "Delegation lifecycle path has no parent".to_string())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("Failed to create lifecycle directory: {}", error))?;
        let file_name = lifecycle_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| "Delegation lifecycle path has no file name".to_string())?;
        let lock_path = parent.join(format!(".{}.terminal-claim.lock", file_name));
        let started = Instant::now();

        loop {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(_) => return Ok(Self { path: lock_path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    if started.elapsed() >= TERMINAL_CLAIM_LOCK_TIMEOUT {
                        return Err(
                            "Timed out acquiring delegation terminal claim lock".to_string()
                        );
                    }
                    thread::sleep(TERMINAL_CLAIM_LOCK_RETRY);
                }
                Err(error) => {
                    return Err(format!(
                        "Failed to acquire delegation terminal claim lock: {}",
                        error
                    ))
                }
            }
        }
    }
}

impl Drop for LifecycleClaimLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn clear_delegation_lifecycle(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<(), String> {
    let path = lifecycle_path(ws, scope_id)?;
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("Failed to remove delegation lifecycle: {}", error)),
    }
}

fn record_readiness_evidence(
    ws: &Workspace,
    scope_id: &str,
) -> std::result::Result<DelegationLifecycle, String> {
    let mut lifecycle =
        load_delegation_lifecycle(ws, scope_id)?.unwrap_or_else(|| new_lifecycle(scope_id, 1));
    if lifecycle.ready_ms.is_none() {
        let now = now_ms();
        lifecycle.ready_ms = Some(now);
        lifecycle.updated_ms = now;
        save_lifecycle(ws, scope_id, &lifecycle)?;
    }
    Ok(lifecycle)
}

fn new_lifecycle(scope_id: &str, generation: u64) -> DelegationLifecycle {
    let now = now_ms();
    DelegationLifecycle {
        version: 1,
        scope_id: scope_id.to_string(),
        generation,
        generation_started_ms: now,
        ready_ms: None,
        terminal_state: None,
        terminal_ms: None,
        terminal_detail: None,
        session_retained: false,
        retained_since_ms: None,
        lease_expires_ms: None,
        updated_ms: now,
    }
}

fn default_generation() -> u64 {
    1
}

fn blocked_state_detail(state: &TaskState) -> Option<String> {
    let blocked = state
        .items
        .iter()
        .filter(|item| item.status == TaskStatus::Blocked)
        .map(blocked_item_detail)
        .collect::<Vec<_>>();
    if blocked.is_empty() {
        None
    } else {
        Some(blocked.join("; "))
    }
}

fn blocked_item_detail(item: &TaskItem) -> String {
    match item.note.as_deref() {
        Some(note) => format!("{} {}: {}", item.id, item.title, note),
        None => format!("{} {}", item.id, item.title),
    }
}

fn save_task_state(
    ws: &Workspace,
    scope_id: &str,
    state: &TaskState,
) -> std::result::Result<(), String> {
    let path = task_state_path(ws, scope_id)?;
    atomic_write_json(&path, state, "task state")
}

fn save_lifecycle(
    ws: &Workspace,
    scope_id: &str,
    state: &DelegationLifecycle,
) -> std::result::Result<(), String> {
    let path = lifecycle_path(ws, scope_id)?;
    atomic_write_json(&path, state, "delegation lifecycle")
}

fn atomic_write_json<T: Serialize>(
    path: &PathBuf,
    state: &T,
    label: &str,
) -> std::result::Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("{} path has no parent", label))?;
    fs::create_dir_all(parent)
        .map_err(|e| format!("Failed to create {} directory: {}", label, e))?;

    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|e| format!("Failed to serialize {}: {}", label, e))?;
    let temp = parent.join(format!(
        ".{}-{}.tmp",
        label.replace(' ', "-"),
        uuid::Uuid::new_v4()
    ));
    fs::write(&temp, bytes).map_err(|e| format!("Failed to write {}: {}", label, e))?;
    fs::rename(&temp, path).map_err(|e| {
        let _ = fs::remove_file(&temp);
        format!("Failed to atomically persist {}: {}", label, e)
    })?;
    Ok(())
}

fn task_state_path(ws: &Workspace, scope_id: &str) -> std::result::Result<PathBuf, String> {
    Ok(default_bridge_base_dir()
        .join("task-state")
        .join(format!("{}.json", scope_key(ws, scope_id)?)))
}

fn lifecycle_path(ws: &Workspace, scope_id: &str) -> std::result::Result<PathBuf, String> {
    Ok(default_bridge_base_dir()
        .join("delegation-lifecycle")
        .join(format!("{}.json", scope_key(ws, scope_id)?)))
}

fn scope_key(ws: &Workspace, scope_id: &str) -> std::result::Result<String, String> {
    uuid::Uuid::parse_str(scope_id).map_err(|_| "Invalid task scope id".to_string())?;
    let root = ws.root().to_string_lossy();
    Ok(format!(
        "{:x}",
        Sha256::digest(format!("{}:{}", root, scope_id).as_bytes())
    ))
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
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    const SCOPE_A: &str = "11111111-1111-4111-8111-111111111111";
    const SCOPE_B: &str = "22222222-2222-4222-8222-222222222222";

    #[test]
    fn test_task_plan_update_and_persistence() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();

        let plan = handle_task_plan(
            &ws,
            SCOPE_A,
            "Implement feature",
            vec!["Inspect code".into(), "Run tests".into()],
        );
        assert!(plan.success);

        let update = handle_task_update(&ws, SCOPE_A, "T1", "done", Some("inspected"));
        assert!(update.success);

        let state = load_task_state(&ws, SCOPE_A).unwrap().unwrap();
        assert_eq!(state.goal, "Implement feature");
        assert_eq!(state.items[0].status, TaskStatus::Done);
        assert_eq!(state.items[0].note.as_deref(), Some("inspected"));
    }

    #[test]
    fn successful_task_state_records_authoritative_readiness() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        let lifecycle = start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        assert_eq!(lifecycle.generation, 1);

        let result = handle_task_state(&ws, SCOPE_A);
        assert!(result.success);
        let lifecycle = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert!(lifecycle.ready_ms.is_some());
        assert!(lifecycle.terminal_state.is_none());
        assert!(!lifecycle.session_retained);
    }

    #[test]
    fn retained_session_lease_records_and_expires() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        let retained = retain_session_with_lease(&ws, SCOPE_A, 10_000).unwrap();
        assert!(retained.session_retained);
        assert!(retained.retained_since_ms.is_some());
        let expiry = retained.lease_expires_ms.unwrap();
        assert!(!retained_session_expired(
            &retained,
            expiry.saturating_sub(1)
        ));
        assert!(retained_session_expired(&retained, expiry));

        let released = release_session_retention(&ws, SCOPE_A).unwrap();
        assert!(!released.session_retained);
        assert!(released.retained_since_ms.is_none());
        assert!(released.lease_expires_ms.is_none());
    }

    #[test]
    fn retained_terminal_session_starts_next_generation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&ws, SCOPE_A, 10_000).unwrap();

        let next = start_next_delegation_generation(&ws, SCOPE_A, false).unwrap();
        assert_eq!(next.generation, 2);
        assert!(next.ready_ms.is_none());
        assert!(next.terminal_state.is_none());
        assert!(!next.session_retained);
        assert!(next.lease_expires_ms.is_none());
    }

    #[test]
    fn non_retained_session_cannot_start_next_generation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();

        let error = start_next_delegation_generation(&ws, SCOPE_A, false).unwrap_err();
        assert!(error.contains("not retained/resumable"));
    }

    #[test]
    fn blocked_resume_reopens_only_blocked_items_and_preserves_notes() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        handle_task_plan(
            &ws,
            SCOPE_A,
            "Blocked task",
            vec!["Reach service".into(), "Already done".into()],
        );
        handle_task_update(&ws, SCOPE_A, "T1", "blocked", Some("service unavailable"));
        handle_task_update(&ws, SCOPE_A, "T2", "done", Some("finished"));
        retain_session_with_lease(&ws, SCOPE_A, 10_000).unwrap();

        start_next_delegation_generation(&ws, SCOPE_A, true).unwrap();
        let state = load_task_state(&ws, SCOPE_A).unwrap().unwrap();
        assert_eq!(state.items[0].status, TaskStatus::InProgress);
        assert_eq!(state.items[0].note.as_deref(), Some("service unavailable"));
        assert_eq!(state.items[1].status, TaskStatus::Done);
        assert_eq!(state.items[1].note.as_deref(), Some("finished"));
        assert!(handle_task_state(&ws, SCOPE_A).success);
        let lifecycle = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert!(lifecycle.ready_ms.is_some());
        assert!(lifecycle.terminal_state.is_none());
    }

    #[test]
    fn completed_resume_can_replace_done_plan_with_new_followup_plan() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        assert!(handle_task_plan(&ws, SCOPE_A, "First", vec!["Finish first".into()]).success);
        assert!(handle_task_update(&ws, SCOPE_A, "T1", "done", None).success);
        record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&ws, SCOPE_A, 10_000).unwrap();
        start_next_delegation_generation(&ws, SCOPE_A, false).unwrap();

        let new_plan = handle_task_plan(&ws, SCOPE_A, "Follow-up", vec!["Do follow-up".into()]);
        assert!(new_plan.success);
        assert_eq!(
            load_task_state(&ws, SCOPE_A).unwrap().unwrap().goal,
            "Follow-up"
        );
    }

    #[test]
    fn blocked_task_update_records_authoritative_terminal_state() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        handle_task_plan(&ws, SCOPE_A, "Blocked task", vec!["Reach service".into()]);

        let result = handle_task_update(
            &ws,
            SCOPE_A,
            "T1",
            "blocked",
            Some("external service unavailable"),
        );
        assert!(result.success);
        let lifecycle = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert_eq!(
            lifecycle.terminal_state,
            Some(DelegationTerminalState::Blocked)
        );
        assert!(lifecycle
            .terminal_detail
            .as_deref()
            .unwrap()
            .contains("external service unavailable"));
    }

    #[test]
    fn task_state_reconciles_existing_blocked_plan_into_terminal_evidence() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        handle_task_plan(&ws, SCOPE_A, "Blocked task", vec!["Reach service".into()]);
        handle_task_update(&ws, SCOPE_A, "T1", "blocked", Some("blocked"));
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();

        assert!(handle_task_state(&ws, SCOPE_A).success);
        let lifecycle = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert!(lifecycle.ready_ms.is_some());
        assert_eq!(
            lifecycle.terminal_state,
            Some(DelegationTerminalState::Blocked)
        );
    }

    #[test]
    fn terminal_evidence_is_first_writer_wins_within_generation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Failed,
            Some("transport failed"),
        )
        .unwrap();
        let state = record_terminal_evidence(
            &ws,
            SCOPE_A,
            DelegationTerminalState::Completed,
            Some("late completion"),
        )
        .unwrap();
        assert_eq!(state.terminal_state, Some(DelegationTerminalState::Failed));
        assert_eq!(state.terminal_detail.as_deref(), Some("transport failed"));
    }

    #[test]
    fn concurrent_terminal_claims_are_atomic_and_first_writer_wins() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let ws = Workspace::open(&root).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();

        let worker_count = 16;
        let barrier = Arc::new(Barrier::new(worker_count));
        let mut handles = Vec::with_capacity(worker_count);
        for index in 0..worker_count {
            let root = root.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let ws = Workspace::open(&root).unwrap();
                let state = if index % 2 == 0 {
                    DelegationTerminalState::Completed
                } else {
                    DelegationTerminalState::Blocked
                };
                let detail = if index % 2 == 0 {
                    "worker_completed"
                } else {
                    "helper_blocked"
                };
                barrier.wait();
                record_terminal_evidence_if_active(&ws, SCOPE_A, 1, state, Some(detail)).unwrap()
            }));
        }

        let claims = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let final_state = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert!(final_state.terminal_state.is_some());
        for claim in claims {
            assert_eq!(claim.terminal_state, final_state.terminal_state);
            assert_eq!(claim.terminal_detail, final_state.terminal_detail);
        }
    }

    #[test]
    fn terminal_claim_rejects_stale_generation_without_mutating_current_generation() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        clear_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        start_fresh_delegation_lifecycle(&ws, SCOPE_A).unwrap();
        record_terminal_evidence_if_active(
            &ws,
            SCOPE_A,
            1,
            DelegationTerminalState::Completed,
            Some("generation_one_done"),
        )
        .unwrap();
        retain_session_with_lease(&ws, SCOPE_A, 10_000).unwrap();
        let next = start_next_delegation_generation(&ws, SCOPE_A, false).unwrap();
        assert_eq!(next.generation, 2);

        let error = record_terminal_evidence_if_active(
            &ws,
            SCOPE_A,
            1,
            DelegationTerminalState::Blocked,
            Some("stale_helper"),
        )
        .unwrap_err();
        assert!(error.contains("stale generation"));
        let current = load_delegation_lifecycle(&ws, SCOPE_A).unwrap().unwrap();
        assert_eq!(current.generation, 2);
        assert!(current.terminal_state.is_none());
    }

    #[test]
    fn test_mutation_and_verification_are_recorded() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        handle_task_plan(&ws, SCOPE_A, "Change code", vec!["Edit".into()]);

        record_mutation(&ws, SCOPE_A, "src/lib.rs");
        record_verification(&ws, SCOPE_A, "cargo test", true, Some(0), 123);

        let state = load_task_state(&ws, SCOPE_A).unwrap().unwrap();
        assert_eq!(state.last_mutation_path.as_deref(), Some("src/lib.rs"));
        assert_eq!(state.verifications.len(), 1);
        assert!(state.verifications[0].success);
    }

    #[test]
    fn test_non_verification_command_is_not_recorded() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        handle_task_plan(&ws, SCOPE_A, "Inspect", vec!["Inspect".into()]);
        record_verification(&ws, SCOPE_A, "git --version", true, Some(0), 1);
        let state = load_task_state(&ws, SCOPE_A).unwrap().unwrap();
        assert!(state.verifications.is_empty());
    }

    #[test]
    fn task_state_is_isolated_by_scope_even_in_same_workspace() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        assert!(handle_task_plan(&ws, SCOPE_A, "Task A", vec!["A".into()]).success);
        assert!(handle_task_plan(&ws, SCOPE_B, "Task B", vec!["B".into()]).success);
        assert_eq!(
            load_task_state(&ws, SCOPE_A).unwrap().unwrap().goal,
            "Task A"
        );
        assert_eq!(
            load_task_state(&ws, SCOPE_B).unwrap().unwrap().goal,
            "Task B"
        );
    }

    #[test]
    fn incomplete_plan_cannot_be_overwritten_within_same_scope() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        assert!(handle_task_plan(&ws, SCOPE_A, "First task", vec!["Do work".into()]).success);
        let blocked = handle_task_plan(&ws, SCOPE_A, "Second task", vec!["Other work".into()]);
        assert!(!blocked.success);
        assert!(blocked.error.unwrap().contains("incomplete task plan"));
    }
}
