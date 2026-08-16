use crate::security::Workspace;
use crate::tools::run_command::{prepare_command, PreparedCommand};
use crate::tools::task_state::{
    is_verification_command, load_delegation_lifecycle, record_verification,
};
use crate::tools::ToolCallResult;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::io::Read;
use std::path::{Component, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const MAX_RING_BYTES_PER_STREAM: usize = 256 * 1024;
const MAX_RESPONSE_BYTES_PER_STREAM: usize = 32 * 1024;
const MAX_RECENT_COMMANDS: usize = 256;
const MAX_POLL_WAIT_MS: u64 = 15_000;
const DEFAULT_SYNC_WAIT_MS: u64 = 15_000;
const DEFAULT_KILL_GRACE_MS: u64 = 1_500;
const NORMAL_DESCENDANT_GRACE_MS: u64 = 100;
const WAIT_TICK_MS: u64 = 20;
const FALLBACK_PATH: &str = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";

const SENSITIVE_CHILD_ENV: &[&str] = &[
    "OMO_BRIDGE_TOKEN",
    "OMO_SUBAGENT_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GITHUB_TOKEN",
    "GH_TOKEN",
];

#[derive(Clone)]
pub struct CommandManager {
    inner: Arc<CommandManagerInner>,
}

struct CommandManagerInner {
    state: Mutex<ManagerState>,
    changed: Condvar,
    sync_wait: Duration,
    kill_grace: Duration,
}

#[derive(Default)]
struct ManagerState {
    commands: HashMap<String, CommandRecord>,
    order: VecDeque<String>,
    idempotency: HashMap<IdempotencyKey, String>,
    workspace_revisions: HashMap<String, u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IdempotencyKey {
    scope_id: String,
    generation: u64,
    client_request_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CommandStatus {
    Running,
    Completed,
    TimedOut,
    Cancelled,
    Failed,
}

impl CommandStatus {
    fn is_terminal(self) -> bool {
        self != Self::Running
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

struct CommandRecord {
    command_id: String,
    scope_id: String,
    generation: u64,
    command: String,
    workspace_revision: u64,
    client_request_id: Option<String>,
    started_at: Instant,
    started_ms: u64,
    finished_ms: Option<u64>,
    timeout_ms: u64,
    status: CommandStatus,
    exit_code: Option<i64>,
    error: Option<String>,
    stdout: Arc<Mutex<BoundedRing>>,
    stderr: Arc<Mutex<BoundedRing>>,
    stdout_cursor: u64,
    stderr_cursor: u64,
    cancel_requested: Arc<AtomicBool>,
    process_group_id: Option<i32>,
    verification_recorded: bool,
}

#[derive(Default)]
struct BoundedRing {
    bytes: VecDeque<u8>,
    start_offset: u64,
    end_offset: u64,
    dropped_bytes: u64,
}

struct OutputPage {
    text: String,
    next_offset: u64,
    dropped_before: u64,
    more_available: bool,
}

impl BoundedRing {
    fn push(&mut self, chunk: &[u8]) {
        for byte in chunk {
            self.bytes.push_back(*byte);
            self.end_offset = self.end_offset.saturating_add(1);
        }
        while self.bytes.len() > MAX_RING_BYTES_PER_STREAM {
            let _ = self.bytes.pop_front();
            self.start_offset = self.start_offset.saturating_add(1);
            self.dropped_bytes = self.dropped_bytes.saturating_add(1);
        }
    }

    fn page_from(&self, cursor: u64) -> OutputPage {
        let effective_start = cursor.max(self.start_offset).min(self.end_offset);
        let dropped_before = effective_start.saturating_sub(cursor);
        let relative = effective_start.saturating_sub(self.start_offset) as usize;
        let available = self.bytes.len().saturating_sub(relative);
        let take = available.min(MAX_RESPONSE_BYTES_PER_STREAM);
        let bytes = self
            .bytes
            .iter()
            .skip(relative)
            .take(take)
            .copied()
            .collect::<Vec<_>>();
        let next_offset = effective_start.saturating_add(take as u64);
        OutputPage {
            text: lossy_bounded(&bytes, MAX_RESPONSE_BYTES_PER_STREAM),
            next_offset,
            dropped_before,
            more_available: next_offset < self.end_offset,
        }
    }
}

impl Default for CommandManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandManager {
    pub fn new() -> Self {
        Self::with_limits(
            Duration::from_millis(DEFAULT_SYNC_WAIT_MS),
            Duration::from_millis(DEFAULT_KILL_GRACE_MS),
        )
    }

    fn with_limits(sync_wait: Duration, kill_grace: Duration) -> Self {
        Self {
            inner: Arc::new(CommandManagerInner {
                state: Mutex::new(ManagerState::default()),
                changed: Condvar::new(),
                sync_wait,
                kill_grace,
            }),
        }
    }

    pub fn workspace_revision(&self, scope_id: &str) -> u64 {
        let state = lock_unpoisoned(&self.inner.state);
        state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0)
    }

    pub fn note_workspace_mutation(&self, scope_id: &str) -> u64 {
        let mut state = lock_unpoisoned(&self.inner.state);
        let revision = state
            .workspace_revisions
            .entry(scope_id.to_string())
            .or_insert(0);
        *revision = revision.saturating_add(1);
        self.inner.changed.notify_all();
        *revision
    }

    pub fn run_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command: &str,
        timeout_ms: u64,
        client_request_id: Option<&str>,
    ) -> ToolCallResult {
        let prepared = match prepare_command(ws, command) {
            Ok(prepared) => prepared,
            Err(error) => return ToolCallResult::err(error),
        };
        let generation = current_generation(ws, scope_id);
        let timeout_ms = timeout_ms.max(1);
        let client_request_id = client_request_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        if let Some(request_id) = client_request_id.as_deref() {
            let key = IdempotencyKey {
                scope_id: scope_id.to_string(),
                generation,
                client_request_id: request_id.to_string(),
            };
            let mut state = lock_unpoisoned(&self.inner.state);
            if let Some(existing_id) = state.idempotency.get(&key).cloned() {
                let current_revision = state
                    .workspace_revisions
                    .get(scope_id)
                    .copied()
                    .unwrap_or(0);
                let Some(existing) = state.commands.get_mut(&existing_id) else {
                    state.idempotency.remove(&key);
                    drop(state);
                    return self.spawn_and_wait(
                        ws,
                        scope_id,
                        generation,
                        command,
                        prepared,
                        timeout_ms,
                        client_request_id,
                    );
                };
                if existing.command != command {
                    return ToolCallResult::err(format!(
                        "client_request_id '{}' is already bound to a different command in this scope generation",
                        request_id
                    ));
                }
                let value = command_snapshot(existing, generation, current_revision, false, true);
                return ToolCallResult::ok(value);
            }
        }

        self.spawn_and_wait(
            ws,
            scope_id,
            generation,
            command,
            prepared,
            timeout_ms,
            client_request_id,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_and_wait(
        &self,
        ws: &Workspace,
        scope_id: &str,
        generation: u64,
        command: &str,
        prepared: PreparedCommand,
        timeout_ms: u64,
        client_request_id: Option<String>,
    ) -> ToolCallResult {
        let command_id = uuid::Uuid::new_v4().to_string();
        let stdout = Arc::new(Mutex::new(BoundedRing::default()));
        let stderr = Arc::new(Mutex::new(BoundedRing::default()));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let root = ws.root().to_path_buf();
        let revision;

        {
            let mut state = lock_unpoisoned(&self.inner.state);
            revision = state
                .workspace_revisions
                .get(scope_id)
                .copied()
                .unwrap_or(0);
            prune_recent(&mut state);
            let record = CommandRecord {
                command_id: command_id.clone(),
                scope_id: scope_id.to_string(),
                generation,
                command: command.to_string(),
                workspace_revision: revision,
                client_request_id: client_request_id.clone(),
                started_at: Instant::now(),
                started_ms: now_ms(),
                finished_ms: None,
                timeout_ms,
                status: CommandStatus::Running,
                exit_code: None,
                error: None,
                stdout: Arc::clone(&stdout),
                stderr: Arc::clone(&stderr),
                stdout_cursor: 0,
                stderr_cursor: 0,
                cancel_requested: Arc::clone(&cancel_requested),
                process_group_id: None,
                verification_recorded: false,
            };
            state.order.push_back(command_id.clone());
            state.commands.insert(command_id.clone(), record);
            if let Some(request_id) = client_request_id {
                state.idempotency.insert(
                    IdempotencyKey {
                        scope_id: scope_id.to_string(),
                        generation,
                        client_request_id: request_id,
                    },
                    command_id.clone(),
                );
            }
        }

        let manager = self.clone();
        let worker_id = command_id.clone();
        thread::spawn(move || {
            manager.run_worker(
                worker_id,
                root,
                prepared,
                timeout_ms,
                stdout,
                stderr,
                cancel_requested,
            );
        });

        let finished = self.wait_for_terminal(&command_id, self.inner.sync_wait);
        self.reconcile_scope(ws, scope_id);
        let mut state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let Some(record) = state.commands.get_mut(&command_id) else {
            return ToolCallResult::err("Command disappeared from daemon command manager");
        };
        let mut value = command_snapshot(record, generation, current_revision, false, false);
        if !finished && record.status == CommandStatus::Running {
            value["status"] = Value::String("detached_running".to_string());
        }
        ToolCallResult::ok(value)
    }

    #[allow(clippy::too_many_arguments)]
    fn run_worker(
        &self,
        command_id: String,
        root: PathBuf,
        prepared: PreparedCommand,
        timeout_ms: u64,
        stdout_buffer: Arc<Mutex<BoundedRing>>,
        stderr_buffer: Arc<Mutex<BoundedRing>>,
        cancel_requested: Arc<AtomicBool>,
    ) {
        let mut command = Command::new(&prepared.binary);
        command
            .args(&prepared.args)
            .current_dir(root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        sanitize_child_environment(&mut command);
        configure_process_group(&mut command);

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                self.finish_record(
                    &command_id,
                    CommandStatus::Failed,
                    None,
                    Some(format!("Failed to execute command: {error}")),
                    None,
                );
                return;
            }
        };

        let process_group_id = child.id() as i32;
        {
            let mut state = lock_unpoisoned(&self.inner.state);
            if let Some(record) = state.commands.get_mut(&command_id) {
                record.process_group_id = Some(process_group_id);
            }
        }

        let stdout_reader = spawn_reader(child.stdout.take(), stdout_buffer);
        let stderr_reader = spawn_reader(child.stderr.take(), stderr_buffer);
        let started = Instant::now();

        let (final_status, exit_status, error) = loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    cleanup_remaining_group(process_group_id);
                    break (CommandStatus::Completed, Some(status), None);
                }
                Ok(None) if cancel_requested.load(Ordering::SeqCst) => {
                    let status =
                        terminate_process_tree(&mut child, process_group_id, self.inner.kill_grace);
                    break (CommandStatus::Cancelled, status, None);
                }
                Ok(None) if started.elapsed() >= Duration::from_millis(timeout_ms) => {
                    let status =
                        terminate_process_tree(&mut child, process_group_id, self.inner.kill_grace);
                    break (CommandStatus::TimedOut, status, None);
                }
                Ok(None) => thread::sleep(Duration::from_millis(WAIT_TICK_MS)),
                Err(error) => {
                    let _ =
                        terminate_process_tree(&mut child, process_group_id, self.inner.kill_grace);
                    break (
                        CommandStatus::Failed,
                        None,
                        Some(format!("Failed while waiting for command: {error}")),
                    );
                }
            }
        };

        let _ = stdout_reader.join();
        let _ = stderr_reader.join();
        let exit_code = exit_status
            .as_ref()
            .and_then(|status| status.code())
            .map(i64::from);
        self.finish_record(
            &command_id,
            final_status,
            exit_code,
            error,
            Some(process_group_id),
        );
    }

    fn finish_record(
        &self,
        command_id: &str,
        status: CommandStatus,
        exit_code: Option<i64>,
        error: Option<String>,
        process_group_id: Option<i32>,
    ) {
        let mut state = lock_unpoisoned(&self.inner.state);
        if let Some(record) = state.commands.get_mut(command_id) {
            record.status = status;
            record.exit_code = exit_code;
            record.error = error;
            record.finished_ms = Some(now_ms());
            if process_group_id.is_some() {
                record.process_group_id = process_group_id;
            }
        }
        self.inner.changed.notify_all();
    }

    fn wait_for_terminal(&self, command_id: &str, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        let mut state = lock_unpoisoned(&self.inner.state);
        loop {
            let terminal = state
                .commands
                .get(command_id)
                .is_none_or(|record| record.status.is_terminal());
            if terminal {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (guard, _) = wait_timeout_unpoisoned(&self.inner.changed, state, remaining);
            state = guard;
        }
    }

    pub fn poll_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command_id: &str,
        wait_timeout_ms: Option<u64>,
    ) -> ToolCallResult {
        if !self.command_belongs_to_scope(scope_id, command_id) {
            return ToolCallResult::err("Unknown command_id for this scope");
        }
        let wait_ms = wait_timeout_ms.unwrap_or(0).min(MAX_POLL_WAIT_MS);
        if wait_ms > 0 {
            let _ = self.wait_for_terminal(command_id, Duration::from_millis(wait_ms));
        }
        self.reconcile_scope(ws, scope_id);
        let generation = current_generation(ws, scope_id);
        let mut state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let Some(record) = state.commands.get_mut(command_id) else {
            return ToolCallResult::err("Unknown command_id for this scope");
        };
        ToolCallResult::ok(command_snapshot(
            record,
            generation,
            current_revision,
            true,
            false,
        ))
    }

    pub fn list_commands(&self, ws: &Workspace, scope_id: &str) -> ToolCallResult {
        self.reconcile_scope(ws, scope_id);
        let generation = current_generation(ws, scope_id);
        let state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let commands = state
            .order
            .iter()
            .rev()
            .filter_map(|command_id| state.commands.get(command_id))
            .filter(|record| record.scope_id == scope_id)
            .take(100)
            .map(|record| command_summary(record, generation, current_revision))
            .collect::<Vec<_>>();
        let active_count = commands
            .iter()
            .filter(|value| value["status"] == "running")
            .count();
        ToolCallResult::ok(json!({
            "scope_id": scope_id,
            "generation": generation,
            "workspace_revision": current_revision,
            "active_count": active_count,
            "commands": commands,
        }))
    }

    pub fn cancel_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command_id: &str,
    ) -> ToolCallResult {
        {
            let state = lock_unpoisoned(&self.inner.state);
            let Some(record) = state.commands.get(command_id) else {
                return ToolCallResult::err("Unknown command_id for this scope");
            };
            if record.scope_id != scope_id {
                return ToolCallResult::err("Unknown command_id for this scope");
            }
            record.cancel_requested.store(true, Ordering::SeqCst);
        }
        self.inner.changed.notify_all();
        let wait = self.inner.kill_grace + Duration::from_millis(500);
        let _ = self.wait_for_terminal(command_id, wait);
        self.reconcile_scope(ws, scope_id);
        let generation = current_generation(ws, scope_id);
        let mut state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let Some(record) = state.commands.get_mut(command_id) else {
            return ToolCallResult::err("Unknown command_id for this scope");
        };
        ToolCallResult::ok(command_snapshot(
            record,
            generation,
            current_revision,
            true,
            false,
        ))
    }

    pub fn reconcile_scope(&self, ws: &Workspace, scope_id: &str) {
        let generation = current_generation(ws, scope_id);
        let mut state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let ids = state.order.iter().cloned().collect::<Vec<_>>();
        for command_id in ids {
            let Some(record) = state.commands.get_mut(&command_id) else {
                continue;
            };
            if record.scope_id != scope_id
                || !record.status.is_terminal()
                || record.verification_recorded
                || !is_verification_command(&record.command)
            {
                continue;
            }
            if record.generation != generation || record.workspace_revision != current_revision {
                continue;
            }
            let success = command_success(record);
            record_verification(
                ws,
                scope_id,
                &record.command,
                success,
                record.exit_code,
                record.started_at.elapsed().as_millis() as u64,
            );
            record.verification_recorded = true;
        }
    }

    pub fn latest_verification_evidence(&self, ws: &Workspace, scope_id: &str) -> Option<Value> {
        self.reconcile_scope(ws, scope_id);
        let generation = current_generation(ws, scope_id);
        let state = lock_unpoisoned(&self.inner.state);
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        state
            .order
            .iter()
            .rev()
            .filter_map(|command_id| state.commands.get(command_id))
            .find(|record| {
                record.scope_id == scope_id
                    && record.generation == generation
                    && record.workspace_revision == current_revision
                    && record.verification_recorded
                    && command_success(record)
            })
            .map(|record| {
                json!({
                    "command_id": record.command_id,
                    "command": record.command,
                    "generation": record.generation,
                    "workspace_revision": record.workspace_revision,
                    "exit_code": record.exit_code,
                    "duration_ms": record.started_at.elapsed().as_millis() as u64,
                    "evidence_status": "recorded",
                })
            })
    }

    fn command_belongs_to_scope(&self, scope_id: &str, command_id: &str) -> bool {
        let state = lock_unpoisoned(&self.inner.state);
        state
            .commands
            .get(command_id)
            .is_some_and(|record| record.scope_id == scope_id)
    }
}

fn command_snapshot(
    record: &mut CommandRecord,
    current_generation: u64,
    current_revision: u64,
    delta_names: bool,
    idempotent_replay: bool,
) -> Value {
    let stdout_page = {
        let buffer = lock_unpoisoned(&record.stdout);
        buffer.page_from(record.stdout_cursor)
    };
    let stderr_page = {
        let buffer = lock_unpoisoned(&record.stderr);
        buffer.page_from(record.stderr_cursor)
    };
    record.stdout_cursor = stdout_page.next_offset;
    record.stderr_cursor = stderr_page.next_offset;

    let mut value = command_summary(record, current_generation, current_revision);
    let object = value.as_object_mut().expect("command summary is an object");
    let stdout_key = if delta_names {
        "stdout_delta"
    } else {
        "stdout"
    };
    let stderr_key = if delta_names {
        "stderr_delta"
    } else {
        "stderr"
    };
    object.insert(stdout_key.into(), Value::String(stdout_page.text));
    object.insert(stderr_key.into(), Value::String(stderr_page.text));
    object.insert(
        "stdout_next_offset".into(),
        Value::from(stdout_page.next_offset),
    );
    object.insert(
        "stderr_next_offset".into(),
        Value::from(stderr_page.next_offset),
    );
    object.insert(
        "stdout_truncated".into(),
        Value::Bool(stdout_page.dropped_before > 0 || stdout_page.more_available),
    );
    object.insert(
        "stderr_truncated".into(),
        Value::Bool(stderr_page.dropped_before > 0 || stderr_page.more_available),
    );
    object.insert(
        "stdout_dropped_before".into(),
        Value::from(stdout_page.dropped_before),
    );
    object.insert(
        "stderr_dropped_before".into(),
        Value::from(stderr_page.dropped_before),
    );
    object.insert("idempotent_replay".into(), Value::Bool(idempotent_replay));
    value
}

fn command_summary(
    record: &CommandRecord,
    current_generation: u64,
    current_revision: u64,
) -> Value {
    json!({
        "command_id": record.command_id,
        "command": record.command,
        "status": record.status.as_str(),
        "generation": record.generation,
        "workspace_revision": record.workspace_revision,
        "current_workspace_revision": current_revision,
        "started_ms": record.started_ms,
        "finished_ms": record.finished_ms,
        "elapsed_ms": record.started_at.elapsed().as_millis() as u64,
        "timeout_ms": record.timeout_ms,
        "exit_code": record.exit_code,
        "timed_out": record.status == CommandStatus::TimedOut,
        "cancelled": record.status == CommandStatus::Cancelled,
        "command_success": command_success(record),
        "evidence_status": evidence_status(record, current_generation, current_revision),
        "client_request_id": record.client_request_id,
        "process_group_id": record.process_group_id,
        "error": record.error,
    })
}

fn evidence_status(
    record: &CommandRecord,
    current_generation: u64,
    current_revision: u64,
) -> &'static str {
    if !is_verification_command(&record.command) {
        return "not_verification";
    }
    if record.status == CommandStatus::Running {
        return "pending";
    }
    if record.generation != current_generation {
        return "stale_generation";
    }
    if record.workspace_revision != current_revision {
        return "stale_revision";
    }
    if record.verification_recorded && command_success(record) {
        return "recorded";
    }
    if command_success(record) {
        return "recordable";
    }
    "failed_verification"
}

fn command_success(record: &CommandRecord) -> bool {
    record.status == CommandStatus::Completed && record.exit_code == Some(0)
}

fn current_generation(ws: &Workspace, scope_id: &str) -> u64 {
    load_delegation_lifecycle(ws, scope_id)
        .ok()
        .flatten()
        .map(|lifecycle| lifecycle.generation)
        .unwrap_or(1)
}

fn prune_recent(state: &mut ManagerState) {
    while state.commands.len() >= MAX_RECENT_COMMANDS {
        let removable = state.order.iter().find_map(|command_id| {
            state
                .commands
                .get(command_id)
                .filter(|record| record.status.is_terminal())
                .map(|_| command_id.clone())
        });
        let Some(command_id) = removable else {
            break;
        };
        state.order.retain(|candidate| candidate != &command_id);
        state.commands.remove(&command_id);
        state.idempotency.retain(|_, value| value != &command_id);
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    pipe: Option<R>,
    buffer: Arc<Mutex<BoundedRing>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Some(mut pipe) = pipe else {
            return;
        };
        let mut chunk = [0u8; 8192];
        loop {
            match pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => lock_unpoisoned(&buffer).push(&chunk[..count]),
                Err(_) => break,
            }
        }
    })
}

fn sanitize_child_environment(command: &mut Command) {
    for key in SENSITIVE_CHILD_ENV {
        command.env_remove(key);
    }
    command.env("PATH", clean_path());
}

fn clean_path() -> OsString {
    let current = std::env::var_os("PATH").unwrap_or_else(|| OsString::from(FALLBACK_PATH));
    let clean = std::env::split_paths(&current)
        .filter(|path| {
            path.is_absolute()
                && !path
                    .components()
                    .any(|component| matches!(component, Component::ParentDir))
        })
        .collect::<Vec<_>>();
    std::env::join_paths(clean).unwrap_or_else(|_| OsString::from(FALLBACK_PATH))
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::io;
    use std::os::unix::process::CommandExt;

    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child, pgid: i32, grace: Duration) -> Option<ExitStatus> {
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + grace;
    let mut status = None;
    loop {
        if status.is_none() {
            status = child.try_wait().ok().flatten();
        }
        if !process_group_alive(pgid) || Instant::now() >= deadline {
            break;
        }
        thread::sleep(Duration::from_millis(WAIT_TICK_MS));
    }
    if process_group_alive(pgid) {
        signal_group(pgid, libc::SIGKILL);
    }
    status.or_else(|| child.wait().ok())
}

#[cfg(not(unix))]
fn terminate_process_tree(child: &mut Child, _pgid: i32, _grace: Duration) -> Option<ExitStatus> {
    let _ = child.kill();
    child.wait().ok()
}

#[cfg(unix)]
fn cleanup_remaining_group(pgid: i32) {
    if !process_group_alive(pgid) {
        return;
    }
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + Duration::from_millis(NORMAL_DESCENDANT_GRACE_MS);
    while process_group_alive(pgid) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if process_group_alive(pgid) {
        signal_group(pgid, libc::SIGKILL);
    }
}

#[cfg(not(unix))]
fn cleanup_remaining_group(_pgid: i32) {}

#[cfg(unix)]
fn signal_group(pgid: i32, signal: i32) {
    if pgid > 0 {
        unsafe {
            libc::kill(-pgid, signal);
        }
    }
}

#[cfg(unix)]
fn process_group_alive(pgid: i32) -> bool {
    if pgid <= 0 {
        return false;
    }
    let result = unsafe { libc::kill(-pgid, 0) };
    if result == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn lossy_bounded(bytes: &[u8], max_bytes: usize) -> String {
    let value = String::from_utf8_lossy(bytes);
    if value.len() <= max_bytes {
        return value.into_owned();
    }
    let mut end = max_bytes;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_timeout_unpoisoned<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    duration: Duration,
) -> (MutexGuard<'a, T>, std::sync::WaitTimeoutResult) {
    condvar
        .wait_timeout(guard, duration)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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
    use crate::tools::task_state::{handle_task_plan, load_task_state, record_mutation};
    use std::fs;
    use tempfile::tempdir;

    const SCOPE: &str = "55555555-5555-4555-8555-555555555555";

    fn test_manager() -> CommandManager {
        CommandManager::with_limits(Duration::from_millis(100), Duration::from_millis(150))
    }

    #[test]
    fn configured_response_and_wait_budgets_match_contract() {
        assert_eq!(DEFAULT_SYNC_WAIT_MS, 15_000);
        assert_eq!(MAX_POLL_WAIT_MS, 15_000);
        assert_eq!(MAX_RESPONSE_BYTES_PER_STREAM * 2, 64 * 1024);
    }

    #[test]
    fn bounded_ring_caps_total_and_response_page() {
        let mut ring = BoundedRing::default();
        let input = vec![b'x'; MAX_RING_BYTES_PER_STREAM + 4096];
        ring.push(&input);
        assert_eq!(ring.bytes.len(), MAX_RING_BYTES_PER_STREAM);
        assert_eq!(ring.dropped_bytes, 4096);
        let page = ring.page_from(0);
        assert_eq!(page.dropped_before, 4096);
        assert_eq!(page.text.len(), MAX_RESPONSE_BYTES_PER_STREAM);
        assert!(page.more_available);
    }

    #[test]
    fn child_environment_removes_daemon_credentials_and_sets_clean_path() {
        let mut command = Command::new("git");
        command
            .env("OMO_BRIDGE_TOKEN", "secret")
            .env("OMO_SUBAGENT_API_KEY", "secret-too");
        sanitize_child_environment(&mut command);
        let envs = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(envs.get("OMO_BRIDGE_TOKEN"), Some(&None));
        assert_eq!(envs.get("OMO_SUBAGENT_API_KEY"), Some(&None));
        let path = envs
            .get("PATH")
            .and_then(Option::as_deref)
            .expect("sanitized command should define PATH");
        assert!(std::env::split_paths(path).all(|entry| entry.is_absolute()));
    }

    #[test]
    fn quick_command_completes_synchronously() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let result = manager.run_command(&ws, SCOPE, "git --version", 2_000, None);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["command_success"], true);
        assert!(data["command_id"].as_str().is_some());
    }

    #[test]
    fn slow_command_auto_detaches_and_long_poll_finishes() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.30\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let result = manager.run_command(&ws, SCOPE, "make test", 2_000, None);
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["status"], "detached_running");
        let command_id = data["command_id"].as_str().unwrap();
        let polled = manager.poll_command(&ws, SCOPE, command_id, Some(2_000));
        assert!(polled.success);
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["command_success"], true);
    }

    #[test]
    fn mutation_marks_inflight_verification_stale_revision() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.30\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        assert!(handle_task_plan(&ws, SCOPE, "verify", vec!["run".into()]).success);
        let result = manager.run_command(&ws, SCOPE, "make test", 2_000, None);
        let command_id = result.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        record_mutation(&ws, SCOPE, "src/lib.rs");
        assert_eq!(manager.note_workspace_mutation(SCOPE), 1);
        let polled = manager.poll_command(&ws, SCOPE, &command_id, Some(2_000));
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["evidence_status"], "stale_revision");
        assert!(load_task_state(&ws, SCOPE)
            .unwrap()
            .unwrap()
            .verifications
            .is_empty());
    }

    #[test]
    fn duplicate_client_request_id_reuses_command_id() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.30\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let first = manager.run_command(&ws, SCOPE, "make test", 2_000, Some("retry-1"));
        let first_data = first.data.unwrap();
        let first_id = first_data["command_id"].as_str().unwrap().to_string();
        let second = manager.run_command(&ws, SCOPE, "make test", 2_000, Some("retry-1"));
        let second_data = second.data.unwrap();
        assert_eq!(second_data["command_id"], first_id);
        assert_eq!(second_data["idempotent_replay"], true);
        let _ = manager.cancel_command(&ws, SCOPE, &first_id);
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_descendant_process_group() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@sh -c '(sleep 0.5; echo survived > survived.txt) & wait'\n",
        )
        .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let started = manager.run_command(&ws, SCOPE, "make test", 180, None);
        let command_id = started.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        let polled = manager.poll_command(&ws, SCOPE, &command_id, Some(2_000));
        assert!(polled.success);
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "timed_out");
        assert_eq!(data["command_success"], false);
        thread::sleep(Duration::from_millis(550));
        assert!(!dir.path().join("survived.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_kills_descendant_process_group() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@sh -c '(sleep 0.5; echo survived > survived.txt) & wait'\n",
        )
        .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let started = manager.run_command(&ws, SCOPE, "make test", 5_000, None);
        let command_id = started.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        let cancelled = manager.cancel_command(&ws, SCOPE, &command_id);
        assert!(cancelled.success);
        assert_eq!(cancelled.data.unwrap()["status"], "cancelled");
        thread::sleep(Duration::from_millis(550));
        assert!(!dir.path().join("survived.txt").exists());
    }
}
