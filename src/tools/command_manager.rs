use crate::security::Workspace;
use crate::tools::run_command::{
    is_arbitrary_commands_allowed, prepare_command_with_policy, PreparedCommand,
};
use crate::tools::task_state::{
    is_verification_command, load_delegation_lifecycle, record_verification,
};
use crate::tools::ToolCallResult;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::ffi::OsString;
use std::io::Read;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Component, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex as StdMutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, Notify, OwnedSemaphorePermit, Semaphore};

const MAX_RING_BYTES_PER_STREAM: usize = 256 * 1024;
const MAX_RESPONSE_BYTES_PER_STREAM: usize = 32 * 1024;
const MAX_RECENT_COMMANDS: usize = 256;
const MAX_POLL_WAIT_MS: u64 = 15_000;
const DEFAULT_SYNC_WAIT_MS: u64 = 15_000;
const DEFAULT_KILL_GRACE_MS: u64 = 1_500;
const NORMAL_DESCENDANT_GRACE_MS: u64 = 100;
const WAIT_TICK_MS: u64 = 20;
/// Upper bound on draining the child's pipes once the process group has been signalled. A
/// descendant that escaped the group still holds the write ends, so EOF may never arrive.
const READER_DRAIN_BOUND_MS: u64 = 500;
/// How often an abandoned reader rechecks whether it should stop waiting for more output.
#[cfg(unix)]
const READER_ABANDON_POLL_MS: i32 = 50;
const CAPTURE_TRUNCATED_NOTE: &str =
    "output capture truncated: descendant processes kept the command pipes open after the process \
     group was signalled";
const DEFAULT_MAX_ACTIVE_COMMANDS_GLOBAL: usize = 32;
const DEFAULT_MAX_ACTIVE_COMMANDS_PER_SCOPE: usize = 8;
const DEFAULT_MAX_CONCURRENT_TOOLS_PER_SCOPE: usize = 64;
const FALLBACK_PATH: &str = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin";

const ALLOWED_CHILD_ENV: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TERM",
    "TMPDIR",
    "SSL_CERT_FILE",
    "RUSTUP_HOME",
    "CARGO_HOME",
    "NODE_PATH",
];

// These are non-secret build settings that may be configured by the daemon for the workspace.
// They are forwarded explicitly rather than allowing the child to inherit anything else.
const WORKSPACE_CHILD_ENV: &[&str] = &["CARGO_TARGET_DIR", "CARGO_BUILD_TARGET"];

#[derive(Clone)]
pub struct CommandManager {
    inner: Arc<CommandManagerInner>,
}

struct CommandManagerInner {
    state: Mutex<ManagerState>,
    changed: Notify,
    sync_wait: Duration,
    kill_grace: Duration,
    allow_arbitrary: bool,
    max_active_global: usize,
    max_active_per_scope: usize,
    tool_semaphores: StdMutex<HashMap<String, Arc<Semaphore>>>,
    active_waiters: AtomicUsize,
    waiters_changed: Notify,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IdempotencyEntry {
    Reserved { command: String },
    Active(String),
}

#[derive(Default)]
struct ManagerState {
    commands: HashMap<String, CommandRecord>,
    order: VecDeque<String>,
    idempotency: HashMap<IdempotencyKey, IdempotencyEntry>,
    workspace_revisions: HashMap<String, u64>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct IdempotencyKey {
    scope_id: String,
    generation: u64,
    client_request_id: String,
}

struct PendingVerification {
    command: String,
    success: bool,
    exit_code: Option<i64>,
    elapsed_ms: u64,
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
    stdout: Arc<StdMutex<BoundedRing>>,
    stderr: Arc<StdMutex<BoundedRing>>,
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
        if chunk.is_empty() {
            return;
        }
        self.end_offset = self.end_offset.saturating_add(chunk.len() as u64);
        if chunk.len() >= MAX_RING_BYTES_PER_STREAM {
            let excess = self
                .bytes
                .len()
                .saturating_add(chunk.len())
                .saturating_sub(MAX_RING_BYTES_PER_STREAM);
            self.bytes.clear();
            let keep_slice = &chunk[chunk.len() - MAX_RING_BYTES_PER_STREAM..];
            self.bytes.extend(keep_slice.iter().copied());
            self.start_offset = self.start_offset.saturating_add(excess as u64);
            self.dropped_bytes = self.dropped_bytes.saturating_add(excess as u64);
        } else {
            self.bytes.extend(chunk.iter().copied());
            let excess = self.bytes.len().saturating_sub(MAX_RING_BYTES_PER_STREAM);
            if excess > 0 {
                self.bytes.drain(..excess);
                self.start_offset = self.start_offset.saturating_add(excess as u64);
                self.dropped_bytes = self.dropped_bytes.saturating_add(excess as u64);
            }
        }
    }

    fn page_from(&self, cursor: u64) -> OutputPage {
        let effective_start = cursor.max(self.start_offset).min(self.end_offset);
        let dropped_before = effective_start.saturating_sub(cursor);
        let relative = effective_start.saturating_sub(self.start_offset) as usize;
        let available = self.bytes.len().saturating_sub(relative);
        let take = available.min(MAX_RESPONSE_BYTES_PER_STREAM);
        let mut bytes = Vec::with_capacity(take);
        let (s1, s2) = self.bytes.as_slices();
        if relative < s1.len() {
            let chunk1 = &s1[relative..];
            let take1 = chunk1.len().min(take);
            bytes.extend_from_slice(&chunk1[..take1]);
            let remaining = take - take1;
            if remaining > 0 {
                bytes.extend_from_slice(&s2[..remaining]);
            }
        } else {
            let offset = relative - s1.len();
            bytes.extend_from_slice(&s2[offset..offset + take]);
        }
        let next_offset = effective_start.saturating_add(take as u64);
        OutputPage {
            text: lossy_bounded(&bytes, MAX_RESPONSE_BYTES_PER_STREAM),
            next_offset,
            dropped_before,
            more_available: next_offset < self.end_offset,
        }
    }
}

struct WaiterGuard<'a> {
    inner: &'a CommandManagerInner,
}

impl Drop for WaiterGuard<'_> {
    fn drop(&mut self) {
        self.inner.active_waiters.fetch_sub(1, Ordering::SeqCst);
        self.inner.waiters_changed.notify_waiters();
    }
}

struct ReservationGuard {
    inner: Arc<CommandManagerInner>,
    key: Option<IdempotencyKey>,
}

impl ReservationGuard {
    fn new(inner: Arc<CommandManagerInner>, key: IdempotencyKey) -> Self {
        Self {
            inner,
            key: Some(key),
        }
    }

    fn disarm(&mut self) {
        self.key = None;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            let inner = Arc::clone(&self.inner);
            tokio::spawn(async move {
                let mut state = inner.state.lock().await;
                if matches!(state.idempotency.get(&key), Some(IdempotencyEntry::Reserved { .. })) {
                    state.idempotency.remove(&key);
                    inner.changed.notify_waiters();
                }
            });
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
        Self::with_limits_and_policy(
            Duration::from_millis(DEFAULT_SYNC_WAIT_MS),
            Duration::from_millis(DEFAULT_KILL_GRACE_MS),
            is_arbitrary_commands_allowed(),
            DEFAULT_MAX_ACTIVE_COMMANDS_GLOBAL,
            DEFAULT_MAX_ACTIVE_COMMANDS_PER_SCOPE,
        )
    }

    pub fn with_allow_arbitrary(allow_arbitrary: bool) -> Self {
        Self::with_limits_and_policy(
            Duration::from_millis(DEFAULT_SYNC_WAIT_MS),
            Duration::from_millis(DEFAULT_KILL_GRACE_MS),
            allow_arbitrary || is_arbitrary_commands_allowed(),
            DEFAULT_MAX_ACTIVE_COMMANDS_GLOBAL,
            DEFAULT_MAX_ACTIVE_COMMANDS_PER_SCOPE,
        )
    }

    #[cfg(test)]
    fn with_limits(sync_wait: Duration, kill_grace: Duration) -> Self {
        Self::with_limits_and_policy(
            sync_wait,
            kill_grace,
            is_arbitrary_commands_allowed(),
            DEFAULT_MAX_ACTIVE_COMMANDS_GLOBAL,
            DEFAULT_MAX_ACTIVE_COMMANDS_PER_SCOPE,
        )
    }

    #[cfg(test)]
    fn with_capacity_limits(
        sync_wait: Duration,
        kill_grace: Duration,
        max_active_global: usize,
        max_active_per_scope: usize,
    ) -> Self {
        Self::with_limits_and_policy(
            sync_wait,
            kill_grace,
            is_arbitrary_commands_allowed(),
            max_active_global,
            max_active_per_scope,
        )
    }

    fn with_limits_and_policy(
        sync_wait: Duration,
        kill_grace: Duration,
        allow_arbitrary: bool,
        max_active_global: usize,
        max_active_per_scope: usize,
    ) -> Self {
        Self {
            inner: Arc::new(CommandManagerInner {
                state: Mutex::new(ManagerState::default()),
                changed: Notify::new(),
                sync_wait,
                kill_grace,
                allow_arbitrary,
                max_active_global: max_active_global.max(1),
                max_active_per_scope: max_active_per_scope.max(1),
                tool_semaphores: StdMutex::new(HashMap::new()),
                active_waiters: AtomicUsize::new(0),
                waiters_changed: Notify::new(),
            }),
        }
    }

    pub async fn acquire_tool_permit(&self, scope_id: &str) -> OwnedSemaphorePermit {
        let semaphore = {
            let mut semaphores = lock_unpoisoned(&self.inner.tool_semaphores);
            Arc::clone(semaphores.entry(scope_id.to_string()).or_insert_with(|| {
                Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_TOOLS_PER_SCOPE))
            }))
        };
        semaphore
            .acquire_owned()
            .await
            .expect("per-scope tool semaphore must remain open")
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_waiter_count_for_tests(
        &self,
        minimum: usize,
        wait: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.inner.waiters_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.active_waiters.load(Ordering::SeqCst) >= minimum {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    pub async fn workspace_revision(&self, scope_id: &str) -> u64 {
        let state = self.inner.state.lock().await;
        state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0)
    }

    pub async fn note_workspace_mutation(&self, scope_id: &str) -> u64 {
        let mut state = self.inner.state.lock().await;
        let revision = state
            .workspace_revisions
            .entry(scope_id.to_string())
            .or_insert(0);
        *revision = revision.saturating_add(1);
        self.inner.changed.notify_waiters();
        *revision
    }

    pub async fn run_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command: &str,
        timeout_ms: u64,
        client_request_id: Option<&str>,
    ) -> ToolCallResult {
        let prepared = match prepare_command_with_policy(ws, command, self.inner.allow_arbitrary) {
            Ok(prepared) => prepared,
            Err(error) => return ToolCallResult::err(error),
        };
        let generation = current_generation_async(ws, scope_id).await;
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

            let mut reservation_guard = loop {
                let notified = self.inner.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                enum StepAction {
                    Wait,
                    Retry,
                    Attach(String),
                    Reserved,
                }

                let step = {
                    let mut state = self.inner.state.lock().await;
                    match state.idempotency.get(&key) {
                        Some(IdempotencyEntry::Reserved {
                            command: existing_command,
                        }) => {
                            if existing_command != command {
                                return ToolCallResult::err(format!(
                                    "client_request_id '{}' is already bound to a different command in this scope generation",
                                    request_id
                                ));
                            }
                            StepAction::Wait
                        }
                        Some(IdempotencyEntry::Active(existing_id)) => {
                            let existing_id = existing_id.clone();
                            match state.commands.get_mut(&existing_id) {
                                None => {
                                    state.idempotency.remove(&key);
                                    StepAction::Retry
                                }
                                Some(existing) => {
                                    if existing.command != command {
                                        return ToolCallResult::err(format!(
                                            "client_request_id '{}' is already bound to a different command in this scope generation",
                                            request_id
                                        ));
                                    }
                                    StepAction::Attach(existing_id)
                                }
                            }
                        }
                        None => {
                            state.idempotency.insert(
                                key.clone(),
                                IdempotencyEntry::Reserved {
                                    command: command.to_string(),
                                },
                            );
                            StepAction::Reserved
                        }
                    }
                };

                match step {
                    StepAction::Wait => {
                        let _ = tokio::time::timeout(self.inner.sync_wait, notified).await;
                    }
                    StepAction::Retry => {}
                    StepAction::Attach(existing_id) => {
                        return self
                            .attach_and_wait(ws, scope_id, generation, &existing_id)
                            .await;
                    }
                    StepAction::Reserved => {
                        break ReservationGuard::new(Arc::clone(&self.inner), key);
                    }
                }
            };

            let result = self
                .spawn_and_wait(
                    ws,
                    scope_id,
                    generation,
                    command,
                    prepared,
                    timeout_ms,
                    client_request_id,
                )
                .await;
            reservation_guard.disarm();
            return result;
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
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_and_wait(
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
        let stdout = Arc::new(StdMutex::new(BoundedRing::default()));
        let stderr = Arc::new(StdMutex::new(BoundedRing::default()));
        let cancel_requested = Arc::new(AtomicBool::new(false));
        let root = ws.root().to_path_buf();
        let revision;

        {
            let mut state = self.inner.state.lock().await;
            let global_active = state
                .commands
                .values()
                .filter(|record| record.status == CommandStatus::Running)
                .count();
            if global_active >= self.inner.max_active_global {
                if let Some(ref request_id) = client_request_id {
                    let key = IdempotencyKey {
                        scope_id: scope_id.to_string(),
                        generation,
                        client_request_id: request_id.clone(),
                    };
                    state.idempotency.remove(&key);
                    self.inner.changed.notify_waiters();
                }
                return ToolCallResult::err(format!(
                    "command admission limit reached: {} active daemon commands globally (limit {})",
                    global_active, self.inner.max_active_global
                ));
            }
            let scope_active = state
                .commands
                .values()
                .filter(|record| {
                    record.status == CommandStatus::Running && record.scope_id == scope_id
                })
                .count();
            if scope_active >= self.inner.max_active_per_scope {
                if let Some(ref request_id) = client_request_id {
                    let key = IdempotencyKey {
                        scope_id: scope_id.to_string(),
                        generation,
                        client_request_id: request_id.clone(),
                    };
                    state.idempotency.remove(&key);
                    self.inner.changed.notify_waiters();
                }
                return ToolCallResult::err(format!(
                    "command admission limit reached for scope {}: {} active commands (limit {})",
                    scope_id, scope_active, self.inner.max_active_per_scope
                ));
            }

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
                    IdempotencyEntry::Active(command_id.clone()),
                );
                self.inner.changed.notify_waiters();
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

        let finished = self
            .wait_for_terminal(&command_id, self.inner.sync_wait)
            .await;
        self.reconcile_scope(ws, scope_id).await;
        let mut state = self.inner.state.lock().await;
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

    async fn attach_and_wait(
        &self,
        ws: &Workspace,
        scope_id: &str,
        generation: u64,
        command_id: &str,
    ) -> ToolCallResult {
        let finished = self
            .wait_for_terminal(command_id, self.inner.sync_wait)
            .await;
        self.reconcile_scope(ws, scope_id).await;
        let mut state = self.inner.state.lock().await;
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let Some(record) = state.commands.get_mut(command_id) else {
            return ToolCallResult::err("Command disappeared from daemon command manager");
        };
        let mut value = command_snapshot(record, generation, current_revision, false, true);
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
        stdout_buffer: Arc<StdMutex<BoundedRing>>,
        stderr_buffer: Arc<StdMutex<BoundedRing>>,
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
            let mut state = self.inner.state.blocking_lock();
            if let Some(record) = state.commands.get_mut(&command_id) {
                record.process_group_id = Some(process_group_id);
            }
        }

        let readers_abandoned = Arc::new(AtomicBool::new(false));
        let (readers_done_tx, readers_done) = mpsc::channel::<Infallible>();
        spawn_reader(
            child.stdout.take(),
            stdout_buffer,
            Arc::clone(&readers_abandoned),
            readers_done_tx.clone(),
        );
        spawn_reader(
            child.stderr.take(),
            stderr_buffer,
            Arc::clone(&readers_abandoned),
            readers_done_tx,
        );
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

        let error = if wait_for_readers(&readers_done, Duration::from_millis(READER_DRAIN_BOUND_MS))
        {
            error
        } else {
            // The readers cannot reach EOF while an escaped descendant holds the pipes. Release
            // them so the command still reaches a terminal state and frees its admission slot,
            // keeping whatever output was captured before the bound elapsed.
            readers_abandoned.store(true, Ordering::SeqCst);
            Some(match error {
                Some(existing) => format!("{existing}; {CAPTURE_TRUNCATED_NOTE}"),
                None => CAPTURE_TRUNCATED_NOTE.to_string(),
            })
        };
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
        let mut state = self.inner.state.blocking_lock();
        if let Some(record) = state.commands.get_mut(command_id) {
            record.status = status;
            record.exit_code = exit_code;
            record.error = error;
            record.finished_ms = Some(now_ms());
            if process_group_id.is_some() {
                record.process_group_id = process_group_id;
            }
        }
        self.inner.changed.notify_waiters();
    }

    async fn wait_for_terminal(&self, command_id: &str, wait: Duration) -> bool {
        self.inner.active_waiters.fetch_add(1, Ordering::SeqCst);
        self.inner.waiters_changed.notify_waiters();
        let _waiter_guard = WaiterGuard { inner: &self.inner };
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.inner.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.inner.state.lock().await;
                let terminal = state
                    .commands
                    .get(command_id)
                    .is_none_or(|record| record.status.is_terminal());
                if terminal {
                    return true;
                }
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    pub async fn poll_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command_id: &str,
        wait_timeout_ms: Option<u64>,
    ) -> ToolCallResult {
        if !self.command_belongs_to_scope(scope_id, command_id).await {
            return ToolCallResult::err("Unknown command_id for this scope");
        }
        let wait_ms = wait_timeout_ms.unwrap_or(0).min(MAX_POLL_WAIT_MS);
        if wait_ms > 0 {
            let _ = self
                .wait_for_terminal(command_id, Duration::from_millis(wait_ms))
                .await;
        }
        self.reconcile_scope(ws, scope_id).await;
        let generation = current_generation_async(ws, scope_id).await;
        let mut state = self.inner.state.lock().await;
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

    pub async fn list_commands(&self, ws: &Workspace, scope_id: &str) -> ToolCallResult {
        self.reconcile_scope(ws, scope_id).await;
        let generation = current_generation_async(ws, scope_id).await;
        let state = self.inner.state.lock().await;
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
            "active_limit_per_scope": self.inner.max_active_per_scope,
            "active_limit_global": self.inner.max_active_global,
            "commands": commands,
        }))
    }

    pub async fn cancel_command(
        &self,
        ws: &Workspace,
        scope_id: &str,
        command_id: &str,
    ) -> ToolCallResult {
        {
            let state = self.inner.state.lock().await;
            let Some(record) = state.commands.get(command_id) else {
                return ToolCallResult::err("Unknown command_id for this scope");
            };
            if record.scope_id != scope_id {
                return ToolCallResult::err("Unknown command_id for this scope");
            }
            record.cancel_requested.store(true, Ordering::SeqCst);
        }
        self.inner.changed.notify_waiters();
        let wait = self.inner.kill_grace + Duration::from_millis(500);
        let _ = self.wait_for_terminal(command_id, wait).await;
        self.reconcile_scope(ws, scope_id).await;
        let generation = current_generation_async(ws, scope_id).await;
        let mut state = self.inner.state.lock().await;
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

    pub async fn reconcile_scope(&self, ws: &Workspace, scope_id: &str) {
        let generation = current_generation_async(ws, scope_id).await;

        // Crash-window tradeoff: `record.verification_recorded = true` is marked under the
        // in-memory lock before persisting to disk so concurrent reconciliation calls never produce
        // duplicate verification records. In the event of an ungraceful process crash between marking
        // the in-memory flag and completing disk persistence, the command remains marked-but-unpersisted
        // rather than duplicated on recovery, preserving exactly-once semantics.
        let pending: Vec<PendingVerification> = {
            let mut state = self.inner.state.lock().await;
            let current_revision = state
                .workspace_revisions
                .get(scope_id)
                .copied()
                .unwrap_or(0);
            let ids = state.order.iter().cloned().collect::<Vec<_>>();
            let mut collected = Vec::new();
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
                let elapsed_ms = record.started_at.elapsed().as_millis() as u64;
                record.verification_recorded = true;
                collected.push(PendingVerification {
                    command: record.command.clone(),
                    success,
                    exit_code: record.exit_code,
                    elapsed_ms,
                });
            }
            collected
        };

        if !pending.is_empty() {
            let ws_clone = ws.clone();
            let scope_id_owned = scope_id.to_string();
            let _ = tokio::task::spawn_blocking(move || {
                for item in pending {
                    record_verification(
                        &ws_clone,
                        &scope_id_owned,
                        &item.command,
                        item.success,
                        item.exit_code,
                        item.elapsed_ms,
                    );
                }
            })
            .await;
        }
    }

    pub async fn latest_verification_evidence(
        &self,
        ws: &Workspace,
        scope_id: &str,
    ) -> Option<Value> {
        self.reconcile_scope(ws, scope_id).await;
        let generation = current_generation_async(ws, scope_id).await;
        let state = self.inner.state.lock().await;
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

    async fn command_belongs_to_scope(&self, scope_id: &str, command_id: &str) -> bool {
        let state = self.inner.state.lock().await;
        state
            .commands
            .get(command_id)
            .is_some_and(|record| record.scope_id == scope_id)
    }

    #[cfg(test)]
    pub async fn seed_pending_verification_for_test(
        &self,
        scope_id: &str,
        generation: u64,
        command: &str,
        exit_code: Option<i64>,
    ) {
        let mut state = self.inner.state.lock().await;
        let current_revision = state
            .workspace_revisions
            .get(scope_id)
            .copied()
            .unwrap_or(0);
        let command_id = uuid::Uuid::new_v4().to_string();
        let record = CommandRecord {
            command_id: command_id.clone(),
            scope_id: scope_id.to_string(),
            generation,
            command: command.to_string(),
            workspace_revision: current_revision,
            client_request_id: None,
            started_at: Instant::now(),
            started_ms: now_ms(),
            finished_ms: Some(now_ms()),
            timeout_ms: 1000,
            status: CommandStatus::Completed,
            exit_code,
            error: None,
            stdout: Arc::new(StdMutex::new(BoundedRing::default())),
            stderr: Arc::new(StdMutex::new(BoundedRing::default())),
            stdout_cursor: 0,
            stderr_cursor: 0,
            cancel_requested: Arc::new(AtomicBool::new(false)),
            process_group_id: None,
            verification_recorded: false,
        };
        state.order.push_back(command_id.clone());
        state.commands.insert(command_id, record);
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
    let Some(object) = value.as_object_mut() else {
        return json!({
            "command_id": record.command_id,
            "status": "failed",
            "error": "internal command summary serialization invariant failed"
        });
    };
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

async fn current_generation_async(ws: &Workspace, scope_id: &str) -> u64 {
    let ws_clone = ws.clone();
    let scope_id_owned = scope_id.to_string();
    tokio::task::spawn_blocking(move || current_generation(&ws_clone, &scope_id_owned))
        .await
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
        state.idempotency.retain(|_, value| match value {
            IdempotencyEntry::Active(id) => id != &command_id,
            IdempotencyEntry::Reserved { .. } => true,
        });
    }
}

/// Returns `true` once every reader thread has finished, `false` if `bound` elapsed first.
/// Readers signal completion by dropping their `Sender`, so the normal EOF case returns as soon
/// as the last reader exits rather than waiting out the bound.
fn wait_for_readers(readers_done: &mpsc::Receiver<Infallible>, bound: Duration) -> bool {
    match readers_done.recv_timeout(bound) {
        Ok(never) => match never {},
        Err(RecvTimeoutError::Disconnected) => true,
        Err(RecvTimeoutError::Timeout) => false,
    }
}

#[cfg(unix)]
fn spawn_reader<R: Read + Send + AsRawFd + 'static>(
    pipe: Option<R>,
    buffer: Arc<StdMutex<BoundedRing>>,
    abandoned: Arc<AtomicBool>,
    reader_done: mpsc::Sender<Infallible>,
) {
    thread::spawn(move || {
        // Dropped when this thread returns, which is how the worker observes reader completion.
        let _reader_done = reader_done;
        let Some(mut pipe) = pipe else {
            return;
        };
        let fd = pipe.as_raw_fd();
        let mut chunk = [0u8; 8192];
        loop {
            if abandoned.load(Ordering::SeqCst) {
                return;
            }
            if !pipe_readable(fd, READER_ABANDON_POLL_MS) {
                continue;
            }
            match pipe.read(&mut chunk) {
                Ok(0) => break,
                Ok(count) => lock_unpoisoned(&buffer).push(&chunk[..count]),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    });
}

#[cfg(unix)]
fn pipe_readable(fd: RawFd, timeout_ms: i32) -> bool {
    let mut poll_fd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    unsafe { libc::poll(&mut poll_fd, 1, timeout_ms) > 0 }
}

#[cfg(not(unix))]
fn spawn_reader<R: Read + Send + 'static>(
    pipe: Option<R>,
    buffer: Arc<StdMutex<BoundedRing>>,
    _abandoned: Arc<AtomicBool>,
    reader_done: mpsc::Sender<Infallible>,
) {
    thread::spawn(move || {
        let _reader_done = reader_done;
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
    });
}

fn sanitize_child_environment(command: &mut Command) {
    command.env_clear();
    command.env("PATH", clean_path());
    for key in ALLOWED_CHILD_ENV {
        if *key == "PATH" {
            continue;
        }
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for key in WORKSPACE_CHILD_ENV {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
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

fn lock_unpoisoned<T>(mutex: &StdMutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
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
        CommandManager::with_limits(Duration::from_millis(500), Duration::from_millis(150))
    }

    #[test]
    fn configured_response_and_wait_budgets_match_contract() {
        assert_eq!(DEFAULT_SYNC_WAIT_MS, 15_000);
        assert_eq!(MAX_POLL_WAIT_MS, 15_000);
        assert_eq!(MAX_RESPONSE_BYTES_PER_STREAM * 2, 64 * 1024);
        assert_eq!(DEFAULT_MAX_ACTIVE_COMMANDS_GLOBAL, 32);
        assert_eq!(DEFAULT_MAX_ACTIVE_COMMANDS_PER_SCOPE, 8);
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
    fn bounded_ring_push_chunks_and_bulk_drain() {
        let mut ring = BoundedRing::default();
        ring.push(b"hello ");
        assert_eq!(ring.bytes.len(), 6);
        assert_eq!(ring.start_offset, 0);
        assert_eq!(ring.end_offset, 6);
        assert_eq!(ring.dropped_bytes, 0);

        ring.push(b"world");
        assert_eq!(ring.bytes.len(), 11);
        assert_eq!(ring.start_offset, 0);
        assert_eq!(ring.end_offset, 11);
        assert_eq!(ring.dropped_bytes, 0);

        let fill = vec![b'a'; MAX_RING_BYTES_PER_STREAM - 11];
        ring.push(&fill);
        assert_eq!(ring.bytes.len(), MAX_RING_BYTES_PER_STREAM);
        assert_eq!(ring.start_offset, 0);
        assert_eq!(ring.end_offset, MAX_RING_BYTES_PER_STREAM as u64);
        assert_eq!(ring.dropped_bytes, 0);

        ring.push(b"12345");
        assert_eq!(ring.bytes.len(), MAX_RING_BYTES_PER_STREAM);
        assert_eq!(ring.start_offset, 5);
        assert_eq!(ring.end_offset, MAX_RING_BYTES_PER_STREAM as u64 + 5);
        assert_eq!(ring.dropped_bytes, 5);

        ring.push(b"");
        assert_eq!(ring.bytes.len(), MAX_RING_BYTES_PER_STREAM);
        assert_eq!(ring.start_offset, 5);
        assert_eq!(ring.end_offset, MAX_RING_BYTES_PER_STREAM as u64 + 5);
        assert_eq!(ring.dropped_bytes, 5);
    }

    #[test]
    fn bounded_ring_page_from_slice_extraction() {
        let mut ring = BoundedRing::default();
        ring.push(b"first section ");
        ring.push(b"second section ");
        ring.push(b"third section");

        let page1 = ring.page_from(0);
        assert_eq!(page1.text, "first section second section third section");
        assert_eq!(page1.dropped_before, 0);
        assert_eq!(page1.next_offset, 42);
        assert!(!page1.more_available);

        let page_partial = ring.page_from(14);
        assert_eq!(page_partial.text, "second section third section");
        assert_eq!(page_partial.next_offset, 42);
        assert!(!page_partial.more_available);
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
        assert!(!envs.contains_key("OMO_BRIDGE_TOKEN"));
        assert!(!envs.contains_key("OMO_SUBAGENT_API_KEY"));
        assert!(!envs.contains_key("OPENAI_API_KEY"));
        assert!(!envs.contains_key("RUSTFLAGS"));
        let path = envs
            .get("PATH")
            .and_then(Option::as_deref)
            .expect("sanitized command should define PATH");
        assert!(std::env::split_paths(path).all(|entry| entry.is_absolute()));
    }

    #[tokio::test]
    async fn quick_command_completes_synchronously() {
        let dir = tempdir().unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let result = manager
            .run_command(&ws, SCOPE, "git --version", 2_000, None)
            .await;
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["command_success"], true);
        assert!(data["command_id"].as_str().is_some());
    }

    #[tokio::test]
    async fn slow_command_auto_detaches_and_long_poll_finishes() {
        let dir = tempdir().unwrap();
        let gate = dir.path().join("gate");
        assert!(Command::new("mkfifo")
            .arg(&gate)
            .status()
            .unwrap()
            .success());
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@cat gate > /dev/null\n",
        )
        .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let result = manager
            .run_command(&ws, SCOPE, "make test", 2_000, None)
            .await;
        assert!(result.success);
        let data = result.data.unwrap();
        assert_eq!(data["status"], "detached_running");
        let command_id = data["command_id"].as_str().unwrap();
        let gate_writer = thread::spawn(move || {
            use std::io::Write;

            let mut gate = fs::OpenOptions::new().write(true).open(gate).unwrap();
            gate.write_all(b"release\n").unwrap();
        });
        let polled = manager
            .poll_command(&ws, SCOPE, command_id, Some(2_000))
            .await;
        gate_writer.join().unwrap();
        assert!(polled.success);
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["command_success"], true);
    }

    #[tokio::test]
    async fn active_command_limits_reject_scope_and_global_overload() {
        let first_scope = uuid::Uuid::new_v4().to_string();
        let second_scope = uuid::Uuid::new_v4().to_string();
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 1\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = CommandManager::with_capacity_limits(
            Duration::from_millis(20),
            Duration::from_millis(100),
            2,
            1,
        );

        let first = manager
            .run_command(&ws, &first_scope, "make test", 5_000, None)
            .await;
        assert!(first.success);
        let first_id = first.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();

        let same_scope = manager
            .run_command(&ws, &first_scope, "make test", 5_000, None)
            .await;
        assert!(!same_scope.success);
        assert!(same_scope
            .error
            .as_deref()
            .unwrap()
            .contains("admission limit reached for scope"));

        let second = manager
            .run_command(&ws, &second_scope, "make test", 5_000, None)
            .await;
        assert!(second.success);
        let second_id = second.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();

        let third_scope = uuid::Uuid::new_v4().to_string();
        let global = manager
            .run_command(&ws, &third_scope, "make test", 5_000, None)
            .await;
        assert!(!global.success);
        assert!(global
            .error
            .as_deref()
            .unwrap()
            .contains("active daemon commands globally"));

        let _ = manager.cancel_command(&ws, &first_scope, &first_id).await;
        let _ = manager.cancel_command(&ws, &second_scope, &second_id).await;
    }

    #[tokio::test]
    async fn mutation_marks_inflight_verification_stale_revision() {
        let scope_id = uuid::Uuid::new_v4().to_string();
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.50\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        assert!(handle_task_plan(&ws, &scope_id, "verify", vec!["run".into()]).success);
        let result = manager
            .run_command(&ws, &scope_id, "make test", 2_000, None)
            .await;
        if !result.success {
            panic!("run_command failed: {:?}", result.error);
        }
        let command_id = result.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        record_mutation(&ws, &scope_id, "src/lib.rs");
        assert_eq!(manager.note_workspace_mutation(&scope_id).await, 1);
        let polled = manager
            .poll_command(&ws, &scope_id, &command_id, Some(2_000))
            .await;
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "completed");
        assert_eq!(data["evidence_status"], "stale_revision");
        assert!(load_task_state(&ws, &scope_id)
            .unwrap()
            .unwrap()
            .verifications
            .is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_client_request_id_barrier_launches_exactly_once() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.30\n").unwrap();
        let ws = Arc::new(Workspace::open(dir.path()).unwrap());
        let manager = test_manager();
        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let b = Arc::clone(&barrier);
            let m = manager.clone();
            let w = Arc::clone(&ws);
            handles.push(tokio::spawn(async move {
                b.wait().await;
                m.run_command(&w, SCOPE, "make test", 2_000, Some("barrier-shared-req"))
                    .await
            }));
        }

        let mut results = Vec::new();
        for handle in handles {
            let res = handle.await.expect("task join succeeded");
            assert!(res.success, "run_command failed: {:?}", res.error);
            results.push(res);
        }

        let command_ids: Vec<String> = results
            .iter()
            .map(|r| {
                r.data
                    .as_ref()
                    .unwrap()["command_id"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();

        let unique_ids: std::collections::HashSet<_> = command_ids.iter().cloned().collect();
        assert_eq!(
            unique_ids.len(),
            1,
            "expected exactly 1 distinct command launched across 8 concurrent tasks, got {}: {:?}",
            unique_ids.len(),
            command_ids
        );

        let listed = manager.list_commands(&ws, SCOPE).await;
        let listed_commands = listed.data.unwrap()["commands"].as_array().unwrap().clone();
        assert_eq!(
            listed_commands.len(),
            1,
            "expected exactly 1 command launched in manager, found {}",
            listed_commands.len()
        );

        let shared_id = unique_ids.into_iter().next().unwrap();
        assert_eq!(
            listed_commands[0]["command_id"].as_str().unwrap(),
            shared_id
        );

        let _ = manager.cancel_command(&ws, SCOPE, &shared_id).await;
    }

    #[tokio::test]
    async fn concurrent_client_request_id_mismatched_command_rejected() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@sleep 0.30\ntest2:\n\t@sleep 0.30\n",
        )
        .unwrap();
        let ws = Arc::new(Workspace::open(dir.path()).unwrap());
        let manager = test_manager();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let b1 = Arc::clone(&barrier);
        let m1 = manager.clone();
        let w1 = Arc::clone(&ws);
        let h1 = tokio::spawn(async move {
            b1.wait().await;
            m1.run_command(&w1, SCOPE, "make test", 2_000, Some("mismatch-req"))
                .await
        });

        let b2 = Arc::clone(&barrier);
        let m2 = manager.clone();
        let w2 = Arc::clone(&ws);
        let h2 = tokio::spawn(async move {
            b2.wait().await;
            m2.run_command(&w2, SCOPE, "make test2", 2_000, Some("mismatch-req"))
                .await
        });

        let r1 = h1.await.unwrap();
        let r2 = h2.await.unwrap();

        assert!(
            r1.success ^ r2.success,
            "one must succeed and one must fail: r1={:?}, r2={:?}",
            r1,
            r2
        );
        let err = if !r1.success {
            r1.error.unwrap()
        } else {
            r2.error.unwrap()
        };
        assert!(
            err.contains("already bound to a different command"),
            "unexpected error message: {}",
            err
        );

        let success_id = if r1.success {
            r1.data.unwrap()["command_id"].as_str().unwrap().to_string()
        } else {
            r2.data.unwrap()["command_id"].as_str().unwrap().to_string()
        };
        let _ = manager.cancel_command(&ws, SCOPE, &success_id).await;
    }

    #[tokio::test]
    async fn duplicate_client_request_id_reuses_command_id() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@sleep 0.30\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let first = manager
            .run_command(&ws, SCOPE, "make test", 2_000, Some("retry-1"))
            .await;
        let first_data = first.data.unwrap();
        let first_id = first_data["command_id"].as_str().unwrap().to_string();
        let second = manager
            .run_command(&ws, SCOPE, "make test", 2_000, Some("retry-1"))
            .await;
        let second_data = second.data.unwrap();
        assert_eq!(second_data["command_id"], first_id);
        assert_eq!(second_data["idempotent_replay"], true);
        let _ = manager.cancel_command(&ws, SCOPE, &first_id).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_descendant_process_group() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@sh -c '(sleep 0.5; echo survived > survived.txt) & wait'\n",
        )
        .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let started = manager
            .run_command(&ws, SCOPE, "make test", 180, None)
            .await;
        let command_id = started.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        let polled = manager
            .poll_command(&ws, SCOPE, &command_id, Some(2_000))
            .await;
        assert!(polled.success);
        let data = polled.data.unwrap();
        assert_eq!(data["status"], "timed_out");
        assert_eq!(data["command_success"], false);
        thread::sleep(Duration::from_millis(550));
        assert!(!dir.path().join("survived.txt").exists());
    }

    #[cfg(unix)]
    fn reap_escaped_grandchild(root: &std::path::Path) {
        let Ok(raw) = fs::read_to_string(root.join("grandchild.pid")) else {
            return;
        };
        if let Ok(pid) = raw.trim().parse::<i32>() {
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
    }

    /// A descendant that leaves the killed process group keeps the child's stdout/stderr write
    /// ends open, so the pipe readers never observe EOF. The command must still reach a terminal
    /// state and release its admission slot.
    #[cfg(unix)]
    #[tokio::test]
    async fn escaped_descendant_does_not_pin_command_slot() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("escape.pl"),
            "use POSIX qw(setsid);\n\
             my $pid = fork();\n\
             die \"fork failed\" unless defined $pid;\n\
             if ($pid == 0) {\n\
                 setsid();\n\
                 open(my $fh, '>', 'grandchild.pid') or exit 1;\n\
                 print $fh \"$$\";\n\
                 close $fh;\n\
                 sleep 10;\n\
                 exit 0;\n\
             }\n\
             sleep 10;\n",
        )
        .unwrap();
        fs::write(dir.path().join("Makefile"), "test:\n\t@perl escape.pl\n").unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let started = manager
            .run_command(&ws, SCOPE, "make test", 200, None)
            .await;
        assert!(started.success, "run_command failed: {:?}", started.error);
        let command_id = started.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();

        let polled = manager
            .poll_command(&ws, SCOPE, &command_id, Some(3_000))
            .await;
        assert!(polled.success);
        let data = polled.data.unwrap();
        reap_escaped_grandchild(dir.path());

        assert_eq!(
            data["status"], "timed_out",
            "command with an escaped descendant never reached a terminal state; it still occupies \
             its admission slot (snapshot: {data})"
        );
        let error = data["error"].as_str().unwrap_or_default();
        assert!(
            error.contains("output capture truncated"),
            "terminal record must report that capture was cut short, got {error:?}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_kills_descendant_process_group() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("Makefile"),
            "test:\n\t@sh -c '(sleep 1.0; echo survived > survived.txt) & wait'\n",
        )
        .unwrap();
        let ws = Workspace::open(dir.path()).unwrap();
        let manager = test_manager();
        let started = manager
            .run_command(&ws, SCOPE, "make test", 5_000, None)
            .await;
        let command_id = started.data.unwrap()["command_id"]
            .as_str()
            .unwrap()
            .to_string();
        let cancelled = manager.cancel_command(&ws, SCOPE, &command_id).await;
        assert!(cancelled.success);
        assert_eq!(cancelled.data.unwrap()["status"], "cancelled");
        thread::sleep(Duration::from_millis(1_050));
        assert!(!dir.path().join("survived.txt").exists());
    }
}
