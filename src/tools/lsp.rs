use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use url::Url;

type SessionCacheMap = HashMap<(PathBuf, String), Vec<PooledSession>>;

static COMMAND_CACHE: LazyLock<Mutex<HashMap<String, bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static SESSION_CACHE: LazyLock<Mutex<SessionCacheMap>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const DEFAULT_TIMEOUT_MS: u64 = 15_000;
const DIAGNOSTIC_QUIET_MS: u64 = 600;
const MAX_SESSIONS_PER_KEY: usize = 2;

/// Default maximum idle duration before a pooled LSP session is reaped.
pub const DEFAULT_MAX_IDLE: Duration = Duration::from_secs(300);

/// Bounded capacity for the LSP stdout message feed.
/// Prevents memory bloat when idle sessions accumulate background notifications.
pub const LSP_STDOUT_CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug)]
pub enum LspOperation {
    Diagnostics,
    Definition,
    References,
    Symbols,
}

impl LspOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Diagnostics => "diagnostics",
            Self::Definition => "definition",
            Self::References => "references",
            Self::Symbols => "symbols",
        }
    }
}

#[derive(Clone, Debug)]
struct ServerSpec {
    command: String,
    args: Vec<String>,
    language_id: String,
}

pub async fn handle_lsp(
    ws: &Workspace,
    operation: LspOperation,
    path_str: &str,
    line: Option<usize>,
    character: Option<usize>,
    timeout_ms: Option<u64>,
) -> ToolCallResult {
    let full_path = match ws.resolve_relative(path_str) {
        Ok(path) => path,
        Err(e) => return ToolCallResult::err(e.to_string()),
    };
    let rel_path = match crate::security::PathPolicy::sanitize_relative_path(path_str) {
        Ok(path) => path,
        Err(e) => return ToolCallResult::err(e.to_string()),
    };
    if let Err(e) =
        crate::security::PathPolicy::ensure_resolved_target_allowed(ws.root(), &rel_path)
    {
        return ToolCallResult::err(e.to_string());
    }

    // Read through the workspace's retained capability instead of the ambient pathname returned by
    // resolve_relative: the ambient read re-resolves the path after the containment check, which is
    // the check/use window STATE-3 describes.
    let source = {
        let dir = match ws.cap_dir() {
            Ok(dir) => dir,
            Err(e) => {
                return ToolCallResult::err(format!("Failed to open workspace capability: {}", e))
            }
        };
        let mut file = match dir.open(&rel_path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return ToolCallResult::err("LSP target is not a regular file")
            }
            Err(e) => return ToolCallResult::err(format!("Failed to read LSP target: {}", e)),
        };
        match file.metadata() {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => return ToolCallResult::err("LSP target is not a regular file"),
            Err(e) => return ToolCallResult::err(format!("Failed to read LSP target: {}", e)),
        }
        let mut buf = String::new();
        use std::io::Read;
        match file.read_to_string(&mut buf) {
            Ok(_) => buf,
            Err(e) => return ToolCallResult::err(format!("Failed to read LSP target: {}", e)),
        }
    };

    let spec = match detect_server(&full_path) {
        Some(spec) => spec,
        None => {
            return ToolCallResult::err(format!(
                "No supported LSP server mapping for file extension: {}",
                full_path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .unwrap_or("<none>")
            ))
        }
    };

    if !command_exists(&spec.command).await {
        return ToolCallResult::err(format!(
            "Required language server '{}' is not installed or not on PATH",
            spec.command
        ));
    }

    let root_uri = match Url::from_directory_path(ws.root()) {
        Ok(uri) => uri.to_string(),
        Err(_) => return ToolCallResult::err("Failed to construct workspace file URI"),
    };
    let file_uri = match Url::from_file_path(&full_path) {
        Ok(uri) => uri.to_string(),
        Err(_) => return ToolCallResult::err("Failed to construct file URI"),
    };

    let timeout = Duration::from_millis(
        timeout_ms
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(1_000, 60_000),
    );
    let started = Instant::now();
    let session_key = (ws.root().to_path_buf(), spec.command.clone());
    let session_opt = acquire_session(&session_key);

    let is_new_session = session_opt.is_none();
    let mut session = match session_opt {
        Some(s) => s,
        None => match LspSession::spawn(&spec, ws.root()) {
            Ok(s) => s,
            Err(e) => return ToolCallResult::err(e),
        },
    };

    if is_new_session {
        let init_id = session.next_id();
        let init = json!({
            "jsonrpc": "2.0",
            "id": init_id,
            "method": "initialize",
            "params": {
                "processId": Value::Null,
                "rootUri": root_uri,
                "capabilities": {
                    "textDocument": {
                        "publishDiagnostics": {"relatedInformation": true},
                        "definition": {"dynamicRegistration": false, "linkSupport": true},
                        "references": {"dynamicRegistration": false},
                        "documentSymbol": {"dynamicRegistration": false, "hierarchicalDocumentSymbolSupport": true}
                    },
                    "workspace": {"workspaceFolders": true}
                },
                "workspaceFolders": [{"uri": root_uri, "name": "workspace"}],
                "trace": "off"
            }
        });
        if let Err(e) = session.send(&init).await {
            session.terminate().await;
            return ToolCallResult::err(e);
        }

        let init_response = match session.wait_for_response(init_id, timeout).await {
            Ok(response) => response,
            Err(e) => {
                let stderr = session.terminate().await;
                return ToolCallResult::err(format!(
                    "LSP initialize failed via '{}': {}{}",
                    spec.command,
                    e,
                    if stderr.trim().is_empty() {
                        String::new()
                    } else {
                        format!("; server stderr: {}", truncate(&stderr, 1500))
                    }
                ));
            }
        };
        if let Some(error) = init_response.get("error") {
            let stderr = session.terminate().await;
            return ToolCallResult::err(format!(
                "LSP initialize returned error: {}{}",
                error,
                if stderr.trim().is_empty() {
                    String::new()
                } else {
                    format!("; server stderr: {}", truncate(&stderr, 1500))
                }
            ));
        }

        if let Err(e) = session
            .send(&json!({
                "jsonrpc": "2.0",
                "method": "initialized",
                "params": {}
            }))
            .await
        {
            session.terminate().await;
            return ToolCallResult::err(e);
        }
    } else {
        // Close prior document handle if open to ensure clean state
        let _ = session
            .send(&json!({
                "jsonrpc": "2.0",
                "method": "textDocument/didClose",
                "params": {
                    "textDocument": {
                        "uri": file_uri
                    }
                }
            }))
            .await;
    }

    session.drain_rx();

    if let Err(e) = session
        .send(&json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {
                "textDocument": {
                    "uri": file_uri,
                    "languageId": spec.language_id,
                    "version": 1,
                    "text": source
                }
            }
        }))
        .await
    {
        session.terminate().await;
        return ToolCallResult::err(e);
    }

    if !matches!(operation, LspOperation::Diagnostics) {
        session
            .wait_for_semantic_ready(timeout.min(Duration::from_secs(5)))
            .await;
    }

    let req_id = session.next_id();
    let operation_result = match operation {
        LspOperation::Diagnostics => session.collect_diagnostics(&file_uri, timeout).await,
        LspOperation::Definition | LspOperation::References => {
            let line = line.unwrap_or(1);
            let character = character.unwrap_or(1);
            if line == 0 || character == 0 {
                Err("LSP line and character are 1-indexed and must be >= 1".into())
            } else {
                let method = match operation {
                    LspOperation::Definition => "textDocument/definition",
                    LspOperation::References => "textDocument/references",
                    _ => unreachable!(),
                };
                let mut params = json!({
                    "textDocument": {"uri": file_uri},
                    "position": {"line": line - 1, "character": character - 1}
                });
                if matches!(operation, LspOperation::References) {
                    params["context"] = json!({"includeDeclaration": true});
                }
                session.request_value(req_id, method, params, timeout).await
            }
        }
        LspOperation::Symbols => {
            session
                .request_value(
                    req_id,
                    "textDocument/documentSymbol",
                    json!({"textDocument": {"uri": file_uri}}),
                    timeout,
                )
                .await
        }
    };

    let is_session_healthy = session.is_alive() && operation_result.is_ok();
    let stderr = session.get_stderr();

    if is_session_healthy {
        release_session(session_key, session);
    } else {
        session.terminate().await;
    }
    match operation_result {
        Ok(result) => ToolCallResult::ok(json!({
            "operation": operation.as_str(),
            "path": path_str,
            "server": spec.command,
            "language_id": spec.language_id,
            "result": result,
            "duration_ms": started.elapsed().as_millis() as u64,
            "stderr": stderr,
        })),
        Err(e) => ToolCallResult::err(format!(
            "LSP {} failed via {}: {}{}",
            operation.as_str(),
            spec.command,
            e,
            if stderr.trim().is_empty() {
                String::new()
            } else {
                format!("; server stderr: {}", truncate(&stderr, 1500))
            }
        )),
    }
}

fn detect_server(path: &Path) -> Option<ServerSpec> {
    let ext = path.extension()?.to_str()?.to_ascii_lowercase();
    match ext.as_str() {
        "rs" => Some(ServerSpec {
            command: "rust-analyzer".into(),
            args: vec![],
            language_id: "rust".into(),
        }),
        "ts" => Some(ServerSpec {
            command: "typescript-language-server".into(),
            args: vec!["--stdio".into()],
            language_id: "typescript".into(),
        }),
        "tsx" => Some(ServerSpec {
            command: "typescript-language-server".into(),
            args: vec!["--stdio".into()],
            language_id: "typescriptreact".into(),
        }),
        "js" | "mjs" | "cjs" => Some(ServerSpec {
            command: "typescript-language-server".into(),
            args: vec!["--stdio".into()],
            language_id: "javascript".into(),
        }),
        "jsx" => Some(ServerSpec {
            command: "typescript-language-server".into(),
            args: vec!["--stdio".into()],
            language_id: "javascriptreact".into(),
        }),
        "py" => Some(ServerSpec {
            command: "pyright-langserver".into(),
            args: vec!["--stdio".into()],
            language_id: "python".into(),
        }),
        "go" => Some(ServerSpec {
            command: "gopls".into(),
            args: vec!["serve".into()],
            language_id: "go".into(),
        }),
        _ => None,
    }
}

async fn command_exists(command: &str) -> bool {
    if let Ok(guard) = COMMAND_CACHE.lock() {
        if let Some(&exists) = guard.get(command) {
            return exists;
        }
    }

    let mut probe = Command::new(command);
    probe
        .arg("--help")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let exists = timeout(Duration::from_secs(2), probe.status())
        .await
        .ok()
        .and_then(Result::ok)
        .is_some();

    if let Ok(mut guard) = COMMAND_CACHE.lock() {
        guard.insert(command.to_string(), exists);
    }
    exists
}

struct PooledSession {
    session: LspSession,
    last_used: Instant,
}

/// Reaps idle and dead sessions from the session cache under an existing lock.
fn reap_idle_sessions_locked(guard: &mut SessionCacheMap, max_idle: Duration) -> usize {
    let mut terminated = 0;
    let now = Instant::now();
    for sessions in guard.values_mut() {
        let mut active = Vec::new();
        for mut pooled in sessions.drain(..) {
            if now.saturating_duration_since(pooled.last_used) > max_idle
                || !pooled.session.is_alive()
            {
                pooled.session.terminate_now();
                terminated += 1;
            } else {
                active.push(pooled);
            }
        }
        *sessions = active;
    }
    guard.retain(|_, sessions| !sessions.is_empty());
    terminated
}

fn acquire_session(key: &(PathBuf, String)) -> Option<LspSession> {
    let mut guard = SESSION_CACHE.lock().ok()?;
    // Wire idle-session reaping deterministically into pool access
    reap_idle_sessions_locked(&mut guard, DEFAULT_MAX_IDLE);
    if let Some(sessions) = guard.get_mut(key) {
        while let Some(mut pooled) = sessions.pop() {
            if pooled.session.is_alive() {
                return Some(pooled.session);
            } else {
                pooled.session.terminate_now();
            }
        }
    }
    None
}

fn release_session(key: (PathBuf, String), mut session: LspSession) {
    if !session.is_alive() {
        session.terminate_now();
        return;
    }
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        // Wire idle-session reaping deterministically into pool access
        reap_idle_sessions_locked(&mut guard, DEFAULT_MAX_IDLE);
        let sessions = guard.entry(key).or_default();
        if sessions.len() < MAX_SESSIONS_PER_KEY {
            sessions.push(PooledSession {
                session,
                last_used: Instant::now(),
            });
        } else {
            session.terminate_now();
        }
    } else {
        session.terminate_now();
    }
}

/// Terminate all pooled LSP servers for a specific workspace root.
pub fn terminate_workspace_lsp(workspace_root: &Path) -> usize {
    let canonical =
        dunce::canonicalize(workspace_root).unwrap_or_else(|_| workspace_root.to_path_buf());
    let mut terminated = 0;
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        let mut keys_to_remove = Vec::new();
        for (key, sessions) in guard.iter_mut() {
            if key.0 == canonical || key.0 == workspace_root {
                for mut pooled in sessions.drain(..) {
                    pooled.session.terminate_now();
                    terminated += 1;
                }
                keys_to_remove.push(key.clone());
            }
        }
        for key in keys_to_remove {
            guard.remove(&key);
        }
    }
    terminated
}

/// Terminate all pooled LSP servers that have been idle longer than `max_idle`.
pub fn terminate_idle_lsp(max_idle: Duration) -> usize {
    let mut terminated = 0;
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        terminated = reap_idle_sessions_locked(&mut guard, max_idle);
    }
    terminated
}

/// Explicit pool sweep trigger to reap idle sessions across all workspaces.
pub fn sweep_idle_lsp(max_idle: Duration) -> usize {
    terminate_idle_lsp(max_idle)
}

/// Terminate and remove all LSP servers across all workspaces.
pub fn shutdown_lsp_pool() -> usize {
    let mut terminated = 0;
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        for (_, mut sessions) in guard.drain() {
            for mut pooled in sessions.drain(..) {
                pooled.session.terminate_now();
                terminated += 1;
            }
        }
    }
    terminated
}

/// Returns the total number of idle LSP sessions currently pooled.
pub fn lsp_pool_size() -> usize {
    SESSION_CACHE
        .lock()
        .map(|guard| guard.values().map(|v| v.len()).sum())
        .unwrap_or(0)
}

/// Internal state for the bounded LSP stdout queue.
struct BoundedQueueState {
    buffer: VecDeque<Value>,
    capacity: usize,
    closed: bool,
}

/// Sender handle for bounded LSP stdout messages with explicit overflow policy.
struct BoundedLspSender {
    state: Arc<Mutex<BoundedQueueState>>,
    notify: Arc<tokio::sync::Notify>,
}

/// Receiver handle for bounded LSP stdout messages.
struct BoundedLspReceiver {
    state: Arc<Mutex<BoundedQueueState>>,
    notify: Arc<tokio::sync::Notify>,
}

/// Check if a JSON-RPC message is an LSP response awaited by the request path.
/// Responses carry a non-null `id` field and represent server replies to client requests.
/// Upper bound on retained language-server stderr.
const STDERR_BUFFER_LIMIT: usize = 64 * 1024;

/// Deadline for a single write to a language server's stdin.
const LSP_WRITE_TIMEOUT: Duration = Duration::from_secs(15);

/// Trims a stderr buffer to `limit` bytes, cutting only on a UTF-8 character boundary.
///
/// `String::drain` takes BYTE offsets and panics when the offset splits a codepoint, so the naive
/// `buf.len() - limit` arithmetic crashes the reader task as soon as a language server emits
/// multi-byte output (Korean is 3 bytes per character) that crosses the limit.
fn trim_stderr_buffer(buf: &mut String, limit: usize) {
    if buf.len() <= limit {
        return;
    }
    let target = buf.len() - limit;
    let cut = buf
        .char_indices()
        .map(|(idx, _)| idx)
        .find(|idx| *idx >= target)
        .unwrap_or(buf.len());
    buf.drain(..cut);
}

fn is_lsp_response(msg: &Value) -> bool {
    msg.get("id").is_some() && !msg["id"].is_null()
}

/// Creates a bounded LSP stdout channel with capacity and overflow policy.
///
/// # Overflow Policy:
/// Pooled sessions can accumulate diagnostics and progress notifications while idle.
/// To keep memory usage strictly bounded:
/// 1. The channel enforces `capacity` elements at most.
/// 2. Stale progress/notifications (`id` absent/null) are evicted from the front (oldest first)
///    when capacity is exceeded, ensuring fresh notifications are retained.
/// 3. Responses awaited by the request path (`id` present) are NEVER dropped:
///    - If space is needed for a response, the oldest notification is evicted.
///    - Even if the queue is full of responses, incoming responses are enqueued to prevent deadlocks.
fn bounded_lsp_channel(capacity: usize) -> (BoundedLspSender, BoundedLspReceiver) {
    let state = Arc::new(Mutex::new(BoundedQueueState {
        buffer: VecDeque::with_capacity(capacity),
        capacity,
        closed: false,
    }));
    let notify = Arc::new(tokio::sync::Notify::new());
    (
        BoundedLspSender {
            state: Arc::clone(&state),
            notify: Arc::clone(&notify),
        },
        BoundedLspReceiver { state, notify },
    )
}

impl BoundedLspSender {
    fn send(&self, msg: Value) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "LSP message queue poisoned".to_string())?;
        if state.closed {
            return Err("LSP message queue closed".to_string());
        }

        let is_resp = is_lsp_response(&msg);

        if state.buffer.len() >= state.capacity {
            if is_resp {
                // Never drop responses the request path awaits.
                // Evict the oldest stale notification/progress message if one exists.
                if let Some(idx) = state.buffer.iter().position(|m| !is_lsp_response(m)) {
                    state.buffer.remove(idx);
                }
                state.buffer.push_back(msg);
            } else {
                // Incoming message is a notification/progress message.
                // On overflow drop stale notification/progress messages (oldest first).
                if let Some(idx) = state.buffer.iter().position(|m| !is_lsp_response(m)) {
                    state.buffer.remove(idx);
                    state.buffer.push_back(msg);
                }
                // If buffer is entirely responses, drop new notification to protect responses.
            }
        } else {
            state.buffer.push_back(msg);
        }

        drop(state);
        self.notify.notify_one();
        Ok(())
    }
}

impl Drop for BoundedLspSender {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
        self.notify.notify_waiters();
    }
}

impl BoundedLspReceiver {
    async fn recv(&mut self) -> Option<Value> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            {
                let mut state = self.state.lock().ok()?;
                if let Some(msg) = state.buffer.pop_front() {
                    return Some(msg);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn try_recv(&mut self) -> Result<Value, mpsc::error::TryRecvError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| mpsc::error::TryRecvError::Disconnected)?;
        if let Some(msg) = state.buffer.pop_front() {
            Ok(msg)
        } else if state.closed {
            Err(mpsc::error::TryRecvError::Disconnected)
        } else {
            Err(mpsc::error::TryRecvError::Empty)
        }
    }
}

impl Drop for BoundedLspReceiver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
    }
}

struct LspSession {
    child: Child,
    stdin: ChildStdin,
    rx: BoundedLspReceiver,
    stderr_buf: Arc<Mutex<String>>,
    reader_task: Option<JoinHandle<()>>,
    stderr_task: Option<JoinHandle<()>>,
    next_req_id: i64,
    terminated: bool,
}

impl Drop for LspSession {
    fn drop(&mut self) {
        self.terminate_now();
    }
}

impl LspSession {
    fn spawn(spec: &ServerSpec, cwd: &Path) -> Result<Self, String> {
        let mut command = Command::new(&spec.command);
        command
            .args(&spec.args)
            .current_dir(cwd)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|e| format!("Failed to spawn {}: {}", spec.command, e))?;

        let stdin = child.stdin.take().ok_or("Failed to capture LSP stdin")?;
        let stdout = child.stdout.take().ok_or("Failed to capture LSP stdout")?;
        let stderr = child.stderr.take().ok_or("Failed to capture LSP stderr")?;
        let (tx, rx) = bounded_lsp_channel(LSP_STDOUT_CAPACITY);
        let reader_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            while let Ok(Some(message)) = read_lsp_message(&mut reader).await {
                if tx.send(message).is_err() {
                    break;
                }
            }
        });
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf_clone = Arc::clone(&stderr_buf);
        let stderr_task = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {
                        if let Ok(mut buf) = stderr_buf_clone.lock() {
                            buf.push_str(&line);
                            trim_stderr_buffer(&mut buf, STDERR_BUFFER_LIMIT);
                        }
                    }
                }
            }
        });

        Ok(Self {
            child,
            stdin,
            rx,
            stderr_buf,
            reader_task: Some(reader_task),
            stderr_task: Some(stderr_task),
            next_req_id: 1,
            terminated: false,
        })
    }

    fn is_alive(&mut self) -> bool {
        if self.terminated {
            return false;
        }
        matches!(self.child.try_wait(), Ok(None))
    }

    fn next_id(&mut self) -> i64 {
        let id = self.next_req_id;
        self.next_req_id += 1;
        id
    }

    fn drain_rx(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }

    fn get_stderr(&self) -> String {
        self.stderr_buf
            .lock()
            .map(|b| b.clone())
            .unwrap_or_default()
    }

    async fn send(&mut self, value: &Value) -> Result<(), String> {
        let body = serde_json::to_vec(value).map_err(|e| format!("LSP JSON encode failed: {e}"))?;
        // A language server that stops draining its stdin fills the pipe buffer and these writes
        // never return, so the caller's timeout_ms would not bound the operation (STATE-1).
        tokio::time::timeout(LSP_WRITE_TIMEOUT, async {
            self.stdin
                .write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes())
                .await
                .map_err(|e| format!("LSP header write failed: {e}"))?;
            self.stdin
                .write_all(&body)
                .await
                .map_err(|e| format!("LSP body write failed: {e}"))?;
            self.stdin
                .flush()
                .await
                .map_err(|e| format!("LSP flush failed: {e}"))
        })
        .await
        .map_err(|_| {
            "LSP write timed out; the language server stopped reading its stdin".to_string()
        })?
    }

    async fn wait_for_response(&mut self, id: i64, wait: Duration) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!("timed out waiting for response id {id}"));
            }
            let value = timeout(remaining, self.rx.recv())
                .await
                .map_err(|_| format!("timed out waiting for response id {id}"))?
                .ok_or_else(|| format!("LSP channel closed waiting for response id {id}"))?;
            if value.get("id").and_then(Value::as_i64) == Some(id) {
                return Ok(value);
            }
        }
    }

    async fn request_value(
        &mut self,
        id: i64,
        method: &str,
        params: Value,
        wait: Duration,
    ) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + wait;
        for attempt in 0..3i64 {
            let request_id = id + attempt;
            self.send(&json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params.clone()
            }))
            .await?;
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(format!("timed out waiting for {method}"));
            }
            let response = self.wait_for_response(request_id, remaining).await?;
            if let Some(error) = response.get("error") {
                let code = error.get("code").and_then(Value::as_i64);
                if code == Some(-32801) && attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(150 * (attempt as u64 + 1))).await;
                    continue;
                }
                return Err(format!("server returned error: {error}"));
            }
            return Ok(response.get("result").cloned().unwrap_or(Value::Null));
        }
        Err(format!("{method} exhausted ContentModified retries"))
    }

    async fn wait_for_semantic_ready(&mut self, wait: Duration) {
        let deadline = tokio::time::Instant::now() + wait;
        let minimum_settle = Duration::from_millis(500);
        let poll = Duration::from_millis(250);
        let started = tokio::time::Instant::now();
        let mut saw_busy = false;

        while tokio::time::Instant::now() < deadline {
            let remaining = deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .min(poll);
            match timeout(remaining, self.rx.recv()).await {
                Ok(Some(value)) => {
                    let method = value.get("method").and_then(Value::as_str);
                    if method == Some("experimental/serverStatus") {
                        match value.pointer("/params/quiescent").and_then(Value::as_bool) {
                            Some(true) => return,
                            Some(false) => saw_busy = true,
                            None => {}
                        }
                    }
                    if method == Some("textDocument/publishDiagnostics") {
                        return;
                    }
                }
                Ok(None) => return,
                Err(_) => {
                    if !saw_busy && started.elapsed() >= minimum_settle {
                        return;
                    }
                }
            }
        }
    }

    async fn collect_diagnostics(&mut self, uri: &str, wait: Duration) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + wait;
        let quiet = Duration::from_millis(DIAGNOSTIC_QUIET_MS);
        let mut latest = None::<Value>;
        let mut last_matching = None::<tokio::time::Instant>;

        loop {
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            if let Some(last) = last_matching {
                if now.duration_since(last) >= quiet {
                    break;
                }
            }
            let remaining = if let Some(last) = last_matching {
                quiet
                    .saturating_sub(now.duration_since(last))
                    .min(deadline.saturating_duration_since(now))
            } else {
                deadline.saturating_duration_since(now)
            };
            match timeout(remaining, self.rx.recv()).await {
                Ok(Some(value)) => {
                    if value.get("method").and_then(Value::as_str)
                        == Some("textDocument/publishDiagnostics")
                        && value.pointer("/params/uri").and_then(Value::as_str) == Some(uri)
                    {
                        latest = Some(
                            value
                                .pointer("/params/diagnostics")
                                .cloned()
                                .unwrap_or_else(|| json!([])),
                        );
                        last_matching = Some(tokio::time::Instant::now());
                    }
                }
                Ok(None) | Err(_) => break,
            }
        }

        let received_publish_diagnostics = latest.is_some();
        let diagnostics = latest.unwrap_or_else(|| json!([]));
        Ok(json!({
            "diagnostics": diagnostics,
            "received_publish_diagnostics": received_publish_diagnostics
        }))
    }

    async fn terminate(&mut self) -> String {
        if self.terminated {
            return self.get_stderr();
        }
        self.terminated = true;
        let shutdown_id = self.next_id();
        let _ = self
            .send(
                &json!({"jsonrpc":"2.0","id":shutdown_id,"method":"shutdown","params":Value::Null}),
            )
            .await;
        let _ = self
            .send(&json!({"jsonrpc":"2.0","method":"exit","params":Value::Null}))
            .await;
        if timeout(Duration::from_millis(50), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.start_kill();
            let _ = timeout(Duration::from_secs(1), self.child.wait()).await;
        }
        self.abort_tasks();
        self.get_stderr()
    }

    fn terminate_now(&mut self) {
        if self.terminated {
            self.abort_tasks();
            return;
        }
        self.terminated = true;
        let _ = self.child.start_kill();
        self.abort_tasks();
    }

    fn abort_tasks(&mut self) {
        if let Some(task) = self.reader_task.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}

async fn read_lsp_message<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Value>, String> {
    let mut content_length = None::<usize>;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .map_err(|e| format!("LSP header read failed: {e}"))?;
        if read == 0 {
            return Ok(None);
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|e| format!("Invalid LSP Content-Length: {e}"))?,
            );
        }
    }

    let length = content_length.ok_or("LSP message missing Content-Length")?;
    if length > 16 * 1024 * 1024 {
        return Err("LSP message exceeds 16MB safety limit".into());
    }
    let mut body = vec![0u8; length];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|e| format!("LSP body read failed: {e}"))?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|e| format!("LSP JSON decode failed: {e}"))
}

fn truncate(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        let mut text: String = value.chars().take(max_chars).collect();
        text.push('…');
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    static SESSION_POOL_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn stderr_trim_never_splits_a_multibyte_character() {
        // Korean is 3 bytes per char, so a naive `len() - limit` byte offset lands mid-codepoint
        // and `String::drain` panics (STATE-2, which I reproduced at runtime).
        let limit = 64usize;
        let mut buf = String::new();
        while buf.len() <= limit + 10 {
            buf.push('\u{D55C}');
        }
        let original_tail: String = buf.chars().rev().take(4).collect();

        trim_stderr_buffer(&mut buf, limit);

        assert!(
            buf.len() <= limit + 3,
            "buffer must be trimmed to about the limit, got {} bytes",
            buf.len()
        );
        let kept_tail: String = buf.chars().rev().take(4).collect();
        assert_eq!(
            kept_tail, original_tail,
            "trimming must drop the OLDEST bytes and keep the newest stderr"
        );
    }

    #[test]
    fn maps_supported_extensions() {
        let rust = detect_server(Path::new("a.rs")).unwrap();
        assert_eq!(rust.language_id, "rust");
        assert_eq!(rust.command, "rust-analyzer");

        let ts = detect_server(Path::new("a.ts")).unwrap();
        assert_eq!(ts.language_id, "typescript");
        assert_eq!(ts.command, "typescript-language-server");

        let tsx = detect_server(Path::new("a.tsx")).unwrap();
        assert_eq!(tsx.language_id, "typescriptreact");

        let js = detect_server(Path::new("a.js")).unwrap();
        assert_eq!(js.language_id, "javascript");

        let py = detect_server(Path::new("a.py")).unwrap();
        assert_eq!(py.language_id, "python");
        assert_eq!(py.command, "pyright-langserver");

        let go = detect_server(Path::new("a.go")).unwrap();
        assert_eq!(go.language_id, "go");
        assert_eq!(go.command, "gopls");

        assert!(detect_server(Path::new("a.txt")).is_none());
        assert!(detect_server(Path::new("a")).is_none());
    }

    #[test]
    fn lsp_operation_names() {
        assert_eq!(LspOperation::Diagnostics.as_str(), "diagnostics");
        assert_eq!(LspOperation::Definition.as_str(), "definition");
        assert_eq!(LspOperation::References.as_str(), "references");
        assert_eq!(LspOperation::Symbols.as_str(), "symbols");
    }

    #[tokio::test]
    async fn parses_lsp_framing() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(framed.into_bytes());
        let message = read_lsp_message(&mut cursor).await.unwrap().unwrap();
        assert_eq!(message["id"], 1);

        // Empty reader returns None
        let mut empty = Cursor::new(b"");
        assert!(read_lsp_message(&mut empty).await.unwrap().is_none());

        // Header without Content-Length returns Err
        let mut invalid_header = Cursor::new(b"Some-Header: 123\r\n\r\n");
        assert!(read_lsp_message(&mut invalid_header).await.is_err());
    }

    #[tokio::test]
    async fn caches_command_exists() {
        let non_existent = "nonexistent_lsp_command_probe_test_bin";
        assert!(!command_exists(non_existent).await);
        {
            let guard = COMMAND_CACHE.lock().unwrap();
            assert_eq!(guard.get(non_existent), Some(&false));
        }
        // Second call should read from cache
        assert!(!command_exists(non_existent).await);
    }

    #[test]
    fn session_cache_mutex_accessible() {
        let key = (PathBuf::from("/tmp/test_ws"), "test-lsp".to_string());
        {
            let guard = SESSION_CACHE.lock().unwrap();
            assert!(!guard.contains_key(&key));
        }
    }

    #[test]
    fn pool_acquire_and_release_lifecycle() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        assert_eq!(lsp_pool_size(), 0);

        let ws = PathBuf::from("/tmp/test_ws_lifecycle");
        let server = "rust-analyzer".to_string();
        let key = (ws.clone(), server.clone());

        // Empty pool returns None
        assert!(acquire_session(&key).is_none());

        // Terminate functions on empty pool return 0
        assert_eq!(terminate_workspace_lsp(&ws), 0);
        assert_eq!(terminate_idle_lsp(Duration::from_secs(10)), 0);
        assert_eq!(shutdown_lsp_pool(), 0);
    }

    #[tokio::test]
    async fn pool_stores_and_terminates_spawned_session() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let spec = ServerSpec {
            command: "cat".into(),
            args: vec![],
            language_id: "test".into(),
        };

        let session =
            LspSession::spawn(&spec, &ws_path).expect("failed to spawn cat dummy session");
        let key = (ws_path.clone(), "dummy-cat".to_string());

        // Pool was empty
        assert!(acquire_session(&key).is_none());

        // Release spawned session into pool
        release_session(key.clone(), session);
        assert_eq!(lsp_pool_size(), 1);

        // Acquire returns the pooled session
        let mut acquired = acquire_session(&key).expect("expected session in pool");
        assert!(acquired.is_alive());
        assert_eq!(lsp_pool_size(), 0);

        // Put it back
        release_session(key.clone(), acquired);
        assert_eq!(lsp_pool_size(), 1);

        // Terminate by workspace
        let terminated = terminate_workspace_lsp(&ws_path);
        assert_eq!(terminated, 1);
        assert_eq!(lsp_pool_size(), 0);

        // Terminate idle when empty
        assert_eq!(terminate_idle_lsp(Duration::ZERO), 0);
    }

    #[tokio::test]
    async fn pool_idle_reaping_and_shutdown() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let spec = ServerSpec {
            command: "cat".into(),
            args: vec![],
            language_id: "test".into(),
        };

        let s1 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s1");
        let s2 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s2");

        let key1 = (ws_path.clone(), "cat1".to_string());
        let key2 = (ws_path.clone(), "cat2".to_string());

        release_session(key1.clone(), s1);
        release_session(key2.clone(), s2);
        assert_eq!(lsp_pool_size(), 2);

        // Idle duration zero terminates everything older than 0s
        let reaped = terminate_idle_lsp(Duration::ZERO);
        assert_eq!(reaped, 2);
        assert_eq!(lsp_pool_size(), 0);

        // Spawn another and test shutdown_lsp_pool
        let s3 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s3");
        release_session(key1.clone(), s3);
        assert_eq!(lsp_pool_size(), 1);
        assert_eq!(shutdown_lsp_pool(), 1);
        assert_eq!(lsp_pool_size(), 0);
    }

    #[tokio::test]
    async fn bounded_queue_flooding_idle_session_stays_bounded() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let flood_cmd = r#"for i in $(seq 1 200); do msg="{\"jsonrpc\":\"2.0\",\"method\":\"$/progress\",\"params\":{\"i\":$i}}"; printf "Content-Length: %d\r\n\r\n%s" "${#msg}" "$msg"; done"#;

        let spec = ServerSpec {
            command: "sh".into(),
            args: vec!["-c".into(), flood_cmd.into()],
            language_id: "test".into(),
        };

        let mut session =
            LspSession::spawn(&spec, &ws_path).expect("failed to spawn sh dummy session");

        // Wait for sh to exit and reader_task to finish reading all 200 flooded messages
        if let Some(reader) = session.reader_task.take() {
            timeout(Duration::from_secs(5), reader)
                .await
                .expect("reader task timed out")
                .expect("reader task join failed");
        }

        // Count messages currently queued in the session feed
        let mut queued = 0;
        while session.rx.try_recv().is_ok() {
            queued += 1;
        }

        // Under a bounded queue (e.g. capacity 64), the queue must stay bounded
        // On current unbounded channel, all 200 messages are queued and this assertion fails (RED)
        assert!(
            queued <= 64,
            "Expected queue to stay bounded <= 64, but got {queued} queued messages"
        );
    }

    #[tokio::test]
    async fn idle_reap_triggered_via_production_sweep() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let spec = ServerSpec {
            command: "cat".into(),
            args: vec![],
            language_id: "test".into(),
        };

        let s1 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn dummy session 1");
        let key1 = (ws_path.clone(), "cat1".to_string());
        let key2 = (ws_path.clone(), "cat2".to_string());

        release_session(key1.clone(), s1);
        assert_eq!(lsp_pool_size(), 1);

        // Artificially age the session in SESSION_CACHE past the idle timeout
        {
            let mut guard = SESSION_CACHE.lock().unwrap();
            if let Some(sessions) = guard.get_mut(&key1) {
                for pooled in sessions.iter_mut() {
                    pooled.last_used = Instant::now() - Duration::from_secs(600);
                }
            }
        }

        // Trigger production pool access (acquire_session)
        let _ = acquire_session(&key2);

        // Idle session must be reaped deterministically on pool access/sweep
        assert_eq!(
            lsp_pool_size(),
            0,
            "Expected idle session to be reaped on pool access sweep"
        );
    }

    #[tokio::test]
    async fn bounded_queue_preserves_awaited_responses_under_flood() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        // Floods 200 notifications, then sends 1 response with id 42
        let flood_and_respond_cmd = r#"for i in $(seq 1 200); do msg="{\"jsonrpc\":\"2.0\",\"method\":\"$/progress\",\"params\":{\"i\":$i}}"; printf "Content-Length: %d\r\n\r\n%s" "${#msg}" "$msg"; done; resp='{"jsonrpc":"2.0","id":42,"result":{"verified":true}}'; printf "Content-Length: %d\r\n\r\n%s" "${#resp}" "$resp""#;

        let spec = ServerSpec {
            command: "sh".into(),
            args: vec!["-c".into(), flood_and_respond_cmd.into()],
            language_id: "test".into(),
        };

        let mut session =
            LspSession::spawn(&spec, &ws_path).expect("failed to spawn dummy session");

        // Response must be preserved and awaited successfully despite 200 notifications
        let response = session
            .wait_for_response(42, Duration::from_secs(5))
            .await
            .expect("response with id 42 must not be dropped by bounded queue overflow");
        assert_eq!(
            response["result"]["verified"], true,
            "response payload must match"
        );
    }

    #[tokio::test]
    async fn idle_reap_triggered_on_release_session() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let spec = ServerSpec {
            command: "cat".into(),
            args: vec![],
            language_id: "test".into(),
        };

        let s1 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s1");
        let s2 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s2");
        let key1 = (ws_path.clone(), "cat1".to_string());
        let key2 = (ws_path.clone(), "cat2".to_string());

        release_session(key1.clone(), s1);
        assert_eq!(lsp_pool_size(), 1);

        // Age s1 past idle timeout
        {
            let mut guard = SESSION_CACHE.lock().unwrap();
            if let Some(sessions) = guard.get_mut(&key1) {
                for pooled in sessions.iter_mut() {
                    pooled.last_used = Instant::now() - Duration::from_secs(600);
                }
            }
        }

        // Releasing s2 into pool triggers idle reaping of s1
        release_session(key2.clone(), s2);
        assert_eq!(lsp_pool_size(), 1); // s1 reaped, only s2 remains

        // Verify key1 was reaped and key2 is present
        assert!(acquire_session(&key1).is_none());
        assert!(acquire_session(&key2).is_some());
    }

    #[tokio::test]
    async fn sweep_idle_lsp_explicit_trigger() {
        let _pool_lock = SESSION_POOL_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        shutdown_lsp_pool();
        let temp_dir = tempfile::tempdir().unwrap();
        let ws_path = dunce::canonicalize(temp_dir.path()).unwrap();

        let spec = ServerSpec {
            command: "cat".into(),
            args: vec![],
            language_id: "test".into(),
        };

        let s1 = LspSession::spawn(&spec, &ws_path).expect("failed to spawn s1");
        let key1 = (ws_path.clone(), "cat1".to_string());

        release_session(key1.clone(), s1);
        assert_eq!(lsp_pool_size(), 1);

        // Age s1 past 10 seconds
        {
            let mut guard = SESSION_CACHE.lock().unwrap();
            if let Some(sessions) = guard.get_mut(&key1) {
                for pooled in sessions.iter_mut() {
                    pooled.last_used = Instant::now() - Duration::from_secs(20);
                }
            }
        }

        // Explicit sweep trigger with 10s threshold
        let reaped = sweep_idle_lsp(Duration::from_secs(10));
        assert_eq!(reaped, 1);
        assert_eq!(lsp_pool_size(), 0);
    }
}
