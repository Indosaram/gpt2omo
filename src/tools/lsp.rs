use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use url::Url;

type SessionCacheMap = HashMap<(PathBuf, String), Vec<PooledSession>>;

static COMMAND_CACHE: LazyLock<Mutex<HashMap<String, bool>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static SESSION_CACHE: LazyLock<Mutex<SessionCacheMap>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const DEFAULT_TIMEOUT_MS: u64 = 15_000;
const DIAGNOSTIC_QUIET_MS: u64 = 600;
const MAX_SESSIONS_PER_KEY: usize = 2;

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

pub fn handle_lsp(
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
    if !full_path.is_file() {
        return ToolCallResult::err("LSP target is not a regular file");
    }

    let source = match fs::read_to_string(&full_path) {
        Ok(source) => source,
        Err(e) => return ToolCallResult::err(format!("Failed to read LSP target: {}", e)),
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

    if !command_exists(&spec.command) {
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
        if let Err(e) = session.send(&init) {
            session.terminate();
            return ToolCallResult::err(e);
        }

        let init_response = match session.wait_for_response(init_id, timeout) {
            Ok(response) => response,
            Err(e) => {
                let stderr = session.terminate();
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
            let stderr = session.terminate();
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

        if let Err(e) = session.send(&json!({
            "jsonrpc": "2.0",
            "method": "initialized",
            "params": {}
        })) {
            session.terminate();
            return ToolCallResult::err(e);
        }
    } else {
        // Close prior document handle if open to ensure clean state
        let _ = session.send(&json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didClose",
            "params": {
                "textDocument": {
                    "uri": file_uri
                }
            }
        }));
    }

    session.drain_rx();

    if let Err(e) = session.send(&json!({
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
    })) {
        session.terminate();
        return ToolCallResult::err(e);
    }

    if !matches!(operation, LspOperation::Diagnostics) {
        session.wait_for_semantic_ready(timeout.min(Duration::from_secs(5)));
    }

    let req_id = session.next_id();
    let operation_result = match operation {
        LspOperation::Diagnostics => session.collect_diagnostics(&file_uri, timeout),
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
                session.request_value(req_id, method, params, timeout)
            }
        }
        LspOperation::Symbols => session.request_value(
            req_id,
            "textDocument/documentSymbol",
            json!({"textDocument": {"uri": file_uri}}),
            timeout,
        ),
    };

    let is_session_healthy = session.is_alive() && operation_result.is_ok();
    let stderr = session.get_stderr();

    if is_session_healthy {
        release_session(session_key, session);
    } else {
        session.terminate();
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

fn command_exists(command: &str) -> bool {
    if let Ok(guard) = COMMAND_CACHE.lock() {
        if let Some(&exists) = guard.get(command) {
            return exists;
        }
    }

    let exists = Command::new(command)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok();

    if let Ok(mut guard) = COMMAND_CACHE.lock() {
        guard.insert(command.to_string(), exists);
    }
    exists
}

struct PooledSession {
    session: LspSession,
    last_used: Instant,
}

fn acquire_session(key: &(PathBuf, String)) -> Option<LspSession> {
    let mut guard = SESSION_CACHE.lock().ok()?;
    if let Some(sessions) = guard.get_mut(key) {
        while let Some(mut pooled) = sessions.pop() {
            if pooled.session.is_alive() {
                return Some(pooled.session);
            } else {
                pooled.session.terminate();
            }
        }
    }
    None
}

fn release_session(key: (PathBuf, String), mut session: LspSession) {
    if !session.is_alive() {
        session.terminate();
        return;
    }
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        let sessions = guard.entry(key).or_default();
        if sessions.len() < MAX_SESSIONS_PER_KEY {
            sessions.push(PooledSession {
                session,
                last_used: Instant::now(),
            });
        } else {
            session.terminate();
        }
    } else {
        session.terminate();
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
                    pooled.session.terminate();
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
        let now = Instant::now();
        for sessions in guard.values_mut() {
            let mut active = Vec::new();
            for mut pooled in sessions.drain(..) {
                if now.saturating_duration_since(pooled.last_used) > max_idle
                    || !pooled.session.is_alive()
                {
                    pooled.session.terminate();
                    terminated += 1;
                } else {
                    active.push(pooled);
                }
            }
            *sessions = active;
        }
        guard.retain(|_, sessions| !sessions.is_empty());
    }
    terminated
}

/// Terminate and remove all LSP servers across all workspaces.
pub fn shutdown_lsp_pool() -> usize {
    let mut terminated = 0;
    if let Ok(mut guard) = SESSION_CACHE.lock() {
        for (_, mut sessions) in guard.drain() {
            for mut pooled in sessions.drain(..) {
                pooled.session.terminate();
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

struct LspSession {
    child: Child,
    stdin: ChildStdin,
    rx: Receiver<Value>,
    stderr_buf: Arc<Mutex<String>>,
    stderr_thread: Option<thread::JoinHandle<()>>,
    next_req_id: i64,
    terminated: bool,
}

impl Drop for LspSession {
    fn drop(&mut self) {
        self.terminate();
    }
}

impl LspSession {
    fn spawn(spec: &ServerSpec, cwd: &Path) -> Result<Self, String> {
        let mut child = Command::new(&spec.command)
            .args(&spec.args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("Failed to spawn {}: {}", spec.command, e))?;

        let stdin = child.stdin.take().ok_or("Failed to capture LSP stdin")?;
        let stdout = child.stdout.take().ok_or("Failed to capture LSP stdout")?;
        let stderr = child.stderr.take().ok_or("Failed to capture LSP stderr")?;
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            while let Ok(Some(message)) = read_lsp_message(&mut reader) {
                if tx.send(message).is_err() {
                    break;
                }
            }
        });
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf_clone = Arc::clone(&stderr_buf);
        let stderr_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line) {
                if n == 0 {
                    break;
                }
                if let Ok(mut buf) = stderr_buf_clone.lock() {
                    buf.push_str(&line);
                    if buf.len() > 64 * 1024 {
                        let excess = buf.len() - 64 * 1024;
                        buf.drain(..excess);
                    }
                }
                line.clear();
            }
        });

        Ok(Self {
            child,
            stdin,
            rx,
            stderr_buf,
            stderr_thread: Some(stderr_thread),
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

    fn drain_rx(&self) {
        while self.rx.try_recv().is_ok() {}
    }

    fn get_stderr(&self) -> String {
        self.stderr_buf
            .lock()
            .map(|b| b.clone())
            .unwrap_or_default()
    }

    fn send(&mut self, value: &Value) -> Result<(), String> {
        let body =
            serde_json::to_vec(value).map_err(|e| format!("LSP JSON encode failed: {}", e))?;
        write!(self.stdin, "Content-Length: {}\r\n\r\n", body.len())
            .map_err(|e| format!("LSP header write failed: {}", e))?;
        self.stdin
            .write_all(&body)
            .map_err(|e| format!("LSP body write failed: {}", e))?;
        self.stdin
            .flush()
            .map_err(|e| format!("LSP flush failed: {}", e))
    }

    fn wait_for_response(&self, id: i64, timeout: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!("timed out waiting for response id {}", id));
            }
            let value = self
                .rx
                .recv_timeout(remaining)
                .map_err(|_| format!("timed out waiting for response id {}", id))?;
            if value.get("id").and_then(Value::as_i64) == Some(id) {
                return Ok(value);
            }
        }
    }

    fn request_value(
        &mut self,
        id: i64,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        for attempt in 0..3i64 {
            let request_id = id + attempt;
            self.send(&json!({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params.clone()
            }))?;
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!("timed out waiting for {}", method));
            }
            let response = self.wait_for_response(request_id, remaining)?;
            if let Some(error) = response.get("error") {
                let code = error.get("code").and_then(Value::as_i64);
                if code == Some(-32801) && attempt < 2 {
                    thread::sleep(Duration::from_millis(150 * (attempt as u64 + 1)));
                    continue;
                }
                return Err(format!("server returned error: {}", error));
            }
            return Ok(response.get("result").cloned().unwrap_or(Value::Null));
        }
        Err(format!("{} exhausted ContentModified retries", method))
    }

    fn wait_for_semantic_ready(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let minimum_settle = Duration::from_millis(500);
        let poll = Duration::from_millis(250);
        let started = Instant::now();
        let mut saw_busy = false;

        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now()).min(poll);
            match self.rx.recv_timeout(remaining) {
                Ok(value) => {
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
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if !saw_busy && started.elapsed() >= minimum_settle {
                        return;
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    fn collect_diagnostics(&self, uri: &str, timeout: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        let quiet = Duration::from_millis(DIAGNOSTIC_QUIET_MS);
        let mut latest = None::<Value>;
        let mut last_matching = None::<Instant>;

        loop {
            let now = Instant::now();
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

            match self.rx.recv_timeout(remaining) {
                Ok(value) => {
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
                        last_matching = Some(Instant::now());
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        let received_publish_diagnostics = latest.is_some();
        let diagnostics = latest.unwrap_or_else(|| json!([]));
        Ok(json!({
            "diagnostics": diagnostics,
            "received_publish_diagnostics": received_publish_diagnostics
        }))
    }

    fn terminate(&mut self) -> String {
        if self.terminated {
            return self.get_stderr();
        }
        self.terminated = true;
        let shutdown_id = self.next_id();
        let _ = self.send(
            &json!({"jsonrpc":"2.0","id":shutdown_id,"method":"shutdown","params":Value::Null}),
        );
        let _ = self.send(&json!({"jsonrpc":"2.0","method":"exit","params":Value::Null}));
        thread::sleep(Duration::from_millis(50));
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stderr_thread.take() {
            let _ = handle.join();
        }
        self.get_stderr()
    }
}

fn read_lsp_message<R: BufRead>(reader: &mut R) -> Result<Option<Value>, String> {
    let mut content_length = None::<usize>;
    loop {
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .map_err(|e| format!("LSP header read failed: {}", e))?;
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
                    .map_err(|e| format!("Invalid LSP Content-Length: {}", e))?,
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
        .map_err(|e| format!("LSP body read failed: {}", e))?;
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|e| format!("LSP JSON decode failed: {}", e))
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

    #[test]
    fn parses_lsp_framing() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(framed.into_bytes());
        let message = read_lsp_message(&mut cursor).unwrap().unwrap();
        assert_eq!(message["id"], 1);

        // Empty reader returns None
        let mut empty = Cursor::new(b"");
        assert!(read_lsp_message(&mut empty).unwrap().is_none());

        // Header without Content-Length returns Err
        let mut invalid_header = Cursor::new(b"Some-Header: 123\r\n\r\n");
        assert!(read_lsp_message(&mut invalid_header).is_err());
    }

    #[test]
    fn caches_command_exists() {
        let non_existent = "nonexistent_lsp_command_probe_test_bin";
        assert!(!command_exists(non_existent));
        {
            let guard = COMMAND_CACHE.lock().unwrap();
            assert_eq!(guard.get(non_existent), Some(&false));
        }
        // Second call should read from cache
        assert!(!command_exists(non_existent));
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
        let _pool_lock = SESSION_POOL_TEST_LOCK.lock().unwrap();
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

    #[test]
    fn pool_stores_and_terminates_spawned_session() {
        let _pool_lock = SESSION_POOL_TEST_LOCK.lock().unwrap();
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

    #[test]
    fn pool_idle_reaping_and_shutdown() {
        let _pool_lock = SESSION_POOL_TEST_LOCK.lock().unwrap();
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
}
