use crate::security::Workspace;
use crate::tools::ToolCallResult;
use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use url::Url;

const DEFAULT_TIMEOUT_MS: u64 = 15_000;
const DIAGNOSTIC_QUIET_MS: u64 = 600;

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
    let mut session = match LspSession::spawn(&spec, ws.root()) {
        Ok(session) => session,
        Err(e) => return ToolCallResult::err(e),
    };

    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
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

    let init_response = match session.wait_for_response(1, timeout) {
        Ok(response) => response,
        Err(e) => {
            session.terminate();
            return ToolCallResult::err(format!(
                "LSP initialize failed via '{}': {}",
                spec.command, e
            ));
        }
    };
    if let Some(error) = init_response.get("error") {
        session.terminate();
        return ToolCallResult::err(format!("LSP initialize returned error: {}", error));
    }

    if let Err(e) = session.send(&json!({
        "jsonrpc": "2.0",
        "method": "initialized",
        "params": {}
    })) {
        session.terminate();
        return ToolCallResult::err(e);
    }
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
                session.request_value(2, method, params, timeout)
            }
        }
        LspOperation::Symbols => session.request_value(
            2,
            "textDocument/documentSymbol",
            json!({"textDocument": {"uri": file_uri}}),
            timeout,
        ),
    };

    let stderr = session.terminate();
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
    Command::new(command)
        .arg("--help")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok()
}

struct LspSession {
    child: Child,
    stdin: ChildStdin,
    rx: Receiver<Value>,
    stderr_thread: Option<thread::JoinHandle<String>>,
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
        let stderr_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut text = String::new();
            let _ = reader.read_to_string(&mut text);
            text
        });

        Ok(Self {
            child,
            stdin,
            rx,
            stderr_thread: Some(stderr_thread),
        })
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
        let _ =
            self.send(&json!({"jsonrpc":"2.0","id":99,"method":"shutdown","params":Value::Null}));
        let _ = self.send(&json!({"jsonrpc":"2.0","method":"exit","params":Value::Null}));
        thread::sleep(Duration::from_millis(50));
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.stderr_thread
            .take()
            .and_then(|handle| handle.join().ok())
            .unwrap_or_default()
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

    #[test]
    fn maps_supported_extensions() {
        let rust = detect_server(Path::new("a.rs")).unwrap();
        assert_eq!(rust.language_id, "rust");
        assert_eq!(rust.command, "rust-analyzer");
        assert_eq!(
            detect_server(Path::new("a.tsx")).unwrap().language_id,
            "typescriptreact"
        );
        assert_eq!(
            detect_server(Path::new("a.py")).unwrap().language_id,
            "python"
        );
        assert!(detect_server(Path::new("a.txt")).is_none());
    }

    #[test]
    fn parses_lsp_framing() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let framed = format!("Content-Length: {}\r\n\r\n{}", body.len(), body);
        let mut cursor = Cursor::new(framed.into_bytes());
        let message = read_lsp_message(&mut cursor).unwrap().unwrap();
        assert_eq!(message["id"], 1);
    }
}
