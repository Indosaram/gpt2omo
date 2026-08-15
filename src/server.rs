use crate::cli::Cli;
use crate::error::{BridgeError, Result};
use crate::events::{EventBus, HarnessEvent};
use crate::security::{Workspace, WorkspaceMux};
use crate::tools::*;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::stream::{self, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast::error::RecvError;
use tower_http::cors::CorsLayer;

const CODING_AGENT_INSTRUCTIONS: &str = r#"You are directly responsible for coding in the workspace scope assigned to this delegation by the terminal orchestrator. omo-bridge is an I/O and verification harness, not another coding agent: do not delegate implementation back to OpenCode, OMO, Codex, or another agent through this bridge. The daemon may have machine-root mount authority, but every MCP coding tool call requires this delegation scope_id and is sandboxed to that scope. Multiple scopes may run concurrently. Never omit or substitute the scope_id from the delegation prompt.

For non-trivial implementation tasks, use this workflow:
1. Inspect the relevant files, tests, and repository structure before editing. Prefer search_text, ast_grep, LSP queries, and targeted read_file calls over guessing filenames, symbols, references, or APIs.
2. Recover task_state first. Continue a matching incomplete task; otherwise create a task_plan that captures the delegated task acceptance criteria. Keep it current with task_update as work progresses.
3. Make edits with patch_file. When replacing an existing file, pass the SHA256 returned by the latest read_file whenever practical; if the precondition fails, re-read instead of overwriting stale content.
4. Run the project verification commands after edits (tests, type checks, lint, build, cargo check/clippy, etc.). Do not claim a command passed unless its returned data.success is true.
5. Diagnose failures yourself, edit again, and rerun verification. Do not stop after merely writing code.
6. Inspect git_status_diff before declaring completion so accidental or incomplete changes are visible.
7. Mark task-plan items done only when there is concrete evidence. Call completion_check at the end of a non-trivial coding task; if ready=false, continue working on its blockers.

For small read-only questions or a trivial single edit, a task plan is optional. Never fabricate file contents, command output, test results, or completion evidence. All file and command paths must remain relative to this delegation workspace, and every tool call must include its scope_id."#;

#[derive(Clone)]
pub struct AppState {
    pub workspace: Arc<WorkspaceMux>,
    pub cli: Arc<Cli>,
    pub events: Arc<EventBus>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/mcp", post(mcp_post_handler).get(mcp_sse_handler))
        .route("/events", get(events_sse_handler))
        .layer(
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any),
        )
        .with_state(state)
}

async fn healthz_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    verify_auth(&state, &headers)?;
    Ok(Json(serde_json::json!({
        "status": "ok",
        "service": "omo-bridge",
        "version": "0.7.0",
        "events": "/events",
        "workspace_mode": "multiplexed_scopes"
    })))
}

async fn mcp_sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    verify_auth(&state, &headers)?;

    let endpoint_event = Event::default().event("endpoint").data("/mcp");
    let stream = stream::iter(vec![Ok(endpoint_event)]);
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn events_sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    verify_auth(&state, &headers)?;

    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let replay = state.events.subscribe_from(last_event_id);
    let hello = state.events.connection_event();
    let gap = replay.missed.map(|(from, to)| {
        Event::default().event("lagged").data(
            serde_json::json!({
                "kind": "lagged",
                "missed_from": from,
                "missed_to": to,
                "message": "SSE replay history was too small; reconcile with task_state/completion_check"
            })
            .to_string(),
        )
    });
    let replay_through = replay.replayed_through;
    let replay_events = stream::iter(
        replay
            .events
            .into_iter()
            .map(|event| Ok(harness_event_to_sse(event))),
    );
    let initial = stream::once(async move { Ok(harness_event_to_sse(hello)) })
        .chain(stream::iter(gap.into_iter().map(Ok)))
        .chain(replay_events);
    let updates = stream::unfold(
        (replay.receiver, replay_through),
        |(mut receiver, mut last_sent)| async move {
            loop {
                match receiver.recv().await {
                    Ok(event) if event.seq <= last_sent => continue,
                    Ok(event) => {
                        last_sent = event.seq;
                        return Some((Ok(harness_event_to_sse(event)), (receiver, last_sent)));
                    }
                    Err(RecvError::Lagged(missed)) => {
                        let event = Event::default().event("lagged").data(
                            serde_json::json!({
                                "kind": "lagged",
                                "missed": missed,
                                "message": "SSE subscriber fell behind the event buffer; reconcile with task_state/completion_check"
                            })
                            .to_string(),
                        );
                        return Some((Ok(event), (receiver, last_sent)));
                    }
                    Err(RecvError::Closed) => return None,
                }
            }
        },
    );

    Ok(Sse::new(initial.chain(updates)).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

fn harness_event_to_sse(event: HarnessEvent) -> Event {
    let mut sse = Event::default()
        .event(event.kind.clone())
        .data(serde_json::to_string(&event).unwrap_or_else(|_| "{}".into()));
    if event.kind != "connected" {
        sse = sse.id(event.seq.to_string());
    }
    sse
}

async fn mcp_post_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<JsonRpcRequest>,
) -> Result<Json<JsonRpcResponse>> {
    verify_auth(&state, &headers)?;

    let res = match req.method.as_str() {
        "initialize" => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(initialize_result()),
            error: None,
        },
        "tools/list" => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(serde_json::json!({ "tools": tool_definitions() })),
            error: None,
        },
        "tools/call" => {
            let params = req.params.unwrap_or(Value::Null);
            let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let arguments = params
                .get("arguments")
                .cloned()
                .unwrap_or_else(|| serde_json::json!({}));
            let call_id = uuid::Uuid::new_v4().to_string();
            let scope_id = arguments
                .get("scope_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            let metadata = tool_event_metadata(tool_name, &arguments);

            state.events.publish(
                "tool_started",
                serde_json::json!({
                    "call_id": call_id,
                    "scope_id": scope_id,
                    "tool": tool_name,
                    "arguments": metadata,
                }),
            );

            let started = Instant::now();
            let tool_res = if scope_id.is_empty() {
                ToolCallResult::err("scope_id is required for every omo-bridge tool call")
            } else {
                match state.workspace.resolve(scope_id) {
                    Ok(workspace) => dispatch_tool(
                        &workspace,
                        &state.cli,
                        scope_id,
                        tool_name,
                        arguments.clone(),
                    ),
                    Err(error) => ToolCallResult::err(error.to_string()),
                }
            };
            let elapsed_ms = started.elapsed().as_millis() as u64;

            state.events.publish(
                "tool_finished",
                serde_json::json!({
                    "call_id": call_id,
                    "scope_id": scope_id,
                    "tool": tool_name,
                    "success": tool_res.success,
                    "error": tool_res.error,
                    "duration_ms": elapsed_ms,
                }),
            );
            publish_specialized_events(&state.events, scope_id, tool_name, &arguments, &tool_res);

            JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id: req.id,
                result: Some(serde_json::json!({
                    "content": [
                        {
                            "type": "text",
                            "text": serde_json::to_string_pretty(&tool_res).unwrap_or_default()
                        }
                    ],
                    "isError": !tool_res.success
                })),
                error: None,
            }
        }
        "ping" | "notifications/initialized" => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: Some(serde_json::json!({})),
            error: None,
        },
        _ => JsonRpcResponse {
            jsonrpc: "2.0".into(),
            id: req.id,
            result: None,
            error: Some(serde_json::json!({
                "code": -32601,
                "message": format!("Method '{}' not found", req.method)
            })),
        },
    };

    Ok(Json(res))
}

fn initialize_result() -> Value {
    serde_json::json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {
                "listChanged": false
            }
        },
        "serverInfo": {
            "name": "omo-bridge",
            "version": "0.7.0"
        },
        "instructions": CODING_AGENT_INSTRUCTIONS
    })
}

fn tool_definitions() -> Vec<Value> {
    let mut tools = vec![
        serde_json::json!({
            "name": "read_file",
            "description": "Read a UTF-8 file in the delegation workspace. Returns SHA256 plus line-sliced content; use the SHA256 as patch_file's optimistic precondition before replacing an existing file.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative file path" },
                    "start_line": { "type": "integer", "minimum": 1, "description": "Start line (1-indexed)" },
                    "max_lines": { "type": "integer", "minimum": 1, "description": "Maximum lines to read" }
                },
                "required": ["path"]
            }
        }),
        serde_json::json!({
            "name": "patch_file",
            "description": "Atomically create or replace one workspace file. For existing files, pass expected_sha256 from the latest read_file to prevent stale overwrites. A successful bridge edit is recorded as a task mutation for completion verification.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative file path" },
                    "expected_sha256": { "type": "string", "description": "Expected SHA256 of the existing file before modification" },
                    "content": { "type": "string", "description": "Complete new file content" }
                },
                "required": ["path", "content"]
            }
        }),
        serde_json::json!({
            "name": "list_files",
            "description": "List workspace files/directories while respecting .gitignore and hiding dotfiles. Omit path or use '.' for the workspace root.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Optional workspace-relative subdirectory" },
                    "max_depth": { "type": "integer", "minimum": 1, "description": "Maximum traversal depth" },
                    "limit": { "type": "integer", "minimum": 1, "description": "Maximum returned entries" }
                }
            }
        }),
        serde_json::json!({
            "name": "search_text",
            "description": "Search UTF-8 source files for literal text across the delegation workspace. Returns file, line, column and preview. Prefer this before broad file reads when locating symbols, tests, routes, or configuration.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Literal text to search for" },
                    "path": { "type": "string", "description": "Optional workspace-relative file or directory scope" },
                    "case_sensitive": { "type": "boolean", "description": "Whether matching is case-sensitive (default false)" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 500, "description": "Maximum matches (default 80)" }
                },
                "required": ["query"]
            }
        }),
        serde_json::json!({
            "name": "ast_grep",
            "description": "Run structural AST pattern search with ast-grep/sg inside the delegation workspace. Use this for syntax-aware call/import/class/function patterns when literal search_text is insufficient.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "pattern": { "type": "string", "description": "ast-grep pattern such as console.log($A) or function $F($$$ARGS) { $$$BODY }" },
                    "path": { "type": "string", "description": "Optional workspace-relative search scope; default '.'" },
                    "language": { "type": "string", "description": "Optional ast-grep language override such as js, ts, rust, python" },
                    "max_results": { "type": "integer", "minimum": 1, "maximum": 1000, "description": "Maximum parsed matches; default 100" }
                },
                "required": ["pattern"]
            }
        }),
        serde_json::json!({
            "name": "lsp_diagnostics",
            "description": "Open one source file in its installed language server and return publishDiagnostics. Supported mappings include Rust/rust-analyzer, JS/TS/typescript-language-server, Python/pyright-langserver, and Go/gopls when installed.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative source file" },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 60000, "description": "Language-server timeout; default 15000" }
                },
                "required": ["path"]
            }
        }),
        serde_json::json!({
            "name": "lsp_definition",
            "description": "Resolve the definition for the symbol at a 1-indexed line and character using the file's language server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative source file" },
                    "line": { "type": "integer", "minimum": 1, "description": "1-indexed line" },
                    "character": { "type": "integer", "minimum": 1, "description": "1-indexed UTF-16/LSP character position approximation" },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 60000 }
                },
                "required": ["path", "line", "character"]
            }
        }),
        serde_json::json!({
            "name": "lsp_references",
            "description": "Find references, including the declaration, for the symbol at a 1-indexed line and character using the file's language server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative source file" },
                    "line": { "type": "integer", "minimum": 1 },
                    "character": { "type": "integer", "minimum": 1 },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 60000 }
                },
                "required": ["path", "line", "character"]
            }
        }),
        serde_json::json!({
            "name": "lsp_symbols",
            "description": "Return document symbols for one source file using its language server; useful for code navigation before broad reads.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "path": { "type": "string", "description": "Workspace-relative source file" },
                    "timeout_ms": { "type": "integer", "minimum": 1000, "maximum": 60000 }
                },
                "required": ["path"]
            }
        }),
        serde_json::json!({
            "name": "run_command",
            "description": "Run a whitelisted build/test/verification command directly in the workspace without a shell. Read-only Git inspection is available by default; repository-controlled build/test commands require --allow-host-command-execution and an OS-level sandbox. Enforces the configured timeout and caps captured output.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "command": { "type": "string", "description": "Command such as cargo test, cargo clippy -- -D warnings, npm test, pytest, vitest, go test, or git status" }
                },
                "required": ["command"]
            }
        }),
        serde_json::json!({
            "name": "git_status_diff",
            "description": "Inspect git porcelain status, diff stat, bounded staged/unstaged unified diff, and git diff --check results before completion.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        serde_json::json!({
            "name": "task_plan",
            "description": "Create or replace the persistent implementation plan for a non-trivial coding task. State is stored outside the repository and survives Chat context compaction and bridge restarts while temporary state remains available.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "goal": { "type": "string", "description": "Concise task objective/acceptance target" },
                    "items": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 100,
                        "items": { "type": "string" },
                        "description": "Concrete implementation and verification steps"
                    }
                },
                "required": ["goal", "items"]
            }
        }),
        serde_json::json!({
            "name": "task_update",
            "description": "Update one persistent task-plan item as work progresses. Do not mark an item done until its acceptance condition has evidence.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "item_id": { "type": "string", "description": "Plan item id such as T1" },
                    "status": { "type": "string", "enum": ["pending", "in_progress", "done", "blocked"] },
                    "note": { "type": "string", "description": "Optional evidence, result, or blocker note" }
                },
                "required": ["item_id", "status"]
            }
        }),
        serde_json::json!({
            "name": "task_state",
            "description": "Recover the current persistent goal, checklist, latest bridge mutation, and recorded verification history. Use after context compaction or when resuming work.",
            "inputSchema": { "type": "object", "properties": {} }
        }),
        serde_json::json!({
            "name": "completion_check",
            "description": "Deterministic completion audit for direct ChatGPT coding: checks task-plan completion, successful verification after the latest bridge edit, working-tree change evidence, and git diff --check. Working-tree changes are always required; if ready=false, continue working.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "require_task_plan": { "type": "boolean", "enum": [true], "description": "Legacy compatibility field; if supplied it must be true because a fully-done task plan is always required" },
                    "require_verification": { "type": "boolean", "enum": [true], "description": "Legacy compatibility field; if supplied it must be true because post-mutation verification is always required" },
                    "require_changes": { "type": "boolean", "enum": [true], "description": "Legacy compatibility field; if supplied it must be true because working-tree change evidence is always required" }
                }
            }
        }),
    ];
    for tool in &mut tools {
        let Some(schema) = tool.get_mut("inputSchema").and_then(Value::as_object_mut) else {
            continue;
        };
        let properties = schema
            .entry("properties")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(properties) = properties.as_object_mut() {
            properties.insert(
                "scope_id".into(),
                serde_json::json!({
                    "type": "string",
                    "description": "Per-delegation workspace scope id supplied by the OMO delegation prompt"
                }),
            );
        }
        let required = schema
            .entry("required")
            .or_insert_with(|| serde_json::json!([]));
        if let Some(required) = required.as_array_mut() {
            if !required
                .iter()
                .any(|value| value.as_str() == Some("scope_id"))
            {
                required.push(serde_json::json!("scope_id"));
            }
        }
    }
    tools
}

fn verify_auth(state: &AppState, headers: &HeaderMap) -> Result<()> {
    if let Some(expected_token) = &state.cli.token {
        let auth_header = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");

        let expected = format!("Bearer {}", expected_token);
        if auth_header != expected {
            return Err(BridgeError::Security(
                "Unauthorized: Invalid Bearer token".into(),
            ));
        }
    }
    Ok(())
}

fn tool_event_metadata(name: &str, args: &Value) -> Value {
    match name {
        "read_file" => serde_json::json!({
            "path": args.get("path"),
            "start_line": args.get("start_line"),
            "max_lines": args.get("max_lines"),
        }),
        "patch_file" => serde_json::json!({
            "path": args.get("path"),
            "has_precondition": args.get("expected_sha256").is_some(),
            "content_bytes": args.get("content").and_then(Value::as_str).map(str::len),
        }),
        "list_files" => serde_json::json!({
            "path": args.get("path"),
            "max_depth": args.get("max_depth"),
            "limit": args.get("limit"),
        }),
        "search_text" => serde_json::json!({
            "query": args.get("query"),
            "path": args.get("path"),
            "case_sensitive": args.get("case_sensitive"),
            "max_results": args.get("max_results"),
        }),
        "ast_grep" => serde_json::json!({
            "pattern": args.get("pattern"),
            "path": args.get("path"),
            "language": args.get("language"),
            "max_results": args.get("max_results"),
        }),
        "lsp_diagnostics" | "lsp_symbols" => serde_json::json!({
            "path": args.get("path"),
            "timeout_ms": args.get("timeout_ms"),
        }),
        "lsp_definition" | "lsp_references" => serde_json::json!({
            "path": args.get("path"),
            "line": args.get("line"),
            "character": args.get("character"),
            "timeout_ms": args.get("timeout_ms"),
        }),
        "run_command" => serde_json::json!({
            "command": args.get("command"),
        }),
        "task_plan" => serde_json::json!({
            "goal": args.get("goal"),
            "item_count": args.get("items").and_then(Value::as_array).map(Vec::len),
        }),
        "task_update" => serde_json::json!({
            "item_id": args.get("item_id"),
            "status": args.get("status"),
            "has_note": args.get("note").is_some(),
        }),
        "completion_check" => serde_json::json!({
            "require_task_plan": args.get("require_task_plan"),
            "require_verification": args.get("require_verification"),
            "require_changes": args.get("require_changes"),
        }),
        _ => serde_json::json!({}),
    }
}

fn publish_specialized_events(
    events: &EventBus,
    scope_id: &str,
    name: &str,
    args: &Value,
    result: &ToolCallResult,
) {
    if name == "run_command" {
        let command = args.get("command").and_then(Value::as_str).unwrap_or("");
        if crate::tools::task_state::is_verification_command(command) {
            let data = result.data.as_ref();
            events.publish(
                "verification",
                serde_json::json!({
                    "scope_id": scope_id,
                    "command": command,
                    "success": data.and_then(|v| v.get("success")).and_then(Value::as_bool).unwrap_or(false),
                    "exit_code": data.and_then(|v| v.get("exit_code")),
                    "duration_ms": data.and_then(|v| v.get("duration_ms")),
                    "timed_out": data.and_then(|v| v.get("timed_out")).and_then(Value::as_bool).unwrap_or(false),
                    "tool_error": result.error,
                }),
            );
        }
    }

    if name == "completion_check" {
        let data = result.data.as_ref();
        let ready = data
            .and_then(|v| v.get("ready"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let blockers = data
            .and_then(|v| v.get("blockers"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));
        events.publish(
            "completion",
            serde_json::json!({
                "scope_id": scope_id,
                "ready": ready,
                "blockers": blockers,
                "verification_evidence": data.and_then(|v| v.get("verification_evidence")).cloned().unwrap_or(Value::Null),
                "tool_success": result.success,
                "tool_error": result.error,
            }),
        );

        if !ready {
            let blocker_lines = blockers
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|item| format!("- {}", item))
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .filter(|text| !text.is_empty())
                .unwrap_or_else(|| "- completion_check did not return ready=true".into());
            let prompt = format!(
                "The coding task for scope {} is not complete. Continue working in this same ChatGPT conversation.\n\nCompletion blockers:\n{}\n\nUse scope_id {} on every omo-bridge tool call. Recover task_state if context was compacted, resolve every blocker, rerun the relevant verification, inspect git_status_diff, and call completion_check again. Do not stop until completion_check returns ready=true unless an external blocker makes progress impossible.",
                scope_id, blocker_lines, scope_id
            );
            events.publish(
                "continuation_required",
                serde_json::json!({
                    "scope_id": scope_id,
                    "prompt": prompt,
                    "blockers": blockers,
                    "relay_to_same_chat": true,
                }),
            );
        }
    }
}

fn dispatch_tool(
    ws: &Workspace,
    cli: &Cli,
    scope_id: &str,
    name: &str,
    args: Value,
) -> ToolCallResult {
    match name {
        "read_file" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let start = args
                .get("start_line")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            let max = args
                .get("max_lines")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            handle_read_file(ws, path, start, max, cli.max_file_bytes)
        }
        "list_files" => {
            let path = args.get("path").and_then(Value::as_str);
            let depth = args
                .get("max_depth")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            let limit = args
                .get("limit")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            handle_list_files(ws, path, depth, limit)
        }
        "search_text" => {
            let query = args.get("query").and_then(Value::as_str).unwrap_or("");
            let path = args.get("path").and_then(Value::as_str);
            let case_sensitive = args.get("case_sensitive").and_then(Value::as_bool);
            let max_results = args
                .get("max_results")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            handle_search_text(ws, query, path, case_sensitive, max_results)
        }
        "ast_grep" => {
            let pattern = args.get("pattern").and_then(Value::as_str).unwrap_or("");
            let path = args.get("path").and_then(Value::as_str);
            let language = args.get("language").and_then(Value::as_str);
            let max_results = args
                .get("max_results")
                .and_then(Value::as_u64)
                .map(|value| value as usize);
            handle_ast_grep(ws, pattern, path, language, max_results)
        }
        "lsp_diagnostics" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let timeout = args.get("timeout_ms").and_then(Value::as_u64);
            handle_lsp(ws, LspOperation::Diagnostics, path, None, None, timeout)
        }
        "lsp_definition" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let line = args.get("line").and_then(Value::as_u64).map(|v| v as usize);
            let character = args
                .get("character")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            let timeout = args.get("timeout_ms").and_then(Value::as_u64);
            handle_lsp(ws, LspOperation::Definition, path, line, character, timeout)
        }
        "lsp_references" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let line = args.get("line").and_then(Value::as_u64).map(|v| v as usize);
            let character = args
                .get("character")
                .and_then(Value::as_u64)
                .map(|v| v as usize);
            let timeout = args.get("timeout_ms").and_then(Value::as_u64);
            handle_lsp(ws, LspOperation::References, path, line, character, timeout)
        }
        "lsp_symbols" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let timeout = args.get("timeout_ms").and_then(Value::as_u64);
            handle_lsp(ws, LspOperation::Symbols, path, None, None, timeout)
        }
        "patch_file" => {
            let path = args.get("path").and_then(Value::as_str).unwrap_or("");
            let sha = args.get("expected_sha256").and_then(Value::as_str);
            let content = args.get("content").and_then(Value::as_str).unwrap_or("");
            let result = handle_patch_file(ws, path, sha, content);
            if result.success {
                record_mutation(ws, scope_id, path);
            }
            result
        }
        "run_command" => {
            let cmd = args.get("command").and_then(Value::as_str).unwrap_or("");
            let result = handle_run_command(ws, cmd, cli.command_timeout_ms, cli.allow_host_command_execution);
            if result.success {
                if let Some(data) = result.data.as_ref() {
                    let success = data
                        .get("success")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    let exit_code = data.get("exit_code").and_then(Value::as_i64);
                    let duration_ms = data.get("duration_ms").and_then(Value::as_u64).unwrap_or(0);
                    record_verification(ws, scope_id, cmd, success, exit_code, duration_ms);
                }
            }
            result
        }
        "git_status_diff" => handle_git_status(ws),
        "task_plan" => {
            let goal = args.get("goal").and_then(Value::as_str).unwrap_or("");
            let Some(raw_items) = args.get("items").and_then(Value::as_array) else {
                return ToolCallResult::err("items must be an array of strings");
            };
            let mut items = Vec::with_capacity(raw_items.len());
            for item in raw_items {
                let Some(item) = item.as_str() else {
                    return ToolCallResult::err("items must contain only strings");
                };
                items.push(item.to_string());
            }
            handle_task_plan(ws, scope_id, goal, items)
        }
        "task_update" => {
            let item_id = args.get("item_id").and_then(Value::as_str).unwrap_or("");
            let status = args.get("status").and_then(Value::as_str).unwrap_or("");
            let note = args.get("note").and_then(Value::as_str);
            handle_task_update(ws, scope_id, item_id, status, note)
        }
        "task_state" => handle_task_state(ws, scope_id),
        "completion_check" => {
            let require_task_plan = args.get("require_task_plan").and_then(Value::as_bool);
            let require_verification = args.get("require_verification").and_then(Value::as_bool);
            let require_changes = args.get("require_changes").and_then(Value::as_bool);
            handle_completion_check(
                ws,
                scope_id,
                require_task_plan,
                require_verification,
                require_changes,
            )
        }
        _ => ToolCallResult::err(format!("Unknown tool: {}", name)),
    }
}

impl IntoResponse for BridgeError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            BridgeError::Security(msg) => (StatusCode::UNAUTHORIZED, msg),
            _ => (StatusCode::BAD_REQUEST, self.to_string()),
        };
        (status, Json(serde_json::json!({ "error": msg }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_advertises_single_agent_coding_instructions() {
        let init = initialize_result();
        let instructions = init["instructions"].as_str().unwrap();
        assert!(instructions.contains("directly responsible for coding"));
        assert!(instructions.contains("completion_check"));
        assert_eq!(init["serverInfo"]["version"], "0.7.0");
    }

    #[test]
    fn tools_list_contains_harness_tools_and_original_tools() {
        let tools = tool_definitions();
        let names: Vec<&str> = tools
            .iter()
            .filter_map(|tool| tool["name"].as_str())
            .collect();
        for required in [
            "read_file",
            "patch_file",
            "list_files",
            "run_command",
            "git_status_diff",
            "search_text",
            "ast_grep",
            "lsp_diagnostics",
            "lsp_definition",
            "lsp_references",
            "lsp_symbols",
            "task_plan",
            "task_update",
            "task_state",
            "completion_check",
        ] {
            assert!(names.contains(&required), "missing tool: {}", required);
        }
    }

    #[test]
    fn patch_event_metadata_does_not_include_file_content() {
        let metadata = tool_event_metadata(
            "patch_file",
            &serde_json::json!({
                "path": "src/lib.rs",
                "expected_sha256": "abc",
                "content": "super secret source"
            }),
        );
        let serialized = metadata.to_string();
        assert!(!serialized.contains("super secret source"));
        assert_eq!(metadata["path"], "src/lib.rs");
        assert_eq!(metadata["content_bytes"], 19);
    }

    #[tokio::test]
    async fn incomplete_completion_publishes_continuation_prompt() {
        let events = EventBus::new("workspace");
        let mut rx = events.subscribe();
        let result = ToolCallResult::ok(serde_json::json!({
            "ready": false,
            "blockers": ["tests are failing", "T2 is pending"],
            "verification_evidence": null
        }));

        publish_specialized_events(
            &events,
            "33333333-3333-4333-8333-333333333333",
            "completion_check",
            &serde_json::json!({}),
            &result,
        );

        let completion = rx.recv().await.unwrap();
        let continuation = rx.recv().await.unwrap();
        assert_eq!(completion.kind, "completion");
        assert_eq!(continuation.kind, "continuation_required");
        assert_eq!(continuation.data["relay_to_same_chat"], true);
        assert_eq!(
            continuation.data["scope_id"],
            "33333333-3333-4333-8333-333333333333"
        );
        let prompt = continuation.data["prompt"].as_str().unwrap();
        assert!(prompt.contains("tests are failing"));
        assert!(prompt.contains("completion_check returns ready=true"));
    }
}
