use axum::body::{to_bytes, Body};
use gpt2omo::tools::task_state::{
    clear_delegation_lifecycle, load_delegation_lifecycle, DelegationTerminalState,
};
use gpt2omo::{create_router, AppState, Cli, EventBus, WorkspaceMux};
use http::{Request, StatusCode};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

fn test_app(dir: &TempDir) -> (axum::Router, Arc<EventBus>, WorkspaceMux, String, TempDir) {
    // The scope control dir must live OUTSIDE the mount root, but a plain sibling of the TempDir
    // is never reclaimed and leaks a *.scopes directory on every run (TEST-1). A second TempDir
    // keeps it off the mount root AND under RAII cleanup.
    let scope_home = tempfile::tempdir().unwrap();
    let mount_root = dir.path().to_path_buf();
    let scope_dir = scope_home.path().join("scopes");
    let mux = WorkspaceMux::new(&mount_root, &scope_dir).unwrap();
    let scope = mux.register(&mount_root, Some("term-test".into())).unwrap();
    let scope = mux.refresh_capability_secret(&scope.scope_id).unwrap();
    let cli = Cli {
        mount_root: mount_root.clone(),
        scope_dir: Some(scope_dir),
        bind: "127.0.0.1:0".into(),
        token: None,
        token_file: None,
        insecure_no_auth: true,
        max_file_bytes: 10 * 1024 * 1024,
        command_timeout_ms: 5_000,
        subagent_endpoint: None,
        subagent_api_key: None,
        subagent_model: "deepseek-v4-flash-free".into(),
        subagent_allow_remote: false,
        allow_arbitrary_commands: false,
        read_only: false,
    };
    let events = Arc::new(EventBus::new(dir.path().to_string_lossy().to_string()));
    let app = create_router(AppState {
        workspace: Arc::new(mux.clone()),
        cli: Arc::new(cli),
        events: events.clone(),
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    });
    (app, events, mux, scope.scope_id, scope_home)
}

fn app_for_mux(mount: &TempDir, scope_dir: std::path::PathBuf, mux: &WorkspaceMux) -> axum::Router {
    let cli = Cli {
        mount_root: mount.path().join("mount-root"),
        scope_dir: Some(scope_dir),
        bind: "127.0.0.1:0".into(),
        token: None,
        token_file: None,
        insecure_no_auth: true,
        max_file_bytes: 10 * 1024 * 1024,
        command_timeout_ms: 5_000,
        subagent_endpoint: None,
        subagent_api_key: None,
        subagent_model: "deepseek-v4-flash-free".into(),
        subagent_allow_remote: false,
        allow_arbitrary_commands: false,
        read_only: false,
    };
    let events = Arc::new(EventBus::new(mount.path().to_string_lossy().to_string()));
    create_router(AppState {
        workspace: Arc::new(mux.clone()),
        cli: Arc::new(cli),
        events,
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    })
}

async fn rpc(app: axum::Router, payload: Value) -> Value {
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn rpc_with_bearer(app: axum::Router, payload: Value, token: &str) -> Value {
    let request = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(payload.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn scope_capability_secret(mux: &WorkspaceMux, scope_id: &str) -> String {
    mux.lookup(scope_id)
        .unwrap()
        .capability_secret
        .expect("test scope must carry capability_secret")
}

fn nested_tool_result(response: &Value) -> Value {
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn transport_bearer_and_scope_capability_are_both_enforced() {
    let mount = tempfile::tempdir().unwrap();
    let project = mount.path().join("mount-root").join("project");
    std::fs::create_dir_all(&project).unwrap();
    let mount_root = mount.path().join("mount-root");
    std::fs::create_dir_all(&mount_root).unwrap();
    let scope_dir = mount.path().join("scopes");
    let mux = WorkspaceMux::new(&mount_root, &scope_dir).unwrap();
    let scope = mux.register_browser(&project, "page-one".into()).unwrap();
    let scope = mux.refresh_capability_secret(&scope.scope_id).unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();
    clear_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    let cli = Cli {
        mount_root: mount.path().join("mount-root"),
        scope_dir: Some(scope_dir),
        bind: "127.0.0.1:0".into(),
        token: Some("relay-control-token".into()),
        token_file: None,
        insecure_no_auth: false,
        max_file_bytes: 10 * 1024 * 1024,
        command_timeout_ms: 5_000,
        subagent_endpoint: None,
        subagent_api_key: None,
        subagent_model: "deepseek-v4-flash-free".into(),
        subagent_allow_remote: false,
        allow_arbitrary_commands: false,
        read_only: false,
    };
    let app = create_router(AppState {
        workspace: Arc::new(mux),
        cli: Arc::new(cli),
        events: Arc::new(EventBus::new(mount.path().to_string_lossy().to_string())),
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    });

    let unauthenticated = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "jsonrpc": "2.0",
                "id": 699,
                "method": "tools/call",
                "params": {
                    "name": "task_state",
                    "arguments": {"scope_id": scope.scope_id}
                }
            })
            .to_string(),
        ))
        .unwrap();
    let unauthenticated = app.clone().oneshot(unauthenticated).await.unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let valid = rpc_with_bearer(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 700,
            "method": "tools/call",
            "params": {
                "name": "task_state",
                "arguments": {"scope_id": scope.scope_id}
            }
        }),
        "relay-control-token",
    )
    .await;
    assert_eq!(valid["result"]["isError"], false);

    let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
        .unwrap()
        .expect("a valid bearer plus scope capability must record readiness");
    assert!(lifecycle.ready_ms.is_some());

    let invalid = rpc_with_bearer(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 701,
            "method": "tools/call",
            "params": {
                "name": "task_state",
                "arguments": {"scope_id": "77777777-7777-4777-8777-777777777777"}
            }
        }),
        "relay-control-token",
    )
    .await;
    assert_eq!(invalid["result"]["isError"], true);
    let failure = nested_tool_result(&invalid);
    assert!(failure["error"]
        .as_str()
        .unwrap()
        .contains("Unknown or expired workspace scope"));
}

#[tokio::test]
async fn initialize_and_tools_list_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _, _, _, _scope_home) = test_app(&dir);

    let init = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        }),
    )
    .await;
    assert_eq!(init["result"]["serverInfo"]["version"], "0.7.0");
    let instructions = init["result"]["instructions"].as_str().unwrap();
    assert!(instructions.contains("scope_id"));
    assert!(instructions.contains("detached_running"));
    assert!(instructions.contains("stale_revision"));

    let tools = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    )
    .await;
    let tools = tools["result"]["tools"].as_array().unwrap();
    assert_eq!(
        tools.len(),
        18,
        "the MCP schema must expose 18 standard tools when subagent support is disabled"
    );
    let names: Vec<&str> = tools
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect();
    for required in [
        "search_text",
        "ast_grep",
        "lsp_diagnostics",
        "lsp_definition",
        "lsp_references",
        "lsp_symbols",
        "run_command",
        "poll_command",
        "list_commands",
        "cancel_command",
        "task_plan",
        "completion_check",
    ] {
        assert!(names.contains(&required), "missing tool: {}", required);
    }
    assert!(!names.contains(&"query_subagent"));
    for tool in tools {
        assert!(tool["inputSchema"]["properties"]["scope_id"].is_object());
        assert!(tool["inputSchema"]["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str() == Some("scope_id")));
        let name = tool["name"].as_str().unwrap();
        let mutating = matches!(
            name,
            "patch_file"
                | "run_command"
                | "cancel_command"
                | "task_plan"
                | "task_update"
                | "completion_check"
        );
        assert_eq!(
            tool["inputSchema"]["properties"]
                .get("capability_secret")
                .is_some(),
            mutating,
            "unexpected capability_secret schema for {name}"
        );
    }
}

#[tokio::test]
async fn actual_mcp_one_worker_readiness_smoke() {
    let mount = tempfile::tempdir().unwrap();
    let project = mount.path().join("mount-root").join("project");
    std::fs::create_dir_all(&project).unwrap();
    let mount_root = mount.path().join("mount-root");
    std::fs::create_dir_all(&mount_root).unwrap();
    let scope_dir = mount.path().join("scopes");
    let mux = WorkspaceMux::new(&mount_root, &scope_dir).unwrap();
    let scope = mux.register_browser(&project, "page-one".into()).unwrap();
    let scope = mux.refresh_capability_secret(&scope.scope_id).unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();
    clear_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    let app = app_for_mux(&mount, scope_dir, &mux);

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 200,
            "method": "tools/call",
            "params": {
                "name": "task_state",
                "arguments": {"scope_id": scope.scope_id}
            }
        }),
    )
    .await;
    assert_eq!(response["result"]["isError"], false);

    let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
        .unwrap()
        .expect("successful MCP task_state must record readiness");
    assert!(lifecycle.ready_ms.is_some());
    assert!(lifecycle.ready_ms.unwrap() >= scope.created_ms);
    assert!(lifecycle.terminal_state.is_none());
}

#[tokio::test]
async fn actual_mcp_three_worker_readiness_smoke() {
    let mount = tempfile::tempdir().unwrap();
    let project = mount.path().join("mount-root").join("project");
    std::fs::create_dir_all(&project).unwrap();
    let mount_root = mount.path().join("mount-root");
    std::fs::create_dir_all(&mount_root).unwrap();
    let scope_dir = mount.path().join("scopes");
    let mux = WorkspaceMux::new(&mount_root, &scope_dir).unwrap();
    let scopes = (1..=3)
        .map(|index| {
            let scope = mux
                .register_browser(&project, format!("page-{index}"))
                .unwrap();
            mux.refresh_capability_secret(&scope.scope_id).unwrap()
        })
        .collect::<Vec<_>>();
    for scope in &scopes {
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        clear_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    }
    let app = app_for_mux(&mount, scope_dir, &mux);

    for (index, scope) in scopes.iter().enumerate() {
        let response = rpc(
            app.clone(),
            json!({
                "jsonrpc": "2.0",
                "id": 210 + index,
                "method": "tools/call",
                "params": {
                    "name": "task_state",
                    "arguments": {"scope_id": scope.scope_id}
                }
            }),
        )
        .await;
        assert_eq!(response["result"]["isError"], false);
    }

    for (index, scope) in scopes.iter().enumerate() {
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .expect("each successful MCP task_state must record readiness");
        assert!(lifecycle.ready_ms.is_some());
        assert_eq!(
            mux.lookup(&scope.scope_id)
                .unwrap()
                .browser_page_id
                .as_deref(),
            Some(format!("page-{}", index + 1).as_str())
        );
    }
}

#[tokio::test]
async fn tools_call_dispatches_search_text_smoke() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("sample.rs"),
        "fn alpha() {}\nfn important_symbol() {}\n",
    )
    .unwrap();
    let (app, _, _, scope_id, _scope_home) = test_app(&dir);

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "search_text",
                "arguments": {
                    "scope_id": scope_id,
                    "query": "important_symbol",
                    "path": "."
                }
            }
        }),
    )
    .await;

    assert_eq!(response["result"]["isError"], false);
    let nested = nested_tool_result(&response);
    assert_eq!(nested["success"], true);
    assert_eq!(nested["data"]["match_count"], 1);
    assert_eq!(nested["data"]["matches"][0]["path"], "sample.rs");
}

#[tokio::test]
async fn command_endpoints_share_daemon_state_and_idempotency() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _, mux, scope_id, _scope_home) = test_app(&dir);
    let capability_secret = scope_capability_secret(&mux, &scope_id);

    let first = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 300,
            "method": "tools/call",
            "params": {
                "name": "run_command",
                "arguments": {
                    "scope_id": scope_id,
                    "capability_secret": capability_secret.as_str(),
                    "command": "git --version",
                    "client_request_id": "quick-sync-1"
                }
            }
        }),
    )
    .await;
    assert_eq!(first["result"]["isError"], false);
    let first_nested = nested_tool_result(&first);
    assert_eq!(first_nested["data"]["status"], "completed");
    assert_eq!(first_nested["data"]["command_success"], true);
    let command_id = first_nested["data"]["command_id"]
        .as_str()
        .unwrap()
        .to_string();

    let retry = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 301,
            "method": "tools/call",
            "params": {
                "name": "run_command",
                "arguments": {
                    "scope_id": scope_id,
                    "capability_secret": capability_secret.as_str(),
                    "command": "git --version",
                    "client_request_id": "quick-sync-1"
                }
            }
        }),
    )
    .await;
    let retry_nested = nested_tool_result(&retry);
    assert_eq!(retry_nested["data"]["command_id"], command_id);
    assert_eq!(retry_nested["data"]["idempotent_replay"], true);

    let polled = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 302,
            "method": "tools/call",
            "params": {
                "name": "poll_command",
                "arguments": {
                    "scope_id": scope_id,
                    "command_id": command_id,
                    "wait_timeout_ms": 15000
                }
            }
        }),
    )
    .await;
    let polled_nested = nested_tool_result(&polled);
    assert_eq!(polled_nested["data"]["status"], "completed");
    assert_eq!(polled_nested["data"]["command_success"], true);

    let listed = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 303,
            "method": "tools/call",
            "params": {
                "name": "list_commands",
                "arguments": {"scope_id": scope_id}
            }
        }),
    )
    .await;
    let listed_nested = nested_tool_result(&listed);
    assert_eq!(listed_nested["data"]["active_count"], 0);
    assert!(listed_nested["data"]["commands"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry["command_id"] == command_id));
}

#[tokio::test]
async fn events_endpoint_is_sse() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _, _, _, _scope_home) = test_app(&dir);
    let request = Request::builder()
        .method("GET")
        .uri("/events")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
}

#[tokio::test]
async fn tool_calls_publish_started_and_finished_events_with_scope() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sample.txt"), "needle\n").unwrap();
    let (app, events, _, scope_id, _scope_home) = test_app(&dir);
    let mut receiver = events.subscribe();

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "search_text",
                "arguments": {"scope_id": scope_id, "query": "needle"}
            }
        }),
    )
    .await;
    assert_eq!(response["result"]["isError"], false);

    let started = receiver.recv().await.unwrap();
    let finished = receiver.recv().await.unwrap();
    assert_eq!(started.kind, "tool_started");
    assert_eq!(started.data["tool"], "search_text");
    assert_eq!(started.data["scope_id"], scope_id);
    assert_eq!(finished.kind, "tool_finished");
    assert_eq!(finished.data["scope_id"], scope_id);
    assert!(finished.seq > started.seq);
}

#[tokio::test]
async fn tool_calls_require_scope_id() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _, _, _, _scope_home) = test_app(&dir);

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {
                "name": "read_file",
                "arguments": {"path": "Cargo.toml"}
            }
        }),
    )
    .await;

    assert_eq!(response["result"]["isError"], true);
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("scope_id is required"));
}

#[tokio::test]
async fn two_scopes_access_separate_workspaces_without_global_switch() {
    let mount = tempfile::tempdir().unwrap();
    let first = mount.path().join("mount-root").join("first");
    let second = mount.path().join("mount-root").join("second");
    std::fs::create_dir_all(&first).unwrap();
    std::fs::create_dir_all(&second).unwrap();
    std::fs::write(first.join("only-first.txt"), "first").unwrap();
    std::fs::write(second.join("only-second.txt"), "second").unwrap();

    let mount_root = mount.path().join("mount-root");
    std::fs::create_dir_all(&mount_root).unwrap();
    let scope_dir = mount.path().join("scopes");
    let mux = WorkspaceMux::new(&mount_root, &scope_dir).unwrap();
    let scope_a = mux.register(&first, Some("term-a".into())).unwrap();
    let scope_b = mux.register(&second, Some("term-b".into())).unwrap();
    let cli = Cli {
        mount_root: mount.path().join("mount-root"),
        scope_dir: Some(scope_dir),
        bind: "127.0.0.1:0".into(),
        token: None,
        token_file: None,
        insecure_no_auth: true,
        max_file_bytes: 1024,
        command_timeout_ms: 5_000,
        subagent_endpoint: None,
        subagent_api_key: None,
        subagent_model: "deepseek-v4-flash-free".into(),
        subagent_allow_remote: false,
        allow_arbitrary_commands: false,
        read_only: false,
    };
    let events = Arc::new(EventBus::new(mount.path().to_string_lossy().to_string()));
    let app = create_router(AppState {
        workspace: Arc::new(mux),
        cli: Arc::new(cli),
        events,
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    });

    let first_read = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 100,
            "method": "tools/call",
            "params": {
                "name": "read_file",
                "arguments": {"scope_id": scope_a.scope_id, "path": "only-first.txt"}
            }
        }),
    )
    .await;
    assert_eq!(first_read["result"]["isError"], false);

    let second_read = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 101,
            "method": "tools/call",
            "params": {
                "name": "read_file",
                "arguments": {"scope_id": scope_b.scope_id, "path": "only-second.txt"}
            }
        }),
    )
    .await;
    assert_eq!(second_read["result"]["isError"], false);

    let crossed = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 102,
            "method": "tools/call",
            "params": {
                "name": "read_file",
                "arguments": {"scope_id": scope_b.scope_id, "path": "only-first.txt"}
            }
        }),
    )
    .await;
    assert_eq!(crossed["result"]["isError"], true);
}

#[tokio::test]
async fn verification_completion_and_continuation_events_keep_scope() {
    let dir = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let (app, events, mux, scope_id, _scope_home) = test_app(&dir);
    let capability_secret = scope_capability_secret(&mux, &scope_id);
    let mut receiver = events.subscribe();

    rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "run_command",
                "arguments": {"scope_id": scope_id, "capability_secret": capability_secret.as_str(), "command": "cargo fmt --check"}
            }
        }),
    )
    .await;

    let mut verification = None;
    for _ in 0..3 {
        let event = receiver.recv().await.unwrap();
        if event.kind == "verification" {
            verification = Some(event);
            break;
        }
    }
    let verification = verification.expect("verification event was not published");
    assert_eq!(verification.data["scope_id"], scope_id);

    rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "completion_check",
                "arguments": {
                    "scope_id": scope_id,
                    "capability_secret": capability_secret.as_str(),
                    "require_task_plan": false,
                    "require_verification": false,
                    "require_changes": false,
                    "result": {
                        "summary": "Verified completion event flow",
                        "changed_files": [],
                        "verification": ["cargo fmt --check: passed"],
                        "blockers": [],
                        "final_message": "Completion event flow verified."
                    }
                }
            }
        }),
    )
    .await;

    let mut completion = None;
    for _ in 0..3 {
        let event = receiver.recv().await.unwrap();
        if event.kind == "completion" {
            completion = Some(event);
            break;
        }
    }
    let completion = completion.expect("completion event was not published");
    assert_eq!(completion.data["scope_id"], scope_id);
    assert_eq!(completion.data["ready"], true);
    assert_eq!(
        completion.data["task_result"]["summary"],
        "Verified completion event flow"
    );

    let workspace = dir.path();
    let completed_lifecycle =
        load_delegation_lifecycle(&gpt2omo::Workspace::open(workspace).unwrap(), &scope_id)
            .unwrap()
            .expect("ready completion must leave terminal evidence");
    assert_eq!(
        completed_lifecycle.terminal_state,
        Some(DelegationTerminalState::Completed)
    );

    rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "completion_check",
                "arguments": {
                    "scope_id": scope_id,
                    "capability_secret": capability_secret.as_str(),
                    "require_task_plan": true,
                    "require_verification": false,
                    "require_changes": false
                }
            }
        }),
    )
    .await;

    let mut continuation = None;
    for _ in 0..4 {
        let event = receiver.recv().await.unwrap();
        if event.kind == "continuation_required" {
            continuation = Some(event);
            break;
        }
    }
    let continuation = continuation.expect("continuation event was not published");
    assert_eq!(continuation.data["scope_id"], scope_id);
    assert!(continuation.data["prompt"]
        .as_str()
        .unwrap()
        .contains(&scope_id));
}

#[tokio::test]
async fn completion_check_accepts_result_json_string_fallback_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _, mux, scope_id, _scope_home) = test_app(&dir);
    let capability_secret = scope_capability_secret(&mux, &scope_id);

    // Start a delegation lifecycle so a structured result can be recorded.
    let readiness = rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 400,
            "method": "tools/call",
            "params": {
                "name": "task_state",
                "arguments": {"scope_id": scope_id}
            }
        }),
    )
    .await;
    assert_eq!(readiness["result"]["isError"], false);

    let result_json = r#"{"summary":"Flat string transport","changed_files":["src/lib.rs"],"verification":["cargo test"],"blockers":[],"final_message":"All green."}"#;
    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 401,
            "method": "tools/call",
            "params": {
                "name": "completion_check",
                "arguments": {
                    "scope_id": scope_id,
                    "capability_secret": capability_secret.as_str(),
                    "require_task_plan": false,
                    "require_verification": false,
                    "result_json": result_json
                }
            }
        }),
    )
    .await;

    assert_eq!(response["result"]["isError"], false);
    let nested = nested_tool_result(&response);
    assert_eq!(
        nested["success"], true,
        "unexpected error: {:?}",
        nested["error"]
    );
    let data = &nested["data"];
    assert_eq!(data["task_result"]["summary"], "Flat string transport");
    let blockers = data["blockers"].as_array().unwrap();
    assert!(
        !blockers
            .iter()
            .filter_map(Value::as_str)
            .any(|blocker| blocker.contains("No structured task_result")),
        "result_json must satisfy the missing-task_result audit: {blockers:?}"
    );
}
