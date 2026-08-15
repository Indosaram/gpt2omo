use axum::body::{to_bytes, Body};
use http::{Request, StatusCode};
use omo_bridge::{create_router, AppState, Cli, EventBus, Workspace};
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tower::ServiceExt;

fn test_app(dir: &TempDir) -> (axum::Router, Arc<EventBus>) {
    let workspace = Workspace::open(dir.path()).unwrap();
    let cli = Cli {
        workspace: dir.path().to_path_buf(),
        bind: "127.0.0.1:0".into(),
        token: None,
        max_file_bytes: 10 * 1024 * 1024,
        command_timeout_ms: 5_000,
    };
    let events = Arc::new(EventBus::new(dir.path().to_string_lossy().to_string()));
    let app = create_router(AppState {
        workspace: Arc::new(workspace),
        cli: Arc::new(cli),
        events: events.clone(),
    });
    (app, events)
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

#[tokio::test]
async fn initialize_and_tools_list_smoke() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _) = test_app(&dir);

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
    assert_eq!(init["result"]["serverInfo"]["version"], "0.5.0");
    assert!(init["result"]["instructions"]
        .as_str()
        .unwrap()
        .contains("directly responsible for coding"));

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
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
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
        "task_plan",
        "completion_check",
    ] {
        assert!(names.contains(&required), "missing tool: {}", required);
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
    let (app, _) = test_app(&dir);

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "search_text",
                "arguments": {
                    "query": "important_symbol",
                    "path": "."
                }
            }
        }),
    )
    .await;

    assert_eq!(response["result"]["isError"], false);
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    let nested: Value = serde_json::from_str(text).unwrap();
    assert_eq!(nested["success"], true);
    assert_eq!(nested["data"]["match_count"], 1);
    assert_eq!(nested["data"]["matches"][0]["path"], "sample.rs");
}

#[tokio::test]
async fn events_endpoint_is_sse() {
    let dir = tempfile::tempdir().unwrap();
    let (app, _) = test_app(&dir);
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
async fn tool_calls_publish_started_and_finished_events() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sample.txt"), "needle\n").unwrap();
    let (app, events) = test_app(&dir);
    let mut receiver = events.subscribe();

    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "search_text",
                "arguments": {"query": "needle"}
            }
        }),
    )
    .await;
    assert_eq!(response["result"]["isError"], false);

    let started = receiver.recv().await.unwrap();
    let finished = receiver.recv().await.unwrap();
    assert_eq!(started.kind, "tool_started");
    assert_eq!(started.data["tool"], "search_text");
    assert_eq!(finished.kind, "tool_finished");
    assert_eq!(finished.data["tool"], "search_text");
    assert_eq!(finished.data["success"], true);
    assert!(finished.seq > started.seq);
}

#[tokio::test]
async fn verification_and_completion_have_specialized_events() {
    let dir = tempfile::tempdir().unwrap();
    std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(dir.path())
        .status()
        .unwrap();
    let (app, events) = test_app(&dir);
    let mut receiver = events.subscribe();

    rpc(
        app.clone(),
        json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "run_command",
                "arguments": {"command": "cargo fmt --check"}
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
    assert_eq!(verification.data["command"], "cargo fmt --check");

    rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "completion_check",
                "arguments": {
                    "require_task_plan": false,
                    "require_verification": false,
                    "require_changes": false
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
    assert_eq!(completion.data["ready"], true);
}
