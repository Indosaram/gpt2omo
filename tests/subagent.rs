use axum::body::{to_bytes, Body};
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use gpt2omo::{create_router, AppState, Cli, EventBus, WorkspaceMux};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tower::ServiceExt;

#[derive(Clone, Copy)]
enum MockMode {
    Success,
    Error,
    Oversized,
}

#[derive(Clone)]
struct MockState {
    mode: MockMode,
    requests: Arc<AtomicUsize>,
    last_body: Arc<Mutex<Option<Value>>>,
    last_authorization: Arc<Mutex<Option<String>>>,
}

async fn mock_completion(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    state.requests.fetch_add(1, Ordering::SeqCst);
    *state.last_body.lock().unwrap() = Some(body);
    *state.last_authorization.lock().unwrap() = headers
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .map(str::to_string);

    match state.mode {
        MockMode::Success => Json(json!({
            "id": "mock-completion",
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "Use a generation-keyed atomic counter and keep the advice non-authoritative."
                }
            }],
            "usage": {
                "prompt_tokens": 11,
                "completion_tokens": 13,
                "total_tokens": 24
            }
        }))
        .into_response(),
        MockMode::Error => (
            StatusCode::BAD_GATEWAY,
            "SECRET_UPSTREAM_BODY internal-header=X-Oracle-Secret",
        )
            .into_response(),
        MockMode::Oversized => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Body::from(vec![b'x'; 256 * 1024 + 1]))
            .unwrap(),
    }
}

async fn spawn_mock(mode: MockMode) -> (String, MockState) {
    let state = MockState {
        mode,
        requests: Arc::new(AtomicUsize::new(0)),
        last_body: Arc::new(Mutex::new(None)),
        last_authorization: Arc::new(Mutex::new(None)),
    };
    let app = Router::new()
        .route("/v1/chat/completions", post(mock_completion))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), state)
}

fn cli_for(dir: &TempDir, scope_dir: std::path::PathBuf, endpoint: Option<String>) -> Cli {
    Cli {
        mount_root: dir.path().to_path_buf(),
        scope_dir: Some(scope_dir),
        bind: "127.0.0.1:0".into(),
        token: None,
        token_file: None,
        max_file_bytes: 10 * 1024 * 1024,
        command_timeout_ms: 5_000,
        subagent_endpoint: endpoint,
        subagent_api_key: Some("test-secret-key".into()),
        subagent_model: "mock-model".into(),
        subagent_allow_remote: false,
        insecure_no_auth: true,
        allow_arbitrary_commands: false,
    }
}

fn test_app(dir: &TempDir, endpoint: Option<String>) -> (axum::Router, WorkspaceMux, String) {
    let scope_dir = dir.path().join("scopes");
    let mux = WorkspaceMux::new(dir.path(), &scope_dir).unwrap();
    let scope = mux
        .register(dir.path(), Some("subagent-test".into()))
        .unwrap();
    let cli = cli_for(dir, scope_dir, endpoint);
    let events = Arc::new(EventBus::new(dir.path().to_string_lossy().to_string()));
    let app = create_router(AppState {
        workspace: Arc::new(mux.clone()),
        cli: Arc::new(cli),
        events,
        commands: Arc::new(gpt2omo::tools::CommandManager::new()),
    });
    (app, mux, scope.scope_id)
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
    let bytes = to_bytes(response.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn nested_tool_result(response: &Value) -> Value {
    let text = response["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

async fn establish_lifecycle(app: axum::Router, scope_id: &str) {
    let response = rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": "ready",
            "method": "tools/call",
            "params": {
                "name": "task_state",
                "arguments": {"scope_id": scope_id}
            }
        }),
    )
    .await;
    assert_eq!(response["result"]["isError"], false);
}

async fn query(app: axum::Router, scope_id: &str, prompt: Value, timeout: Option<Value>) -> Value {
    let mut arguments = json!({
        "scope_id": scope_id,
        "prompt": prompt,
    });
    if let Some(timeout) = timeout {
        arguments["timeout_ms"] = timeout;
    }
    rpc(
        app,
        json!({
            "jsonrpc": "2.0",
            "id": "query",
            "method": "tools/call",
            "params": {
                "name": "query_subagent",
                "arguments": arguments
            }
        }),
    )
    .await
}

#[tokio::test]
async fn discovery_is_disabled_without_endpoint_and_enabled_with_endpoint() {
    let disabled_dir = tempfile::tempdir().unwrap();
    let (disabled_app, _, _) = test_app(&disabled_dir, None);
    let disabled = rpc(
        disabled_app,
        json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}),
    )
    .await;
    let disabled_names = disabled["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool["name"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(disabled_names.len(), 18);
    assert!(!disabled_names.contains(&"query_subagent"));

    let (endpoint, _) = spawn_mock(MockMode::Success).await;
    let enabled_dir = tempfile::tempdir().unwrap();
    let (enabled_app, _, _) = test_app(&enabled_dir, Some(endpoint));
    let enabled = rpc(
        enabled_app,
        json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}),
    )
    .await;
    let enabled_tools = enabled["result"]["tools"].as_array().unwrap();
    assert_eq!(enabled_tools.len(), 19);
    let subagent = enabled_tools
        .iter()
        .find(|tool| tool["name"] == "query_subagent")
        .unwrap();
    assert!(subagent["inputSchema"]["properties"]["scope_id"].is_object());
    assert!(subagent["inputSchema"]["properties"]["prompt"].is_object());
}

#[tokio::test]
async fn successful_query_parses_advice_usage_latency_and_sends_model_and_auth() {
    let (endpoint, mock) = spawn_mock(MockMode::Success).await;
    let dir = tempfile::tempdir().unwrap();
    let (app, _, scope_id) = test_app(&dir, Some(endpoint));
    establish_lifecycle(app.clone(), &scope_id).await;

    let response = query(
        app,
        &scope_id,
        json!("Review this quota design."),
        Some(json!(10)),
    )
    .await;
    assert_eq!(response["result"]["isError"], false);
    let nested = nested_tool_result(&response);
    assert_eq!(nested["success"], true);
    assert_eq!(nested["data"]["trust"], "untrusted_advisory");
    assert_eq!(nested["data"]["usage"]["prompt_tokens"], 11);
    assert_eq!(nested["data"]["usage"]["completion_tokens"], 13);
    assert_eq!(nested["data"]["usage"]["total_tokens"], 24);
    assert!(nested["data"]["latency_ms"].as_u64().is_some());
    assert_eq!(nested["data"]["quota_call"], 1);
    assert_eq!(nested["data"]["quota_limit"], 4);

    let body = mock.last_body.lock().unwrap().clone().unwrap();
    assert_eq!(body["model"], "mock-model");
    assert_eq!(body["messages"][0]["role"], "user");
    assert_eq!(body["messages"][0]["content"], "Review this quota design.");
    assert_eq!(
        mock.last_authorization.lock().unwrap().as_deref(),
        Some("Bearer test-secret-key")
    );
}

#[tokio::test]
async fn active_lifecycle_and_input_bounds_are_enforced_before_network_or_quota() {
    let (endpoint, mock) = spawn_mock(MockMode::Success).await;
    let dir = tempfile::tempdir().unwrap();
    let (app, _, scope_id) = test_app(&dir, Some(endpoint));

    let no_lifecycle = query(app.clone(), &scope_id, json!("hello"), None).await;
    let nested = nested_tool_result(&no_lifecycle);
    assert_eq!(nested["success"], false);
    assert!(nested["error"]
        .as_str()
        .unwrap()
        .contains("active delegation lifecycle"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 0);

    establish_lifecycle(app.clone(), &scope_id).await;
    let oversized = "x".repeat(32 * 1024 + 1);
    let too_large = query(app.clone(), &scope_id, json!(oversized), None).await;
    let nested = nested_tool_result(&too_large);
    assert_eq!(nested["success"], false);
    assert!(nested["error"].as_str().unwrap().contains("32768-byte"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 0);

    let bad_timeout = query(app.clone(), &scope_id, json!("hello"), Some(json!(1.5))).await;
    let nested = nested_tool_result(&bad_timeout);
    assert_eq!(nested["success"], false);
    assert!(nested["error"]
        .as_str()
        .unwrap()
        .contains("timeout_ms must be an integer"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 0);

    let valid = query(app, &scope_id, json!("hello"), Some(json!(0))).await;
    let nested = nested_tool_result(&valid);
    assert_eq!(nested["success"], true);
    assert_eq!(nested["data"]["quota_call"], 1);
    assert_eq!(mock.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn quota_allows_four_calls_per_generation_and_rejects_the_fifth() {
    let (endpoint, mock) = spawn_mock(MockMode::Success).await;
    let dir = tempfile::tempdir().unwrap();
    let (app, _, scope_id) = test_app(&dir, Some(endpoint));
    establish_lifecycle(app.clone(), &scope_id).await;

    for expected in 1..=4 {
        let response = query(
            app.clone(),
            &scope_id,
            json!(format!("call {expected}")),
            None,
        )
        .await;
        let nested = nested_tool_result(&response);
        assert_eq!(nested["success"], true);
        assert_eq!(nested["data"]["quota_call"], expected);
    }
    let fifth = query(app, &scope_id, json!("call five"), None).await;
    let nested = nested_tool_result(&fifth);
    assert_eq!(nested["success"], false);
    assert!(nested["error"].as_str().unwrap().contains("quota exceeded"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn oversized_raw_response_is_rejected_before_json_parsing() {
    let (endpoint, mock) = spawn_mock(MockMode::Oversized).await;
    let dir = tempfile::tempdir().unwrap();
    let (app, _, scope_id) = test_app(&dir, Some(endpoint));
    establish_lifecycle(app.clone(), &scope_id).await;

    let response = query(app, &scope_id, json!("large response please"), None).await;
    let nested = nested_tool_result(&response);
    assert_eq!(nested["success"], false);
    assert!(nested["error"]
        .as_str()
        .unwrap()
        .contains("262144-byte limit"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn upstream_http_errors_are_sanitized() {
    let (endpoint, mock) = spawn_mock(MockMode::Error).await;
    let dir = tempfile::tempdir().unwrap();
    let (app, _, scope_id) = test_app(&dir, Some(endpoint));
    establish_lifecycle(app.clone(), &scope_id).await;

    let response = query(app, &scope_id, json!("cause upstream error"), None).await;
    let nested = nested_tool_result(&response);
    assert_eq!(nested["success"], false);
    let error = nested["error"].as_str().unwrap();
    assert_eq!(error, "Subagent upstream returned HTTP status 502");
    assert!(!error.contains("SECRET_UPSTREAM_BODY"));
    assert!(!error.contains("X-Oracle-Secret"));
    assert_eq!(mock.requests.load(Ordering::SeqCst), 1);
}
