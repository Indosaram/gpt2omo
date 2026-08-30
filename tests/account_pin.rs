use axum::{routing::get, Json, Router};
use gpt2omo::{AccountRouter, LegacyAccountConfig, RouterError, WorkspaceMux};
use serde_json::json;
use std::fs;
use tokio::net::TcpListener;

fn isolated_roots() -> (
    tempfile::TempDir,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let root = tempfile::tempdir().unwrap();
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    let scopes = root.path().join("scopes");
    fs::create_dir_all(&bridge).unwrap();
    fs::create_dir_all(&mount).unwrap();
    fs::create_dir_all(&scopes).unwrap();
    (root, bridge, mount, scopes)
}

#[tokio::test]
async fn account_pin_targets_exact_account_or_errors_with_account_unavailable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/json/version",
                    get(|| async { Json(json!({"Browser": "Chrome"})) }),
                )
                .route("/json/list", get(|| async { Json(json!([])) })),
        )
        .await
        .unwrap();
    });

    let (_root, bridge, mount, scopes) = isolated_roots();
    fs::write(
        bridge.join("accounts.json"),
        format!(
            r#"{{
              "version": 1,
              "routing": {{
                "strategy": "round_robin",
                "reservation_ttl_seconds": 10,
                "selection_failure_backoff_seconds": 5
              }},
              "defaults": {{
                "limits": {{
                  "window_seconds": 60,
                  "max_dispatches": 10,
                  "max_active_workers": 2
                }}
              }},
              "accounts": [
                {{
                  "id": "first",
                  "browser": {{
                    "instance": "first-instance"
                  }}
                }},
                {{
                  "id": "second",
                  "browser": {{
                    "driver": "orca",
                    "instance": "second-instance",
                    "launch_mode": "attach_only",
                    "cdp_endpoint": "http://127.0.0.1:{}"
                  }}
                }}
              ]
            }}"#,
            address.port()
        ),
    )
    .unwrap();

    let router = AccountRouter::new(&bridge, &mount, LegacyAccountConfig::default());
    let mux = WorkspaceMux::new(&mount, &scopes).unwrap();

    // 1. reserve_batch_for_mux picks the first account (proves fixture would NOT pick the pinned one by default)
    let unpinned = router.reserve_batch_for_mux(&mux, 1, 1_000).unwrap();
    assert_eq!(unpinned.len(), 1);
    assert_eq!(unpinned[0].account.id, "first");
    router.release(&unpinned[0], 1_001).unwrap();

    // 2. pinned path returns the SECOND account for id "second"
    let pinned = router
        .reserve_for_account_for_mux(&mux, "second", 1_002)
        .unwrap();
    assert_eq!(pinned.account.id, "second");
    router.release(&pinned, 1_003).unwrap();

    // 3. unknown id errors with AccountUnavailable
    let error = router
        .reserve_for_account_for_mux(&mux, "unknown", 1_004)
        .unwrap_err();
    match error {
        RouterError::AccountUnavailable(account_id) => {
            assert_eq!(account_id, "unknown");
        }
        other => panic!("expected AccountUnavailable, got: {:?}", other),
    }

    server.abort();
}
