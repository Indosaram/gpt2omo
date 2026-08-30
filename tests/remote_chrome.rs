use axum::{routing::get, Json, Router};
use gpt2omo::accounts::parse_accounts_config;
use gpt2omo::orca::{BrowserDriverConfig, BrowserDriverKind};
use gpt2omo::{BrowserLaunchMode, BrowserPool, LegacyAccountConfig};
use serde_json::json;
use std::fs;
use tokio::net::TcpListener;

fn isolated_roots() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    fs::create_dir_all(&bridge).unwrap();
    fs::create_dir_all(&mount).unwrap();
    (root, bridge, mount)
}

#[test]
fn attach_only_configuration_accepts_loopback_forward_without_local_profile() {
    let (_root, bridge, mount) = isolated_roots();
    let config = parse_accounts_config(
        r#"{
          "version": 1,
          "accounts": [{
            "id": "remote-primary",
            "browser": {
              "driver": "orca",
              "instance": "remote-primary",
              "launch_mode": "attach_only",
              "cdp_endpoint": "http://127.0.0.1:19223"
            }
          }]
        }"#,
        &bridge,
        &mount,
    )
    .unwrap();

    assert_eq!(
        config.accounts[0].browser.launch_mode,
        BrowserLaunchMode::AttachOnly
    );
    assert!(config.accounts[0].browser.user_data_dir.is_none());
}

#[test]
fn attach_only_configuration_requires_a_loopback_cdp_endpoint() {
    let (_root, bridge, mount) = isolated_roots();
    let error = parse_accounts_config(
        r#"{
          "version": 1,
          "accounts": [{
            "id": "remote-primary",
            "browser": {
              "instance": "remote-primary",
              "launch_mode": "attach_only"
            }
          }]
        }"#,
        &bridge,
        &mount,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("attach_only requires browser.cdp_endpoint"));
}

#[test]
fn attach_only_configuration_rejects_a_websocket_discovery_endpoint() {
    let (_root, bridge, mount) = isolated_roots();
    let error = parse_accounts_config(
        r#"{
          "version": 1,
          "accounts": [{
            "id": "remote-primary",
            "browser": {
              "instance": "remote-primary",
              "launch_mode": "attach_only",
              "cdp_endpoint": "ws://127.0.0.1:19223"
            }
          }]
        }"#,
        &bridge,
        &mount,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("must use http or https"));
}

#[test]
fn attach_only_configuration_rejects_a_local_profile_path() {
    let (_root, bridge, mount) = isolated_roots();
    let profile = bridge.join("browser-profiles/remote-primary");
    let error = parse_accounts_config(
        &format!(
            r#"{{
              "version": 1,
              "accounts": [{{
                "id": "remote-primary",
                "browser": {{
                  "instance": "remote-primary",
                  "launch_mode": "attach_only",
                  "user_data_dir": "{}",
                  "cdp_endpoint": "http://127.0.0.1:19223"
                }}
              }}]
            }}"#,
            profile.display()
        ),
        &bridge,
        &mount,
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("attach_only must not set browser.user_data_dir"));
}

#[tokio::test]
async fn attach_only_fails_closed_when_the_remote_forward_is_unavailable() {
    let (_root, bridge, mount) = isolated_roots();
    fs::write(
        bridge.join("accounts.json"),
        r#"{
          "version": 1,
          "accounts": [{
            "id": "remote-primary",
            "browser": {
              "driver": "orca",
              "instance": "remote-primary",
              "launch_mode": "attach_only",
              "cdp_endpoint": "http://127.0.0.1:9"
            }
          }]
        }"#,
    )
    .unwrap();
    let pool = BrowserPool::new(
        &bridge,
        &mount,
        LegacyAccountConfig::default(),
        BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
    );

    let error = pool
        .open_chatgpt_login_page("remote-primary")
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("attach_only"));
    assert!(error.contains("will not start a local Chromium"));
}

#[tokio::test]
async fn attach_only_reaches_a_loopback_cdp_forward_without_a_local_profile() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route(
                    "/json/version",
                    get(|| async { Json(json!({"Browser":"Chrome"})) }),
                )
                .route("/json/list", get(|| async { Json(json!([])) })),
        )
        .await
        .unwrap();
    });

    let (_root, bridge, mount) = isolated_roots();
    fs::write(
        bridge.join("accounts.json"),
        format!(
            r#"{{
              "version": 1,
              "accounts": [{{
                "id": "remote-primary",
                "browser": {{
                  "driver": "orca",
                  "instance": "remote-primary",
                  "launch_mode": "attach_only",
                  "cdp_endpoint": "http://127.0.0.1:{}"
                }}
              }}]
            }}"#,
            address.port()
        ),
    )
    .unwrap();
    let pool = BrowserPool::new(
        &bridge,
        &mount,
        LegacyAccountConfig::default(),
        BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Orca), None, "active", None),
    );

    let health = pool.health("remote-primary").await;
    server.abort();

    assert_eq!(health.reachability, gpt2omo::BrowserReachability::Reachable);
    assert_eq!(health.instance, "remote-primary");
    assert!(health.detail.unwrap().contains("no live ChatGPT page"));
}
