use gpt2omo::orca::{BrowserDriverConfig, BrowserDriverKind};
use gpt2omo::{
    AccountRouter, BrowserBinding, BrowserInstanceConfig, BrowserPool, LegacyAccountConfig,
    WorkspaceMux,
};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::tempdir;

fn write_isolated_accounts(bridge: &Path) {
    let a_profile = bridge.join("browser-profiles/a");
    let b_profile = bridge.join("browser-profiles/b");
    fs::write(
        bridge.join("accounts.json"),
        format!(
            r#"{{
              "version": 1,
              "accounts": [
                {{
                  "id": "a",
                  "browser": {{
                    "driver": "orca",
                    "instance": "instance-a",
                    "user_data_dir": "{}",
                    "cdp_endpoint": "http://127.0.0.1:19223"
                  }}
                }},
                {{
                  "id": "b",
                  "browser": {{
                    "driver": "orca",
                    "instance": "instance-b",
                    "user_data_dir": "{}",
                    "cdp_endpoint": "http://127.0.0.1:19224"
                  }}
                }}
              ]
            }}"#,
            a_profile.display(),
            b_profile.display()
        ),
    )
    .unwrap();
}

fn legacy() -> LegacyAccountConfig {
    LegacyAccountConfig {
        browser: BrowserInstanceConfig::legacy("active"),
        ..LegacyAccountConfig::default()
    }
}

fn driver() -> BrowserDriverConfig {
    BrowserDriverConfig::with_driver(
        Some(BrowserDriverKind::Orca),
        Some("orca".into()),
        "active",
        None,
    )
}

#[tokio::test]
async fn persisted_bindings_survive_mux_restart_and_identical_page_ids_route_by_instance() {
    let root = tempdir().unwrap();
    let mount = root.path().join("mount");
    let bridge = root.path().join("bridge");
    let scopes = root.path().join("scope-state");
    let project = mount.join("project");
    fs::create_dir_all(&project).unwrap();
    fs::create_dir_all(&bridge).unwrap();
    write_isolated_accounts(&bridge);

    let first_mux = WorkspaceMux::new(&mount, &scopes).unwrap();
    let a = first_mux
        .register_browser_binding(
            &project,
            BrowserBinding::new("a", BrowserDriverKind::Orca, "instance-a", "same-page-id"),
        )
        .unwrap();
    let b = first_mux
        .register_browser_binding(
            &project,
            BrowserBinding::new("b", BrowserDriverKind::Orca, "instance-b", "same-page-id"),
        )
        .unwrap();
    drop(first_mux);

    let restarted_mux = WorkspaceMux::new(&mount, &scopes).unwrap();
    let a_binding = restarted_mux.lookup(&a.scope_id).unwrap().browser.unwrap();
    let b_binding = restarted_mux.lookup(&b.scope_id).unwrap().browser.unwrap();
    let pool = BrowserPool::new(&bridge, &mount, legacy(), driver());
    let a_target = pool.target_for_binding(&a_binding).await.unwrap();
    let b_target = pool.target_for_binding(&b_binding).await.unwrap();

    assert_eq!(a_binding.page_id, b_binding.page_id);
    assert_eq!(a_target.instance, "instance-a");
    assert_eq!(b_target.instance, "instance-b");
    assert_eq!(
        a_target.cdp_endpoint.as_deref(),
        Some("http://127.0.0.1:19223")
    );
    assert_eq!(
        b_target.cdp_endpoint.as_deref(),
        Some("http://127.0.0.1:19224")
    );
}

#[tokio::test]
async fn removed_account_never_migrates_a_retained_binding_to_another_browser() {
    let root = tempdir().unwrap();
    let mount = root.path().join("mount");
    let bridge = root.path().join("bridge");
    fs::create_dir_all(&mount).unwrap();
    fs::create_dir_all(&bridge).unwrap();
    write_isolated_accounts(&bridge);
    let pool = BrowserPool::new(&bridge, &mount, legacy(), driver());
    let binding = BrowserBinding::new("a", BrowserDriverKind::Orca, "instance-a", "retained-page");
    assert_eq!(
        pool.target_for_binding(&binding).await.unwrap().instance,
        "instance-a"
    );

    let b_profile = bridge.join("browser-profiles/b");
    fs::write(
        bridge.join("accounts.json"),
        format!(
            r#"{{"version":1,"accounts":[{{
              "id":"b",
              "browser":{{
                "driver":"orca",
                "instance":"instance-b",
                "user_data_dir":"{}",
                "cdp_endpoint":"http://127.0.0.1:19224"
              }}
            }}]}}"#,
            b_profile.display()
        ),
    )
    .unwrap();

    let error = pool
        .target_for_binding(&binding)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("BROWSER_ACCOUNT_UNAVAILABLE"));
    assert!(error.contains("account 'a' is missing"));
}

#[test]
fn draining_transition_preserves_forced_affinity_while_new_work_moves_elsewhere() {
    let root = tempdir().unwrap();
    let mount = root.path().join("mount");
    let bridge = root.path().join("bridge");
    fs::create_dir_all(&mount).unwrap();
    fs::create_dir_all(&bridge).unwrap();
    fs::write(
        bridge.join("accounts.json"),
        r#"{
          "version":1,
          "accounts":[
            {"id":"old","enabled":true,"draining":true,"browser":{"instance":"old"}},
            {"id":"new","enabled":true,"browser":{"instance":"new"}}
          ]
        }"#,
    )
    .unwrap();
    let router = AccountRouter::new(&bridge, &mount, legacy());

    let fresh = router.reserve_one(&HashMap::new(), 10_000).unwrap();
    assert_eq!(fresh.account.id, "new");
    router.release(&fresh, 10_000).unwrap();

    let retained = router
        .reserve_for_account("old", &HashMap::new(), 10_000)
        .unwrap();
    assert_eq!(retained.account.id, "old");
}

#[test]
fn multi_account_config_without_real_profile_and_endpoint_is_not_sufficient_for_browser_isolation()
{
    let root = tempdir().unwrap();
    let mount = root.path().join("mount");
    let bridge = root.path().join("bridge");
    fs::create_dir_all(&mount).unwrap();
    fs::create_dir_all(&bridge).unwrap();
    fs::write(
        bridge.join("accounts.json"),
        r#"{
          "version":1,
          "accounts":[
            {"id":"a","browser":{"instance":"a"}},
            {"id":"b","browser":{"instance":"b"}}
          ]
        }"#,
    )
    .unwrap();

    let pool = BrowserPool::new(&bridge, &mount, legacy(), driver());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let error = runtime
        .block_on(pool.create_chatgpt_page("a"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("distinct browser.cdp_endpoint and browser.user_data_dir"));
}
