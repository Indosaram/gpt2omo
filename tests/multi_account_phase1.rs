use gpt2omo::orca::BrowserDriverKind;
use gpt2omo::tools::task_state::start_fresh_delegation_lifecycle;
use gpt2omo::{AccountRouter, BrowserBinding, LegacyAccountConfig, RouterError, WorkspaceMux};
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

struct Harness {
    _root: TempDir,
    bridge: std::path::PathBuf,
    mount: std::path::PathBuf,
    scopes: std::path::PathBuf,
}

impl Harness {
    fn new(accounts_json: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        let scopes = root.path().join("scopes");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        fs::create_dir_all(&scopes).unwrap();
        fs::write(bridge.join("accounts.json"), accounts_json).unwrap();
        Self {
            _root: root,
            bridge,
            mount,
            scopes,
        }
    }

    fn router(&self) -> AccountRouter {
        AccountRouter::new(&self.bridge, &self.mount, LegacyAccountConfig::default())
    }

    fn mux(&self) -> WorkspaceMux {
        WorkspaceMux::new(&self.mount, &self.scopes).unwrap()
    }

    fn project(&self, name: &str) -> std::path::PathBuf {
        let path = self.mount.join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }
}

fn two_account_config(max_active_workers: usize) -> String {
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
              "max_active_workers": {max_active_workers}
            }}
          }},
          "accounts": [
            {{"id": "a", "browser": {{"instance": "instance-a"}}}},
            {{"id": "b", "browser": {{"instance": "instance-b"}}}}
          ]
        }}"#
    )
}

#[test]
fn batch_routing_persists_distinct_bindings_and_cooldown_isolation() {
    let harness = Harness::new(&two_account_config(2));
    let router = harness.router();
    let mux = harness.mux();

    let reservations = router.reserve_batch_for_mux(&mux, 2, 1_000).unwrap();
    assert_eq!(
        reservations
            .iter()
            .map(|reservation| reservation.account.id.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );

    for (index, reservation) in reservations.iter().enumerate() {
        let project = harness.project(&format!("project-{index}"));
        let scope = mux
            .register_browser_binding(
                &project,
                BrowserBinding::new(
                    reservation.account.id.clone(),
                    BrowserDriverKind::Orca,
                    reservation.account.browser.instance.clone(),
                    "same-page-id",
                ),
            )
            .unwrap();
        let persisted = mux.lookup(&scope.scope_id).unwrap();
        assert_eq!(persisted.account_id(), reservation.account.id);
        assert_eq!(
            persisted.browser_instance(),
            Some(reservation.account.browser.instance.as_str())
        );
        assert_eq!(persisted.page_id(), Some("same-page-id"));
        router.commit(reservation, 1_100 + index as u64).unwrap();
    }

    router
        .apply_rate_limit("a", "too_many_requests", Some(60), 2_000)
        .unwrap();
    let selected = router.reserve_one(&HashMap::new(), 2_001).unwrap();
    assert_eq!(selected.account.id, "b");
    router.release(&selected, 2_002).unwrap();

    let state_a = router.state_for_account("a", 2_002).unwrap();
    let state_b = router.state_for_account("b", 2_002).unwrap();
    assert_eq!(
        state_a.cooldown_reason.as_deref(),
        Some("too_many_requests")
    );
    assert_eq!(state_b.cooldown_until_ms, None);
}

#[test]
fn live_scope_headroom_is_counted_atomically_and_ghost_scope_is_reclaimed() {
    let harness = Harness::new(&two_account_config(1));
    let router = harness.router();
    let mux = harness.mux();
    let project = harness.project("live-a");

    let scope = mux
        .register_browser_binding(
            &project,
            BrowserBinding::new("a", BrowserDriverKind::Orca, "instance-a", "page-a"),
        )
        .unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();
    start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();

    let scope_lock = mux.lock_scope(&scope.scope_id).unwrap();
    let selected = router.reserve_batch_for_mux(&mux, 1, 3_000).unwrap();
    assert_eq!(selected[0].account.id, "b");
    router.release(&selected[0], 3_001).unwrap();

    drop(scope_lock);
    let reclaimed = router
        .reserve_for_account_for_mux(&mux, "a", 3_002)
        .unwrap();
    assert_eq!(reclaimed.account.id, "a");
    router.release(&reclaimed, 3_003).unwrap();
}

#[test]
fn exhausted_window_reports_exact_retry_boundary() {
    let harness = Harness::new(
        r#"{
          "version": 1,
          "routing": {
            "strategy": "round_robin",
            "reservation_ttl_seconds": 10,
            "selection_failure_backoff_seconds": 5
          },
          "defaults": {
            "limits": {
              "window_seconds": 10,
              "max_dispatches": 1,
              "max_active_workers": 1
            }
          },
          "accounts": [
            {"id": "only", "browser": {"instance": "only-instance"}}
          ]
        }"#,
    );
    let router = harness.router();

    let reservation = router.reserve_one(&HashMap::new(), 1_000).unwrap();
    router.commit(&reservation, 1_000).unwrap();

    let error = router.reserve_one(&HashMap::new(), 1_001).unwrap_err();
    let RouterError::Exhausted(exhausted) = error else {
        panic!("expected structured exhaustion");
    };
    assert_eq!(exhausted.accounts.len(), 1);
    assert_eq!(exhausted.accounts[0].account_id, "only");
    assert_eq!(exhausted.accounts[0].reason, "window_exhausted");
    assert_eq!(exhausted.accounts[0].retry_at_ms, Some(11_000));

    let available_at_boundary = router.reserve_one(&HashMap::new(), 11_000).unwrap();
    assert_eq!(available_at_boundary.account.id, "only");
}

#[test]
fn retained_affinity_does_not_migrate_to_an_enabled_account() {
    let harness = Harness::new(
        r#"{
          "version": 1,
          "routing": {
            "strategy": "round_robin",
            "reservation_ttl_seconds": 10,
            "selection_failure_backoff_seconds": 5
          },
          "defaults": {
            "limits": {
              "window_seconds": 60,
              "max_dispatches": 10,
              "max_active_workers": 2
            }
          },
          "accounts": [
            {"id": "retained", "enabled": false, "browser": {"instance": "retained-instance"}},
            {"id": "fresh", "enabled": true, "browser": {"instance": "fresh-instance"}}
          ]
        }"#,
    );
    let router = harness.router();
    let mux = harness.mux();

    let fresh = router.reserve_batch_for_mux(&mux, 1, 5_000).unwrap();
    assert_eq!(fresh[0].account.id, "fresh");
    router.release(&fresh[0], 5_001).unwrap();

    let retained = router
        .reserve_for_account_for_mux(&mux, "retained", 5_002)
        .unwrap();
    assert_eq!(retained.account.id, "retained");
    assert_ne!(retained.account.id, "fresh");
}

#[test]
fn disabled_accounts_may_share_a_dormant_browser_target() {
    let harness = Harness::new(
        r#"{
          "version": 1,
          "accounts": [
            {"id": "old-a", "enabled": false, "browser": {"instance": "dormant"}},
            {"id": "old-b", "enabled": false, "browser": {"instance": "dormant"}},
            {"id": "active", "enabled": true, "browser": {"instance": "active-instance"}}
          ]
        }"#,
    );
    let config = harness.router().load_config().unwrap();
    assert_eq!(config.accounts.len(), 3);
    assert_eq!(
        config
            .accounts
            .iter()
            .filter(|account| account.enabled)
            .count(),
        1
    );
}
