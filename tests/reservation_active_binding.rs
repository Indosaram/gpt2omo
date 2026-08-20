use gpt2omo::orca::BrowserDriverKind;
use gpt2omo::tools::task_state::start_fresh_delegation_lifecycle;
use gpt2omo::{AccountRouter, BrowserBinding, LegacyAccountConfig, RouterError, WorkspaceMux};
use std::fs;

#[test]
fn bound_reservation_is_not_double_counted_against_active_headroom() {
    let root = tempfile::tempdir().unwrap();
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    let scopes = root.path().join("scopes");
    let project = mount.join("project");
    fs::create_dir_all(&bridge).unwrap();
    fs::create_dir_all(&project).unwrap();
    fs::write(
        bridge.join("accounts.json"),
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
            {"id": "only", "browser": {"instance": "only-instance"}}
          ]
        }"#,
    )
    .unwrap();

    let router = AccountRouter::new(&bridge, &mount, LegacyAccountConfig::default());
    let mux = WorkspaceMux::new(&mount, &scopes).unwrap();

    let first = router.reserve_batch_for_mux(&mux, 1, 1_000).unwrap();
    let first = &first[0];
    let scope = mux
        .register_browser_binding(
            &project,
            BrowserBinding::new(
                "only",
                BrowserDriverKind::Orca,
                "only-instance",
                "page-only",
            ),
        )
        .unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();
    start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    let scope_lock = mux.lock_scope(&scope.scope_id).unwrap();
    router.bind_scope(first, &scope.scope_id, 1_001).unwrap();
    let bound_state = router.state_for_account("only", 1_001).unwrap();
    assert_eq!(
        bound_state.reservations[0].scope_id.as_deref(),
        Some(scope.scope_id.as_str())
    );

    let second = router.reserve_batch_for_mux(&mux, 1, 1_002).unwrap();
    assert_eq!(second[0].account.id, "only");

    let error = router.reserve_batch_for_mux(&mux, 1, 1_003).unwrap_err();
    let RouterError::Exhausted(exhausted) = error else {
        panic!("expected structured exhaustion");
    };
    assert_eq!(exhausted.accounts.len(), 1);
    assert_eq!(exhausted.accounts[0].account_id, "only");
    assert_eq!(exhausted.accounts[0].reason, "active_workers_exhausted");

    router.release(&second[0], 1_004).unwrap();
    router.release(first, 1_004).unwrap();
    drop(scope_lock);
}
