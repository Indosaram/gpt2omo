use gpt2omo::tools::task_state::{
    load_delegation_lifecycle, record_terminal_evidence, retain_session_with_lease,
    start_fresh_delegation_lifecycle, start_next_delegation_generation, DelegationTerminalState,
};
use gpt2omo::WorkspaceMux;
use tempfile::tempdir;

#[test]
fn repeated_generations_consume_and_renew_idle_retention_lease() {
    let mount = tempdir().unwrap();
    let project = mount.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let state = tempdir().unwrap();
    let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
    let scope = mux
        .register_browser(&project, "same-browser-page".into())
        .unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();

    let first = start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    assert_eq!(first.generation, 1);
    record_terminal_evidence(
        &workspace,
        &scope.scope_id,
        DelegationTerminalState::Completed,
        Some("generation one complete"),
    )
    .unwrap();
    let first_idle = retain_session_with_lease(&workspace, &scope.scope_id, 60_000).unwrap();
    assert!(first_idle.session_retained);
    let first_expiry = first_idle.lease_expires_ms.unwrap();

    let second = start_next_delegation_generation(&workspace, &scope.scope_id, false).unwrap();
    assert_eq!(second.generation, 2);
    assert!(!second.session_retained);
    assert!(second.retained_since_ms.is_none());
    assert!(second.lease_expires_ms.is_none());
    assert_eq!(
        mux.lookup(&scope.scope_id)
            .unwrap()
            .browser_page_id
            .as_deref(),
        Some("same-browser-page")
    );

    record_terminal_evidence(
        &workspace,
        &scope.scope_id,
        DelegationTerminalState::Completed,
        Some("generation two complete"),
    )
    .unwrap();
    let second_idle = retain_session_with_lease(&workspace, &scope.scope_id, 60_000).unwrap();
    assert_eq!(second_idle.generation, 2);
    assert!(second_idle.session_retained);
    assert!(second_idle.lease_expires_ms.unwrap() >= first_expiry);

    let persisted = load_delegation_lifecycle(&workspace, &scope.scope_id)
        .unwrap()
        .unwrap();
    assert_eq!(persisted.generation, 2);
    assert!(persisted.session_retained);
    assert!(persisted.lease_expires_ms.is_some());
}
