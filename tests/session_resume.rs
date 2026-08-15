use omo_bridge::tools::task_state::{
    handle_task_plan, handle_task_state, handle_task_update, load_delegation_lifecycle,
    load_task_state, mark_session_retained, record_terminal_evidence,
    start_fresh_delegation_lifecycle, start_next_delegation_generation, DelegationTerminalState,
    TaskStatus,
};
use omo_bridge::WorkspaceMux;
use tempfile::tempdir;

#[test]
fn retained_completed_scope_resumes_same_browser_page_in_next_generation() {
    let mount = tempdir().unwrap();
    let project = mount.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let scopes = tempdir().unwrap();
    let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
    let scope = mux
        .register_browser(&project, "same-browser-page".into())
        .unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();

    let first = start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    assert_eq!(first.generation, 1);
    assert!(handle_task_state(&workspace, &scope.scope_id).success);
    record_terminal_evidence(
        &workspace,
        &scope.scope_id,
        DelegationTerminalState::Completed,
        Some("generation one complete"),
    )
    .unwrap();
    mark_session_retained(&workspace, &scope.scope_id, true).unwrap();

    let second = start_next_delegation_generation(&workspace, &scope.scope_id, false).unwrap();
    assert_eq!(second.generation, 2);
    assert!(second.ready_ms.is_none());
    assert!(second.terminal_state.is_none());
    assert!(!second.session_retained);
    assert_eq!(
        mux.lookup(&scope.scope_id)
            .unwrap()
            .browser_page_id
            .as_deref(),
        Some("same-browser-page")
    );
}

#[test]
fn retained_blocked_scope_reopens_blocked_item_without_losing_context() {
    let mount = tempdir().unwrap();
    let project = mount.path().join("project");
    std::fs::create_dir_all(&project).unwrap();
    let scopes = tempdir().unwrap();
    let mux = WorkspaceMux::new(mount.path(), scopes.path()).unwrap();
    let scope = mux
        .register_browser(&project, "blocked-browser-page".into())
        .unwrap();
    let workspace = mux.resolve(&scope.scope_id).unwrap();

    start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
    assert!(
        handle_task_plan(
            &workspace,
            &scope.scope_id,
            "Wait for dependency",
            vec!["Retry external dependency".into()]
        )
        .success
    );
    assert!(
        handle_task_update(
            &workspace,
            &scope.scope_id,
            "T1",
            "blocked",
            Some("dependency was offline")
        )
        .success
    );
    mark_session_retained(&workspace, &scope.scope_id, true).unwrap();

    let second = start_next_delegation_generation(&workspace, &scope.scope_id, true).unwrap();
    assert_eq!(second.generation, 2);
    let task_state = load_task_state(&workspace, &scope.scope_id)
        .unwrap()
        .unwrap();
    assert_eq!(task_state.items[0].status, TaskStatus::InProgress);
    assert_eq!(
        task_state.items[0].note.as_deref(),
        Some("dependency was offline")
    );

    assert!(handle_task_state(&workspace, &scope.scope_id).success);
    let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
        .unwrap()
        .unwrap();
    assert_eq!(lifecycle.generation, 2);
    assert!(lifecycle.ready_ms.is_some());
    assert!(lifecycle.terminal_state.is_none());
}
