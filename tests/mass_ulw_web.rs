use gpt2omo::mass_ulw_web::{
    DelegateWebConfig, DelegateWebResult, WebTask, DELEGATE_WEB_MAX_WORKERS,
    DELEGATE_WEB_SPAWN_STAGGER_SECS,
};
use serde_json::Value;
use std::path::PathBuf;

fn config() -> DelegateWebConfig {
    DelegateWebConfig::new("/opt/omo/delegate_to_chatgpt_web")
        .with_bridge_url("https://code.checka.cc")
}

#[test]
fn safety_contract_matches_bridge_policy() {
    assert_eq!(DELEGATE_WEB_MAX_WORKERS, 2);
    assert_eq!(DELEGATE_WEB_SPAWN_STAGGER_SECS, 10);
}

#[test]
fn single_retained_session_uses_stdin_and_keeps_scope_resumable_by_default() {
    let task = WebTask::new("core", "Inspect, implement, and verify the core change.")
        .with_workspace("/workspace/project");
    let invocation = config().single(&task).unwrap();

    assert_eq!(
        invocation.program,
        PathBuf::from("/opt/omo/delegate_to_chatgpt_web")
    );
    assert!(invocation
        .args
        .windows(2)
        .any(|pair| pair == ["--bridge-url", "https://code.checka.cc"]));
    assert!(invocation
        .args
        .windows(2)
        .any(|pair| pair == ["--workspace", "/workspace/project"]));
    assert!(invocation.args.contains(&"--stdin".to_string()));
    assert!(invocation.args.contains(&"--json".to_string()));
    assert!(!invocation.args.contains(&"--close-on-terminal".to_string()));
    assert_eq!(
        invocation.stdin.as_deref(),
        Some("Inspect, implement, and verify the core change.")
    );
}

#[test]
fn parallel_pair_is_one_batch_stdin_process_with_exactly_two_domains() {
    let backend =
        WebTask::new("backend", "Own backend files only.").with_workspace("/workspace/project");
    let frontend =
        WebTask::new("frontend", "Own frontend files only.").with_workspace("/workspace/project");
    let invocation = config().parallel_pair(&backend, &frontend).unwrap();

    assert!(invocation.args.contains(&"--batch-stdin".to_string()));
    assert!(!invocation.args.contains(&"--resume-scope".to_string()));
    let manifest: Value = serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
    let tasks = manifest["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0]["label"], "backend");
    assert_eq!(tasks[0]["workspace"], "/workspace/project");
    assert_eq!(tasks[1]["label"], "frontend");
}

#[test]
fn batch_wrapper_rejects_more_than_two_workers_before_spawn() {
    let tasks = [
        WebTask::new("a", "task a"),
        WebTask::new("b", "task b"),
        WebTask::new("c", "task c"),
    ];
    let error = config().batch(&tasks).unwrap_err().to_string();
    assert!(error.contains("1..=2"));
    assert!(error.contains("received 3"));
}

#[test]
fn resume_loop_targets_exact_retained_scope_without_workspace_or_batch_flags() {
    let invocation = config()
        .resume(
            "scope-123",
            "Local cargo test failed in auth::tests::expired_token; fix it and rerun verification.",
        )
        .unwrap();

    assert!(invocation
        .args
        .windows(2)
        .any(|pair| pair == ["--resume-scope", "scope-123"]));
    assert!(invocation.args.contains(&"--stdin".to_string()));
    assert!(!invocation.args.contains(&"--batch-stdin".to_string()));
    assert!(!invocation.args.contains(&"--workspace".to_string()));
    assert!(invocation
        .stdin
        .as_deref()
        .unwrap()
        .contains("Local cargo test failed"));
}

#[test]
fn delegate_json_drives_retained_fan_in_resume() {
    let result = DelegateWebResult::parse(
        r#"{
          "ok": true,
          "sent": true,
          "ready": true,
          "terminal": true,
          "delegations": [
            {
              "label": "backend",
              "scope_id": "scope-backend",
              "terminal_state": "COMPLETED",
              "terminal_detail": "backend implementation complete",
              "session_retained": true,
              "resumable": true
            },
            {
              "label": "frontend",
              "scope_id": "scope-frontend",
              "terminal_state": "COMPLETED",
              "terminal_detail": "frontend implementation complete",
              "session_retained": true,
              "resumable": true
            }
          ]
        }"#,
    )
    .unwrap();

    assert_eq!(
        result.retained_scope_for_label("backend").unwrap(),
        "scope-backend"
    );
    let handoff = result
        .fan_in_handoff(
            "backend",
            "Reconcile backend and frontend and make the full suite green.",
            "cargo test: one cross-domain integration assertion failed",
        )
        .unwrap();
    assert_eq!(handoff.primary_scope_id, "scope-backend");
    assert!(handoff
        .resume_prompt
        .contains("backend: terminal=COMPLETED"));
    assert!(handoff
        .resume_prompt
        .contains("frontend: terminal=COMPLETED"));
    assert!(handoff
        .resume_prompt
        .contains("one cross-domain integration assertion failed"));
    assert!(handoff
        .resume_prompt
        .contains("completion_check is ready=true"));

    let resume = handoff.resume_invocation(&config()).unwrap();
    assert!(resume
        .args
        .windows(2)
        .any(|pair| pair == ["--resume-scope", "scope-backend"]));
    assert_eq!(
        resume.stdin.as_deref(),
        Some(handoff.resume_prompt.as_str())
    );
}

#[test]
fn fan_in_waits_for_terminal_batch_and_requires_retained_primary_scope() {
    let non_terminal = DelegateWebResult::parse(
        r#"{
          "ok": false,
          "terminal": false,
          "delegations": [{
            "label": "core",
            "scope_id": "scope-core",
            "session_retained": true,
            "resumable": true
          }]
        }"#,
    )
    .unwrap();
    assert!(non_terminal
        .fan_in_handoff("core", "integrate", "")
        .unwrap_err()
        .to_string()
        .contains("not terminal"));

    let not_retained = DelegateWebResult::parse(
        r#"{
          "ok": true,
          "terminal": true,
          "delegations": [{
            "label": "core",
            "scope_id": "scope-core",
            "terminal_state": "COMPLETED",
            "session_retained": false,
            "resumable": false
          }]
        }"#,
    )
    .unwrap();
    assert!(not_retained
        .retained_scope_for_label("core")
        .unwrap_err()
        .to_string()
        .contains("not retained/resumable"));
}

#[test]
fn accepted_scope_can_be_closed_explicitly_after_final_approval() {
    let invocation = config().close_scope("scope-final").unwrap();
    assert!(invocation
        .args
        .windows(2)
        .any(|pair| pair == ["--close-scope", "scope-final"]));
    assert!(invocation.stdin.is_none());
}

#[test]
fn js_python_and_docs_preserve_the_same_workflow_contract() {
    let js = include_str!("../examples/mass_ulw_web.mjs");
    let python = include_str!("../examples/mass_ulw_web.py");
    let docs = include_str!("../docs/mass-ulw-web.md");

    for source in [js, python] {
        assert!(source.contains("MAX_WEB_WORKERS = 2"));
        assert!(source.contains("SPAWN_STAGGER_SECONDS = 10"));
        assert!(source.contains("--batch-stdin"));
        assert!(source.contains("--resume-scope"));
        assert!(source.contains("--close-scope"));
        assert!(source.contains("completion_check"));
    }

    assert!(js.contains("spawn(invocation.program, invocation.args"));
    assert!(python.contains("subprocess.run("));
    assert!(docs.contains("tool.dag"));
    assert!(docs.contains("Python `eval` pipeline"));
    assert!(docs.contains("active rate-limit lockout"));
    assert!(docs.contains("10 seconds"));
}

#[test]
fn wrapper_policy_is_pinned_to_delegate_runtime_guards() {
    let delegate = include_str!("../src/bin/delegate_to_chatgpt_web.rs");

    assert!(delegate.contains("const MAX_NEW_DISPATCH_WORKERS: usize = 2;"));
    assert!(delegate.contains("const MAX_CONCURRENT_IN_FLIGHT_WORKERS: usize = 3;"));
    assert!(delegate.contains("const SPAWN_STAGGER_DELAY: Duration = Duration::from_secs(10);"));
    assert!(delegate.contains("check_rate_limit_and_window_guards(tasks.len())?;"));
    assert!(delegate.contains("count_active_in_flight_workers(mux)?"));
    assert!(delegate.contains("sleep(SPAWN_STAGGER_DELAY).await;"));
    assert!(delegate.contains("--resume-scope cannot be combined with --batch-stdin"));
}
