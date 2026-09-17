use crate::browser_pool::{browser_verify_failure_is_definitive, BrowserPool};
use crate::security::BrowserBinding;
use crate::security::workspace::default_bridge_base_dir;
use crate::tools::task_state::{
    load_delegation_lifecycle, record_terminal_evidence, release_session_retention,
    retain_session_with_lease, retained_session_expired, DelegationLifecycle,
    DelegationTerminalState,
};
use crate::{Result, WorkspaceMux, WorkspaceScope};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ExpiredSessionCleanup {
    pub scope_id: String,
    pub browser_page_id: Option<String>,
    pub account_id: Option<String>,
    pub browser_instance: Option<String>,
    pub scope_removed: bool,
    pub page_closed: bool,
    pub close_error: Option<String>,
}

struct ClaimedExpiredSession {
    scope_id: String,
    browser: Option<BrowserBinding>,
    _scope_lock: crate::WorkspaceScopeLock,
}

pub async fn cleanup_expired_retained_sessions(
    mux: &WorkspaceMux,
    browsers: &BrowserPool,
    now_ms: u64,
    legacy_ttl_ms: u64,
    exclude_scope_id: Option<&str>,
) -> Result<Vec<ExpiredSessionCleanup>> {
    let scopes = mux.list_scopes()?;
    let mut cleaned = Vec::new();

    for scope in scopes {
        if exclude_scope_id.is_some_and(|excluded| excluded == scope.scope_id) {
            continue;
        }
        let claimed = match claim_expired_retained_scope(mux, &scope, now_ms, legacy_ttl_ms) {
            Ok(Some(claimed)) => claimed,
            Ok(None) => continue,
            // One unresolvable workspace must not abort the whole GC pass;
            // stale metadata for a dead workspace is removed and reported.
            Err(error) => {
                if remove_stale_unresolvable_scope(mux, &scope, now_ms).unwrap_or(false) {
                    cleaned.push(ExpiredSessionCleanup {
                        scope_id: scope.scope_id.clone(),
                        browser_page_id: scope.browser.as_ref().map(|b| b.page_id.clone()),
                        account_id: scope.browser.as_ref().map(|b| b.account_id.clone()),
                        browser_instance: scope.browser.as_ref().map(|b| b.instance.clone()),
                        scope_removed: true,
                        page_closed: false,
                        close_error: Some(format!(
                            "workspace unresolvable; removed stale scope metadata: {error}"
                        )),
                    });
                }
                continue;
            }
        };

        let close_result = match claimed.browser.as_ref() {
            Some(binding) => browsers.close(binding).await,
            None => Ok(()),
        };
        let page_closed = close_result.is_ok();
        let mut cleanup_error = close_result.as_ref().err().map(ToString::to_string);
        let scope_removed = if page_closed {
            match mux.resolve(&claimed.scope_id) {
                Ok(workspace) => match mux.remove(&claimed.scope_id) {
                    Ok(()) => {
                        if let Err(error) = release_session_retention(&workspace, &claimed.scope_id)
                        {
                            cleanup_error = Some(format!(
                                "browser tab closed and scope removed, but retained lifecycle cleanup failed: {error}"
                            ));
                        }
                        true
                    }
                    Err(error) => {
                        cleanup_error = Some(format!(
                            "browser tab closed, but scope metadata cleanup failed; retry is safe because browser close is idempotent for CDP targets: {error}"
                        ));
                        false
                    }
                },
                Err(error) => {
                    cleanup_error = Some(format!(
                        "browser tab closed, but workspace resolution failed before scope cleanup: {error}"
                    ));
                    false
                }
            }
        } else if close_result
            .as_ref()
            .err()
            .is_some_and(browser_close_failure_is_definitive)
        {
            // The account/driver this scope was bound to no longer exists, so
            // the tab can never be closed through any driver. The lease already
            // expired and this GC claim holds the flock; keeping the scope file
            // would only make the GC retry the impossible close forever.
            match mux.remove(&claimed.scope_id) {
                Ok(()) => {
                    cleanup_error = Some(format!(
                        "definitive browser-account close failure ({original}); removed expired scope metadata",
                        original = close_result.as_ref().expect_err("definitive branch requires close error")
                    ));
                    true
                }
                Err(error) => {
                    cleanup_error = Some(format!(
                        "definitive close failure and scope removal failed: {error}"
                    ));
                    false
                }
            }
        } else {
            false
        };
        cleaned.push(ExpiredSessionCleanup {
            scope_id: claimed.scope_id,
            browser_page_id: claimed
                .browser
                .as_ref()
                .map(|binding| binding.page_id.clone()),
            account_id: claimed
                .browser
                .as_ref()
                .map(|binding| binding.account_id.clone()),
            browser_instance: claimed
                .browser
                .as_ref()
                .map(|binding| binding.instance.clone()),
            scope_removed,
            page_closed,
            close_error: cleanup_error,
        });
    }

    Ok(cleaned)
}

pub async fn recover_dead_browser_scopes(
    mux: &WorkspaceMux,
    browsers: &BrowserPool,
) -> Result<Vec<String>> {
    let scopes = mux.list_scopes()?;
    let mut recovered = Vec::new();

    for scope in scopes {
        let Some(binding) = scope.browser.as_ref() else {
            continue;
        };
        let Some(scope_lock) = mux.try_lock_scope(&scope.scope_id)? else {
            continue;
        };
        let Ok(workspace) = mux.resolve(&scope.scope_id) else {
            continue;
        };
        let Ok(Some(lifecycle)) = load_delegation_lifecycle(&workspace, &scope.scope_id) else {
            continue;
        };
        if lifecycle.terminal_state.is_some() {
            continue;
        }

        // Scope has no live process holding the lock and is marked nonterminal.
        // Check if the bound browser page still exists on the browser instance.
        let verify_result = browsers.verify(binding).await;
        if let Err(error) = verify_result {
            if browser_verify_failure_is_definitive(&error) {
                let detail = format!(
                    "bound browser tab {} was closed or does not exist: {}",
                    binding.page_id, error
                );
                // Only drop the scope file after the terminal write actually
                // succeeded; an ignored terminal-write failure here is exactly
                // what produced nonterminal lifecycle ghosts in the past.
                match record_terminal_evidence(
                    &workspace,
                    &scope.scope_id,
                    DelegationTerminalState::Failed,
                    Some(&detail),
                ) {
                    Ok(_) => {
                        let _ = release_session_retention(&workspace, &scope.scope_id);
                        let _ = mux.remove(&scope.scope_id);
                        recovered.push(scope.scope_id);
                    }
                    Err(record_error) => tracing::warn!(
                        scope_id = %scope.scope_id,
                        record_error = %record_error,
                        "kept scope after definitive browser death because terminal evidence could not be recorded"
                    ),
                }
            }
        }
        drop(scope_lock);
    }

    Ok(recovered)
}

fn claim_expired_retained_scope(
    mux: &WorkspaceMux,
    listed_scope: &WorkspaceScope,
    now_ms: u64,
    legacy_ttl_ms: u64,
) -> Result<Option<ClaimedExpiredSession>> {
    let Some(scope_lock) = mux.try_lock_scope(&listed_scope.scope_id)? else {
        return Ok(None);
    };

    let scope = match mux.lookup(&listed_scope.scope_id) {
        Ok(scope) => scope,
        Err(_) => return Ok(None),
    };
    let workspace = mux.resolve(&scope.scope_id)?;
    let Some(mut lifecycle) =
        load_delegation_lifecycle(&workspace, &scope.scope_id).map_err(crate::BridgeError::Path)?
    else {
        return Ok(None);
    };
    if !lifecycle.session_retained || lifecycle.terminal_state.is_none() {
        return Ok(None);
    }

    if lifecycle.lease_expires_ms.is_none() {
        if legacy_ttl_ms == 0 {
            return Ok(None);
        }
        lifecycle = retain_session_with_lease(&workspace, &scope.scope_id, legacy_ttl_ms)
            .map_err(crate::BridgeError::Path)?;
    }
    if !retained_session_expired(&lifecycle, now_ms) {
        return Ok(None);
    }

    Ok(Some(ClaimedExpiredSession {
        scope_id: scope.scope_id,
        browser: scope.browser,
        _scope_lock: scope_lock,
    }))
}

fn remove_stale_unresolvable_scope(
    mux: &WorkspaceMux,
    scope: &WorkspaceScope,
    now_ms: u64,
) -> Result<bool> {
    let Some(_scope_lock) = mux.try_lock_scope(&scope.scope_id)? else {
        return Ok(false);
    };
    if now_ms.saturating_sub(scope.updated_ms) <= LEDGER_STALE_SCOPE_GRACE_MS {
        return Ok(false);
    }
    mux.remove(&scope.scope_id)?;
    Ok(true)
}

fn browser_close_failure_is_definitive(error: &impl std::fmt::Display) -> bool {
    let message = format!("{error:#}");
    message.contains("BROWSER_ACCOUNT_UNAVAILABLE") || message.contains("driver changed")
}

const LEDGER_STALE_SCOPE_GRACE_MS: u64 = 60 * 60 * 1000;
const LEDGER_RECONCILE_DETAIL: &str =
    "scope unregistered without terminal record; reconciled by session GC";

#[derive(Debug, Default, Serialize)]
pub struct LedgerReconcileReport {
    pub terminalized_lifecycle: Vec<String>,
    pub cleared_pending_continuations: Vec<String>,
    pub removed_stale_scopes: Vec<String>,
}

/// Reconcile durable delegation ledgers with the live scope registry.
///
/// Closes three recurring ledger/registry drift modes:
/// - a scope removed without a terminal lifecycle write (helper crash or an
///   ignored terminal-write failure) leaves a permanently "active" generation;
/// - a workspace deleted underneath a scope leaves metadata that the GC warns
///   about on every pass;
/// - continuation_required events persisted for scopes that later terminated
///   without completion are replayed forever.
///
/// Safety: scopes are only rewritten or removed once `try_lock_scope` proves
/// no live process holds the flock, and stale-scope removal additionally
/// requires the metadata to be older than `LEDGER_STALE_SCOPE_GRACE_MS`.
pub fn reconcile_delegation_ledger(
    mux: &WorkspaceMux,
    now_ms: u64,
) -> Result<LedgerReconcileReport> {
    let mut report = LedgerReconcileReport::default();
    let listed = mux.list_scopes()?;
    let listed_ids: HashSet<String> = listed.iter().map(|s| s.scope_id.clone()).collect();

    let lifecycle_dir = default_bridge_base_dir().join("delegation-lifecycle");
    let mut index: HashMap<String, Vec<(PathBuf, DelegationLifecycle)>> = HashMap::new();
    if let Ok(entries) = fs::read_dir(&lifecycle_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Ok(bytes) = fs::read(&path) else { continue };
            let Ok(lifecycle) = serde_json::from_slice::<DelegationLifecycle>(&bytes) else {
                continue;
            };
            index
                .entry(lifecycle.scope_id.clone())
                .or_default()
                .push((path, lifecycle));
        }
    }

    // A) Unregistered scopes can never host a worker again, so any
    // nonterminal lifecycle entry is ledger drift; terminalize in place.
    for (scope_id, entries) in &index {
        if listed_ids.contains(scope_id) {
            continue;
        }
        for (path, lifecycle) in entries {
            if lifecycle.terminal_state.is_some() {
                continue;
            }
            let mut terminal = lifecycle.clone();
            terminal.terminal_state = Some(DelegationTerminalState::Failed);
            terminal.terminal_ms = Some(now_ms);
            terminal.terminal_detail = Some(LEDGER_RECONCILE_DETAIL.to_string());
            match rewrite_lifecycle_file_atomic(path, &terminal) {
                Ok(()) => report.terminalized_lifecycle.push(scope_id.clone()),
                Err(error) => tracing::warn!(
                    scope_id = %scope_id,
                    path = %path.display(),
                    error = %error,
                    "failed to terminalize orphaned delegation lifecycle entry"
                ),
            }
        }
    }

    // B) Pending continuations whose scope already reached a terminal state.
    let terminal_scopes: HashSet<String> = index
        .iter()
        .filter(|(_, entries)| {
            entries
                .iter()
                .max_by_key(|(_, lifecycle)| lifecycle.updated_ms)
                .is_some_and(|(_, lifecycle)| lifecycle.terminal_state.is_some())
        })
        .map(|(scope_id, _)| scope_id.clone())
        .collect();
    if !terminal_scopes.is_empty() {
        let pending_dir = default_bridge_base_dir().join("pending-continuations");
        if let Ok(entries) = fs::read_dir(&pending_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) != Some("json") {
                    continue;
                }
                let Ok(bytes) = fs::read(&path) else { continue };
                let Ok(event) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
                    continue;
                };
                let Some(scope_id) = pending_event_scope_id(&event) else { continue };
                if !terminal_scopes.contains(&scope_id) {
                    continue;
                }
                match fs::remove_file(&path) {
                    Ok(()) => report.cleared_pending_continuations.push(scope_id),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "failed to clear stale pending continuation"
                    ),
                }
            }
        }
    }

    // C) Registered scopes whose workspace disappeared, and registered scopes
    //    with no browser binding: neither can host a live worker. After the
    //    flock is free and the metadata is stale, terminalize and drop them.
    for scope in listed {
        let unresolvable = mux.resolve(&scope.scope_id).is_err();
        let bindingless = scope.browser.is_none();
        if (!unresolvable && !bindingless)
            || now_ms.saturating_sub(scope.updated_ms) <= LEDGER_STALE_SCOPE_GRACE_MS
        {
            continue;
        }
        let scope_lock = match mux.try_lock_scope(&scope.scope_id) {
            Ok(Some(scope_lock)) => scope_lock,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(
                    scope_id = %scope.scope_id,
                    error = %error,
                    "ledger reconcile could not lock scope"
                );
                continue;
            }
        };
        let terminal_detail = if unresolvable {
            "workspace unresolvable; reconciled by session GC"
        } else {
            "no browser binding; reconciled by session GC"
        };
        let terminal_written = match mux.resolve(&scope.scope_id) {
            Ok(workspace) => record_terminal_evidence(
                &workspace,
                &scope.scope_id,
                DelegationTerminalState::Failed,
                Some(terminal_detail),
            )
            .is_ok(),
            Err(_) => entries_terminalized_in_place(
                index.get(&scope.scope_id),
                terminal_detail,
                now_ms,
            ),
        };
        if terminal_written {
            match mux.remove(&scope.scope_id) {
                Ok(()) => report.removed_stale_scopes.push(scope.scope_id.clone()),
                Err(error) => tracing::warn!(
                    scope_id = %scope.scope_id,
                    error = %error,
                    "failed to remove stale scope metadata"
                ),
            }
        }
        drop(scope_lock);
    }

    // D) Scope files on disk that list_scopes rejects (corrupted workspace
    //    metadata). They poison every scan with warnings; once stale they are
    //    dead by any lease and are dropped together with their lifecycle.
    let scope_dir = mux.scope_dir().to_path_buf();
    if let Ok(entries) = fs::read_dir(&scope_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let Some(scope_id) = path.file_stem().and_then(|s| s.to_str()).map(str::to_string)
            else {
                continue;
            };
            if uuid::Uuid::parse_str(&scope_id).is_err() || listed_ids.contains(&scope_id) {
                continue;
            }
            let updated_ms = fs::read(&path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|value| value.get("updated_ms").and_then(serde_json::Value::as_u64))
                .unwrap_or(0);
            if now_ms.saturating_sub(updated_ms) <= LEDGER_STALE_SCOPE_GRACE_MS {
                continue;
            }
            let scope_lock = match mux.try_lock_scope(&scope_id) {
                Ok(Some(scope_lock)) => scope_lock,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        scope_id = %scope_id,
                        error = %error,
                        "ledger reconcile could not lock rejected scope"
                    );
                    continue;
                }
            };
            entries_terminalized_in_place(
                index.get(&scope_id),
                "scope metadata rejected by registry; reconciled by session GC",
                now_ms,
            );
            match mux.remove(&scope_id) {
                Ok(()) => report.removed_stale_scopes.push(scope_id),
                Err(error) => tracing::warn!(
                    scope_id = %scope_id,
                    error = %error,
                    "failed to remove rejected scope file"
                ),
            }
            drop(scope_lock);
        }
    }

    Ok(report)
}

fn entries_terminalized_in_place(
    entries: Option<&Vec<(PathBuf, DelegationLifecycle)>>,
    detail: &str,
    now_ms: u64,
) -> bool {
    match entries {
        None => true,
        Some(entries) => entries.iter().all(|(path, lifecycle)| {
            if lifecycle.terminal_state.is_some() {
                return true;
            }
            let mut terminal = lifecycle.clone();
            terminal.terminal_state = Some(DelegationTerminalState::Failed);
            terminal.terminal_ms = Some(now_ms);
            terminal.terminal_detail = Some(detail.to_string());
            rewrite_lifecycle_file_atomic(path, &terminal).is_ok()
        }),
    }
}

fn rewrite_lifecycle_file_atomic(
    path: &Path,
    lifecycle: &DelegationLifecycle,
) -> std::result::Result<(), std::io::Error> {
    let tmp = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(lifecycle)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)
}

fn pending_event_scope_id(event: &serde_json::Value) -> Option<String> {
    if let Some(scope_id) = event
        .get("data")
        .and_then(|data| data.get("scope_id"))
        .and_then(serde_json::Value::as_str)
    {
        return Some(scope_id.to_string());
    }
    let prompt = event
        .pointer("/data/prompt")
        .and_then(serde_json::Value::as_str)?;
    let marker = "scope ";
    let start = prompt.find(marker)? + marker.len();
    let candidate = prompt.get(start..start + 36)?;
    uuid::Uuid::parse_str(candidate).ok().map(|uuid| uuid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orca::{BrowserDriverConfig, BrowserDriverKind};
    use crate::tools::task_state::{
        load_delegation_lifecycle, mark_session_retained, record_terminal_evidence,
        start_fresh_delegation_lifecycle, DelegationTerminalState,
    };
    use crate::{BrowserBinding, LegacyAccountConfig};
    use std::fs;
    use tempfile::tempdir;

    // Reconcile tests share the process-global bridge base dir (lifecycle and
    // pending-continuation paths are keyed by the real base dir), so they must
    // not run concurrently with each other.
    static RECONCILE_LEDGER_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn ghost_scope_setup() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        WorkspaceMux,
        WorkspaceScope,
    ) {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                BrowserBinding::new("default", BrowserDriverKind::Orca, "legacy", "page-a"),
            )
            .unwrap();
        start_fresh_delegation_lifecycle(&mux.resolve(&scope.scope_id).unwrap(), &scope.scope_id)
            .unwrap();
        (mount, state, mux, scope)
    }

    fn lifecycle_entry_for(scope_id: &str) -> Option<DelegationLifecycle> {
        let entries = std::fs::read_dir(default_bridge_base_dir().join("delegation-lifecycle")).ok()?;
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(bytes) = std::fs::read(&path) else { continue };
            let Ok(lifecycle) = serde_json::from_slice::<DelegationLifecycle>(&bytes) else {
                continue;
            };
            if lifecycle.scope_id == scope_id {
                return Some(lifecycle);
            }
        }
        None
    }

    #[test]
    fn reconcile_terminalizes_lifecycle_for_unregistered_scope() {
        let _ledger_guard = RECONCILE_LEDGER_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (_mount, _state, mux, scope) = ghost_scope_setup();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        // Registry loses the scope without a terminal lifecycle write.
        mux.remove(&scope.scope_id).unwrap();
        let report = reconcile_delegation_ledger(&mux, 123).unwrap();
        assert!(report.terminalized_lifecycle.contains(&scope.scope_id));
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            lifecycle.terminal_state,
            Some(DelegationTerminalState::Failed)
        );
    }

    #[test]
    fn reconcile_clears_pending_continuation_for_terminal_scope() {
        let _ledger_guard = RECONCILE_LEDGER_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (_mount, _state, mux, scope) = ghost_scope_setup();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        let pending_path = default_bridge_base_dir()
            .join("pending-continuations")
            .join(format!("{}.json", scope.scope_id));
        serde_json::to_writer(
            std::fs::File::create(&pending_path).unwrap(),
            &serde_json::json!({
                "seq": 1,
                "kind": "continuation_required",
                "timestamp_ms": 1,
                "workspace": "/",
                "data": {
                    "scope_id": scope.scope_id,
                    "prompt": format!(
                        "The coding task for scope {} is not complete.",
                        scope.scope_id
                    )
                }
            }),
        )
        .unwrap();
        let report = reconcile_delegation_ledger(&mux, 456).unwrap();
        assert!(report
            .cleared_pending_continuations
            .contains(&scope.scope_id.to_string()));
        assert!(!pending_path.exists());
    }

    #[test]
    fn reconcile_removes_stale_scope_with_missing_workspace() {
        let _ledger_guard = RECONCILE_LEDGER_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (mount, _state, mux, scope) = ghost_scope_setup();
        let scope_path = mux.scope_dir().join(format!("{}.json", scope.scope_id));
        let mut file_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&scope_path).unwrap()).unwrap();
        file_json["updated_ms"] = serde_json::json!(1_000);
        fs::write(&scope_path, serde_json::to_vec(&file_json).unwrap()).unwrap();
        let _ = fs::remove_dir_all(mount.path().join("project"));
        let report = reconcile_delegation_ledger(&mux, 999_999_999_999).unwrap();
        assert!(report.removed_stale_scopes.contains(&scope.scope_id));
        assert!(mux.lookup(&scope.scope_id).is_err());
        let lifecycle = lifecycle_entry_for(&scope.scope_id).expect("lifecycle entry");
        assert_eq!(
            lifecycle.terminal_state,
            Some(DelegationTerminalState::Failed)
        );
    }

    fn retained_scope_with_lease(
        ttl_ms: u64,
    ) -> (
        tempfile::TempDir,
        tempfile::TempDir,
        WorkspaceMux,
        WorkspaceScope,
    ) {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                BrowserBinding::new("default", BrowserDriverKind::Orca, "legacy", "page-a"),
            )
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        retain_session_with_lease(&workspace, &scope.scope_id, ttl_ms).unwrap();
        (mount, state, mux, scope)
    }

    #[test]
    fn non_expired_retained_scope_is_not_claimed() {
        let (_mount, _state, mux, scope) = retained_scope_with_lease(60_000);
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        let before_expiry = lifecycle.lease_expires_ms.unwrap().saturating_sub(1);
        assert!(
            claim_expired_retained_scope(&mux, &scope, before_expiry, 60_000)
                .unwrap()
                .is_none()
        );
        assert!(mux.lookup(&scope.scope_id).is_ok());
    }

    #[test]
    fn expired_retained_scope_claim_preserves_binding_until_browser_close_succeeds() {
        let (_mount, _state, mux, scope) = retained_scope_with_lease(60_000);
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        let expiry = lifecycle.lease_expires_ms.unwrap();
        let claimed = claim_expired_retained_scope(&mux, &scope, expiry, 60_000)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.scope_id, scope.scope_id);
        let binding = claimed.browser.unwrap();
        assert_eq!(binding.account_id, "default");
        assert_eq!(binding.instance, "legacy");
        assert_eq!(binding.page_id, "page-a");
        assert!(mux.lookup(&scope.scope_id).is_ok());
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert!(lifecycle.session_retained);
        assert_eq!(lifecycle.lease_expires_ms, Some(expiry));
    }

    #[test]
    fn legacy_retained_scope_without_lease_is_migrated_before_gc() {
        let mount = tempdir().unwrap();
        let project = mount.path().join("project");
        std::fs::create_dir_all(&project).unwrap();
        let state = tempdir().unwrap();
        let mux = WorkspaceMux::new(mount.path(), state.path()).unwrap();
        let scope = mux
            .register_browser(&project, "legacy-page".into())
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Blocked,
            Some("blocked"),
        )
        .unwrap();
        let legacy = mark_session_retained(&workspace, &scope.scope_id, true).unwrap();
        assert!(legacy.lease_expires_ms.is_none());

        assert!(claim_expired_retained_scope(&mux, &scope, 0, 60_000)
            .unwrap()
            .is_none());
        let migrated = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert!(migrated.lease_expires_ms.is_some());
        assert!(mux.lookup(&scope.scope_id).is_ok());
    }

    #[tokio::test]
    async fn cleanup_uses_bound_account_instance_and_reports_missing_account_safely() {
        let root = tempdir().unwrap();
        let mount = root.path().join("mount");
        let bridge = root.path().join("bridge");
        let scopes = root.path().join("scopes");
        let project = mount.join("project");
        fs::create_dir_all(&project).unwrap();
        fs::create_dir_all(&bridge).unwrap();
        let mux = WorkspaceMux::new(&mount, &scopes).unwrap();
        let scope = mux
            .register_browser_binding(
                &project,
                BrowserBinding::new(
                    "removed",
                    BrowserDriverKind::Orca,
                    "instance-a",
                    "same-page",
                ),
            )
            .unwrap();
        let workspace = mux.resolve(&scope.scope_id).unwrap();
        start_fresh_delegation_lifecycle(&workspace, &scope.scope_id).unwrap();
        record_terminal_evidence(
            &workspace,
            &scope.scope_id,
            DelegationTerminalState::Completed,
            Some("done"),
        )
        .unwrap();
        let lifecycle = retain_session_with_lease(&workspace, &scope.scope_id, 1).unwrap();
        let lease_expires_ms = lifecycle.lease_expires_ms;
        let pool = BrowserPool::new(
            &bridge,
            &mount,
            LegacyAccountConfig::default(),
            BrowserDriverConfig::with_driver(
                Some(BrowserDriverKind::Orca),
                Some("orca".into()),
                "active",
                None,
            ),
        );
        let cleaned = cleanup_expired_retained_sessions(
            &mux,
            &pool,
            lifecycle.lease_expires_ms.unwrap(),
            1,
            None,
        )
        .await
        .unwrap();
        assert_eq!(cleaned.len(), 1);
        assert_eq!(cleaned[0].account_id.as_deref(), Some("removed"));
        assert_eq!(cleaned[0].browser_instance.as_deref(), Some("instance-a"));
        assert!(!cleaned[0].page_closed);
        assert!(cleaned[0]
            .close_error
            .as_deref()
            .is_some_and(|error| error.contains("BROWSER_ACCOUNT_UNAVAILABLE")));
        assert!(cleaned[0].scope_removed);
        assert!(mux.lookup(&scope.scope_id).is_err());
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert!(lifecycle.session_retained);
        assert_eq!(lifecycle.lease_expires_ms, lease_expires_ms);
    }
}
