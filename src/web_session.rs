use crate::browser_pool::BrowserPool;
use crate::security::BrowserBinding;
use crate::tools::task_state::{
    load_delegation_lifecycle, release_session_retention, retain_session_with_lease,
    retained_session_expired,
};
use crate::{Result, WorkspaceMux, WorkspaceScope};
use serde::Serialize;

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
        let Some(claimed) = claim_expired_retained_scope(mux, &scope, now_ms, legacy_ttl_ms)?
        else {
            continue;
        };

        let close_result = match claimed.browser.as_ref() {
            Some(binding) => browsers.close(binding).await,
            None => Ok(()),
        };
        let scope_removed = if close_result.is_ok() {
            let workspace = mux.resolve(&claimed.scope_id)?;
            release_session_retention(&workspace, &claimed.scope_id)
                .map_err(crate::BridgeError::Path)?;
            mux.remove(&claimed.scope_id)?;
            true
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
            page_closed: close_result.is_ok(),
            close_error: close_result.err().map(|error| error.to_string()),
        });
    }

    Ok(cleaned)
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
        assert!(!cleaned[0].scope_removed);
        assert!(mux.lookup(&scope.scope_id).is_ok());
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert!(lifecycle.session_retained);
    }
}
