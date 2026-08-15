use crate::orca::{close_browser_page, OrcaConfig};
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
    pub scope_removed: bool,
    pub page_closed: bool,
    pub close_error: Option<String>,
}

#[derive(Clone, Debug)]
struct ClaimedExpiredSession {
    scope_id: String,
    browser_page_id: Option<String>,
}

pub async fn cleanup_expired_retained_sessions(
    mux: &WorkspaceMux,
    orca: &OrcaConfig,
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

        let close_result = match claimed.browser_page_id.as_deref() {
            Some(page) => close_browser_page(orca, page).await,
            None => Ok(()),
        };
        cleaned.push(ExpiredSessionCleanup {
            scope_id: claimed.scope_id,
            browser_page_id: claimed.browser_page_id,
            scope_removed: true,
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
    let Some(_lock) = mux.try_lock_scope(&listed_scope.scope_id)? else {
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

    release_session_retention(&workspace, &scope.scope_id).map_err(crate::BridgeError::Path)?;
    mux.remove(&scope.scope_id)?;
    Ok(Some(ClaimedExpiredSession {
        scope_id: scope.scope_id,
        browser_page_id: scope.browser_page_id,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::task_state::{
        load_delegation_lifecycle, mark_session_retained, record_terminal_evidence,
        start_fresh_delegation_lifecycle, DelegationTerminalState,
    };
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
        let scope = mux.register_browser(&project, "page-a".into()).unwrap();
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
    fn expired_retained_scope_is_atomically_released_and_removed() {
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
        assert_eq!(claimed.browser_page_id.as_deref(), Some("page-a"));
        assert!(mux.lookup(&scope.scope_id).is_err());
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .unwrap()
            .unwrap();
        assert!(!lifecycle.session_retained);
        assert!(lifecycle.lease_expires_ms.is_none());
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
}
