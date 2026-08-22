use crate::account_state::{AccountHealth, AccountRuntimeState};
use crate::accounts::AccountConfig;
use crate::browser_pool::{BrowserHealth, BrowserLoginState, BrowserPool, BrowserReachability};
use crate::orca::BrowserDriverKind;
use crate::router::AccountRouter;
use crate::security::WorkspaceMux;
use crate::tools::task_state::load_delegation_lifecycle;
use anyhow::{Context, Result};
use futures::future::join_all;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

pub const ACCOUNT_DIAGNOSTICS_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountRoutingState {
    Ready,
    Disabled,
    Draining,
    AuthenticationRequired,
    Degraded,
    Cooldown,
    DispatchWindowFull,
    ActiveWorkersFull,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AccountDiagnostic {
    pub account_id: String,
    pub enabled: bool,
    pub draining: bool,
    pub routing_state: AccountRoutingState,
    pub scheduler_health: AccountHealth,
    pub active_workers: usize,
    pub reserved_workers: usize,
    pub dispatches_in_window: usize,
    pub reserved_dispatches: usize,
    pub max_active_workers: usize,
    pub max_dispatches: usize,
    pub window_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_slot_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_until_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_reason: Option<String>,
    pub browser_instance: String,
    pub browser_reachability: BrowserReachability,
    pub browser_login_state: BrowserLoginState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AccountDiagnosticsReport {
    pub version: u32,
    pub generated_ms: u64,
    pub accounts: Vec<AccountDiagnostic>,
}

#[derive(Default)]
struct LiveWorkers {
    counts: HashMap<String, usize>,
    scope_ids: HashSet<String>,
}

pub async fn recover_stale_account_health(
    router: &AccountRouter,
    browsers: &BrowserPool,
    now_ms: u64,
) -> Result<usize> {
    let config = router
        .load_config()
        .context("failed to load account routing configuration")?;
    let mut candidates = Vec::new();
    for account in &config.accounts {
        let state = router
            .state_for_account(&account.id, now_ms)
            .with_context(|| format!("failed to inspect account health for '{}'", account.id))?;
        if state.health != AccountHealth::Ready {
            candidates.push(account);
        }
    }
    let health = join_all(
        candidates
            .iter()
            .map(|account| browsers.health(&account.id)),
    )
    .await;
    let mut recovered = 0usize;
    for (account, browser) in candidates.iter().zip(health.iter()) {
        match browser.login_state {
            BrowserLoginState::Ready => {
                router
                    .mark_ready(&account.id, now_ms)
                    .with_context(|| format!("failed to mark account '{}' ready", account.id))?;
                recovered += 1;
            }
            BrowserLoginState::Unknown if can_recover_legacy_cmux_auth(browser) => {
                // The legacy cmux driver can prove its browser is reachable but cannot enumerate
                // an existing ChatGPT page to distinguish a logged-in session from an old auth
                // failure. Clear only the stale scheduler block here; staging still performs the
                // authoritative composer probe before sending any user task.
                router.mark_ready(&account.id, now_ms).with_context(|| {
                    format!("failed to recover legacy account '{}'", account.id)
                })?;
                recovered += 1;
            }
            BrowserLoginState::AuthenticationRequired => {
                router
                    .mark_auth_required(&account.id, now_ms)
                    .with_context(|| {
                        format!(
                            "failed to preserve auth-required state for '{}'",
                            account.id
                        )
                    })?;
            }
            BrowserLoginState::Unknown => {}
        }
    }
    Ok(recovered)
}

fn can_recover_legacy_cmux_auth(browser: &BrowserHealth) -> bool {
    browser.login_state == BrowserLoginState::Unknown
        && browser.reachability == BrowserReachability::Reachable
        && browser.driver == Some(BrowserDriverKind::Cmux)
}

pub async fn collect_account_diagnostics(
    router: &AccountRouter,
    browsers: &BrowserPool,
    mux: &WorkspaceMux,
    now_ms: u64,
) -> Result<AccountDiagnosticsReport> {
    let config = router
        .load_config()
        .context("failed to load account routing configuration")?;
    let live = count_live_workers(mux)?;

    let states = config
        .accounts
        .iter()
        .map(|account| {
            router
                .state_for_account(&account.id, now_ms)
                .with_context(|| format!("failed to load account state for '{}'", account.id))
        })
        .collect::<Result<Vec<_>>>()?;
    let health = join_all(
        config
            .accounts
            .iter()
            .map(|account| browsers.health(&account.id)),
    )
    .await;

    let accounts = config
        .accounts
        .iter()
        .zip(states.iter())
        .zip(health.iter())
        .map(|((account, state), browser)| {
            build_account_diagnostic(account, state, browser, &live, now_ms)
        })
        .collect();

    Ok(AccountDiagnosticsReport {
        version: ACCOUNT_DIAGNOSTICS_VERSION,
        generated_ms: now_ms,
        accounts,
    })
}

fn count_live_workers(mux: &WorkspaceMux) -> Result<LiveWorkers> {
    let mut live = LiveWorkers::default();
    for scope in mux.list_scopes()? {
        let workspace = mux.resolve(&scope.scope_id)?;
        let lifecycle =
            load_delegation_lifecycle(&workspace, &scope.scope_id).map_err(anyhow::Error::msg)?;
        if lifecycle
            .as_ref()
            .is_none_or(|lifecycle| lifecycle.terminal_state.is_some())
        {
            continue;
        }

        let count_as_live = match mux.try_lock_scope(&scope.scope_id) {
            Ok(None) => true,
            Ok(Some(_lock)) => false,
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "diagnostic scope-lock probe failed; counting worker defensively"
                );
                true
            }
        };
        if count_as_live {
            live.scope_ids.insert(scope.scope_id.clone());
            *live
                .counts
                .entry(scope.account_id().to_string())
                .or_insert(0) += 1;
        }
    }
    Ok(live)
}

fn build_account_diagnostic(
    account: &AccountConfig,
    state: &AccountRuntimeState,
    browser: &BrowserHealth,
    live: &LiveWorkers,
    now_ms: u64,
) -> AccountDiagnostic {
    let active_workers = live.counts.get(&account.id).copied().unwrap_or(0);
    let reserved_workers = state
        .reservations
        .iter()
        .filter(|reservation| {
            reservation
                .scope_id
                .as_ref()
                .is_none_or(|scope_id| !live.scope_ids.contains(scope_id))
        })
        .count();
    let reserved_dispatches = state.reservations.len();
    let dispatches_in_window = state.window_used();

    let active_full =
        active_workers.saturating_add(reserved_workers) >= account.limits.max_active_workers;
    let window_full =
        dispatches_in_window.saturating_add(reserved_dispatches) >= account.limits.max_dispatches;
    let cooldown = state.cooldown_until_ms.filter(|until| *until > now_ms);

    let routing_state = if !account.enabled {
        AccountRoutingState::Disabled
    } else if account.draining {
        AccountRoutingState::Draining
    } else if state.health == AccountHealth::AuthRequired {
        AccountRoutingState::AuthenticationRequired
    } else if state.health == AccountHealth::Degraded {
        AccountRoutingState::Degraded
    } else if cooldown.is_some() {
        AccountRoutingState::Cooldown
    } else if window_full {
        AccountRoutingState::DispatchWindowFull
    } else if active_full {
        AccountRoutingState::ActiveWorkersFull
    } else {
        AccountRoutingState::Ready
    };

    let next_slot_ms = next_slot_ms(
        account,
        state,
        active_workers,
        reserved_workers,
        now_ms,
        routing_state,
    );

    AccountDiagnostic {
        account_id: account.id.clone(),
        enabled: account.enabled,
        draining: account.draining,
        routing_state,
        scheduler_health: state.health,
        active_workers,
        reserved_workers,
        dispatches_in_window,
        reserved_dispatches,
        max_active_workers: account.limits.max_active_workers,
        max_dispatches: account.limits.max_dispatches,
        window_seconds: account.limits.window_seconds,
        next_slot_ms,
        cooldown_until_ms: cooldown,
        cooldown_reason: cooldown.and(state.cooldown_reason.clone()),
        browser_instance: browser.instance.clone(),
        browser_reachability: browser.reachability,
        browser_login_state: browser.login_state,
    }
}

fn next_slot_ms(
    account: &AccountConfig,
    state: &AccountRuntimeState,
    active_workers: usize,
    reserved_workers: usize,
    now_ms: u64,
    routing_state: AccountRoutingState,
) -> Option<u64> {
    if matches!(
        routing_state,
        AccountRoutingState::Disabled
            | AccountRoutingState::Draining
            | AccountRoutingState::AuthenticationRequired
            | AccountRoutingState::Degraded
    ) {
        return None;
    }

    let mut gate_times = Vec::new();
    if let Some(until) = state.cooldown_until_ms.filter(|until| *until > now_ms) {
        gate_times.push(until);
    }

    if state.window_used().saturating_add(state.reservations.len()) >= account.limits.max_dispatches
    {
        let dispatch_release = state
            .dispatches_ms
            .iter()
            .map(|timestamp| timestamp.saturating_add(account.limits.window_ms()))
            .min();
        let reservation_release = state
            .reservations
            .iter()
            .map(|reservation| reservation.expires_ms)
            .min();
        let release = match (dispatch_release, reservation_release) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(value), None) | (None, Some(value)) => Some(value),
            (None, None) => None,
        }?;
        gate_times.push(release);
    }

    if active_workers.saturating_add(reserved_workers) >= account.limits.max_active_workers {
        // A live worker has no predictable completion time. We can only publish a next slot when
        // expiring *unbound* reservations alone are sufficient to free capacity.
        if active_workers >= account.limits.max_active_workers {
            return None;
        }
        let needed = active_workers
            .saturating_add(reserved_workers)
            .saturating_sub(account.limits.max_active_workers)
            .saturating_add(1);
        let mut expiries = state
            .reservations
            .iter()
            .filter(|reservation| reservation.scope_id.is_none())
            .map(|reservation| reservation.expires_ms)
            .collect::<Vec<_>>();
        expiries.sort_unstable();
        let release = expiries.get(needed.saturating_sub(1)).copied()?;
        gate_times.push(release);
    }

    gate_times.into_iter().max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_state::AccountReservation;
    use crate::accounts::{AccountLimits, BrowserInstanceConfig};
    use crate::orca::BrowserDriverKind;

    fn account() -> AccountConfig {
        AccountConfig {
            id: "alpha".into(),
            enabled: true,
            draining: false,
            limits: AccountLimits {
                window_seconds: 10,
                max_dispatches: 2,
                max_active_workers: 2,
            },
            browser: BrowserInstanceConfig {
                driver: Some(BrowserDriverKind::Orca),
                instance: "alpha-browser".into(),
                user_data_dir: None,
                cdp_endpoint: None,
                worktree: "active".into(),
            },
        }
    }

    fn browser() -> BrowserHealth {
        BrowserHealth {
            account_id: "alpha".into(),
            instance: "alpha-browser".into(),
            driver: Some(BrowserDriverKind::Orca),
            reachability: BrowserReachability::Reachable,
            login_state: BrowserLoginState::Ready,
            login_required: false,
            detail: Some("this field must not enter diagnostics".into()),
        }
    }

    #[test]
    fn only_reachable_unknown_cmux_health_recovers_stale_auth() {
        let cmux_unknown = BrowserHealth {
            driver: Some(BrowserDriverKind::Cmux),
            login_state: BrowserLoginState::Unknown,
            ..browser()
        };
        assert!(can_recover_legacy_cmux_auth(&cmux_unknown));

        let cmux_unreachable = BrowserHealth {
            reachability: BrowserReachability::Unreachable,
            ..cmux_unknown.clone()
        };
        assert!(!can_recover_legacy_cmux_auth(&cmux_unreachable));

        let explicit_auth_required = BrowserHealth {
            login_state: BrowserLoginState::AuthenticationRequired,
            ..cmux_unknown
        };
        assert!(!can_recover_legacy_cmux_auth(&explicit_auth_required));

        let orca_unknown = BrowserHealth {
            login_state: BrowserLoginState::Unknown,
            ..browser()
        };
        assert!(!can_recover_legacy_cmux_auth(&orca_unknown));
    }

    #[test]
    fn report_contains_capacity_without_scope_page_or_secret_material() {
        let mut state = AccountRuntimeState::new("alpha");
        state.dispatches_ms.push(1_000);
        state.reservations.push(AccountReservation {
            reservation_id: "reservation-secret".into(),
            scope_id: Some("scope-secret".into()),
            created_ms: 2_000,
            expires_ms: 5_000,
        });
        let mut live = LiveWorkers::default();
        live.counts.insert("alpha".into(), 1);
        live.scope_ids.insert("scope-secret".into());

        let diagnostic = build_account_diagnostic(&account(), &state, &browser(), &live, 2_500);
        assert_eq!(diagnostic.active_workers, 1);
        assert_eq!(diagnostic.reserved_workers, 0);
        assert_eq!(diagnostic.reserved_dispatches, 1);
        let json = serde_json::to_string(&diagnostic).unwrap();
        for forbidden in [
            "scope_id",
            "scope-secret",
            "page_id",
            "cdp_endpoint",
            "user_data_dir",
            "reservation-secret",
            "cookie",
            "email",
            "token",
            "this field must not enter diagnostics",
        ] {
            assert!(!json.contains(forbidden), "diagnostics leaked {forbidden}");
        }
    }

    #[test]
    fn next_slot_is_maximum_of_current_blocking_gate_release_times() {
        let mut state = AccountRuntimeState::new("alpha");
        state.dispatches_ms.extend([1_000, 2_000]);
        state.cooldown_until_ms = Some(15_000);
        state.cooldown_reason = Some("capacity".into());
        let diagnostic = build_account_diagnostic(
            &account(),
            &state,
            &browser(),
            &LiveWorkers::default(),
            5_000,
        );
        assert_eq!(diagnostic.routing_state, AccountRoutingState::Cooldown);
        // Window first frees at 11s, but cooldown is the later blocking gate.
        assert_eq!(diagnostic.next_slot_ms, Some(15_000));
    }

    #[test]
    fn active_live_worker_saturation_has_no_invented_completion_time() {
        let mut live = LiveWorkers::default();
        live.counts.insert("alpha".into(), 2);
        let diagnostic = build_account_diagnostic(
            &account(),
            &AccountRuntimeState::new("alpha"),
            &browser(),
            &live,
            1_000,
        );
        assert_eq!(
            diagnostic.routing_state,
            AccountRoutingState::ActiveWorkersFull
        );
        assert_eq!(diagnostic.next_slot_ms, None);
    }
}
