use crate::account_state::{
    AccountActivationLock, AccountHealth, AccountReservation, AccountRuntimeState,
    AccountStateStore, RouterRuntimeState,
};
use crate::accounts::{
    load_accounts_config, AccountConfig, AccountsConfig, LegacyAccountConfig, RoutingStrategy,
};
use crate::error::BridgeError;
use crate::security::WorkspaceMux;
use crate::tools::task_state::load_delegation_lifecycle;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteReservation {
    pub reservation_id: String,
    pub account: AccountConfig,
    pub created_ms: u64,
    pub expires_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AccountExhaustion {
    pub account_id: String,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RoutingExhausted {
    pub now_ms: u64,
    pub accounts: Vec<AccountExhaustion>,
}

impl fmt::Display for RoutingExhausted {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let encoded = serde_json::to_string(self).unwrap_or_else(|_| "{}".to_string());
        write!(formatter, "no eligible ChatGPT Web account: {encoded}")
    }
}

#[derive(thiserror::Error, Debug)]
pub enum RouterError {
    #[error(transparent)]
    State(#[from] BridgeError),
    #[error("{0}")]
    Exhausted(RoutingExhausted),
    #[error("configured account '{0}' is unavailable for retained-scope affinity")]
    AccountUnavailable(String),
}

pub type RouterResult<T> = std::result::Result<T, RouterError>;

#[derive(Clone, Debug)]
pub struct AccountRouter {
    bridge_dir: PathBuf,
    mount_root: PathBuf,
    legacy: LegacyAccountConfig,
    store: AccountStateStore,
}

impl AccountRouter {
    pub fn new(
        bridge_dir: impl AsRef<Path>,
        mount_root: impl AsRef<Path>,
        legacy: LegacyAccountConfig,
    ) -> Self {
        let bridge_dir = bridge_dir.as_ref().to_path_buf();
        Self {
            store: AccountStateStore::new(&bridge_dir),
            bridge_dir,
            mount_root: mount_root.as_ref().to_path_buf(),
            legacy,
        }
    }

    pub fn load_config(&self) -> RouterResult<AccountsConfig> {
        Ok(load_accounts_config(
            &self.bridge_dir,
            &self.mount_root,
            self.legacy.clone(),
        )?)
    }

    pub fn lock_account_activation(&self) -> RouterResult<AccountActivationLock> {
        Ok(self.store.lock_account_activation()?)
    }

    pub fn reserve_batch_for_mux(
        &self,
        mux: &WorkspaceMux,
        count: usize,
        now_ms: u64,
    ) -> RouterResult<Vec<RouteReservation>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _lock = self.store.lock()?;
        let active_workers = count_active_workers_by_account(mux)?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let mut router_state = self.store.load_router()?;
        let mut states = self.load_reconciled_states(&config, now_ms)?;
        let mut reservations = Vec::with_capacity(count);

        for _ in 0..count {
            let selected = select_account(
                &config,
                &states,
                &active_workers.counts,
                Some(&active_workers.scope_ids),
                &mut router_state,
                AccountSelection::fresh(now_ms),
            )
            .map_err(RouterError::Exhausted)?;
            let account = config.accounts[selected].clone();
            let expires_ms =
                now_ms.saturating_add(config.routing.reservation_ttl_seconds.saturating_mul(1000));
            let reservation_id = uuid::Uuid::new_v4().to_string();
            let state = states
                .get_mut(&account.id)
                .expect("selected account state must exist");
            state.reservations.push(AccountReservation {
                reservation_id: reservation_id.clone(),
                scope_id: None,
                created_ms: now_ms,
                expires_ms,
            });
            state.last_selected_ms = Some(now_ms);
            reservations.push(RouteReservation {
                reservation_id,
                account,
                created_ms: now_ms,
                expires_ms,
            });
        }

        router_state.updated_ms = now_ms;
        self.persist_states(&states)?;
        self.store.save_router(&router_state)?;
        Ok(reservations)
    }

    pub fn reserve_for_account_for_mux(
        &self,
        mux: &WorkspaceMux,
        account_id: &str,
        now_ms: u64,
    ) -> RouterResult<RouteReservation> {
        let _lock = self.store.lock()?;
        let active_workers = count_active_workers_by_account(mux)?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account_index = config
            .accounts
            .iter()
            .position(|account| account.id == account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut router_state = self.store.load_router()?;
        let mut states = self.load_reconciled_states(&config, now_ms)?;
        let selected = select_account(
            &config,
            &states,
            &active_workers.counts,
            Some(&active_workers.scope_ids),
            &mut router_state,
            AccountSelection::retained(now_ms, account_index),
        )
        .map_err(RouterError::Exhausted)?;
        debug_assert_eq!(selected, account_index);
        let account = config.accounts[account_index].clone();
        let expires_ms =
            now_ms.saturating_add(config.routing.reservation_ttl_seconds.saturating_mul(1000));
        let reservation_id = uuid::Uuid::new_v4().to_string();
        let state = states
            .get_mut(account_id)
            .expect("affine account state must exist");
        state.reservations.push(AccountReservation {
            reservation_id: reservation_id.clone(),
            scope_id: None,
            created_ms: now_ms,
            expires_ms,
        });
        state.last_selected_ms = Some(now_ms);
        router_state.updated_ms = now_ms;
        self.persist_states(&states)?;
        self.store.save_router(&router_state)?;
        Ok(RouteReservation {
            reservation_id,
            account,
            created_ms: now_ms,
            expires_ms,
        })
    }

    pub fn reserve_one(
        &self,
        active_workers: &HashMap<String, usize>,
        now_ms: u64,
    ) -> RouterResult<RouteReservation> {
        self.reserve_batch(active_workers, 1, now_ms)
            .map(|mut reservations| reservations.remove(0))
    }

    pub fn reserve_batch(
        &self,
        active_workers: &HashMap<String, usize>,
        count: usize,
        now_ms: u64,
    ) -> RouterResult<Vec<RouteReservation>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let mut router_state = self.store.load_router()?;
        let mut states = self.load_reconciled_states(&config, now_ms)?;
        let mut reservations = Vec::with_capacity(count);

        for _ in 0..count {
            let selected = match select_account(
                &config,
                &states,
                active_workers,
                None,
                &mut router_state,
                AccountSelection::fresh(now_ms),
            ) {
                Ok(selected) => selected,
                Err(exhausted) => return Err(RouterError::Exhausted(exhausted)),
            };
            let account = config.accounts[selected].clone();
            let ttl_ms = config.routing.reservation_ttl_seconds.saturating_mul(1000);
            let expires_ms = now_ms.saturating_add(ttl_ms);
            let reservation_id = uuid::Uuid::new_v4().to_string();
            let state = states
                .get_mut(&account.id)
                .expect("selected account state must exist");
            state.reservations.push(AccountReservation {
                reservation_id: reservation_id.clone(),
                scope_id: None,
                created_ms: now_ms,
                expires_ms,
            });
            state.last_selected_ms = Some(now_ms);
            reservations.push(RouteReservation {
                reservation_id,
                account,
                created_ms: now_ms,
                expires_ms,
            });
        }

        router_state.updated_ms = now_ms;
        self.persist_states(&states)?;
        self.store.save_router(&router_state)?;
        Ok(reservations)
    }

    pub fn reserve_for_account(
        &self,
        account_id: &str,
        active_workers: &HashMap<String, usize>,
        now_ms: u64,
    ) -> RouterResult<RouteReservation> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account_index = config
            .accounts
            .iter()
            .position(|account| account.id == account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut router_state = self.store.load_router()?;
        let mut states = self.load_reconciled_states(&config, now_ms)?;
        let selected = select_account(
            &config,
            &states,
            active_workers,
            None,
            &mut router_state,
            AccountSelection::retained(now_ms, account_index),
        )
        .map_err(RouterError::Exhausted)?;
        debug_assert_eq!(selected, account_index);
        let account = config.accounts[account_index].clone();
        let expires_ms =
            now_ms.saturating_add(config.routing.reservation_ttl_seconds.saturating_mul(1000));
        let reservation_id = uuid::Uuid::new_v4().to_string();
        let state = states
            .get_mut(account_id)
            .expect("affine account state must exist");
        state.reservations.push(AccountReservation {
            reservation_id: reservation_id.clone(),
            scope_id: None,
            created_ms: now_ms,
            expires_ms,
        });
        state.last_selected_ms = Some(now_ms);
        router_state.updated_ms = now_ms;
        self.persist_states(&states)?;
        self.store.save_router(&router_state)?;
        Ok(RouteReservation {
            reservation_id,
            account,
            created_ms: now_ms,
            expires_ms,
        })
    }

    pub fn bind_scope(
        &self,
        reservation: &RouteReservation,
        scope_id: &str,
        now_ms: u64,
    ) -> RouterResult<()> {
        if uuid::Uuid::parse_str(scope_id).is_err() {
            return Err(RouterError::State(BridgeError::Security(
                "invalid scope id for account reservation binding".into(),
            )));
        }
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(&reservation.account.id)
            .ok_or_else(|| RouterError::AccountUnavailable(reservation.account.id.clone()))?;
        let mut state = self.store.load_account(&account.id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        let pending = state
            .reservations
            .iter_mut()
            .find(|pending| pending.reservation_id == reservation.reservation_id)
            .ok_or_else(|| {
                RouterError::State(BridgeError::Precondition(format!(
                    "reservation {} for account {} is missing or expired",
                    reservation.reservation_id, account.id
                )))
            })?;
        if pending
            .scope_id
            .as_ref()
            .is_some_and(|bound| bound != scope_id)
        {
            return Err(RouterError::State(BridgeError::Precondition(format!(
                "reservation {} for account {} is already bound to another scope",
                reservation.reservation_id, account.id
            ))));
        }
        pending.scope_id = Some(scope_id.to_string());
        self.store.save_account(&state)?;
        Ok(())
    }

    pub fn commit(&self, reservation: &RouteReservation, dispatch_ms: u64) -> RouterResult<()> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(&reservation.account.id)
            .ok_or_else(|| RouterError::AccountUnavailable(reservation.account.id.clone()))?;
        let mut state = self.store.load_account(&account.id)?;
        state.reconcile(dispatch_ms, account.limits.window_ms());
        if state
            .remove_reservation(&reservation.reservation_id)
            .is_none()
        {
            return Err(RouterError::State(BridgeError::Precondition(format!(
                "reservation {} for account '{}' is missing or expired",
                reservation.reservation_id, account.id
            ))));
        }
        state.dispatches_ms.push(dispatch_ms);
        self.store.save_account(&state)?;
        Ok(())
    }

    pub fn release(&self, reservation: &RouteReservation, now_ms: u64) -> RouterResult<()> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let Some(account) = config.account(&reservation.account.id) else {
            return Ok(());
        };
        let mut state = self.store.load_account(&account.id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        state.remove_reservation(&reservation.reservation_id);
        self.store.save_account(&state)?;
        Ok(())
    }

    pub fn apply_rate_limit(
        &self,
        account_id: &str,
        reason: &str,
        reset_after_seconds: Option<u64>,
        now_ms: u64,
    ) -> RouterResult<u64> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut state = self.store.load_account(account_id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        let fallback_seconds = if reason == "capacity" {
            config.routing.selection_failure_backoff_seconds
        } else {
            config.defaults.cooldown.unknown_rate_limit_seconds
        };
        let cooldown_seconds = reset_after_seconds.unwrap_or(fallback_seconds).max(1);
        let until = now_ms.saturating_add(cooldown_seconds.saturating_mul(1000));
        state.cooldown_until_ms = Some(until);
        state.cooldown_reason = Some(reason.to_string());
        self.store.save_account(&state)?;
        Ok(until)
    }

    pub fn apply_delivery_failure(&self, account_id: &str, now_ms: u64) -> RouterResult<u64> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut state = self.store.load_account(account_id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        let until = now_ms.saturating_add(
            config
                .defaults
                .cooldown
                .delivery_failure_seconds
                .saturating_mul(1000),
        );
        state.cooldown_until_ms = Some(until);
        state.cooldown_reason = Some("delivery_failure".to_string());
        self.store.save_account(&state)?;
        Ok(until)
    }

    pub fn mark_auth_required(&self, account_id: &str, now_ms: u64) -> RouterResult<()> {
        self.set_health(account_id, AccountHealth::AuthRequired, now_ms)
    }

    pub fn mark_ready(&self, account_id: &str, now_ms: u64) -> RouterResult<()> {
        self.set_health(account_id, AccountHealth::Ready, now_ms)
    }

    pub fn state_for_account(
        &self,
        account_id: &str,
        now_ms: u64,
    ) -> RouterResult<AccountRuntimeState> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut state = self.store.load_account(account_id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        Ok(state)
    }

    fn set_health(&self, account_id: &str, health: AccountHealth, now_ms: u64) -> RouterResult<()> {
        let _lock = self.store.lock()?;
        let config = load_accounts_config(&self.bridge_dir, &self.mount_root, self.legacy.clone())?;
        let account = config
            .account(account_id)
            .ok_or_else(|| RouterError::AccountUnavailable(account_id.to_string()))?;
        let mut state = self.store.load_account(account_id)?;
        state.reconcile(now_ms, account.limits.window_ms());
        state.health = health;
        self.store.save_account(&state)?;
        Ok(())
    }

    fn load_reconciled_states(
        &self,
        config: &AccountsConfig,
        now_ms: u64,
    ) -> RouterResult<HashMap<String, AccountRuntimeState>> {
        let mut states = HashMap::with_capacity(config.accounts.len());
        for account in &config.accounts {
            let mut state = self.store.load_account(&account.id)?;
            state.reconcile(now_ms, account.limits.window_ms());
            states.insert(account.id.clone(), state);
        }
        Ok(states)
    }

    fn persist_states(&self, states: &HashMap<String, AccountRuntimeState>) -> RouterResult<()> {
        let mut account_ids = states.keys().cloned().collect::<Vec<_>>();
        account_ids.sort();
        for account_id in account_ids {
            self.store
                .save_account(states.get(&account_id).expect("known account state"))?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct LiveWorkers {
    counts: HashMap<String, usize>,
    scope_ids: HashSet<String>,
}

fn count_active_workers_by_account(mux: &WorkspaceMux) -> RouterResult<LiveWorkers> {
    let mut active = LiveWorkers::default();
    for scope in mux.list_scopes()? {
        let workspace = mux.resolve(&scope.scope_id)?;
        let lifecycle = load_delegation_lifecycle(&workspace, &scope.scope_id)
            .map_err(|error| RouterError::State(BridgeError::Precondition(error)))?;
        if lifecycle
            .as_ref()
            .is_none_or(|lifecycle| lifecycle.terminal_state.is_some())
        {
            continue;
        }
        match mux.try_lock_scope(&scope.scope_id) {
            Ok(None) => {
                active.scope_ids.insert(scope.scope_id.clone());
                *active
                    .counts
                    .entry(scope.account_id().to_string())
                    .or_insert(0) += 1;
            }
            Ok(Some(_lock)) => {
                tracing::debug!(
                    scope_id = %scope.scope_id,
                    "ghost scope excluded from account active-worker count"
                );
            }
            Err(error) => {
                tracing::warn!(
                    scope_id = %scope.scope_id,
                    error = %error,
                    "scope lock probe failed; counting account worker defensively"
                );
                active.scope_ids.insert(scope.scope_id.clone());
                *active
                    .counts
                    .entry(scope.account_id().to_string())
                    .or_insert(0) += 1;
            }
        }
    }
    Ok(active)
}

#[derive(Clone, Copy)]
struct AccountSelection {
    now_ms: u64,
    allow_disabled: bool,
    forced_index: Option<usize>,
}

impl AccountSelection {
    const fn fresh(now_ms: u64) -> Self {
        Self {
            now_ms,
            allow_disabled: false,
            forced_index: None,
        }
    }

    const fn retained(now_ms: u64, account_index: usize) -> Self {
        Self {
            now_ms,
            allow_disabled: true,
            forced_index: Some(account_index),
        }
    }
}

fn select_account(
    config: &AccountsConfig,
    states: &HashMap<String, AccountRuntimeState>,
    active_workers: &HashMap<String, usize>,
    active_scope_ids: Option<&HashSet<String>>,
    router_state: &mut RouterRuntimeState,
    selection: AccountSelection,
) -> std::result::Result<usize, RoutingExhausted> {
    let candidate_indices = if let Some(index) = selection.forced_index {
        vec![index]
    } else {
        (0..config.accounts.len()).collect::<Vec<_>>()
    };
    let eligible = candidate_indices
        .iter()
        .copied()
        .filter(|index| {
            let account = &config.accounts[*index];
            let state = states
                .get(&account.id)
                .expect("configured account state must exist");
            eligibility_reason(
                account,
                state,
                active_workers.get(&account.id).copied().unwrap_or(0),
                active_scope_ids,
                selection.now_ms,
                selection.allow_disabled,
            )
            .is_none()
        })
        .collect::<Vec<_>>();

    if eligible.is_empty() {
        return Err(build_exhaustion(
            config,
            states,
            active_workers,
            active_scope_ids,
            selection.now_ms,
            selection.allow_disabled,
            selection.forced_index,
        ));
    }

    if selection.forced_index.is_some() {
        return Ok(eligible[0]);
    }

    match config.routing.strategy {
        RoutingStrategy::RoundRobin => {
            let count = config.accounts.len() as u64;
            let start = (router_state.round_robin_cursor % count) as usize;
            for offset in 0..config.accounts.len() {
                let index = (start + offset) % config.accounts.len();
                if eligible.contains(&index) {
                    router_state.round_robin_cursor = router_state
                        .round_robin_cursor
                        .saturating_add(offset as u64 + 1);
                    return Ok(index);
                }
            }
            unreachable!("eligible round-robin account must be found")
        }
        RoutingStrategy::LeastLoaded => eligible
            .into_iter()
            .min_by(|left, right| {
                compare_load(
                    &config.accounts[*left],
                    states
                        .get(&config.accounts[*left].id)
                        .expect("left account state"),
                    active_workers
                        .get(&config.accounts[*left].id)
                        .copied()
                        .unwrap_or(0),
                    &config.accounts[*right],
                    states
                        .get(&config.accounts[*right].id)
                        .expect("right account state"),
                    active_workers
                        .get(&config.accounts[*right].id)
                        .copied()
                        .unwrap_or(0),
                    active_scope_ids,
                )
            })
            .ok_or_else(|| {
                build_exhaustion(
                    config,
                    states,
                    active_workers,
                    active_scope_ids,
                    selection.now_ms,
                    selection.allow_disabled,
                    selection.forced_index,
                )
            }),
    }
}

fn eligibility_reason(
    account: &AccountConfig,
    state: &AccountRuntimeState,
    active_workers: usize,
    active_scope_ids: Option<&HashSet<String>>,
    now_ms: u64,
    allow_disabled: bool,
) -> Option<AccountExhaustion> {
    if !allow_disabled && !account.enabled {
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: "disabled".into(),
            retry_at_ms: None,
        });
    }
    if !allow_disabled && account.draining {
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: "draining".into(),
            retry_at_ms: None,
        });
    }
    if state.health != AccountHealth::Ready {
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: match state.health {
                AccountHealth::Ready => unreachable!(),
                AccountHealth::AuthRequired => "auth_required",
                AccountHealth::Degraded => "degraded",
            }
            .into(),
            retry_at_ms: None,
        });
    }
    if state.cooldown_until_ms.is_some_and(|until| until > now_ms) {
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: state
                .cooldown_reason
                .clone()
                .unwrap_or_else(|| "cooldown".into()),
            retry_at_ms: state.cooldown_until_ms,
        });
    }
    let reserved_window = state.reserved();
    let reserved_active = reserved_active(state, active_scope_ids);
    if state.window_used().saturating_add(reserved_window) >= account.limits.max_dispatches {
        let dispatch_retry = state.retry_after_window_ms(account.limits.window_ms());
        let reservation_retry = state
            .reservations
            .iter()
            .map(|reservation| reservation.expires_ms)
            .min();
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: "window_exhausted".into(),
            retry_at_ms: min_option(dispatch_retry, reservation_retry),
        });
    }
    if active_workers.saturating_add(reserved_active) >= account.limits.max_active_workers {
        return Some(AccountExhaustion {
            account_id: account.id.clone(),
            reason: "active_workers_exhausted".into(),
            retry_at_ms: state
                .reservations
                .iter()
                .map(|reservation| reservation.expires_ms)
                .min(),
        });
    }
    None
}

fn build_exhaustion(
    config: &AccountsConfig,
    states: &HashMap<String, AccountRuntimeState>,
    active_workers: &HashMap<String, usize>,
    active_scope_ids: Option<&HashSet<String>>,
    now_ms: u64,
    allow_disabled: bool,
    forced_index: Option<usize>,
) -> RoutingExhausted {
    let iter: Box<dyn Iterator<Item = &AccountConfig>> = match forced_index {
        Some(index) => Box::new(std::iter::once(&config.accounts[index])),
        None => Box::new(config.accounts.iter()),
    };
    let accounts = iter
        .filter_map(|account| {
            eligibility_reason(
                account,
                states
                    .get(&account.id)
                    .expect("configured account state must exist"),
                active_workers.get(&account.id).copied().unwrap_or(0),
                active_scope_ids,
                now_ms,
                allow_disabled,
            )
        })
        .collect();
    RoutingExhausted { now_ms, accounts }
}

fn compare_load(
    left_account: &AccountConfig,
    left_state: &AccountRuntimeState,
    left_active: usize,
    right_account: &AccountConfig,
    right_state: &AccountRuntimeState,
    right_active: usize,
    active_scope_ids: Option<&HashSet<String>>,
) -> Ordering {
    let left_reserved = left_state.reserved();
    let right_reserved = right_state.reserved();
    let left_reserved_active = reserved_active(left_state, active_scope_ids);
    let right_reserved_active = reserved_active(right_state, active_scope_ids);
    compare_fraction(
        left_active.saturating_add(left_reserved_active),
        left_account.limits.max_active_workers,
        right_active.saturating_add(right_reserved_active),
        right_account.limits.max_active_workers,
    )
    .then_with(|| {
        compare_fraction(
            left_state.window_used().saturating_add(left_reserved),
            left_account.limits.max_dispatches,
            right_state.window_used().saturating_add(right_reserved),
            right_account.limits.max_dispatches,
        )
    })
    .then_with(|| {
        left_state
            .last_selected_ms
            .cmp(&right_state.last_selected_ms)
    })
    .then_with(|| left_account.id.cmp(&right_account.id))
}

fn reserved_active(
    state: &AccountRuntimeState,
    active_scope_ids: Option<&HashSet<String>>,
) -> usize {
    match active_scope_ids {
        Some(active_scope_ids) => state
            .reservations
            .iter()
            .filter(|reservation| {
                reservation
                    .scope_id
                    .as_ref()
                    .is_none_or(|scope_id| !active_scope_ids.contains(scope_id))
            })
            .count(),
        None => state.reserved(),
    }
}

fn compare_fraction(
    left_num: usize,
    left_den: usize,
    right_num: usize,
    right_den: usize,
) -> Ordering {
    (left_num as u128 * right_den as u128).cmp(&(right_num as u128 * left_den as u128))
}

fn min_option(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accounts::{AccountLimits, BrowserInstanceConfig, CooldownConfig, RoutingConfig};
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    fn setup_router(
        strategy: RoutingStrategy,
        accounts_json: &str,
    ) -> (tempfile::TempDir, AccountRouter) {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let content = accounts_json.replace(
            "__STRATEGY__",
            match strategy {
                RoutingStrategy::RoundRobin => "round_robin",
                RoutingStrategy::LeastLoaded => "least_loaded",
            },
        );
        fs::write(bridge.join("accounts.json"), content).unwrap();
        let router = AccountRouter::new(&bridge, &mount, LegacyAccountConfig::default());
        (root, router)
    }

    fn two_accounts_json() -> &'static str {
        r#"{
          "version":1,
          "routing":{"strategy":"__STRATEGY__","reservation_ttl_seconds":10,"selection_failure_backoff_seconds":5},
          "defaults":{"limits":{"window_seconds":100,"max_dispatches":10,"max_active_workers":3}},
          "accounts":[
            {"id":"a","browser":{"instance":"instance-a"}},
            {"id":"b","browser":{"instance":"instance-b"}}
          ]
        }"#
    }

    #[test]
    fn round_robin_advances_after_selected_account_even_when_skipping() {
        let (_root, router) = setup_router(RoutingStrategy::RoundRobin, two_accounts_json());
        let active = HashMap::new();
        let first = router.reserve_one(&active, 1_000).unwrap();
        let second = router.reserve_one(&active, 1_001).unwrap();
        assert_eq!(first.account.id, "a");
        assert_eq!(second.account.id, "b");
        router.release(&first, 1_002).unwrap();
        router.release(&second, 1_002).unwrap();
        let third = router.reserve_one(&active, 1_003).unwrap();
        assert_eq!(third.account.id, "a");
    }

    #[test]
    fn least_loaded_uses_active_ratio_then_window_ratio() {
        let (_root, router) = setup_router(RoutingStrategy::LeastLoaded, two_accounts_json());
        let mut active = HashMap::new();
        active.insert("a".to_string(), 1);
        active.insert("b".to_string(), 0);
        let selected = router.reserve_one(&active, 1_000).unwrap();
        assert_eq!(selected.account.id, "b");
        router.release(&selected, 1_001).unwrap();

        let _lock = router.store.lock().unwrap();
        let mut a = router.store.load_account("a").unwrap();
        let mut b = router.store.load_account("b").unwrap();
        a.dispatches_ms = vec![950];
        b.dispatches_ms = vec![951, 952, 953, 954, 955];
        router.store.save_account(&a).unwrap();
        router.store.save_account(&b).unwrap();
        drop(_lock);

        let selected = router.reserve_one(&HashMap::new(), 1_010).unwrap();
        assert_eq!(selected.account.id, "a");
    }

    #[test]
    fn reservations_prevent_concurrent_overbooking() {
        let json = r#"{
          "version":1,
          "routing":{"strategy":"round_robin","reservation_ttl_seconds":10,"selection_failure_backoff_seconds":5},
          "defaults":{"limits":{"window_seconds":100,"max_dispatches":1,"max_active_workers":1}},
          "accounts":[{"id":"only","browser":{"instance":"only-instance"}}]
        }"#;
        let (_root, router) = setup_router(RoutingStrategy::RoundRobin, json);
        let router = Arc::new(router);
        let barrier = Arc::new(Barrier::new(3));
        let mut threads = Vec::new();
        for _ in 0..2 {
            let router = Arc::clone(&router);
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                router.reserve_one(&HashMap::new(), 1_000)
            }));
        }
        barrier.wait();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(RouterError::Exhausted(_))))
                .count(),
            1
        );
    }

    #[test]
    fn expired_reservation_is_reclaimed_at_exact_ttl_boundary() {
        let json = r#"{
          "version":1,
          "routing":{"strategy":"round_robin","reservation_ttl_seconds":1,"selection_failure_backoff_seconds":5},
          "defaults":{"limits":{"window_seconds":100,"max_dispatches":1,"max_active_workers":1}},
          "accounts":[{"id":"only","browser":{"instance":"only-instance"}}]
        }"#;
        let (_root, router) = setup_router(RoutingStrategy::RoundRobin, json);
        let first = router.reserve_one(&HashMap::new(), 1_000).unwrap();
        assert!(router.reserve_one(&HashMap::new(), 1_999).is_err());
        let second = router.reserve_one(&HashMap::new(), 2_000).unwrap();
        assert_ne!(first.reservation_id, second.reservation_id);
    }

    #[test]
    fn cooldown_and_auth_health_are_isolated_to_one_account() {
        let (_root, router) = setup_router(RoutingStrategy::RoundRobin, two_accounts_json());
        router
            .apply_rate_limit("a", "too_many_requests", Some(60), 1_000)
            .unwrap();
        let selected = router.reserve_one(&HashMap::new(), 1_001).unwrap();
        assert_eq!(selected.account.id, "b");
        router.release(&selected, 1_002).unwrap();

        router.mark_auth_required("b", 1_003).unwrap();
        let exhausted = router.reserve_one(&HashMap::new(), 1_004).unwrap_err();
        let RouterError::Exhausted(exhausted) = exhausted else {
            panic!("expected structured exhaustion");
        };
        assert_eq!(exhausted.accounts.len(), 2);
        assert!(exhausted
            .accounts
            .iter()
            .any(|entry| entry.account_id == "a" && entry.reason == "too_many_requests"));
        assert!(exhausted
            .accounts
            .iter()
            .any(|entry| entry.account_id == "b" && entry.reason == "auth_required"));
    }

    #[test]
    fn retained_affinity_ignores_disabled_for_new_assignment_but_never_migrates() {
        let json = r#"{
          "version":1,
          "routing":{"strategy":"round_robin","reservation_ttl_seconds":10,"selection_failure_backoff_seconds":5},
          "defaults":{"limits":{"window_seconds":100,"max_dispatches":10,"max_active_workers":3}},
          "accounts":[
            {"id":"a","enabled":false,"browser":{"instance":"instance-a"}},
            {"id":"b","enabled":true,"browser":{"instance":"instance-b"}}
          ]
        }"#;
        let (_root, router) = setup_router(RoutingStrategy::RoundRobin, json);
        let fresh = router.reserve_one(&HashMap::new(), 1_000).unwrap();
        assert_eq!(fresh.account.id, "b");
        router.release(&fresh, 1_001).unwrap();
        let resumed = router
            .reserve_for_account("a", &HashMap::new(), 1_002)
            .unwrap();
        assert_eq!(resumed.account.id, "a");
        assert!(matches!(
            router.reserve_for_account("missing", &HashMap::new(), 1_003),
            Err(RouterError::AccountUnavailable(id)) if id == "missing"
        ));
    }

    #[test]
    fn draining_account_skips_fresh_work_but_forced_retained_affinity_remains_allowed() {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        fs::write(
            bridge.join("accounts.json"),
            r#"{"version":1,"accounts":[{"id":"drain","enabled":true,"draining":true,"browser":{"instance":"drain"}},{"id":"ready","browser":{"instance":"ready"}}]}"#,
        ).unwrap();
        let router = AccountRouter::new(&bridge, &mount, LegacyAccountConfig::default());
        let fresh = router.reserve_one(&HashMap::new(), 1_000).unwrap();
        assert_eq!(fresh.account.id, "ready");
        router.release(&fresh, 1_000).unwrap();
        let retained = router
            .reserve_for_account("drain", &HashMap::new(), 1_000)
            .unwrap();
        assert_eq!(retained.account.id, "drain");
    }

    #[test]
    fn legacy_fallback_routes_to_default_account() {
        let root = tempdir().unwrap();
        let bridge = root.path().join("bridge");
        let mount = root.path().join("mount");
        fs::create_dir_all(&bridge).unwrap();
        fs::create_dir_all(&mount).unwrap();
        let legacy = LegacyAccountConfig {
            routing: RoutingConfig {
                strategy: RoutingStrategy::RoundRobin,
                reservation_ttl_seconds: 5,
                selection_failure_backoff_seconds: 5,
            },
            limits: AccountLimits {
                window_seconds: 60,
                max_dispatches: 2,
                max_active_workers: 1,
            },
            cooldown: CooldownConfig::default(),
            browser: BrowserInstanceConfig::legacy("legacy-worktree"),
        };
        let router = AccountRouter::new(&bridge, &mount, legacy);
        let reservation = router.reserve_one(&HashMap::new(), 1_000).unwrap();
        assert_eq!(reservation.account.id, "default");
        assert_eq!(reservation.account.browser.worktree, "legacy-worktree");
    }
}
