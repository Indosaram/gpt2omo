pub mod account_diagnostics;
pub mod account_state;
pub mod accounts;
pub mod browser_pool;
pub mod cli;
pub mod error;
pub mod events;
pub mod fresh_dispatch;
pub mod mass_ulw_web;
pub mod onboarding;
pub mod orca;
pub mod router;
pub mod security;
pub mod server;
pub mod telemetry;
pub mod tools;
pub mod web_session;

pub use account_diagnostics::{
    collect_account_diagnostics, recover_stale_account_health, AccountDiagnostic,
    AccountDiagnosticsReport, AccountRoutingState, ACCOUNT_DIAGNOSTICS_VERSION,
};
pub use accounts::{
    AccountConfig, AccountDefaults, AccountLimits, AccountsConfig, BrowserInstanceConfig,
    CooldownConfig, LegacyAccountConfig, RoutingConfig, RoutingStrategy, LEGACY_ACCOUNT_ID,
};
pub use browser_pool::{
    BrowserHealth, BrowserLoginState, BrowserPool, BrowserReachability, BrowserTarget, PageHandle,
};
pub use cli::Cli;
pub use error::{BridgeError, Result};
pub use events::{EventBus, HarnessEvent};
pub use onboarding::{
    activate_pending_accounts_config, activation_blocking_scope_ids, legacy_scope_blockers,
    load_pending_accounts_config, pending_account_health, pending_accounts_path,
    prepare_pending_accounts_config, LegacyScopeBlocker, LegacyScopeBlockerKind,
};
pub use router::{AccountRouter, RouteReservation, RouterError, RoutingExhausted};
pub use security::{
    default_bridge_base_dir, default_scope_dir, BrowserBinding, Workspace, WorkspaceMux,
    WorkspaceScope, WorkspaceScopeLock,
};
pub use server::{create_router, AppState};

pub fn load_dotenv_if_present() {
    if let Ok(content) = std::fs::read_to_string(std::path::Path::new(".env")) {
        for line in content.lines() {
            let Some((key, value)) = parse_dotenv_assignment(line) else {
                continue;
            };
            if std::env::var_os(key).is_none() {
                std::env::set_var(key, value);
            }
        }
    }
}

fn parse_dotenv_assignment(line: &str) -> Option<(&str, &str)> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }

    let (key, raw_value) = line.split_once('=')?;
    let key = key.trim();
    if !key.starts_with("OMO_") || key.as_bytes().contains(&0) {
        return None;
    }

    let mut value = raw_value.trim();
    if value.len() >= 2
        && ((value.starts_with('"') && value.ends_with('"'))
            || (value.starts_with('\'') && value.ends_with('\'')))
    {
        value = &value[1..value.len() - 1];
    }
    if value.as_bytes().contains(&0) {
        return None;
    }

    Some((key, value))
}

#[cfg(test)]
mod tests {
    use super::parse_dotenv_assignment;

    #[test]
    fn dotenv_assignment_parses_plain_and_quoted_omo_values() {
        assert_eq!(
            parse_dotenv_assignment(" OMO_SCOPE_DIR = scopes "),
            Some(("OMO_SCOPE_DIR", "scopes"))
        );
        assert_eq!(
            parse_dotenv_assignment("OMO_BRIDGE_URL=\"http://127.0.0.1:18800\""),
            Some(("OMO_BRIDGE_URL", "http://127.0.0.1:18800"))
        );
        assert_eq!(
            parse_dotenv_assignment("OMO_SUBAGENT_MODEL='mock model'"),
            Some(("OMO_SUBAGENT_MODEL", "mock model"))
        );
    }

    #[test]
    fn dotenv_assignment_rejects_unrelated_keys_and_nul_without_panicking() {
        assert_eq!(parse_dotenv_assignment("="), None);
        assert_eq!(parse_dotenv_assignment("PATH=/untrusted/bin"), None);
        assert_eq!(parse_dotenv_assignment("RUST_LOG=trace"), None);
        assert_eq!(parse_dotenv_assignment("OMO_BAD\0KEY=value"), None);
        assert_eq!(parse_dotenv_assignment("OMO_KEY=bad\0value"), None);
        assert_eq!(parse_dotenv_assignment("# comment"), None);
    }

    #[test]
    fn dotenv_assignment_handles_unmatched_quote_as_literal() {
        assert_eq!(
            parse_dotenv_assignment("OMO_KEY=\""),
            Some(("OMO_KEY", "\""))
        );
        assert_eq!(parse_dotenv_assignment("OMO_KEY='"), Some(("OMO_KEY", "'")));
    }
}
