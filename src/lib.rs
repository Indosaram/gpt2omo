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
    collect_account_diagnostics, collect_account_diagnostics_opt, recover_stale_account_health,
    AccountDiagnostic, AccountDiagnosticsReport, AccountRoutingState, ACCOUNT_DIAGNOSTICS_VERSION,
};
pub use accounts::{
    AccountConfig, AccountDefaults, AccountLimits, AccountPlanTier, AccountsConfig,
    BrowserInstanceConfig, BrowserLaunchMode, CooldownConfig, LegacyAccountConfig, RoutingConfig,
    RoutingStrategy, LEGACY_ACCOUNT_ID,
};
pub use browser_pool::{
    browser_verify_failure_is_definitive, BrowserHealth, BrowserLoginState, BrowserPool,
    BrowserReachability, BrowserTarget, PageHandle, PageInspection,
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
pub use web_session::{cleanup_expired_retained_sessions, recover_dead_browser_scopes};

/// Security-critical settings that a dotenv file in the current working
/// directory must never be able to supply: the daemon may be started inside an
/// untrusted repository, and these keys control authentication, the command
/// allowlist, and the workspace scope location.
const DOTENV_DENIED_KEYS: &[&str] = &[
    "OMO_BRIDGE_TOKEN",
    "OMO_BRIDGE_TOKEN_FILE",
    "OMO_BRIDGE_INSECURE_NO_AUTH",
    "OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS",
    "OMO_BRIDGE_READ_ONLY",
    "OMO_SCOPE_DIR",
    "OMO_ALLOWED_BINARIES",
    "ALLOWED_BINARIES",
];

/// Pure predicate deciding whether a dotenv key may be imported into the
/// process environment.
fn dotenv_key_is_importable(key: &str) -> bool {
    !DOTENV_DENIED_KEYS.contains(&key)
}

pub fn load_dotenv_if_present() {
    if let Ok(content) = std::fs::read_to_string(std::path::Path::new(".env")) {
        for line in content.lines() {
            let Some((key, value)) = parse_dotenv_assignment(line) else {
                continue;
            };
            if !dotenv_key_is_importable(key) {
                eprintln!(
                    "warning: ignoring security-critical key {key} from .env; set it in the real environment or pass the matching CLI flag"
                );
                continue;
            }
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
    use super::{dotenv_key_is_importable, parse_dotenv_assignment, DOTENV_DENIED_KEYS};

    #[test]
    fn dotenv_denies_security_critical_keys_and_allows_ordinary_omo_keys() {
        assert!(!dotenv_key_is_importable("OMO_BRIDGE_INSECURE_NO_AUTH"));
        for key in DOTENV_DENIED_KEYS {
            assert!(!dotenv_key_is_importable(key), "{key} must stay denied");
        }
        assert!(dotenv_key_is_importable("OMO_SUBAGENT_MODEL"));
        assert!(dotenv_key_is_importable("OMO_BRIDGE_URL"));
    }

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
