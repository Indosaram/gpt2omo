pub mod cli;
pub mod error;
pub mod events;
pub mod mass_ulw_web;
pub mod orca;
pub mod security;
pub mod server;
pub mod telemetry;
pub mod tools;
pub mod web_session;

pub use cli::Cli;
pub use error::{BridgeError, Result};
pub use events::{EventBus, HarnessEvent};
pub use security::{
    default_bridge_base_dir, default_scope_dir, Workspace, WorkspaceMux, WorkspaceScope,
};
pub use server::{create_router, AppState};

pub fn load_dotenv_if_present() {
    let candidates = [
        std::path::PathBuf::from(".env"),
        std::path::PathBuf::from("../.env"),
    ];
    for path in &candidates {
        if let Ok(content) = std::fs::read_to_string(path) {
            for line in content.lines() {
                let Some((key, value)) = parse_dotenv_assignment(line) else {
                    continue;
                };
                if std::env::var_os(key).is_none() {
                    std::env::set_var(key, value);
                }
            }
            break;
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
