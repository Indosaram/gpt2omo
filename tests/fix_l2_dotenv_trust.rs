//! A `.env` in the current working directory must never be able to flip
//! security-critical bridge settings (auth, token, command allowlist, scope dir).
//! Non-security `OMO_` keys must keep working.

use std::fs;

const DENIED_KEYS: &[&str] = &[
    "OMO_BRIDGE_TOKEN",
    "OMO_BRIDGE_TOKEN_FILE",
    "OMO_BRIDGE_INSECURE_NO_AUTH",
    "OMO_BRIDGE_ALLOW_ARBITRARY_COMMANDS",
    "OMO_BRIDGE_READ_ONLY",
    "OMO_SCOPE_DIR",
    "OMO_ALLOWED_BINARIES",
];

const ALLOWED_KEY: &str = "OMO_SUBAGENT_MODEL";

#[test]
fn cwd_dotenv_cannot_inject_security_critical_settings() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dotenv = DENIED_KEYS
        .iter()
        .map(|key| format!("{key}=attacker-controlled\n"))
        .collect::<String>()
        + &format!("{ALLOWED_KEY}=mock-model\n");
    fs::write(dir.path().join(".env"), dotenv).expect("write .env");

    for key in DENIED_KEYS.iter().chain(std::iter::once(&ALLOWED_KEY)) {
        std::env::remove_var(key);
    }

    let original_cwd = std::env::current_dir().expect("cwd");
    std::env::set_current_dir(dir.path()).expect("enter untrusted repo");
    gpt2omo::load_dotenv_if_present();
    std::env::set_current_dir(&original_cwd).expect("restore cwd");

    let imported_security_keys: Vec<&str> = DENIED_KEYS
        .iter()
        .copied()
        .filter(|key| std::env::var_os(key).is_some())
        .collect();
    let allowed_value = std::env::var(ALLOWED_KEY).ok();

    for key in DENIED_KEYS.iter().chain(std::iter::once(&ALLOWED_KEY)) {
        std::env::remove_var(key);
    }

    assert!(
        imported_security_keys.is_empty(),
        "untrusted .env injected security-critical settings into the process environment: {imported_security_keys:?}"
    );
    assert_eq!(
        allowed_value.as_deref(),
        Some("mock-model"),
        "non-security OMO_ keys must still be importable from .env"
    );
}
