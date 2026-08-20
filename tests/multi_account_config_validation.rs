use gpt2omo::{AccountRouter, LegacyAccountConfig};
use std::fs;

#[test]
fn disabled_accounts_can_share_dormant_browser_instance() {
    let root = tempfile::tempdir().unwrap();
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    fs::create_dir_all(&bridge).unwrap();
    fs::create_dir_all(&mount).unwrap();
    fs::write(
        bridge.join("accounts.json"),
        r#"{
          "version": 1,
          "accounts": [
            {"id": "old-a", "enabled": false, "browser": {"instance": "dormant"}},
            {"id": "old-b", "enabled": false, "browser": {"instance": "dormant"}},
            {"id": "active", "enabled": true, "browser": {"instance": "active-instance"}}
          ]
        }"#,
    )
    .unwrap();

    let config = AccountRouter::new(&bridge, &mount, LegacyAccountConfig::default())
        .load_config()
        .unwrap();
    assert_eq!(config.accounts.len(), 3);
    assert_eq!(
        config
            .accounts
            .iter()
            .filter(|account| account.enabled)
            .count(),
        1
    );
}
