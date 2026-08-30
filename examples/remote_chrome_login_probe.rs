use gpt2omo::orca::{BrowserDriverConfig, BrowserDriverKind};
use gpt2omo::{BrowserPool, LegacyAccountConfig};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

fn isolated_roots() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let root = tempfile::tempdir().expect("failed to create tempdir");
    let bridge = root.path().join("bridge");
    let mount = root.path().join("mount");
    fs::create_dir_all(&bridge).expect("failed to create bridge dir");
    fs::create_dir_all(&mount).expect("failed to create mount dir");
    (root, bridge, mount)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = "http://127.0.0.1:9333";
    println!("=== Remote Chrome Login Probe ===");

    let (_root, bridge, mount) = isolated_roots();
    let accounts_json = r#"{
  "version": 1,
  "accounts": [{
    "id": "remote-chrome",
    "browser": {
      "driver": "chrome",
      "instance": "remote-chrome",
      "launch_mode": "attach_only",
      "cdp_endpoint": "http://127.0.0.1:9333"
    }
  }]
}"#;
    fs::write(bridge.join("accounts.json"), accounts_json)?;

    let pool = BrowserPool::new(
        &bridge,
        &mount,
        LegacyAccountConfig::default(),
        BrowserDriverConfig::with_driver(Some(BrowserDriverKind::Chrome), None, "active", None),
    );

    let health_before = pool.health("remote-chrome").await;
    println!(
        "login_state before page: {:?} ({})",
        health_before.login_state,
        health_before.detail.clone().unwrap_or_default()
    );

    let handle = pool.open_chatgpt_login_page("remote-chrome").await?;
    println!("page created: {}", handle.binding().page_id);

    for waited in [5usize, 10, 15] {
        tokio::time::sleep(Duration::from_secs(5)).await;
        match pool.verify(&handle.binding()).await {
            Ok(probe) => println!(
                "after ~{waited}s verify: url={} title={} generating={}",
                probe.url, probe.title, probe.generating
            ),
            Err(error) => println!("after ~{waited}s verify error: {error}"),
        }
        let health = pool.health("remote-chrome").await;
        println!(
            "after ~{waited}s health login_state: {:?} ({})",
            health.login_state,
            health.detail.unwrap_or_default()
        );
    }

    pool.close(&handle.binding()).await?;
    println!("probe page closed");
    Ok(())
}
